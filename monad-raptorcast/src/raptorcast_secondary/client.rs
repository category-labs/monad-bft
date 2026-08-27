// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{collections::BTreeMap, time::Instant};

use iset::IntervalMap;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor::ExecutorMetrics;
use monad_types::{NodeId, Round, RoundSpan, GENESIS_ROUND};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, warn};

use super::{
    super::config::RaptorCastConfigSecondaryClient,
    group_message::{ConfirmGroup, PrepareGroup, PrepareGroupResponse},
};
use crate::{
    raptorcast_secondary::group_message::NoConfirm,
    util::{SecondaryGroup, SecondaryGroupAssignment, MAX_GROUPS_PER_VALIDATOR},
};

// Default full_node_raptorcast.round_span is 240. Refuse to accept
// invites that exceed 4x that.
pub(crate) const MAX_ACCEPTED_ROUND_SPAN: Round = Round(960);

monad_executor::metric_consts! {
    pub CLIENT_NUM_CURRENT_GROUPS {
        name: "monad.bft.raptorcast.secondary.client.num_current_groups",
        help: "Current number of raptorcast secondary groups as client",
    }
    pub CLIENT_RECEIVED_INVITES {
        name: "monad.bft.raptorcast.secondary.client.received_invites",
        help: "Group invites received as raptorcast secondary client",
    }
    pub CLIENT_RECEIVED_CONFIRMS {
        name: "monad.bft.raptorcast.secondary.client.received_confirms",
        help: "Group confirmations received as raptorcast secondary client",
    }
}

fn init_executor_metrics() -> ExecutorMetrics {
    ExecutorMetrics::with_metric_defs(&[
        CLIENT_NUM_CURRENT_GROUPS,
        CLIENT_RECEIVED_INVITES,
        CLIENT_RECEIVED_CONFIRMS,
    ])
}

// The state we hold for a single publishing validator.
//
// Invariants, both maintained at invite admission:
// - live_count() <= MAX_GROUPS_PER_VALIDATOR, by freshest-wins
//   eviction;
// - live round spans (pending and confirmed together) are pairwise
//   disjoint, by rejecting overlapping invites.
struct ValidatorGroups<PT: PubKey> {
    // start_round -> accepted group invite awaiting confirmation
    pending: BTreeMap<Round, PrepareGroup<PT>>,
    // Confirmed groups
    confirmed: IntervalMap<Round, SecondaryGroupAssignment<PT>>,
}

impl<PT: PubKey> Default for ValidatorGroups<PT> {
    fn default() -> Self {
        Self {
            pending: BTreeMap::new(),
            confirmed: IntervalMap::new(),
        }
    }
}

impl<PT: PubKey> ValidatorGroups<PT> {
    fn live_count(&self) -> usize {
        self.pending.len() + self.confirmed.len()
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.confirmed.is_empty()
    }

    // Whether [start, end) overlaps any live (pending or confirmed)
    // entry.
    fn has_overlap(&self, start: Round, end: Round) -> bool {
        let confirmed_overlaps = self.confirmed.has_overlap(start..end);
        let pending_overlaps = self
            .pending
            .range(..end)
            .any(|(_, invite)| invite.end_round > start);
        confirmed_overlaps || pending_overlaps
    }

    // Drop entries that can no longer become active: pending invites
    // whose start round has passed, confirmed groups that ended, and
    // entries starting beyond max_start_round (cold-start leftovers
    // that live admission would have rejected).
    fn prune(&mut self, curr_round: Round, max_start_round: Round) {
        self.pending
            .retain(|&start_round, _| start_round > curr_round && start_round <= max_start_round);

        let expired: Vec<_> = self
            .confirmed
            .intervals(..)
            .filter(|interval| interval.end <= curr_round || interval.start > max_start_round)
            .collect();
        for interval in expired {
            self.confirmed.remove(interval);
        }
    }

    // Remove the live entry with the smallest end round, returning
    // its start round. On a tie, prefer dropping a pending invite.
    fn evict_stalest(&mut self) -> Option<Round> {
        let pending = self
            .pending
            .iter()
            .min_by_key(|(_, invite)| invite.end_round)
            .map(|(&start_round, invite)| (start_round, invite.end_round));
        let confirmed = self
            .confirmed
            .intervals(..)
            .min_by_key(|interval| interval.end);

        match (pending, confirmed) {
            (Some((start_round, pending_end)), Some(interval)) => {
                if pending_end <= interval.end {
                    self.pending.remove(&start_round);
                    Some(start_round)
                } else {
                    let start_round = interval.start;
                    self.confirmed.remove(interval);
                    Some(start_round)
                }
            }
            (Some((start_round, _)), None) => {
                self.pending.remove(&start_round);
                Some(start_round)
            }
            (None, Some(interval)) => {
                let start_round = interval.start;
                self.confirmed.remove(interval);
                Some(start_round)
            }
            (None, None) => None,
        }
    }
}

// This is for when the router is playing the role of a client
// That is, we are a full-node receiving group invites from a validator
pub struct Client<ST>
where
    ST: CertificateSignatureRecoverable,
{
    client_node_id: NodeId<CertificateSignaturePubKey<ST>>, // Our (full-node) node_id as an invitee

    // Full nodes may choose to reject a request if it doesn’t have enough
    // upload bandwidth to broadcast chunk to a large group.
    config: RaptorCastConfigSecondaryClient,

    // Per-validator group formation state: invites we have accepted
    // and confirmed groups that haven't expired yet. Groups
    // may overlap across validators.
    validator_groups: BTreeMap<
        NodeId<CertificateSignaturePubKey<ST>>,
        ValidatorGroups<CertificateSignaturePubKey<ST>>,
    >,

    // Once a group is confirmed, it is sent to this channel
    group_sink_channel: UnboundedSender<SecondaryGroupAssignment<CertificateSignaturePubKey<ST>>>,

    // For avoiding accepting invites/confirms for rounds we've already started
    curr_round: Round,

    // Helps bootstrap the full-node when it is not yet receiving proposals and
    // cannot call enter_round() to advance state as it doesn't know what round
    // we are at.
    last_round_heartbeat: Instant,

    // Metrics
    metrics: ExecutorMetrics,
}

impl<ST> Client<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new(
        client_node_id: NodeId<CertificateSignaturePubKey<ST>>,
        group_sink_channel: UnboundedSender<
            SecondaryGroupAssignment<CertificateSignaturePubKey<ST>>,
        >,
        config: RaptorCastConfigSecondaryClient,
    ) -> Self {
        assert!(
            config.max_num_group > 0,
            "max_num_group must be greater than 0"
        );
        // There's no Instant::zero(). Use a value such that we will accept the
        // first invite, even though its far off `curr_round`.
        let last_round_heartbeat = Instant::now() - config.invite_accept_heartbeat;
        Self {
            client_node_id,
            config,
            validator_groups: BTreeMap::new(),
            group_sink_channel,
            curr_round: GENESIS_ROUND,
            last_round_heartbeat,
            metrics: init_executor_metrics(),
        }
    }

    // Called from UpdateCurrentRound
    pub fn enter_round(&mut self, curr_round: Round) {
        // Sanity check on curr_round
        if curr_round < self.curr_round {
            error!(
                "RaptorCastSecondary ignoring backwards round \
                {:?} -> {:?}",
                self.curr_round, curr_round
            );
            return;
        } else if curr_round > self.curr_round + Round(1) {
            debug!(
                "RaptorCastSecondary detected round gap \
                {:?} -> {:?}",
                self.curr_round, curr_round
            );
        }

        self.curr_round = curr_round;
        self.last_round_heartbeat = Instant::now();

        // Clean up old invitations and groups that should already
        // have been consumed, along with cold-start leftovers beyond
        // the invite window.
        let max_start_round = curr_round + self.config.invite_future_dist_max;
        self.validator_groups.retain(|_, groups| {
            groups.prune(curr_round, max_start_round);
            !groups.is_empty()
        });
        let current_group_count = self.get_current_group_count();
        self.metrics
            .gauge(CLIENT_NUM_CURRENT_GROUPS)
            .set(current_group_count);
    }

    // If we are not receiving proposals, then we don't know what the current
    // round is, and should advance the state machine despite self.curr_round
    fn is_receiving_proposals(&self) -> bool {
        Instant::now() < self.last_round_heartbeat + self.config.invite_accept_heartbeat
    }

    fn validate_prepare_group_message(
        &self,
        invite_msg: &PrepareGroup<CertificateSignaturePubKey<ST>>,
    ) -> bool {
        // Round span sanity checks
        if invite_msg.start_round >= invite_msg.end_round {
            warn!(
                "RaptorCastSecondary rejecting invite message with \
                   empty/malformed round span, invite = {:?}",
                invite_msg
            );
            return false;
        }
        let span_len = invite_msg.end_round - invite_msg.start_round;
        if span_len > MAX_ACCEPTED_ROUND_SPAN {
            warn!(
                "RaptorCastSecondary rejecting invite message with \
                     round span exceeding max accepted span, invite = {:?}",
                invite_msg
            );
            return false;
        }

        // Reject late round
        if invite_msg.start_round <= self.curr_round {
            warn!(
                "RaptorCastSecondary rejecting invite for round that \
                        already started, curr_round {:?}, invite = {:?}",
                self.curr_round, invite_msg
            );
            return false;
        }

        // Reject invite outside config invitation bounds, unless we are
        // not receiving any proposals ATM and should accept first group
        // invite, even if far from what we believe is the current round.
        if (invite_msg.start_round < self.curr_round + self.config.invite_future_dist_min
            || invite_msg.start_round > self.curr_round + self.config.invite_future_dist_max)
            && self.is_receiving_proposals()
        {
            warn!(
                "RaptorCastSecondary rejecting invite outside bounds \
                        [{:?}, {:?}], curr_round {:?}, invite = {:?}",
                self.config.invite_future_dist_min,
                self.config.invite_future_dist_max,
                self.curr_round,
                invite_msg
            );
            return false;
        }

        // Check doesn't exceed max group size
        if invite_msg.max_group_size > self.config.max_group_size {
            warn!(
                "RaptorCastSecondary rejecting invite with group size \
                        {} that exceeds max group size {}",
                invite_msg.max_group_size, self.config.max_group_size
            );
            return false;
        }

        let log_exceed_max_num_group = || {
            debug!(
                "RaptorCastSecondary rejected invite for rounds \
                        [{:?}, {:?}) from validator {:?} due to exceeding number of active groups",
                invite_msg.start_round, invite_msg.end_round, invite_msg.validator_id
            );
        };
        let mut num_current_groups = 0;

        for (validator_id, groups) in self.validator_groups.iter() {
            // Check if we already have an overlapping group from the
            // same validator, e.g. [30, 40)->validator3 but we already
            // have [25, 35)->validator3. If so reject the invite.
            //
            // Note that we accept overlaps across different validators,
            // e.g. [30, 40)->validator3 + [25, 35)->validator4
            if validator_id == &invite_msg.validator_id
                && groups.has_overlap(invite_msg.start_round, invite_msg.end_round)
            {
                warn!(
                    "RaptorCastSecondary received self-overlapping \
                            invite for rounds [{:?}, {:?}) from validator {:?}",
                    invite_msg.start_round, invite_msg.end_round, invite_msg.validator_id
                );
                return false;
            }

            // Check that the confirmed groups, plus the groups we were
            // invited to but are still unconfirmed, don't exceed max
            // number of groups during the invite's round span
            num_current_groups += groups
                .confirmed
                .intervals(invite_msg.start_round..invite_msg.end_round)
                .count();
            num_current_groups += groups
                .pending
                .range(..invite_msg.end_round)
                .filter(|(_, other)| Self::overlaps(other.start_round, other.end_round, invite_msg))
                .count();
            if num_current_groups + 1 > self.config.max_num_group {
                log_exceed_max_num_group();
                return false;
            }
        }

        true
    }

    pub fn handle_prepare_group_message(
        &mut self,
        invite_msg: PrepareGroup<CertificateSignaturePubKey<ST>>,
    ) -> PrepareGroupResponse<CertificateSignaturePubKey<ST>> {
        debug!(
            ?invite_msg,
            "RaptorCastSecondary Client received group invite"
        );
        self.metrics.gauge(CLIENT_RECEIVED_INVITES).inc();

        // Check the invite for duplicates & bandwidth requirements
        if !self.validate_prepare_group_message(&invite_msg) {
            return PrepareGroupResponse {
                req: invite_msg,
                node_id: self.client_node_id,
                accept: false,
            };
        }

        let mut accept = true;

        // Let's remember about this invite so that we don't blindly
        // accept any confirmation message.
        let groups = self
            .validator_groups
            .entry(invite_msg.validator_id)
            .or_default();
        groups
            .pending
            .insert(invite_msg.start_round, invite_msg.clone());

        // Bound the state a single validator can make us hold across
        // pending invites and confirmed groups. Caveat: eviction does
        // not yet revoke groups already forwarded to the primary
        // instance.
        while groups.live_count() > MAX_GROUPS_PER_VALIDATOR {
            let evicted_start = groups.evict_stalest();
            if evicted_start == Some(invite_msg.start_round) {
                warn!(
                    ?invite_msg,
                    "RaptorCastSecondary rejecting invite staler than all live groups held for its validator"
                );
                accept = false;
            } else {
                debug!(
                    ?evicted_start,
                    ?invite_msg,
                    "RaptorCastSecondary evicting stalest group to admit fresher invite"
                );
            }
        }

        PrepareGroupResponse {
            req: invite_msg,
            node_id: self.client_node_id,
            accept,
        }
    }

    pub fn handle_confirm_group_message(&mut self, confirm_msg: &ConfirmGroup<ST>) -> bool {
        let start_round = &confirm_msg.prepare.start_round;

        // Drop the message if the round span is invalid
        let Some(round_span) = RoundSpan::new(
            confirm_msg.prepare.start_round,
            confirm_msg.prepare.end_round,
        ) else {
            warn!(
                "RaptorCastSecondary ignoring invalid round span, confirm = {:?}",
                confirm_msg
            );
            return false;
        };

        // Drop the group if we've already entered the round
        if start_round <= &self.curr_round {
            warn!(
                "RaptorCastSecondary ignoring late confirm, curr_round \
                        {:?}, confirm = {:?}",
                self.curr_round, confirm_msg
            );
            return false;
        }

        let Some(groups) = self
            .validator_groups
            .get_mut(&confirm_msg.prepare.validator_id)
        else {
            warn!(
                ?confirm_msg,
                "RaptorCastSecondary ignoring ConfirmGroup from \
                            unrecognized validator id"
            );
            return false;
        };

        // TODO: discount validator reputation if it has sent an invalid
        // ConfirmGroup
        let Some(accepted_invite) = groups.pending.get(start_round) else {
            warn!(
                "RaptorCastSecondary Ignoring confirmation message \
                        for unrecognized start round: {:?}",
                confirm_msg
            );
            return false;
        };

        if accepted_invite != &confirm_msg.prepare {
            warn!(
                ?accepted_invite,
                confirming_invite = ?confirm_msg.prepare,
                "RaptorCastSecondary dropping ConfirmGroup that \
                                doesn't match the original invite"
            );
            return false;
        }

        let confirm_group_size = confirm_msg.peers.len();
        if confirm_group_size > confirm_msg.prepare.max_group_size {
            warn!(
                ?confirm_msg,
                "RaptorCastSecondary dropping ConfirmGroup that \
                                is larger ({}) than the promised max_group_size ({})",
                confirm_msg.peers.len(),
                confirm_msg.prepare.max_group_size,
            );
            return false;
        }

        if !confirm_msg.peers.contains(&self.client_node_id) {
            if confirm_msg.peers.len() == confirm_msg.prepare.max_group_size {
                debug!(
                    ?confirm_msg,
                    "Group invite has reached max_group_size. Node participation is not confirmed"
                );
                return false;
            }
            warn!(
                ?confirm_msg,
                "RaptorCastSecondary dropping ConfirmGroup \
                                with a group that does not contain our node_id",
            );
            return false;
        }

        let members = confirm_msg.peers.iter().copied().collect();
        let group = SecondaryGroupAssignment::new(
            confirm_msg.prepare.validator_id,
            round_span,
            // SAFETY: `members` includes self_id in a previous check,
            // so it must not be empty.
            SecondaryGroup::new_unchecked(members),
        );

        // Send the group to primary instance right away.
        // Doing so helps resolve activation of groups for full-nodes experiencing
        // round gaps, as they aren't receiving proposals and hence can't advance
        // (enter) rounds.
        if let Err(error) = self.group_sink_channel.send(group.clone()) {
            error!(
                "RaptorCastSecondary failed to send group to primary \
                                    Raptorcast instance: {}",
                error
            );
        }

        self.metrics.gauge(CLIENT_RECEIVED_CONFIRMS).inc();
        debug!(
            "RaptorCastSecondary Client confirmed group for \
             rounds [{:?}, {:?}) from validator {:?}, group size {}",
            confirm_msg.prepare.start_round,
            confirm_msg.prepare.end_round,
            confirm_msg.prepare.validator_id,
            confirm_group_size,
        );

        // Move the invite from pending to confirmed. Admission keeps
        // live spans disjoint, so this can never displace an entry.
        groups.pending.remove(&confirm_msg.prepare.start_round);
        groups
            .confirmed
            .insert(round_span.start..round_span.end, group);

        let current_group_count = self.get_current_group_count();
        self.metrics
            .gauge(CLIENT_NUM_CURRENT_GROUPS)
            .set(current_group_count);

        true
    }

    pub fn handle_no_confirm_message(
        &mut self,
        no_confim_msg: NoConfirm<CertificateSignaturePubKey<ST>>,
    ) {
        let validator_id = no_confim_msg.prepare.validator_id;
        let Some(groups) = self.validator_groups.get_mut(&validator_id) else {
            warn!(
                ?no_confim_msg,
                "RaptorCastSecondary ignoring no confirm message: unrecognized validator id",
            );
            return;
        };

        let Some(accepted_invite) = groups.pending.get(&no_confim_msg.prepare.start_round) else {
            warn!(
                ?no_confim_msg,
                "RaptorCastSecondary ignoring no confirm message: unrecognized start round",
            );
            return;
        };

        if accepted_invite != &no_confim_msg.prepare {
            warn!(
                ?accepted_invite,
                ?no_confim_msg,
                "RaptorCastSecondary ignoring no confirm message: invite mismatch",
            );
            return;
        };

        groups.pending.remove(&no_confim_msg.prepare.start_round);
        if groups.is_empty() {
            self.validator_groups.remove(&validator_id);
        }
        debug!(
            ?no_confim_msg,
            "RaptorCastSecondary received no confirm message. Releasing round to other invites",
        );
    }

    fn get_current_group_count(&self) -> u64 {
        self.validator_groups
            .values()
            .map(|groups| {
                groups
                    .confirmed
                    .intervals(self.curr_round..self.curr_round + Round(1))
                    .count() as u64
            })
            .sum()
    }

    fn overlaps(
        begin: Round,
        end: Round,
        group: &PrepareGroup<CertificateSignaturePubKey<ST>>,
    ) -> bool {
        assert!(begin <= end);
        assert!(group.start_round <= group.end_round);
        group.start_round < end && group.end_round > begin
    }

    pub fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    #[cfg(test)]
    pub fn get_curr_round(&self) -> Round {
        self.curr_round
    }

    #[cfg(test)]
    pub fn num_pending_confirms(&self) -> usize {
        self.validator_groups
            .values()
            .map(|groups| groups.pending.len())
            .sum()
    }

    #[cfg(test)]
    pub fn num_confirmed_groups(&self) -> usize {
        self.validator_groups
            .values()
            .map(|groups| groups.confirmed.len())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

    use super::{
        super::{
            super::{
                config::RaptorCastConfigSecondaryClient,
                util::{SecondaryGroup, SecondaryGroupAssignment},
            },
            group_message::{ConfirmGroup, NoConfirm, NoConfirmReason, PrepareGroup},
            Client,
        },
        *,
    };

    type ST = SecpSignature;
    type PubKeyType = CertificateSignaturePubKey<ST>;
    type RcToRcChannelGrp = (
        UnboundedSender<SecondaryGroupAssignment<PubKeyType>>,
        UnboundedReceiver<SecondaryGroupAssignment<PubKeyType>>,
    );

    #[test]
    fn malformed_prepare_messages() {
        let (clt_tx, _clt_rx): RcToRcChannelGrp = unbounded_channel();
        let self_id = nid(1);
        let mut clt = Client::<ST>::new(
            self_id,
            clt_tx,
            RaptorCastConfigSecondaryClient {
                max_num_group: 5,
                max_group_size: 10,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
                invite_accept_heartbeat: Duration::from_secs(10),
            },
        );

        let grp = SecondaryGroupAssignment::new(
            nid(2),
            RoundSpan::new(Round(1), Round(5)).unwrap(),
            SecondaryGroup::new([self_id, nid(3), nid(4), nid(5)].into_iter().collect()).unwrap(),
        );
        assert!(grp.is_member(&self_id));

        clt.validator_groups
            .entry(nid(2))
            .or_default()
            .confirmed
            .insert(Round(1)..Round(5), grp);

        let malformed_messages = [
            // group size overflow
            PrepareGroup {
                max_group_size: 11,
                validator_id: nid(2),
                start_round: Round(5),
                end_round: Round(6),
            },
            // round span backwards
            PrepareGroup {
                max_group_size: 1,
                validator_id: nid(2),
                start_round: Round(7),
                end_round: Round(5),
            },
            // unreasonably long round span
            PrepareGroup {
                max_group_size: 1,
                validator_id: nid(2),
                start_round: Round(5),
                // 5 + MAX_ACCEPTED_ROUND_SPAN + 1
                end_round: Round(966),
            },
        ];

        for message in malformed_messages {
            // handles without crashing
            let resp = clt.handle_prepare_group_message(message);
            assert!(!resp.accept)
        }
    }

    #[test]
    fn exceed_max_num_group() {
        let (clt_tx, _clt_rx): RcToRcChannelGrp = unbounded_channel();
        let self_id = nid(1);
        let mut clt = Client::<ST>::new(
            self_id,
            clt_tx,
            RaptorCastConfigSecondaryClient {
                max_num_group: 2,
                max_group_size: 50,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
                invite_accept_heartbeat: Duration::from_secs(10),
            },
        );

        let grp = SecondaryGroupAssignment::new(
            nid(2),
            RoundSpan::new(Round(1), Round(5)).unwrap(),
            SecondaryGroup::new([self_id, nid(3), nid(4), nid(5)].into_iter().collect()).unwrap(),
        );
        assert!(grp.is_member(&self_id));

        let groups = clt.validator_groups.entry(nid(2)).or_default();
        groups.confirmed.insert(Round(1)..Round(5), grp);
        groups.pending.insert(
            Round(4),
            PrepareGroup {
                max_group_size: 1,
                validator_id: nid(2),
                start_round: Round(4),
                end_round: Round(7),
            },
        );

        let malformed_message = PrepareGroup {
            max_group_size: 1,
            validator_id: nid(2),
            start_round: Round(3),
            end_round: Round(6),
        };
        let resp = clt.handle_prepare_group_message(malformed_message);
        assert!(!resp.accept);
    }

    #[test]
    fn test_get_current_group_count() {
        let (clt_tx, _clt_rx): RcToRcChannelGrp = unbounded_channel();
        let self_id = nid(1);
        let mut clt = Client::<ST>::new(
            self_id,
            clt_tx,
            RaptorCastConfigSecondaryClient {
                max_num_group: 2,
                max_group_size: 50,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
                invite_accept_heartbeat: Duration::from_secs(10),
            },
        );

        clt.curr_round = Round(2);
        let grp = SecondaryGroupAssignment::new(
            nid(2),
            RoundSpan::new(Round(1), Round(5)).unwrap(),
            SecondaryGroup::new([self_id, nid(3), nid(4), nid(5)].into_iter().collect()).unwrap(),
        );
        assert!(grp.is_member(&self_id));

        clt.validator_groups
            .entry(nid(2))
            .or_default()
            .confirmed
            .insert(Round(1)..Round(5), grp);

        clt.enter_round(Round(3));
        assert_eq!(clt.get_current_group_count(), 1);

        // when we enter round 5, the group [1, 5) is expired
        clt.enter_round(Round(5));
        assert_eq!(clt.get_current_group_count(), 0);
    }

    #[test]
    fn test_no_confirm_message() {
        let (clt_tx, _clt_rx): RcToRcChannelGrp = unbounded_channel();
        let self_id = nid(1);
        let mut clt = Client::<ST>::new(
            self_id,
            clt_tx,
            RaptorCastConfigSecondaryClient {
                max_num_group: 5,
                max_group_size: 3,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
                invite_accept_heartbeat: Duration::from_secs(10),
            },
        );

        let validator_id = nid(2);
        let prepare_invite = PrepareGroup {
            max_group_size: 3,
            validator_id,
            start_round: Round(5),
            end_round: Round(10),
        };

        // Set up client state: client has accepted an invite and is waiting for confirmation
        clt.validator_groups
            .entry(validator_id)
            .or_default()
            .pending
            .insert(Round(5), prepare_invite.clone());

        // Test 1: Valid NoConfirm message - should be accepted and remove the invite
        let valid_no_confirm = NoConfirm {
            prepare: prepare_invite,
            reason: NoConfirmReason::GroupFull,
        };

        clt.handle_no_confirm_message(valid_no_confirm);

        // Verify the invite was removed, together with the validator's
        // now-empty group state
        assert!(
            !clt.validator_groups.contains_key(&validator_id),
            "Valid NoConfirm should remove the invite from the pending confirms"
        );
    }

    // During cold-start we are not receiving proposals, so the invite
    // round window is waived and enter_round() is not called to prune
    // state. The per-validator cap must bound the state instead:
    // fresher invites evict the stalest entry, staler ones are
    // rejected.
    #[test]
    fn cold_start_pending_invites_capped_per_validator() {
        let (clt_tx, _clt_rx): RcToRcChannelGrp = unbounded_channel();
        let self_id = nid(1);
        let mut clt = Client::<ST>::new(
            self_id,
            clt_tx,
            RaptorCastConfigSecondaryClient {
                max_num_group: 5,
                max_group_size: 10,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
                invite_accept_heartbeat: Duration::from_secs(10),
            },
        );

        // Disjoint far-future spans from a single validator, so
        // neither the self-overlap check nor the max_num_group check
        // rejects them. Ends are increasing, so every invite past the
        // cap evicts the stalest one, keeping the freshest
        // MAX_GROUPS_PER_VALIDATOR.
        let mut num_accepted = 0;
        for k in 0..10 * MAX_GROUPS_PER_VALIDATOR as u64 {
            let invite = PrepareGroup {
                max_group_size: 10,
                validator_id: nid(2),
                start_round: Round(1_000 + 2 * k),
                end_round: Round(1_001 + 2 * k),
            };
            if clt.handle_prepare_group_message(invite).accept {
                num_accepted += 1;
            }
        }
        assert_eq!(num_accepted, 10 * MAX_GROUPS_PER_VALIDATOR);
        assert_eq!(clt.num_pending_confirms(), MAX_GROUPS_PER_VALIDATOR);

        // An invite staler than everything we hold is rejected
        let stale_invite = PrepareGroup {
            max_group_size: 10,
            validator_id: nid(2),
            start_round: Round(900),
            end_round: Round(901),
        };
        assert!(!clt.handle_prepare_group_message(stale_invite).accept);

        // A confirm for the first, since-evicted invite is rejected
        let evicted_confirm = ConfirmGroup {
            prepare: PrepareGroup {
                max_group_size: 10,
                validator_id: nid(2),
                start_round: Round(1_000),
                end_round: Round(1_001),
            },
            peers: vec![self_id, nid(3)].into(),
            name_records: Vec::new().into(),
        };
        assert!(!clt.handle_confirm_group_message(&evicted_confirm));

        // A confirm for the freshest, surviving invite succeeds
        let last_k = 10 * MAX_GROUPS_PER_VALIDATOR as u64 - 1;
        let survivor_confirm = ConfirmGroup {
            prepare: PrepareGroup {
                max_group_size: 10,
                validator_id: nid(2),
                start_round: Round(1_000 + 2 * last_k),
                end_round: Round(1_001 + 2 * last_k),
            },
            peers: vec![self_id, nid(3)].into(),
            name_records: Vec::new().into(),
        };
        assert!(clt.handle_confirm_group_message(&survivor_confirm));

        // The cap is per validator: another validator is not affected
        let invite = PrepareGroup {
            max_group_size: 10,
            validator_id: nid(3),
            start_round: Round(1_000),
            end_round: Round(1_001),
        };
        assert!(clt.handle_prepare_group_message(invite).accept);
    }

    // Alternating invite -> confirm keeps the pending side empty, so
    // eviction must cover confirmed groups too: past the cap, each
    // fresher confirm evicts the stalest confirmed group.
    //
    // Note that every confirmed group is still forwarded to the
    // primary instance and eviction does not revoke it there;
    // bounding the primary's map is a separate concern.
    #[test]
    fn cold_start_confirm_loop_capped_per_validator() {
        let (clt_tx, mut clt_rx): RcToRcChannelGrp = unbounded_channel();
        let self_id = nid(1);
        let mut clt = Client::<ST>::new(
            self_id,
            clt_tx,
            RaptorCastConfigSecondaryClient {
                max_num_group: 5,
                max_group_size: 10,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
                invite_accept_heartbeat: Duration::from_secs(10),
            },
        );

        let validator_id = nid(2);
        let mut num_confirmed = 0;
        for k in 0..10 * MAX_GROUPS_PER_VALIDATOR as u64 {
            let prepare = PrepareGroup {
                max_group_size: 10,
                validator_id,
                start_round: Round(1_000 + 2 * k),
                end_round: Round(1_001 + 2 * k),
            };
            if !clt.handle_prepare_group_message(prepare.clone()).accept {
                continue;
            }
            let confirm = ConfirmGroup {
                prepare,
                peers: vec![self_id, nid(3)].into(),
                name_records: Vec::new().into(),
            };
            if clt.handle_confirm_group_message(&confirm) {
                num_confirmed += 1;
            }
        }
        assert_eq!(num_confirmed, 10 * MAX_GROUPS_PER_VALIDATOR);
        assert_eq!(clt.num_confirmed_groups(), MAX_GROUPS_PER_VALIDATOR);

        // The primary instance received every confirmed group,
        // including since-evicted ones
        let mut num_forwarded = 0;
        while clt_rx.try_recv().is_ok() {
            num_forwarded += 1;
        }
        assert_eq!(num_forwarded, 10 * MAX_GROUPS_PER_VALIDATOR);

        // Advancing past the accepted spans frees the budget
        clt.enter_round(Round(2_000));
        let invite = PrepareGroup {
            max_group_size: 10,
            validator_id,
            start_round: Round(2_010),
            end_round: Round(2_011),
        };
        assert!(clt.handle_prepare_group_message(invite).accept);
    }

    // Far-future entries admitted during cold-start can never
    // activate once rounds advance (live admission would have
    // rejected them). They must be pruned so they free the
    // validator's budget instead of consuming it forever.
    #[test]
    fn far_future_entries_pruned_on_round_advance() {
        let (clt_tx, _clt_rx): RcToRcChannelGrp = unbounded_channel();
        let self_id = nid(1);
        let mut clt = Client::<ST>::new(
            self_id,
            clt_tx,
            RaptorCastConfigSecondaryClient {
                max_num_group: 5,
                max_group_size: 10,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
                invite_accept_heartbeat: Duration::from_secs(10),
            },
        );

        let validator_id = nid(2);
        let invite = |start: u64, end: u64| PrepareGroup {
            max_group_size: 10,
            validator_id,
            start_round: Round(start),
            end_round: Round(end),
        };

        // Cold-start: accept an in-window invite, a far-future invite
        // and a far-future confirmed group
        assert!(clt.handle_prepare_group_message(invite(50, 60)).accept);
        assert!(
            clt.handle_prepare_group_message(invite(5_000, 5_010))
                .accept
        );
        assert!(
            clt.handle_prepare_group_message(invite(6_000, 6_010))
                .accept
        );
        let confirm = ConfirmGroup {
            prepare: invite(6_000, 6_010),
            peers: vec![self_id, nid(3)].into(),
            name_records: Vec::new().into(),
        };
        assert!(clt.handle_confirm_group_message(&confirm));

        // Entering a round prunes everything starting beyond
        // curr_round + invite_future_dist_max, but keeps the
        // in-window invite
        clt.enter_round(Round(10));
        assert_eq!(clt.num_pending_confirms(), 1);
        assert_eq!(clt.get_current_group_count(), 0);

        // The validator's budget is freed: once the in-window invite
        // from above expires too, fresh in-window invites are all
        // admitted again, evicting the stalest ones past the cap
        clt.enter_round(Round(60));
        let mut num_accepted = 0;
        for k in 0..2 * MAX_GROUPS_PER_VALIDATOR as u64 {
            let invite = invite(61 + 2 * k, 62 + 2 * k);
            if clt.handle_prepare_group_message(invite).accept {
                num_accepted += 1;
            }
        }
        assert_eq!(num_accepted, 2 * MAX_GROUPS_PER_VALIDATOR);
        assert_eq!(clt.num_pending_confirms(), MAX_GROUPS_PER_VALIDATOR);
    }

    // Overlapping spans from the same validator are refused outright,
    // whether the held entry is pending or confirmed, and including
    // exact re-sends (the publisher never re-invites the same node
    // for a group, so this refuses no honest invite). Overlaps across
    // different validators are accepted.
    #[test]
    fn overlapping_invites_from_same_validator_rejected() {
        let (clt_tx, _clt_rx): RcToRcChannelGrp = unbounded_channel();
        let self_id = nid(1);
        let mut clt = Client::<ST>::new(
            self_id,
            clt_tx,
            RaptorCastConfigSecondaryClient {
                max_num_group: 5,
                max_group_size: 10,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
                invite_accept_heartbeat: Duration::from_secs(10),
            },
        );

        let invite = |validator: u64, start: u64, end: u64| PrepareGroup {
            max_group_size: 10,
            validator_id: nid(validator),
            start_round: Round(start),
            end_round: Round(end),
        };

        // Hold a pending invite [10, 20) and a confirmed group
        // [30, 40) from validator 2
        assert!(clt.handle_prepare_group_message(invite(2, 10, 20)).accept);
        assert!(clt.handle_prepare_group_message(invite(2, 30, 40)).accept);
        let confirm = ConfirmGroup {
            prepare: invite(2, 30, 40),
            peers: vec![self_id, nid(3)].into(),
            name_records: Vec::new().into(),
        };
        assert!(clt.handle_confirm_group_message(&confirm));

        // Overlapping the pending invite is refused, including an
        // exact re-send
        assert!(!clt.handle_prepare_group_message(invite(2, 10, 20)).accept);
        assert!(!clt.handle_prepare_group_message(invite(2, 15, 25)).accept);
        assert!(!clt.handle_prepare_group_message(invite(2, 5, 11)).accept);

        // Overlapping the confirmed group is refused
        assert!(!clt.handle_prepare_group_message(invite(2, 35, 45)).accept);
        assert!(!clt.handle_prepare_group_message(invite(2, 25, 31)).accept);

        // The refused invites left the held entries untouched
        assert_eq!(clt.num_pending_confirms(), 1);
        assert_eq!(clt.num_confirmed_groups(), 1);

        // Touching spans do not overlap
        assert!(clt.handle_prepare_group_message(invite(2, 20, 30)).accept);

        // Overlaps across validators are accepted
        assert!(clt.handle_prepare_group_message(invite(3, 15, 35)).accept);
    }

    // Creates a node id that we can refer to just from its seed
    fn nid(seed: u64) -> NodeId<PubKeyType> {
        let key_pair = get_key::<ST>(seed);
        let pub_key = key_pair.pubkey();
        NodeId::new(pub_key)
    }
}
