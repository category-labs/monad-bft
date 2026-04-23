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
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::ExecutorMetrics;
use monad_types::{BoundedU64, NodeId, Round, RoundSpan, GENESIS_ROUND};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, warn};

use super::{
    super::config::RaptorCastConfigSecondaryClient,
    group_message::{
        ConfirmGroup, PeerParticipation, PeerParticipationReport, PrepareGroup,
        PrepareGroupResponse,
    },
};
use crate::{
    raptorcast_secondary::group_message::NoConfirm,
    util::{SecondaryGroup, SecondaryGroupAssignment},
};

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
    pub CLIENT_SENT_REPORTS {
        name: "monad.bft.raptorcast.secondary.client.sent_reports",
        help: "Participation reports sent as raptorcast secondary client",
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

    // [start_round, end_round) -> SecondaryGroupAssignment
    // Represents all raptorcast groups that we have accepted and haven't expired
    // yet. The groups may overlap.
    confirmed_groups: IntervalMap<Round, SecondaryGroupAssignment<CertificateSignaturePubKey<ST>>>,

    // start_round -> validator_id -> group invite
    // Once we receive an invite, we remember how the invite looked like, so
    // that we don't blindly accept any confirmation message.
    pending_confirms: BTreeMap<
        Round,
        BTreeMap<
            NodeId<CertificateSignaturePubKey<ST>>,
            PrepareGroup<CertificateSignaturePubKey<ST>>,
        >,
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
            confirmed_groups: IntervalMap::new(),
            pending_confirms: BTreeMap::new(),
            group_sink_channel,
            curr_round: GENESIS_ROUND,
            last_round_heartbeat,
            metrics: ExecutorMetrics::default(),
        }
    }

    // Called from UpdateCurrentRound.
    // Returns participation reports for any groups that expired this round,
    // paired with the validator node_id to send each report to.
    pub fn enter_round(
        &mut self,
        curr_round: Round,
    ) -> Vec<(
        PeerParticipationReport<CertificateSignaturePubKey<ST>>,
        NodeId<CertificateSignaturePubKey<ST>>,
    )> {
        // Sanity check on curr_round
        if curr_round < self.curr_round {
            error!(
                "RaptorCastSecondary ignoring backwards round \
                {:?} -> {:?}",
                self.curr_round, curr_round
            );
            return Vec::new();
        } else if curr_round > self.curr_round + Round(1) {
            debug!(
                "RaptorCastSecondary detected round gap \
                {:?} -> {:?}",
                self.curr_round, curr_round
            );
        }

        self.curr_round = curr_round;
        self.last_round_heartbeat = Instant::now();

        // Clean up old invitations.
        self.pending_confirms.retain(|&key, _| key > curr_round);

        // Remove all groups that should already have been consumed
        let mut keys_to_remove = Vec::new();
        for interval in self.confirmed_groups.intervals(..self.curr_round) {
            if interval.end <= self.curr_round {
                keys_to_remove.push(interval.clone());
            }
        }

        let mut reports = Vec::new();
        for interval_key in keys_to_remove {
            if let Some(group_assignment) = self.confirmed_groups.remove(interval_key) {
                let report = self.build_participation_report(&group_assignment);
                let validator_id = *group_assignment.publisher_id();
                reports.push((report, validator_id));
                self.metrics[CLIENT_SENT_REPORTS] += 1;
            }
        }

        self.metrics[CLIENT_NUM_CURRENT_GROUPS] = self.get_current_group_count();

        reports
    }

    fn build_participation_report(
        &self,
        group: &SecondaryGroupAssignment<CertificateSignaturePubKey<ST>>,
    ) -> PeerParticipationReport<CertificateSignaturePubKey<ST>> {
        // TODO: Replace placeholder scores with actual chunk-based participation metrics
        let peer_scores: Vec<PeerParticipation<CertificateSignaturePubKey<ST>>> = group
            .group()
            .iter()
            .filter(|peer| **peer != self.client_node_id)
            .map(|peer| PeerParticipation {
                peer: *peer,
                participation_score: BoundedU64::new(100).unwrap(),
            })
            .collect();

        debug!(
            round_span = ?group.round_span(),
            validator = ?group.publisher_id(),
            num_peers = peer_scores.len(),
            "Building participation report"
        );

        PeerParticipationReport {
            reporter: self.client_node_id,
            validator_id: *group.publisher_id(),
            round_span: *group.round_span(),
            peer_scores: peer_scores.into(),
        }
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
        // Sanity check the message
        if invite_msg.start_round >= invite_msg.end_round {
            warn!(
                "RaptorCastSecondary rejecting invite message due to \
                        failed sanity check: {:?}",
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

        // Check confirmed groups
        for group in self
            .confirmed_groups
            .values(invite_msg.start_round..invite_msg.end_round)
        {
            // Check if we already have an overlapping invite from same
            // validator, e.g. [30, 40)->validator3 but we already
            // have [25, 35)->validator3
            // Note that we accept overlaps across different validators,
            // e.g. [30, 40)->validator3 + [25, 35)->validator4
            if group.publisher_id() == &invite_msg.validator_id {
                warn!(
                    "RaptorCastSecondary received self-overlapping \
                            invite for rounds [{:?}, {:?}) from validator {:?}",
                    invite_msg.start_round, invite_msg.end_round, invite_msg.validator_id
                );
                return false;
            }

            // Check that it doesn't exceed max number of groups during round span
            num_current_groups += 1;
            if num_current_groups + 1 > self.config.max_num_group {
                log_exceed_max_num_group();
                return false;
            }
        }

        // Check groups we were invited to but are still unconfirmed
        for (&key, other_invites) in self.pending_confirms.iter() {
            if key >= invite_msg.end_round {
                // Remaining keys are outside the invite range
                break;
            }

            for other in other_invites.values() {
                if !Self::overlaps(other.start_round, other.end_round, invite_msg) {
                    continue;
                }

                num_current_groups += 1;
                if num_current_groups + 1 > self.config.max_num_group {
                    log_exceed_max_num_group();
                    return false;
                }
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
        self.metrics[CLIENT_RECEIVED_INVITES] += 1;

        // Check the invite for duplicates & bandwidth requirements
        let accept = self.validate_prepare_group_message(&invite_msg);

        if accept {
            // Let's remember about this invite so that we don't blindly
            // accept any confirmation message.
            self.pending_confirms
                .entry(invite_msg.start_round)
                .or_default()
                .insert(invite_msg.validator_id, invite_msg.clone());
        }

        PrepareGroupResponse {
            req: invite_msg,
            node_id: self.client_node_id,
            accept,
        }
    }

    pub fn handle_confirm_group_message(&mut self, confirm_msg: ConfirmGroup<ST>) -> bool {
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

        let Some(invites) = self.pending_confirms.get_mut(start_round) else {
            warn!(
                "RaptorCastSecondary Ignoring confirmation message \
                        for unrecognized start round: {:?}",
                confirm_msg
            );
            return false;
        };

        // TODO: discount validator reputation if it has sent an invalid
        // ConfirmGroup
        let Some(accepted_invite) = invites.get(&confirm_msg.prepare.validator_id) else {
            warn!(
                ?confirm_msg,
                "RaptorCastSecondary ignoring ConfirmGroup from \
                            unrecognized validator id"
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

        let members = confirm_msg.peers.into_inner().into_iter().collect();
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

        self.metrics[CLIENT_RECEIVED_CONFIRMS] += 1;
        debug!(
            "RaptorCastSecondary Client confirmed group for \
             rounds [{:?}, {:?}) from validator {:?}, group size {}",
            confirm_msg.prepare.start_round,
            confirm_msg.prepare.end_round,
            confirm_msg.prepare.validator_id,
            confirm_group_size,
        );

        // Remove the invite from pending_confirms and add the group to confirmed_groups
        invites.remove(&confirm_msg.prepare.validator_id);
        self.confirmed_groups
            .force_insert(round_span.start..round_span.end, group);
        self.metrics[CLIENT_NUM_CURRENT_GROUPS] = self.get_current_group_count();

        true
    }

    pub fn handle_no_confirm_message(
        &mut self,
        no_confim_msg: NoConfirm<CertificateSignaturePubKey<ST>>,
    ) {
        let Some(invites) = self
            .pending_confirms
            .get_mut(&no_confim_msg.prepare.start_round)
        else {
            warn!(
                ?no_confim_msg,
                "RaptorCastSecondary ignoring no confirm message: unrecognized start round",
            );
            return;
        };

        let Some(accepted_invite) = invites.get(&no_confim_msg.prepare.validator_id) else {
            warn!(
                ?no_confim_msg,
                "RaptorCastSecondary ignoring no confirm message: unrecognized validator id",
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

        invites.remove(&no_confim_msg.prepare.validator_id);
        debug!(
            ?no_confim_msg,
            "RaptorCastSecondary received no confirm message. Releasing round to other invites",
        );
    }

    fn get_current_group_count(&self) -> u64 {
        self.confirmed_groups
            .intervals(self.curr_round..self.curr_round + Round(1))
            .count() as u64
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
        self.pending_confirms.len()
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
            group_message::{NoConfirm, NoConfirmReason, PrepareGroup},
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

    fn make_client(
        self_id: NodeId<PubKeyType>,
    ) -> (
        Client<ST>,
        UnboundedReceiver<SecondaryGroupAssignment<PubKeyType>>,
    ) {
        let (clt_tx, clt_rx): RcToRcChannelGrp = unbounded_channel();
        let clt = Client::<ST>::new(
            self_id,
            clt_tx,
            RaptorCastConfigSecondaryClient {
                max_num_group: 5,
                max_group_size: 50,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
                invite_accept_heartbeat: Duration::from_secs(10),
            },
        );
        (clt, clt_rx)
    }

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

        clt.confirmed_groups.insert(Round(1)..Round(5), grp);

        let malformed_messages = [
            // group size overflow
            PrepareGroup {
                max_group_size: 11,
                validator_id: nid(2),
                start_round: Round(5),
                end_round: Round(6),
            },
            // invalid round span
            PrepareGroup {
                max_group_size: 1,
                validator_id: nid(2),
                start_round: Round(7),
                end_round: Round(5),
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

        clt.confirmed_groups.insert(Round(1)..Round(5), grp);

        clt.pending_confirms.insert(
            Round(4),
            BTreeMap::from([(
                nid(2),
                PrepareGroup {
                    max_group_size: 1,
                    validator_id: nid(2),
                    start_round: Round(4),
                    end_round: Round(7),
                },
            )]),
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
        let self_id = nid(1);
        let (mut clt, _rx) = make_client(self_id);

        clt.curr_round = Round(2);
        let grp = SecondaryGroupAssignment::new(
            nid(2),
            RoundSpan::new(Round(1), Round(5)).unwrap(),
            SecondaryGroup::new([self_id, nid(3), nid(4), nid(5)].into_iter().collect()).unwrap(),
        );
        assert!(grp.is_member(&self_id));

        clt.confirmed_groups.insert(Round(1)..Round(5), grp);

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
        clt.pending_confirms.insert(
            Round(5),
            BTreeMap::from([(validator_id, prepare_invite.clone())]),
        );

        // Test 1: Valid NoConfirm message - should be accepted and remove the invite
        let valid_no_confirm = NoConfirm {
            prepare: prepare_invite,
            reason: NoConfirmReason::GroupFull,
        };

        clt.handle_no_confirm_message(valid_no_confirm);

        // Verify the invite was removed from pending_confirms
        assert!(
            !clt.pending_confirms
                .get(&Round(5))
                .unwrap()
                .contains_key(&validator_id),
            "Valid NoConfirm should remove the invite from pending_confirms"
        );
    }

    #[test]
    fn enter_round_returns_reports_on_expiry() {
        let self_id = nid(1);
        let (mut clt, _rx) = make_client(self_id);

        let validator_id = nid(2);
        let grp = SecondaryGroupAssignment::new(
            validator_id,
            RoundSpan::new(Round(1), Round(5)).unwrap(),
            SecondaryGroup::new([self_id, nid(3), nid(4)].into_iter().collect()).unwrap(),
        );
        clt.confirmed_groups.insert(Round(1)..Round(5), grp);

        // Group still active at round 3
        let reports = clt.enter_round(Round(3));
        assert!(reports.is_empty());

        // Group expires at round 5
        let reports = clt.enter_round(Round(5));
        assert_eq!(reports.len(), 1);

        let (report, dest) = &reports[0];
        assert_eq!(*dest, validator_id);
        assert_eq!(report.reporter, self_id);
        assert_eq!(report.validator_id, validator_id);
        assert_eq!(
            report.round_span,
            RoundSpan::new(Round(1), Round(5)).unwrap()
        );
        // All peers except self (nid(3), nid(4))
        assert_eq!(report.peer_scores.len(), 2);
        for score in report.peer_scores.iter() {
            assert_ne!(
                score.peer, self_id,
                "self should be excluded from peer_scores"
            );
        }
    }

    #[test]
    fn multiple_groups_produce_multiple_reports() {
        let self_id = nid(1);
        let (mut clt, _rx) = make_client(self_id);

        // Two groups from different validators, both expiring at round 5
        let grp1 = SecondaryGroupAssignment::new(
            nid(10),
            RoundSpan::new(Round(1), Round(5)).unwrap(),
            SecondaryGroup::new([self_id, nid(3)].into_iter().collect()).unwrap(),
        );
        let grp2 = SecondaryGroupAssignment::new(
            nid(20),
            RoundSpan::new(Round(2), Round(5)).unwrap(),
            SecondaryGroup::new([self_id, nid(4)].into_iter().collect()).unwrap(),
        );
        clt.confirmed_groups.force_insert(Round(1)..Round(5), grp1);
        clt.confirmed_groups.force_insert(Round(2)..Round(5), grp2);

        let reports = clt.enter_round(Round(5));
        assert_eq!(reports.len(), 2);

        let validators: std::collections::BTreeSet<_> =
            reports.iter().map(|(_, dest)| *dest).collect();
        assert!(validators.contains(&nid(10)));
        assert!(validators.contains(&nid(20)));
    }

    // Creates a node id that we can refer to just from its seed
    fn nid(seed: u64) -> NodeId<PubKeyType> {
        let key_pair = get_key::<ST>(seed);
        let pub_key = key_pair.pubkey();
        NodeId::new(pub_key)
    }
}
