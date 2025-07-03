use std::{collections::BTreeMap, sync::mpsc::Sender, time::Instant};

use iset::IntervalMap;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{NodeId, Round, RoundSpan, GENESIS_ROUND};

use super::{
    super::{config::RaptorCastConfigSecondaryClient, util::Group},
    group_message::{FullNodesGroupMessage, PrepareGroup, PrepareGroupResponse},
};

type Bandwidth = u64;
type GroupAsClient<ST> = Group<ST>;

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

    // [start_round, end_round) -> GroupAsClient
    // Represents all raptorcast groups that we have accepted. They may overlap.
    confirmed_groups: IntervalMap<Round, GroupAsClient<ST>>,

    // start_round -> validator_id -> group invite
    // Once we receive an invite, we remember how the invite looked like, so
    // that we don't blindly accept any confirmation message.
    pending_confirms:
        BTreeMap<Round, BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, PrepareGroup<ST>>>,

    // Once a group is confirmed, it is sent to this channel
    group_sink_channel: Sender<GroupAsClient<ST>>,

    // For avoiding accepting invites/confirms for rounds we've already started
    curr_round: Round,

    // Helps bootstrap the full-node when it is not yet receiving proposals and
    // cannot call enter_round() to advance state as it doesn't know what round
    // we are at.
    last_round_heartbeat: Instant,
}

impl<ST> Client<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new(
        client_node_id: NodeId<CertificateSignaturePubKey<ST>>,
        group_sink_channel: Sender<GroupAsClient<ST>>,
        config: RaptorCastConfigSecondaryClient,
    ) -> Self {
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
        }
    }

    // Called from UpdateCurrentRound
    pub fn enter_round(&mut self, curr_round: Round) {
        // Sanity check on curr_round
        if curr_round < self.curr_round {
            tracing::error!(
                "RaptorCastSecondary ignoring backwards round \
                {:?} -> {:?}",
                self.curr_round,
                curr_round
            );
            return;
        } else if curr_round > self.curr_round + Round(1) {
            tracing::debug!(
                "RaptorCastSecondary detected round gap \
                {:?} -> {:?}",
                self.curr_round,
                curr_round
            );
        }

        self.curr_round = curr_round;
        self.last_round_heartbeat = Instant::now();

        // Clean up old invitations.
        self.pending_confirms.retain(|&key, _| key > curr_round);

        // Send out group information to the Primary instance, so that it can
        // re-broadcast raptorcast chunks. This is the normal path when we are
        // receiving proposals and thus the round increases.
        let consume_end = curr_round + Round(1);
        for group in self.confirmed_groups.values(curr_round..consume_end) {
            if let Err(error) = self.group_sink_channel.send(group.clone()) {
                tracing::error!(
                    "Failed to send group to secondary Raptorcast instance: {}",
                    error
                );
            }
        }

        // Remove all groups that should already have been consumed
        let mut keys_to_remove = Vec::new();
        for iv in self.confirmed_groups.intervals(..consume_end) {
            keys_to_remove.push(iv);
        }
        for iv in keys_to_remove {
            self.confirmed_groups.remove(iv);
        }
    }

    // If we are not receiving proposals, then we don't know what the current
    // round is, and should advance the state machine despite self.curr_round
    fn is_receiving_proposals(&self) -> bool {
        Instant::now() < self.last_round_heartbeat + self.config.invite_accept_heartbeat
    }

    // Called when group invite or group confirmation is received from validator
    pub fn on_receive_group_message(
        &mut self,
        msg: FullNodesGroupMessage<ST>,
    ) -> Option<(
        FullNodesGroupMessage<ST>,
        NodeId<CertificateSignaturePubKey<ST>>,
    )> {
        match msg {
            //-----------------------------
            // INVITE from validator
            //-----------------------------
            FullNodesGroupMessage::PrepareGroup(invite_msg) => {
                // Check the invite for duplicates & bandwidth requirements
                let mut accept = true;

                // Sanity check the message
                if invite_msg.start_round >= invite_msg.end_round {
                    tracing::warn!(
                        "RaptorCastSecondary rejecting invite message due to \
                        failed sanity check: {:?}",
                        invite_msg
                    );
                    accept = false;
                }

                // Reject late round
                if invite_msg.start_round <= self.curr_round {
                    tracing::warn!(
                        "RaptorCastSecondary rejecting invite for round that \
                        already started, curr_round {:?}, invite = {:?}",
                        self.curr_round,
                        invite_msg
                    );
                    accept = false;
                }

                // Reject invite outside config invitation bounds, unless we are
                // not receiving any proposals ATM and should accept first group
                // invite, even if far from what we believe is the current round.
                if (invite_msg.start_round < self.curr_round + self.config.invite_future_dist_min
                    || invite_msg.start_round
                        > self.curr_round + self.config.invite_future_dist_max)
                    && self.is_receiving_proposals()
                {
                    tracing::warn!(
                        "RaptorCastSecondary rejecting invite outside bounds \
                        [{:?}, {:?}], curr_round {:?}, invite = {:?}",
                        self.config.invite_future_dist_min,
                        self.config.invite_future_dist_max,
                        self.curr_round,
                        invite_msg
                    );
                    accept = false;
                }

                let mut future_bandwidth: Bandwidth =
                    self.bandwidth_cost(invite_msg.max_group_size);

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
                    if group.get_validator_id() == &invite_msg.validator_id {
                        tracing::warn!(
                            "RaptorCastSecondary received self-overlapping \
                            invite for rounds [{:?}, {:?}) from validator {:?}",
                            invite_msg.start_round,
                            invite_msg.end_round,
                            invite_msg.validator_id
                        );
                        accept = false;
                        break;
                    }
                    // Check that we'll have enough bandwidth during round span
                    future_bandwidth += self.bandwidth_cost(group.size_excl_self());
                }

                // Check groups we were invited to but are still unconfirmed
                for (&key, other_invites) in self.pending_confirms.iter() {
                    if key >= invite_msg.end_round {
                        // Remaining keys are outside the invite range
                        break;
                    }
                    for other in other_invites.values() {
                        if Self::overlaps(other.start_round, other.end_round, &invite_msg) {
                            future_bandwidth += self.bandwidth_cost(other.max_group_size)
                        }
                    }
                }
                // Final bandwidth check
                if future_bandwidth > self.config.bandwidth_capacity {
                    tracing::debug!(
                        "RaptorCastSecondary rejected invite for rounds \
                        [{:?}, {:?}) from validator {:?} due to low bandwidth",
                        invite_msg.start_round,
                        invite_msg.end_round,
                        invite_msg.validator_id
                    );
                    accept = false;
                }

                if accept {
                    // Let's remember about this invite so that we don't blindly
                    // accept any confirmation message.
                    self.pending_confirms
                        .entry(invite_msg.start_round)
                        .or_default()
                        .insert(invite_msg.validator_id, invite_msg.clone());
                }
                let dest_node_id = invite_msg.validator_id;
                let response = PrepareGroupResponse {
                    req: invite_msg,
                    node_id: self.client_node_id,
                    accept,
                };
                Some((
                    FullNodesGroupMessage::PrepareGroupResponse(response),
                    dest_node_id,
                ))
            }

            //-----------------------------
            // ConfirmGroup
            //-----------------------------
            FullNodesGroupMessage::ConfirmGroup(confirm_msg) => {
                let start_round = &confirm_msg.prepare.start_round;

                // Drop the group if we've already entered the round
                if start_round <= &self.curr_round {
                    tracing::warn!(
                        "RaptorCastSecondary ignoring late confirm, curr_round \
                        {:?}, confirm = {:?}",
                        self.curr_round,
                        confirm_msg
                    );
                    return None;
                }

                let is_receiving_proposals = self.is_receiving_proposals();

                if let Some(invites) = self.pending_confirms.get_mut(start_round) {
                    let round_span = RoundSpan::new(
                        confirm_msg.prepare.start_round,
                        confirm_msg.prepare.end_round,
                    );
                    let maybe_entry = invites.get(&confirm_msg.prepare.validator_id);
                    if let Some(old_invite) = maybe_entry {
                        if old_invite != &confirm_msg.prepare {
                            tracing::warn!(
                                "RaptorCastSecondary ignoring ConfirmGroup that \
                                doesn't match the original invite. Expected: {:?}, got: {:?}",
                                old_invite,
                                confirm_msg.prepare
                            );
                            return None;
                        }
                        if confirm_msg.peers.len() > confirm_msg.prepare.max_group_size {
                            tracing::warn!(
                                "RaptorCastSecondary ignoring ConfirmGroup that \
                                is larger ({}) than the promised max_group_size ({}). \
                                Message details: {:?}",
                                confirm_msg.peers.len(),
                                confirm_msg.prepare.max_group_size,
                                confirm_msg
                            );
                            return None;
                        }
                        if confirm_msg.peers.contains(&self.client_node_id) {
                            let group = GroupAsClient::new_fullnode_group(
                                confirm_msg.peers,
                                &self.client_node_id,
                                confirm_msg.prepare.validator_id,
                                round_span,
                            );

                            // Send the group to primary instance right away.
                            // Doing so helps resolve activation of groups for
                            // bootstrapping full-nodes, as they aren't receiving
                            // proposals and hence can't advance (enter) rounds.
                            // The primary instance use the group for determining
                            // which other full-nodes to re-broadcast raptorcast
                            // chunks to.
                            if !is_receiving_proposals {
                                if let Err(error) = self.group_sink_channel.send(group.clone()) {
                                    tracing::error!(
                                        "RaptorCastSecondary failed to send group to primary \
                                        Raptorcast instance: {}",
                                        error
                                    );
                                }
                            }

                            self.confirmed_groups
                                .force_insert(round_span.start..round_span.end, group);
                            invites.remove(&confirm_msg.prepare.validator_id);
                        } else {
                            tracing::warn!(
                                "RaptorCastSecondary ignoring ConfirmGroup \
                                with a group that does not contain our node_id: {:?}",
                                confirm_msg
                            );
                        }
                    } else {
                        tracing::warn!(
                            "RaptorCastSecondary ignoring ConfirmGroup from \
                            unrecognized validator id: {:?}",
                            confirm_msg
                        );
                    }
                } else {
                    tracing::warn!(
                        "RaptorCastSecondary Ignoring confirmation message \
                        for unrecognized start round: {:?}",
                        confirm_msg
                    );
                }
                None
            }

            FullNodesGroupMessage::PrepareGroupResponse(_) => {
                tracing::error!(
                    "RaptorCastSecondary client received a \
                                PrepareGroupResponse message"
                );
                None
            }
        }
    }

    fn bandwidth_cost(&self, group_size: usize) -> Bandwidth {
        group_size as u64 * self.config.bandwidth_cost_per_group_member
    }

    fn overlaps(bgn: Round, end: Round, group: &PrepareGroup<ST>) -> bool {
        assert!(bgn <= end);
        assert!(group.start_round <= group.end_round);
        group.start_round < end && group.end_round > bgn
    }

    #[cfg(test)]
    pub fn get_curr_round(&self) -> Round {
        self.curr_round
    }

    #[cfg(test)]
    pub fn get_client_node_id(&self) -> NodeId<CertificateSignaturePubKey<ST>> {
        self.client_node_id
    }

    #[cfg(test)]
    pub fn num_pending_confirms(&self) -> usize {
        self.pending_confirms.len()
    }
}
