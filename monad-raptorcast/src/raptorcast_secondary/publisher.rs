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

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::ExecutorMetrics;
use monad_types::{NodeId, Round, RoundSpan};
use rand::seq::SliceRandom;
use rand_chacha::ChaCha8Rng;
use tracing::{debug, error, info, trace, warn};

use super::{
    super::{
        config::{GroupSchedulingConfig, RaptorCastConfigSecondaryPublisher},
        util::{FullNodes, Group},
    },
    group_message::{ConfirmGroup, FullNodesGroupMessage, PrepareGroup},
};

/// Metrics constant
pub const PUBLISHER_CURRENT_GROUP_SIZE: &str =
    "monad.bft.raptorcast.secondary.publisher.current_group_size";
pub const PUBLISHER_SENT_INVITES: &str = "monad.bft.raptorcast.secondary.publisher.sent_invites";

type FullNodesST<ST> = FullNodes<CertificateSignaturePubKey<ST>>;
type TimePoint = Round;

// This is for when the router is playing the role of a Publisher
// That is, we are a validator sending group invites to random full-nodes for
// raptor-casting messages to them

// Main state machine for when the secondary RaptorCast router is acting as a publisher
pub struct Publisher<ST>
where
    ST: CertificateSignatureRecoverable,
{
    // Constant data
    validator_node_id: NodeId<CertificateSignaturePubKey<ST>>,
    scheduling_cfg: GroupSchedulingConfig,

    // Dynamic data
    group_schedule: BTreeMap<Round, GroupAsPublisher<ST>>, // start_round -> GroupAsPublisher
    always_ask_full_nodes: FullNodesST<ST>,                // priority ones, coming from config
    peer_disc_full_nodes: FullNodesST<ST>,                 // public ones, via peer discovery
    rng: ChaCha8Rng,   // random number generator for shuffling full-nodes
    curr_round: Round, // just for debug checks

    // This is the group we are curr. broadcasting to, popped off group_schedule
    // We can't keep it inside the map because we want to return a reference to
    // the full-nodes in it (FullNodesView).
    // Actually we only need full_nodes_accepted & end_round from curr_group.
    curr_group: Group<ST>,

    // Metrics
    metrics: ExecutorMetrics,
}

impl<ST> Publisher<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new(
        validator_node_id: NodeId<CertificateSignaturePubKey<ST>>,
        config: RaptorCastConfigSecondaryPublisher<ST>,
        rng: ChaCha8Rng,
    ) -> Self {
        let scheduling_cfg = config.group_scheduling;
        let min_allowed_init_span = // Allow for at least 1 invite timer tick
            scheduling_cfg.max_invite_wait +
            scheduling_cfg.deadline_round_dist;
        if scheduling_cfg.init_empty_round_span < min_allowed_init_span {
            panic!("init_empty_round_span infeasibly short");
        }
        if scheduling_cfg.invite_lookahead < min_allowed_init_span {
            panic!("invite_lookahead infeasibly short");
        }

        // Remove duplicate entries from always_ask_full_nodes, but making sure
        // we maintain the original order/
        info!(
            num_prio_full_nodes =? config.full_nodes_prioritized.len(),
            "RaptorCastSecondary initializing Publisher",
        );
        let mut always_ask_full_nodes: FullNodesST<ST> = FullNodes::default();

        {
            let mut seen = HashSet::new();
            for node in config.full_nodes_prioritized {
                if seen.insert(node) {
                    trace!(?node, "insert prioritized full node");
                    always_ask_full_nodes.list.push(node);
                } else {
                    info!(?node, "duplicate prioritized full node, ignoring");
                }
            }
        }

        Self {
            validator_node_id,
            scheduling_cfg,
            group_schedule: BTreeMap::new(),
            always_ask_full_nodes,
            peer_disc_full_nodes: FullNodes::new(Vec::new()),
            rng,
            curr_round: Round::MIN,
            curr_group: Group::default(),
            metrics: ExecutorMetrics::default(),
        }
    }

    fn new_empty_group(&self, start_round: Round) -> Group<ST> {
        let end_round = start_round + self.scheduling_cfg.init_empty_round_span;
        let round_span = RoundSpan::new(start_round, end_round).expect("round is near Round::MAX");

        Group::new_fullnode_group(
            Vec::new(),
            &self.validator_node_id,
            self.validator_node_id,
            round_span,
        )
    }

    // While we don't have a real timer, we can call this instead of
    // enter_round() + step_until()
    pub fn enter_round_and_step_until(
        &mut self,
        round: Round,
    ) -> Option<(FullNodesGroupMessage<ST>, FullNodesST<ST>)> {
        trace!(?round, "enter_round_and_step_until");
        // Just some sanity check
        {
            if round < self.curr_round {
                error!(
                    "RaptorCastSecondary ignoring backwards round \
                    {:?} -> {:?}",
                    self.curr_round, round
                );
                return None;
            }
            if round > self.curr_round + Round(1) {
                debug!(
                    "RaptorCastSecondary detected round gap \
                    {:?} -> {:?}",
                    self.curr_round, round
                );
            }
            self.curr_round = round;
        }
        self.enter_round(round);
        self.step_until(round)
    }

    // Populate self.curr_group and clean up expired groups.
    // When we have a real timer, this can be called from UpdateCurrentRound
    fn enter_round(&mut self, new_round: Round) {
        trace!(?new_round, "enter_round");
        assert_ne!(new_round, Round::MAX);

        if new_round < self.curr_group.get_round_span().end {
            // We don't need to advance to the next group yet
            assert!(new_round >= self.curr_group.get_round_span().start);
            return;
        }

        // Remove all groups that have ended.
        self.group_schedule
            .retain(|_, group| group.end_round >= new_round);

        let Some(next_group) = self.group_schedule.first_entry() else {
            // We didn't manage to form a group in time for the new round.
            // Might be due to gap, unexpectedly long delays or Round 0/
            // We currently have an empty group for some rounds while we
            // allow invites for a future group to complete.
            tracing::debug!(
                ?new_round,
                "No group scheduled for RaptorCastSecondary \
                    round nor any other future round yet.",
            );
            // Not serving any full nodes in current round
            self.metrics[PUBLISHER_CURRENT_GROUP_SIZE] = 0;
            self.curr_group = self.new_empty_group(new_round);
            return;
        };

        // After a round gap, its possible that arg `next_group` lands in the
        // middle of `next_group`. In this case, the dynamic full-nodes will
        // also experience round gaps unless they are being broadcast the
        // missing rounds from other validators.
        let group_start_round = *next_group.key();
        if group_start_round < new_round {
            tracing::debug!(
                ?new_round,
                ?group_start_round,
                "RaptorCastSecondary jumping into middle of future scheduled group after a gap. \
                Downstream full-nodes might see round gaps as well."
            );
        }

        if group_start_round > new_round {
            // The next group is not yet scheduled to start. This can happen
            // when there are gaps in the round sequence.
            tracing::debug!(
                ?new_round,
                ?next_group,
                "No group scheduled for RaptorCastSecondary \
                    round, next group is",
            );
            // Not serving any full nodes in current round
            self.metrics[PUBLISHER_CURRENT_GROUP_SIZE] = 0;
            self.curr_group = self.new_empty_group(new_round);
            return;
        }

        // If `round` belongs in the next group, pop & make it the current one.
        self.curr_group = next_group
            .remove()
            .to_finalized_group(self.validator_node_id);
        self.metrics[PUBLISHER_CURRENT_GROUP_SIZE] = self.curr_group.size_excl_self() as u64;
    }

    // Advances the state machine to the given "time point".
    // Time point is represented as a Round for now, but can be Duration later.
    // For now called from UpdateCurrentRound, but later from timer mechanism
    fn step_until(
        &mut self,
        time_point: TimePoint,
    ) -> Option<(FullNodesGroupMessage<ST>, FullNodesST<ST>)> {
        let schedule_end: Round = match self.group_schedule.last_entry() {
            Some(grp) => grp.get().end_round,
            None => self.curr_group.get_round_span().end,
        };

        trace!(?time_point, ?schedule_end, "RaptorCastSecondary step_until");

        // Check if scheduled groups need servicing: time-outs, invites, confirm
        for (_, group) in self.group_schedule.iter_mut() {
            if let Some(out_msg) =
                group.advance_invites(time_point, &self.scheduling_cfg, self.validator_node_id)
            {
                trace!(
                    ?out_msg,
                    "RaptorCastSecondary step_until advanced invites and will send",
                );

                // record number of invites
                if let FullNodesGroupMessage::PrepareGroup(_) = &out_msg.0 {
                    self.metrics[PUBLISHER_SENT_INVITES] += out_msg.1.list.len() as u64;
                }

                return Some(out_msg);
            }
        }

        // Check if it's time to schedule a new future group
        if schedule_end < time_point + self.scheduling_cfg.invite_lookahead {
            let mut new_group = GroupAsPublisher::new_randomized(
                schedule_end,
                self.scheduling_cfg.round_span,
                &mut self.rng,
                &self.always_ask_full_nodes,
                &self.peer_disc_full_nodes,
            );
            let maybe_invites =
                new_group.advance_invites(time_point, &self.scheduling_cfg, self.validator_node_id);
            trace!(
                ?maybe_invites,
                "RaptorCastSecondary step_until new_randomized group, will send:",
            );
            self.group_schedule.insert(new_group.start_round, new_group);

            // record number of invites for new group
            self.metrics[PUBLISHER_SENT_INVITES] += maybe_invites
                .as_ref()
                .map_or(0, |(_, full_nodes)| full_nodes.list.len() as u64);

            return maybe_invites;
        }

        // No invites to send out at the moment.
        None
    }

    // Process response from a full-node who we previously invited to join one
    // of our RaptorCast group scheduled for a future round.
    // We've received a response from the candidate but won't send any message
    // back right away, but when we have enough responses to form a group.
    pub fn on_candidate_response(&mut self, msg: FullNodesGroupMessage<ST>) {
        trace!(?msg, "RaptorCastSecondary received candidate response");
        match msg {
            FullNodesGroupMessage::PrepareGroupResponse(response) => {
                let candidate = response.node_id;
                let start_round = response.req.start_round;
                if let Some(group) = self.group_schedule.get_mut(&start_round) {
                    group.on_candidate_response(candidate, response.accept);
                } else {
                    warn!(
                        ?candidate,
                        ?start_round,
                        "Ignoring response from FullNode, \
                        as there is no group scheduled to start in round",
                    );
                }
            }
            _ => {
                debug!(
                    ?msg,
                    "RaptorCastSecondary publisher received \
                    unexpected message",
                );
            }
        }
    }

    pub fn get_current_raptorcast_group(&self) -> Option<&Group<ST>> {
        if self.curr_group.get_round_span().contains(self.curr_round) {
            Some(&self.curr_group)
        } else {
            None
        }
    }

    pub fn update_always_ask_full_nodes(
        &mut self,
        prioritized_full_nodes: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    ) {
        self.always_ask_full_nodes.list = prioritized_full_nodes;
        // Remove the nodes from always_ask, otherwise we might send two
        // invites to the same node.
        self.peer_disc_full_nodes
            .list
            .retain(|node| !self.always_ask_full_nodes.list.contains(node));
    }

    pub fn upsert_peer_disc_full_nodes(
        &mut self,
        additional_fn: FullNodes<CertificateSignaturePubKey<ST>>,
    ) {
        let mut full_nodes = Vec::new();
        for node in additional_fn.list {
            if self.always_ask_full_nodes.list.contains(&node) || // already in priority list
               node == self.validator_node_id
            // we can't be a candidate
            {
                continue;
            }
            full_nodes.push(node);
        }
        self.peer_disc_full_nodes.list = full_nodes;
    }

    pub fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }
}

// A RaptorCast group of full-nodes we currently are broadcasting to (or later)
struct GroupAsPublisher<ST>
where
    ST: CertificateSignatureRecoverable,
{
    full_nodes_accepted: FullNodesST<ST>,
    full_nodes_rejected: FullNodesST<ST>,

    // Pre-randomized permutation of always_ask_full_nodes[] + peer_disc_full_nodes[]
    // Stays const once created.
    full_nodes_candidates: FullNodesST<ST>,
    num_invites_sent: usize, // also an index into full_nodes_candidates[]

    start_round: Round, // inclusive
    end_round: Round,   // exclusive

    // Time point when we should take some action, like send more invites or a
    // group confirmation message.
    // Will be a timestamp once switched to real timers
    // This is set to TimePoint::MAX to indicate the group is now locked in.
    next_invite_tp: TimePoint,
}

impl<ST> fmt::Debug for GroupAsPublisher<ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Group")
            .field("start", &self.start_round.0)
            .field("end", &self.end_round.0)
            .field("candidates", &self.full_nodes_candidates.list.len())
            .field("invited", &self.num_invites_sent)
            .field("accepted", &self.full_nodes_accepted.list.len())
            .field("rejected", &self.full_nodes_rejected.list.len())
            .finish()
    }
}

impl<ST> GroupAsPublisher<ST>
where
    ST: CertificateSignatureRecoverable,
{
    // The typical case constructor.
    pub fn new_randomized(
        start_round: Round,
        round_span: Round,
        rng: &mut ChaCha8Rng,
        always_ask_full_nodes: &FullNodesST<ST>, // priority nodes, asked first
        peer_disc_full_nodes: &FullNodesST<ST>,  // randomized public nodes
    ) -> Self {
        let mut new_group = Self {
            full_nodes_accepted: FullNodes::default(),
            full_nodes_rejected: FullNodes::default(),
            full_nodes_candidates: always_ask_full_nodes.clone(),
            num_invites_sent: 0,
            start_round,
            end_round: start_round + round_span,
            next_invite_tp: TimePoint::MIN,
        };

        // Add randomized public nodes.
        // We don't know how many will refuse, timeout, or ignore the invite,
        // so we just include all and let a timer periodically pick more nodes
        // until we either hit the target or get too close to the start round.
        let mut rand_public_nodes = peer_disc_full_nodes.clone();
        rand_public_nodes.list.shuffle(rng);
        new_group
            .full_nodes_candidates
            .list
            .extend(rand_public_nodes.list);

        new_group
    }

    pub fn is_locked(&self) -> bool {
        self.next_invite_tp == TimePoint::MAX
    }

    // Returns a set of full-nodes where the group invites should be sent to.
    pub fn advance_invites(
        &mut self,
        curr_timestamp: TimePoint,
        cfg: &GroupSchedulingConfig,
        validator_id: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Option<(FullNodesGroupMessage<ST>, FullNodesST<ST>)> {
        if curr_timestamp < self.next_invite_tp {
            trace!("RaptorCastSecondary Publisher advance_invites: None");
            return None;
        }

        // PrepareGroup data is needed in either case: send invites or confirm
        let prep_grp_data = PrepareGroup {
            validator_id,
            max_group_size: cfg.max_group_size,
            start_round: self.start_round,
            end_round: self.end_round,
        };

        // Decide if we should:
        // 1) send GroupConfirm and lock the group, or
        // 2) send more invites
        if self.full_nodes_accepted.list.len() >= cfg.max_group_size || // reached target
           self.num_invites_sent >= self.full_nodes_candidates.list.len() || // no more candidates
           curr_timestamp + cfg.deadline_round_dist >= self.start_round
        // group is starting soon
        {
            debug!(
                ?self.full_nodes_accepted.list,
                "RaptorCastSecondary Publisher confirm group formed",
            );
            self.next_invite_tp = TimePoint::MAX; // lock the group
            let confirm_data = ConfirmGroup {
                prepare: prep_grp_data,
                peers: self.full_nodes_accepted.list.clone(),
                name_records: Default::default(), // to be filled by next layer
            };
            // ConfirmGroup is sent to all accepted peers
            let grp_msg = FullNodesGroupMessage::ConfirmGroup(confirm_data);
            return Some((grp_msg, self.full_nodes_accepted.clone()));
        }

        // Send more invites
        // This PrepareGroup message is sent to just the missing invitees.
        self.next_invite_tp = curr_timestamp + cfg.max_invite_wait;
        let num_missing = cfg.max_group_size - self.full_nodes_accepted.list.len();
        let next_invitees = FullNodes::new(
            self.full_nodes_candidates
                .list
                .iter()
                .skip(self.num_invites_sent)
                .take(num_missing)
                .cloned()
                .collect::<Vec<_>>(),
        );
        let invite_msg = FullNodesGroupMessage::PrepareGroup(prep_grp_data);
        self.num_invites_sent += next_invitees.list.len();
        trace!(?next_invitees.list,
            "RaptorCastSecondary Publisher advance_invites: send Invites to peers",

        );
        Some((invite_msg, next_invitees))
    }

    pub fn on_candidate_response(
        &mut self,
        candidate: NodeId<CertificateSignaturePubKey<ST>>,
        accepted: bool,
    ) {
        trace!(
            ?candidate,
            ?accepted,
            "RaptorCastSecondary Publisher on_candidate_response",
        );
        if self.is_locked() {
            // Already formed the group
            trace!(
                ?candidate,
                ?accepted,
                "RaptorCastSecondary Publisher on_candidate_response - Already formed the group"
            );
            return;
        }
        if !self.full_nodes_candidates.list.contains(&candidate) {
            warn!(
                ?candidate,
                ?self,
                "Ignoring response from FullNode who was \
                never invited to RaptorCastSecondary group",
            );
            return;
        }
        if self.full_nodes_accepted.list.contains(&candidate) {
            warn!(
                ?candidate,
                ?self,
                "Ignoring duplicate response from FullNode \
                who was already accepted into RaptorCastSecondary group",
            );
            return;
        }
        if self.full_nodes_rejected.list.contains(&candidate) {
            warn!(
                ?candidate,
                ?self,
                "Ignoring duplicate response from FullNode who \
                has already rejected an invite from RaptorCastSecondary group",
            );
            return;
        }
        if accepted {
            self.full_nodes_accepted.list.push(candidate);
            debug!(
                ?candidate,
                ?self,
                "RaptorCastSecondary group invite accepted by, for group",
            );
        } else {
            self.full_nodes_rejected.list.push(candidate);
            debug!(
                ?candidate,
                ?self,
                "RaptorCastSecondary group invite rejected by, for group",
            );
        }
    }

    pub fn to_finalized_group(
        &self,
        validator_id: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Group<ST> {
        let round_span =
            RoundSpan::new(self.start_round, self.end_round).expect("invalid round span");

        Group::new_fullnode_group(
            self.full_nodes_accepted.list.clone(),
            &validator_id,
            validator_id,
            round_span,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::min, io, sync::Once, time::Duration};

    use iset::{interval_map, IntervalMap};
    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;
    use monad_types::{Epoch, Round};
    use rand::SeedableRng;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
    use tracing_subscriber::fmt::format::FmtSpan;

    use super::{
        super::{
            super::{
                config::RaptorCastConfigSecondaryClient,
                util::{Group, ReBroadcastGroupMap},
            },
            group_message::PrepareGroupResponse,
            Client,
        },
        *,
    };

    type ST = SecpSignature;
    type PubKeyType = CertificateSignaturePubKey<ST>;
    type RcToRcChannelGrp<ST> = (UnboundedSender<Group<ST>>, UnboundedReceiver<Group<ST>>);
    type NodeIdST<ST> = NodeId<CertificateSignaturePubKey<ST>>;

    // Creates a node id that we can refer to just from its seed
    fn nid(seed: u64) -> NodeId<PubKeyType> {
        let key_pair = get_key::<ST>(seed);
        let pub_key = key_pair.pubkey();
        NodeId::new(pub_key)
    }

    // During test failures. allows one to more easily determine the identity
    // and purpose of a given NodeId.
    fn nid_str(node_id: &NodeId<PubKeyType>) -> String {
        for seed in 0..100 {
            if node_id == &nid(seed) {
                return format!("nid_{:02}", seed);
            }
        }
        format!("{:?}", node_id)
    }

    // Convert a vec of real NodeIDs to a string like "nid_10, nid_15, nid_18"
    // etc, for easier diagnosis in tests
    fn nid_list_str(list: &Vec<NodeId<PubKeyType>>) -> String {
        let mut res = String::new();
        for node_id in list {
            res.push_str(&nid_str(node_id));
            res.push_str(", ");
        }
        res
    }

    // Compare two sets of node ids and point out the first mismatch
    fn equal_node_vec(lhs: &Vec<NodeId<PubKeyType>>, rhs: &Vec<NodeId<PubKeyType>>) -> bool {
        for ii in 0..min(lhs.len(), rhs.len()) {
            if lhs[ii] != rhs[ii] {
                println!(
                    "Mismatch: lhs[{}] = {}, rhs[{}] = {}:",
                    ii,
                    nid_str(&lhs[ii]),
                    ii,
                    nid_str(&rhs[ii])
                );
                println!("    lhs: {}", nid_list_str(lhs));
                println!("    rhs: {}", nid_list_str(rhs));
                return false;
            }
        }
        if lhs.len() != rhs.len() {
            println!("Mismatched len, lhs: {}, rhs: {}", lhs.len(), rhs.len());
            println!("  lhs: {}", nid_list_str(lhs));
            println!("  rhs: {}", nid_list_str(rhs));
            return false;
        }
        true
    }

    fn equal_node_set(lhs_slice: &[NodeId<PubKeyType>], rhs_slice: &[NodeId<PubKeyType>]) -> bool {
        let mut lhs = lhs_slice.to_vec();
        lhs.sort();
        let mut rhs = rhs_slice.to_vec();
        rhs.sort();
        equal_node_vec(&lhs, &rhs)
    }

    // Creates a vec of node ids from a list of seeds
    macro_rules! node_ids_vec {
        ($($x:expr),+ $(,)?) => {
            vec![$(nid($x)),+]
        };
    }

    macro_rules! node_ids {
        ($($x:expr),+ $(,)?) => {
            [$(nid($x)),+]
        };
    }

    fn make_invite_response(
        validator_id: NodeId<PubKeyType>,
        full_node_id: NodeId<PubKeyType>,
        accept: bool,
        start_round: Round,
        cfg: &GroupSchedulingConfig,
    ) -> FullNodesGroupMessage<ST> {
        let req = PrepareGroup {
            validator_id,
            max_group_size: cfg.max_group_size,
            start_round,
            end_round: start_round + cfg.round_span,
        };

        let response_data = PrepareGroupResponse {
            req,
            node_id: full_node_id,
            accept,
        };

        FullNodesGroupMessage::PrepareGroupResponse(response_data)
    }

    // Prints the internal state of a group - just the parts relevant to tests
    fn dump_group_state(grp: &GroupAsPublisher<ST>) -> String {
        let mut res = String::new();
        res += format!("[{:?}-{:?})", grp.start_round, grp.end_round).as_str();
        res += format!(
            " candidates[ {}]",
            nid_list_str(&grp.full_nodes_candidates.list)
        )
        .as_str();
        res += format!(
            " accepted[ {}]",
            nid_list_str(&grp.full_nodes_accepted.list)
        )
        .as_str();
        res += format!(" invited={}", grp.num_invites_sent).as_str();
        res
    }

    fn dump_formed_grp(grp: &Group<ST>) -> String {
        let mut res = String::new();
        let span = grp.get_round_span();
        res += format!("[{:?}-{:?})", span.start, span.end).as_str();
        res += format!(" other_peers[ {}]", nid_list_str(grp.get_other_peers())).as_str();
        res
    }

    // Prints the internal state of a publisher; just the parts relevant to test
    fn dump_pub_sched(pbl: &Publisher<ST>) -> String {
        let mut res = String::new();
        res += format!("last_seen_round=[{:?}]", pbl.curr_round).as_str();
        res += format!("\n  curr  {}", dump_formed_grp(&pbl.curr_group)).as_str();
        for grp in pbl.group_schedule.values() {
            res += format!("\n  sched {}", dump_group_state(grp)).as_str();
        }
        res
    }

    // Test assumptions about the interval library.
    // Alternative implementations for interval map:
    //      332k https://crates.io/crates/iset
    //       13k https://crates.io/crates/superintervals
    //       37k https://crates.io/crates/nodit
    //       28k https://crates.io/crates/rb-interval-map
    //       20k https://crates.io/crates/span-map
    //       88k https://github.com/theban/interval-tree?tab=readme-ov-file
    // Alternatives that only implements Set:
    //      https://docs.rs/intervallum/1.4.1/interval/interval/index.html
    //      https://docs.rs/intervallum/1.4.1/interval/interval_set/index.html
    //      https://crates.io/crates/intervals-general
    //      https://github.com/pkhuong/closed-interval-set/blob/main/README.md
    // cargo test -p monad-raptorcast raptorcast_secondary::tests::test_interval_map -- --nocapture
    #[test]
    fn test_interval_map() {
        enable_tracer();
        let mut round_map: IntervalMap<Round, char> = IntervalMap::new();
        round_map.insert(Round(5)..Round(8), 'x');

        //      0   5   10  15  20  25  30
        // a:                   [_______)
        // b:               [_______) 'later replaced with 'z'
        // c:           [_______)
        // d:           [_______) 'replaces 'c'
        // e:       [_______)
        // f:       [_______) 'force_insert, does not replace 'e'
        let mut map: IntervalMap<i32, char> =
            interval_map! { 20..30 => 'a', 15..25 => 'b', 10..20 => 'c' };
        println!("map.00: {:?}", map);
        // map.00: {10..20 => 'c', 15..25 => 'b', 20..30 => 'a'}

        assert_eq!(map.insert(10..20, 'd'), Some('c')); // this replaces 'c'. force_insert() would have kept 'c'
        println!("map.01: {:?}", map);
        // map.01: {10..20 => 'd', 15..25 => 'b', 20..30 => 'a'}

        assert_eq!(map.insert(5..15, 'e'), None);
        assert_eq!(map.len(), 4);
        println!("map.02: {:?}", map);
        // map.02: {5..15 => 'e', 10..20 => 'd', 15..25 => 'b', 20..30 => 'a'}

        map.force_insert(5..15, 'f');
        assert_eq!(map.len(), 5);
        println!("map.03: {:?}", map);
        // map.03: {5..15 => 'e', 5..15 => 'f', 10..20 => 'd', 15..25 => 'b', 20..30 => 'a'}

        //Note: map.insert(29..32, 'a') does not merge with tail entry for 'a'

        assert_eq!(map.iter(..).collect::<Vec<_>>().len(), 5);
        let mut iter_len = 0;
        for (k, v) in map.iter(..) {
            println!("Query A: {:?} -> {:?}", k, v);
            iter_len += 1;
        }
        assert_eq!(iter_len, 5);

        assert_eq!(map.iter(7..15).collect::<Vec<_>>().len(), 3);
        for (k, v) in map.iter(7..15) {
            println!("Query B: {:?} -> {:?}", k, v);
        }

        assert_eq!(map.iter(10..11).collect::<Vec<_>>().len(), 3);
        for (k, v) in map.iter(10..11) {
            println!("Query C: {:?} -> {:?}", k, v);
        }

        // Iterator over all pairs (range, value). Output is sorted.
        let a: Vec<_> = map.iter(..).collect();
        assert_eq!(
            a,
            &[
                (5..15, &'e'),
                (5..15, &'f'),
                (10..20, &'d'),
                (15..25, &'b'),
                (20..30, &'a')
            ]
        );

        // Iterate over intervals that overlap query (..20 here). Output is sorted.
        let b: Vec<_> = map.intervals(..20).collect();
        assert_eq!(b, &[5..15, 5..15, 10..20, 15..25]);

        assert_eq!(map[15..25], 'b');
        // Replace 15..25 => 'b' into 'z'.
        *map.get_mut(15..25).unwrap() = 'z';

        // Iterate over values that overlap query (20.. here). Output is sorted by intervals.
        let c: Vec<_> = map.values(20..).collect();
        assert_eq!(c, &[&'z', &'a']);

        // Remove 10..20 => 'd'.
        assert_eq!(map.remove(10..20), Some('d'));

        println!("map.04: {:?}", map);
        // map.04: {5..15 => 'e', 5..15 => 'f', 15..25 => 'z', 20..30 => 'a'}

        // Remove all intervals that are already completed by curr_round = 17
        let curr_round = 17;
        for (k, v) in map.iter_mut(..curr_round) {
            println!("Query D: {:?} -> {:?}", k, v);
        }
        // Query D: 5..15 -> 'e'
        // Query D: 5..15 -> 'f'
        // Query D: 15..25 -> 'z'
        assert_eq!(
            map.iter(..curr_round).collect::<Vec<_>>(),
            &[(5..15, &'e'), (5..15, &'f'), (15..25, &'z')]
        );

        let mut keys_to_remove = Vec::new();
        for iv in map.intervals(..curr_round) {
            if iv.end <= curr_round {
                keys_to_remove.push(iv);
            }
        }
        for iv in keys_to_remove {
            map.remove(iv);
        }
        for (k, v) in map.iter_mut(..curr_round) {
            println!("Query E: {:?} -> {:?}", k, v);
        }
        // Query E: 15..25 -> 'z'
        assert_eq!(
            map.iter(..curr_round).collect::<Vec<_>>(),
            &[(15..25, &'z')]
        );
    }

    #[test]
    fn group_randomizing() {
        let always_ask_full_nodes = FullNodesST::<ST> {
            list: node_ids_vec![10, 11],
        };
        let peer_disc_full_nodes = FullNodesST::<ST> {
            list: node_ids_vec![12, 13, 14, 15],
        };

        let num_always_ask = always_ask_full_nodes.list.len();
        let num_peer_disc = peer_disc_full_nodes.list.len();
        let num_total = num_always_ask + num_peer_disc;

        let mut rng = ChaCha8Rng::seed_from_u64(42);

        let group_a: GroupAsPublisher<ST> = GroupAsPublisher::new_randomized(
            Round(0),
            Round(1),
            &mut rng,
            &always_ask_full_nodes,
            &peer_disc_full_nodes,
        );

        let group_b: GroupAsPublisher<ST> = GroupAsPublisher::new_randomized(
            Round(0),
            Round(1),
            &mut rng,
            &always_ask_full_nodes,
            &peer_disc_full_nodes,
        );

        let group_c: GroupAsPublisher<ST> = GroupAsPublisher::new_randomized(
            Round(0),
            Round(1),
            &mut rng,
            &always_ask_full_nodes,
            &peer_disc_full_nodes,
        );

        assert_eq!(group_a.full_nodes_candidates.list.len(), num_total);
        assert_eq!(group_b.full_nodes_candidates.list.len(), num_total);
        assert_eq!(group_c.full_nodes_candidates.list.len(), num_total);

        assert_eq!(
            &group_a.full_nodes_candidates.list[..num_always_ask],
            &group_b.full_nodes_candidates.list[..num_always_ask]
        );

        assert_eq!(
            &group_b.full_nodes_candidates.list[..num_always_ask],
            &group_c.full_nodes_candidates.list[..num_always_ask]
        );

        assert_ne!(
            nid_list_str(&group_a.full_nodes_candidates.list),
            nid_list_str(&group_b.full_nodes_candidates.list)
        );

        assert_ne!(
            nid_list_str(&group_b.full_nodes_candidates.list),
            nid_list_str(&group_c.full_nodes_candidates.list)
        );

        assert_ne!(
            nid_list_str(&group_a.full_nodes_candidates.list),
            nid_list_str(&group_c.full_nodes_candidates.list)
        );
    }

    static TRACING_ONCE_SETUP: Once = Once::new();

    fn enable_tracer() {
        TRACING_ONCE_SETUP.call_once(|| {
            let _ = tracing_subscriber::fmt::fmt()
                .with_max_level(tracing::Level::ERROR)
                .with_writer(io::stdout)
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .with_span_events(FmtSpan::CLOSE)
                .try_init();
        });
    }

    fn get_curr_rc_group(publisher: &Publisher<ST>) -> Vec<NodeIdST<ST>> {
        if let Some(group) = publisher.get_current_raptorcast_group() {
            group.get_other_peers().clone()
        } else {
            Vec::new()
        }
    }

    // This is a mock of how the primary raptorcast instance would represent
    // the rebroadcast group map.
    struct MockGroupMap {
        rx_from_client: UnboundedReceiver<Group<ST>>,
        group_map: ReBroadcastGroupMap<ST>,
    }
    impl MockGroupMap {
        fn new(
            clt_node_id: NodeId<CertificateSignaturePubKey<ST>>,
            rx_from_client: UnboundedReceiver<Group<ST>>,
        ) -> Self {
            Self {
                group_map: ReBroadcastGroupMap::new(clt_node_id),
                rx_from_client,
            }
        }

        fn update(&mut self, clt: &Client<ST>) {
            let curr_round = clt.get_curr_round();
            self.group_map.delete_expired_groups(Epoch(0), curr_round);
            while let Ok(group) = self.rx_from_client.try_recv() {
                println!("Received group: {:?}", group);
                println!(
                    "   Other Peers: {:?}",
                    nid_list_str(group.get_other_peers())
                );
                self.group_map.push_group_fullnodes(group);
            }
            self.group_map.delete_expired_groups(Epoch(0), curr_round);
        }

        fn is_empty(&mut self, clt: &Client<ST>) -> bool {
            self.update(clt);
            self.group_map.get_fullnode_map().is_empty()
        }

        fn get_rc_group_peers(
            &mut self,
            clt: &Client<ST>,
            validator_id: &NodeIdST<ST>,
        ) -> Vec<NodeIdST<ST>> {
            self.update(clt);
            let fn_group_map = self.group_map.get_fullnode_map();
            let Some(group) = fn_group_map.get(validator_id) else {
                return Vec::new();
            };
            let mut group_incl_self = group.get_other_peers().clone();
            group_incl_self.push(clt.get_client_node_id());
            group_incl_self
        }
    }

    // cargo test -p monad-raptorcast raptorcast_secondary::tests::standalone_publisher -- --nocapture
    #[test]
    fn standalone_publisher() {
        enable_tracer();
        let sched_cfg = GroupSchedulingConfig {
            max_group_size: 3,
            round_span: Round(5),
            invite_lookahead: Round(8),
            max_invite_wait: Round(2),
            deadline_round_dist: Round(3),
            init_empty_round_span: Round(7), // >= invite_wait + deadline
        };

        //----------------------
        // Topology node ids
        //----------------------
        // nid(  0 ) : Validator V0
        // nid( 10 ) : Full-node nid_10 (always_ask[0] for V0)
        // nid( 11 ) : Full-node nid_11 (always_ask[1] for V0)
        // nid( 12 ) : Full-node nid_12 (peer_disc[0] for V0)
        // nid( 13 ) : Full-node nid_13 (peer_disc[1] for V0)
        // nid( 14 ) : Full-node nid_14 (peer_disc[2] for V0)
        // nid( 15 ) : Full-node nid_15 (peer_disc[3] for V0)
        // nid( 16 ) : Full-node nid_16 (new always_ask[0] for V0)

        //----------------------------------------------------------------------
        // Schedule
        //----------------------------------------------------------------------
        // enter_round()
        // |       get_curr_rc_group()
        // |       |       send_invites_t0
        // |       |       |       send_invites_t1
        // |       |       |       |       send_confirm
        // |       |       |       |       |       look_ahead_bgn
        // |       |       |       |       |       |       look_ahead_last
        // |       |       |       |       |       |       |       generate_grp
        //----------------------------------------------------------------------
        // 1       0       1                       4       8       1
        // 2       0                               5       9       1
        // 3       0               1               6       10      1
        // 4       0                               7       11      1
        // 5       0                       1       8       12      1
        // 6       0       2                       9       13      2
        // 7       0                               10      14      2
        //----------------------------------------------------------------------
        // 8       1               2               11      15      2
        // 9       1                               12      16      2
        // 10      1                       2       13      17      2
        // 11      1       3                       14      18      3
        // 12      1                               15      19      3
        //----------------------------------------------------------------------
        // 13      2               3               16      20      3
        // 14      2                               17      21      3
        // 15      2                       3       18      22      3
        // 16      2       4                       19      23      4
        // 17      2                               20      24      4
        //----------------------------------------------------------------------

        for ii in 0..15 {
            println!("nid {:2}: {:?}", ii, nid(ii));
        }

        let v0_node_id = nid(0);

        let mut v0_fsm: Publisher<ST> = Publisher::new(
            v0_node_id,
            RaptorCastConfigSecondaryPublisher {
                full_nodes_prioritized: vec![nid(10), nid(11)],
                group_scheduling: sched_cfg,
            },
            ChaCha8Rng::seed_from_u64(42),
        );

        // We should be able to call this during the initial state.
        // Note that although we have some initial always_ask_full_nodes, we
        // haven't invited them yet to any group, so current group must be empty
        assert_eq!(get_curr_rc_group(&v0_fsm).len(), 0);

        // Peer discovery gives us some new full-nodes to chose from.
        v0_fsm.upsert_peer_disc_full_nodes(FullNodes::new(vec![
            nid(12),
            nid(13),
            nid(14),
            nid(15),
        ]));

        //-------------------------------------------------------------------[1]
        // 1st group invites t0
        //----------------------------------------------------------------------
        // Move into the first round + tick a timer event into the FSM.
        // It should want to send invites.
        let (group_msg, invitees) = v0_fsm
            .enter_round_and_step_until(Round(1))
            .expect("FSM should have returned invites to be sent");

        // We should still not have a raptor-cast group at this point.
        assert_eq!(get_curr_rc_group(&v0_fsm).len(), 0);

        println!("V0 now: {}", dump_pub_sched(&v0_fsm));

        // Verify that it is an invite message the FSM wants to send
        // Given current seed, the candidates for 1st group are randomized as:
        // sched [8-13) candidates[ nid_10, nid_11, nid_14, nid_13, nid_12, ]
        if let FullNodesGroupMessage::PrepareGroup(invite_msg) = group_msg {
            assert_eq!(invite_msg.start_round, Round(8));
            assert_eq!(invite_msg.end_round, Round(13));
            assert_eq!(invite_msg.max_group_size, 3);
            assert_eq!(invite_msg.validator_id, nid(0));
            // Verify that the FSM invites 2 always_ask + 1 random peer_disc
            // Note that group 1 candidates were randomized as:
            //  |----- first 3 invites--|
            //                          v
            // [ nid_10, nid_11, nid_12, nid_14, nid_13, nid_15 ]
            assert_eq!(invitees.list.len(), 3);
            assert!(equal_node_vec(&invitees.list, &node_ids_vec![10, 11, 12]));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        // Advance to next round, without receiving inviting responses yet
        // Should still not have any raptorcast group.
        assert_eq!(get_curr_rc_group(&v0_fsm).len(), 0);

        // Executing for same round 1 should yield nothing more.
        let res = v0_fsm.enter_round_and_step_until(Round(1));
        assert!(res.is_none());

        //-------------------------------------------------------------------[2]
        // Since max_invite_wait=2 rounds, were' still waiting for the invite
        // response. It hasn't timed out yet so were not sending new invites yet
        //----------------------------------------------------------------------
        let res = v0_fsm.enter_round_and_step_until(Round(2));
        assert!(res.is_none(), "Should still be waiting for invite response");

        //----------------------------------------------------------------------
        // 1st group accept (only nid_11)
        //----------------------------------------------------------------------
        let accept_msg = make_invite_response(nid(0), nid(11), true, Round(8), &sched_cfg);
        v0_fsm.on_candidate_response(accept_msg);

        //-------------------------------------------------------------------[3]
        // 1st group invites t1
        //----------------------------------------------------------------------
        // Now we should invite remaining nodes:
        //  - skip nid_11, because it already accepted
        //  - skip nid_10 and nid_12, as they've just timed out
        //  - take nid_13 and nid_14, as they are the 2 remaining candidates
        let (group_msg, invitees) = v0_fsm
            .enter_round_and_step_until(Round(3))
            .expect("FSM should have returned invites to be sent");

        // We should still not have a raptor-cast group at this point.
        assert_eq!(get_curr_rc_group(&v0_fsm).len(), 0);

        // Verify that it is an invite message the FSM wants to send
        if let FullNodesGroupMessage::PrepareGroup(invite_msg) = group_msg {
            assert_eq!(invite_msg.start_round, Round(8));
            assert_eq!(invite_msg.end_round, Round(13));
            assert_eq!(invite_msg.max_group_size, 3);
            assert_eq!(invite_msg.validator_id, nid(0));
            // Verify that the FSM invites 2 random peer_disc
            // Note that group 1 candidates were randomized as:
            //  +----- first 3 invites--|--new invites-|
            //                          v              v
            // [ nid_10, nid_11, nid_15, nid_13, nid_12, nid_14 ]
            assert_eq!(invitees.list.len(), 2);
            assert!(equal_node_vec(&invitees.list, &node_ids_vec![14, 13]));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        //-------------------------------------------------------------------[4]
        // Still waiting
        //----------------------------------------------------------------------
        let res = v0_fsm.enter_round_and_step_until(Round(4));
        assert!(res.is_none(), "Should still be waiting for invite response");

        //----------------------------------------------------------------------
        // 1st group accept (now nid_13, in addition to old nid_11)
        //----------------------------------------------------------------------
        let accept_msg = make_invite_response(nid(0), nid(13), true, Round(8), &sched_cfg);
        v0_fsm.on_candidate_response(accept_msg);

        // Received response from an odd note which wasn't invited for the round
        // The FSM should ignore this response.
        let accept_msg = make_invite_response(nid(0), nid(22), true, Round(8), &sched_cfg);
        v0_fsm.on_candidate_response(accept_msg);

        //-------------------------------------------------------------------[5]
        // 1st group timeout
        //----------------------------------------------------------------------
        // Now we should hit the timeout for forming the 1st group
        let Some((group_msg, members)) = v0_fsm.enter_round_and_step_until(Round(5)) else {
            panic!(
                "FSM should have returned invites for 2nd group\n\
                publisher v0: {}",
                dump_pub_sched(&v0_fsm)
            );
        };

        // Verify that it is an invite message the FSM wants to send
        if let FullNodesGroupMessage::ConfirmGroup(confirm_msg) = group_msg {
            assert_eq!(confirm_msg.prepare.start_round, Round(8));
            assert_eq!(confirm_msg.prepare.end_round, Round(13));
            assert_eq!(confirm_msg.prepare.max_group_size, 3);
            assert_eq!(confirm_msg.prepare.validator_id, nid(0));
            assert!(equal_node_vec(&confirm_msg.peers, &node_ids_vec![11, 13]));
            assert!(equal_node_vec(&members.list, &node_ids_vec![11, 13]));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        println!("V0 now: {}", dump_pub_sched(&v0_fsm));

        // We have a raptorcast group starting at Round(6) but we are still at 3
        assert_eq!(get_curr_rc_group(&v0_fsm).len(), 0);

        // Should have nothing more to do for round 5
        let res = v0_fsm.enter_round_and_step_until(Round(5));
        assert!(res.is_none());

        //-------------------------------------------------------------------[6]
        // 2nd group invites.t0
        //----------------------------------------------------------------------
        // Now we should start invites for the 2nd group due to invite_lookahead
        let (group_msg, invitees) = v0_fsm
            .enter_round_and_step_until(Round(6))
            .expect("FSM should have returned invites for 2nd group");

        // We should still not have a raptor-cast group at this point.
        assert_eq!(get_curr_rc_group(&v0_fsm).len(), 0);

        // Verify that it is an invite message the FSM wants to send
        if let FullNodesGroupMessage::PrepareGroup(invite_msg) = group_msg {
            assert_eq!(invite_msg.start_round, Round(13));
            assert_eq!(invite_msg.end_round, Round(18));
            assert_eq!(invite_msg.max_group_size, 3);
            assert_eq!(invite_msg.validator_id, nid(0));
            // Verify that the FSM invites 2 always_ask + 1 random peer_disc
            assert_eq!(invitees.list.len(), 3);
            assert!(equal_node_vec(&invitees.list, &node_ids_vec![10, 11, 15]));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        println!("V0 now: {}", dump_pub_sched(&v0_fsm));

        //-------------------------------------------------------------------[7]
        // We should still not have a raptor-cast group at this point.
        // We should also have nothing scheduled for this round
        let res = v0_fsm.enter_round_and_step_until(Round(7));
        assert!(res.is_none());
        assert_eq!(get_curr_rc_group(&v0_fsm).len(), 0);

        // Replace always-ask full-nodes. Due to look-ahead of 8, this should
        // not affect groups 1, 2, since they were generated during rounds
        // 1 and 6, respectively. This should only affect group 3 (generated
        // during round 11, confirmed at round 15, used at round 18)
        v0_fsm.update_always_ask_full_nodes(node_ids_vec![16]);
        // Upserting peer-discovered full nodes that already are always-ask
        // should have no effect.
        v0_fsm.upsert_peer_disc_full_nodes(FullNodes::new(node_ids_vec![11, 16]));

        //-------------------------------------------------------------------[8]
        // 2nd group invites.t1; 1st raptorcast group available for use
        //----------------------------------------------------------------------
        let res = v0_fsm.enter_round_and_step_until(Round(8));

        // Finally, the Raptorcast group for 1st group1 @ rounds [8, 13)
        assert!(equal_node_vec(
            &get_curr_rc_group(&v0_fsm),
            &node_ids_vec![11, 13]
        ));

        let Some((group_msg, invitees)) = res else {
            panic!(
                "FSM should have returned invites for 3rd group\n\
                publisher v0: {}",
                dump_pub_sched(&v0_fsm)
            );
        };
        if let FullNodesGroupMessage::PrepareGroup(invite_msg) = group_msg {
            assert_eq!(invite_msg.start_round, Round(13));
            assert_eq!(invite_msg.end_round, Round(18));
            assert_eq!(invite_msg.max_group_size, 3);
            assert_eq!(invite_msg.validator_id, nid(0));
            // Verify that the FSM invites 3 more random peer_disc,
            // since we didn't receive any response from the first 3 invites.
            // Note that group 1 candidates were randomized as:
            //  +----- first 3 invites--|----new invites-------|
            //                          v                      v
            // [ nid_10, nid_11, nid_15, nid_14, nid_12, nid_13 ]
            assert_eq!(invitees.list.len(), 3);
            assert!(equal_node_vec(&invitees.list, &node_ids_vec![14, 12, 13]));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        println!("V0 now: {}", dump_pub_sched(&v0_fsm));

        //----------------------------------------------------------------[9-11]
        // Gap to round 11, we should be sending invites for group 3
        // The FSM should use the new always_ask_full_nodes set during round 7
        let res = v0_fsm.enter_round_and_step_until(Round(9));
        assert!(res.is_none());
        let res = v0_fsm.enter_round_and_step_until(Round(10));
        assert!(res.is_some());
        let res = v0_fsm.enter_round_and_step_until(Round(11));
        let (group_msg, invitees) = res.expect("FSM should have returned invites for 2nd group");

        // Verify Group 3
        if let FullNodesGroupMessage::PrepareGroup(invite_msg) = group_msg {
            assert_eq!(invite_msg.start_round, Round(18));
            assert_eq!(invite_msg.end_round, Round(23));
            assert_eq!(invite_msg.max_group_size, 3);
            assert_eq!(invite_msg.validator_id, nid(0));
            // Verify that the FSM invites 1 always_ask + 2 random peer_disc.
            // since after last call to update_always_ask_full_nodes, we only
            // have nid(16) as always_ask_full_node, and it should appear first.
            assert_eq!(invitees.list.len(), 2);
            assert!(equal_node_vec(&invitees.list, &node_ids_vec![16, 11]));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }
    }

    #[test]
    fn client_avoid_one_round_gap() {
        enable_tracer();
        let (clt_tx, clt_rx): RcToRcChannelGrp<ST> = unbounded_channel();
        let mut clt =
            Client::<ST>::new(nid(10), clt_tx, RaptorCastConfigSecondaryClient::default());
        let mut group_map = MockGroupMap::new(nid(10), clt_rx);

        // Represents an invite message received from some validator
        let make_prep_data = |start_round: u64| PrepareGroup {
            validator_id: nid(0),
            max_group_size: 2,
            start_round: Round(start_round),
            end_round: Round(start_round + 2),
        };

        let make_invite_msg = |start_round: u64| make_prep_data(start_round);

        let make_confirm_msg = |start_round: u64| ConfirmGroup {
            prepare: make_prep_data(start_round),
            peers: node_ids_vec![10, 11],
            name_records: Default::default(),
        };

        // Receive invite for rounds [50, 52)
        let invite_msg = make_invite_msg(50);
        let validator_id = invite_msg.validator_id;
        let reply_msg = clt.handle_prepare_group_message(invite_msg);

        assert_eq!(validator_id, nid(0));
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.node_id, nid(10));
        assert_eq!(reply_msg.req, make_prep_data(50));

        let is_valid = clt.handle_confirm_group_message(make_confirm_msg(50));
        assert!(is_valid, "ConfirmGroup for rounds [50, 52) should be valid");

        // Receive invite for rounds [52, 54), from V0
        clt.handle_prepare_group_message(make_invite_msg(52));
        assert!(clt.handle_confirm_group_message(make_confirm_msg(52)));

        // Receive invite for rounds [60, 62), from V0
        clt.handle_prepare_group_message(make_invite_msg(60));
        assert!(clt.handle_confirm_group_message(make_confirm_msg(60)));

        // Receive invite for rounds [62, 64), from V0
        clt.handle_prepare_group_message(make_invite_msg(62));
        assert!(clt.handle_confirm_group_message(make_confirm_msg(62)));

        // Receive raptorcast with proposal 50
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![11, 10]));
        clt.enter_round(Round(50));
        group_map.update(&clt);

        // Receive raptorcast with proposal 51
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![11, 10]));
        clt.enter_round(Round(51));
        group_map.update(&clt);

        // Receive raptorcast with proposal 52
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![11, 10]));
        clt.enter_round(Round(52));
        group_map.update(&clt);

        // Receive raptorcast with proposal 53
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![11, 10]));
        clt.enter_round(Round(53));
        group_map.update(&clt);

        // ~~ gap between rounds 54-60

        // Receive raptorcast with proposal 60
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![11, 10]));
        clt.enter_round(Round(60));
        group_map.update(&clt);

        // Receive raptorcast with proposal 61
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![11, 10]));
        clt.enter_round(Round(61));
        group_map.update(&clt);

        // Receive raptorcast with proposal 62
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![11, 10]));
        clt.enter_round(Round(62));
        group_map.update(&clt);

        // Receive raptorcast with proposal 63
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![11, 10]));
        clt.enter_round(Round(63));
        group_map.update(&clt);
    }

    #[test]
    fn standalone_client_single_group() {
        enable_tracer();
        let (clt_tx, clt_rx): RcToRcChannelGrp<ST> = unbounded_channel();
        let mut clt = Client::<ST>::new(
            nid(10),
            clt_tx,
            RaptorCastConfigSecondaryClient {
                max_num_group: 5,
                max_group_size: 50,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
                invite_accept_heartbeat: Duration::from_secs(10),
            },
        );
        let mut group_map = MockGroupMap::new(nid(10), clt_rx);

        // Represents an invite message received from some validator
        let make_prep_data = |start_round: u64| PrepareGroup {
            validator_id: nid(0),
            max_group_size: 2,
            start_round: Round(start_round),
            end_round: Round(start_round + 2),
        };

        let make_invite_msg = |start_round: u64| make_prep_data(start_round);

        let make_confirm_msg = |start_round: u64| ConfirmGroup {
            prepare: make_prep_data(start_round),
            peers: node_ids_vec![10, 10 + start_round],
            name_records: Default::default(),
        };

        //-------------------------------------------------------------------[1]
        clt.enter_round(Round(1));

        // If asked about some unknown group, return an empty set
        assert!(group_map.is_empty(&clt));

        // Receive invite for round [5, 7). It should be accepted.
        let invite_msg = make_invite_msg(5);
        let validator_id = invite_msg.validator_id;
        let reply_msg = clt.handle_prepare_group_message(invite_msg);

        assert_eq!(validator_id, nid(0));
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.node_id, nid(10));
        assert_eq!(reply_msg.req, make_prep_data(5));

        //-------------------------------------------------------------------[2]
        clt.enter_round(Round(2));

        // We accepted invite for round 5 but it's still only round 2.
        assert!(group_map.is_empty(&clt));

        //-------------------------------------------------------------------[3]
        clt.enter_round(Round(3));
        assert!(group_map.is_empty(&clt));

        // Receive invite for round [8, 10). It should be accepted.
        // Note we have a group-less gap [7, 8)
        let invite_msg = make_invite_msg(8);
        let validator_id = invite_msg.validator_id;
        let reply_msg = clt.handle_prepare_group_message(invite_msg);

        assert_eq!(validator_id, nid(0));
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.node_id, nid(10));
        assert_eq!(reply_msg.req, make_prep_data(8));

        //-------------------------------------------------------------------[4]
        clt.enter_round(Round(4));
        assert!(group_map.is_empty(&clt));

        // This is the last opportunity to receive confirm for group 1.
        // Lets accept both here, out of order.
        assert_eq!(clt.num_pending_confirms(), 2);
        assert!(clt.handle_confirm_group_message(make_confirm_msg(8)));
        assert!(clt.handle_confirm_group_message(make_confirm_msg(5)));

        //-------------------------------------------------------------------[5]
        clt.enter_round(Round(5));

        // Now we should have a proper raptorcast group keyed on v0 (nid(0))
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        // We should have 2 peers in the group from v0 (nid_0): nid_10, nid_15
        assert!(equal_node_set(rc_grp, &node_ids![15, 10]));

        //-------------------------------------------------------------------[6]
        clt.enter_round(Round(6));

        // RaptorCast group from v0 should still be up
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![15, 10]));

        //-------------------------------------------------------------------[7]
        clt.enter_round(Round(7));

        // Although raptorCast group from v0 only covered rounds [5, 7), we will
        // still see the next group when querying for a group. This is because
        // in the new behaviour, all groups are pushed to primary and will
        // prefer to yield a future group if such one exists.
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![18, 10]));

        //-------------------------------------------------------------------[8]
        clt.enter_round(Round(8));

        // We should now see the second group from v0 (nid_0): nid_10, nid_18
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![18, 10]));

        // Pending confirms are lazily cleaned up after the group starts.
        assert_eq!(clt.num_pending_confirms(), 0);
    }

    #[test]
    fn mid_round_client_start() {
        enable_tracer();
        let (clt_tx, clt_rx): RcToRcChannelGrp<ST> = unbounded_channel();
        let mut clt = Client::<ST>::new(
            nid(10),
            clt_tx,
            RaptorCastConfigSecondaryClient {
                max_num_group: 5,
                max_group_size: 50,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
                invite_accept_heartbeat: Duration::from_secs(10),
            },
        );
        let mut group_map = MockGroupMap::new(nid(10), clt_rx);

        let make_prep_data = |start_round: u64| PrepareGroup {
            validator_id: nid(0),
            max_group_size: 2,
            start_round: Round(start_round),
            end_round: Round(start_round + 2),
        };

        let make_invite_msg = |start_round: u64| make_prep_data(start_round);

        let make_confirm_msg = |start_round: u64| ConfirmGroup {
            prepare: make_prep_data(start_round),
            peers: node_ids_vec![10, 10 + start_round],
            name_records: Default::default(),
        };

        //------------------------------------------------------------------[31]
        clt.enter_round(Round(31));

        // If asked about some unknown group, return an empty set
        assert!(group_map.is_empty(&clt));

        // Receive invite for round [35, 37). It should be accepted.
        let invite_msg = make_invite_msg(35);
        let validator_id = invite_msg.validator_id;
        let reply_msg = clt.handle_prepare_group_message(invite_msg);

        assert_eq!(validator_id, nid(0));
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.node_id, nid(10));
        assert_eq!(reply_msg.req, make_prep_data(35));

        //------------------------------------------------------------------[32]
        clt.enter_round(Round(32));

        // We accepted invite for round 5 but it's still only round 32.
        assert!(group_map.is_empty(&clt));

        //------------------------------------------------------------------[33]
        clt.enter_round(Round(33));
        assert!(group_map.is_empty(&clt));

        // Receive invite for round [38, 40). It should be accepted.
        // Note we have a group-less gap [37, 38)

        let invite_msg = make_invite_msg(38);
        let validator_id = invite_msg.validator_id;
        let reply_msg = clt.handle_prepare_group_message(invite_msg);

        assert_eq!(validator_id, nid(0));
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.node_id, nid(10));
        assert_eq!(reply_msg.req, make_prep_data(38));

        //------------------------------------------------------------------[34]
        clt.enter_round(Round(34));
        assert!(group_map.is_empty(&clt));

        // This is the last opportunity to receive confirm for group 31.
        // Lets accept both here, out of order.
        // Note that make_confirm_msg() return node ids = start_round + 10,
        // so the first node in the second group is 45
        assert_eq!(clt.num_pending_confirms(), 2);
        assert!(clt.handle_confirm_group_message(make_confirm_msg(38)));
        assert!(clt.handle_confirm_group_message(make_confirm_msg(35)));

        //------------------------------------------------------------------[35]
        clt.enter_round(Round(35));

        // Now we should have a proper raptorcast group keyed on v0 (nid(0))
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        // We should have 2 peers in the group from v0 (nid_0): nid_10, nid_45
        assert!(equal_node_set(rc_grp, &node_ids![45, 10]));

        //------------------------------------------------------------------[36]
        clt.enter_round(Round(36));

        // RaptorCast group from v0 should still be up
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![45, 10]));

        //------------------------------------------------------------------[37]
        clt.enter_round(Round(37));

        // In the new behaviour, all groups are pushed to primary and will hence
        // avoid a gap if a future group for the author id exists.
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![48, 10]));

        //------------------------------------------------------------------[38]
        clt.enter_round(Round(38));

        // We should now see the second group from v0 (nid_0): nid_10, nid_18
        let rc_grp = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![48, 10]));

        // Pending confirms are lazily cleaned up after the group starts.
        assert_eq!(clt.num_pending_confirms(), 0);
    }

    #[test]
    fn standalone_client_multi_group() {
        enable_tracer();
        // Group schedule
        //----------------------------+
        // Round    | Validator.Group |
        //----------------------------+
        // 8        | v0.0
        // 9        | v0.0  v1.0
        // 10       |       v1.0  v2.0
        // 11       | v0.1        v2.0
        // 12       | v0.1
        // 13       |
        // 14       | v0.2  v1.1  v2.1
        // 15       | v0.2  v1.1  v2.1
        // 16       | v0.2  v1.1  v2.1

        let me = 10;
        let (clt_tx, clt_rx): RcToRcChannelGrp<ST> = unbounded_channel();
        let mut clt = Client::<ST>::new(
            nid(me),
            clt_tx,
            RaptorCastConfigSecondaryClient {
                max_num_group: 5,
                max_group_size: 50,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
                invite_accept_heartbeat: Duration::from_secs(10),
            },
        );
        let mut group_map = MockGroupMap::new(nid(me), clt_rx);

        // Represents an invite message received from some validator
        let invite_data = |start_round: u64, validator_id: u64| PrepareGroup {
            validator_id: nid(validator_id),
            max_group_size: 2,
            start_round: Round(start_round),
            end_round: Round(start_round + 2),
        };

        let make_invite_msg =
            |start_round: u64, validator_id: u64| invite_data(start_round, validator_id);

        let make_confirm_msg = |start_round: u64, validator_id: u64| ConfirmGroup {
            prepare: invite_data(start_round, validator_id),
            peers: node_ids_vec![me, me + start_round],
            name_records: Default::default(),
        };

        //-------------------------------------------------------------------[1]
        clt.enter_round(Round(1));

        //----------------------------+
        // Round    | Validator.Group |
        //----------------------------+
        // 8        | v0.0
        // 9        | v0.0  v1.0
        // 10       |       v1.0

        // Invite v0.0
        let invite_msg = make_invite_msg(8, 0);
        let validator_id = invite_msg.validator_id;
        let reply_msg = clt.handle_prepare_group_message(invite_msg);

        assert_eq!(validator_id, nid(0));
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.req, invite_data(8, 0));
        assert!(clt.handle_confirm_group_message(make_confirm_msg(8, 0)));

        // Invite v1.0
        let invite_msg = make_invite_msg(9, 1);
        let validator_id = invite_msg.validator_id;
        let reply_msg = clt.handle_prepare_group_message(invite_msg);

        assert_eq!(validator_id, nid(1));
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.req, invite_data(9, 1));
        assert!(clt.handle_confirm_group_message(make_confirm_msg(9, 1)));

        //-------------------------------------------------------------------[2]
        clt.enter_round(Round(2));

        //----------------------------+
        // Round    | Validator.Group |
        //----------------------------+
        // 10       |             v2.0
        // 11       | v0.1        v2.0
        // 12       | v0.1

        // Invite v2.0
        let invite_msg = make_invite_msg(me, 2);
        let validator_id = invite_msg.validator_id;
        let reply_msg = clt.handle_prepare_group_message(invite_msg);

        assert_eq!(validator_id, nid(2));
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.req, invite_data(me, 2));
        assert!(clt.handle_confirm_group_message(make_confirm_msg(me, 2)));

        // Invite v0.1
        let invite_msg = make_invite_msg(11, 0);
        let validator_id = invite_msg.validator_id;
        let reply_msg = clt.handle_prepare_group_message(invite_msg);

        assert_eq!(validator_id, nid(0));
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.req, invite_data(11, 0));
        assert!(clt.handle_confirm_group_message(make_confirm_msg(11, 0)));

        //-------------------------------------------------------------------[3]
        clt.enter_round(Round(3));

        //----------------------------+
        // Round    | Validator.Group |
        //----------------------------+
        // 14       | v0.2  v1.1  v2.1
        // 15       | v0.2  v1.1  v2.1
        // 16       | v0.2  v1.1  v2.1

        // Invite v0.2
        let invite_msg = make_invite_msg(14, 0);
        let validator_id = invite_msg.validator_id;
        let reply_msg = clt.handle_prepare_group_message(invite_msg);

        assert_eq!(validator_id, nid(0));
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.req, invite_data(14, 0));
        assert!(clt.handle_confirm_group_message(make_confirm_msg(14, 0)));

        // Invite v1.1
        let invite_msg = make_invite_msg(14, 1);
        let validator_id = invite_msg.validator_id;
        let reply_msg = clt.handle_prepare_group_message(invite_msg);

        assert_eq!(validator_id, nid(1));
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.req, invite_data(14, 1));
        assert!(clt.handle_confirm_group_message(make_confirm_msg(14, 1)));

        // Invite v2.1
        let invite_msg = make_invite_msg(14, 2);
        let validator_id = invite_msg.validator_id;
        let reply_msg = clt.handle_prepare_group_message(invite_msg);

        assert_eq!(validator_id, nid(2));
        assert_eq!(reply_msg.req, invite_data(14, 2));
        assert!(clt.handle_confirm_group_message(make_confirm_msg(14, 2)));

        //-----------------------------------------------------------------[4-7]
        // Simulated gap - no receiving proposals

        //-------------------------------------------------------------------[8]
        clt.enter_round(Round(8));

        // Now we should have raptorcast group v0.0 only: nid_10, nid_15
        let rc = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc, &node_ids![18, me]));

        //-------------------------------------------------------------------[9]
        clt.enter_round(Round(9));

        // We should still have raptorcast group v0.0
        let rc = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc, &node_ids![18, me]));

        // We should now also have raptorcast group v1.0
        let rc = &group_map.get_rc_group_peers(&clt, &nid(1));
        assert!(equal_node_set(rc, &node_ids![19, me]));

        //------------------------------------------------------------------[10]
        clt.enter_round(Round(10));

        let rc = &group_map.get_rc_group_peers(&clt, &nid(1));
        assert!(equal_node_set(rc, &node_ids![19, me]));

        let rc = &group_map.get_rc_group_peers(&clt, &nid(2));
        assert!(equal_node_set(rc, &node_ids![20, me]));

        //------------------------------------------------------------------[11]
        clt.enter_round(Round(11));

        let rc = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc, &node_ids![21, me]));

        let rc = &group_map.get_rc_group_peers(&clt, &nid(2));
        assert!(equal_node_set(rc, &node_ids![20, me]));

        //------------------------------------------------------------------[12]
        clt.enter_round(Round(12));

        let rc = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert!(equal_node_set(rc, &node_ids![21, me]));

        //---------------------------------------------------------------[13-15]
        for round in 14..=15 {
            //----------------------------+
            // Round    | Validator.Group |
            //----------------------------+
            // 14       | v0.2  v1.1  v2.1
            // 15       | v0.2  v1.1  v2.1
            clt.enter_round(Round(round));

            let rc = &group_map.get_rc_group_peers(&clt, &nid(0));
            assert!(equal_node_set(rc, &node_ids![24, me]));

            let rc = &group_map.get_rc_group_peers(&clt, &nid(1));
            assert!(equal_node_set(rc, &node_ids![24, me]));

            let rc = &group_map.get_rc_group_peers(&clt, &nid(2));
            assert!(equal_node_set(rc, &node_ids![24, me]));
        }

        //------------------------------------------------------------------[16]
        clt.enter_round(Round(16));

        //----------------------------+
        // Round    | Validator.Group |
        //----------------------------+
        // 16       |

        let rc = &group_map.get_rc_group_peers(&clt, &nid(0));
        assert_eq!(rc.len(), 0);

        let rc = &group_map.get_rc_group_peers(&clt, &nid(1));
        assert_eq!(rc.len(), 0);

        let rc = &group_map.get_rc_group_peers(&clt, &nid(2));
        assert_eq!(rc.len(), 0);

        // Pending confirms are lazily cleaned up after the group starts.
        assert_eq!(clt.num_pending_confirms(), 0);
    }

    // cargo test -p monad-raptorcast raptorcast_secondary::tests::gap_into_middle_of_scheduled_group -- --nocapture
    // The validator was seen crashing after seeing a round gap which causes it
    // to enter the middle of a scheduled group. e.g.
    // Scheduled groups:  [3, 6)  [6, 9)  [9, 11)
    // Round gap: 5 -> 10
    #[test]
    fn gap_into_middle_of_scheduled_group() {
        enable_tracer();
        let sched_cfg = GroupSchedulingConfig {
            max_group_size: 1,
            round_span: Round(3),
            invite_lookahead: Round(100),
            max_invite_wait: Round(1),
            deadline_round_dist: Round(1),
            init_empty_round_span: Round(2), // >= invite_wait + deadline
        };

        let mut v0_fsm: Publisher<ST> = Publisher::new(
            nid(0),
            RaptorCastConfigSecondaryPublisher {
                full_nodes_prioritized: vec![nid(10)],
                group_scheduling: sched_cfg,
            },
            ChaCha8Rng::seed_from_u64(42),
        );

        // PREPARE [3, 6)
        let (group_msg, _invitees) = v0_fsm
            .enter_round_and_step_until(Round(1))
            .expect("FSM should have returned invites to be sent");

        if let FullNodesGroupMessage::PrepareGroup(invite_msg) = group_msg {
            assert_eq!(invite_msg.start_round, Round(3));
            assert_eq!(invite_msg.end_round, Round(6));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        // ACCEPT [3, 6)
        let accept_msg = make_invite_response(nid(0), nid(10), true, Round(3), &sched_cfg);
        v0_fsm.on_candidate_response(accept_msg);

        // CONFIRM [3, 6)
        let (group_msg, _invitees) = v0_fsm
            .enter_round_and_step_until(Round(2))
            .expect("FSM should have returned invites to be sent");

        if let FullNodesGroupMessage::ConfirmGroup(confirm_msg) = group_msg {
            assert_eq!(confirm_msg.prepare.start_round, Round(3));
            assert_eq!(confirm_msg.prepare.end_round, Round(6));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        // PREPARE [6, 9)
        let (group_msg, _invitees) = v0_fsm
            .enter_round_and_step_until(Round(3))
            .expect("FSM should have returned invites to be sent");

        if let FullNodesGroupMessage::PrepareGroup(invite_msg) = group_msg {
            assert_eq!(invite_msg.start_round, Round(6));
            assert_eq!(invite_msg.end_round, Round(9));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        // ACCEPT [6, 9)
        let accept_msg = make_invite_response(nid(0), nid(10), true, Round(6), &sched_cfg);
        v0_fsm.on_candidate_response(accept_msg);

        // CONFIRM [6, 9)
        let (group_msg, _invitees) = v0_fsm
            .enter_round_and_step_until(Round(4))
            .expect("FSM should have returned invites to be sent");

        if let FullNodesGroupMessage::ConfirmGroup(confirm_msg) = group_msg {
            assert_eq!(confirm_msg.prepare.start_round, Round(6));
            assert_eq!(confirm_msg.prepare.end_round, Round(9));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        // PREPARE [9, 12)
        let (group_msg, _invitees) = v0_fsm
            .enter_round_and_step_until(Round(4))
            .expect("FSM should have returned invites to be sent");

        if let FullNodesGroupMessage::PrepareGroup(invite_msg) = group_msg {
            assert_eq!(invite_msg.start_round, Round(9));
            assert_eq!(invite_msg.end_round, Round(12));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        // ACCEPT [9, 12)
        let accept_msg = make_invite_response(nid(0), nid(10), true, Round(9), &sched_cfg);
        v0_fsm.on_candidate_response(accept_msg);

        // CONFIRM [9, 12)
        let (group_msg, _invitees) = v0_fsm
            .enter_round_and_step_until(Round(5))
            .expect("FSM should have returned invites to be sent");

        if let FullNodesGroupMessage::ConfirmGroup(confirm_msg) = group_msg {
            assert_eq!(confirm_msg.prepare.start_round, Round(9));
            assert_eq!(confirm_msg.prepare.end_round, Round(12));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        //============================================================================
        // Now gap 5 -> 11, crossing into the middle of future group [3, 6) -> [9, 11)
        // Before the fix this would trigger a panic.
        //============================================================================
        let (group_msg, _invitees) = v0_fsm
            .enter_round_and_step_until(Round(10))
            .expect("FSM should have returned invites to be sent");

        // Make sure the group still is there for use
        assert!(equal_node_vec(
            &get_curr_rc_group(&v0_fsm),
            &node_ids_vec![10]
        ));

        if let FullNodesGroupMessage::PrepareGroup(invite_msg) = group_msg {
            assert_eq!(invite_msg.start_round, Round(12));
            assert_eq!(invite_msg.end_round, Round(15));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        // Check that group scheduling continues as usual
        let (group_msg, _invitees) = v0_fsm
            .enter_round_and_step_until(Round(11))
            .expect("FSM should have returned invites to be sent");

        if let FullNodesGroupMessage::ConfirmGroup(confirm_msg) = group_msg {
            assert_eq!(confirm_msg.prepare.start_round, Round(12));
            assert_eq!(confirm_msg.prepare.end_round, Round(15));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        // Check that group scheduling continues as usual
        let (group_msg, _invitees) = v0_fsm
            .enter_round_and_step_until(Round(12))
            .expect("FSM should have returned invites to be sent");

        if let FullNodesGroupMessage::PrepareGroup(invite_msg) = group_msg {
            assert_eq!(invite_msg.start_round, Round(15));
            assert_eq!(invite_msg.end_round, Round(18));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }
    }

    // cargo test -p monad-raptorcast raptorcast_secondary::tests::reject_and_accept_counter -- --nocapture
    #[test]
    fn reject_and_accept_counter() {
        enable_tracer();
        let sched_cfg = GroupSchedulingConfig {
            max_group_size: 3,
            round_span: Round(5),
            invite_lookahead: Round(8),
            max_invite_wait: Round(2),
            deadline_round_dist: Round(3),
            init_empty_round_span: Round(7),
        };

        let v0_node_id = nid(0);

        let mut v0_fsm: Publisher<ST> = Publisher::new(
            v0_node_id,
            RaptorCastConfigSecondaryPublisher {
                full_nodes_prioritized: vec![nid(10), nid(11)],
                group_scheduling: sched_cfg,
            },
            ChaCha8Rng::seed_from_u64(42),
        );

        // Peer discovery gives us some new full-nodes to chose from.
        v0_fsm.upsert_peer_disc_full_nodes(FullNodes::new(vec![
            nid(12),
            nid(13),
            nid(14),
            nid(15),
        ]));

        let (group_msg, invitees) = v0_fsm
            .enter_round_and_step_until(Round(1))
            .expect("FSM should have returned invites to be sent");

        if let FullNodesGroupMessage::PrepareGroup(invite_msg) = group_msg {
            assert_eq!(invite_msg.start_round, Round(8));
            assert_eq!(invite_msg.end_round, Round(13));
            assert_eq!(invite_msg.max_group_size, 3);
            assert_eq!(invite_msg.validator_id, nid(0));
            assert_eq!(invitees.list.len(), 3);
            assert!(equal_node_vec(&invitees.list, &node_ids_vec![10, 11, 12]));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }

        //----------------------------------------------------------------------
        // 1st group nid_10, nid_11: accept. nid_12: reject
        //----------------------------------------------------------------------

        let response = make_invite_response(nid(0), nid(10), true, Round(8), &sched_cfg);
        v0_fsm.on_candidate_response(response);

        let response = make_invite_response(nid(0), nid(11), true, Round(8), &sched_cfg);
        v0_fsm.on_candidate_response(response);

        // A reject from node 12
        let response = make_invite_response(nid(0), nid(12), false, Round(8), &sched_cfg);
        v0_fsm.on_candidate_response(response);

        //----------------------------------------------------------------------
        // 1st group invites t1
        //----------------------------------------------------------------------
        let (group_msg, invitees) = v0_fsm
            .enter_round_and_step_until(Round(3))
            .expect("FSM should have returned invites to be sent");

        // Verify that it is an invite message the FSM wants to send
        if let FullNodesGroupMessage::PrepareGroup(invite_msg) = group_msg {
            assert_eq!(invite_msg.start_round, Round(8));
            assert_eq!(invite_msg.end_round, Round(13));
            assert_eq!(invite_msg.max_group_size, 3);
            assert_eq!(invite_msg.validator_id, nid(0));
            // Verify that only 1 more invite is sent, and that its for node 14
            assert_eq!(invitees.list.len(), 1);
            assert!(equal_node_vec(&invitees.list, &node_ids_vec![14]));
        } else {
            panic!(
                "Expected FullNodesGroupMessage::PrepareGroup, got: {:?}\n\
                publisher v0: {}",
                group_msg,
                dump_pub_sched(&v0_fsm)
            );
        }
        // Check that the publisher is aware of of who has rejected the invite
        if let Some(group) = v0_fsm.group_schedule.get(&Round(8)) {
            assert_eq!(group.full_nodes_accepted.list.len(), 2);
            assert_eq!(group.full_nodes_rejected.list.len(), 1);
            assert!(equal_node_vec(
                &group.full_nodes_rejected.list,
                &node_ids_vec![12]
            ));
        } else {
            panic!("Expected a group to be scheduled for round 8");
        }

        //----------------------------------------------------------------------
        // Node 12 now sends an accept after already have sent a reject
        //----------------------------------------------------------------------
        let bogus_response = make_invite_response(nid(0), nid(12), true, Round(8), &sched_cfg);
        v0_fsm.on_candidate_response(bogus_response);

        // Verify that the publisher does not change accept/reject states
        if let Some(group) = v0_fsm.group_schedule.get(&Round(8)) {
            assert_eq!(group.full_nodes_accepted.list.len(), 2);
            assert_eq!(group.full_nodes_rejected.list.len(), 1);
            assert!(equal_node_vec(
                &group.full_nodes_rejected.list,
                &node_ids_vec![12]
            ));
        } else {
            panic!("Expected a group to be scheduled for round 8");
        }
    }
}
