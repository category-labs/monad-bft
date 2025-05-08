use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt,
    marker::PhantomData,
    net::SocketAddr,
    pin::Pin,
    sync::{mpsc::Receiver, Arc, Mutex},
    task::{Context, Poll, Waker},
};

use bytes::Bytes;
use futures::Stream;
use iset::IntervalMap;
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::{udp::segment_size_for_mtu, BroadcastMsg, Dataplane, UnicastMsg};
use monad_discovery::message::{FullNodesGroupMessage, OutboundRouterMessage};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{Message, RouterCommand};
use monad_types::{
    Deserializable,
    Epoch,
    NodeId,
    Round,
    Serializable,
    // DropTimer, RouterTarget,
    GENESIS_ROUND,
};
use rand::{seq::SliceRandom, SeedableRng};
use rand_chacha::ChaCha8Rng;

use super::{
    config::{
        GroupSchedulingConfig, RaptorCastConfig, RaptorCastConfigSecondaryClient,
        RaptorCastConfigSecondaryPublisher,
    },
    //udp,
    util::{FullNodes, FullNodesView}, // EpochValidators, BuildTarget
    PeerManagerResponse,
    RaptorCastEvent,
};

type NodeIdST<ST> = NodeId<CertificateSignaturePubKey<ST>>;
type FullNodesST<ST> = FullNodes<CertificateSignaturePubKey<ST>>;
type FullNodesGroupMsg<ST> = FullNodesGroupMessage<CertificateSignaturePubKey<ST>>;
type PrepareGroupMsg<ST> = monad_discovery::message::PrepareGroup<CertificateSignaturePubKey<ST>>;
type TimePoint = Round;
type Bandwidth = u64;

// This firs section is for when the router is playing the role of: PUBLISHER
// That is, we are a validator sending group invites to random full-nodes for
// raptor-casting messages to them

// Main state machine for when the secondary RC router is acting as a publisher
struct Publisher<ST>
where
    ST: CertificateSignatureRecoverable,
{
    // Constant data
    validator_node_id: NodeIdST<ST>,
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
    curr_group: GroupAsPublisher<ST>,
}

impl<ST> Publisher<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new(
        validator_node_id: NodeIdST<ST>,
        config: RaptorCastConfigSecondaryPublisher<ST>,
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
        let mut always_ask_full_nodes: FullNodesST<ST> = FullNodes::default();

        {
            let mut seen = HashSet::new();
            for node in config.full_nodes_prioritized {
                if seen.insert(node) {
                    always_ask_full_nodes.list.push(node);
                }
            }
        }

        Self {
            validator_node_id,
            scheduling_cfg,
            group_schedule: BTreeMap::new(),
            always_ask_full_nodes,
            peer_disc_full_nodes: FullNodes::new(Vec::new()),
            rng: ChaCha8Rng::seed_from_u64(42), // useful for tests
            curr_round: Round::MIN,
            curr_group: GroupAsPublisher::new_empty_locked(GENESIS_ROUND, GENESIS_ROUND),
        }
    }

    // While we don't have a real timer, we can call this instead of
    // enter_round() + step_until()
    pub fn enter_round_and_step_until(
        &mut self,
        round: Round,
    ) -> Option<(FullNodesGroupMsg<ST>, FullNodesST<ST>)> {
        // Just some sanity check
        {
            if round < self.curr_round {
                tracing::error!(
                    "RaptorCastSecondary ignoring backwards round \
                    {:?} -> {:?}",
                    self.curr_round,
                    round
                );
                return None;
            }
            if round > self.curr_round + Round(1) {
                tracing::warn!(
                    "RaptorCastSecondary detected round gap \
                    {:?} -> {:?}",
                    self.curr_round,
                    round
                );
            }
            self.curr_round = round;
        }
        self.enter_round(round);
        self.step_until(round)
    }

    // Populate self.curr_group and clean up expired groups.
    // When we have a real timer, this can be called from UpdateCurrentRound
    fn enter_round(&mut self, round: Round) {
        assert_ne!(round, Round::MAX);

        if round < self.curr_group.end_round {
            // We don't need to advance to the next group yet
            assert!(round >= self.curr_group.start_round);
            return;
        }

        // Remove all groups that have ended.
        self.group_schedule
            .retain(|_, group| group.end_round > round);

        // If `round` belongs in the next group, pop & make it the current one.
        match self.group_schedule.first_key_value() {
            Some((start_round, group)) => {
                assert!(start_round >= &round);
                assert!(start_round == &group.start_round);
                if round >= group.start_round && round < group.end_round {
                    // The typical, correct case.
                    self.curr_group = self.group_schedule.pop_first().unwrap().1;
                    return;
                }
                // The next group doesn't match the new round. Is this possible
                // when there are gaps in the round sequence?
                tracing::error!(
                    "No group scheduled for RaptorcastSecondary \
                    round {:?}, next group is {:?}",
                    round,
                    group
                );
            }
            None => {
                // We didn't manage to form a group in time for the new round.
                // Might be due to gap, unexpectedly long delays or Round 0/
                // We currently have an empty group for some rounds while we
                // allow invites for a future group to complete.
            }
        }
        // Fallback group for error cases.
        self.curr_group =
            GroupAsPublisher::new_empty_locked(round, self.scheduling_cfg.init_empty_round_span);
    }

    // Advances the state machine to the given "time point".
    // Time point is represented as a Round for now, but can be Duration later.
    // For now called from UpdateCurrentRound, but later from timer mechanism
    fn step_until(
        &mut self,
        time_point: TimePoint,
    ) -> Option<(FullNodesGroupMsg<ST>, FullNodesST<ST>)> {
        let schedule_end: Round = match self.group_schedule.last_entry() {
            Some(grp) => grp.get().end_round,
            None => self.curr_group.end_round,
        };

        // Check if scheduled groups need servicing: time-outs, invites, confirm
        for (_, group) in self.group_schedule.iter_mut() {
            if let Some(out_msg) =
                group.advance_invites(time_point, &self.scheduling_cfg, self.validator_node_id)
            {
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
            self.group_schedule.insert(new_group.start_round, new_group);
            return maybe_invites;
        }

        // No invites to send out at the moment.
        None
    }

    // Process response from a full-node who we previously invited to join one
    // of our Raptorcast group scheduled for a future round.
    pub fn on_candidate_response(&mut self, msg: FullNodesGroupMsg<ST>) {
        match msg {
            FullNodesGroupMessage::PrepareGroupResponse(response) => {
                let candidate = response.node_id;
                let start_round = response.req.start_round;
                if let Some(group) = self.group_schedule.get_mut(&start_round) {
                    group.on_candidate_response(candidate, response.accept);
                } else {
                    tracing::warn!(
                        "Ignoring response from FullNode {:?}, \
                        as there is no group scheduled to start in round {:?}",
                        candidate,
                        start_round
                    );
                }
            }
            _ => {
                tracing::error!(
                    "RaptorCastSecondary publisher received \
                    unexpected message: {:?}",
                    msg
                );
            }
        }
    }

    pub fn get_current_raptorcast_group(
        &mut self,
    ) -> FullNodesView<CertificateSignaturePubKey<ST>> {
        self.curr_group.get_full_node_view_for_raptorcast()
    }

    pub fn update_always_ask_full_nodes(
        &mut self,
        replace_fn: FullNodes<CertificateSignaturePubKey<ST>>,
    ) {
        self.always_ask_full_nodes = replace_fn;
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
        for node in additional_fn.list {
            if self.peer_disc_full_nodes.list.contains(&node) || // already have
               self.always_ask_full_nodes.list.contains(&node) || // already ask
               node == self.validator_node_id
            // we can't be a candidate
            {
                continue;
            }
            self.peer_disc_full_nodes.list.push(node);
        }
    }
}

// A RaptorCast group of full-nodes we currently are broadcasting to (or later)
struct GroupAsPublisher<ST>
where
    ST: CertificateSignatureRecoverable,
{
    full_nodes_accepted: FullNodesST<ST>,

    // Pre-randomized permutation of always_ask_full_nodes[] + peer_disc_full_nodes[]
    // Stays const once created.
    full_nodes_candidates: FullNodesST<ST>,
    num_invites_sent: usize,     // also an index into full_nodes_candidates[]
    num_invites_rejected: usize, // just for debug info for now

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
            full_nodes_candidates: always_ask_full_nodes.clone(),
            num_invites_sent: 0,
            num_invites_rejected: 0,
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

    // For when we need a group now and don't have time to invite full-nodes.
    pub fn new_empty_locked(start_round: Round, span: Round) -> Self {
        Self {
            full_nodes_accepted: FullNodes::default(),
            full_nodes_candidates: FullNodes::default(),
            num_invites_sent: 0,
            num_invites_rejected: 0,
            start_round,
            end_round: start_round + span,
            next_invite_tp: TimePoint::MAX,
        }
    }

    pub fn is_locked(&self) -> bool {
        // self.full_nodes_candidates.list.is_empty()
        self.next_invite_tp == TimePoint::MAX
    }

    // Returns a set of full-nodes where the group invites should be sent to.
    pub fn advance_invites(
        &mut self,
        curr_timestamp: TimePoint,
        cfg: &GroupSchedulingConfig,
        validator_id: NodeIdST<ST>,
    ) -> Option<(FullNodesGroupMsg<ST>, FullNodesST<ST>)> {
        if curr_timestamp < self.next_invite_tp {
            return None;
        }

        // PrepareGroup data is needed in either case: send invites or confirm
        let prep_grp_data = monad_discovery::message::PrepareGroup {
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
            self.next_invite_tp = TimePoint::MAX; // lock the group
                                                  //self.full_nodes_candidates.clear(); // lock the group
            let confirm_data = monad_discovery::message::ConfirmGroup {
                prepare: prep_grp_data,
                peers: self.full_nodes_accepted.list.clone(),
                name_records: None, // to be filled by next layer
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
        Some((invite_msg, next_invitees))
    }

    pub fn on_candidate_response(&mut self, candidate: NodeIdST<ST>, accepted: bool) {
        if self.is_locked() {
            // Already formed the group
            return;
        }
        if !self.full_nodes_candidates.list.contains(&candidate) {
            tracing::warn!(
                "Ignoring response from FullNode {:?} who was \
                never invited to RaptorcastSecondary group {:?}",
                candidate,
                self
            );
            return;
        }
        if self.full_nodes_accepted.list.contains(&candidate) {
            tracing::warn!(
                "Ignoring duplicate response from FullNode {:?} \
                who was already accepted into RaptorcastSecondary group {:?}",
                candidate,
                self
            );
            return;
        }
        if accepted {
            self.full_nodes_accepted.list.push(candidate);
            tracing::debug!(
                "RaptorcastSecondary group invite accepted by {:?} \
                for group {:?}",
                candidate,
                self
            );
        } else {
            self.num_invites_rejected += 1;
            tracing::debug!(
                "RaptorcastSecondary group invite rejected by {:?} \
                for group {:?}",
                candidate,
                self
            );
        }
    }

    pub fn get_full_node_view_for_raptorcast(
        &self,
    ) -> FullNodesView<CertificateSignaturePubKey<ST>> {
        self.full_nodes_accepted.view()
    }
}

// The following section is for when the router is playing the role of: CLIENT
// That is, we are a full-node receiving group invites from a validator

// Main state machine for client
struct Client<ST>
where
    ST: CertificateSignatureRecoverable,
{
    client_node_id: NodeIdST<ST>, // Our (full-node) node_id as an invitee

    // Full nodes may choose to reject a request if it doesn’t have enough
    // upload bandwidth to broadcast chunk to a large group.
    config: RaptorCastConfigSecondaryClient,

    // [start_round, end_round) -> GroupAsClient
    // Represents all raptorcast groups that we have accepted. They may overlap.
    confirmed_groups: IntervalMap<Round, GroupAsClient<ST>>,

    // start_round -> validator_id -> group invite
    // Once we receive an invite, we remember how the invite looked like, so
    // that we don't blindly accept any confirmation message.
    pending_confirms: BTreeMap<Round, BTreeMap<NodeIdST<ST>, PrepareGroupMsg<ST>>>,

    // This always reflects confirmed_groups[curr_round], and is empty if
    // confirmed_groups is empty. Rebuilt every time we enter a new round.
    // Key: validator's node_id, i.e. inbound Raptorcast chunk's `author` field
    curr_raptorcast_groups: BTreeMap<NodeIdST<ST>, Vec<NodeIdST<ST>>>,
    EMPTY_GROUP: Vec<NodeIdST<ST>>,

    // For avoiding accepting invites/confirms for rounds we've already started
    curr_round: Round,
}

impl<ST> Client<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new(client_node_id: NodeIdST<ST>, config: RaptorCastConfigSecondaryClient) -> Self {
        Self {
            client_node_id,
            config,
            confirmed_groups: IntervalMap::new(),
            pending_confirms: BTreeMap::new(),
            curr_raptorcast_groups: BTreeMap::new(),
            EMPTY_GROUP: Vec::new(),
            curr_round: GENESIS_ROUND,
        }
    }

    // Called from UpdateCurrentRound
    pub fn enter_round(&mut self, curr_round: Round) {
        // Sanity check on curr_round
        if curr_round < self.curr_round {
            tracing::error!(
                "RaptorcastSecondary ignoring backwards round \
                {:?} -> {:?}",
                self.curr_round,
                curr_round
            );
            return;
        } else if curr_round > self.curr_round + Round(1) {
            tracing::warn!(
                "RaptorcastSecondary detected round gap \
                {:?} -> {:?}",
                self.curr_round,
                curr_round
            );
        }
        self.curr_round = curr_round;

        // Remove all groups that have ended:
        //            curr_round
        //               |
        // 0.........1.........2.........3.........4
        //  [..A..)                   remove
        //       [...B...)            remove
        //     [......C...)           keep
        //              [....D...]    keep
        //               [....E...]   not looked at
        {
            let mut keys_to_remove = Vec::new();
            for iv in self.confirmed_groups.intervals(..curr_round) {
                if iv.end <= curr_round {
                    keys_to_remove.push(iv);
                }
            }
            for iv in keys_to_remove {
                self.confirmed_groups.remove(iv);
            }
        }

        // Clean up old invitations.
        self.pending_confirms.retain(|&key, _| key > curr_round);

        // It's easier to rebuild this every time we enter a new round, but
        // ideally we'd only rebuild when the union set of groups changes.
        // Perhaps we can detect that by using self.confirmed_groups.intervals()
        self.curr_raptorcast_groups.clear();
        let curr_round_end = curr_round + Round(1);
        for group in self.confirmed_groups.values(curr_round..curr_round_end) {
            self.curr_raptorcast_groups
                .insert(group.validator_id, group.all_peers.clone());
        }
    }

    // Lets Raptorcast's UdpState know which peers to re-broadcast to.
    // Must match the argument 'get_peers` for UdpState::handle_message().
    // `get_peers` is the refactored version of old arg `epoch_validators`.
    pub fn get_curr_raptorcast_group(&self, _: Epoch, author: &NodeIdST<ST>) -> &Vec<NodeIdST<ST>> {
        self.curr_raptorcast_groups
            .get(author)
            .unwrap_or(&self.EMPTY_GROUP)
    }

    // Called when group invite or group confirmation is received from validator
    // TODO: Remember to consume confirm_msg.name_records
    pub fn on_receive_group_message(
        &mut self,
        msg: FullNodesGroupMsg<ST>,
    ) -> Option<(FullNodesGroupMsg<ST>, NodeIdST<ST>)> {
        match msg {
            //-----------------------------
            // INVITE from validator
            //-----------------------------
            FullNodesGroupMessage::PrepareGroup(invite_msg) => {
                // Check the invite for duplicates & bandwidth requirements
                let mut accept = true;

                // Reject late round
                if invite_msg.start_round <= self.curr_round {
                    tracing::warn!(
                        "RaptorcastSecondary rejecting invite for round that \
                        already started, curr_round {:?}, invite = {:?}",
                        self.curr_round,
                        invite_msg
                    );
                    accept = false;
                }

                // Reject invite outside config invitation bounds
                if invite_msg.start_round < self.curr_round + self.config.invite_future_dist_min
                    || invite_msg.start_round > self.curr_round + self.config.invite_future_dist_max
                {
                    tracing::warn!(
                        "RaptorcastSecondary rejecting invite outside bounds \
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
                    if group.validator_id == invite_msg.validator_id {
                        tracing::warn!(
                            "RaptorcastSecondary received self-overlapping \
                            invite for rounds [{:?}, {:?}) from validator {:?}",
                            invite_msg.start_round,
                            invite_msg.end_round,
                            invite_msg.validator_id
                        );
                        accept = false;
                    }
                    // Check that we'll have enough bandwidth during round span
                    future_bandwidth += self.bandwidth_cost(group.all_peers.len());
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
                        "RaptorcastSecondary rejected invite for rounds \
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
                let response = monad_discovery::message::PrepareGroupResponse {
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
                        "RaptorcastSecondary ignoring late confirm, curr_round \
                        {:?}, confirm = {:?}",
                        self.curr_round,
                        confirm_msg
                    );
                    return None;
                }

                if let Some(invites) = self.pending_confirms.get_mut(start_round) {
                    if invites.contains_key(&confirm_msg.prepare.validator_id) {
                        let group = GroupAsClient {
                            start_round: confirm_msg.prepare.start_round,
                            end_round: confirm_msg.prepare.end_round,
                            validator_id: confirm_msg.prepare.validator_id,
                            all_peers: confirm_msg.peers,
                        };
                        self.confirmed_groups
                            .force_insert(group.start_round..group.end_round, group);
                        invites.remove(&confirm_msg.prepare.validator_id);
                    } else {
                        tracing::warn!(
                            "RaptorcastSecondary ignoring ConfirmGroup with \
                            mismatched prepare group: {:?}",
                            confirm_msg
                        );
                    }
                } else {
                    tracing::warn!(
                        "RaptorcastSecondary Ignoring confirmation message for \
                        unknown prepare group: {:?}",
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

    fn overlaps(bgn: Round, end: Round, group: &PrepareGroupMsg<ST>) -> bool {
        assert!(bgn <= end);
        assert!(group.start_round <= group.end_round);
        group.start_round < end && group.end_round > bgn
    }
}

struct GroupAsClient<ST>
where
    ST: CertificateSignatureRecoverable,
{
    start_round: Round,           // inclusive
    end_round: Round,             // exclusive
    validator_id: NodeIdST<ST>,   // the raptorcast publisher for this group
    all_peers: Vec<NodeIdST<ST>>, // including self + author
}

// We're planning to merge monad-node (validator binary) and monad-full-node
// (full node binary), so it's possible for a node to switch between roles at
// runtime.
enum Role<ST>
where
    ST: CertificateSignatureRecoverable,
{
    Publisher(Publisher<ST>),
    Client(Client<ST>),
}

pub struct RaptorCastSecondary<ST, M, OM, SE>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes>,
    OM: Serializable<Bytes> + Into<M> + Clone,
{
    // Our id, regardless of role as publisher (validator) or client (full-node)
    node_id: NodeIdST<ST>,

    // Main state machine, depending on wether we are playing the publisher role
    // (i.e. we are a validator) or a client role (full-node raptor-casted to)
    // Represents only the group logic, excluding everything network related.
    role: Role<ST>,

    // Args for encoding outbound (validator -> full-node) messages
    signing_key: Arc<ST::KeyPairType>, // for re-signing app messages
    raptor10_redundancy: f64,

    // These are populated from inbound group confirmation messages.
    // Self SockAddr is removed.
    known_addresses: HashMap<NodeIdST<ST>, SocketAddr>,

    mtu: u16,
    dataplane: Arc<Mutex<Dataplane>>,
    pending_events: VecDeque<RaptorCastEvent<M::Event, CertificateSignaturePubKey<ST>>>,
    channel_from_primary: Receiver<FullNodesGroupMsg<ST>>,
    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    _phantom: PhantomData<(OM, SE)>,
}

impl<ST, M, OM, SE> RaptorCastSecondary<ST, M, OM, SE>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes>,
    OM: Serializable<Bytes> + Into<M> + Clone,
{
    pub fn new(
        config: &RaptorCastConfig<ST>,
        shared_dataplane: Arc<Mutex<Dataplane>>,
        channel_from_primary: Receiver<FullNodesGroupMsg<ST>>,
    ) -> Self {
        tracing::info!("NEW RAPTORCAST-SECONDARY");
        let node_id = NodeId::new(config.shared_key.pubkey());
        let sec_config = config.secondary_instance.clone();
        let mut raptor10_redundancy = 0.0;

        // Instantiate either publisher or client state machine
        let role = match sec_config {
            super::config::SecondaryRcModeConfig::Publisher(publisher_cfg) => {
                raptor10_redundancy = publisher_cfg.raptor10_redundancy;
                let publisher = Publisher::new(node_id, publisher_cfg);
                Role::Publisher(publisher)
            }
            super::config::SecondaryRcModeConfig::Client(client_cfg) => {
                let client = Client::new(node_id, client_cfg);
                Role::Client(client)
            }
            super::config::SecondaryRcModeConfig::None => panic!(
                "secondary_instance is not set in config during \
                    instantiation of RaptorCastSecondary"
            ),
        };

        Self {
            node_id,
            role,
            signing_key: config.shared_key.clone(),
            raptor10_redundancy,
            known_addresses: config.known_addresses.clone(),
            mtu: config.mtu,
            dataplane: shared_dataplane,
            pending_events: Default::default(),
            channel_from_primary,
            waker: None,
            metrics: Default::default(),
            _phantom: PhantomData,
        }
    }

    fn send_single_msg(&self, group_msg: FullNodesGroupMsg<ST>, dest_node: &NodeIdST<ST>) {
        let router_msg: OutboundRouterMessage<'_, OM, CertificateSignaturePubKey<ST>> =
            OutboundRouterMessage::FullNodesGroup(group_msg);
        let msg_bytes = router_msg.serialize(); // Bytes::new();
        let dest_addr: &SocketAddr = self
            .known_addresses
            .get(dest_node)
            .expect("RaptorCastSecondary tried to send to unknown node id");
        let stride = segment_size_for_mtu(self.mtu);
        let udp_msg = UnicastMsg {
            msgs: vec![(*dest_addr, msg_bytes)],
            stride,
        };
        self.dataplane.lock().unwrap().udp_write_unicast(udp_msg);
    }

    fn send_group_msg(&self, group_msg: FullNodesGroupMsg<ST>, dest_node_ids: FullNodesST<ST>) {
        let router_msg: OutboundRouterMessage<'_, OM, CertificateSignaturePubKey<ST>> =
            OutboundRouterMessage::FullNodesGroup(group_msg);
        let udp_msg = BroadcastMsg {
            stride: segment_size_for_mtu(self.mtu),
            payload: router_msg.serialize(),
            targets: dest_node_ids
                .list
                .iter()
                .filter_map(|node_id| self.known_addresses.get(node_id).cloned())
                .collect(),
        };
        self.dataplane.lock().unwrap().udp_write_broadcast(udp_msg);
    }
}

impl<ST, M, OM, SE> Executor for RaptorCastSecondary<ST, M, OM, SE>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes>,
    OM: Serializable<Bytes> + Into<M> + Clone,
{
    type Command = RouterCommand<CertificateSignaturePubKey<ST>, OM>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        tracing::info!("RaptorCastSecondary EXEC");

        for command in commands {
            match command {
                Self::Command::Publish { .. } => {
                    panic!("Command routed to secondary RaptorCast: Publish")
                }
                Self::Command::AddEpochValidatorSet { .. } => {
                    panic!("Command routed to secondary RaptorCast: AddEpochValidatorSet")
                }
                Self::Command::GetPeers { .. } => {
                    panic!("Command routed to secondary RaptorCast: GetPeers")
                }
                Self::Command::UpdatePeers { .. } => {
                    panic!("Command routed to secondary RaptorCast: UpdatePeers")
                }

                Self::Command::UpdateCurrentRound(_epoch, round) => match &mut self.role {
                    Role::Client(client) => {
                        client.enter_round(round);
                    }
                    Role::Publisher(publisher) => {
                        if let Some((group_msg, fn_set)) =
                            publisher.enter_round_and_step_until(round)
                        {
                            self.send_group_msg(group_msg, fn_set);
                        }
                    }
                },

                Self::Command::GetFullNodes => {
                    // List of full nodes is dynamic, so we return the currently accepted group
                    match &mut self.role {
                        Role::Client(_) => {}
                        Role::Publisher(publisher) => {
                            let curr_group = publisher.get_current_raptorcast_group();
                            self.pending_events
                                .push_back(RaptorCastEvent::PeerManagerResponse(
                                    PeerManagerResponse::FullNodes(curr_group.view().clone()),
                                ));
                            if let Some(waker) = self.waker.take() {
                                waker.wake();
                            }
                        }
                    }
                }

                Self::Command::UpdateFullNodes(new_set_of_always_ask_nodes) => {
                    // Are these full nodes coming from control panel?
                    match &mut self.role {
                        Role::Client(_) => {
                            tracing::warn!(
                                "Ignoring UpdateFullNode, since we \
                                are running as a full-node already"
                            );
                        }
                        Role::Publisher(publisher) => {
                            publisher.update_always_ask_full_nodes(FullNodes::new(
                                new_set_of_always_ask_nodes,
                            ));
                        }
                    }
                } // PublishToFullNodes changes will be merged in the next PR
                  /*
                  Self::Command::PublishToFullNodes { target, message } => {
                      tracing::info!("RaptorCastSecondary EXEC PublishToFullNodes");

                      let app_message = message.serialize();
                      let app_message_len = app_message.len();

                      let _timer = DropTimer::start(Duration::from_millis(20), |elapsed| {
                          tracing::warn!(?elapsed, app_message_len, "long time to publish message")
                      });

                      let curr_group = match &mut self.role {
                          Role::Client(_) => {continue;},
                          Role::Publisher(publisher) =>
                          {
                              publisher.get_current_raptorcast_group()
                          }
                      };

                      let udp_build = |epoch: &Epoch,
                                      build_target: BuildTarget<ST>,
                                      app_message: Bytes|
                      -> UnicastMsg {
                          let segment_size = segment_size_for_mtu(self.mtu);

                          let unix_ts_ms = std::time::UNIX_EPOCH
                              .elapsed()
                              .expect("time went backwards")
                              .as_millis()
                              .try_into()
                              .expect("unix epoch doesn't fit in u64");

                          let messages = udp::build_messages::<ST>(
                              &self.signing_key,
                              segment_size,
                              app_message,
                              self.raptor10_redundancy,
                              epoch.0,
                              unix_ts_ms,
                              build_target,
                              &self.known_addresses,
                          );

                          UnicastMsg {
                              msgs: messages,
                              stride: segment_size,
                          }
                      };

                      match target {
                          RouterTarget::Broadcast(epoch) | RouterTarget::Raptorcast(epoch) => {
                              tracing::info!("RaptorCastSecondary EXEC Publish Broadcast");

                              // Temporary hack to avoid large changes in udp_write_unicast()
                              // epoch_validators_view is only used for prioritizing by stake
                              let mut epoch_validators = EpochValidators::new_from_full_nodes(curr_group);
                              let epoch_validators_view = epoch_validators.view_without(Vec::new());
                              let empty_fn = FullNodes::default();

                              // Create the UDP message(s) to send to full_nodes_view
                              let build_target = match &target {
                                  RouterTarget::Broadcast(_) => {
                                      BuildTarget::Broadcast(epoch_validators_view)
                                  }
                                  RouterTarget::Raptorcast(_) => BuildTarget::Raptorcast((
                                      epoch_validators_view,
                                      empty_fn.view()
                                  )),
                                  _ => unreachable!(),
                              };

                              // Send the UDP datagrams composing the Raptorcast
                              self.dataplane.lock().unwrap().udp_write_unicast(udp_build(
                                  &epoch,
                                  build_target,
                                  app_message,
                              ));
                          }
                          RouterTarget::PointToPoint(..) => {},
                          RouterTarget::TcpPointToPoint {..} => {},
                      };
                  }
                  */
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}
impl<ST, M, OM, E> Stream for RaptorCastSecondary<ST, M, OM, E>
where
    ST: CertificateSignatureRecoverable,
    M: Message<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Deserializable<Bytes>,
    OM: Serializable<Bytes> + Into<M> + Clone,
    E: From<RaptorCastEvent<M::Event, CertificateSignaturePubKey<ST>>>,
    Self: Unpin,
{
    type Item = E;

    // Since we are sending to full-nodes only, and not receiving anything from them,
    // we don't need to handle any receive here and this is just to satisfy traits
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }

        if let Some(event) = this.pending_events.pop_front() {
            return Poll::Ready(Some(event.into()));
        }

        match this.channel_from_primary.try_recv() {
            Ok(msg) => match &mut this.role {
                Role::Publisher(publisher) => {
                    publisher.on_candidate_response(msg);
                }
                Role::Client(client) => {
                    client.on_receive_group_message(msg);
                }
            },
            Err(err) => {
                if let std::sync::mpsc::TryRecvError::Disconnected = err {
                    tracing::error!("RaptorCastSecondary channel disconnected.");
                }
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::min, io, sync::Once};

    use iset::{interval_map, IntervalMap};
    use monad_secp::SecpSignature;
    use monad_testutil::signing::get_key;
    use tracing_subscriber::fmt::format::FmtSpan;

    use super::*;

    type SignatureType = SecpSignature;
    type PubKeyType = CertificateSignaturePubKey<SignatureType>;
    type ST = SignatureType;

    // Creates a node id that we can refer to as just from its seed
    fn nid(seed: u64) -> NodeId<PubKeyType> {
        let key_pair = get_key::<SignatureType>(seed);
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
    ) -> FullNodesGroupMsg<SignatureType> {
        let req = monad_discovery::message::PrepareGroup {
            validator_id,
            max_group_size: cfg.max_group_size,
            start_round,
            end_round: start_round + cfg.round_span,
        };

        let response_data = monad_discovery::message::PrepareGroupResponse {
            req,
            node_id: full_node_id,
            accept,
        };

        FullNodesGroupMessage::PrepareGroupResponse(response_data)
    }

    // Prints the internal state of a group - just the parts relevant to tests
    fn dump_group_state(grp: &GroupAsPublisher<SignatureType>) -> String {
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

    // Prints the internal state of a publisher; just the parts relevant to test
    fn dump_pub_sched(pbl: &Publisher<SignatureType>) -> String {
        let mut res = String::new();
        res += format!("last_seen_round=[{:?}]", pbl.curr_round).as_str();
        res += format!("\n  curr  {}", dump_group_state(&pbl.curr_group)).as_str();
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
        let always_ask_full_nodes = FullNodesST::<SignatureType> {
            list: node_ids_vec![10, 11],
        };
        let peer_disc_full_nodes = FullNodesST::<SignatureType> {
            list: node_ids_vec![12, 13, 14, 15],
        };

        let num_always_ask = always_ask_full_nodes.list.len();
        let num_peer_disc = peer_disc_full_nodes.list.len();
        let num_total = num_always_ask + num_peer_disc;

        let mut rng = ChaCha8Rng::seed_from_u64(42);

        let group_a: GroupAsPublisher<SignatureType> = GroupAsPublisher::new_randomized(
            Round(0),
            Round(1),
            &mut rng,
            &always_ask_full_nodes,
            &peer_disc_full_nodes,
        );

        let group_b: GroupAsPublisher<SignatureType> = GroupAsPublisher::new_randomized(
            Round(0),
            Round(1),
            &mut rng,
            &always_ask_full_nodes,
            &peer_disc_full_nodes,
        );

        let group_c: GroupAsPublisher<SignatureType> = GroupAsPublisher::new_randomized(
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
            tracing_subscriber::fmt::fmt()
                .with_max_level(tracing::Level::ERROR)
                .with_writer(io::stdout)
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .with_span_events(FmtSpan::CLOSE)
                .init();
        });
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
        // |       get_current_raptorcast_group()
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

        let mut v0_fsm: Publisher<SignatureType> = Publisher::new(
            v0_node_id,
            RaptorCastConfigSecondaryPublisher {
                full_nodes_prioritized: vec![nid(10), nid(11)],
                group_scheduling: sched_cfg,
                raptor10_redundancy: 2.0,
            },
        );

        // We should be able to call this during the initial state.
        // Note that although we have some initial always_ask_full_nodes, we
        // haven't invited them yet to any group, so current group must be empty
        assert_eq!(v0_fsm.get_current_raptorcast_group().view().len(), 0);

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
        assert_eq!(v0_fsm.get_current_raptorcast_group().view().len(), 0);

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
        assert_eq!(v0_fsm.get_current_raptorcast_group().view().len(), 0);

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
        assert_eq!(v0_fsm.get_current_raptorcast_group().view().len(), 0);

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
        assert_eq!(v0_fsm.get_current_raptorcast_group().view().len(), 0);

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
        assert_eq!(v0_fsm.get_current_raptorcast_group().view().len(), 0);

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
        assert_eq!(v0_fsm.get_current_raptorcast_group().view().len(), 0);

        // Replace always-ask full-nodes. Due to look-ahead of 8, this should
        // not affect groups 1, 2, since they were generated during rounds
        // 1 and 6, respectively. This should only affect group 3 (generated
        // during round 11, confirmed at round 15, used at round 18)
        v0_fsm.update_always_ask_full_nodes(FullNodes::new(node_ids_vec![16]));
        // Upserting peer-discovered full nodes that already are always-ask
        // should have no effect.
        v0_fsm.upsert_peer_disc_full_nodes(FullNodes::new(node_ids_vec![11, 16]));

        //-------------------------------------------------------------------[8]
        // 2nd group invites.t1; 1st raptorcast group available for use
        //----------------------------------------------------------------------
        let res = v0_fsm.enter_round_and_step_until(Round(8));

        // Finally, the Raptorcast group for 1st group1 @ rounds [8, 13)
        assert!(equal_node_vec(
            v0_fsm.get_current_raptorcast_group().view(),
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
            assert_eq!(invitees.list.len(), 3);
            assert!(equal_node_vec(&invitees.list, &node_ids_vec![16, 13, 12]));
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
    fn standalone_client_single_group() {
        enable_tracer();
        let mut clt = Client::<ST>::new(
            nid(10),
            RaptorCastConfigSecondaryClient {
                bandwidth_cost_per_group_member: 1,
                bandwidth_capacity: 5,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
            },
        );

        // Represents an invite message received from some validator
        let make_prep_data = |start_round: u64| monad_discovery::message::PrepareGroup {
            validator_id: nid(0),
            max_group_size: 2,
            start_round: Round(start_round),
            end_round: Round(start_round + 2),
        };

        let make_invite_msg =
            |start_round: u64| FullNodesGroupMessage::PrepareGroup(make_prep_data(start_round));

        let make_confirm_msg = |start_round: u64| {
            FullNodesGroupMessage::ConfirmGroup(monad_discovery::message::ConfirmGroup {
                prepare: make_prep_data(start_round),
                peers: node_ids_vec![10, 10 + start_round],
                name_records: None,
            })
        };

        //-------------------------------------------------------------------[1]
        clt.enter_round(Round(1));

        // If asked about some unknown group, return an empty set
        let rc_grp = &clt.get_curr_raptorcast_group(Epoch(0), &nid(99));
        assert_eq!(rc_grp.len(), 0);

        // Receive invite for round [5, 7). It should be accepted.
        let (group_msg, validator_id) = clt
            .on_receive_group_message(make_invite_msg(5))
            .expect("Client should have returned accept responses to be sent");
        assert_eq!(validator_id, nid(0));
        let FullNodesGroupMessage::PrepareGroupResponse(reply_msg) = group_msg else {
            panic!("Expected PrepareGroupResponse, got: {:?}", group_msg);
        };
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.node_id, nid(10));
        assert_eq!(reply_msg.req, make_prep_data(5));

        //-------------------------------------------------------------------[2]
        clt.enter_round(Round(2));

        // We accepted invite for round 5 but it's still only round 2.
        let rc_grp = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
        assert_eq!(rc_grp.len(), 0);

        //-------------------------------------------------------------------[3]
        clt.enter_round(Round(3));
        let rc_grp = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
        assert_eq!(rc_grp.len(), 0);

        // Receive invite for round [8, 10). It should be accepted.
        // Note we have a group-less gap [7, 8)
        let (group_msg, validator_id) = clt
            .on_receive_group_message(make_invite_msg(8))
            .expect("Client should have returned accept responses to be sent");
        assert_eq!(validator_id, nid(0));
        let FullNodesGroupMessage::PrepareGroupResponse(reply_msg) = group_msg else {
            panic!("Expected PrepareGroupResponse, got: {:?}", group_msg);
        };
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.node_id, nid(10));
        assert_eq!(reply_msg.req, make_prep_data(8));

        //-------------------------------------------------------------------[4]
        clt.enter_round(Round(4));
        let rc_grp = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
        assert_eq!(rc_grp.len(), 0);

        // This is the last opportunity to receive confirm for group 1.
        // Lets accept both here, out of order.
        assert_eq!(clt.pending_confirms.len(), 2);
        let res = clt.on_receive_group_message(make_confirm_msg(8));
        if !res.is_none() {
            panic!("Expected None from Client, got: {:?}", res);
        }
        let res = clt.on_receive_group_message(make_confirm_msg(5));
        if !res.is_none() {
            panic!("Expected None from Client, got: {:?}", res);
        }

        //-------------------------------------------------------------------[5]
        clt.enter_round(Round(5));

        // Now we should have a proper raptorcast group keyed on v0 (nid(0))
        let rc_grp = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));

        // We should have 2 peers in the group from v0 (nid_0): nid_10, nid_15
        assert!(equal_node_set(rc_grp, &node_ids![15, 10]));

        //-------------------------------------------------------------------[6]
        clt.enter_round(Round(6));

        // RC group from v0 should still be up
        let rc_grp = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![15, 10]));

        //-------------------------------------------------------------------[6]
        clt.enter_round(Round(7));

        // RC group from v0 should be down now, as it only covered rounds [5, 7)
        // Here we should see a group gap
        let rc_grp = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
        assert_eq!(rc_grp.len(), 0);

        //-------------------------------------------------------------------[8]
        clt.enter_round(Round(8));

        // We should now see the second group from v0 (nid_0): nid_10, nid_18
        let rc_grp = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
        assert!(equal_node_set(rc_grp, &node_ids![18, 10]));

        // Pending confirms are lazily cleaned up after the group starts.
        assert_eq!(clt.pending_confirms.len(), 0);
    }

    fn make_sched(round_span: Round) -> GroupSchedulingConfig {
        GroupSchedulingConfig {
            max_group_size: 3,
            round_span,
            invite_lookahead: Round(8),
            max_invite_wait: Round(1),
            deadline_round_dist: Round(1),
            init_empty_round_span: Round(4), // >= invite_wait + deadline
        }
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
        let mut clt = Client::<ST>::new(
            nid(me),
            RaptorCastConfigSecondaryClient {
                bandwidth_cost_per_group_member: 1,
                bandwidth_capacity: 6,
                invite_future_dist_min: Round(1),
                invite_future_dist_max: Round(100),
            },
        );

        // Represents an invite message received from some validator
        let invite_data =
            |start_round: u64, validator_id: u64| monad_discovery::message::PrepareGroup {
                validator_id: nid(validator_id),
                max_group_size: 2,
                start_round: Round(start_round),
                end_round: Round(start_round + 2),
            };

        let invite_msg = |start_round: u64, validator_id: u64| {
            FullNodesGroupMessage::PrepareGroup(invite_data(start_round, validator_id))
        };

        let confirm_msg = |start_round: u64, validator_id: u64| {
            FullNodesGroupMessage::ConfirmGroup(monad_discovery::message::ConfirmGroup {
                prepare: invite_data(start_round, validator_id),
                peers: node_ids_vec![me, me + start_round],
                name_records: None,
            })
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
        let (group_msg, validator_id) = clt
            .on_receive_group_message(invite_msg(8, 0))
            .expect("Client should have returned accept responses to be sent");
        assert_eq!(validator_id, nid(0));
        let FullNodesGroupMessage::PrepareGroupResponse(reply_msg) = group_msg else {
            panic!("Expected PrepareGroupResponse, got: {:?}", group_msg);
        };
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.req, invite_data(8, 0));
        assert!(clt.on_receive_group_message(confirm_msg(8, 0)).is_none());

        // Invite v1.0
        let (group_msg, validator_id) = clt
            .on_receive_group_message(invite_msg(9, 1))
            .expect("Client should have returned accept responses to be sent");
        assert_eq!(validator_id, nid(1));
        let FullNodesGroupMessage::PrepareGroupResponse(reply_msg) = group_msg else {
            panic!("Expected PrepareGroupResponse, got: {:?}", group_msg);
        };
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.req, invite_data(9, 1));
        assert!(clt.on_receive_group_message(confirm_msg(9, 1)).is_none());

        //-------------------------------------------------------------------[2]
        clt.enter_round(Round(2));

        //----------------------------+
        // Round    | Validator.Group |
        //----------------------------+
        // 10       |             v2.0
        // 11       | v0.1        v2.0
        // 12       | v0.1

        // Invite v2.0
        let (group_msg, validator_id) = clt
            .on_receive_group_message(invite_msg(me, 2))
            .expect("Client should have returned accept responses to be sent");
        assert_eq!(validator_id, nid(2));
        let FullNodesGroupMessage::PrepareGroupResponse(reply_msg) = group_msg else {
            panic!("Expected PrepareGroupResponse, got: {:?}", group_msg);
        };
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.req, invite_data(me, 2));
        assert!(clt.on_receive_group_message(confirm_msg(me, 2)).is_none());

        // Invite v0.1
        let (group_msg, validator_id) = clt
            .on_receive_group_message(invite_msg(11, 0))
            .expect("Client should have returned accept responses to be sent");
        assert_eq!(validator_id, nid(0));
        let FullNodesGroupMessage::PrepareGroupResponse(reply_msg) = group_msg else {
            panic!("Expected PrepareGroupResponse, got: {:?}", group_msg);
        };
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.req, invite_data(11, 0));
        assert!(clt.on_receive_group_message(confirm_msg(11, 0)).is_none());

        //-------------------------------------------------------------------[3]
        clt.enter_round(Round(3));

        //----------------------------+
        // Round    | Validator.Group |
        //----------------------------+
        // 14       | v0.2  v1.1  v2.1
        // 15       | v0.2  v1.1  v2.1
        // 16       | v0.2  v1.1  v2.1

        // Invite v0.2
        let (group_msg, validator_id) = clt
            .on_receive_group_message(invite_msg(14, 0))
            .expect("Client should have returned accept responses to be sent");
        assert_eq!(validator_id, nid(0));
        let FullNodesGroupMessage::PrepareGroupResponse(reply_msg) = group_msg else {
            panic!("Expected PrepareGroupResponse, got: {:?}", group_msg);
        };
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.req, invite_data(14, 0));
        assert!(clt.on_receive_group_message(confirm_msg(14, 0)).is_none());

        // Invite v1.1
        let (group_msg, validator_id) = clt
            .on_receive_group_message(invite_msg(14, 1))
            .expect("Client should have returned accept responses to be sent");
        assert_eq!(validator_id, nid(1));
        let FullNodesGroupMessage::PrepareGroupResponse(reply_msg) = group_msg else {
            panic!("Expected PrepareGroupResponse, got: {:?}", group_msg);
        };
        assert!(reply_msg.accept);
        assert_eq!(reply_msg.req, invite_data(14, 1));
        assert!(clt.on_receive_group_message(confirm_msg(14, 1)).is_none());

        // Invite v2.1
        let (group_msg, validator_id) = clt
            .on_receive_group_message(invite_msg(14, 2))
            .expect("Client should have returned accept responses to be sent");
        assert_eq!(validator_id, nid(2));
        let FullNodesGroupMessage::PrepareGroupResponse(reply_msg) = group_msg else {
            panic!("Expected PrepareGroupResponse, got: {:?}", group_msg);
        };
        assert_eq!(reply_msg.accept, clt.config.bandwidth_capacity >= 6);
        assert_eq!(reply_msg.req, invite_data(14, 2));
        assert!(clt.on_receive_group_message(confirm_msg(14, 2)).is_none());

        //-------------------------------------------------------------------[4]
        clt.enter_round(Round(4));
        // We shouldn't have a raptorcast group yet, first one starts at round 8
        for vid in 0..3 {
            let rc_grp = clt.get_curr_raptorcast_group(Epoch(0), &nid(vid));
            assert_eq!(rc_grp.len(), 0);
        }

        //-----------------------------------------------------------------[5-7]
        clt.enter_round(Round(5));
        clt.enter_round(Round(6));
        clt.enter_round(Round(7));
        // Should still not have any raptorcast group
        for vid in 0..3 {
            let rc_grp = clt.get_curr_raptorcast_group(Epoch(0), &nid(vid));
            assert_eq!(rc_grp.len(), 0);
        }

        //-------------------------------------------------------------------[8]
        clt.enter_round(Round(8));

        // Raptorcast groups keyed on v1 and v2 should still be empty
        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(1));
        assert_eq!(rc.len(), 0);
        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(2));
        assert_eq!(rc.len(), 0);
        // Group keyed on unknown node should also be empty
        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(99));
        assert_eq!(rc.len(), 0);

        // Now we should have raptorcast group v0.0 only: nid_10, nid_15
        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
        assert!(equal_node_set(rc, &node_ids![18, me]));

        //-------------------------------------------------------------------[9]
        clt.enter_round(Round(9));

        // v2 groups should still be invisible
        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(2));
        assert_eq!(rc.len(), 0);

        // We should still have raptorcast group v0.0
        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
        assert!(equal_node_set(rc, &node_ids![18, me]));

        // We should now also have raptorcast group v1.0
        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(1));
        assert!(equal_node_set(rc, &node_ids![19, me]));

        //------------------------------------------------------------------[10]
        clt.enter_round(Round(10));

        //----------------------------+
        // Round    | Validator.Group |
        //----------------------------+
        // 10       |       v1.0  v2.0
        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
        assert_eq!(rc.len(), 0);

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(1));
        assert!(equal_node_set(rc, &node_ids![19, me]));

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(2));
        assert!(equal_node_set(rc, &node_ids![20, me]));

        //------------------------------------------------------------------[11]
        clt.enter_round(Round(11));

        //----------------------------+
        // Round    | Validator.Group |
        //----------------------------+
        // 11       | v0.1        v2.0

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
        assert!(equal_node_set(rc, &node_ids![21, me]));

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(1));

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(2));
        assert!(equal_node_set(rc, &node_ids![20, me]));

        //------------------------------------------------------------------[12]
        clt.enter_round(Round(12));

        //----------------------------+
        // Round    | Validator.Group |
        //----------------------------+
        // 12       | v0.1

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
        assert!(equal_node_set(rc, &node_ids![21, me]));

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(1));
        assert_eq!(rc.len(), 0);

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(2));
        assert_eq!(rc.len(), 0);

        //------------------------------------------------------------------[13]
        clt.enter_round(Round(13));

        //----------------------------+
        // Round    | Validator.Group |
        //----------------------------+
        // 13       |

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
        assert_eq!(rc.len(), 0);

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(1));
        assert_eq!(rc.len(), 0);

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(2));
        assert_eq!(rc.len(), 0);

        //---------------------------------------------------------------[14-15]

        for round in 14..=15 {
            //----------------------------+
            // Round    | Validator.Group |
            //----------------------------+
            // 14       | v0.2  v1.1  v2.1
            // 15       | v0.2  v1.1  v2.1
            clt.enter_round(Round(round));

            let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
            assert!(equal_node_set(rc, &node_ids![24, me]));

            let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(1));
            assert!(equal_node_set(rc, &node_ids![24, me]));

            let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(2));
            assert!(equal_node_set(rc, &node_ids![24, me]));
        }

        //------------------------------------------------------------------[16]
        clt.enter_round(Round(16));

        //----------------------------+
        // Round    | Validator.Group |
        //----------------------------+
        // 16       |

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(0));
        assert_eq!(rc.len(), 0);

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(1));
        assert_eq!(rc.len(), 0);

        let rc = clt.get_curr_raptorcast_group(Epoch(0), &nid(2));
        assert_eq!(rc.len(), 0);

        // Pending confirms are lazily cleaned up after the group starts.
        assert_eq!(clt.pending_confirms.len(), 0);
    }
}
