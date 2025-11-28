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
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    net::SocketAddrV4,
    time::Duration,
};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::ExecutorMetrics;
use monad_executor_glue::PeerEntry;
use monad_types::{Epoch, NodeId, Round};
use rand::{RngCore, seq::IteratorRandom};
use rand_chacha::ChaCha8Rng;
use tracing::{debug, info, trace, warn};

use crate::{
    MonadNameRecord, NameRecord, PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder, PeerDiscoveryCommand,
    PeerDiscoveryEvent, PeerDiscoveryMessage, PeerDiscoveryMetricsCommand,
    PeerDiscoveryTimerCommand, PeerLookupRequest, PeerLookupResponse, Ping, Pong, TimerKind,
    ipv4_validation::{IpCheckError, validate_socket_ipv4_address},
};

/// Maximum number of peers to be included in a PeerLookupResponse
const MAX_PEER_IN_RESPONSE: usize = 16;
/// Number of peers to send lookup request to
const NUM_LOOKUP_PEERS: usize = 3;
/// Number of validators to connect to if self is a full node
// TODO: this should be configurable
const NUM_UPSTREAM_VALIDATORS: usize = 3;

/// Metrics constant
pub const GAUGE_PEER_DISC_SEND_PING: &str = "monad.peer_disc.send_ping";
pub const GAUGE_PEER_DISC_RECV_PING: &str = "monad.peer_disc.recv_ping";
pub const GAUGE_PEER_DISC_DROP_PING: &str = "monad.peer_disc.drop_ping";
pub const GAUGE_PEER_DISC_PING_TIMEOUT: &str = "monad.peer_disc.ping_timeout";
pub const GAUGE_PEER_DISC_SEND_PONG: &str = "monad.peer_disc.send_pong";
pub const GAUGE_PEER_DISC_RECV_PONG: &str = "monad.peer_disc.recv_pong";
pub const GAUGE_PEER_DISC_DROP_PONG: &str = "monad.peer_disc.drop_pong";
pub const GAUGE_PEER_DISC_SEND_LOOKUP_REQUEST: &str = "monad.peer_disc.send_lookup_request";
pub const GAUGE_PEER_DISC_RECV_LOOKUP_REQUEST: &str = "monad.peer_disc.recv_lookup_request";
pub const GAUGE_PEER_DISC_RECV_OPEN_LOOKUP_REQUEST: &str =
    "monad.peer_disc.recv_open_lookup_request";
pub const GAUGE_PEER_DISC_RECV_TARGETED_LOOKUP_REQUEST: &str =
    "monad.peer_disc.recv_targeted_lookup_request";
pub const GAUGE_PEER_DISC_RETRY_LOOKUP_REQUEST: &str = "monad.peer_disc.retry_lookup_request";
pub const GAUGE_PEER_DISC_SEND_LOOKUP_RESPONSE: &str = "monad.peer_disc.send_lookup_response";
pub const GAUGE_PEER_DISC_RECV_LOOKUP_RESPONSE: &str = "monad.peer_disc.recv_lookup_response";
pub const GAUGE_PEER_DISC_DROP_LOOKUP_RESPONSE: &str = "monad.peer_disc.drop_lookup_response";
pub const GAUGE_PEER_DISC_LOOKUP_TIMEOUT: &str = "monad.peer_disc.lookup_timeout";
pub const GAUGE_PEER_DISC_SEND_RAPTORCAST_REQUEST: &str = "monad.peer_disc.send_raptorcast_request";
pub const GAUGE_PEER_DISC_RECV_RAPTORCAST_REQUEST: &str = "monad.peer_disc.recv_raptorcast_request";
pub const GAUGE_PEER_DISC_SEND_RAPTORCAST_RESPONSE: &str =
    "monad.peer_disc.send_raptorcast_response";
pub const GAUGE_PEER_DISC_RECV_RAPTORCAST_RESPONSE: &str =
    "monad.peer_disc.recv_raptorcast_response";
pub const GAUGE_PEER_DISC_REFRESH: &str = "monad.peer_disc.refresh";
pub const GAUGE_PEER_DISC_NUM_PEERS: &str = "monad.peer_disc.num_peers";
pub const GAUGE_PEER_DISC_NUM_PENDING_PEERS: &str = "monad.peer_disc.num_pending_peers";
pub const GAUGE_PEER_DISC_NUM_UPSTREAM_VALIDATORS: &str = "monad.peer_disc.num_upstream_validators";
pub const GAUGE_PEER_DISC_NUM_DOWNSTREAM_FULLNODES: &str =
    "monad.peer_disc.num_downstream_fullnodes";

/// validator role is given if the node is a validator in the current or next epoch.
/// this is to ensure the node starts connecting to other validators even if joining
/// as a validator only in the next epoch
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerDiscoveryRole {
    ValidatorNone,      // validator set as None in secondary raptorcast
    ValidatorPublisher, // validator set as Publisher in secondary raptorcast
    FullNodeNone,       // full node set as None in secondary raptorcast
    FullNodeClient,     // full node set as Client in secondary raptorcast
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionInfo<ST: CertificateSignatureRecoverable> {
    pub last_ping: Ping<ST>,
    pub unresponsive_pings: u32,
    pub name_record: MonadNameRecord<ST>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecondaryRaptorcastConnectionStatus {
    Connected,
    Pending,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecondaryRaptorcastInfo {
    pub status: SecondaryRaptorcastConnectionStatus,
    pub num_retries: u32,
    pub last_active: Round,
}

#[derive(Debug, Clone, Copy)]
pub struct LookupInfo<ST: CertificateSignatureRecoverable> {
    // current number of retries, once above unresponsive_prune_threshold, drop this request
    pub num_retries: u32,
    // receiver of the peer lookup request
    pub receiver: NodeId<CertificateSignaturePubKey<ST>>,
    // if set to true, peers should return additional nodes other than the target specified
    pub open_discovery: bool,
}

pub struct PeerDiscovery<ST: CertificateSignatureRecoverable> {
    pub self_id: NodeId<CertificateSignaturePubKey<ST>>,
    pub self_record: MonadNameRecord<ST>,
    // role of the node in the current epoch
    self_role: PeerDiscoveryRole,
    pub current_round: Round,
    pub current_epoch: Epoch,
    // mapping of epoch to validators in that epoch
    pub epoch_validators: BTreeMap<Epoch, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,
    // initial bootstrap peers set in config file
    pub initial_bootstrap_peers: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    // prioritized full nodes for secondary raptorcast, set in config file
    pub prioritized_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    // pinned full nodes contains dedicated and prioritized full nodes, and initial bootstrap peers that will not be pruned
    pub pinned_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    // mapping of node IDs to their corresponding name records
    pub routing_info: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, MonadNameRecord<ST>>,
    // mapping of node IDs to their participation info, used to track participation in raptorcast
    pub participation_info:
        BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, SecondaryRaptorcastInfo>,
    // mapping of node IDs to their connection info, used to track ping status
    // a node is inserted into pending_queue and only promoted to routing_info upon a successful ping pong roundtrip
    // this is to ensure the node is reachable at its ip address and port
    pub pending_queue: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, ConnectionInfo<ST>>,
    // mapping of lookup IDs to their corresponding lookup info, node will only entertain lookup response
    // that matches a local lookup ID
    pub outstanding_lookup_requests: HashMap<u32, LookupInfo<ST>>,
    pub metrics: ExecutorMetrics,
    // duration before checking min/max watermark and decide to look for more peers or prune peers
    pub refresh_period: Duration,
    // duration before outstanding pings and lookup requests are dropped
    pub request_timeout: Duration,
    // number of unresponsive pings allowed before dropping connection
    pub unresponsive_prune_threshold: u32,
    // number of rounds since last participation before pruning a full node
    pub last_participation_prune_threshold: Round,
    // minimum number of peers before actively sending peer lookup requests
    pub min_num_peers: usize,
    // maximum number of peers before pruning
    pub max_num_peers: usize,
    // maximum number of peers in a raptorcast group
    pub max_group_size: usize,
    // secondary raptorcast setting: enable publisher mode when self is a validator
    pub enable_publisher: bool,
    // secondary raptorcast setting: enable client mode when self is a full node
    pub enable_client: bool,
    pub rng: ChaCha8Rng,
}

pub struct PeerDiscoveryBuilder<ST: CertificateSignatureRecoverable> {
    pub self_id: NodeId<CertificateSignaturePubKey<ST>>,
    pub self_record: MonadNameRecord<ST>,
    pub current_round: Round,
    pub current_epoch: Epoch,
    pub epoch_validators: BTreeMap<Epoch, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,
    pub pinned_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    pub prioritized_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    pub bootstrap_peers: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, MonadNameRecord<ST>>,
    pub refresh_period: Duration,
    pub request_timeout: Duration,
    pub unresponsive_prune_threshold: u32,
    pub last_participation_prune_threshold: Round,
    pub min_num_peers: usize,
    pub max_num_peers: usize,
    pub max_group_size: usize,
    pub enable_publisher: bool,
    pub enable_client: bool,
    pub rng: ChaCha8Rng,
}

impl<ST: CertificateSignatureRecoverable> PeerDiscoveryAlgoBuilder for PeerDiscoveryBuilder<ST> {
    type PeerDiscoveryAlgoType = PeerDiscovery<ST>;

    fn build(
        self,
    ) -> (
        Self::PeerDiscoveryAlgoType,
        Vec<
            PeerDiscoveryCommand<<Self::PeerDiscoveryAlgoType as PeerDiscoveryAlgo>::SignatureType>,
        >,
    ) {
        debug!("initializing peer discovery");
        assert!(self.max_num_peers > self.min_num_peers);

        let is_current_epoch_validator = self
            .epoch_validators
            .get(&self.current_epoch)
            .is_some_and(|validators| validators.contains(&self.self_id));
        let self_role = match (
            is_current_epoch_validator,
            self.enable_publisher,
            self.enable_client,
        ) {
            (true, true, _) => {
                info!("Starting peer discovery as ValidatorPublisher");
                PeerDiscoveryRole::ValidatorPublisher
            }
            (true, false, _) => {
                info!("Starting peer discovery as ValidatorNone");
                PeerDiscoveryRole::ValidatorNone
            }
            (false, _, true) => {
                info!("Starting peer discovery as FullNodeClient");
                PeerDiscoveryRole::FullNodeClient
            }
            (false, _, false) => {
                info!("Starting peer discovery as FullNodeNone");
                PeerDiscoveryRole::FullNodeNone
            }
        };

        let mut state = PeerDiscovery {
            self_id: self.self_id,
            self_record: self.self_record,
            self_role,
            current_round: self.current_round,
            current_epoch: self.current_epoch,
            epoch_validators: self.epoch_validators,
            initial_bootstrap_peers: self
                .bootstrap_peers
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>(),
            prioritized_full_nodes: self.prioritized_full_nodes,
            pinned_full_nodes: self.pinned_full_nodes,
            routing_info: Default::default(),
            participation_info: Default::default(),
            pending_queue: Default::default(),
            outstanding_lookup_requests: Default::default(),
            metrics: Default::default(),
            refresh_period: self.refresh_period,
            request_timeout: self.request_timeout,
            unresponsive_prune_threshold: self.unresponsive_prune_threshold,
            last_participation_prune_threshold: self.last_participation_prune_threshold,
            min_num_peers: self.min_num_peers,
            max_num_peers: self.max_num_peers,
            max_group_size: self.max_group_size,
            enable_publisher: self.enable_publisher,
            enable_client: self.enable_client,
            rng: self.rng,
        };

        let mut cmds = Vec::new();
        self.bootstrap_peers
            .into_iter()
            .for_each(|(peer_id, name_record)| {
                if let Ok(cmds_from_insert) = state.insert_peer_to_pending(peer_id, name_record) {
                    cmds.extend(cmds_from_insert);
                }
            });

        cmds.extend(state.refresh());

        (state, cmds)
    }
}

impl<ST: CertificateSignatureRecoverable> PeerDiscovery<ST> {
    // schedule for ping timeout
    fn schedule_ping_timeout(
        &self,
        peer: NodeId<CertificateSignaturePubKey<ST>>,
        ping_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        vec![PeerDiscoveryCommand::TimerCommand(
            PeerDiscoveryTimerCommand::Schedule {
                node_id: peer,
                timer_kind: TimerKind::PingTimeout,
                duration: self.request_timeout,
                on_timeout: PeerDiscoveryEvent::PingTimeout { to: peer, ping_id },
            },
        )]
    }

    // clear ping timeout timer
    fn clear_ping_timeout(
        &self,
        peer: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        vec![PeerDiscoveryCommand::TimerCommand(
            PeerDiscoveryTimerCommand::ScheduleReset {
                node_id: peer,
                timer_kind: TimerKind::PingTimeout,
            },
        )]
    }

    // schedule for next refresh
    fn reset_refresh_timer(&self) -> Vec<PeerDiscoveryCommand<ST>> {
        vec![
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::ScheduleReset {
                node_id: self.self_id,
                timer_kind: TimerKind::Refresh,
            }),
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::Schedule {
                node_id: self.self_id,
                timer_kind: TimerKind::Refresh,
                duration: self.refresh_period,
                on_timeout: PeerDiscoveryEvent::Refresh,
            }),
        ]
    }

    // schedule for full node raptorcast request
    fn schedule_full_node_raptorcast_timeout(
        &self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        vec![
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::ScheduleReset {
                node_id: to,
                timer_kind: TimerKind::FullNodeRaptorcastRequest,
            }),
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::Schedule {
                node_id: to,
                timer_kind: TimerKind::FullNodeRaptorcastRequest,
                duration: self.request_timeout,
                on_timeout: PeerDiscoveryEvent::SendFullNodeRaptorcastRequest { to },
            }),
        ]
    }

    // schedule for lookup request timeout
    fn schedule_lookup_timeout(
        &self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
        lookup_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        vec![PeerDiscoveryCommand::TimerCommand(
            PeerDiscoveryTimerCommand::Schedule {
                node_id: to,
                timer_kind: TimerKind::RetryPeerLookup { lookup_id },
                duration: self.request_timeout,
                on_timeout: PeerDiscoveryEvent::PeerLookupTimeout {
                    to,
                    target,
                    lookup_id,
                },
            },
        )]
    }

    fn clear_connection_info(&mut self) {
        self.participation_info.iter_mut().for_each(|(_, info)| {
            info.status = SecondaryRaptorcastConnectionStatus::None;
        });
    }

    fn insert_peer_to_pending(
        &mut self,
        peer_id: NodeId<CertificateSignaturePubKey<ST>>,
        name_record: MonadNameRecord<ST>,
    ) -> Result<Vec<PeerDiscoveryCommand<ST>>, IpCheckError> {
        // make sure self record is never inserted
        if peer_id == self.self_id {
            return Ok(vec![]);
        }

        // check if the peer ip address is valid
        if let Err(err) = validate_socket_ipv4_address(
            &name_record.udp_address(),
            &self.self_record.udp_address(),
        ) {
            warn!(
                ?peer_id,
                ?name_record,
                "peer ip address failed check, ignoring"
            );
            return Err(err);
        }

        // only accept new name record or name record with higher sequence number
        if let Some(current_name_record) = self.routing_info.get(&peer_id) {
            if name_record.seq() <= current_name_record.seq() {
                // no updates are required, exit
                debug!(
                    ?peer_id,
                    ?name_record,
                    ?current_name_record,
                    "name record already exists with same or lower seq in routing info"
                );
                return Ok(vec![]);
            }
        }
        if let Some(info) = self.pending_queue.get(&peer_id) {
            if name_record.seq() <= info.name_record.seq() {
                // no updates are required, exit
                debug!(?peer_id, ?name_record, ?info.name_record, "name record already exists with same or lower seq in pending queue");
                return Ok(vec![]);
            }
        }

        // insert into pending queue and send ping
        let ping_msg = Ping {
            id: self.rng.next_u32(),
            local_name_record: self.self_record.clone(),
        };
        self.pending_queue.insert(peer_id, ConnectionInfo {
            last_ping: ping_msg.clone(),
            unresponsive_pings: 0,
            name_record: name_record.clone(),
        });
        self.metrics[GAUGE_PEER_DISC_NUM_PENDING_PEERS] = self.pending_queue.len() as u64;

        // send ping to the peer, which will also insert the peer into pending queue
        Ok(self.send_ping(peer_id, name_record.name_record, ping_msg))
    }

    fn remove_peer_from_pending(
        &mut self,
        peer_id: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        let mut cmds = Vec::new();

        // remove from pending queue
        self.pending_queue.remove(&peer_id);
        cmds.extend(self.clear_ping_timeout(peer_id));

        self.metrics[GAUGE_PEER_DISC_NUM_PENDING_PEERS] = self.pending_queue.len() as u64;

        cmds
    }

    fn promote_peer_to_routing_info(
        &mut self,
        peer: NodeId<CertificateSignaturePubKey<ST>>,
        name_record: MonadNameRecord<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        let mut cmds = Vec::new();

        self.routing_info.insert(peer, name_record);
        self.participation_info
            .entry(peer)
            .and_modify(|info| {
                if info.last_active < self.current_round {
                    info.last_active = self.current_round;
                }
            })
            .or_insert_with(|| SecondaryRaptorcastInfo {
                status: SecondaryRaptorcastConnectionStatus::None,
                num_retries: 0,
                last_active: self.current_round,
            });
        cmds.extend(self.remove_peer_from_pending(peer));
        self.metrics[GAUGE_PEER_DISC_NUM_PEERS] = self.routing_info.len() as u64;

        if self.self_role == PeerDiscoveryRole::FullNodeClient {
            cmds.extend(self.look_for_upstream_validators());
        }

        cmds
    }

    // for a public full node to look for upstream validators running as Publisher
    fn look_for_upstream_validators(&mut self) -> Vec<PeerDiscoveryCommand<ST>> {
        let mut cmds = Vec::new();

        // connected upstream validators that have not participated beyond threshold is set to None
        for (id, info) in self.participation_info.iter_mut() {
            if info.status == SecondaryRaptorcastConnectionStatus::Connected
                && self.current_round.max(info.last_active) - info.last_active
                    >= self.last_participation_prune_threshold
            {
                info.status = SecondaryRaptorcastConnectionStatus::None;
                debug!(
                    ?id,
                    "upstream validator has not participated, marking as None"
                );
            }
        }

        // full node will try to connect to NUM_UPSTREAM_VALIDATORS
        // if insufficient connected upstream validators, try to send pings to new validators
        let connected_validators = self
            .participation_info
            .iter()
            .filter(|(_, info)| info.status != SecondaryRaptorcastConnectionStatus::None)
            .map(|(id, _)| id)
            .collect::<HashSet<_>>();

        self.metrics[GAUGE_PEER_DISC_NUM_UPSTREAM_VALIDATORS] = connected_validators.len() as u64;

        let slots_to_fill = NUM_UPSTREAM_VALIDATORS.saturating_sub(connected_validators.len());
        if slots_to_fill > 0 {
            // when selecting new upstream validators, make sure they are not already currently connected validators
            // TODO: we should also prioritize validators that are not recently pruned for connection info for better heuristics
            let available_validators = self
                .routing_info
                .keys()
                .filter(|&node_id| {
                    !connected_validators.contains(node_id)
                        && self.check_current_epoch_validator(node_id)
                        && node_id != &self.self_id
                })
                .copied()
                .collect::<Vec<_>>();

            let new_upstream_validators = available_validators
                .iter()
                .choose_multiple(&mut self.rng, slots_to_fill);

            debug!(
                ?new_upstream_validators,
                "looking for upstream validators to connect to",
            );

            for validator in new_upstream_validators {
                // send ping to advertise name record and send full node raptorcast request
                cmds.push(PeerDiscoveryCommand::RouterCommand {
                    target: *validator,
                    message: PeerDiscoveryMessage::Ping(Ping {
                        id: self.rng.next_u32(),
                        local_name_record: self.self_record.clone(),
                    }),
                });
                cmds.extend(self.send_full_node_raptorcast_request(*validator));
            }
        }

        cmds
    }

    fn select_peers_to_lookup_from(&mut self) -> Vec<NodeId<CertificateSignaturePubKey<ST>>> {
        match self.self_role {
            // validators will lookup name records from any current peers
            PeerDiscoveryRole::ValidatorNone | PeerDiscoveryRole::ValidatorPublisher => self
                .routing_info
                .keys()
                .cloned()
                .choose_multiple(&mut self.rng, NUM_LOOKUP_PEERS),
            // public full nodes will lookup name records from connected upstream validators
            // fallback to all known peers if insufficient
            PeerDiscoveryRole::FullNodeClient => {
                let mut selected = self
                    .participation_info
                    .iter()
                    .filter(|(_, info)| {
                        info.status == SecondaryRaptorcastConnectionStatus::Connected
                    })
                    .map(|(id, _)| *id)
                    .choose_multiple(&mut self.rng, NUM_LOOKUP_PEERS);
                if selected.len() < NUM_LOOKUP_PEERS {
                    let needed = NUM_LOOKUP_PEERS - selected.len();
                    let fallback = self
                        .routing_info
                        .keys()
                        .filter(|id| !selected.contains(id))
                        .cloned()
                        .choose_multiple(&mut self.rng, needed);

                    selected.extend(fallback);
                }
                selected
            }
            // dedicated full nodes will lookup name records from their initial bootstrap peers (their whitelisted upstream)
            PeerDiscoveryRole::FullNodeNone => self
                .initial_bootstrap_peers
                .iter()
                .cloned()
                .choose_multiple(&mut self.rng, NUM_LOOKUP_PEERS),
        }
    }

    // a helper function to check if a node is a validator in the current epoch
    fn check_current_epoch_validator(
        &self,
        peer_id: &NodeId<CertificateSignaturePubKey<ST>>,
    ) -> bool {
        self.epoch_validators
            .get(&self.current_epoch)
            .is_some_and(|validators| validators.contains(peer_id))
    }

    // a helper function check if a node is a validator in the next epoch
    fn check_next_epoch_validator(&self, peer_id: &NodeId<CertificateSignaturePubKey<ST>>) -> bool {
        self.epoch_validators
            .get(&(self.current_epoch + Epoch(1)))
            .is_some_and(|validators| validators.contains(peer_id))
    }

    // a helper function to check if a node is a validator in the current or next epoch
    fn check_validator_membership(&self, peer_id: &NodeId<CertificateSignaturePubKey<ST>>) -> bool {
        self.check_current_epoch_validator(peer_id) || self.check_next_epoch_validator(peer_id)
    }

    // a helper function to check if a node is a validator or a pinned full node
    fn is_pinned_node(&self, peer_id: &NodeId<CertificateSignaturePubKey<ST>>) -> bool {
        self.check_validator_membership(peer_id) || self.pinned_full_nodes.contains(peer_id)
    }
}

impl<ST> PeerDiscoveryAlgo for PeerDiscovery<ST>
where
    ST: CertificateSignatureRecoverable,
{
    type SignatureType = ST;

    fn send_ping(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        name_record: NameRecord,
        ping: Ping<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, "sending ping request");

        let mut cmds = Vec::new();

        cmds.extend(self.schedule_ping_timeout(to, ping.id));

        cmds.push(PeerDiscoveryCommand::PingPongCommand {
            target: to,
            name_record,
            message: PeerDiscoveryMessage::Ping(ping),
        });

        self.metrics[GAUGE_PEER_DISC_SEND_PING] += 1;
        cmds
    }

    fn handle_ping(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        ping_msg: Ping<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?ping_msg, "handling ping request");
        self.metrics[GAUGE_PEER_DISC_RECV_PING] += 1;

        let mut cmds = Vec::new();

        // do not insert to pending queue if peer list is full and incoming ping is not from a validator or pinned full node
        // we still respond with pong even if peer list is full
        let mut peer_list_full = false;
        if self.routing_info.len() + self.pending_queue.len() >= self.max_num_peers
            && !self.is_pinned_node(&from)
            && !self.routing_info.contains_key(&from)
        {
            debug!(
                ?from,
                "peer list is full, not inserting name record from non-validator and non-pinned-full-node peer"
            );
            self.metrics[GAUGE_PEER_DISC_DROP_PING] += 1;
            peer_list_full = true;
        }

        if !peer_list_full {
            let peer_name_record = ping_msg.local_name_record.clone();
            if self
                .routing_info
                .get(&from)
                .is_none_or(|local| peer_name_record.seq() > local.seq())
            {
                let verified = peer_name_record
                    .recover_pubkey()
                    .is_ok_and(|recovered_node_id| recovered_node_id == from);

                if verified {
                    match self.insert_peer_to_pending(from, peer_name_record) {
                        Ok(cmds_from_insert) => cmds.extend(cmds_from_insert),
                        Err(_) => return cmds,
                    }
                } else {
                    debug!("invalid signature in ping.local_name_record");
                    return cmds;
                }
            } else if self
                .routing_info
                .get(&from)
                .is_some_and(|local| peer_name_record.seq() < local.seq())
            {
                warn!(
                    ?from,
                    "peer updated name record sequence number went backwards"
                );
                return cmds;
            } else if self.routing_info.get(&from).is_some_and(|local| {
                peer_name_record.seq() == local.seq() && peer_name_record != *local
            }) {
                warn!(
                    ?from,
                    "peer updated name record without bumping sequence number"
                );
                return cmds;
            }
        }

        // respond to ping
        let pong_msg = Pong {
            ping_id: ping_msg.id,
            local_record_seq: self.self_record.seq(),
        };
        cmds.push(PeerDiscoveryCommand::PingPongCommand {
            target: from,
            name_record: ping_msg.local_name_record.name_record,
            message: PeerDiscoveryMessage::Pong(pong_msg),
        });
        self.metrics[GAUGE_PEER_DISC_SEND_PONG] += 1;

        cmds
    }

    fn handle_pong(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        pong_msg: Pong,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?pong_msg, "handling pong response");
        self.metrics[GAUGE_PEER_DISC_RECV_PONG] += 1;

        let mut cmds = Vec::new();

        if let Some(info) = self.pending_queue.get(&from) {
            if info.last_ping.id == pong_msg.ping_id {
                // if ping id matches, promote peer to routing_info
                debug!(?from, ?info.name_record, "promoting peer to routing info");
                cmds.extend(self.promote_peer_to_routing_info(from, info.name_record.clone()));
            } else {
                debug!(?from, "dropping pong, ping id does not match");
                self.metrics[GAUGE_PEER_DISC_DROP_PONG] += 1;
            }
        } else {
            debug!(
                ?from,
                "dropping pong, ping sender does not exist in pending queue"
            );
            self.metrics[GAUGE_PEER_DISC_DROP_PONG] += 1;
        }

        cmds
    }

    fn handle_ping_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        ping_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, "ping timeout");
        let mut cmds = Vec::new();

        let Some(info) = self.pending_queue.get_mut(&to) else {
            debug!(
                ?to,
                "connection info not present locally, dropping ping timeout..."
            );
            return cmds;
        };

        // record timeout
        if info.last_ping.id == ping_id {
            debug!(?to, ?ping_id, "handling ping timeout");
            self.metrics[GAUGE_PEER_DISC_PING_TIMEOUT] += 1;
            info.unresponsive_pings += 1;

            // if unresponsive pings exceeds threshold, remove from pending queue
            if info.unresponsive_pings >= self.unresponsive_prune_threshold {
                debug!(
                    ?to,
                    "unresponsive pings exceeded threshold, dropping connection"
                );
                cmds.extend(self.remove_peer_from_pending(to));
            } else {
                // retry ping
                let name_record = info.name_record.name_record.clone();
                let ping = Ping {
                    id: self.rng.next_u32(),
                    local_name_record: self.self_record.clone(),
                };
                info.last_ping = ping.clone();
                cmds.extend(self.send_ping(to, name_record, ping));
            }
        }

        cmds
    }

    fn send_peer_lookup_request(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
        open_discovery: bool,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        let mut cmds = Vec::new();

        // new lookup request
        let mut lookup_id = self.rng.next_u32();
        // make sure lookup id is unique
        while self.outstanding_lookup_requests.contains_key(&lookup_id) {
            lookup_id = self.rng.next_u32();
        }
        debug!(?to, ?target, ?lookup_id, "sending peer lookup request");

        self.outstanding_lookup_requests
            .insert(lookup_id, LookupInfo {
                num_retries: 0,
                receiver: to,
                open_discovery,
            });
        let peer_lookup_request = PeerLookupRequest {
            lookup_id,
            target,
            open_discovery,
        };

        // schedule for peer lookup retry
        cmds.extend(self.schedule_lookup_timeout(to, target, lookup_id));

        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: to,
            message: PeerDiscoveryMessage::PeerLookupRequest(peer_lookup_request),
        });

        self.metrics[GAUGE_PEER_DISC_SEND_LOOKUP_REQUEST] += 1;
        cmds
    }

    fn handle_peer_lookup_request(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        request: PeerLookupRequest<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?request, "handling peer lookup request");
        self.metrics[GAUGE_PEER_DISC_RECV_LOOKUP_REQUEST] += 1;

        let mut cmds = Vec::new();
        let target = request.target;

        let mut name_records = if target == self.self_id {
            vec![self.self_record.clone()]
        } else {
            match self.routing_info.get(&target) {
                Some(name_record) => vec![name_record.clone()],
                None => vec![],
            }
        };

        // if open discovery, return more nodes other than requested nodes
        if request.open_discovery {
            self.metrics[GAUGE_PEER_DISC_RECV_OPEN_LOOKUP_REQUEST] += 1;
            // return random subset of validators (current and next epoch) up to MAX_PEER_IN_RESPONSE
            let validators: BTreeMap<_, _> = self
                .routing_info
                .iter()
                .filter(|(node_id, _)| self.check_validator_membership(node_id))
                .collect();
            name_records.extend(
                validators
                    .iter()
                    .choose_multiple(
                        &mut self.rng,
                        MAX_PEER_IN_RESPONSE.saturating_sub(name_records.len()),
                    )
                    .into_iter()
                    .map(|(_, name_record)| (*name_record).clone()),
            );
        } else {
            self.metrics[GAUGE_PEER_DISC_RECV_TARGETED_LOOKUP_REQUEST] += 1;
        }

        let peer_lookup_response = PeerLookupResponse {
            lookup_id: request.lookup_id,
            target,
            name_records,
        };

        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: from,
            message: PeerDiscoveryMessage::PeerLookupResponse(peer_lookup_response),
        });

        self.metrics[GAUGE_PEER_DISC_SEND_LOOKUP_RESPONSE] += 1;
        cmds
    }

    fn handle_peer_lookup_response(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        response: PeerLookupResponse<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?response, "handling peer lookup response");
        self.metrics[GAUGE_PEER_DISC_RECV_LOOKUP_RESPONSE] += 1;

        let mut cmds = Vec::new();

        // if lookup id is not in outstanding requests, drop the response
        if self
            .outstanding_lookup_requests
            .get(&response.lookup_id)
            .is_none_or(|peer| peer.receiver != from)
        {
            warn!(
                ?response,
                "peer lookup response not in outstanding requests, dropping response..."
            );
            self.metrics[GAUGE_PEER_DISC_DROP_LOOKUP_RESPONSE] += 1;
            return cmds;
        }

        if response.name_records.len() > MAX_PEER_IN_RESPONSE {
            warn!(
                ?response,
                "response includes number of peers larger than max, dropping response..."
            );
            self.metrics[GAUGE_PEER_DISC_DROP_LOOKUP_RESPONSE] += 1;
            return cmds;
        }

        // insert peer to pending queue
        for name_record in response.name_records {
            // verify signature of name record
            let node_id = match name_record.recover_pubkey() {
                Ok(node_id) => node_id,
                Err(e) => {
                    warn!(?e, "invalid name record signature, dropping record...");
                    continue;
                }
            };

            match self.insert_peer_to_pending(node_id, name_record) {
                Ok(cmds_from_insert) => cmds.extend(cmds_from_insert),
                Err(_) => continue,
            }
        }

        // drop from outstanding requests
        self.outstanding_lookup_requests.remove(&response.lookup_id);

        cmds
    }

    fn handle_peer_lookup_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
        lookup_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        trace!(?to, "peer lookup request timeout");

        let mut cmds = Vec::new();
        let Some(lookup_info) = self.outstanding_lookup_requests.get_mut(&lookup_id) else {
            return cmds;
        };

        // retry lookup request
        debug!(
            ?to,
            ?target,
            ?lookup_id,
            "handling peer lookup request timeout"
        );
        self.metrics[GAUGE_PEER_DISC_LOOKUP_TIMEOUT] += 1;
        if lookup_info.num_retries >= self.unresponsive_prune_threshold {
            debug!(
                ?lookup_id,
                ?to,
                ?target,
                "peer lookup request exceeded number of retries, dropping..."
            );
            self.outstanding_lookup_requests.remove(&lookup_id);
            return cmds;
        }
        let open_discovery = lookup_info.open_discovery;
        let num_retries = lookup_info.num_retries + 1;

        // generate new unique lookup id
        self.outstanding_lookup_requests.remove(&lookup_id);
        let mut new_lookup_id = self.rng.next_u32();
        while self
            .outstanding_lookup_requests
            .contains_key(&new_lookup_id)
        {
            new_lookup_id = self.rng.next_u32();
        }
        self.outstanding_lookup_requests
            .insert(new_lookup_id, LookupInfo {
                num_retries,
                receiver: to,
                open_discovery,
            });

        let peer_lookup_request = PeerLookupRequest {
            lookup_id: new_lookup_id,
            target,
            open_discovery,
        };

        // schedule for next peer lookup retry
        cmds.extend(self.schedule_lookup_timeout(to, target, new_lookup_id));

        self.metrics[GAUGE_PEER_DISC_SEND_LOOKUP_REQUEST] += 1;
        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: to,
            message: PeerDiscoveryMessage::PeerLookupRequest(peer_lookup_request),
        });

        debug!(
            ?to,
            ?target,
            ?new_lookup_id,
            "rescheduling peer lookup request"
        );
        self.metrics[GAUGE_PEER_DISC_RETRY_LOOKUP_REQUEST] += 1;

        cmds
    }

    // a full node sends raptorcast request to a validator to get connected
    fn send_full_node_raptorcast_request(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, "sending full node raptorcast request");

        let mut cmds = Vec::new();

        // only a full node running as Client mode should send request
        if self.self_role != PeerDiscoveryRole::FullNodeClient {
            debug!("not running as FullNodeClient, skipping full node raptorcast request");
            return cmds;
        }

        // record participation info
        if let Some(info) = self.participation_info.get_mut(&to) {
            match info.status {
                SecondaryRaptorcastConnectionStatus::None => {
                    info.status = SecondaryRaptorcastConnectionStatus::Pending;
                    info.num_retries = 0;
                }
                SecondaryRaptorcastConnectionStatus::Pending => {
                    info.num_retries += 1;
                    if info.num_retries >= self.unresponsive_prune_threshold {
                        debug!(
                            ?to,
                            "full node raptorcast request exceeded number of retries, dropping..."
                        );
                        info.status = SecondaryRaptorcastConnectionStatus::None;
                        info.num_retries = 0;
                        cmds.extend(self.look_for_upstream_validators());
                        return cmds;
                    }
                }
                SecondaryRaptorcastConnectionStatus::Connected => {
                    debug!(?to, "already connected, skip sending request");
                    return cmds;
                }
            }
        } else {
            debug!("no participation info found");
            return cmds;
        }

        self.metrics[GAUGE_PEER_DISC_SEND_RAPTORCAST_REQUEST] += 1;
        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: to,
            message: PeerDiscoveryMessage::FullNodeRaptorcastRequest,
        });

        cmds.extend(self.schedule_full_node_raptorcast_timeout(to));

        cmds
    }

    fn handle_full_node_raptorcast_request(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, "handling full node raptorcast request");

        let mut cmds = Vec::new();
        self.metrics[GAUGE_PEER_DISC_RECV_RAPTORCAST_REQUEST] += 1;

        // drop request if not running as Publisher
        if self.self_role != PeerDiscoveryRole::ValidatorPublisher {
            debug!(
                ?from,
                "not running as ValidatorPublisher but receiving raptorcast request"
            );
            return cmds;
        }

        // drop request if incoming request is a public full node and max_group_size is already reached
        if !self.prioritized_full_nodes.contains(&from) {
            let connected_public_full_nodes = self
                .participation_info
                .iter()
                .filter(|(node_id, info)| {
                    info.status == SecondaryRaptorcastConnectionStatus::Connected
                        && !self.prioritized_full_nodes.contains(node_id)
                })
                .count();

            if connected_public_full_nodes + self.prioritized_full_nodes.len()
                >= self.max_group_size
            {
                debug!(
                    ?from,
                    "connected full nodes already exceeds max group size, dropping request"
                );
                return cmds;
            }
        }

        if let Some(info) = self.participation_info.get_mut(&from) {
            info.status = SecondaryRaptorcastConnectionStatus::Connected;
        } else {
            debug!("no participation info found for full node");
            return cmds;
        }

        // respond to full node raptorcast request
        self.metrics[GAUGE_PEER_DISC_SEND_RAPTORCAST_RESPONSE] += 1;
        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: from,
            message: PeerDiscoveryMessage::FullNodeRaptorcastResponse,
        });

        cmds
    }

    fn handle_full_node_raptorcast_response(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, "handling full node raptorcast response");

        let cmds = Vec::new();

        // only a full node running as Client mode should receive response
        if self.self_role != PeerDiscoveryRole::FullNodeClient {
            debug!("not running as FullNodeClient, dropping full node raptorcast response");
            return cmds;
        }

        self.metrics[GAUGE_PEER_DISC_RECV_RAPTORCAST_RESPONSE] += 1;

        // update secondary raptorcast node status
        if let Some(info) = self.participation_info.get_mut(&from) {
            if info.status == SecondaryRaptorcastConnectionStatus::Pending {
                debug!(
                    ?from,
                    "received full node raptorcast response, marking as connected"
                );
                info.status = SecondaryRaptorcastConnectionStatus::Connected;
                info.num_retries = 0;
            } else {
                debug!(?from, "unexpected full node raptorcast response");
            }
        } else {
            debug!("no participation info found for validator");
        }

        cmds
    }

    fn refresh(&mut self) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!("refreshing peer discovery");

        self.metrics[GAUGE_PEER_DISC_REFRESH] += 1;
        let mut cmds = Vec::new();

        // remove nodes that have not participated in secondary raptorcast beyond last_participation_prune_threshold
        let non_participating_nodes: Vec<_> = self
            .participation_info
            .iter()
            .filter_map(|(node_id, info)| {
                if self.current_round.max(info.last_active) - info.last_active
                    >= self.last_participation_prune_threshold
                {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in non_participating_nodes {
            if self.is_pinned_node(&node_id) {
                debug!(?node_id, "clearing participation info for pinned node");
                if let Some(info) = self.participation_info.get_mut(&node_id) {
                    info.status = SecondaryRaptorcastConnectionStatus::None;
                }
            } else {
                debug!(?node_id, "removing non-participating peer");
                self.participation_info.remove(&node_id);
                self.routing_info.remove(&node_id);
            }
        }

        // if number of peers above max number of peers, randomly choose a few full nodes and prune them from routing_info
        // validators and pinned full nodes will not be pruned
        if self.routing_info.len() > self.max_num_peers {
            let num_to_prune = self.routing_info.len() - self.max_num_peers;
            let excessive_full_nodes: Vec<_> = self
                .routing_info
                .keys()
                .filter(|node| !self.is_pinned_node(node))
                .cloned()
                .collect();

            let nodes_to_prune: Vec<_> = excessive_full_nodes
                .into_iter()
                .choose_multiple(&mut self.rng, num_to_prune);

            if nodes_to_prune.is_empty() {
                info!("more validators and pinned full nodes than max number of peers");
            } else {
                for node_id in nodes_to_prune {
                    debug!(?node_id, "pruning excessive full nodes");
                    self.participation_info.remove(&node_id);
                    self.routing_info.remove(&node_id);
                }
            }
        }
        trace!("Current routing info: {:?}", self.routing_info);
        trace!("Current pending queue: {:?}", self.pending_queue);

        // get missing validators in the current and next epoch
        let missing_validators = self
            .epoch_validators
            .get(&self.current_epoch)
            .into_iter()
            .flatten()
            .chain(
                self.epoch_validators
                    .get(&(self.current_epoch + Epoch(1)))
                    .into_iter()
                    .flatten(),
            )
            .filter(|validator| {
                !self.routing_info.contains_key(validator) && *validator != &self.self_id
            })
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .choose_multiple(&mut self.rng, NUM_LOOKUP_PEERS);

        let chosen_peers = self.select_peers_to_lookup_from();

        if self.routing_info.len() < self.min_num_peers {
            // if number of peers below the min number of peers, choose a few peers and do open discovery
            debug!(?chosen_peers, "discover more peers");

            for (validator_id, peer) in missing_validators.iter().zip(chosen_peers.iter()) {
                cmds.extend(self.send_peer_lookup_request(*peer, *validator_id, true));
            }
        } else if !missing_validators.is_empty() {
            // if number of peers already above the min number of peers, send targeted peer lookup request for missing validators
            for (validator_id, peer) in missing_validators.iter().zip(chosen_peers.iter()) {
                debug!(
                    ?validator_id,
                    "sending targeted peer lookup for missing validator"
                );
                cmds.extend(self.send_peer_lookup_request(*peer, *validator_id, false));
            }
        }

        // for full nodes, do an open discovery every refresh period to discover any validator name records update
        if self.self_role == PeerDiscoveryRole::FullNodeNone
            || self.self_role == PeerDiscoveryRole::FullNodeClient
        {
            if let Some(validators) = self.epoch_validators.get(&self.current_epoch) {
                if let Some(peer) = chosen_peers.last() {
                    if let Some(random_validator) = validators.iter().choose(&mut self.rng) {
                        trace!(
                            ?random_validator,
                            "full node sending open discovery to random validator"
                        );
                        cmds.extend(self.send_peer_lookup_request(*peer, *random_validator, true));
                    }
                }
            }
        }

        // if self is a full node, try to connect to a few current validators if not already connected
        // collect metrics
        if self.self_role == PeerDiscoveryRole::FullNodeClient {
            cmds.extend(self.look_for_upstream_validators());
        } else if self.self_role == PeerDiscoveryRole::ValidatorPublisher {
            let connected_public_full_nodes = self
                .participation_info
                .iter()
                .filter(|(_, info)| info.status == SecondaryRaptorcastConnectionStatus::Connected)
                .map(|(id, _)| id)
                .collect::<Vec<_>>();
            self.metrics[GAUGE_PEER_DISC_NUM_DOWNSTREAM_FULLNODES] =
                connected_public_full_nodes.len() as u64;
        }

        self.metrics[GAUGE_PEER_DISC_NUM_PEERS] = self.routing_info.len() as u64;
        self.metrics[GAUGE_PEER_DISC_NUM_PENDING_PEERS] = self.pending_queue.len() as u64;

        // reset timer to schedule for the next refresh
        cmds.extend(self.reset_refresh_timer());

        // export metrics
        cmds.push(PeerDiscoveryCommand::MetricsCommand(
            PeerDiscoveryMetricsCommand(self.metrics.clone()),
        ));

        cmds
    }

    fn update_current_round(
        &mut self,
        round: Round,
        epoch: Epoch,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        let cmds = Vec::new();

        if round > self.current_round {
            trace!(?round, "updating current round in peer discovery");
            self.current_round = round;
        }

        if epoch > self.current_epoch {
            debug!(?epoch, "updating current epoch in peer discovery");
            self.current_epoch = epoch;

            // when a full node is promoted to a validator
            // if publisher mode is enabled, switch role to ValidatorPublisher
            // if publisher mode is not enabled, switch role to ValidatorNone
            if (self.self_role == PeerDiscoveryRole::FullNodeNone
                || self.self_role == PeerDiscoveryRole::FullNodeClient)
                && self.check_current_epoch_validator(&self.self_id)
            {
                debug!(?epoch, ?self.enable_publisher, "full node promoted to validator");
                if self.enable_publisher {
                    self.self_role = PeerDiscoveryRole::ValidatorPublisher;
                } else {
                    self.self_role = PeerDiscoveryRole::ValidatorNone;
                }
                // clear secondary raptorcast connection info
                self.clear_connection_info();
            }
        }

        // clean up historical epoch validators
        self.epoch_validators
            .retain(|epoch, _| *epoch + Epoch(1) >= self.current_epoch);

        cmds
    }

    fn update_validator_set(
        &mut self,
        epoch: Epoch,
        validators: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(
            ?epoch,
            ?validators,
            "updating validator set in peer discovery"
        );

        let mut cmds = Vec::new();
        self.epoch_validators.insert(epoch, validators);

        // validator set update are done during epoch boundary block
        if epoch == self.current_epoch + Epoch(1) {
            let is_next_epoch_validator = self.check_next_epoch_validator(&self.self_id);

            // if a full node is going to be promoted to a validator in the next epoch
            // advertise self name record to the other validators in the next epoch
            if (self.self_role == PeerDiscoveryRole::FullNodeClient
                || self.self_role == PeerDiscoveryRole::FullNodeNone)
                && is_next_epoch_validator
            {
                debug!(?epoch, "sending pings to next epoch validators");
                let next_validators = self
                    .epoch_validators
                    .get(&epoch)
                    .cloned()
                    .unwrap_or_default();
                for validator in next_validators {
                    if self.routing_info.contains_key(&validator) {
                        // send ping to advertise name record
                        cmds.push(PeerDiscoveryCommand::RouterCommand {
                            target: validator,
                            message: PeerDiscoveryMessage::Ping(Ping {
                                id: self.rng.next_u32(),
                                local_name_record: self.self_record.clone(),
                            }),
                        });
                    }
                }
            }
            // validator is going to be demoted to a full node in the next epoch
            // if client mode is enabled, switch role to FullNodeClient and start looking for upstream validators
            // if client mode is not enabled, switch role to FullNodeNone
            else if (self.self_role == PeerDiscoveryRole::ValidatorNone
                || self.self_role == PeerDiscoveryRole::ValidatorPublisher)
                && !is_next_epoch_validator
            {
                debug!(?epoch, ?self.enable_client, "validator demoted to full node");
                if self.enable_client {
                    self.self_role = PeerDiscoveryRole::FullNodeClient;
                    self.clear_connection_info();
                    cmds.extend(self.look_for_upstream_validators());
                } else {
                    self.self_role = PeerDiscoveryRole::FullNodeNone;
                    self.clear_connection_info();
                }
            }
        }

        cmds
    }

    fn update_peers(&mut self, peers: Vec<PeerEntry<ST>>) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?peers, "updating peers");

        let mut cmds = Vec::new();

        for peer in peers {
            let node_id = NodeId::new(peer.pubkey);

            // verify signature of name record
            let name_record = MonadNameRecord {
                name_record: NameRecord::new(
                    *peer.addr.ip(),
                    peer.addr.port(),
                    peer.record_seq_num,
                ),
                signature: peer.signature,
            };
            let verified = name_record
                .recover_pubkey()
                .is_ok_and(|recovered_node_id| recovered_node_id == node_id);
            if verified {
                match self.insert_peer_to_pending(node_id, name_record) {
                    Ok(cmds_from_insert) => cmds.extend(cmds_from_insert),
                    Err(_) => continue,
                }
            } else {
                warn!(?node_id, "invalid name record signature");
            }
        }

        cmds
    }

    fn update_pinned_nodes(
        &mut self,
        dedicated_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
        prioritized_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(
            ?dedicated_full_nodes,
            ?prioritized_full_nodes,
            "updating pinned nodes"
        );

        self.pinned_full_nodes = dedicated_full_nodes
            .iter()
            .chain(prioritized_full_nodes.iter())
            .cloned()
            .collect();
        self.prioritized_full_nodes = prioritized_full_nodes;

        Vec::new()
    }

    fn update_peer_participation(
        &mut self,
        round: Round,
        peers: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?round, ?peers, "updating peer participation");

        let cmds = Vec::new();

        for peer in peers {
            if peer == self.self_id {
                continue; // skip self
            }
            self.participation_info
                .entry(peer)
                .and_modify(|info| {
                    if info.last_active < round {
                        info.last_active = round;
                    }
                })
                .or_insert_with(|| SecondaryRaptorcastInfo {
                    status: SecondaryRaptorcastConnectionStatus::None,
                    num_retries: 0,
                    last_active: round,
                });
        }

        cmds
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    fn get_pending_addr_by_id(
        &self,
        id: &NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Option<SocketAddrV4> {
        self.pending_queue
            .get(id)
            .map(|info| info.name_record.udp_address())
    }

    fn get_addr_by_id(&self, id: &NodeId<CertificateSignaturePubKey<ST>>) -> Option<SocketAddrV4> {
        self.routing_info
            .get(id)
            .map(|name_record| name_record.udp_address())
    }

    fn get_known_addrs(&self) -> HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddrV4> {
        self.routing_info
            .iter()
            .map(|(id, name_record)| (*id, name_record.udp_address()))
            .collect()
    }

    fn get_secondary_fullnodes(&self) -> Vec<NodeId<CertificateSignaturePubKey<ST>>> {
        self.routing_info
            .keys()
            .filter(|id| {
                self.participation_info
                    .get(id)
                    .is_some_and(|p| p.status == SecondaryRaptorcastConnectionStatus::Connected)
            })
            .copied()
            .collect()
    }

    fn get_name_records(
        &self,
    ) -> HashMap<NodeId<CertificateSignaturePubKey<ST>>, MonadNameRecord<ST>> {
        self.routing_info
            .iter()
            .map(|(id, name_record)| (*id, name_record.clone()))
            .collect()
    }

    fn get_name_record(
        &self,
        id: &NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Option<&MonadNameRecord<ST>> {
        self.routing_info.get(id)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddrV4};

    use alloy_rlp::Encodable;
    use monad_crypto::{
        NopKeyPair, NopSignature,
        certificate_signature::{CertificateKeyPair, CertificateSignature},
        signing_domain,
    };
    use monad_testutil::signing::create_keys;
    use monad_types::NodeId;
    use rand::SeedableRng;
    use test_case::test_case;

    use super::*;
    use crate::NameRecord;

    type KeyPairType = NopKeyPair;
    type SignatureType = NopSignature;

    const DUMMY_ADDR: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(1, 1, 1, 1), 8000);

    fn generate_name_record(keypair: &KeyPairType, seq_num: u64) -> MonadNameRecord<SignatureType> {
        let name_record = NameRecord::new(*DUMMY_ADDR.ip(), DUMMY_ADDR.port(), seq_num);
        let mut encoded = Vec::new();
        name_record.encode(&mut encoded);
        let signature = SignatureType::sign::<signing_domain::NameRecord>(&encoded, keypair);
        MonadNameRecord {
            name_record,
            signature,
        }
    }

    fn generate_test_state(
        self_key: &KeyPairType,
        peer_keys: Vec<&KeyPairType>,
    ) -> PeerDiscovery<SignatureType> {
        let routing_info = peer_keys
            .clone()
            .into_iter()
            .map(|key| {
                let node_id = NodeId::new(key.pubkey());
                let name_record = generate_name_record(key, 0);
                (node_id, name_record)
            })
            .collect::<BTreeMap<_, _>>();
        let participation_info = peer_keys
            .into_iter()
            .map(|key| {
                let node_id = NodeId::new(key.pubkey());
                let status = SecondaryRaptorcastConnectionStatus::None;
                (node_id, SecondaryRaptorcastInfo {
                    status,
                    num_retries: 0,
                    last_active: Round(0),
                })
            })
            .collect::<BTreeMap<_, _>>();

        PeerDiscovery {
            self_id: NodeId::new(self_key.pubkey()),
            self_record: generate_name_record(self_key, 0),
            self_role: PeerDiscoveryRole::FullNodeNone,
            current_round: Round(1),
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::new(),
            initial_bootstrap_peers: routing_info.keys().cloned().collect(),
            pinned_full_nodes: BTreeSet::new(),
            prioritized_full_nodes: BTreeSet::new(),
            routing_info,
            participation_info,
            pending_queue: BTreeMap::new(),
            outstanding_lookup_requests: HashMap::new(),
            metrics: ExecutorMetrics::default(),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            unresponsive_prune_threshold: 10,
            last_participation_prune_threshold: Round(5000),
            min_num_peers: 5,
            max_num_peers: 50,
            max_group_size: 50,
            enable_publisher: false,
            enable_client: false,
            rng: ChaCha8Rng::seed_from_u64(123456),
        }
    }

    fn extract_lookup_requests(
        cmds: Vec<PeerDiscoveryCommand<SignatureType>>,
    ) -> Vec<PeerLookupRequest<SignatureType>> {
        cmds.into_iter()
            .filter_map(|c| match c {
                PeerDiscoveryCommand::RouterCommand {
                    target: _target,
                    message: PeerDiscoveryMessage::PeerLookupRequest(request),
                } => Some(request),
                _ => None,
            })
            .collect::<Vec<_>>()
    }

    fn extract_lookup_responses(
        cmds: Vec<PeerDiscoveryCommand<SignatureType>>,
    ) -> Vec<PeerLookupResponse<SignatureType>> {
        cmds.into_iter()
            .filter_map(|c| match c {
                PeerDiscoveryCommand::RouterCommand {
                    target: _target,
                    message: PeerDiscoveryMessage::PeerLookupResponse(response),
                } => Some(response),
                _ => None,
            })
            .collect::<Vec<_>>()
    }

    fn extract_ping(
        cmds: Vec<PeerDiscoveryCommand<SignatureType>>,
    ) -> Vec<(
        NodeId<CertificateSignaturePubKey<SignatureType>>,
        Ping<SignatureType>,
    )> {
        cmds.into_iter()
            .filter_map(|c| match c {
                PeerDiscoveryCommand::PingPongCommand {
                    target,
                    name_record: _,
                    message: PeerDiscoveryMessage::Ping(ping),
                } => Some((target, ping)),
                _ => None,
            })
            .collect::<Vec<_>>()
    }

    fn extract_pong(cmds: Vec<PeerDiscoveryCommand<SignatureType>>) -> Vec<Pong> {
        cmds.into_iter()
            .filter_map(|c| match c {
                PeerDiscoveryCommand::PingPongCommand {
                    target: _,
                    name_record: _,
                    message: PeerDiscoveryMessage::Pong(pong),
                } => Some(pong),
                _ => None,
            })
            .collect::<Vec<_>>()
    }

    #[test]
    fn test_send_ping() {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let mut state = generate_test_state(peer0, vec![peer1]);

        // send ping to peer1
        let ping = Ping {
            id: 12345,
            local_name_record: state.self_record.clone(),
        };
        let peer1_name_record = generate_name_record(peer1, 1).name_record;
        let cmds = state.send_ping(peer1_pubkey, peer1_name_record, ping);

        // should send a ping command and schedule a ping timeout
        assert_eq!(cmds.len(), 2);
        assert!(matches!(
            cmds[0],
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::Schedule {
                timer_kind: TimerKind::PingTimeout,
                ..
            })
        ));
        assert!(matches!(cmds[1], PeerDiscoveryCommand::PingPongCommand {
            target: _,
            name_record: _,
            message: PeerDiscoveryMessage::Ping(_)
        }));
    }

    #[test]
    fn test_drop_pong_with_incorrect_ping_id() {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let mut state = generate_test_state(peer0, vec![peer1]);
        let last_ping = Ping {
            id: 12345,
            local_name_record: generate_name_record(peer0, 0),
        };
        state.pending_queue.insert(peer1_pubkey, ConnectionInfo {
            last_ping: last_ping.clone(),
            unresponsive_pings: 3,
            name_record: generate_name_record(peer1, 0),
        });

        // should not record pong when ping_id doesn't match
        state.handle_pong(peer1_pubkey, Pong {
            ping_id: 54321, // incorrect ping id,
            local_record_seq: 0,
        });
        let connection_info = state.pending_queue.get(&peer1_pubkey);
        assert!(connection_info.is_some());
        assert_eq!(connection_info.unwrap().last_ping, last_ping);
    }

    #[test]
    fn test_incoming_record() {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let mut state = generate_test_state(peer0, vec![]);

        let name_record = generate_name_record(peer1, 0);
        let ping = Ping {
            id: 12345,
            local_name_record: name_record.clone(),
        };
        let cmds = state.handle_ping(peer1_pubkey, ping);

        // should insert peer1 to pending queue and send ping, also respond with pong
        // 3 commands: one ping command, one pong command, and one timer command for ping timeout
        assert_eq!(cmds.len(), 3);
        let ping = extract_ping(cmds.clone());
        assert_eq!(ping.len(), 1);
        assert_eq!(ping[0].0, peer1_pubkey);
        assert_eq!(ping[0].1.local_name_record, state.self_record);

        let pong = extract_pong(cmds);
        assert_eq!(pong.len(), 1);
        assert_eq!(pong[0].ping_id, 12345);
        assert_eq!(pong[0].local_record_seq, state.self_record.seq());

        // added to pending queue but not yet to routing_info
        assert!(state.pending_queue.contains_key(&peer1_pubkey));
        assert!(!state.routing_info.contains_key(&peer1_pubkey));
        let connection_info = state.pending_queue.get(&peer1_pubkey).unwrap();
        assert_eq!(connection_info.last_ping, ping[0].1);

        // added to routing_info after receiving corresponding pong
        state.handle_pong(peer1_pubkey, Pong {
            ping_id: ping[0].1.id,
            local_record_seq: 0,
        });
        assert!(state.routing_info.contains_key(&peer1_pubkey));
        assert_eq!(state.routing_info.get(&peer1_pubkey).unwrap(), &name_record);
        assert!(!state.pending_queue.contains_key(&peer1_pubkey));
    }

    #[test]
    fn test_peer_lookup() {
        let keys = create_keys::<SignatureType>(3);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());

        let mut state = generate_test_state(peer0, vec![peer1]);

        let cmds = state.send_peer_lookup_request(peer1_pubkey, peer2_pubkey, false);

        // 2 commands: one timer command for peer lookup timeout, and one router command for peer lookup request
        assert_eq!(cmds.len(), 2);
        assert!(matches!(
            cmds[0],
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::Schedule {
                timer_kind: TimerKind::RetryPeerLookup { .. },
                ..
            })
        ));
        assert!(matches!(cmds[1], PeerDiscoveryCommand::RouterCommand {
            target: _,
            message: PeerDiscoveryMessage::PeerLookupRequest(_)
        }));

        // outstanding lookup requests created but not yet added to routing_info
        assert_eq!(state.outstanding_lookup_requests.keys().len(), 1);
        assert!(!state.routing_info.contains_key(&peer2_pubkey));
        let requests = extract_lookup_requests(cmds);
        let original_lookup_id = requests[0].lookup_id;
        assert_eq!(
            state
                .outstanding_lookup_requests
                .get(&original_lookup_id)
                .unwrap()
                .receiver,
            peer1_pubkey
        );

        // retry peer lookup request
        let cmds =
            state.handle_peer_lookup_timeout(peer1_pubkey, peer2_pubkey, requests[0].lookup_id);
        assert_eq!(cmds.len(), 2);
        assert_eq!(state.outstanding_lookup_requests.keys().len(), 1);
        assert!(!state.routing_info.contains_key(&peer2_pubkey));
        let requests = extract_lookup_requests(cmds);
        assert_eq!(requests.len(), 1);
        assert_ne!(original_lookup_id, requests[0].lookup_id); // new lookup id generated
        assert_eq!(
            state
                .outstanding_lookup_requests
                .get(&requests[0].lookup_id)
                .unwrap()
                .receiver,
            peer1_pubkey
        );

        let record = generate_name_record(peer2, 0);
        let cmds = state.handle_peer_lookup_response(peer1_pubkey, PeerLookupResponse {
            lookup_id: requests[0].lookup_id,
            target: peer2_pubkey,
            name_records: vec![record.clone()],
        });

        // peer2 should be added to pending queue and outstanding requests should be cleared
        assert!(state.pending_queue.contains_key(&peer2_pubkey));
        assert!(!state.routing_info.contains_key(&peer2_pubkey));
        assert_eq!(state.outstanding_lookup_requests.keys().len(), 0);

        // should send a ping to peer2 to establish a ping pong round trip before adding to routing_info
        let ping = extract_ping(cmds);
        assert_eq!(ping.len(), 1);
        assert_eq!(ping[0].0, peer2_pubkey);
        assert_eq!(ping[0].1.local_name_record, state.self_record);

        // peer2 should be added to routing_info after receiving corresponding pong
        state.handle_pong(peer2_pubkey, Pong {
            ping_id: ping[0].1.id,
            local_record_seq: 0,
        });
        assert!(state.routing_info.contains_key(&peer2_pubkey));
        assert_eq!(state.routing_info.get(&peer2_pubkey).unwrap(), &record);
        assert!(!state.pending_queue.contains_key(&peer2_pubkey));
    }

    #[test]
    fn test_peer_lookup_target_not_found() {
        let keys = create_keys::<SignatureType>(4);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());
        let peer3 = &keys[3];
        let peer3_pubkey = NodeId::new(peer3.pubkey());

        // routing_info contains peer1 and peer2
        let mut state = generate_test_state(peer0, vec![peer1, peer2]);
        state.self_role = PeerDiscoveryRole::ValidatorNone;
        state
            .epoch_validators
            .insert(Epoch(1), BTreeSet::from([peer1_pubkey, peer2_pubkey]));

        // receive a peer lookup request for peer3, which is not in routing_info
        let cmds = state.handle_peer_lookup_request(peer1_pubkey, PeerLookupRequest {
            lookup_id: 1,
            target: peer3_pubkey,
            open_discovery: true,
        });
        assert_eq!(cmds.len(), 1);

        // should return peer1 and peer2 instead
        let response = extract_lookup_responses(cmds);
        let response = response.first().unwrap();
        assert_eq!(response.lookup_id, 1);
        assert_eq!(response.target, peer3_pubkey);
        assert_eq!(response.name_records.len(), 2);
        let response_node_ids: Vec<_> = response
            .name_records
            .iter()
            .map(|record| record.recover_pubkey().unwrap())
            .collect();
        assert!(response_node_ids.contains(&peer1_pubkey));
        assert!(response_node_ids.contains(&peer2_pubkey));
    }

    #[test]
    fn test_update_name_record_sequence_number() {
        let keys = create_keys::<SignatureType>(3);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let mut state = generate_test_state(peer0, vec![peer1]);
        state.outstanding_lookup_requests.insert(1, LookupInfo {
            num_retries: 0,
            receiver: peer1_pubkey,
            open_discovery: false,
        });
        state.outstanding_lookup_requests.insert(2, LookupInfo {
            num_retries: 0,
            receiver: peer1_pubkey,
            open_discovery: false,
        });

        // should add to pending queue and send ping if record has higher sequence number (seq num incremented to 2)
        let record = generate_name_record(peer1, 2);
        let cmds = state
            .insert_peer_to_pending(peer1_pubkey, record.clone())
            .unwrap();
        let pings = extract_ping(cmds);
        assert_eq!(pings.len(), 1);
        assert_eq!(pings[0].0, peer1_pubkey);
        assert_eq!(pings[0].1.local_name_record, state.self_record);
        assert_eq!(
            state.pending_queue.get(&peer1_pubkey).unwrap(),
            &ConnectionInfo {
                last_ping: pings[0].1.clone(),
                unresponsive_pings: 0,
                name_record: record.clone(),
            }
        );
        assert_eq!(state.routing_info.get(&peer1_pubkey).unwrap().seq(), 0);

        // should not replace existing entry in pending queue if record has lower sequence number (seq num decremented to 1)
        let invalid_record = generate_name_record(peer1, 1);
        let cmds = state
            .insert_peer_to_pending(peer1_pubkey, invalid_record.clone())
            .unwrap();
        assert!(cmds.is_empty());

        // insert into routing info after ping pong round trip
        state.handle_pong(peer1_pubkey, Pong {
            ping_id: pings[0].1.id,
            local_record_seq: 0,
        });
        assert!(state.routing_info.contains_key(&peer1_pubkey));
        assert_eq!(state.routing_info.get(&peer1_pubkey).unwrap(), &record);
        assert!(!state.pending_queue.contains_key(&peer1_pubkey));

        // should not update name record if record has lower sequence number (seq num decremented to 1)
        let cmds = state
            .insert_peer_to_pending(peer1_pubkey, invalid_record)
            .unwrap();
        assert!(cmds.is_empty());
    }

    #[test]
    fn test_drop_invalid_lookup_response() {
        let keys = create_keys::<SignatureType>(3);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let mut state = generate_test_state(peer0, vec![]);

        // should not record peer lookup response if not in outstanding requests
        let record = generate_name_record(peer1, 0);
        let cmds = state.handle_peer_lookup_response(peer1_pubkey, PeerLookupResponse {
            lookup_id: 1,
            target: peer1_pubkey,
            name_records: vec![record],
        });
        assert!(cmds.is_empty());
        assert!(!state.pending_queue.contains_key(&peer1_pubkey));
        assert!(!state.routing_info.contains_key(&peer1_pubkey));
    }

    #[test]
    fn test_drop_lookup_response_that_exceeds_max_peers() {
        let keys = create_keys::<SignatureType>(3);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let lookup_id = 1;
        let mut state = generate_test_state(peer0, vec![]);
        state
            .outstanding_lookup_requests
            .insert(lookup_id, LookupInfo {
                num_retries: 0,
                receiver: peer1_pubkey,
                open_discovery: false,
            });

        // should not record peer lookup response if number of records exceed max
        let record = generate_name_record(peer1, 0);
        let cmds = state.handle_peer_lookup_response(peer1_pubkey, PeerLookupResponse {
            lookup_id,
            target: peer1_pubkey,
            name_records: vec![record; MAX_PEER_IN_RESPONSE + 1],
        });
        assert!(cmds.is_empty());
        assert!(!state.pending_queue.contains_key(&peer1_pubkey));
        assert!(!state.routing_info.contains_key(&peer1_pubkey));
    }

    #[test]
    fn test_prune_connections_and_lookup_request() {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let mut state = generate_test_state(peer0, vec![]);

        // add peer1 to pending queue with unresponsive pings
        let ping_id = 12345;
        let pending_queue = BTreeMap::from([(peer1_pubkey, ConnectionInfo {
            last_ping: Ping {
                id: ping_id,
                local_name_record: state.self_record.clone(),
            },
            unresponsive_pings: 5,
            name_record: generate_name_record(peer1, 0),
        })]);
        state.unresponsive_prune_threshold = 3;
        state.pending_queue = pending_queue;

        // ping timeout should remove peer1 from pending queue
        state.handle_ping_timeout(peer1_pubkey, ping_id);
        assert!(!state.pending_queue.contains_key(&peer1_pubkey));

        // add peer1 to outstanding lookup requests with num_retries above prune threshold
        let lookup_id = 1;
        state
            .outstanding_lookup_requests
            .insert(lookup_id, LookupInfo {
                num_retries: 5,
                receiver: peer1_pubkey,
                open_discovery: false,
            });

        // lookup request timeout should remove peer1 from outstanding requests
        state.handle_peer_lookup_timeout(peer1_pubkey, peer1_pubkey, lookup_id);
        assert!(state.outstanding_lookup_requests.is_empty());
    }

    #[test_case(PeerDiscoveryRole::FullNodeNone; "peer full node")]
    #[test_case(PeerDiscoveryRole::ValidatorNone; "peer validator")]
    fn test_non_participating_full_node(role: PeerDiscoveryRole) {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let mut state = generate_test_state(peer0, vec![peer1]);
        state.last_participation_prune_threshold = Round(10);
        if role == PeerDiscoveryRole::ValidatorNone {
            state
                .epoch_validators
                .insert(Epoch(1), BTreeSet::from([peer1_pubkey]));
        }
        state
            .participation_info
            .insert(peer1_pubkey, SecondaryRaptorcastInfo {
                status: SecondaryRaptorcastConnectionStatus::None,
                num_retries: 0,
                last_active: Round(1),
            });

        state.refresh();
        assert!(state.routing_info.contains_key(&peer1_pubkey));
        assert!(state.participation_info.contains_key(&peer1_pubkey));

        // round advances beyond last participation prune threshold
        state.update_current_round(Round(15), Epoch(1));
        state.refresh();
        match role {
            PeerDiscoveryRole::FullNodeNone => {
                // full node should be pruned
                assert!(!state.routing_info.contains_key(&peer1_pubkey));
                assert!(!state.participation_info.contains_key(&peer1_pubkey));
            }
            PeerDiscoveryRole::ValidatorNone => {
                // validator should not be pruned
                assert!(state.routing_info.contains_key(&peer1_pubkey));
                assert!(state.participation_info.contains_key(&peer1_pubkey));
            }
            _ => {}
        }
    }

    #[test]
    fn test_below_min_num_peers() {
        let keys = create_keys::<SignatureType>(3);
        let peer0 = &keys[0];
        let peer0_pubkey = NodeId::new(peer0.pubkey());
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());

        let mut state = generate_test_state(peer0, vec![peer1]);
        state.epoch_validators.insert(
            Epoch(1),
            BTreeSet::from([peer0_pubkey, peer1_pubkey, peer2_pubkey]),
        );

        // should send peer lookup request to peer1
        let cmds = state.refresh();
        let lookup_requests = extract_lookup_requests(cmds);
        assert_eq!(lookup_requests.len(), 2);
        let receiver = state
            .outstanding_lookup_requests
            .get(&lookup_requests[0].lookup_id)
            .unwrap()
            .receiver;
        assert_eq!(receiver, peer1_pubkey);
    }

    #[test]
    fn test_above_max_num_peers() {
        let keys = create_keys::<SignatureType>(4);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());
        let peer3 = &keys[3];
        let peer3_pubkey = NodeId::new(peer3.pubkey());

        // Peer1 in validator set, Peer2 is pinned full node
        let mut state = generate_test_state(peer0, vec![peer1, peer2, peer3]);
        state.min_num_peers = 0;
        state.max_num_peers = 1;
        state
            .epoch_validators
            .insert(Epoch(1), BTreeSet::from([peer1_pubkey]));
        state.pinned_full_nodes.insert(peer2_pubkey);

        // prune nodes, but validators and pinned full nodes are not pruned even if above max number of peers
        state.refresh();
        assert!(state.routing_info.contains_key(&peer1_pubkey));
        assert!(state.routing_info.contains_key(&peer2_pubkey));
        assert!(!state.routing_info.contains_key(&peer3_pubkey));
    }

    #[test]
    fn test_incoming_ping_above_max_num_peers() {
        let keys = create_keys::<SignatureType>(5);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());
        let peer3 = &keys[3];
        let peer3_pubkey = NodeId::new(peer3.pubkey());
        let peer4 = &keys[4];
        let peer4_pubkey = NodeId::new(peer4.pubkey());

        // max number of peers is 1, peer 1 is already in routing info occupying the slot
        let mut state = generate_test_state(peer0, vec![peer1]);
        state.min_num_peers = 0;
        state.max_num_peers = 1;
        state
            .epoch_validators
            .insert(Epoch(1), BTreeSet::from([peer2_pubkey]));
        state.pinned_full_nodes.insert(peer3_pubkey);

        // peer2 is a validator, it is added to pending queue although already exceeding max number of peers
        let cmds = state.handle_ping(peer2_pubkey, Ping {
            id: 2,
            local_name_record: generate_name_record(peer2, 0),
        });
        assert!(state.pending_queue.contains_key(&peer2_pubkey));
        let cmds = extract_ping(cmds);
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].0, peer2_pubkey);

        // peer3 is a pinned full node, it is also added to pending queue
        let cmds = state.handle_ping(peer3_pubkey, Ping {
            id: 3,
            local_name_record: generate_name_record(peer3, 0),
        });
        assert!(state.pending_queue.contains_key(&peer3_pubkey));
        let cmds = extract_ping(cmds);
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].0, peer3_pubkey);

        // peer4 is a full node, it is not added to pending queue
        let cmds = state.handle_ping(peer4_pubkey, Ping {
            id: 4,
            local_name_record: generate_name_record(peer4, 0),
        });
        assert!(!state.pending_queue.contains_key(&peer4_pubkey));
        let cmds = extract_ping(cmds);
        assert!(cmds.is_empty());
    }

    const OLD_ADDR: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(7, 7, 7, 7), 8000);
    const NEW_ADDR: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 8000);

    #[test_case(None, NameRecord::new(*NEW_ADDR.ip(), NEW_ADDR.port(), 1), true, NameRecord::new(*NEW_ADDR.ip(), NEW_ADDR.port(), 1), true; "first record")]
    #[test_case(Some(NameRecord::new(*OLD_ADDR.ip(), OLD_ADDR.port(), 1)), NameRecord::new(*NEW_ADDR.ip(), NEW_ADDR.port(), 2), true, NameRecord::new(*NEW_ADDR.ip(), NEW_ADDR.port(), 2), true; "newer record")]
    #[test_case(Some(NameRecord::new(*OLD_ADDR.ip(), OLD_ADDR.port(), 1)), NameRecord::new(*OLD_ADDR.ip(), OLD_ADDR.port(), 1), true, NameRecord::new(*OLD_ADDR.ip(), OLD_ADDR.port(), 1), false; "same record")]
    #[test_case(Some(NameRecord::new(*NEW_ADDR.ip(), NEW_ADDR.port(), 2)), NameRecord::new(*OLD_ADDR.ip(), OLD_ADDR.port(), 1), false, NameRecord::new(*NEW_ADDR.ip(), NEW_ADDR.port(), 2), false; "older record")]
    #[test_case(Some(NameRecord::new(*OLD_ADDR.ip(), OLD_ADDR.port(), 1)), NameRecord::new(*NEW_ADDR.ip(), NEW_ADDR.port(), 1), false, NameRecord::new(*OLD_ADDR.ip(), OLD_ADDR.port(), 1), false; "conflicting record")]
    fn test_ping_record(
        known_record: Option<NameRecord>,
        incoming_record: NameRecord,
        expected_pong: bool,
        expected_record: NameRecord,
        expected_ping_to_sender: bool,
    ) {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let routing_info = match known_record {
            Some(record) => BTreeMap::from([(peer1_pubkey, MonadNameRecord::new(record, peer1))]),
            None => BTreeMap::new(),
        };

        let mut state = generate_test_state(peer0, vec![]);
        state.self_role = PeerDiscoveryRole::ValidatorNone;
        state.routing_info = routing_info;

        let cmds = state.handle_ping(peer1_pubkey, Ping {
            id: 7,
            local_name_record: MonadNameRecord::new(incoming_record, peer1),
        });

        if expected_pong {
            if expected_ping_to_sender {
                // 1 PingTimeout timer cmd, 1 SendPing cmd, 1 Pong cmd
                assert_eq!(cmds.len(), 3);

                let node1_record = state
                    .pending_queue
                    .get(&peer1_pubkey)
                    .unwrap()
                    .name_record
                    .clone();
                assert_eq!(expected_record, node1_record.name_record);
            } else {
                // 1 Pong cmd
                assert_eq!(cmds.len(), 1);

                assert!(!state.pending_queue.contains_key(&peer1_pubkey));
            }
            let pong = extract_pong(cmds)[0];
            assert_eq!(pong, Pong {
                ping_id: 7,
                local_record_seq: 0
            });
        } else {
            assert!(cmds.is_empty());
        }
    }

    #[test]
    fn test_publisher_participation_info() {
        let keys = create_keys::<SignatureType>(4);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer2 = &keys[2];
        let peer3 = &keys[3];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2_pubkey = NodeId::new(peer2.pubkey());
        let peer3_pubkey = NodeId::new(peer3.pubkey());

        let mut state = generate_test_state(peer0, vec![peer1, peer2, peer3]);

        // do not respond to full node raptorcast request if self is not a validator publisher
        state.self_role = PeerDiscoveryRole::ValidatorNone;
        let cmds = state.handle_full_node_raptorcast_request(peer1_pubkey);
        assert_eq!(cmds.len(), 0);
        assert_eq!(state.get_secondary_fullnodes(), Vec::new());

        // after receiving a full node raptorcast request from peer1,
        // it should mark it as connected
        state.self_role = PeerDiscoveryRole::ValidatorPublisher;
        let cmds = state.handle_full_node_raptorcast_request(peer1_pubkey);
        assert!(state.participation_info.contains_key(&peer1_pubkey));
        assert_eq!(
            state.participation_info.get(&peer1_pubkey).unwrap().status,
            SecondaryRaptorcastConnectionStatus::Connected
        );
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0], PeerDiscoveryCommand::RouterCommand {
            target: _,
            message: PeerDiscoveryMessage::FullNodeRaptorcastResponse
        }));
        assert_eq!(state.get_secondary_fullnodes(), Vec::from([peer1_pubkey]));

        // do not respond to full node raptorcast request if already exceeding max group size
        state.max_group_size = 1;
        let cmds = state.handle_full_node_raptorcast_request(peer2_pubkey);
        assert_eq!(
            state.participation_info.get(&peer2_pubkey).unwrap().status,
            SecondaryRaptorcastConnectionStatus::None
        );
        assert_eq!(cmds.len(), 0);
        assert_eq!(state.get_secondary_fullnodes(), Vec::from([peer1_pubkey]));

        // prioritized full nodes are reserved a slot even if unconnected
        state.prioritized_full_nodes.insert(peer3_pubkey);
        state.max_group_size = 2;
        let cmds = state.handle_full_node_raptorcast_request(peer2_pubkey);
        // peer1 (connected) and peer3 (prioritized) took up the slots
        // peer2 is still rejected
        assert_eq!(
            state.participation_info.get(&peer1_pubkey).unwrap().status,
            SecondaryRaptorcastConnectionStatus::Connected
        );
        assert_eq!(
            state.participation_info.get(&peer2_pubkey).unwrap().status,
            SecondaryRaptorcastConnectionStatus::None
        );
        assert_eq!(
            state.participation_info.get(&peer3_pubkey).unwrap().status,
            SecondaryRaptorcastConnectionStatus::None
        );
        assert_eq!(cmds.len(), 0);
        assert_eq!(state.get_secondary_fullnodes(), Vec::from([peer1_pubkey]));

        // a connected prioritized full node is only counted once towards max_group_size
        state.handle_full_node_raptorcast_request(peer3_pubkey);
        assert_eq!(
            state.participation_info.get(&peer3_pubkey).unwrap().status,
            SecondaryRaptorcastConnectionStatus::Connected
        );
        state.max_group_size = 3;
        let cmds = state.handle_full_node_raptorcast_request(peer2_pubkey);
        // now peer2 is accepted as there are only 2 connections
        assert_eq!(
            state.participation_info.get(&peer2_pubkey).unwrap().status,
            SecondaryRaptorcastConnectionStatus::Connected
        );
        assert_eq!(cmds.len(), 1);
    }

    #[test]
    fn test_client_participation_info() {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let mut state = generate_test_state(peer0, vec![peer1]);
        state
            .epoch_validators
            .insert(state.current_epoch, BTreeSet::from([peer1_pubkey]));

        // do not look for upstream validator if running as None in secondary raptorcast
        state.self_role = PeerDiscoveryRole::FullNodeNone;
        let cmds = state.refresh();
        assert_eq!(cmds.len(), 5); // 2 peer look commands (unrelated to test case), 2 timer commands and 1 metrics command

        // look for upstream validator if running as a Client in secondary raptorcast
        state.self_role = PeerDiscoveryRole::FullNodeClient;
        state.refresh();
        assert_eq!(
            state.participation_info.get(&peer1_pubkey).unwrap().status,
            SecondaryRaptorcastConnectionStatus::Pending
        );

        // when it receives the response, the status becomes connected
        state.handle_full_node_raptorcast_response(peer1_pubkey);
        assert_eq!(
            state.participation_info.get(&peer1_pubkey).unwrap().status,
            SecondaryRaptorcastConnectionStatus::Connected
        );
    }

    #[test]
    fn test_select_peers_to_lookup_from() {
        let keys = create_keys::<SignatureType>(5);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());
        let peer3 = &keys[3];
        let peer3_pubkey = NodeId::new(peer3.pubkey());
        let peer4 = &keys[4];
        let peer4_pubkey = NodeId::new(peer4.pubkey());

        let mut state = generate_test_state(peer0, vec![peer1, peer2]);
        state
            .epoch_validators
            .insert(state.current_epoch, BTreeSet::from([peer1_pubkey]));

        // lookup from initial bootstrap peers if running as a dedicated full node
        state.self_role = PeerDiscoveryRole::FullNodeNone;
        let selected_peers = state.select_peers_to_lookup_from();
        assert_eq!(selected_peers.len(), 2);
        assert!(selected_peers.contains(&peer1_pubkey));
        assert!(selected_peers.contains(&peer2_pubkey));

        // lookup from upstream validator if running as a Client in secondary raptorcast
        state.self_role = PeerDiscoveryRole::FullNodeClient;
        state
            .routing_info
            .insert(peer3_pubkey, generate_name_record(peer3, 0));
        state
            .routing_info
            .insert(peer4_pubkey, generate_name_record(peer4, 0));
        state
            .participation_info
            .insert(peer1_pubkey, SecondaryRaptorcastInfo {
                status: SecondaryRaptorcastConnectionStatus::Connected,
                num_retries: 0,
                last_active: Round(0),
            });
        let selected_peers = state.select_peers_to_lookup_from();
        assert_eq!(selected_peers.len(), 3);
        assert!(selected_peers.contains(&peer1_pubkey));

        // lookup from any peers if running as a validator
        state.self_role = PeerDiscoveryRole::ValidatorNone;
        let selected_peers = state.select_peers_to_lookup_from();
        assert_eq!(selected_peers.len(), 3);
    }
}
