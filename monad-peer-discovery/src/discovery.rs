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
    MonadNameRecord, NameRecord, NodeAddrLookup, PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder,
    PeerDiscoveryCommand, PeerDiscoveryEvent, PeerDiscoveryMessage, PeerDiscoveryMetricsCommand,
    PeerDiscoveryTimerCommand, PeerLookupRequest, PeerLookupResponse, PeerTable, Ping, Pong, Role,
    TimerKind,
};

/// Maximum number of peers to be included in a PeerLookupResponse
const MAX_PEER_IN_RESPONSE: usize = 16;
/// Number of peers to send lookup request to
const NUM_LOOKUP_PEERS: usize = 3;
/// Number of validators to connect to if self is a full node
// TODO: this should be configurable (e.g. a dedicated full node does not need to have 3 upstream validators)
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
pub const GAUGE_PEER_DISC_REFRESH: &str = "monad.peer_disc.refresh";
pub const GAUGE_PEER_DISC_NUM_PEERS: &str = "monad.peer_disc.num_peers";
pub const GAUGE_PEER_DISC_NUM_CONNECTED_PEERS: &str = "monad.peer_disc.num_connected_peers";

#[derive(Debug, Clone, Copy)]
pub struct ConnectionInfo<ST: CertificateSignatureRecoverable> {
    pub last_ping: Option<Ping<ST>>,
    pub unresponsive_pings: u32,
    // used to track the last time a peer participated in secondary raptorcast
    // if never participated, set to the Round when it first establish a connection
    pub last_participation: Round,
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
    self_role: Role,
    pub current_round: Round,
    pub current_epoch: Epoch,
    // mapping of epoch to validators in that epoch
    pub epoch_validators: BTreeMap<Epoch, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,
    // pinned full nodes are dedicated and prioritized full nodes passed in from config that will not be pruned
    pub pinned_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    // mapping of node IDs to their corresponding name records
    pub routing_info: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, MonadNameRecord<ST>>,
    // mapping of node IDs to their connection info, used to track ping status
    // connection_info is a subset of routing_info, i.e. only nodes that are currently connected
    pub connection_info: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, ConnectionInfo<ST>>,
    // mapping of lookup IDs to their corresponding lookup info, node will only entertain lookup response
    // that matches a local lookup ID
    pub outstanding_lookup_requests: HashMap<u32, LookupInfo<ST>>,
    pub metrics: ExecutorMetrics,
    // duration before sending next ping
    pub ping_period: Duration,
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
    pub rng: ChaCha8Rng,
}

pub struct PeerDiscoveryBuilder<ST: CertificateSignatureRecoverable> {
    pub self_id: NodeId<CertificateSignaturePubKey<ST>>,
    pub self_record: MonadNameRecord<ST>,
    pub current_round: Round,
    pub current_epoch: Epoch,
    pub epoch_validators: BTreeMap<Epoch, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,
    pub pinned_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    pub routing_info: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, MonadNameRecord<ST>>,
    pub ping_period: Duration,
    pub refresh_period: Duration,
    pub request_timeout: Duration,
    pub unresponsive_prune_threshold: u32,
    pub last_participation_prune_threshold: Round,
    pub min_num_peers: usize,
    pub max_num_peers: usize,
    pub rng: ChaCha8Rng,
}

impl<ST: CertificateSignatureRecoverable> PeerDiscoveryAlgoBuilder for PeerDiscoveryBuilder<ST> {
    type PeerDiscoveryAlgoType = PeerDiscovery<ST>;

    fn build(
        self,
    ) -> (
        Self::PeerDiscoveryAlgoType,
        Vec<PeerDiscoveryCommand<<Self::PeerDiscoveryAlgoType as NodeAddrLookup>::SignatureType>>,
    ) {
        debug!("initializing peer discovery");
        assert!(self.ping_period > self.request_timeout);
        assert!(self.max_num_peers > self.min_num_peers);

        let self_role = Role::FullNode;
        let mut state = PeerDiscovery {
            self_id: self.self_id,
            self_record: self.self_record,
            self_role,
            current_round: self.current_round,
            current_epoch: self.current_epoch,
            epoch_validators: self.epoch_validators,
            pinned_full_nodes: self.pinned_full_nodes,
            routing_info: self.routing_info.clone(),
            connection_info: Default::default(),
            outstanding_lookup_requests: Default::default(),
            metrics: Default::default(),
            ping_period: self.ping_period,
            refresh_period: self.refresh_period,
            request_timeout: self.request_timeout,
            unresponsive_prune_threshold: self.unresponsive_prune_threshold,
            last_participation_prune_threshold: self.last_participation_prune_threshold,
            min_num_peers: self.min_num_peers,
            max_num_peers: self.max_num_peers,
            rng: self.rng,
        };

        // if self is a validator in the current or next epoch, update self role
        if state.is_validator(&self.self_id) {
            state.self_role = Role::Validator;
        }

        let mut cmds = self
            .routing_info
            .into_iter()
            .flat_map(|(node_id, _)| state.send_ping(node_id))
            .collect::<Vec<_>>();

        cmds.extend(state.refresh());

        (state, cmds)
    }
}

impl<ST: CertificateSignatureRecoverable> PeerDiscovery<ST> {
    // schedule for next ping
    fn reset_ping_timer(
        &self,
        peer: NodeId<CertificateSignaturePubKey<ST>>,
        ping_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        vec![
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::ScheduleReset {
                node_id: peer,
                timer_kind: TimerKind::PingTimeout,
            }),
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::ScheduleReset {
                node_id: peer,
                timer_kind: TimerKind::SendPing,
            }),
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::Schedule {
                node_id: peer,
                timer_kind: TimerKind::PingTimeout,
                duration: self.request_timeout,
                on_timeout: PeerDiscoveryEvent::PingTimeout { to: peer, ping_id },
            }),
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::Schedule {
                node_id: peer,
                timer_kind: TimerKind::SendPing,
                duration: self.ping_period,
                on_timeout: PeerDiscoveryEvent::SendPing { to: peer },
            }),
        ]
    }

    // stop sending ping to a peer
    fn clear_ping_timer(
        &self,
        peer: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        vec![
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::ScheduleReset {
                node_id: peer,
                timer_kind: TimerKind::PingTimeout,
            }),
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::ScheduleReset {
                node_id: peer,
                timer_kind: TimerKind::SendPing,
            }),
        ]
    }

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

    fn schedule_lookup_timer(
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

    fn insert_peer(
        &mut self,
        peer_id: NodeId<CertificateSignaturePubKey<ST>>,
        name_record: MonadNameRecord<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        // make sure self record is never inserted into routing info
        if peer_id == self.self_id {
            return vec![];
        }

        // insert name record
        if let Some(current_name_record) = self.routing_info.get_mut(&peer_id) {
            if name_record.seq() > current_name_record.seq() {
                // update name record
                debug!(?peer_id, ?name_record, "name record updated");
                *current_name_record = name_record;
            } else {
                // exit if seq num is not incremented
                return vec![];
            }
        } else {
            // peer is not present, insert peer
            debug!(?peer_id, ?name_record, "name record inserted");
            self.routing_info.insert(peer_id, name_record);
        }

        self.metrics[GAUGE_PEER_DISC_NUM_PEERS] = self.routing_info.len() as u64;

        // full nodes only need to connect to a few validators so they don't need to connect to other full nodes
        if self.self_role == Role::FullNode {
            if self.is_validator(&peer_id) {
                // look for upstream validator if insufficient
                return self.look_for_upstream_validators();
            } else {
                // no need to send ping to other full nodes but still record last participation
                self.connection_info
                    .entry(peer_id)
                    .and_modify(|connection_entry| {
                        if connection_entry.last_participation < self.current_round {
                            connection_entry.last_participation = self.current_round;
                        }
                    })
                    .or_insert_with(|| ConnectionInfo {
                        last_ping: None,
                        unresponsive_pings: 0,
                        last_participation: self.current_round,
                    });
                return vec![];
            }
        }

        // validators send pings to newly modified/added peers
        self.send_ping(peer_id)
    }

    fn remove_peer(
        &mut self,
        node_id: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        let mut cmds = Vec::new();

        // remove from connection info
        self.connection_info.remove(&node_id);
        cmds.extend(self.clear_ping_timer(node_id));

        // if it's not a pinned node, also remove from routing info
        if !self.is_pinned_node(&node_id) {
            self.routing_info.remove(&node_id);
        }

        self.metrics[GAUGE_PEER_DISC_NUM_PEERS] = self.routing_info.len() as u64;
        self.metrics[GAUGE_PEER_DISC_NUM_CONNECTED_PEERS] = self.connection_info.len() as u64;

        cmds
    }

    fn look_for_upstream_validators(&mut self) -> Vec<PeerDiscoveryCommand<ST>> {
        let mut cmds = Vec::new();

        // full node will try to connect to NUM_UPSTREAM_VALIDATORS
        // if insufficient connected upstream validators, try to send pings to new validators
        let connected_validators = self
            .connection_info
            .keys()
            .filter(|&node_id| self.is_validator(node_id))
            .collect::<HashSet<_>>();

        let slots_to_fill = NUM_UPSTREAM_VALIDATORS.saturating_sub(connected_validators.len());
        if slots_to_fill > 0 {
            // when selecting new upstream validators, make sure they are not already currently connected validators
            // TODO: we should also prioritize validators that are not recently pruned for connection info for better heuristics
            let available_validators = self
                .routing_info
                .keys()
                .filter(|&node_id| {
                    !connected_validators.contains(node_id)
                        && self.is_validator(node_id)
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

            for &validator in new_upstream_validators {
                cmds.extend(self.send_ping(validator));
            }
        }

        cmds
    }

    // a helper function to check if a node is a validator in the current or next epoch
    // TODO: maybe rename this function
    fn is_validator(&self, node_id: &NodeId<CertificateSignaturePubKey<ST>>) -> bool {
        let current_epoch_validators = self.epoch_validators.get(&self.current_epoch);
        let next_epoch_validators = self.epoch_validators.get(&(self.current_epoch + Epoch(1)));
        current_epoch_validators.is_some_and(|validators| validators.contains(node_id))
            || next_epoch_validators.is_some_and(|validators| validators.contains(node_id))
    }

    // a helper function to check if a node is a validator or a pinned full node
    fn is_pinned_node(&self, node_id: &NodeId<CertificateSignaturePubKey<ST>>) -> bool {
        self.is_validator(node_id) || self.pinned_full_nodes.contains(node_id)
    }
}

impl<ST: CertificateSignatureRecoverable> NodeAddrLookup for PeerDiscovery<ST> {
    type SignatureType = ST;

    fn lookup_name_record(
        &self,
        id: &NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<MonadNameRecord<Self::SignatureType>> {
        self.routing_info.get(id).cloned()
    }

    fn name_records(
        &self,
    ) -> HashMap<
        NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        MonadNameRecord<Self::SignatureType>,
    > {
        self.routing_info.clone().into_iter().collect()
    }

    fn role(
        &self,
        node_id: &NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<Role> {
        let is_validator = self
            .epoch_validators
            .get(&self.current_epoch)
            .and_then(|validators| validators.get(node_id))
            .is_some();

        if is_validator {
            Some(Role::Validator)
        } else if self.pinned_full_nodes.contains(node_id)
            || self.routing_info.contains_key(node_id)
        {
            Some(Role::FullNode)
        } else {
            None
        }
    }
}

impl<ST> PeerDiscoveryAlgo for PeerDiscovery<ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn send_ping(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, "sending ping request");

        let mut cmds = Vec::new();

        // must have a MonadNameRecord to send a ping
        if !self.routing_info.contains_key(&to) {
            warn!(
                ?to,
                "name record not present locally but trying to send ping"
            );
            return cmds;
        };

        // TODO: use connection heuristics to decide if attaching local_name_record
        let ping_msg = Ping {
            id: self.rng.next_u32(),
            local_name_record: Some(self.self_record),
        };

        // record connection info
        self.connection_info
            .entry(to)
            .and_modify(|connection_entry| {
                connection_entry.last_ping = Some(ping_msg);
            })
            .or_insert_with(|| ConnectionInfo {
                last_ping: Some(ping_msg),
                unresponsive_pings: 0,
                last_participation: self.current_round,
            });

        // reset timer to schedule for the next ping
        cmds.extend(self.reset_ping_timer(to, ping_msg.id));

        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: to,
            message: PeerDiscoveryMessage::Ping(ping_msg),
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

        // drop ping if peer list is full and incoming ping is not from a validator or pinned full node
        if self.routing_info.len() >= self.max_num_peers
            && !self.is_validator(&from)
            && !self.pinned_full_nodes.contains(&from)
            && !self.routing_info.contains_key(&from)
        {
            debug!(
                ?from,
                "peer list is full, dropping ping request from non-validator and non-pinned-full-node peer"
            );
            self.metrics[GAUGE_PEER_DISC_DROP_PING] += 1;
            return cmds;
        }

        if let Some(peer_name_record) = ping_msg.local_name_record {
            if self
                .routing_info
                .get(&from)
                .is_none_or(|local| peer_name_record.seq() > local.seq())
            {
                let verified = peer_name_record
                    .recover_pubkey()
                    .is_ok_and(|recovered_node_id| recovered_node_id == from);

                if verified {
                    cmds.extend(self.insert_peer(from, peer_name_record));
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
            local_record_seq: self.self_record.name_record.seq,
        };
        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: from,
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

        let cmds = Vec::new();

        // if ping id matches, update connection info
        if let Some(info) = self.connection_info.get_mut(&from) {
            if info
                .last_ping
                .is_some_and(|last_ping| last_ping.id == pong_msg.ping_id)
            {
                info.last_ping = None;
                info.unresponsive_pings = 0;
            } else {
                debug!(?from, "dropping pong, ping id not found or does not match");
                self.metrics[GAUGE_PEER_DISC_DROP_PONG] += 1;
            }
        } else {
            debug!(
                ?from,
                "dropping pong, ping sender does not exist in connection info"
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
        trace!(?to, "ping timeout");
        let mut cmds = Vec::new();

        let Some(info) = self.connection_info.get_mut(&to) else {
            debug!(
                ?to,
                "connection info not present locally, dropping ping timeout..."
            );
            return cmds;
        };

        // record timeout
        if let Some(last_ping) = info.last_ping {
            if last_ping.id == ping_id {
                debug!(?to, ?ping_id, "handling ping timeout");
                self.metrics[GAUGE_PEER_DISC_PING_TIMEOUT] += 1;
                info.unresponsive_pings += 1;
                info.last_ping = None;

                // if unresponsive pings exceeds threshold, stop sending pings to this peer
                if info.unresponsive_pings >= self.unresponsive_prune_threshold {
                    debug!(
                        ?to,
                        "unresponsive pings exceeded threshold, dropping connection"
                    );
                    cmds.extend(self.remove_peer(to));

                    // if self is a full node, look for new upstream validators
                    if self.self_role == Role::FullNode {
                        cmds.extend(self.look_for_upstream_validators());
                    }
                }
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
        cmds.extend(self.schedule_lookup_timer(to, target, lookup_id));

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
            vec![self.self_record]
        } else {
            match self.routing_info.get(&target) {
                Some(name_record) => vec![*name_record],
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
                .filter(|(node_id, _)| self.is_validator(node_id))
                .collect();
            name_records.extend(
                validators
                    .iter()
                    .choose_multiple(
                        &mut self.rng,
                        MAX_PEER_IN_RESPONSE.saturating_sub(name_records.len()),
                    )
                    .into_iter()
                    .map(|(_, name_record)| *name_record),
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

        // update routing info
        for name_record in response.name_records {
            // verify signature of name record
            let node_id = match name_record.recover_pubkey() {
                Ok(node_id) => node_id,
                Err(e) => {
                    warn!(?e, "invalid name record signature, dropping record...");
                    continue;
                }
            };

            cmds.extend(self.insert_peer(node_id, name_record));
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
        cmds.extend(self.schedule_lookup_timer(to, target, new_lookup_id));

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

    fn refresh(&mut self) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!("refreshing peer discovery");

        self.metrics[GAUGE_PEER_DISC_REFRESH] += 1;
        let mut cmds = Vec::new();

        // remove nodes that have not participated in secondary raptorcast beyond last_participation_prune_threshold
        let non_participating_nodes: Vec<_> = self
            .connection_info
            .iter()
            .filter(|(node_id, info)| {
                self.current_round.max(info.last_participation) - info.last_participation
                    >= self.last_participation_prune_threshold
                    && !self.is_pinned_node(node_id)
            })
            .map(|(node_id, _)| *node_id)
            .collect();

        for node_id in non_participating_nodes {
            debug!(?node_id, "stop sending ping to non-participating peer");
            cmds.extend(self.remove_peer(node_id));
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
                    cmds.extend(self.remove_peer(node_id));
                }
            }
        }
        trace!("Current routing info: {:?}", self.routing_info);
        trace!("Current connection info: {:?}", self.connection_info);

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

        let chosen_peers = self
            .routing_info
            .keys()
            .cloned()
            .choose_multiple(&mut self.rng, NUM_LOOKUP_PEERS);

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

        // if self is a full node, try to connect to a few current validators if not already connected
        if self.self_role == Role::FullNode {
            cmds.extend(self.look_for_upstream_validators());
        }

        self.metrics[GAUGE_PEER_DISC_NUM_PEERS] = self.routing_info.len() as u64;
        self.metrics[GAUGE_PEER_DISC_NUM_CONNECTED_PEERS] = self.connection_info.len() as u64;

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
        let mut cmds = Vec::new();

        if round > self.current_round {
            trace!(?round, "updating current round in peer discovery");
            self.current_round = round;
        }

        if epoch > self.current_epoch {
            debug!(?epoch, "updating current epoch in peer discovery");
            self.current_epoch = epoch;

            // if a full node gets promoted to a validator, send pings to all validators in the current and next epoch
            if self.self_role == Role::FullNode && self.is_validator(&self.self_id) {
                self.self_role = Role::Validator;

                let current_validators = self.epoch_validators.get(&epoch);
                let next_validators = self.epoch_validators.get(&(epoch + Epoch(1)));
                let validators: HashSet<_> = current_validators
                    .into_iter()
                    .chain(next_validators)
                    .flat_map(|v| v.iter())
                    .cloned()
                    .collect();

                for validator in validators {
                    cmds.extend(self.send_ping(validator));
                }
            }

            // if a validator gets demoted to a full node, remove connections to all validators but NUM_UPSTREAM_VALIDATORS
            if self.self_role == Role::Validator && !self.is_validator(&self.self_id) {
                self.self_role = Role::FullNode;

                let validators: Vec<_> = self
                    .routing_info
                    .keys()
                    .filter(|node_id| self.is_validator(node_id))
                    .collect();

                let keep = validators
                    .into_iter()
                    .choose_multiple(&mut self.rng, NUM_UPSTREAM_VALIDATORS);

                for peer in self.routing_info.keys() {
                    if !keep.contains(&peer) {
                        cmds.extend(self.clear_ping_timer(*peer));
                        self.connection_info.remove(peer);
                    }
                }
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

        let cmds = Vec::new();
        self.epoch_validators.insert(epoch, validators);

        cmds
    }

    fn update_peers(&mut self, peers: Vec<PeerEntry<ST>>) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?peers, "updating peers");

        let mut cmds = Vec::new();

        for peer in peers {
            let node_id = NodeId::new(peer.pubkey);

            // verify signature of name record
            let name_record = MonadNameRecord {
                name_record: NameRecord {
                    address: peer.addr,
                    seq: peer.record_seq_num,
                },
                signature: peer.signature,
            };
            let verified = name_record
                .recover_pubkey()
                .is_ok_and(|recovered_node_id| recovered_node_id == node_id);
            if verified {
                cmds.extend(self.insert_peer(node_id, name_record));
            } else {
                warn!(?node_id, "invalid name record signature");
            }
        }

        cmds
    }

    fn update_peer_participation(
        &mut self,
        round: Round,
        peers: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?round, ?peers, "updating peer participation");

        let cmds = Vec::new();

        // we don't need to send ping here, that is done when the name record is inserted
        for peer in peers {
            if peer == self.self_id {
                continue; // skip self
            }
            self.connection_info
                .entry(peer)
                .and_modify(|connection_entry| {
                    if connection_entry.last_participation < round {
                        connection_entry.last_participation = round;
                    }
                })
                .or_insert_with(|| ConnectionInfo {
                    last_ping: None,
                    unresponsive_pings: 0,
                    last_participation: round,
                });
        }

        cmds
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    fn peer_table(&self) -> crate::PeerTable<ST> {
        let curr_validators: HashSet<_> = self
            .epoch_validators
            .get(&self.current_epoch)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        PeerTable::from_name_records_and_validators(self.name_records(), curr_validators)
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
        let name_record = NameRecord {
            address: DUMMY_ADDR,
            seq: seq_num,
        };
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
            .into_iter()
            .map(|key| {
                let node_id = NodeId::new(key.pubkey());
                let name_record = generate_name_record(key, 0);
                (node_id, name_record)
            })
            .collect::<BTreeMap<_, _>>();

        PeerDiscovery {
            self_id: NodeId::new(self_key.pubkey()),
            self_record: generate_name_record(self_key, 0),
            self_role: Role::FullNode,
            current_round: Round(1),
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::new(),
            pinned_full_nodes: BTreeSet::new(),
            routing_info,
            connection_info: BTreeMap::new(),
            outstanding_lookup_requests: HashMap::new(),
            metrics: ExecutorMetrics::default(),
            ping_period: Duration::from_secs(60),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            unresponsive_prune_threshold: 10,
            last_participation_prune_threshold: Round(5000),
            min_num_peers: 5,
            max_num_peers: 50,
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
                PeerDiscoveryCommand::RouterCommand {
                    target,
                    message: PeerDiscoveryMessage::Ping(ping),
                } => Some((target, ping)),
                _ => None,
            })
            .collect::<Vec<_>>()
    }

    fn extract_pong(cmds: Vec<PeerDiscoveryCommand<SignatureType>>) -> Vec<Pong> {
        cmds.into_iter()
            .filter_map(|c| match c {
                PeerDiscoveryCommand::RouterCommand {
                    target: _,
                    message: PeerDiscoveryMessage::Pong(pong),
                } => Some(pong),
                _ => None,
            })
            .collect::<Vec<_>>()
    }

    #[test]
    fn test_check_peer_connection() {
        let keys = create_keys::<SignatureType>(3);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());

        // peer1 name record is present but peer2 name record is not present
        let mut state = generate_test_state(peer0, vec![peer1]);

        // should be able to send ping to peer where its name record already exists locally
        let cmds = state.send_ping(peer1_pubkey);
        // four reset timer cmds, one ping cmd
        assert_eq!(cmds.len(), 5);

        // last_ping is recorded correctly
        let connection_info = state.connection_info.get(&peer1_pubkey);
        assert!(connection_info.is_some());
        assert!(connection_info.unwrap().last_ping.is_some());
        assert_eq!(connection_info.unwrap().unresponsive_pings, 0);
        let ping_msg = connection_info.unwrap().last_ping.unwrap();

        // should not be able to send ping to peer where its name record does not exist locally
        let cmds = state.send_ping(peer2_pubkey);
        assert_eq!(cmds.len(), 0);

        // when corresponding pong is received from peer1, last_ping and last_seen is updated
        state.handle_pong(peer1_pubkey, Pong {
            ping_id: ping_msg.id,
            local_record_seq: 0,
        });
        let connection_info = state.connection_info.get(&peer1_pubkey);
        assert!(connection_info.is_some());
        assert!(connection_info.unwrap().last_ping.is_none());
        assert_eq!(connection_info.unwrap().unresponsive_pings, 0);
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
            local_name_record: Some(generate_name_record(peer1, 0)),
        };
        state.connection_info.insert(peer1_pubkey, ConnectionInfo {
            last_ping: Some(last_ping),
            unresponsive_pings: 3,
            last_participation: Round(1),
        });

        // should not record pong when ping_id doesn't match
        state.handle_pong(peer1_pubkey, Pong {
            ping_id: 54321, // incorrect ping id,
            local_record_seq: 0,
        });
        let connection_info = state.connection_info.get(&peer1_pubkey);
        assert!(connection_info.is_some());
        assert_eq!(connection_info.unwrap().last_ping, Some(last_ping));
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
        assert_eq!(cmds.len(), 2);
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
        state.handle_peer_lookup_response(peer1_pubkey, PeerLookupResponse {
            lookup_id: requests[0].lookup_id,
            target: peer2_pubkey,
            name_records: vec![record],
        });

        // peer2 should be added to routing info and outstanding requests should be cleared
        assert!(state.routing_info.contains_key(&peer2_pubkey));
        assert_eq!(state.outstanding_lookup_requests.keys().len(), 0);
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
        state.self_role = Role::Validator;
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

        // should update name record if record has higher sequence number (seq num incremented to 2)
        let record = generate_name_record(peer1, 2);
        state.handle_peer_lookup_response(peer1_pubkey, PeerLookupResponse {
            lookup_id: 1,
            target: peer1_pubkey,
            name_records: vec![record],
        });
        assert_eq!(state.routing_info.get(&peer1_pubkey).unwrap().seq(), 2);

        // should not update name record if record has lower sequence number (seq num decremented to 1)
        let record = generate_name_record(peer1, 1);
        state.handle_peer_lookup_response(peer1_pubkey, PeerLookupResponse {
            lookup_id: 2,
            target: peer1_pubkey,
            name_records: vec![record],
        });
        assert_eq!(state.routing_info.get(&peer1_pubkey).unwrap().seq(), 2);
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
        state.handle_peer_lookup_response(peer1_pubkey, PeerLookupResponse {
            lookup_id: 1,
            target: peer1_pubkey,
            name_records: vec![record],
        });
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
        state.handle_peer_lookup_response(peer1_pubkey, PeerLookupResponse {
            lookup_id,
            target: peer1_pubkey,
            name_records: vec![record; MAX_PEER_IN_RESPONSE + 1],
        });
        assert!(!state.routing_info.contains_key(&peer1_pubkey));
    }

    #[test]
    fn test_prune_connections_and_lookup_request() {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let mut state = generate_test_state(peer0, vec![]);

        // add peer1 to routing info with unresponsive pings
        // and outstanding lookup request with num_retries above prune threshold
        let lookup_id = 1;
        let connection_info = BTreeMap::from([(peer1_pubkey, ConnectionInfo {
            last_ping: None,
            unresponsive_pings: 5,
            last_participation: Round(1),
        })]);
        state.unresponsive_prune_threshold = 3;
        state.connection_info = connection_info;
        state
            .outstanding_lookup_requests
            .insert(lookup_id, LookupInfo {
                num_retries: 5,
                receiver: peer1_pubkey,
                open_discovery: false,
            });

        state.refresh();
        assert!(state.routing_info.is_empty());

        state.handle_peer_lookup_timeout(peer1_pubkey, peer1_pubkey, lookup_id);
        assert!(state.outstanding_lookup_requests.is_empty());
    }

    #[test_case(Role::FullNode; "peer full node")]
    #[test_case(Role::Validator; "peer validator")]
    fn test_non_participating_full_node(role: Role) {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let mut state = generate_test_state(peer0, vec![peer1]);
        state.last_participation_prune_threshold = Round(10);
        if role == Role::Validator {
            state
                .epoch_validators
                .insert(Epoch(1), BTreeSet::from([peer1_pubkey]));
        }
        state.connection_info.insert(peer1_pubkey, ConnectionInfo {
            last_ping: None,
            unresponsive_pings: 0,
            last_participation: Round(1),
        });

        state.refresh();
        assert!(state.routing_info.contains_key(&peer1_pubkey));
        assert!(state.connection_info.contains_key(&peer1_pubkey));

        // round advances beyond last participation prune threshold
        state.update_current_round(Round(15), Epoch(1));
        state.refresh();
        match role {
            Role::FullNode => {
                // full node should be pruned
                assert!(!state.routing_info.contains_key(&peer1_pubkey));
                assert!(!state.connection_info.contains_key(&peer1_pubkey));
            }
            Role::Validator => {
                // validator should not be pruned
                assert!(state.routing_info.contains_key(&peer1_pubkey));
                assert!(state.connection_info.contains_key(&peer1_pubkey));
            }
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
        assert_eq!(lookup_requests.len(), 1);
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

    #[test_case(Role::FullNode; "self full node")]
    #[test_case(Role::Validator; "self validator")]
    fn test_incoming_ping_above_max_num_peers(role: Role) {
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
        state.self_role = role;
        state
            .epoch_validators
            .insert(Epoch(1), BTreeSet::from([peer2_pubkey]));
        state.pinned_full_nodes.insert(peer3_pubkey);

        // peer2 is a validator, it is added to routing info although already exceeding max number of peers
        let cmds = state.handle_ping(peer2_pubkey, Ping {
            id: 2,
            local_name_record: Some(generate_name_record(peer2, 0)),
        });
        assert!(state.routing_info.contains_key(&peer2_pubkey));
        assert_eq!(state.routing_info.len(), 2);
        // both full node and validator should add peer2 to connection info and send ping
        assert!(state.connection_info.contains_key(&peer2_pubkey));
        let cmds = extract_ping(cmds);
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].0, peer2_pubkey);

        // peer3 is a pinned full node, it is also added to the routing info
        let cmds = state.handle_ping(peer3_pubkey, Ping {
            id: 3,
            local_name_record: Some(generate_name_record(peer3, 0)),
        });
        assert!(state.routing_info.contains_key(&peer3_pubkey));
        assert_eq!(state.routing_info.len(), 3);
        match role {
            Role::FullNode => {
                // full node adds peer3 to connection info but should not send ping
                assert!(state.connection_info.contains_key(&peer3_pubkey));
                assert!(extract_ping(cmds).is_empty());
            }
            Role::Validator => {
                // validator should add peer3 to connection info and send ping
                assert!(state.connection_info.contains_key(&peer3_pubkey));
                let ping_cmds = extract_ping(cmds);
                assert_eq!(ping_cmds.len(), 1);
                assert_eq!(ping_cmds[0].0, peer3_pubkey);
            }
        }

        // peer4 is a full node, it is not added to the routing info
        let cmds = state.handle_ping(peer4_pubkey, Ping {
            id: 4,
            local_name_record: Some(generate_name_record(peer4, 0)),
        });
        assert!(!state.routing_info.contains_key(&peer4_pubkey));
        assert_eq!(state.routing_info.len(), 3);
        // peer4 should not be added to connection info and should not send ping
        assert!(!state.connection_info.contains_key(&peer4_pubkey));
        assert!(extract_ping(cmds).is_empty());
    }

    #[test]
    fn test_look_for_upstream_validators() {
        let keys = create_keys::<SignatureType>(5);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());
        let peer3 = &keys[3];
        let peer3_pubkey = NodeId::new(peer3.pubkey());

        // connection info is empty
        let mut state = generate_test_state(peer0, vec![peer1, peer2, peer3]);
        state.epoch_validators.insert(
            Epoch(1),
            BTreeSet::from([peer1_pubkey, peer2_pubkey, peer3_pubkey]),
        );

        // a full node should send pings to look for upstream validators if insufficient
        let cmds = state.refresh();
        let ping_cmds = extract_ping(cmds);
        assert_eq!(ping_cmds.len(), 3);
        // check that pings are sent to peer1, peer2 and peer3
        assert!(ping_cmds.iter().any(|(target, _)| *target == peer1_pubkey));
        assert!(ping_cmds.iter().any(|(target, _)| *target == peer2_pubkey));
        assert!(ping_cmds.iter().any(|(target, _)| *target == peer3_pubkey));
    }

    #[test]
    fn test_ping_unseen_peer() {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let mut state = generate_test_state(peer0, vec![peer1]);

        let cmds = state.send_ping(peer1_pubkey);
        assert_eq!(cmds.len(), 5);

        assert!(matches!(
            cmds[0],
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::ScheduleReset {
                timer_kind: TimerKind::PingTimeout,
                ..
            })
        ));
        assert!(matches!(
            cmds[1],
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::ScheduleReset {
                timer_kind: TimerKind::SendPing,
                ..
            })
        ));
        assert!(matches!(
            cmds[2],
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::Schedule {
                timer_kind: TimerKind::PingTimeout,
                ..
            })
        ));
        assert!(matches!(
            cmds[3],
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::Schedule {
                timer_kind: TimerKind::SendPing,
                ..
            })
        ));
        assert!(matches!(
            cmds[4],
            PeerDiscoveryCommand::RouterCommand { .. }
        ));

        let (_, ping) = extract_ping(cmds)[0];

        // ping always carries local_name_record
        assert!(ping.local_name_record.is_some());
    }

    const OLD_ADDR: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(7, 7, 7, 7), 8000);
    const NEW_ADDR: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 8000);

    #[test_case(None, NameRecord { address: NEW_ADDR, seq: 1 }, true, NameRecord { address: NEW_ADDR, seq: 1 }, true; "first record")]
    #[test_case(Some(NameRecord { address: OLD_ADDR, seq: 1 }), NameRecord { address: NEW_ADDR, seq: 2 }, true, NameRecord { address: NEW_ADDR, seq: 2 }, true; "newer record")]
    #[test_case(Some(NameRecord { address: OLD_ADDR, seq: 1 }), NameRecord { address: OLD_ADDR, seq: 1 }, true, NameRecord { address: OLD_ADDR, seq: 1 }, false; "same record")]
    #[test_case(Some(NameRecord { address: NEW_ADDR, seq: 2 }), NameRecord { address: OLD_ADDR, seq: 1 }, false, NameRecord { address: NEW_ADDR, seq: 2 }, false; "older record")]
    #[test_case(Some(NameRecord { address: OLD_ADDR, seq: 1 }), NameRecord { address: NEW_ADDR, seq: 1 }, false, NameRecord { address: OLD_ADDR, seq: 1 }, false; "conflicting record")]
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
        state.self_role = Role::Validator;
        state.routing_info = routing_info;

        let cmds = state.handle_ping(peer1_pubkey, Ping {
            id: 7,
            local_name_record: Some(MonadNameRecord::new(incoming_record, peer1)),
        });

        if expected_pong {
            if expected_ping_to_sender {
                // 4 timer cmds, 1 SendPing cmd, 1 Pong cmd
                assert_eq!(cmds.len(), 6);
            } else {
                // 1 Pong cmd
                assert_eq!(cmds.len(), 1);
            }
            let pong = extract_pong(cmds)[0];
            assert_eq!(pong, Pong {
                ping_id: 7,
                local_record_seq: 0
            });

            let node1_record = state.routing_info.get(&peer1_pubkey).unwrap().name_record;
            assert_eq!(expected_record, node1_record);
        } else {
            assert!(cmds.is_empty());
        }
    }

    #[test]
    fn test_full_node_addrs() {
        let keys = create_keys::<SignatureType>(3);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer2 = &keys[2];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2_pubkey = NodeId::new(peer2.pubkey());

        let mut state = generate_test_state(peer0, vec![peer1, peer2]);
        state
            .epoch_validators
            .insert(Epoch(1), BTreeSet::from([peer1_pubkey]));

        let all_addrs = state.known_addrs();
        let fullnode_addrs: HashSet<_> = all_addrs
            .keys()
            .filter(|node| {
                state
                    .role(node)
                    .is_some_and(|role| matches!(role, Role::FullNode))
            })
            .collect();
        assert_eq!(fullnode_addrs.len(), 1);
        assert!(!fullnode_addrs.contains(&peer1_pubkey));
        assert!(fullnode_addrs.contains(&peer2_pubkey));
    }
}
