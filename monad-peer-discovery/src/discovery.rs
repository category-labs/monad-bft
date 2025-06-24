use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    net::SocketAddrV4,
    time::Duration,
};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::ExecutorMetrics;
use monad_executor_glue::PeerEntry;
use monad_types::{Epoch, NodeId};
use rand::{RngCore, SeedableRng, seq::IteratorRandom};
use rand_chacha::ChaCha8Rng;
use tracing::{debug, info, trace, warn};

use crate::{
    MonadNameRecord, NameRecord, PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder, PeerDiscoveryCommand,
    PeerDiscoveryEvent, PeerDiscoveryMessage, PeerDiscoveryTimerCommand, PeerLookupRequest,
    PeerLookupResponse, Ping, Pong, TimerKind,
};

/// Maximum number of peers to be included in a PeerLookupResponse
const MAX_PEER_IN_RESPONSE: usize = 16;
/// Number of peers to send lookup request to if fall below min active connections
const NUM_LOOKUP_PEERS: usize = 3;

/// Metrics constant
pub const GAUGE_PEER_DISC_SEND_PING: &str = "monad.peer_disc.send_ping";
pub const GAUGE_PEER_DISC_RECV_PING: &str = "monad.peer_disc.recv_ping";
pub const GAUGE_PEER_DISC_PING_TIMEOUT: &str = "monad.peer_disc.ping_timeout";
pub const GAUGE_PEER_DISC_SEND_PONG: &str = "monad.peer_disc.send_pong";
pub const GAUGE_PEER_DISC_RECV_PONG: &str = "monad.peer_disc.recv_pong";
pub const GAUGE_PEER_DISC_DROP_PONG: &str = "monad.peer_disc.drop_pong";
pub const GAUGE_PEER_DISC_SEND_LOOKUP_REQUEST: &str = "monad.peer_disc.send_lookup_request";
pub const GAUGE_PEER_DISC_RECV_LOOKUP_REQUEST: &str = "monad.peer_disc.recv_lookup_request";
pub const GAUGE_PEER_DISC_RETRY_LOOKUP_REQUEST: &str = "monad.peer_disc.retry_lookup_request";
pub const GAUGE_PEER_DISC_SEND_LOOKUP_RESPONSE: &str = "monad.peer_disc.send_lookup_response";
pub const GAUGE_PEER_DISC_RECV_LOOKUP_RESPONSE: &str = "monad.peer_disc.recv_lookup_response";
pub const GAUGE_PEER_DISC_DROP_LOOKUP_RESPONSE: &str = "monad.peer_disc.drop_lookup_response";
pub const GAUGE_PEER_DISC_LOOKUP_TIMEOUT: &str = "monad.peer_disc.lookup_timeout";
pub const GAUGE_PEER_DISC_REFRESH: &str = "monad.peer_disc.refresh";

#[derive(Debug, Clone, Copy)]
pub struct PeerInfo<ST: CertificateSignatureRecoverable> {
    pub last_ping: Option<Ping<ST>>,
    pub unresponsive_pings: u32,
    pub name_record: MonadNameRecord<ST>,
}

#[derive(Debug, Clone, Copy)]
pub struct LookupInfo<ST: CertificateSignatureRecoverable> {
    // current number of retries, once above prune_threshold, drop this request
    pub num_retries: u32,
    // receiver of the peer lookup request
    pub receiver: NodeId<CertificateSignaturePubKey<ST>>,
    // if set to true, peers should return additional nodes other than the target specified
    pub open_discovery: bool,
}

pub struct PeerDiscovery<ST: CertificateSignatureRecoverable> {
    pub self_id: NodeId<CertificateSignaturePubKey<ST>>,
    pub self_record: MonadNameRecord<ST>,
    pub current_epoch: Epoch,
    pub epoch_validators: BTreeMap<Epoch, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,
    // dedicated full nodes passed in from config which will not be pruned
    pub dedicated_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    // mapping of node IDs to their corresponding name records
    pub peer_info: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, PeerInfo<ST>>,
    pub outstanding_lookup_requests: HashMap<u32, LookupInfo<ST>>,
    pub metrics: ExecutorMetrics,
    // duration before sending next ping
    pub ping_period: Duration,
    // duration before checking min/max watermark and decide to look for more peers or prune peers
    pub refresh_period: Duration,
    // duration before outstanding pings and lookup requests are dropped
    pub request_timeout: Duration,
    // number of unresponsive pings allowed before being pruned
    pub prune_threshold: u32,
    // minimum number of active connections before actively sending peer lookup requests
    pub min_active_connections: usize,
    // maximum number of active connections before pruning
    pub max_active_connections: usize,
    pub rng: ChaCha8Rng,
}

pub struct PeerDiscoveryBuilder<ST: CertificateSignatureRecoverable> {
    pub self_id: NodeId<CertificateSignaturePubKey<ST>>,
    pub self_record: MonadNameRecord<ST>,
    pub current_epoch: Epoch,
    pub epoch_validators: BTreeMap<Epoch, BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>>,
    pub dedicated_full_nodes: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    pub peer_info: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, PeerInfo<ST>>,
    pub ping_period: Duration,
    pub refresh_period: Duration,
    pub request_timeout: Duration,
    pub prune_threshold: u32,
    pub min_active_connections: usize,
    pub max_active_connections: usize,
    pub rng_seed: u64,
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
        assert!(self.ping_period > self.request_timeout);
        assert!(self.max_active_connections > self.min_active_connections);

        let mut state = PeerDiscovery {
            self_id: self.self_id,
            self_record: self.self_record,
            current_epoch: self.current_epoch,
            epoch_validators: self.epoch_validators,
            dedicated_full_nodes: self.dedicated_full_nodes,
            peer_info: self.peer_info.clone(),
            outstanding_lookup_requests: Default::default(),
            metrics: Default::default(),
            ping_period: self.ping_period,
            refresh_period: self.refresh_period,
            request_timeout: self.request_timeout,
            prune_threshold: self.prune_threshold,
            min_active_connections: self.min_active_connections,
            max_active_connections: self.max_active_connections,
            rng: ChaCha8Rng::seed_from_u64(self.rng_seed),
        };

        let mut cmds = self
            .peer_info
            .into_iter()
            .flat_map(|(node_id, _)| state.send_ping(node_id))
            .collect::<Vec<_>>();

        cmds.extend(state.refresh());

        (state, cmds)
    }
}

impl<ST: CertificateSignatureRecoverable> PeerDiscovery<ST> {
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
        node_id: NodeId<CertificateSignaturePubKey<ST>>,
        name_record: MonadNameRecord<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        // insert name record
        if let Some(info) = self.peer_info.get_mut(&node_id) {
            if name_record.seq() > info.name_record.seq() {
                // update name record
                debug!(?node_id, ?name_record, "name record updated");
                info.name_record = name_record;
            } else {
                // exit if seq num is not incremented
                return vec![];
            }
        } else {
            // peer is not present, insert peer
            debug!(?node_id, ?name_record, "name record inserted");
            self.peer_info.insert(node_id, PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record,
            });
        }

        // send pings to newly modified/added peers
        self.send_ping(node_id)
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
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, "sending ping request");

        let mut cmds = Vec::new();

        // must have a MonadNameRecord to send a ping
        let Some(peer_entry) = self.peer_info.get_mut(&to) else {
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

        peer_entry.last_ping = Some(ping_msg);

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

        if let Some(peer_name_record) = ping_msg.local_name_record {
            if self
                .peer_info
                .get(&from)
                .is_none_or(|local| peer_name_record.seq() > local.name_record.seq())
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
                .peer_info
                .get(&from)
                .is_some_and(|local| peer_name_record.seq() < local.name_record.seq())
            {
                warn!(
                    ?from,
                    "peer updated name record sequence number went backwards"
                );
                return cmds;
            } else if self.peer_info.get(&from).is_some_and(|local| {
                peer_name_record.seq() == local.name_record.seq()
                    && peer_name_record != local.name_record
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

        // if ping id matches, update peer info
        if let Some(info) = self.peer_info.get_mut(&from) {
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
                "dropping pong, ping sender does not exist in peer info"
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
        let cmds = Vec::new();

        let Some(peer_entry) = self.peer_info.get_mut(&to) else {
            warn!(
                ?to,
                "name record not present locally, dropping ping timeout..."
            );
            return cmds;
        };

        // record timeout
        if let Some(last_ping) = peer_entry.last_ping {
            if last_ping.id == ping_id {
                debug!(?to, ?ping_id, "handling ping timeout");
                self.metrics[GAUGE_PEER_DISC_PING_TIMEOUT] += 1;
                peer_entry.unresponsive_pings += 1;
                peer_entry.last_ping = None;
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
            match self.peer_info.get(&target) {
                Some(info) => vec![info.name_record],
                None => vec![],
            }
        };

        // if open discovery, return more nodes other than requested nodes
        if request.open_discovery {
            // return random subset of validators (current and next epoch) up to MAX_PEER_IN_RESPONSE
            let current_epoch_validators = self.epoch_validators.get(&self.current_epoch);
            let next_epoch_validators = self.epoch_validators.get(&(self.current_epoch + Epoch(1)));
            let is_validator = |node_id: &NodeId<CertificateSignaturePubKey<ST>>| {
                current_epoch_validators.is_some_and(|validators| validators.contains(node_id))
                    || next_epoch_validators.is_some_and(|validators| validators.contains(node_id))
            };

            name_records.extend(
                self.peer_info
                    .iter()
                    .filter(|(node_id, _)| is_validator(node_id))
                    .choose_multiple(&mut self.rng, MAX_PEER_IN_RESPONSE - name_records.len())
                    .into_iter()
                    .map(|(_, peer)| peer.name_record),
            );
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

        // update peer info
        for name_record in response.name_records {
            // verify signature of name record
            let node_id = match name_record.recover_pubkey() {
                Ok(node_id) => {
                    if node_id == self.self_id {
                        continue;
                    }
                    node_id
                }
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
        if lookup_info.num_retries >= self.prune_threshold {
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

        // check whether a node id is a validator in the current and next epoch
        let current_epoch_validators = self.epoch_validators.get(&self.current_epoch);
        let next_epoch_validators = self.epoch_validators.get(&(self.current_epoch + Epoch(1)));
        let is_validator = |node_id: &NodeId<CertificateSignaturePubKey<ST>>| {
            current_epoch_validators.is_some_and(|validators| validators.contains(node_id))
                || next_epoch_validators.is_some_and(|validators| validators.contains(node_id))
        };
        let is_dedicated_full_node = |node_id: &NodeId<CertificateSignaturePubKey<ST>>| {
            self.dedicated_full_nodes.contains(node_id)
        };

        // prune unresponsive nodes
        // we currently do not prune validators in current and next epoch, and also dedicated full nodes
        self.peer_info.retain(|node_id, info| {
            info.unresponsive_pings < self.prune_threshold
                || is_validator(node_id)
                || is_dedicated_full_node(node_id)
        });

        // if still above max active peers, randomly choose a few full nodes and prune them
        // TODO: do not prune full nodes in ConfirmGroup
        if self.peer_info.len() > self.max_active_connections {
            let num_to_prune = self.peer_info.len() - self.max_active_connections;
            let nodes_to_prune = self
                .peer_info
                .keys()
                .filter_map(|node| {
                    (!is_validator(node) && !is_dedicated_full_node(node)).then_some(node)
                })
                .cloned()
                .choose_multiple(&mut self.rng, num_to_prune);

            if nodes_to_prune.is_empty() {
                info!("more validators and pinned full nodes than max active connections");
            } else {
                for node_id in nodes_to_prune {
                    debug!(?node_id, "pruning excessive full nodes");
                    self.peer_info.remove(&node_id);
                }
            }
        }
        trace!("Current peer info: {:?}", self.peer_info);

        // if fall below the min active peers, choose a few peers and lookup for new peers
        if self.peer_info.len() < self.min_active_connections {
            let chosen_peers = self
                .peer_info
                .keys()
                .cloned()
                .choose_multiple(&mut self.rng, NUM_LOOKUP_PEERS);
            debug!(?chosen_peers, "discover more peers");

            for peer in chosen_peers {
                cmds.extend(self.send_peer_lookup_request(peer, self.self_id, true));
            }
        }

        // reset timer to schedule for the next refresh
        cmds.extend(self.reset_refresh_timer());

        cmds
    }

    fn update_current_epoch(&mut self, epoch: Epoch) -> Vec<PeerDiscoveryCommand<ST>> {
        let cmds = Vec::new();

        if epoch > self.current_epoch {
            debug!(?epoch, "updating current epoch in peer discovery");
            self.current_epoch = epoch;
        }
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
                warn!(?node_id, "invalid name record signature in config file");
            }
        }

        cmds
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    fn get_addr_by_id(&self, id: &NodeId<CertificateSignaturePubKey<ST>>) -> Option<SocketAddrV4> {
        self.peer_info
            .get(id)
            .map(|info| info.name_record.name_record.address)
    }

    fn get_known_addrs(&self) -> HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddrV4> {
        self.peer_info
            .iter()
            .map(|(id, info)| (*id, info.name_record.name_record.address))
            .collect()
    }

    fn get_name_records(
        &self,
    ) -> HashMap<NodeId<CertificateSignaturePubKey<ST>>, MonadNameRecord<ST>> {
        self.peer_info
            .iter()
            .map(|(id, info)| (*id, info.name_record))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddrV4};

    use alloy_rlp::Encodable;
    use monad_crypto::{
        NopKeyPair, NopSignature,
        certificate_signature::{CertificateKeyPair, CertificateSignature},
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
        let signature = SignatureType::sign(&encoded, keypair);
        MonadNameRecord {
            name_record,
            signature,
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

    fn extract_ping(cmds: Vec<PeerDiscoveryCommand<SignatureType>>) -> Vec<Ping<SignatureType>> {
        cmds.into_iter()
            .filter_map(|c| match c {
                PeerDiscoveryCommand::RouterCommand {
                    target: _,
                    message: PeerDiscoveryMessage::Ping(ping),
                } => Some(ping),
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
        let peer0_pubkey = NodeId::new(peer0.pubkey());
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());

        // peer1 name record is present but peer2 name record is not present
        let peer_info = BTreeMap::from([(peer1_pubkey, PeerInfo {
            last_ping: None,
            unresponsive_pings: 0,
            name_record: generate_name_record(peer1, 0),
        })]);
        let mut state = PeerDiscovery {
            self_id: peer0_pubkey,
            self_record: generate_name_record(peer0, 0),
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::new(),
            dedicated_full_nodes: BTreeSet::new(),
            peer_info,
            outstanding_lookup_requests: HashMap::new(),
            metrics: ExecutorMetrics::default(),
            ping_period: Duration::from_secs(60),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
            min_active_connections: 5,
            max_active_connections: 50,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        // should be able to send ping to peer where its name record already exists locally
        let cmds = state.send_ping(peer1_pubkey);
        // four reset timer cmds, one ping cmd
        assert_eq!(cmds.len(), 5);

        // last_ping is recorded correctly
        let peer_info = state.peer_info.get(&peer1_pubkey);
        assert!(peer_info.is_some());
        assert!(peer_info.unwrap().last_ping.is_some());
        assert_eq!(peer_info.unwrap().unresponsive_pings, 0);
        let ping_msg = peer_info.unwrap().last_ping.unwrap();

        // should not be able to send ping to peer where its name record does not exist locally
        let cmds = state.send_ping(peer2_pubkey);
        assert_eq!(cmds.len(), 0);

        // when corresponding pong is received from peer1, last_ping and last_seen is updated
        state.handle_pong(peer1_pubkey, Pong {
            ping_id: ping_msg.id,
            local_record_seq: 0,
        });
        let peer_info = state.peer_info.get(&peer1_pubkey);
        assert!(peer_info.is_some());
        assert!(peer_info.unwrap().last_ping.is_none());
        assert_eq!(peer_info.unwrap().unresponsive_pings, 0);
    }

    #[test]
    fn test_drop_pong_with_incorrect_ping_id() {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer0_pubkey = NodeId::new(peer0.pubkey());
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let peer_info = BTreeMap::from([(peer1_pubkey, PeerInfo {
            last_ping: None,
            unresponsive_pings: 0,
            name_record: generate_name_record(peer1, 0),
        })]);
        let mut state = PeerDiscovery {
            self_id: peer0_pubkey,
            self_record: generate_name_record(peer0, 0),
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::new(),
            dedicated_full_nodes: BTreeSet::new(),
            peer_info,
            outstanding_lookup_requests: HashMap::new(),
            metrics: ExecutorMetrics::default(),
            ping_period: Duration::from_secs(60),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
            min_active_connections: 5,
            max_active_connections: 50,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        // should not record pong when ping_id doesn't match
        state.handle_pong(peer1_pubkey, Pong {
            ping_id: 1,
            local_record_seq: 0,
        });
        let peer_info = state.peer_info.get(&peer1_pubkey);
        assert!(peer_info.is_some());
        assert!(peer_info.unwrap().last_ping.is_none());
        assert_eq!(peer_info.unwrap().unresponsive_pings, 0);
    }

    #[test]
    fn test_peer_lookup() {
        let keys = create_keys::<SignatureType>(3);
        let peer0 = &keys[0];
        let peer0_pubkey = NodeId::new(peer0.pubkey());
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());

        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_id: peer0_pubkey,
            self_record: generate_name_record(peer0, 0),
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::new(),
            dedicated_full_nodes: BTreeSet::new(),
            peer_info: BTreeMap::new(),
            outstanding_lookup_requests: HashMap::new(),
            metrics: ExecutorMetrics::default(),
            ping_period: Duration::from_secs(60),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
            min_active_connections: 5,
            max_active_connections: 50,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        let cmds = state.send_peer_lookup_request(peer1_pubkey, peer2_pubkey, false);
        assert_eq!(cmds.len(), 2);
        assert_eq!(state.outstanding_lookup_requests.keys().len(), 1);
        assert!(!state.peer_info.contains_key(&peer2_pubkey));
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
        assert!(!state.peer_info.contains_key(&peer2_pubkey));
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

        // peer2 should be added to peer info and outstanding requests should be cleared
        assert!(state.peer_info.contains_key(&peer2_pubkey));
        assert_eq!(state.outstanding_lookup_requests.keys().len(), 0);
    }

    #[test]
    fn test_peer_lookup_target_not_found() {
        let keys = create_keys::<SignatureType>(4);
        let peer0 = &keys[0];
        let peer0_pubkey = NodeId::new(peer0.pubkey());
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());
        let peer3 = &keys[3];
        let peer3_pubkey = NodeId::new(peer3.pubkey());

        // peer_info contains peer1 and peer2
        let peer_info = BTreeMap::from([
            (peer1_pubkey, PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(peer1, 0),
            }),
            (peer2_pubkey, PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(peer2, 0),
            }),
        ]);
        let mut epoch_validators = BTreeMap::new();
        epoch_validators.insert(Epoch(1), BTreeSet::from([peer1_pubkey, peer2_pubkey]));
        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_id: peer0_pubkey,
            self_record: generate_name_record(peer0, 0),
            current_epoch: Epoch(1),
            epoch_validators,
            dedicated_full_nodes: BTreeSet::new(),
            peer_info,
            outstanding_lookup_requests: HashMap::new(),
            metrics: ExecutorMetrics::default(),
            ping_period: Duration::from_secs(60),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
            min_active_connections: 5,
            max_active_connections: 50,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        // receive a peer lookup request for peer3, which is not in peer_info
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
        let peer0_pubkey = NodeId::new(peer0.pubkey());
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let peer_info = BTreeMap::from([(peer1_pubkey, PeerInfo {
            last_ping: None,
            unresponsive_pings: 0,
            name_record: generate_name_record(peer1, 1),
        })]);
        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_id: peer0_pubkey,
            self_record: generate_name_record(peer0, 0),
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::new(),
            dedicated_full_nodes: BTreeSet::new(),
            peer_info,
            outstanding_lookup_requests: HashMap::from([
                (1, LookupInfo {
                    num_retries: 0,
                    receiver: peer1_pubkey,
                    open_discovery: false,
                }),
                (2, LookupInfo {
                    num_retries: 0,
                    receiver: peer1_pubkey,
                    open_discovery: false,
                }),
            ]),
            metrics: ExecutorMetrics::default(),
            ping_period: Duration::from_secs(60),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
            min_active_connections: 5,
            max_active_connections: 50,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        // should not update name record if record has lower sequence number
        let record = generate_name_record(peer1, 0);
        state.handle_peer_lookup_response(peer1_pubkey, PeerLookupResponse {
            lookup_id: 1,
            target: peer1_pubkey,
            name_records: vec![record],
        });
        assert_eq!(
            state
                .peer_info
                .get(&peer1_pubkey)
                .unwrap()
                .name_record
                .seq(),
            1
        );

        // should update name record if record has higher sequence number
        let record = generate_name_record(peer1, 2);
        state.handle_peer_lookup_response(peer1_pubkey, PeerLookupResponse {
            lookup_id: 2,
            target: peer1_pubkey,
            name_records: vec![record],
        });
        assert_eq!(
            state
                .peer_info
                .get(&peer1_pubkey)
                .unwrap()
                .name_record
                .seq(),
            2
        );
    }

    #[test]
    fn test_drop_invalid_lookup_response() {
        let keys = create_keys::<SignatureType>(3);
        let peer0 = &keys[0];
        let peer0_pubkey = NodeId::new(peer0.pubkey());
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_id: peer0_pubkey,
            self_record: generate_name_record(peer0, 0),
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::new(),
            dedicated_full_nodes: BTreeSet::new(),
            peer_info: BTreeMap::new(),
            outstanding_lookup_requests: HashMap::new(),
            metrics: ExecutorMetrics::default(),
            ping_period: Duration::from_secs(60),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
            min_active_connections: 5,
            max_active_connections: 50,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        // should not record peer lookup response if not in outstanding requests
        let record = generate_name_record(peer1, 0);
        state.handle_peer_lookup_response(peer1_pubkey, PeerLookupResponse {
            lookup_id: 1,
            target: peer1_pubkey,
            name_records: vec![record],
        });
        assert!(!state.peer_info.contains_key(&peer1_pubkey));
    }

    #[test]
    fn test_drop_lookup_response_that_exceeds_max_peers() {
        let keys = create_keys::<SignatureType>(3);
        let peer0 = &keys[0];
        let peer0_pubkey = NodeId::new(peer0.pubkey());
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let lookup_id = 1;

        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_id: peer0_pubkey,
            self_record: generate_name_record(peer0, 0),
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::new(),
            dedicated_full_nodes: BTreeSet::new(),
            peer_info: BTreeMap::new(),
            outstanding_lookup_requests: HashMap::from([(lookup_id, LookupInfo {
                num_retries: 0,
                receiver: peer1_pubkey,
                open_discovery: false,
            })]),
            metrics: ExecutorMetrics::default(),
            ping_period: Duration::from_secs(60),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
            min_active_connections: 5,
            max_active_connections: 50,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        // should not record peer lookup response if number of records exceed max
        let record = generate_name_record(peer1, 0);
        state.handle_peer_lookup_response(peer1_pubkey, PeerLookupResponse {
            lookup_id,
            target: peer1_pubkey,
            name_records: vec![record; MAX_PEER_IN_RESPONSE + 1],
        });
        assert!(!state.peer_info.contains_key(&peer1_pubkey));
    }

    #[test]
    fn test_prune() {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer0_pubkey = NodeId::new(peer0.pubkey());
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let lookup_id = 1;
        let peer_info = BTreeMap::from([(peer1_pubkey, PeerInfo {
            last_ping: None,
            unresponsive_pings: 5,
            name_record: generate_name_record(peer1, 0),
        })]);
        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_id: peer0_pubkey,
            self_record: generate_name_record(peer0, 0),
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::new(),
            dedicated_full_nodes: BTreeSet::new(),
            peer_info,
            outstanding_lookup_requests: HashMap::from([(lookup_id, LookupInfo {
                num_retries: 5,
                receiver: peer1_pubkey,
                open_discovery: false,
            })]),
            metrics: ExecutorMetrics::default(),
            ping_period: Duration::from_secs(60),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 3,
            min_active_connections: 5,
            max_active_connections: 50,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        state.refresh();
        assert!(state.peer_info.is_empty());

        state.handle_peer_lookup_timeout(peer1_pubkey, peer1_pubkey, lookup_id);
        assert!(state.outstanding_lookup_requests.is_empty());
    }

    #[test]
    fn test_below_min_active_connections() {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer0_pubkey = NodeId::new(peer0.pubkey());
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let peer_info = BTreeMap::from([(peer1_pubkey, PeerInfo {
            last_ping: None,
            unresponsive_pings: 0,
            name_record: generate_name_record(peer1, 0),
        })]);
        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_id: peer0_pubkey,
            self_record: generate_name_record(peer0, 0),
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::new(),
            dedicated_full_nodes: BTreeSet::new(),
            peer_info,
            outstanding_lookup_requests: HashMap::new(),
            metrics: ExecutorMetrics::default(),
            ping_period: Duration::from_secs(60),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 3,
            min_active_connections: 5,
            max_active_connections: 50,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

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
    fn test_above_max_active_connections() {
        let keys = create_keys::<SignatureType>(4);
        let peer0 = &keys[0];
        let peer0_pubkey = NodeId::new(peer0.pubkey());
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());
        let peer3 = &keys[3];
        let peer3_pubkey = NodeId::new(peer3.pubkey());

        let peer_info = BTreeMap::from([
            (peer1_pubkey, PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(peer1, 0),
            }),
            (peer2_pubkey, PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(peer2, 0),
            }),
            (peer3_pubkey, PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(peer3, 0),
            }),
        ]);
        // Peer1 in validator set, Peer2 is dedicated full node
        let mut epoch_validators = BTreeMap::new();
        epoch_validators.insert(Epoch(1), BTreeSet::from([peer1_pubkey]));
        let dedicated_full_nodes = BTreeSet::from([peer2_pubkey]);

        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_id: peer0_pubkey,
            self_record: generate_name_record(peer0, 0),
            current_epoch: Epoch(1),
            epoch_validators,
            dedicated_full_nodes,
            peer_info,
            outstanding_lookup_requests: HashMap::new(),
            metrics: ExecutorMetrics::default(),
            ping_period: Duration::from_secs(60),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 3,
            min_active_connections: 0,
            max_active_connections: 1,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        // prune nodes, but validators and dedicated full nodes are not pruned even if above max active connections
        state.refresh();
        assert!(state.peer_info.contains_key(&peer1_pubkey));
        assert!(state.peer_info.contains_key(&peer2_pubkey));
        assert!(!state.peer_info.contains_key(&peer3_pubkey));
    }

    #[test]
    fn test_ping_unseen_peer() {
        let keys = create_keys::<SignatureType>(2);
        let node0_key = &keys[0];
        let node0 = NodeId::new(node0_key.pubkey());
        let node1_key = &keys[1];
        let node1 = NodeId::new(node1_key.pubkey());

        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_id: node0,
            self_record: generate_name_record(node0_key, 0),
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::new(),
            dedicated_full_nodes: BTreeSet::new(),
            peer_info: BTreeMap::from([(node1, PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: generate_name_record(node1_key, 1),
            })]),
            outstanding_lookup_requests: HashMap::new(),
            metrics: ExecutorMetrics::default(),
            ping_period: Duration::from_secs(60),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 3,
            min_active_connections: 5,
            max_active_connections: 50,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        let cmds = state.send_ping(node1);
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

        let ping = extract_ping(cmds)[0];

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
        let node0_key = &keys[0];
        let node0 = NodeId::new(node0_key.pubkey());
        let node1_key = &keys[1];
        let node1 = NodeId::new(node1_key.pubkey());

        let peer_info = match known_record {
            Some(record) => BTreeMap::from([(node1, PeerInfo {
                last_ping: None,
                unresponsive_pings: 0,
                name_record: MonadNameRecord::new(record, node1_key),
            })]),
            None => BTreeMap::new(),
        };

        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_id: node0,
            self_record: generate_name_record(node0_key, 0),
            current_epoch: Epoch(1),
            epoch_validators: BTreeMap::new(),
            dedicated_full_nodes: BTreeSet::new(),
            peer_info,
            outstanding_lookup_requests: HashMap::new(),
            metrics: ExecutorMetrics::default(),
            ping_period: Duration::from_secs(60),
            refresh_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 3,
            min_active_connections: 5,
            max_active_connections: 50,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        let cmds = state.handle_ping(node1, Ping {
            id: 7,
            local_name_record: Some(MonadNameRecord::new(incoming_record, node1_key)),
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

            let node1_record = state.peer_info.get(&node1).unwrap().name_record.name_record;
            assert_eq!(expected_record, node1_record);
        } else {
            assert!(cmds.is_empty());
        }
    }
}
