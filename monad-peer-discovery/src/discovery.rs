use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddrV4,
    time::Duration,
};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::NodeId;
use rand::{RngCore, SeedableRng, seq::SliceRandom};
use rand_chacha::ChaCha8Rng;
use tracing::{debug, warn};

use crate::{
    MonadNameRecord, PeerDiscMetrics, PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder,
    PeerDiscoveryCommand, PeerDiscoveryEvent, PeerDiscoveryMessage, PeerDiscoveryTimerCommand,
    PeerLookupRequest, PeerLookupResponse, Ping, Pong, TimerKind,
};

/// Maximum number of peers to be included in a PeerLookupResponse
const MAX_PEER_IN_RESPONSE: usize = 16;

#[derive(Debug, Clone, Copy)]
pub struct PeerInfo<ST: CertificateSignatureRecoverable> {
    pub last_ping: Option<Ping>,
    pub unresponsive_pings: u32,
    pub name_record: MonadNameRecord<ST>,
}

#[derive(Debug, Clone, Copy)]
pub struct LookupInfo<ST: CertificateSignatureRecoverable> {
    pub num_retries: u32,
    pub receiver: NodeId<CertificateSignaturePubKey<ST>>,
}

pub struct PeerDiscovery<ST: CertificateSignatureRecoverable> {
    pub self_id: NodeId<CertificateSignaturePubKey<ST>>,
    pub self_record: MonadNameRecord<ST>,
    pub peer_info: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, PeerInfo<ST>>,
    pub outstanding_lookup_requests: HashMap<u32, LookupInfo<ST>>,
    pub metrics: PeerDiscMetrics,
    pub ping_period: Duration,
    pub prune_period: Duration,
    pub request_timeout: Duration,
    // number of unresponsive pings allowed before being pruned
    pub prune_threshold: u32,
    pub rng: ChaCha8Rng,
}

pub struct PeerDiscoveryBuilder<ST: CertificateSignatureRecoverable> {
    pub self_id: NodeId<CertificateSignaturePubKey<ST>>,
    pub self_record: MonadNameRecord<ST>,
    pub peer_info: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, PeerInfo<ST>>,
    pub ping_period: Duration,
    pub prune_period: Duration,
    pub request_timeout: Duration,
    pub prune_threshold: u32,
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
        assert!(self.ping_period > self.request_timeout);

        let mut state = PeerDiscovery {
            self_id: self.self_id,
            self_record: self.self_record,
            peer_info: self.peer_info.clone(),
            outstanding_lookup_requests: Default::default(),
            metrics: Default::default(),
            ping_period: self.ping_period,
            prune_period: self.prune_period,
            request_timeout: self.request_timeout,
            prune_threshold: self.prune_threshold,
            rng: ChaCha8Rng::seed_from_u64(self.rng_seed),
        };

        let mut cmds = self
            .peer_info
            .into_iter()
            .flat_map(|(node_id, _)| state.send_ping(node_id))
            .collect::<Vec<_>>();

        cmds.extend(state.reset_prune_timer());

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

    fn reset_prune_timer(&self) -> Vec<PeerDiscoveryCommand<ST>> {
        vec![
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::ScheduleReset {
                node_id: self.self_id,
                timer_kind: TimerKind::Prune,
            }),
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::Schedule {
                node_id: self.self_id,
                timer_kind: TimerKind::Prune,
                duration: self.prune_period,
                on_timeout: PeerDiscoveryEvent::Prune,
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

        let ping_msg = Ping {
            id: self.rng.next_u32(),
            local_record_seq: self.self_record.name_record.seq,
        };

        peer_entry.last_ping = Some(ping_msg);

        // reset timer to schedule for the next ping
        cmds.extend(self.reset_ping_timer(to, ping_msg.id));

        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: to,
            message: PeerDiscoveryMessage::Ping(ping_msg),
        });

        *self.metrics.entry("send_ping").or_default() += 1;
        cmds
    }

    fn handle_ping(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        ping_msg: Ping,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?ping_msg, "handling ping request");
        *self.metrics.entry("recv_ping").or_default() += 1;

        let mut cmds = Vec::new();

        // if sender of ping does not exist in local record or has higher sequence number
        // send a PeerLookupRequest to get the node information subsequently
        if self
            .peer_info
            .get(&from)
            .is_none_or(|info| info.name_record.seq() < ping_msg.local_record_seq)
        {
            debug!(
                target=?from, "Ping sender not found in local record, sending peer lookup request to target",
            );
            // sends PeerLookupRequest to ping sender
            cmds.extend(self.send_peer_lookup_request(from, from));
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

        *self.metrics.entry("send_pong").or_default() += 1;
        cmds
    }

    fn handle_pong(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        pong_msg: Pong,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?pong_msg, "handling pong response");
        *self.metrics.entry("recv_pong").or_default() += 1;

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
            }
        } else {
            debug!(
                ?from,
                "dropping pong, ping sender does not exist in peer info"
            );
        }

        cmds
    }

    fn handle_ping_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        ping_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, ?ping_id, "handling ping timeout");

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
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, ?target, "sending peer lookup request");

        let mut cmds = Vec::new();

        // new lookup request
        let mut lookup_id = self.rng.next_u32();
        // make sure lookup id is unique
        while self.outstanding_lookup_requests.contains_key(&lookup_id) {
            lookup_id = self.rng.next_u32();
        }
        self.outstanding_lookup_requests
            .insert(lookup_id, LookupInfo {
                num_retries: 0,
                receiver: to,
            });
        let peer_lookup_request = PeerLookupRequest::<ST> { lookup_id, target };

        // schedule for peer lookup retry
        cmds.extend(self.schedule_lookup_timer(to, target, lookup_id));

        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: to,
            message: PeerDiscoveryMessage::PeerLookupRequest(peer_lookup_request),
        });

        *self.metrics.entry("send_lookup_request").or_default() += 1;
        cmds
    }

    fn handle_peer_lookup_request(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        request: PeerLookupRequest<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?request, "handling peer lookup request");
        *self.metrics.entry("recv_lookup_request").or_default() += 1;

        let mut cmds = Vec::new();
        let target = request.target;
        let name_records = if target == self.self_id {
            vec![self.self_record]
        } else {
            match self.peer_info.get(&target) {
                Some(info) => vec![info.name_record],
                None => {
                    // return random subset of peers up to MAX_PEER_IN_RESPONSE
                    let mut peers: Vec<_> = self.peer_info.iter().collect();
                    peers.shuffle(&mut self.rng);
                    peers
                        .into_iter()
                        .take(MAX_PEER_IN_RESPONSE)
                        .map(|(_k, v)| v.name_record)
                        .collect()
                }
            }
        };

        let peer_lookup_response = PeerLookupResponse {
            lookup_id: request.lookup_id,
            target,
            name_records,
        };

        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: from,
            message: PeerDiscoveryMessage::PeerLookupResponse(peer_lookup_response),
        });

        *self.metrics.entry("send_lookup_response").or_default() += 1;
        cmds
    }

    fn handle_peer_lookup_response(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        response: PeerLookupResponse<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?response, "handling peer lookup response");
        *self.metrics.entry("recv_lookup_response").or_default() += 1;

        let cmds = Vec::new();

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
            return cmds;
        }

        if response.name_records.len() > MAX_PEER_IN_RESPONSE {
            warn!(
                ?response,
                "response includes number of peers larger than max, dropping response..."
            );
            return cmds;
        }

        // update peer info
        for name_record in response.name_records {
            // verify signature of name record
            let node_id = match name_record.recover_pubkey() {
                Ok(node_id) => node_id,
                Err(e) => {
                    warn!(?e, "invalid name record signature, dropping record...");
                    continue;
                }
            };

            // insert name record
            self.peer_info
                .entry(node_id)
                .and_modify(|info| {
                    if name_record.seq() > info.name_record.seq() {
                        info.name_record = name_record;
                    }
                })
                .or_insert_with(|| PeerInfo {
                    last_ping: None,
                    unresponsive_pings: 0,
                    name_record,
                });
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
        debug!(?lookup_id, "peer lookup request timeout");

        let mut cmds = Vec::new();

        // retry lookup request
        let Some(lookup_info) = self.outstanding_lookup_requests.get_mut(&lookup_id) else {
            debug!("lookup id not found in outstanding lookup requests");
            return cmds;
        };
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
                receiver: target,
            });

        let peer_lookup_request = PeerLookupRequest::<ST> {
            lookup_id: new_lookup_id,
            target,
        };

        // schedule for next peer lookup retry
        cmds.extend(self.schedule_lookup_timer(to, target, new_lookup_id));

        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: to,
            message: PeerDiscoveryMessage::PeerLookupRequest(peer_lookup_request),
        });

        *self.metrics.entry("retry_lookup_request").or_default() += 1;

        cmds
    }

    fn prune(&mut self) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!("pruning unresponsive peer nodes");

        *self.metrics.entry("prune").or_default() += 1;
        let mut cmds = Vec::new();

        // prune unresponsive nodes
        self.peer_info
            .retain(|_, info| info.unresponsive_pings < self.prune_threshold);

        // reset timer to schedule for the next prune
        cmds.extend(self.reset_prune_timer());

        cmds
    }

    fn metrics(&self) -> &PeerDiscMetrics {
        &self.metrics
    }

    fn get_sock_addr_by_id(
        &self,
        id: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<SocketAddrV4> {
        self.peer_info
            .get(&id)
            .map(|info| info.name_record.name_record.address)
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddrV4, str::FromStr};

    use alloy_rlp::Encodable;
    use monad_crypto::{
        NopKeyPair, NopSignature,
        certificate_signature::{CertificateKeyPair, CertificateSignature},
    };
    use monad_testutil::signing::create_keys;
    use monad_types::NodeId;
    use rand::SeedableRng;

    use super::*;
    use crate::NameRecord;

    type KeyPairType = NopKeyPair;
    type SignatureType = NopSignature;

    fn generate_name_record(keypair: &KeyPairType, seq_num: u64) -> MonadNameRecord<SignatureType> {
        let name_record = NameRecord {
            address: SocketAddrV4::from_str("1.1.1.1:8000").unwrap(),
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
            peer_info,
            outstanding_lookup_requests: HashMap::new(),
            metrics: HashMap::new(),
            ping_period: Duration::from_secs(60),
            prune_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
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
            peer_info,
            outstanding_lookup_requests: HashMap::new(),
            metrics: HashMap::new(),
            ping_period: Duration::from_secs(60),
            prune_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
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
            peer_info: BTreeMap::new(),
            outstanding_lookup_requests: HashMap::new(),
            metrics: HashMap::new(),
            ping_period: Duration::from_secs(60),
            prune_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        let cmds = state.send_peer_lookup_request(peer1_pubkey, peer2_pubkey);
        assert_eq!(cmds.len(), 2);
        assert_eq!(state.outstanding_lookup_requests.keys().len(), 1);
        assert!(!state.peer_info.contains_key(&peer2_pubkey));
        let requests = extract_lookup_requests(cmds);

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
        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_id: peer0_pubkey,
            self_record: generate_name_record(peer0, 0),
            peer_info,
            outstanding_lookup_requests: HashMap::new(),
            metrics: HashMap::new(),
            ping_period: Duration::from_secs(60),
            prune_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        // receive a peer lookup request for peer3, which is not in peer_info
        let cmds = state.handle_peer_lookup_request(peer1_pubkey, PeerLookupRequest {
            lookup_id: 1,
            target: peer3_pubkey,
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
            peer_info,
            outstanding_lookup_requests: HashMap::from([
                (1, LookupInfo {
                    num_retries: 0,
                    receiver: peer1_pubkey,
                }),
                (2, LookupInfo {
                    num_retries: 0,
                    receiver: peer1_pubkey,
                }),
            ]),
            metrics: HashMap::new(),
            ping_period: Duration::from_secs(60),
            prune_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
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
            peer_info: BTreeMap::new(),
            outstanding_lookup_requests: HashMap::new(),
            metrics: HashMap::new(),
            ping_period: Duration::from_secs(60),
            prune_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
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
            peer_info: BTreeMap::new(),
            outstanding_lookup_requests: HashMap::from([(lookup_id, LookupInfo {
                num_retries: 0,
                receiver: peer1_pubkey,
            })]),
            metrics: HashMap::new(),
            ping_period: Duration::from_secs(60),
            prune_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 10,
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
            peer_info,
            outstanding_lookup_requests: HashMap::from([(lookup_id, LookupInfo {
                num_retries: 5,
                receiver: peer1_pubkey,
            })]),
            metrics: HashMap::new(),
            ping_period: Duration::from_secs(60),
            prune_period: Duration::from_secs(120),
            request_timeout: Duration::from_secs(5),
            prune_threshold: 3,
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        state.prune();
        assert!(state.peer_info.is_empty());

        state.handle_peer_lookup_timeout(peer1_pubkey, peer1_pubkey, lookup_id);
        assert!(state.outstanding_lookup_requests.is_empty());
    }
}
