use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddrV4,
    time::Instant,
};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::NodeId;
use rand::RngCore;
use rand_chacha::ChaCha8Rng;
use tracing::{debug, warn};

use crate::{
    MonadNameRecord, PeerDiscMetrics, PeerDiscoveryAlgo, PeerDiscoveryCommand,
    PeerDiscoveryMessage, PeerLookupRequest, PeerLookupResponse, Ping, Pong,
};

const MAX_PEER_IN_RESPONSE: usize = 16;

#[derive(Debug, Clone)]
pub struct PeerInfo<ST: CertificateSignatureRecoverable> {
    pub last_ping: Option<Ping>,
    pub last_seen: Option<Instant>,
    pub name_record: MonadNameRecord<ST>,
}

pub struct PeerDiscovery<ST: CertificateSignatureRecoverable> {
    pub self_record: MonadNameRecord<ST>,
    pub peer_info: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, PeerInfo<ST>>,
    // maps lookup id => receiver of the request
    pub outstanding_lookup_requests: HashMap<u32, NodeId<CertificateSignaturePubKey<ST>>>,
    pub metrics: PeerDiscMetrics,
    pub rng: ChaCha8Rng,
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

        // record ping in peer_info
        peer_entry.last_ping = Some(ping_msg);

        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: to,
            message: PeerDiscoveryMessage::Ping(ping_msg),
        });

        cmds
    }

    fn handle_ping(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        ping_msg: Ping,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?ping_msg, "handling ping request");

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

        cmds
    }

    fn handle_pong(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        pong_msg: Pong,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?pong_msg, "handling pong response");

        let cmds = Vec::new();

        // if ping id matches, update last_seen time
        if let Some(info) = self.peer_info.get_mut(&from) {
            if info
                .last_ping
                .is_some_and(|last_ping| last_ping.id == pong_msg.ping_id)
            {
                info.last_ping = None;
                info.last_seen = Some(Instant::now());
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

    fn send_peer_lookup_request(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, ?target, "sending peer lookup request");

        let mut cmds = Vec::new();
        let peer_lookup_request = PeerLookupRequest::<ST> {
            lookup_id: self.rng.next_u32(),
            target,
        };

        // record request
        self.outstanding_lookup_requests
            .insert(peer_lookup_request.lookup_id, to);

        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: to,
            message: PeerDiscoveryMessage::PeerLookupRequest(peer_lookup_request),
        });
        cmds
    }

    fn handle_peer_lookup_request(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        request: PeerLookupRequest<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?request, "handling peer lookup request");

        let mut cmds = Vec::new();
        let target = request.target;
        let peer_lookup_response = match self.peer_info.get(&target) {
            Some(info) => PeerLookupResponse {
                lookup_id: request.lookup_id,
                target,
                name_records: vec![info.name_record.clone()],
            },
            None => {
                // TODO: optional to return some random peers
                PeerLookupResponse {
                    lookup_id: request.lookup_id,
                    target,
                    name_records: vec![],
                }
            }
        };

        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: from,
            message: PeerDiscoveryMessage::PeerLookupResponse(peer_lookup_response),
        });

        cmds
    }

    fn handle_peer_lookup_response(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        response: PeerLookupResponse<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?response, "handling peer lookup response");

        let cmds = Vec::new();

        // if lookup id is not in outstanding requests, drop the response
        if self
            .outstanding_lookup_requests
            .get(&response.lookup_id)
            .is_none_or(|peer| peer != &from)
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
                        info.name_record = name_record.clone();
                    }
                })
                .or_insert_with(|| PeerInfo {
                    last_ping: None,
                    last_seen: None,
                    name_record,
                });
        }

        // drop from outstanding requests
        self.outstanding_lookup_requests.remove(&response.lookup_id);

        cmds
    }

    // TODO: record metrics when handling event
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

    #[test]
    fn test_check_peer_connection() {
        let keys = create_keys::<SignatureType>(3);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());

        // peer1 name record is present but peer2 name record is not present
        let peer_info = BTreeMap::from([(peer1_pubkey, PeerInfo {
            last_ping: None,
            last_seen: None,
            name_record: generate_name_record(peer1, 0),
        })]);
        let mut state = PeerDiscovery {
            self_record: generate_name_record(peer0, 0),
            peer_info,
            outstanding_lookup_requests: HashMap::new(),
            metrics: HashMap::new(),
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        // should be able to send ping to peer where its name record already exists locally
        let cmds = state.send_ping(peer1_pubkey);
        assert_eq!(cmds.len(), 1);

        // last_ping is recorded correctly
        let peer_info = state.peer_info.get(&peer1_pubkey);
        assert!(peer_info.is_some());
        assert!(peer_info.unwrap().last_ping.is_some());
        assert!(peer_info.unwrap().last_seen.is_none());
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
        assert!(peer_info.unwrap().last_seen.is_some());
    }

    #[test]
    fn test_drop_pong_with_incorrect_ping_id() {
        let keys = create_keys::<SignatureType>(2);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let peer_info = BTreeMap::from([(peer1_pubkey, PeerInfo {
            last_ping: None,
            last_seen: None,
            name_record: generate_name_record(peer1, 0),
        })]);
        let mut state = PeerDiscovery {
            self_record: generate_name_record(peer0, 0),
            peer_info,
            outstanding_lookup_requests: HashMap::new(),
            metrics: HashMap::new(),
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
        assert!(peer_info.unwrap().last_seen.is_none());
    }

    #[test]
    fn test_peer_lookup() {
        let keys = create_keys::<SignatureType>(3);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[2];
        let peer2_pubkey = NodeId::new(peer2.pubkey());

        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_record: generate_name_record(peer0, 0),
            peer_info: BTreeMap::new(),
            outstanding_lookup_requests: HashMap::new(),
            metrics: HashMap::new(),
            rng: ChaCha8Rng::seed_from_u64(123456),
        };

        let cmds = state.send_peer_lookup_request(peer1_pubkey, peer2_pubkey);
        assert_eq!(cmds.len(), 1);
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
    fn test_update_name_record_sequence_number() {
        let keys = create_keys::<SignatureType>(3);
        let peer0 = &keys[0];
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let peer_info = BTreeMap::from([(peer1_pubkey, PeerInfo {
            last_ping: None,
            last_seen: None,
            name_record: generate_name_record(peer1, 1),
        })]);
        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_record: generate_name_record(peer0, 0),
            peer_info,
            outstanding_lookup_requests: HashMap::from([(1, peer1_pubkey), (2, peer1_pubkey)]),
            metrics: HashMap::new(),
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
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());

        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_record: generate_name_record(peer0, 0),
            peer_info: BTreeMap::new(),
            outstanding_lookup_requests: HashMap::new(),
            metrics: HashMap::new(),
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
        let peer1 = &keys[1];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let lookup_id = 1;

        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            self_record: generate_name_record(peer0, 0),
            peer_info: BTreeMap::new(),
            outstanding_lookup_requests: HashMap::from([(lookup_id, peer1_pubkey)]),
            metrics: HashMap::new(),
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
}
