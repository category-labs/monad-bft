use std::{
    collections::{BTreeMap, HashMap},
    time::Instant,
};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::NodeId;
use rand::random;
use tracing::{debug, warn};

use crate::{
    MonadNameRecord, PeerDiscMetrics, PeerDiscoveryAlgo, PeerDiscoveryCommand,
    PeerDiscoveryMessage, PeerLookupRequest, PeerLookupResponse, Ping, Pong,
};

#[derive(Debug, Clone)]
pub struct PeerInfo<ST: CertificateSignatureRecoverable> {
    pub last_ping: Option<Ping>,
    pub last_seen: Option<Instant>,
    pub name_record: MonadNameRecord<ST>,
}

#[derive(Default)]
pub struct PeerDiscovery<ST: CertificateSignatureRecoverable> {
    pub local_record_seq: u64,
    pub peer_info: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, PeerInfo<ST>>,
    // maps lookup id => receiver of the request
    pub outstanding_lookup_requests: HashMap<u32, NodeId<CertificateSignaturePubKey<ST>>>,
    pub metrics: PeerDiscMetrics,
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
        let peer_entry = match self.peer_info.get_mut(&to) {
            Some(entry) => entry,
            None => {
                warn!(
                    ?to,
                    "name record not present locally but trying to send ping"
                );
                return cmds;
            }
        };

        let ping_msg = Ping {
            id: random(),
            local_record_seq: self.local_record_seq,
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
            .map(|info| info.name_record.seq < ping_msg.local_record_seq)
            .unwrap_or(true)
        {
            // TODO: cmd that sends PeerLookupRequest
        }

        // respond to ping
        let pong_msg = Pong {
            ping_id: ping_msg.id,
            local_record_seq: self.local_record_seq,
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
        self.peer_info.entry(from).and_modify(|info| {
            if info.last_ping.as_ref().map(|last_ping| last_ping.id) == Some(pong_msg.ping_id) {
                info.last_ping = None;
                info.last_seen = Some(Instant::now());
            } else {
                debug!(?from, "dropping pong because ping id does not match");
            }
        });

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
            lookup_id: random(),
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
                name_records: vec![(target, info.name_record.clone())],
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
            .filter(|&peer| peer == &from)
            .is_none()
        {
            warn!(
                ?response,
                "peer lookup response not in oustanding requests, dropping requests..."
            );
            return cmds;
        }

        // update peer info
        for (node_id, name_record) in response.name_records {
            self.peer_info
                .entry(node_id)
                .and_modify(|info| info.name_record = name_record.clone())
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
}

#[cfg(test)]
mod tests {
    use std::{
        net::{SocketAddr, SocketAddrV4},
        str::FromStr,
    };

    use monad_crypto::{
        NopKeyPair, NopSignature,
        certificate_signature::{CertificateKeyPair, CertificateSignature},
    };
    use monad_testutil::signing::create_keys;
    use monad_types::NodeId;

    use super::*;

    type KeyPairType = NopKeyPair;
    type SignatureType = NopSignature;

    fn generate_name_record(keypair: &KeyPairType) -> MonadNameRecord<SignatureType> {
        let msg = b"test";
        let signature = SignatureType::sign(msg, keypair);
        MonadNameRecord {
            address: SocketAddr::V4(SocketAddrV4::from_str("1.1.1.1:8000").unwrap()),
            seq: 0,
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
        let keys = create_keys::<SignatureType>(2);
        let peer1 = &keys[0];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[1];
        let peer2_pubkey = NodeId::new(peer2.pubkey());

        // peer1 name record is present but peer2 name record is not present
        let peer_info = BTreeMap::from([(peer1_pubkey, PeerInfo {
            last_ping: None,
            last_seen: None,
            name_record: generate_name_record(peer1),
        })]);
        let mut state = PeerDiscovery {
            local_record_seq: 0,
            peer_info,
            outstanding_lookup_requests: HashMap::new(),
            metrics: HashMap::new(),
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
    fn test_peer_lookup() {
        let keys = create_keys::<SignatureType>(2);
        let peer1 = &keys[0];
        let peer1_pubkey = NodeId::new(peer1.pubkey());
        let peer2 = &keys[1];
        let peer2_pubkey = NodeId::new(peer2.pubkey());

        let mut state: PeerDiscovery<SignatureType> = PeerDiscovery {
            local_record_seq: 0,
            peer_info: BTreeMap::new(),
            outstanding_lookup_requests: HashMap::new(),
            metrics: HashMap::new(),
        };

        let cmds = state.send_peer_lookup_request(peer1_pubkey, peer2_pubkey);
        assert_eq!(cmds.len(), 1);
        assert_eq!(state.outstanding_lookup_requests.keys().len(), 1);
        assert!(!state.peer_info.contains_key(&peer2_pubkey));
        let requests = extract_lookup_requests(cmds);

        let record = generate_name_record(peer2);
        state.handle_peer_lookup_response(peer1_pubkey, PeerLookupResponse {
            lookup_id: requests[0].lookup_id,
            target: peer2_pubkey,
            name_records: vec![(peer2_pubkey, record)],
        });

        // peer2 should be added to peer info and outstanding requests should be cleared
        assert!(state.peer_info.contains_key(&peer2_pubkey));
        assert_eq!(state.outstanding_lookup_requests.keys().len(), 0);
    }
}
