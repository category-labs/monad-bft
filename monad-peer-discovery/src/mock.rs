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
    collections::{BTreeSet, HashMap},
    marker::PhantomData,
    mem,
    net::SocketAddrV4,
    sync::RwLock,
};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::ExecutorMetrics;
use monad_executor_glue::PeerEntry;
use monad_types::{Epoch, NodeId, Round};
use tracing::debug;

use crate::{
    MonadNameRecord, NameRecord, NodeAddrLookup, PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder,
    PeerDiscoveryCommand, PeerDiscoveryEvent, PeerLookupRequest, PeerLookupResponse, PeerTable,
    Ping, Pong, Role,
};

pub struct NopDiscovery<ST: CertificateSignatureRecoverable> {
    peer_table: RwLock<PeerTable<ST>>,
    metrics: ExecutorMetrics,
    events: RwLock<Vec<PeerDiscoveryEvent<ST>>>,
    pd: PhantomData<ST>,
}

pub struct NopDiscoveryBuilder<ST: CertificateSignatureRecoverable> {
    pub known_addresses: HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddrV4>,
    pub pd: PhantomData<ST>,
}

impl<ST: CertificateSignatureRecoverable> Default for NopDiscoveryBuilder<ST> {
    fn default() -> Self {
        Self {
            known_addresses: HashMap::new(),
            pd: PhantomData,
        }
    }
}

impl<ST: CertificateSignatureRecoverable> PeerDiscoveryAlgoBuilder for NopDiscoveryBuilder<ST> {
    type PeerDiscoveryAlgoType = NopDiscovery<ST>;

    fn build(
        self,
    ) -> (
        Self::PeerDiscoveryAlgoType,
        Vec<PeerDiscoveryCommand<<Self::PeerDiscoveryAlgoType as NodeAddrLookup>::SignatureType>>,
    ) {
        let make_name_rec = |address| {
            let name_record = NameRecord { address, seq: 0 };
            // A dummy signature for tests where
            // only the addresses matter. For tests that requires
            // valid signatures, create an empty `NopDiscovery`
            // and insert the records with `update_peers` method.
            let signature = unsafe { mem::zeroed() };
            MonadNameRecord {
                name_record,
                signature,
            }
        };
        let name_records: HashMap<_, _> = self
            .known_addresses
            .into_iter()
            .map(|(node_id, addr)| (node_id, make_name_rec(addr)))
            .collect();

        let state = NopDiscovery {
            peer_table: RwLock::new(PeerTable::from_name_records(name_records)),
            metrics: ExecutorMetrics::default(),
            events: RwLock::new(Vec::new()),
            pd: PhantomData,
        };
        let cmds = Vec::new();

        (state, cmds)
    }
}

impl<ST: CertificateSignatureRecoverable> NodeAddrLookup for NopDiscovery<ST> {
    type SignatureType = ST;

    fn name_records(
        &self,
    ) -> HashMap<
        NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        MonadNameRecord<Self::SignatureType>,
    > {
        self.peer_table.read().unwrap().name_records()
    }

    fn lookup_name_record(
        &self,
        id: &NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<MonadNameRecord<Self::SignatureType>> {
        self.peer_table.read().unwrap().lookup_name_record(id)
    }

    fn role(
        &self,
        node_id: &NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<Role> {
        self.peer_table.read().unwrap().role(node_id)
    }
}

impl<ST> PeerDiscoveryAlgo for NopDiscovery<ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn send_ping(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, "handle send ping");
        self.push_event(PeerDiscoveryEvent::SendPing { to });
        Vec::new()
    }

    fn handle_ping(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        ping: Ping<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?ping, "handle ping");
        self.push_event(PeerDiscoveryEvent::PingRequest { from, ping });
        Vec::new()
    }

    fn handle_pong(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        pong: Pong,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?pong, "handle pong");
        self.push_event(PeerDiscoveryEvent::PongResponse { from, pong });
        Vec::new()
    }

    fn handle_ping_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        ping_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, ?ping_id, "handling ping timeout");
        self.push_event(PeerDiscoveryEvent::PingTimeout { to, ping_id });
        Vec::new()
    }

    fn send_peer_lookup_request(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
        open_discovery: bool,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, ?target, "sending peer lookup request");
        self.push_event(PeerDiscoveryEvent::SendPeerLookup {
            to,
            target,
            open_discovery,
        });
        Vec::new()
    }

    fn handle_peer_lookup_request(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        request: PeerLookupRequest<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?request, "handling peer lookup request");
        self.push_event(PeerDiscoveryEvent::PeerLookupRequest { from, request });
        Vec::new()
    }

    fn handle_peer_lookup_response(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        response: PeerLookupResponse<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?response, "handling peer lookup response");
        self.push_event(PeerDiscoveryEvent::PeerLookupResponse { from, response });
        Vec::new()
    }

    fn handle_peer_lookup_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
        lookup_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?lookup_id, "peer lookup request timeout");
        self.push_event(PeerDiscoveryEvent::PeerLookupTimeout {
            to,
            target,
            lookup_id,
        });
        Vec::new()
    }

    fn refresh(&mut self) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!("pruning unresponsive peer nodes");
        self.push_event(PeerDiscoveryEvent::Refresh);
        Vec::new()
    }

    fn update_current_round(
        &mut self,
        round: Round,
        epoch: Epoch,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?round, ?epoch, "updating current round");
        self.push_event(PeerDiscoveryEvent::UpdateCurrentRound { round, epoch });
        Vec::new()
    }

    fn update_validator_set(
        &mut self,
        epoch: Epoch,
        validators: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!("updating validator set");
        self.push_event(PeerDiscoveryEvent::UpdateValidatorSet { epoch, validators });
        Vec::new()
    }

    fn update_peers(&mut self, peers: Vec<PeerEntry<ST>>) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!("updating peers");
        self.push_event(PeerDiscoveryEvent::UpdatePeers {
            peers: peers.clone(),
        });

        let mut name_records = self.peer_table.read().unwrap().name_records();
        for peer in peers {
            let node_id = NodeId::new(peer.pubkey);
            let name_record = MonadNameRecord {
                name_record: NameRecord {
                    address: peer.addr,
                    seq: peer.record_seq_num,
                },
                signature: peer.signature,
            };
            name_records.insert(node_id, name_record);
        }

        *self.peer_table.write().unwrap() = PeerTable::from_name_records(name_records);

        Vec::new()
    }

    fn update_peer_participation(
        &mut self,
        end_round: Round,
        peers: BTreeSet<NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?end_round, ?peers, "updating peer participation");
        self.push_event(PeerDiscoveryEvent::UpdateConfirmGroup { end_round, peers });
        Vec::new()
    }

    fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    fn peer_table(&self) -> crate::PeerTable<ST> {
        self.peer_table.read().unwrap().clone()
    }
}

impl<ST: CertificateSignatureRecoverable> NopDiscovery<ST> {
    pub fn insert_name_record(
        &self,
        node_id: NodeId<CertificateSignaturePubKey<ST>>,
        name_record: MonadNameRecord<ST>,
    ) {
        let mut peer_table = self.peer_table.write().unwrap();
        peer_table.insert(node_id, name_record);
    }

    pub fn insert_addr_v4(
        &self,
        node_id: NodeId<CertificateSignaturePubKey<ST>>,
        addr: SocketAddrV4,
    ) {
        let name_record = MonadNameRecord {
            name_record: NameRecord {
                address: addr,
                seq: 0,
            },
            signature: unsafe { mem::zeroed() }, // Dummy signature for tests
        };
        self.insert_name_record(node_id, name_record);
    }

    pub fn clear_events(&self) {
        self.events.write().unwrap().clear();
    }

    pub fn events(&self) -> Vec<PeerDiscoveryEvent<ST>> {
        self.events.read().unwrap().clone()
    }

    fn push_event(&mut self, event: PeerDiscoveryEvent<ST>) {
        self.events.get_mut().unwrap().push(event);
    }
}
