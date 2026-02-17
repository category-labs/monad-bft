use std::{
    collections::HashMap,
    marker::PhantomData,
    net::{Ipv4Addr, SocketAddrV4},
};

use clap::ValueEnum;
use monad_peer_discovery::{mock::NopDiscoveryBuilder, MonadNameRecord, NameRecord};
use monad_types::NodeId;

use crate::message::SignatureType;

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum Transport {
    /// Send via RaptorCast UDP chunking (unauthenticated).
    Raptorcast,
    /// Send via RaptorCast, upgrading to WireAuth-authenticated UDP when possible.
    RaptorcastAuth,
    /// Send via LeanUDP point-to-point forwarding.
    Leanudp,
}

#[derive(Copy, Clone, Debug)]
pub struct NodeEndpointsV4 {
    pub rc_tcp_addr: SocketAddrV4,
    pub rc_udp_addr: SocketAddrV4,
    pub rc_auth_udp_addr: SocketAddrV4,
    pub leanudp_addr: SocketAddrV4,
}

impl NodeEndpointsV4 {
    pub fn ip(&self) -> Ipv4Addr {
        *self.rc_udp_addr.ip()
    }

    pub fn nop_discovery_builder_for_node(
        &self,
        node_id: NodeId<monad_secp::PubKey>,
        signer: &monad_secp::KeyPair,
    ) -> NopDiscoveryBuilder<SignatureType> {
        let mut known_addresses: HashMap<NodeId<monad_secp::PubKey>, SocketAddrV4> = HashMap::new();
        known_addresses.insert(node_id, self.rc_udp_addr);

        // Use a V2 name record so we can advertise both the authenticated UDP socket
        // (RaptorCast auth upgrade) and the LeanUDP tx ingestion port (LeanPointToPoint).
        //
        // In production this record is signed by the node; here the signature isn't used
        // by the NOP peer discovery implementation, but we still fill it with a valid
        // signature for debugging/observability.
        let name_record = NameRecord::new_with_lean_udp_p2p(
            self.ip(),
            self.rc_tcp_addr.port(),
            self.rc_udp_addr.port(),
            self.rc_auth_udp_addr.port(),
            self.leanudp_addr.port(),
            0,
        );
        let monad_name_record = MonadNameRecord::<SignatureType>::new(name_record, signer);

        let mut name_records: HashMap<NodeId<monad_secp::PubKey>, MonadNameRecord<SignatureType>> =
            HashMap::new();
        name_records.insert(node_id, monad_name_record);

        NopDiscoveryBuilder {
            known_addresses,
            name_records,
            pd: PhantomData,
        }
    }
}
