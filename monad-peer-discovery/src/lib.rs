use std::{collections::HashMap, net::SocketAddr, time::Duration};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::Message;
use monad_types::NodeId;

pub mod discovery;
pub mod mock;

pub type PeerDiscMetrics = HashMap<&'static str, u64>;

#[derive(Debug, Clone, Copy)]
pub struct Ping {
    id: u32,
    local_record_seq: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct Pong {
    ping_id: u32,
    local_record_seq: u64,
}

#[derive(Debug, Clone)]
pub struct PeerLookupRequest<ST: CertificateSignatureRecoverable> {
    lookup_id: u32,
    target: NodeId<CertificateSignaturePubKey<ST>>,
}

#[derive(Debug, Clone)]
pub struct PeerLookupResponse<ST: CertificateSignatureRecoverable> {
    lookup_id: u32,
    target: NodeId<CertificateSignaturePubKey<ST>>,
    name_records: Vec<(NodeId<CertificateSignaturePubKey<ST>>, MonadNameRecord<ST>)>,
}

#[derive(Debug, Clone)]
pub struct MonadNameRecord<ST: CertificateSignatureRecoverable> {
    pub address: SocketAddr,
    pub seq: u64,
    pub signature: ST,
}

pub enum PeerDiscoveryEvent<ST: CertificateSignatureRecoverable> {
    SendPing {
        to: NodeId<CertificateSignaturePubKey<ST>>,
    },
    PingRequest {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        ping: Ping,
    },
    PongResponse {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        pong: Pong,
    },
    SendPeerLookup {
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
    },
    PeerLookupRequest {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        request: PeerLookupRequest<ST>,
    },
    PeerLookupResponse {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        response: PeerLookupResponse<ST>,
    },
}

#[derive(Debug, Clone)]
pub enum PeerDiscoveryMessage<ST: CertificateSignatureRecoverable> {
    Ping(Ping),
    Pong(Pong),
    PeerLookupRequest(PeerLookupRequest<ST>),
    PeerLookupResponse(PeerLookupResponse<ST>),
}

impl<ST: CertificateSignatureRecoverable> Message for PeerDiscoveryMessage<ST> {
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;
    type Event = PeerDiscoveryEvent<ST>;

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        match self {
            PeerDiscoveryMessage::Ping(ping) => PeerDiscoveryEvent::PingRequest { from, ping },
            PeerDiscoveryMessage::Pong(pong) => PeerDiscoveryEvent::PongResponse { from, pong },
            PeerDiscoveryMessage::PeerLookupRequest(request) => {
                PeerDiscoveryEvent::PeerLookupRequest { from, request }
            }
            PeerDiscoveryMessage::PeerLookupResponse(response) => {
                PeerDiscoveryEvent::PeerLookupResponse { from, response }
            }
        }
    }
}

pub enum PeerDiscoveryTimerCommand<E, ST: CertificateSignatureRecoverable> {
    Schedule {
        node_id: NodeId<CertificateSignaturePubKey<ST>>, // unique identify request to node
        duration: Duration,
        on_timeout: E,
    },
    ScheduleReset {
        node_id: NodeId<CertificateSignaturePubKey<ST>>,
    },
}

pub enum PeerDiscoveryCommand<ST: CertificateSignatureRecoverable> {
    RouterCommand {
        target: NodeId<CertificateSignaturePubKey<ST>>,
        message: PeerDiscoveryMessage<ST>,
    },
    TimerCommand(PeerDiscoveryTimerCommand<PeerDiscoveryEvent<ST>, ST>),
}

pub trait PeerDiscoveryAlgo {
    type SignatureType: CertificateSignatureRecoverable;

    fn send_ping(
        &mut self,
        target: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_ping(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        ping: Ping,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_pong(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        pong: Pong,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn send_peer_lookup_request(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        target: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_peer_lookup_request(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        request: PeerLookupRequest<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn handle_peer_lookup_response(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
        response: PeerLookupResponse<Self::SignatureType>,
    ) -> Vec<PeerDiscoveryCommand<Self::SignatureType>>;

    fn metrics(&self) -> &PeerDiscMetrics;
}

pub trait PeerDiscoveryBuilder {
    type PeerDiscoveryAlgoType: PeerDiscoveryAlgo;

    fn build(
        self,
    ) -> (
        Self::PeerDiscoveryAlgoType,
        Vec<
            PeerDiscoveryCommand<<Self::PeerDiscoveryAlgoType as PeerDiscoveryAlgo>::SignatureType>,
        >,
    );
}
