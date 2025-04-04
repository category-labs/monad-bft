use std::{collections::HashMap, time::Duration};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::Message;
use monad_types::NodeId;

pub type PeerDiscMetrics = HashMap<&'static str, u64>;

pub enum PeerDiscoveryEvent<ST: CertificateSignatureRecoverable> {
    SendPing {
        target: NodeId<CertificateSignaturePubKey<ST>>,
    },
    PingRequest {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        ping: Ping,
    },
    PongResponse {
        from: NodeId<CertificateSignaturePubKey<ST>>,
        pong: Pong,
    },
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

#[derive(Debug, Clone, Copy)]
pub struct Ping {
    pub id: u32,
    pub local_record_seq: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct Pong {
    pub ping_id: u32,
    pub local_record_seq: u64,
}

#[derive(Debug, Clone)]
pub enum PeerDiscoveryMessage<ST: CertificateSignatureRecoverable> {
    Ping(Ping),
    Pong(Pong),
    Unused(ST),
}

impl<ST: CertificateSignatureRecoverable> Message for PeerDiscoveryMessage<ST> {
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;
    type Event = PeerDiscoveryEvent<ST>;

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        match self {
            PeerDiscoveryMessage::Ping(ping) => PeerDiscoveryEvent::PingRequest { from, ping },
            PeerDiscoveryMessage::Pong(pong) => PeerDiscoveryEvent::PongResponse { from, pong },
            PeerDiscoveryMessage::Unused(_) => {
                unreachable!()
            }
        }
    }
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

pub trait PeerDiscoveryAlgo {
    type SignatureType: CertificateSignatureRecoverable;

    fn handle_send_ping(
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

    fn metrics(&self) -> &PeerDiscMetrics;
}
