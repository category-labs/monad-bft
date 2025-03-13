use std::{collections::HashMap, time::Duration};

use monad_crypto::certificate_signature::PubKey;
use monad_executor_glue::Message;
use monad_types::NodeId;

pub type PeerDiscMetrics = HashMap<&'static str, u64>;

pub enum PeerDiscoveryEvent<P: PubKey> {
    SendPing { target: NodeId<P> },
    PingRequest { from: NodeId<P>, ping: Ping },
    PongResponse { from: NodeId<P>, pong: Pong },
}

pub enum PeerDiscoveryTimerCommand<E, P: PubKey> {
    Schedule {
        node_id: NodeId<P>, // unique identify request to node
        duration: Duration,
        on_timeout: E,
    },
    ScheduleReset {
        node_id: NodeId<P>,
    },
}

pub enum PeerDiscoveryCommand<P: PubKey> {
    RouterCommand {
        target: NodeId<P>,
        message: PeerDiscoveryMessage<P>,
    },
    TimerCommand(PeerDiscoveryTimerCommand<PeerDiscoveryEvent<P>, P>),
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
pub enum PeerDiscoveryMessage<P: PubKey> {
    Ping(Ping),
    Pong(Pong),
    Unused(P),
}

impl<P: PubKey> Message for PeerDiscoveryMessage<P> {
    type NodeIdPubKey = P;
    type Event = PeerDiscoveryEvent<P>;

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
        Vec<PeerDiscoveryCommand<<Self::PeerDiscoveryAlgoType as PeerDiscoveryAlgo>::PubKeyType>>,
    );
}

pub trait PeerDiscoveryAlgo {
    type PubKeyType: PubKey;

    fn handle_send_ping(
        &mut self,
        target: NodeId<Self::PubKeyType>,
    ) -> Vec<PeerDiscoveryCommand<Self::PubKeyType>>;

    fn handle_ping(
        &mut self,
        from: NodeId<Self::PubKeyType>,
        ping: Ping,
    ) -> Vec<PeerDiscoveryCommand<Self::PubKeyType>>;

    fn handle_pong(
        &mut self,
        from: NodeId<Self::PubKeyType>,
        pong: Pong,
    ) -> Vec<PeerDiscoveryCommand<Self::PubKeyType>>;

    fn metrics(&self) -> &PeerDiscMetrics;
}
