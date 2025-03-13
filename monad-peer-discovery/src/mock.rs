use std::{collections::BTreeMap, time::Duration};

use monad_crypto::certificate_signature::PubKey;
use monad_types::NodeId;
use tracing::debug;

use crate::algo::{
    PeerDiscMetrics, PeerDiscoveryAlgo, PeerDiscoveryBuilder, PeerDiscoveryCommand,
    PeerDiscoveryEvent, PeerDiscoveryMessage, PeerDiscoveryTimerCommand, Ping, Pong,
};

struct PeerState {
    last_ping: Option<Ping>,
    alive: bool,
}

// Periodically pings the peer. Peers responds with a pong. It's not an actual
// peer discovery implementation
pub struct PingPongDiscovery<P: PubKey> {
    self_id: NodeId<P>,
    peer_state: BTreeMap<NodeId<P>, PeerState>,

    ping_period: Duration,

    metrics: PeerDiscMetrics,
}

pub struct PingPongDiscoveryBuilder<P: PubKey> {
    pub self_id: NodeId<P>,
    pub peers: Vec<NodeId<P>>,
    pub ping_period: Duration,
}

impl<P: PubKey> PeerDiscoveryBuilder for PingPongDiscoveryBuilder<P> {
    type PeerDiscoveryAlgoType = PingPongDiscovery<P>;

    fn build(
        self,
    ) -> (
        Self::PeerDiscoveryAlgoType,
        Vec<PeerDiscoveryCommand<<Self::PeerDiscoveryAlgoType as PeerDiscoveryAlgo>::PubKeyType>>,
    ) {
        let peers: Vec<_> = self
            .peers
            .into_iter()
            .filter(|p| p != &self.self_id)
            .collect();

        let mut state = PingPongDiscovery {
            self_id: self.self_id,
            peer_state: peers
                .iter()
                .cloned()
                .map(|p| {
                    (p, PeerState {
                        last_ping: None,
                        alive: false,
                    })
                })
                .collect(),
            ping_period: self.ping_period,
            metrics: Default::default(),
        };

        let cmds = peers
            .into_iter()
            .flat_map(|peer| state.handle_send_ping(peer))
            .collect();

        (state, cmds)
    }
}

impl<P: PubKey> PingPongDiscovery<P> {
    fn reset_timer(&self, peer: NodeId<P>) -> Vec<PeerDiscoveryCommand<P>> {
        vec![
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::ScheduleReset {
                node_id: peer,
            }),
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::Schedule {
                node_id: peer,
                duration: self.ping_period,
                on_timeout: PeerDiscoveryEvent::SendPing { target: peer },
            }),
        ]
    }
}

impl<P> PeerDiscoveryAlgo for PingPongDiscovery<P>
where
    P: PubKey,
{
    type PubKeyType = P;

    fn handle_send_ping(&mut self, target: NodeId<P>) -> Vec<PeerDiscoveryCommand<P>> {
        debug!(?target, "handle send ping");
        let mut cmds = Vec::new();

        if !self.peer_state.contains_key(&target) {
            return cmds;
        }
        cmds.extend(self.reset_timer(target));
        let peer_state = self.peer_state.get_mut(&target).expect("contains peer");

        let ping_id = peer_state.last_ping.map_or(0, |ping| ping.id + 1);
        let ping = Ping {
            id: ping_id,
            local_record_seq: 1,
        };
        peer_state.last_ping = Some(ping);

        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target,
            message: PeerDiscoveryMessage::Ping(ping),
        });
        *self.metrics.entry("send_ping").or_default() += 1;
        cmds
    }

    fn handle_ping(&mut self, from: NodeId<P>, ping: Ping) -> Vec<PeerDiscoveryCommand<P>> {
        debug!(?from, ?ping, "handle ping");
        *self.metrics.entry("recv_ping").or_default() += 1;
        let mut cmds = Vec::new();
        cmds.push(PeerDiscoveryCommand::RouterCommand {
            target: from,
            message: PeerDiscoveryMessage::Pong(Pong {
                ping_id: ping.id,
                local_record_seq: 2,
            }),
        });
        *self.metrics.entry("send_pong").or_default() += 1;
        cmds
    }

    fn handle_pong(&mut self, from: NodeId<P>, pong: Pong) -> Vec<PeerDiscoveryCommand<P>> {
        debug!(?from, ?pong, "handle pong");
        *self.metrics.entry("recv_pong").or_default() += 1;
        let cmds = Vec::new();
        let Some(peer_state) = self.peer_state.get_mut(&from) else {
            return cmds;
        };

        if peer_state
            .last_ping
            .is_some_and(|ping| ping.id == pong.ping_id)
        {
            peer_state.alive = true;
        }

        cmds
    }

    fn metrics(&self) -> &PeerDiscMetrics {
        &self.metrics
    }
}
