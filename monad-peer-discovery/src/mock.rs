use std::{collections::BTreeMap, net::SocketAddrV4, time::Duration};

use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::NodeId;
use tracing::debug;

use crate::{
    PeerDiscMetrics, PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder, PeerDiscoveryCommand,
    PeerDiscoveryEvent, PeerDiscoveryMessage, PeerDiscoveryTimerCommand, PeerLookupRequest,
    PeerLookupResponse, Ping, Pong, TimerKind,
};

struct PeerState {
    last_ping: Option<Ping>,
    alive: bool,
}

// Periodically pings the peer. Peers responds with a pong. It's not an actual
// peer discovery implementation
pub struct PingPongDiscovery<ST: CertificateSignatureRecoverable> {
    self_id: NodeId<CertificateSignaturePubKey<ST>>,
    peer_state: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, PeerState>,

    ping_period: Duration,

    metrics: PeerDiscMetrics,
}

pub struct PingPongDiscoveryBuilder<ST: CertificateSignatureRecoverable> {
    pub self_id: NodeId<CertificateSignaturePubKey<ST>>,
    pub peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    pub ping_period: Duration,
}

impl<ST: CertificateSignatureRecoverable> PeerDiscoveryAlgoBuilder
    for PingPongDiscoveryBuilder<ST>
{
    type PeerDiscoveryAlgoType = PingPongDiscovery<ST>;

    fn build(
        self,
    ) -> (
        Self::PeerDiscoveryAlgoType,
        Vec<
            PeerDiscoveryCommand<<Self::PeerDiscoveryAlgoType as PeerDiscoveryAlgo>::SignatureType>,
        >,
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
            .flat_map(|peer| state.send_ping(peer))
            .collect();

        (state, cmds)
    }
}

impl<ST: CertificateSignatureRecoverable> PingPongDiscovery<ST> {
    fn reset_timer(
        &self,
        peer: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        vec![
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::ScheduleReset {
                node_id: peer,
                timer_kind: TimerKind::SendPing,
            }),
            PeerDiscoveryCommand::TimerCommand(PeerDiscoveryTimerCommand::Schedule {
                node_id: peer,
                timer_kind: TimerKind::SendPing,
                duration: self.ping_period,
                on_timeout: PeerDiscoveryEvent::SendPing { to: peer },
            }),
        ]
    }
}

impl<ST> PeerDiscoveryAlgo for PingPongDiscovery<ST>
where
    ST: CertificateSignatureRecoverable,
{
    type SignatureType = ST;

    fn send_ping(
        &mut self,
        target: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
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

    fn handle_ping(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        ping: Ping,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
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

    fn handle_pong(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        pong: Pong,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
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

    fn handle_ping_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        ping_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, ?ping_id, "handling ping timeout");

        Vec::new()
    }

    fn send_peer_lookup_request(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?to, ?target, "sending peer lookup request");

        Vec::new()
    }

    fn handle_peer_lookup_request(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        request: PeerLookupRequest<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?request, "handling peer lookup request");

        Vec::new()
    }

    fn handle_peer_lookup_response(
        &mut self,
        from: NodeId<CertificateSignaturePubKey<ST>>,
        response: PeerLookupResponse<ST>,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?from, ?response, "handling peer lookup response");

        Vec::new()
    }

    fn handle_peer_lookup_timeout(
        &mut self,
        to: NodeId<CertificateSignaturePubKey<ST>>,
        target: NodeId<CertificateSignaturePubKey<ST>>,
        lookup_id: u32,
    ) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!(?lookup_id, "peer lookup request timeout");

        Vec::new()
    }

    fn prune(&mut self) -> Vec<PeerDiscoveryCommand<ST>> {
        debug!("pruning unresponsive peer nodes");

        Vec::new()
    }

    fn metrics(&self) -> &PeerDiscMetrics {
        &self.metrics
    }

    fn get_sock_addr_by_id(
        &self,
        id: NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<SocketAddrV4> {
        None
    }
}
