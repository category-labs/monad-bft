use std::{
    collections::{HashMap, VecDeque},
    ops::DerefMut,
    task::{Context, Poll},
    time::Duration,
};

use futures::{Stream, StreamExt};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::NodeId;
use tokio_util::time::{DelayQueue, delay_queue::Key};
use tracing::error;

use crate::{
    PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder, PeerDiscoveryCommand, PeerDiscoveryEvent,
    PeerDiscoveryMessage, PeerDiscoveryTimerCommand, TimerKind,
};

pub enum PeerDiscoveryEmit<ST: CertificateSignatureRecoverable> {
    // TODO: other output events
    RouterCommand {
        target: NodeId<CertificateSignaturePubKey<ST>>,
        message: PeerDiscoveryMessage<ST>,
    },
}

struct PeerDiscTimers<ST: CertificateSignatureRecoverable> {
    timers: DelayQueue<(NodeId<CertificateSignaturePubKey<ST>>, TimerKind)>,
    events:
        HashMap<(NodeId<CertificateSignaturePubKey<ST>>, TimerKind), (Key, PeerDiscoveryEvent<ST>)>,
}

impl<ST: CertificateSignatureRecoverable> Default for PeerDiscTimers<ST> {
    fn default() -> Self {
        Self {
            timers: Default::default(),
            events: Default::default(),
        }
    }
}

impl<ST: CertificateSignatureRecoverable> PeerDiscTimers<ST> {
    fn schedule(
        &mut self,
        duration: Duration,
        node_id: NodeId<CertificateSignaturePubKey<ST>>,
        timer_kind: TimerKind,
        on_timeout: PeerDiscoveryEvent<ST>,
    ) {
        // only one timer can be scheduled per (node id, timer kind) tuple
        // scheduling another timer automatically resets the previous one
        self.schedule_reset(node_id, timer_kind);
        let key = self.timers.insert((node_id, timer_kind), duration);
        self.events.insert((node_id, timer_kind), (key, on_timeout));
    }

    fn schedule_reset(
        &mut self,
        node_id: NodeId<CertificateSignaturePubKey<ST>>,
        timer_kind: TimerKind,
    ) {
        if let Some((key, _event)) = self.events.remove(&(node_id, timer_kind)) {
            // DelayQueue timer panics if key is not found, which indicates a
            // logic error - inconsistency between timers and events
            error!(
                ?key,
                "key is not present in peer discovery timer delay queue"
            );
            self.timers.remove(&key);
        }
    }

    fn exec(&mut self, commands: Vec<PeerDiscoveryTimerCommand<PeerDiscoveryEvent<ST>, ST>>) {
        for cmd in commands {
            match cmd {
                PeerDiscoveryTimerCommand::Schedule {
                    node_id,
                    timer_kind,
                    duration,
                    on_timeout,
                } => {
                    self.schedule(duration, node_id, timer_kind, on_timeout);
                }
                PeerDiscoveryTimerCommand::ScheduleReset {
                    node_id,
                    timer_kind,
                } => {
                    self.schedule_reset(node_id, timer_kind);
                }
            }
        }
    }
}

impl<ST: CertificateSignatureRecoverable> Stream for PeerDiscTimers<ST> {
    type Item = PeerDiscoveryEvent<ST>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let poll_expired = self.timers.poll_next_unpin(cx);
        match poll_expired {
            Poll::Ready(Some(expired)) => {
                let event_key = expired.into_inner();
                let (_, event) = self
                    .events
                    .remove(&event_key)
                    .expect("timers and events entry mapped one to one");
                Poll::Ready(Some(event))
            }
            // DelayQueue::poll_next returns Poll::Ready(None) if there's no
            // active timers. Peer discovery is not terminated and can schedule
            // more timers
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        }
    }
}

pub struct PeerDiscoveryDriver<PD>
where
    PD: PeerDiscoveryAlgo,
{
    pd: PD,
    timer: PeerDiscTimers<PD::SignatureType>,
    pending_emits: VecDeque<PeerDiscoveryEmit<PD::SignatureType>>,
}

impl<PD: PeerDiscoveryAlgo> PeerDiscoveryDriver<PD> {
    pub fn new<B: PeerDiscoveryAlgoBuilder<PeerDiscoveryAlgoType = PD>>(builder: B) -> Self {
        let (peer_discovery, init_cmds) = builder.build();
        let mut driver = Self {
            pd: peer_discovery,
            timer: Default::default(),
            pending_emits: Default::default(),
        };
        let emits = driver.exec_and_emit(init_cmds);
        driver.pending_emits.extend(emits);
        driver
    }

    pub fn update(&mut self, event: PeerDiscoveryEvent<PD::SignatureType>) {
        let cmds = match event {
            PeerDiscoveryEvent::SendPing { to } => self.pd.send_ping(to),
            PeerDiscoveryEvent::PingRequest { from, ping } => self.pd.handle_ping(from, ping),
            PeerDiscoveryEvent::PongResponse { from, pong } => self.pd.handle_pong(from, pong),
            PeerDiscoveryEvent::PingTimeout { to, ping_id } => {
                self.pd.handle_ping_timeout(to, ping_id)
            }
            PeerDiscoveryEvent::SendPeerLookup {
                to,
                target,
                open_discovery,
            } => self.pd.send_peer_lookup_request(to, target, open_discovery),
            PeerDiscoveryEvent::PeerLookupRequest { from, request } => {
                self.pd.handle_peer_lookup_request(from, request)
            }
            PeerDiscoveryEvent::PeerLookupResponse { from, response } => {
                self.pd.handle_peer_lookup_response(from, response)
            }
            PeerDiscoveryEvent::PeerLookupTimeout {
                to,
                target,
                lookup_id,
            } => self.pd.handle_peer_lookup_timeout(to, target, lookup_id),
            PeerDiscoveryEvent::Refresh => self.pd.refresh(),
        };

        let emits = self.exec_and_emit(cmds);
        self.pending_emits.extend(emits);
    }

    fn exec_and_emit(
        &mut self,
        cmds: Vec<PeerDiscoveryCommand<PD::SignatureType>>,
    ) -> Vec<PeerDiscoveryEmit<PD::SignatureType>> {
        let mut emits = Vec::new();
        let mut timer_cmds = Vec::new();
        for cmd in cmds {
            match cmd {
                PeerDiscoveryCommand::RouterCommand { target, message } => {
                    emits.push(PeerDiscoveryEmit::RouterCommand { target, message })
                }
                PeerDiscoveryCommand::TimerCommand(timer_cmd) => {
                    timer_cmds.push(timer_cmd);
                }
            }
        }
        self.timer.exec(timer_cmds);

        emits
    }
}

impl<PD> Stream for PeerDiscoveryDriver<PD>
where
    PD: PeerDiscoveryAlgo,
    Self: Unpin,
{
    type Item = PeerDiscoveryEmit<PD::SignatureType>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some(emit) = this.pending_emits.pop_front() {
            return Poll::Ready(Some(emit));
        }

        match this.timer.poll_next_unpin(cx) {
            Poll::Ready(Some(event)) => {
                this.update(event);
                if let Some(emit) = this.pending_emits.pop_front() {
                    return Poll::Ready(Some(emit));
                }
            }
            Poll::Ready(None) => {
                panic!("Timer stream is never exhausted")
            }
            Poll::Pending => {}
        }

        Poll::Pending
    }
}
