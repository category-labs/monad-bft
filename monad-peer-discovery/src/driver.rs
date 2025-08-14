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
    collections::HashMap,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use arc_swap::ArcSwap;
use futures::{Stream, StreamExt};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::ExecutorMetrics;
use monad_types::NodeId;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_util::time::{DelayQueue, delay_queue::Key};
use tracing::error;

use crate::{
    MonadNameRecord, NodeAddrLookup, PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder,
    PeerDiscoveryCommand, PeerDiscoveryEvent, PeerDiscoveryMessage, PeerDiscoveryTimerCommand,
    PeerTable, Role, TimerKind,
};

pub enum PeerDiscoveryEmit<ST: CertificateSignatureRecoverable> {
    // TODO: other output events
    RouterCommand {
        target: NodeId<CertificateSignaturePubKey<ST>>,
        message: PeerDiscoveryMessage<ST>,
    },
    MetricsCommand(ExecutorMetrics),
}

#[expect(clippy::type_complexity)]
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
    handle: PeerDiscoveryHandle<PD::SignatureType>,
    peer_table: Arc<ArcSwap<PeerTable<PD::SignatureType>>>,

    event_rx: UnboundedReceiver<PeerDiscoveryEvent<PD::SignatureType>>,
    emit_tx: UnboundedSender<PeerDiscoveryEmit<PD::SignatureType>>,
    emit_rx: Option<UnboundedReceiver<PeerDiscoveryEmit<PD::SignatureType>>>,

    timer: PeerDiscTimers<PD::SignatureType>,
}

impl<PD: PeerDiscoveryAlgo> std::fmt::Debug for PeerDiscoveryDriver<PD> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerDiscoveryDriver").finish()
    }
}

impl<PD: PeerDiscoveryAlgo> PeerDiscoveryDriver<PD> {
    pub fn new<B: PeerDiscoveryAlgoBuilder<PeerDiscoveryAlgoType = PD>>(builder: B) -> Self {
        let (peer_discovery, init_cmds) = builder.build();
        let (event_tx, event_rx) = unbounded_channel();
        let peer_table = Arc::new(ArcSwap::from_pointee(peer_discovery.peer_table()));
        let handle = PeerDiscoveryHandle {
            peer_table: peer_table.clone(),
            event_tx,
        };

        let (emit_tx, emit_rx) = unbounded_channel();

        let mut this = Self {
            pd: peer_discovery,
            timer: Default::default(),
            handle,
            peer_table,
            event_rx,
            emit_tx,
            emit_rx: Some(emit_rx),
        };

        this.exec(init_cmds);
        this.update_routing_info();

        this
    }

    pub fn handle(&self) -> PeerDiscoveryHandle<PD::SignatureType> {
        self.handle.clone()
    }

    pub fn take_emit_rx(&mut self) -> UnboundedReceiver<PeerDiscoveryEmit<PD::SignatureType>> {
        let Some(rx) = self.emit_rx.take() else {
            panic!("peer-disc emit receiver already taken");
        };
        rx
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
            PeerDiscoveryEvent::UpdateCurrentRound { round, epoch } => {
                self.pd.update_current_round(round, epoch)
            }
            PeerDiscoveryEvent::UpdateValidatorSet { epoch, validators } => {
                self.pd.update_validator_set(epoch, validators)
            }
            PeerDiscoveryEvent::UpdatePeers { peers } => self.pd.update_peers(peers),
            PeerDiscoveryEvent::UpdateConfirmGroup { end_round, peers } => {
                self.pd.update_peer_participation(end_round, peers)
            }
            PeerDiscoveryEvent::Refresh => self.pd.refresh(),
        };

        self.exec(cmds);
    }

    fn exec(&mut self, cmds: Vec<PeerDiscoveryCommand<PD::SignatureType>>) {
        let mut timer_cmds = Vec::new();

        for cmd in cmds {
            match cmd {
                PeerDiscoveryCommand::RouterCommand { target, message } => {
                    self.emit_tx
                        .send(PeerDiscoveryEmit::RouterCommand {
                            target,
                            message: message.clone(),
                        })
                        .expect("peer discovery emit channel closed");
                }
                PeerDiscoveryCommand::TimerCommand(timer_cmd) => {
                    timer_cmds.push(timer_cmd);
                }
                PeerDiscoveryCommand::MetricsCommand(peer_discovery_metrics_command) => {
                    self.emit_tx
                        .send(PeerDiscoveryEmit::MetricsCommand(
                            peer_discovery_metrics_command.0,
                        ))
                        .expect("peer discovery emit channel closed");
                }
            }
        }

        self.timer.exec(timer_cmds);
    }

    pub fn metrics(&self) -> &ExecutorMetrics {
        self.pd.metrics()
    }

    pub fn poll(&mut self, cx: &mut Context<'_>) {
        // FIXME: use tokio::select! macro
        loop {
            if let Poll::Ready(Some(event)) = self.timer.poll_next_unpin(cx) {
                self.update(event);
                continue;
            };

            if let Poll::Ready(Some(event)) = self.event_rx.poll_recv(cx) {
                self.update(event);
                continue;
            }

            break;
        }

        self.update_routing_info();
    }

    pub fn update_routing_info(&mut self) {
        let routing_info = self.pd.peer_table();
        self.peer_table.store(Arc::new(routing_info));
    }

    #[cfg(test)]
    pub fn algo(&self) -> &PD {
        &self.pd
    }
}

#[derive(Clone)]
pub struct PeerDiscoveryHandle<ST: CertificateSignatureRecoverable> {
    peer_table: Arc<ArcSwap<PeerTable<ST>>>,
    event_tx: UnboundedSender<PeerDiscoveryEvent<ST>>,
}

impl<ST: CertificateSignatureRecoverable> PeerDiscoveryHandle<ST> {
    pub fn send_event(&self, event: PeerDiscoveryEvent<ST>) {
        self.event_tx
            .send(event)
            .expect("peer discovery driver closed");
    }
}

impl<ST: CertificateSignatureRecoverable> NodeAddrLookup for PeerDiscoveryHandle<ST> {
    type SignatureType = ST;

    fn lookup_name_record(
        &self,
        id: &NodeId<CertificateSignaturePubKey<ST>>,
    ) -> Option<MonadNameRecord<ST>> {
        self.peer_table.load().lookup_name_record(id)
    }

    fn name_records(&self) -> HashMap<NodeId<CertificateSignaturePubKey<ST>>, MonadNameRecord<ST>> {
        self.peer_table.load().name_records()
    }

    fn role(
        &self,
        node_id: &NodeId<CertificateSignaturePubKey<Self::SignatureType>>,
    ) -> Option<Role> {
        self.peer_table.load().role(node_id)
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::{NopPubKey, NopSignature, certificate_signature::PubKey as _};

    use super::*;
    use crate::mock::NopDiscoveryBuilder;

    type ST = NopSignature;
    type PT = NopPubKey;
    type NodeId<PT = NopPubKey> = monad_types::NodeId<PT>;

    fn node_id(seed: u64) -> NodeId<PT> {
        NodeId::new(PT::from_bytes(&[seed as u8; 32]).unwrap())
    }

    #[test]
    fn test_routing_info_update() {
        let nop_pd = NopDiscoveryBuilder::<ST>::default();
        let mut pdd = PeerDiscoveryDriver::new(nop_pd);
        let handle = pdd.handle();

        // initially, the name records are empty
        assert!(handle.name_records().is_empty());

        // insert some name record
        pdd.algo()
            .insert_addr_v4(node_id(0), "127.0.0.1:0".parse().unwrap());
        assert!(!pdd.algo().name_records().is_empty());

        // the name record is not present on the handle yet
        assert!(handle.name_records().is_empty());

        // update the routing info to reflect the changes
        pdd.update_routing_info();
        assert!(!handle.name_records().is_empty());
    }

    #[test]
    fn test_polling_flushes_events() {
        let nop_pd = NopDiscoveryBuilder::<ST>::default();
        let mut pdd = PeerDiscoveryDriver::new(nop_pd);
        let handle = pdd.handle();

        pdd.algo().clear_events();
        handle.event_tx.send(PeerDiscoveryEvent::Refresh).unwrap();

        // events are not flushed until poll is called
        assert!(pdd.algo().events().is_empty());

        let mut ctx = Context::from_waker(futures::task::noop_waker_ref());
        pdd.poll(&mut ctx);

        // after polling, the events are flushed
        assert!(!pdd.algo().events().is_empty());
        assert!(
            pdd.algo()
                .events()
                .iter()
                .any(|e| matches!(e, PeerDiscoveryEvent::Refresh))
        );
    }
}
