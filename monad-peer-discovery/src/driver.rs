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
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{Stream, StreamExt};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::ExecutorMetrics;
use monad_types::NodeId;
use tokio_util::time::{DelayQueue, delay_queue::Key};

use crate::{
    MonadNameRecord, PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder, PeerDiscoveryCommand,
    PeerDiscoveryEvent, PeerDiscoveryMessage, PeerDiscoveryTimerCommand, TimerKind,
};

pub enum PeerDiscoveryEmit<ST: CertificateSignatureRecoverable> {
    RouterCommand {
        target: NodeId<CertificateSignaturePubKey<ST>>,
        message: PeerDiscoveryMessage<ST>,
    },
    PingPongCommand {
        target: NodeId<CertificateSignaturePubKey<ST>>,
        name_record: crate::NameRecord,
        message: PeerDiscoveryMessage<ST>,
    },
    MetricsCommand(ExecutorMetrics),
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
    waker: Option<Waker>,
}

impl<PD: PeerDiscoveryAlgo> std::fmt::Debug for PeerDiscoveryDriver<PD> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerDiscoveryDriver").finish()
    }
}

impl<PD: PeerDiscoveryAlgo> PeerDiscoveryDriver<PD> {
    pub fn new<B: PeerDiscoveryAlgoBuilder<PeerDiscoveryAlgoType = PD>>(builder: B) -> Self {
        let (peer_discovery, init_cmds) = builder.build();

        let mut this = Self {
            pd: peer_discovery,
            timer: Default::default(),

            pending_emits: Default::default(),
            waker: None,
        };

        this.exec(init_cmds);

        this
    }

    pub fn update(&mut self, event: PeerDiscoveryEvent<PD::SignatureType>) {
        let cmds = match event {
            PeerDiscoveryEvent::SendPing {
                to,
                name_record,
                ping,
            } => self.pd.send_ping(to, name_record, ping),
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
            PeerDiscoveryEvent::SendFullNodeRaptorcastRequest { to } => {
                self.pd.send_full_node_raptorcast_request(to)
            }
            PeerDiscoveryEvent::FullNodeRaptorcastRequest { from } => {
                self.pd.handle_full_node_raptorcast_request(from.id)
            }
            PeerDiscoveryEvent::FullNodeRaptorcastResponse { from } => {
                self.pd.handle_full_node_raptorcast_response(from.id)
            }
            PeerDiscoveryEvent::UpdateCurrentRound { round, epoch } => {
                self.pd.update_current_round(round, epoch)
            }
            PeerDiscoveryEvent::UpdateValidatorSet { epoch, validators } => {
                self.pd.update_validator_set(epoch, validators)
            }
            PeerDiscoveryEvent::UpdatePeers { peers } => self.pd.update_peers(peers),
            PeerDiscoveryEvent::UpdatePinnedNodes {
                dedicated_full_nodes,
                prioritized_full_nodes,
            } => self
                .pd
                .update_pinned_nodes(dedicated_full_nodes, prioritized_full_nodes),
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
                    self.pending_emits
                        .push_back(PeerDiscoveryEmit::RouterCommand { target, message });

                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
                PeerDiscoveryCommand::PingPongCommand {
                    target,
                    name_record,
                    message,
                } => {
                    self.pending_emits
                        .push_back(PeerDiscoveryEmit::PingPongCommand {
                            target,
                            name_record,
                            message,
                        });

                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
                PeerDiscoveryCommand::TimerCommand(timer_cmd) => {
                    timer_cmds.push(timer_cmd);
                }
                PeerDiscoveryCommand::MetricsCommand(peer_discovery_metrics_command) => {
                    self.pending_emits
                        .push_back(PeerDiscoveryEmit::MetricsCommand(
                            peer_discovery_metrics_command.0,
                        ));

                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
            }
        }

        self.timer.exec(timer_cmds);
    }

    pub fn get_addr(
        &self,
        node_id: &NodeId<CertificateSignaturePubKey<PD::SignatureType>>,
    ) -> Option<SocketAddr> {
        self.pd.get_addr_by_id(node_id).map(SocketAddr::V4)
    }

    pub fn get_lean_udp_p2p_addr(
        &self,
        node_id: &NodeId<CertificateSignaturePubKey<PD::SignatureType>>,
    ) -> Option<SocketAddr> {
        self.pd
            .get_name_record(node_id)
            .and_then(|record| record.lean_udp_p2p_address())
            .map(SocketAddr::V4)
    }

    pub fn get_known_addresses(
        &self,
    ) -> HashMap<NodeId<CertificateSignaturePubKey<PD::SignatureType>>, SocketAddr> {
        self.pd
            .get_known_addrs()
            .into_iter()
            .map(|(k, v)| (k, SocketAddr::V4(v)))
            .collect()
    }

    pub fn get_secondary_fullnodes(
        &self,
    ) -> Vec<NodeId<CertificateSignaturePubKey<PD::SignatureType>>> {
        self.pd.get_secondary_fullnodes()
    }

    pub fn get_name_records(
        &self,
    ) -> HashMap<
        NodeId<CertificateSignaturePubKey<PD::SignatureType>>,
        MonadNameRecord<PD::SignatureType>,
    > {
        self.pd.get_name_records()
    }

    #[doc(hidden)]
    pub fn inner(&self) -> &PD {
        &self.pd
    }

    pub fn get_name_record(
        &self,
        id: &NodeId<CertificateSignaturePubKey<PD::SignatureType>>,
    ) -> Option<&MonadNameRecord<PD::SignatureType>> {
        self.pd.get_name_record(id)
    }

    pub fn metrics(&self) -> &ExecutorMetrics {
        self.pd.metrics()
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
        loop {
            if let Some(emit) = self.pending_emits.pop_front() {
                return Poll::Ready(Some(emit));
            }

            let Poll::Ready(event) = self.timer.poll_next_unpin(cx) else {
                if self
                    .waker
                    .as_ref()
                    .is_none_or(|waker| !waker.will_wake(cx.waker()))
                {
                    self.waker = Some(cx.waker().clone());
                }

                return Poll::Pending;
            };

            let Some(event) = event else {
                panic!("Timer stream is never exhausted")
            };

            self.update(event);
        }
    }
}
