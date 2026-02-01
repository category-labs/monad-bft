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
    collections::{BTreeMap, VecDeque},
    net::{SocketAddr, SocketAddrV4},
    time::Duration,
};

use driver::MockDiscoveryDriver;
use itertools::Itertools;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_peer_discovery::{
    PeerDiscoveryAlgo, PeerDiscoveryAlgoBuilder, PeerDiscoveryEvent, PeerDiscoveryMessage,
};
use monad_router_scheduler::{RouterEvent, RouterScheduler};
use monad_transformer::{ID, LinkMessage, Pipeline};
use monad_types::NodeId;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tracing::debug;

pub mod builder;
pub mod driver;

pub type SwarmPubKeyType<S> =
    CertificateSignaturePubKey<<S as PeerDiscSwarmRelation>::SignatureType>;
pub type SwarmSignatureType<S> = <S as PeerDiscSwarmRelation>::SignatureType;

pub struct Swarm<S>
where
    S: PeerDiscSwarmRelation,
{
    nodes: BTreeMap<NodeId<SwarmPubKeyType<S>>, Node<S>>,
}

pub trait PeerDiscSwarmRelation {
    type SignatureType: CertificateSignatureRecoverable;
    type PeerDiscoveryAlgoType: PeerDiscoveryAlgo<SignatureType = SwarmSignatureType<Self>>;

    type TransportMessage;
    type RouterSchedulerType: RouterScheduler<
            NodeIdPublicKey = SwarmPubKeyType<Self>,
            OutboundMessage = PeerDiscoveryMessage<SwarmSignatureType<Self>>,
            InboundMessage = PeerDiscoveryMessage<SwarmSignatureType<Self>>,
            TransportMessage = Self::TransportMessage,
        >;

    type Pipeline: Pipeline<
            Self::TransportMessage,
            NodeIdPubKey = CertificateSignaturePubKey<Self::SignatureType>,
        >;
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy)]
pub enum SwarmEvent {
    DriverEvent,
    ExecutorEvent,
    InboundMessage,
    TestEvent,
}

pub struct NodeBuilder<S, B>
where
    S: PeerDiscSwarmRelation,
{
    pub id: NodeId<SwarmPubKeyType<S>>,
    pub addr: SocketAddrV4,
    pub algo_builder: B,
    pub router_scheduler: S::RouterSchedulerType,
    pub seed: u64,
    pub outbound_pipeline: S::Pipeline,
}

impl<S, B> NodeBuilder<S, B>
where
    S: PeerDiscSwarmRelation,
    B: PeerDiscoveryAlgoBuilder<PeerDiscoveryAlgoType = S::PeerDiscoveryAlgoType>,
{
    pub fn build(self) -> Node<S> {
        Node::new(
            self.id,
            self.algo_builder,
            self.router_scheduler,
            self.seed,
            self.outbound_pipeline,
        )
    }
}

pub struct Node<S>
where
    S: PeerDiscSwarmRelation,
{
    pub id: NodeId<SwarmPubKeyType<S>>,
    pub peer_disc_driver: MockDiscoveryDriver<
        S::PeerDiscoveryAlgoType,
        PeerDiscoveryEvent<SwarmSignatureType<S>>,
        SwarmSignatureType<S>,
    >,
    pub executor: MockPeerDiscExecutor<S>,
    pub pending_inbound_messages:
        BTreeMap<Duration, VecDeque<LinkMessage<SwarmPubKeyType<S>, S::TransportMessage>>>,
    // used for manually invoking event in testings
    pub pending_test_event: BTreeMap<Duration, PeerDiscoveryEvent<SwarmSignatureType<S>>>,

    rng: ChaCha8Rng,
    current_seed: usize,
    outbound_pipeline: S::Pipeline,
    message_nonce: usize,
}

pub struct MockPeerDiscExecutor<S: PeerDiscSwarmRelation> {
    router: S::RouterSchedulerType,
    tick: Duration,
    id_to_addr: BTreeMap<NodeId<SwarmPubKeyType<S>>, SocketAddrV4>,
}

enum MockPeerDiscExecutorEvent<E, PT: PubKey, TransportMessage> {
    Event(E),
    Send(NodeId<PT>, TransportMessage),
}

impl<S: PeerDiscSwarmRelation> Executor for MockPeerDiscExecutor<S> {
    type Command = RouterCommand<
        SwarmSignatureType<S>,
        <S::RouterSchedulerType as RouterScheduler>::OutboundMessage,
    >;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for cmd in commands {
            match cmd {
                RouterCommand::Publish { target, message } => {
                    self.router.send_outbound(self.tick, target, message)
                }
                RouterCommand::PublishWithPriority {
                    target,
                    message,
                    priority: _,
                } => self.router.send_outbound(self.tick, target, message),
                RouterCommand::AddEpochValidatorSet { .. } => {}
                RouterCommand::UpdateCurrentRound(..) => {}
                RouterCommand::GetPeers => {}
                RouterCommand::UpdatePeers { .. } => {}
                RouterCommand::GetFullNodes => {}
                RouterCommand::UpdateFullNodes { .. } => {}
                RouterCommand::PublishToFullNodes { .. } => {}
            }
        }
    }

    fn metrics(&self) -> monad_executor::ExecutorMetricsChain<'_> {
        Default::default()
    }
}

impl<S: PeerDiscSwarmRelation> MockPeerDiscExecutor<S> {
    fn peek_tick(&self) -> Option<Duration> {
        self.router.peek_tick()
    }

    fn step_until(
        &mut self,
        until: Duration,
    ) -> Option<
        MockPeerDiscExecutorEvent<
            PeerDiscoveryEvent<SwarmSignatureType<S>>,
            SwarmPubKeyType<S>,
            S::TransportMessage,
        >,
    > {
        while let Some(tick) = self.router.peek_tick() {
            if tick > until {
                break;
            }
            self.tick = tick;
            let maybe_router_event = self.router.step_until(tick);
            let event = match maybe_router_event {
                Some(RouterEvent::Rx(from, message)) => {
                    let src_addr = self
                        .id_to_addr
                        .get(&from)
                        .map(|a| SocketAddr::V4(*a))
                        .unwrap_or(SocketAddr::V4(SocketAddrV4::new(
                            std::net::Ipv4Addr::UNSPECIFIED,
                            0,
                        )));
                    MockPeerDiscExecutorEvent::Event(message.event_with_source(from, src_addr))
                }
                Some(RouterEvent::Tx(to, ser)) => MockPeerDiscExecutorEvent::Send(to, ser),
                None => continue,
            };
            return Some(event);
        }
        None
    }

    pub fn send_message(
        &mut self,
        tick: Duration,
        from: NodeId<SwarmPubKeyType<S>>,
        message: S::TransportMessage,
    ) {
        assert!(tick >= self.tick);

        self.router.process_inbound(tick, from, message);
    }

    fn update_tick(&mut self, tick: Duration) {
        self.tick = tick;
    }
}

impl<S> Node<S>
where
    S: PeerDiscSwarmRelation,
{
    pub fn new<B>(
        id: NodeId<SwarmPubKeyType<S>>,
        algo_builder: B,
        router_scheduler: S::RouterSchedulerType,
        seed: u64,
        outbound_pipeline: S::Pipeline,
    ) -> Self
    where
        B: PeerDiscoveryAlgoBuilder<PeerDiscoveryAlgoType = S::PeerDiscoveryAlgoType>,
    {
        let (peer_disc_driver, init_cmds) = MockDiscoveryDriver::new(algo_builder);

        let mut executor = MockPeerDiscExecutor::<S> {
            router: router_scheduler,
            tick: Default::default(),
            id_to_addr: Default::default(),
        };
        executor.exec(init_cmds);

        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let current_seed = rng.r#gen();

        Node {
            id,
            peer_disc_driver,
            executor,
            pending_inbound_messages: Default::default(),
            pending_test_event: Default::default(),
            rng,
            current_seed,
            outbound_pipeline,
            message_nonce: 0,
        }
    }

    fn update_rng(&mut self) {
        self.current_seed = self.rng.r#gen();
    }

    pub fn peek_event(&self) -> Option<(Duration, SwarmEvent)> {
        let events =
            std::iter::empty()
                .chain(
                    self.peer_disc_driver
                        .peek_tick()
                        .map(|tick| (tick, SwarmEvent::DriverEvent)),
                )
                .chain(
                    self.executor
                        .peek_tick()
                        .map(|tick| (tick, SwarmEvent::ExecutorEvent)),
                )
                .chain(self.pending_inbound_messages.first_key_value().map(
                    |(min_scheduled_tick, _)| (*min_scheduled_tick, SwarmEvent::InboundMessage),
                ))
                .chain(
                    self.pending_test_event
                        .first_key_value()
                        .map(|(min_scheduled_tick, _)| {
                            (*min_scheduled_tick, SwarmEvent::TestEvent)
                        }),
                )
                .min_set();

        if !events.is_empty() {
            Some(events[self.current_seed % events.len()])
        } else {
            None
        }
    }

    pub fn step_until(
        &mut self,
        until: Duration,
        emitted_messages: &mut Vec<(
            Duration,
            LinkMessage<SwarmPubKeyType<S>, S::TransportMessage>,
        )>,
    ) -> bool {
        while let Some((tick, event_type)) = self.peek_event() {
            if tick > until {
                break;
            }
            self.update_rng();
            match event_type {
                SwarmEvent::DriverEvent => {
                    if let Some(event) = self.peer_disc_driver.step_until(tick) {
                        let _node_span_entered =
                            tracing::trace_span!("node", id = format!("{}", self.id)).entered();
                        let router_cmds = self.peer_disc_driver.update(event);
                        self.executor.update_tick(tick);
                        self.executor.exec(router_cmds);
                        return true;
                    }
                }
                SwarmEvent::ExecutorEvent => {
                    if let Some(event) = self.executor.step_until(tick) {
                        match event {
                            MockPeerDiscExecutorEvent::Event(event) => {
                                let _node_span_entered =
                                    tracing::trace_span!("node", id = format!("{}", self.id))
                                        .entered();
                                let cmds = self.peer_disc_driver.update(event);
                                self.executor.update_tick(tick);
                                self.executor.exec(cmds);
                                return true;
                            }
                            MockPeerDiscExecutorEvent::Send(to, ser) => {
                                if to == self.id {
                                    self.executor.router.process_inbound(tick, self.id, ser);
                                } else {
                                    let lm = LinkMessage {
                                        from: ID::new(self.id),
                                        to: ID::new(to),
                                        message: ser,

                                        from_tick: tick,
                                        nonce: self.message_nonce,
                                    };
                                    self.message_nonce += 1;
                                    let outbound_transformed = self.outbound_pipeline.process(lm);
                                    for (delay, msg) in outbound_transformed {
                                        let sched_tick = tick + delay;
                                        emitted_messages.push((sched_tick, msg));
                                    }
                                }
                                continue;
                            }
                        }
                    }
                }
                SwarmEvent::InboundMessage => {
                    let mut entry = self
                        .pending_inbound_messages
                        .first_entry()
                        .expect("logic error, should be nonempty");

                    let scheduled_tick = *entry.key();
                    let msgs = entry.get_mut();

                    assert_eq!(tick, scheduled_tick);

                    let message = msgs.pop_front().expect("logic error, should be nonempty");

                    if msgs.is_empty() {
                        entry.remove_entry();
                    }

                    self.executor.send_message(
                        scheduled_tick,
                        *message.from.get_peer_id(),
                        message.message,
                    );
                    continue;
                }
                SwarmEvent::TestEvent => {
                    let (scheduled_tick, event) = self
                        .pending_test_event
                        .first_entry()
                        .expect("logic error, should be nonempty")
                        .remove_entry();

                    assert_eq!(tick, scheduled_tick);

                    let router_cmds = self.peer_disc_driver.update(event);
                    self.executor.update_tick(tick);
                    self.executor.exec(router_cmds);

                    continue;
                }
            }
        }
        false
    }

    fn push_inbound_message(
        &mut self,
        sched_tick: Duration,
        message: LinkMessage<SwarmPubKeyType<S>, S::TransportMessage>,
    ) {
        self.pending_inbound_messages
            .entry(sched_tick)
            .or_default()
            .push_back(message);
    }
}

#[derive(Debug)]
struct RoutingTable<P: PubKey> {
    addr_to_id: BTreeMap<SocketAddrV4, NodeId<P>>,
    id_to_addr: BTreeMap<NodeId<P>, SocketAddrV4>,
}

impl<P: PubKey> RoutingTable<P> {
    fn new() -> Self {
        Self {
            addr_to_id: Default::default(),
            id_to_addr: Default::default(),
        }
    }

    fn route(&self, addr: &SocketAddrV4) -> Option<&NodeId<P>> {
        self.addr_to_id.get(addr)
    }

    fn register(&mut self, addr: SocketAddrV4, node_id: NodeId<P>) {
        // duplicate addr/id registering is test configuration error
        assert!(self.addr_to_id.insert(addr, node_id).is_none());
        assert!(self.id_to_addr.insert(node_id, addr).is_none());
    }

    fn unregister(&mut self, node_id: &NodeId<P>) {
        // attempting to unregister a non-existent node_id is a test logic error
        let addr = self.id_to_addr.remove(node_id).expect("exists");
        assert!(self.addr_to_id.remove(&addr).is_some());
    }
}

pub struct Nodes<S: PeerDiscSwarmRelation> {
    states: BTreeMap<NodeId<SwarmPubKeyType<S>>, Node<S>>,
    routing_table: RoutingTable<SwarmPubKeyType<S>>,
    tick: Duration,

    rng: ChaCha8Rng,
    current_seed: usize,
}

impl<S> Nodes<S>
where
    S: PeerDiscSwarmRelation,
{
    fn update_rng(&mut self) {
        self.current_seed = self.rng.r#gen();
    }

    fn peek_event(&self) -> Option<(Duration, SwarmEvent, NodeId<SwarmPubKeyType<S>>)> {
        let events = self
            .states
            .iter()
            .filter_map(|(id, node)| {
                node.peek_event()
                    .map(|(tick, event_type)| (tick, event_type, *id))
            })
            .min_set_by(|a, b| a.0.cmp(&b.0));

        if !events.is_empty() {
            Some(events[self.current_seed % events.len()])
        } else {
            None
        }
    }

    pub fn peek_tick(&self) -> Option<Duration> {
        self.peek_event().map(|(tick, _, _)| tick)
    }

    // step until exactly the next event, either internal or external event out of all the nodes.
    pub fn step_until(&mut self, until_tick: Duration) -> bool {
        while let Some((tick, _event_type, id)) = self.peek_event() {
            let _span_entered =
                tracing::trace_span!("step", tick = format!("{:?}", tick)).entered();
            if tick > until_tick {
                break;
            }
            self.update_rng();

            let node = self
                .states
                .get_mut(&id)
                .expect("logic error, should be nonempty");

            let mut emitted_messages = Vec::new();
            let state_updated = node.step_until(tick, &mut emitted_messages);
            self.tick = tick;

            for (sched_tick, message) in emitted_messages {
                assert_ne!(message.from, message.to);
                // Convert target node_id to address using sender's peer table
                let peer_disc_state = self
                    .states
                    .get(&id)
                    .expect("logic error, should be nonempty")
                    .peer_disc_driver
                    .get_peer_disc_state();
                let Some(peer_addr) = peer_disc_state
                    .get_pending_addr_by_id(message.to.get_peer_id())
                    .or_else(|| peer_disc_state.get_addr_by_id(message.to.get_peer_id()))
                else {
                    debug!(to=?message.to.get_peer_id(), "dropping outbound message: peer addr not found");
                    continue;
                };

                // Find the target node with routing_table, which is the ground
                // truth for name records
                let Some(target_node) = self.routing_table.route(&peer_addr) else {
                    debug!(to=?message.to.get_peer_id(), addr=?peer_addr, "routing failed: no node found with addr");
                    continue;
                };

                if message.to.get_peer_id() != target_node {
                    debug!("local peer table outdated");
                }

                let node = self
                    .states
                    .get_mut(target_node)
                    .expect("routing table contain only entries for existing states");
                node.push_inbound_message(sched_tick, message);
            }
            if state_updated {
                return true;
            }
        }
        false
    }

    pub fn states(&self) -> &BTreeMap<NodeId<SwarmPubKeyType<S>>, Node<S>> {
        &self.states
    }

    pub fn add_state<B>(&mut self, node_builder: NodeBuilder<S, B>)
    where
        B: PeerDiscoveryAlgoBuilder<PeerDiscoveryAlgoType = S::PeerDiscoveryAlgoType>,
    {
        let id = node_builder.id;
        let addr = node_builder.addr;
        let _node_span_entered = tracing::trace_span!("node", id = format!("{}", id)).entered();
        self.routing_table.register(addr, id);
        let mut node = node_builder.build();
        for (existing_id, existing_node) in &mut self.states {
            existing_node.executor.id_to_addr.insert(id, addr);
            node.executor
                .id_to_addr
                .insert(*existing_id, self.routing_table.id_to_addr[existing_id]);
        }
        self.states.insert(id, node);
    }

    pub fn remove_state(
        &mut self,
        node_id: &NodeId<CertificateSignaturePubKey<S::SignatureType>>,
    ) -> Option<Node<S>> {
        self.routing_table.unregister(node_id);
        let removed = self.states.remove(node_id);
        for existing_node in self.states.values_mut() {
            existing_node.executor.id_to_addr.remove(node_id);
        }
        removed
    }

    pub fn insert_test_event(
        &mut self,
        node_id: &NodeId<CertificateSignaturePubKey<S::SignatureType>>,
        sched_tick: Duration,
        event: PeerDiscoveryEvent<S::SignatureType>,
    ) {
        if let Some(node) = self.states.get_mut(node_id) {
            node.pending_test_event.insert(sched_tick, event);
        }
    }
}
