use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};

use driver::MockDiscoveryDriver;
use itertools::Itertools;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_peer_discovery::algo::{
    PeerDiscoveryAlgo, PeerDiscoveryBuilder, PeerDiscoveryEvent, PeerDiscoveryMessage,
};
use monad_router_scheduler::{RouterEvent, RouterScheduler};
use monad_transformer::{ID, LinkMessage};
use monad_types::NodeId;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

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
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy)]
pub enum SwarmEvent {
    DriverEvent,
    ExecutorEvent,
    InboundMessage,
}

pub struct NodeBuilder<S, B>
where
    S: PeerDiscSwarmRelation,
{
    pub id: NodeId<SwarmPubKeyType<S>>,
    pub algo_builder: B,
    pub router_scheduler: S::RouterSchedulerType,
    pub seed: u64,
}

impl<S, B> NodeBuilder<S, B>
where
    S: PeerDiscSwarmRelation,
    B: PeerDiscoveryBuilder<PeerDiscoveryAlgoType = S::PeerDiscoveryAlgoType>,
{
    pub fn build(self) -> Node<S> {
        Node::new(self.id, self.algo_builder, self.router_scheduler, self.seed)
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

    rng: ChaCha8Rng,
    current_seed: usize,
}

pub struct MockPeerDiscExecutor<S: PeerDiscSwarmRelation> {
    router: S::RouterSchedulerType,
    tick: Duration,
}

enum MockPeerDiscExecutorEvent<E, PT: PubKey, TransportMessage> {
    Event(E),
    Send(NodeId<PT>, TransportMessage),
}

impl<S: PeerDiscSwarmRelation> Executor for MockPeerDiscExecutor<S> {
    type Command = RouterCommand<
        SwarmPubKeyType<S>,
        <S::RouterSchedulerType as RouterScheduler>::OutboundMessage,
    >;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for cmd in commands {
            match cmd {
                RouterCommand::Publish { target, message } => {
                    self.router.send_outbound(self.tick, target, message)
                }
                RouterCommand::AddEpochValidatorSet { .. } => {}
                RouterCommand::UpdateCurrentRound(..) => {}
                RouterCommand::GetPeers => {}
                RouterCommand::UpdatePeers(_) => {}
                RouterCommand::GetFullNodes => {}
                RouterCommand::UpdateFullNodes(_) => {}
            }
        }
    }

    fn metrics(&self) -> monad_executor::ExecutorMetricsChain {
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
                    MockPeerDiscExecutorEvent::Event(message.event(from))
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
    ) -> Self
    where
        B: PeerDiscoveryBuilder<PeerDiscoveryAlgoType = S::PeerDiscoveryAlgoType>,
    {
        let (peer_disc_driver, init_cmds) = MockDiscoveryDriver::new(algo_builder);

        let mut executor = MockPeerDiscExecutor::<S> {
            router: router_scheduler,
            tick: Default::default(),
        };
        executor.exec(init_cmds);

        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let current_seed = rng.r#gen();

        Node {
            id,
            peer_disc_driver,
            executor,
            pending_inbound_messages: Default::default(),
            rng,
            current_seed,
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
                                    // TODO: add delay to message
                                    let lm = LinkMessage {
                                        from: ID::new(self.id),
                                        to: ID::new(to),
                                        message: ser,

                                        from_tick: tick,
                                    };
                                    emitted_messages.push((tick, lm));
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

pub struct Nodes<S: PeerDiscSwarmRelation> {
    states: BTreeMap<NodeId<SwarmPubKeyType<S>>, Node<S>>,
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
                let node = self
                    .states
                    .get_mut(message.to.get_peer_id())
                    .expect("message must be for known peer");
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
}
