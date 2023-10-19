use std::{cmp::Reverse, time::Duration};

use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_crypto::secp256k1::PubKey;
use monad_executor::timed_event::TimedEvent;
use monad_executor_glue::{Identifiable, MonadEvent, PeerId};
use monad_mock_swarm::{
    mock::{MockExecutor, MockableExecutor, RouterScheduler},
    mock_swarm::{Node, Nodes},
    transformer::Pipeline,
};
use monad_types::{Deserializable, Serializable};
use monad_wal::PersistenceLogger;
pub enum NodeEvent<'s, Id, M, E> {
    Message {
        tx_time: Duration,
        rx_time: Duration,
        tx_peer: &'s Id,
        message: &'s M,
    },
    Timer {
        scheduled_time: Duration,
        trip_time: Duration,
        event: &'s E,
    },
}

pub struct NodeState<'s, Id, S, M, E> {
    pub id: &'s Id,
    pub state: &'s S,

    pub pending_events: Vec<NodeEvent<'s, Id, M, E>>,
}

pub trait Graph {
    type State;
    type Message;
    type MessageId;
    type Event;
    type NodeId;

    fn state(&self) -> Vec<NodeState<Self::NodeId, Self::State, Self::Message, Self::Event>>;
    fn tick(&self) -> Duration;
    fn min_tick(&self) -> Duration;
    fn max_tick(&self) -> Duration;

    fn set_tick(&mut self, tick: Duration);
}

pub trait ReplayConfig<S>
where
    S: monad_executor::State,
{
    fn max_tick(&self) -> Duration;
    fn nodes(&self) -> Vec<(PubKey, S::Config)>;
}

pub trait SimulationConfig<S, RS, P, LGR, ME, ST, SCT>
where
    S: monad_executor::State,
    RS: RouterScheduler,
    P: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    ME: MockableExecutor<SignatureCollection = SCT>,
{
    fn max_tick(&self) -> Duration;
    fn nodes(
        &self,
    ) -> Vec<(
        PubKey,
        S::Config,
        LGR::Config,
        RS::Config,
        ME::Config,
        P,
        u64,
    )>;
}

pub struct NodesSimulation<S, RS, P, LGR, C, ME, ST, SCT>
where
    S: monad_executor::State<Event = MonadEvent<ST, SCT>>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    RS: RouterScheduler,
    P: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    C: SimulationConfig<S, RS, P, LGR, ME, ST, SCT>,
    ME: MockableExecutor<SignatureCollection = SCT>,
{
    config: C,

    // TODO move stuff below into separate struct
    pub nodes: Nodes<S, RS, P, LGR, ME, ST, SCT>,
    current_tick: Duration,
}

impl<S, RS, P, LGR, C, ME, ST, SCT> NodesSimulation<S, RS, P, LGR, C, ME, ST, SCT>
where
    S: monad_executor::State<Event = MonadEvent<ST, SCT>, SignatureCollection = SCT>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    RS: RouterScheduler,
    P: Pipeline<RS::Serialized> + Clone,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    C: SimulationConfig<S, RS, P, LGR, ME, ST, SCT>,
    ME: MockableExecutor<Event = S::Event, SignatureCollection = SCT>,

    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,
    MockExecutor<S, RS, ME, ST, SCT>: Unpin,

    Node<S, RS, P, LGR, ME, ST, SCT>: Send,
{
    pub fn new(config: C) -> Self {
        Self {
            nodes: Nodes::new(config.nodes()),
            current_tick: Duration::ZERO,

            config,
        }
    }

    pub fn config(&self) -> &C {
        &self.config
    }

    pub fn update_config(&mut self, config: C) {
        self.config = config;
        let current_tick = self.current_tick;
        self.reset();
        self.set_tick(current_tick);
    }

    fn reset(&mut self) {
        self.nodes = Nodes::new(self.config.nodes());
        self.current_tick = self.min_tick();
    }
}

impl<S, RS, P, LGR, C, ME, ST, SCT> Graph for NodesSimulation<S, RS, P, LGR, C, ME, ST, SCT>
where
    S: monad_executor::State<Event = MonadEvent<ST, SCT>, SignatureCollection = SCT>,
    ST: MessageSignature,
    SCT: SignatureCollection,
    RS: RouterScheduler,
    P: Pipeline<RS::Serialized> + Clone,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    C: SimulationConfig<S, RS, P, LGR, ME, ST, SCT>,
    ME: MockableExecutor<Event = S::Event, SignatureCollection = SCT>,

    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,
    MockExecutor<S, RS, ME, ST, SCT>: Unpin,

    Node<S, RS, P, LGR, ME, ST, SCT>: Send,
{
    type State = S;
    type Message = RS::Serialized;
    type MessageId = <S::Message as Identifiable>::Id;
    type Event = S::Event;
    type NodeId = PeerId;

    fn state(&self) -> Vec<NodeState<Self::NodeId, S, RS::Serialized, MonadEvent<ST, SCT>>> {
        let mut state = self
            .nodes
            .states()
            .iter()
            .map(|(peer_id, node)| NodeState {
                id: peer_id,
                state: &node.state,
                pending_events: node
                    .pending_inbound_messages
                    .iter()
                    .map(|Reverse((rx_time, message))| NodeEvent::Message {
                        tx_time: message.from_tick,
                        rx_time: *rx_time,
                        tx_peer: &message.from,
                        message: &message.message,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        state.sort_by_key(|node| node.id);
        state
    }

    fn tick(&self) -> Duration {
        self.current_tick
    }

    fn min_tick(&self) -> Duration {
        Duration::from_secs(0)
    }

    fn max_tick(&self) -> Duration {
        self.config().max_tick()
    }

    fn set_tick(&mut self, tick: Duration) {
        if tick < self.current_tick {
            self.reset();
        }
        assert!(tick >= self.current_tick);
        while self.nodes.step_until(tick, usize::MAX).is_some() {}
        self.current_tick = tick;
    }
}
