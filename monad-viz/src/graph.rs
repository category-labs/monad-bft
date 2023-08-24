use std::{cmp::Reverse, collections::HashMap, fmt::Debug, time::Duration};

use monad_crypto::secp256k1::PubKey;
use monad_executor::{
    executor::mock::{MockExecutor, RouterScheduler},
    mock_swarm::{LinkMessage, Nodes},
    timed_event::TimedEvent,
    transformer::Pipeline,
    Message, PeerId,
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

pub trait SimulationConfig<S, RS, T, LGR>
where
    S: monad_executor::State,
    RS: RouterScheduler,
    T: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
{
    fn max_tick(&self) -> Duration;
    fn pipeline(&self) -> &T;
    fn nodes(&self) -> Vec<(PubKey, S::Config, LGR::Config, RS::Config)>;
}

pub struct NodesSimulation<S, RS, T, LGR, C>
where
    S: monad_executor::State,
    RS: RouterScheduler,
    T: Pipeline<RS::Serialized>,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    C: SimulationConfig<S, RS, T, LGR>,
{
    config: C,

    // TODO move stuff below into separate struct
    pub nodes: Nodes<S, RS, T, LGR>,
    current_tick: Duration,
}

impl<S, RS, T, LGR, C> NodesSimulation<S, RS, T, LGR, C>
where
    S: monad_executor::State,
    RS: RouterScheduler,
    RS::Serialized: Eq,
    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,
    MockExecutor<S, RS>: Unpin,
    S::Event: Unpin,
    T: Pipeline<RS::Serialized> + Clone,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    C: SimulationConfig<S, RS, T, LGR>,
    S::Event: Serializable<Vec<u8>> + Deserializable<[u8]> + Debug,
{
    pub fn new(config: C) -> Self {
        Self {
            nodes: Nodes::new(config.nodes(), config.pipeline().clone()),
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
        self.nodes = Nodes::new(self.config.nodes(), self.config.pipeline().clone());
        self.current_tick = self.min_tick();
    }
}

impl<S, RS, T, LGR, C> Graph for NodesSimulation<S, RS, T, LGR, C>
where
    S: monad_executor::State,
    RS: RouterScheduler,
    RS::Serialized: Eq,
    S::Message: Deserializable<RS::M>,
    S::OutboundMessage: Serializable<RS::M>,
    MockExecutor<S, RS>: Unpin,
    S::Event: Unpin,
    T: Pipeline<RS::Serialized> + Clone,
    LGR: PersistenceLogger<Event = TimedEvent<S::Event>>,
    C: SimulationConfig<S, RS, T, LGR>,
    S::Event: Serializable<Vec<u8>> + Deserializable<[u8]> + Debug,
{
    type State = S;
    type Message = RS::Serialized;
    type MessageId = <S::Message as Message>::Id;
    type Event = S::Event;
    type NodeId = PeerId;

    fn state(&self) -> Vec<NodeState<Self::NodeId, S, RS::Serialized, S::Event>> {
        let mut pending_messages: HashMap<_, Vec<_>> = Default::default();
        for Reverse((
            rx_time,
            LinkMessage {
                from,
                to: tx_peer,
                message,
                from_tick: tx_time,
            },
        )) in self.nodes.scheduled_messages()
        {
            pending_messages
                .entry(from)
                .or_default()
                .push(NodeEvent::Message {
                    tx_time: *tx_time,
                    rx_time: *rx_time,
                    tx_peer,
                    message,
                });
        }
        let mut state = self
            .nodes
            .states()
            .iter()
            .map(|(peer_id, (_executor, state, _))| NodeState {
                id: peer_id,
                state,
                pending_events: std::iter::empty()
                    .chain(pending_messages.remove(&peer_id).into_iter().flatten())
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
        while let Some(next_tick) = self.nodes.next_tick() {
            if next_tick > tick {
                break;
            }
            self.nodes.step().unwrap();
        }
        self.current_tick = tick;
    }
}
