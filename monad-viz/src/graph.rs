use std::time::Duration;

use monad_crypto::secp256k1::SecpPubKey;
use monad_executor::{
    executor::mock::MockExecutor,
    mock_swarm::{Nodes, Transformer},
    Message, PeerId,
};
use monad_wal::PersistenceLogger;
pub enum NodeEvent<'s, Id, M, MId, E> {
    Message {
        tx_time: Duration,
        rx_time: Duration,
        tx_peer: &'s Id,
        message: &'s M,
    },
    Ack {
        tx_time: Duration,
        rx_time: Duration,
        tx_peer: &'s Id,
        message_id: &'s MId,
    },
    Timer {
        scheduled_time: Duration,
        trip_time: Duration,
        event: &'s E,
    },
}

pub struct NodeState<'s, Id, S, M, MId, E> {
    pub id: &'s Id,
    pub state: &'s S,

    pub pending_events: Vec<NodeEvent<'s, Id, M, MId, E>>,
}

pub trait Graph {
    type State;
    type Message;
    type MessageId;
    type Event;
    type NodeId;

    fn state(
        &self,
    ) -> Vec<NodeState<Self::NodeId, Self::State, Self::Message, Self::MessageId, Self::Event>>;
    fn tick(&self) -> Duration;
    fn min_tick(&self) -> Duration;
    fn max_tick(&self) -> Duration;

    fn set_tick(&mut self, tick: Duration);
}

pub trait SimulationConfig<S, T, LGR>
where
    S: monad_executor::State,
    T: Transformer<S::Message>,
    LGR: PersistenceLogger<Event = S::Event>,
{
    fn max_tick(&self) -> Duration;
    fn transformer(&self) -> &T;
    fn nodes(&self) -> Vec<(SecpPubKey, S::Config, LGR::Config)>;
}

pub struct NodesSimulation<S, T, LGR, C>
where
    S: monad_executor::State,
    T: Transformer<S::Message>,
    LGR: PersistenceLogger<Event = S::Event>,
    C: SimulationConfig<S, T, LGR>,
{
    config: C,

    // TODO move stuff below into separate struct
    pub nodes: Nodes<S, T, LGR>,
    current_tick: Duration,
}

impl<S, T, LGR, C> NodesSimulation<S, T, LGR, C>
where
    S: monad_executor::State,
    MockExecutor<S>: Unpin,
    T: Transformer<S::Message> + Clone,
    LGR: PersistenceLogger<Event = S::Event>,
    C: SimulationConfig<S, T, LGR>,
{
    pub fn new(config: C) -> Self {
        Self {
            nodes: Nodes::new(config.nodes(), config.transformer().clone()),
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
        self.nodes = Nodes::new(self.config.nodes(), self.config.transformer().clone());
        self.current_tick = self.min_tick();
    }
}

impl<S, T, LGR, C> Graph for NodesSimulation<S, T, LGR, C>
where
    S: monad_executor::State,
    MockExecutor<S>: Unpin,
    T: Transformer<S::Message> + Clone,
    LGR: PersistenceLogger<Event = S::Event>,
    C: SimulationConfig<S, T, LGR>,
{
    type State = S;
    type Message = S::Message;
    type MessageId = <S::Message as Message>::Id;
    type Event = S::Event;
    type NodeId = PeerId;

    fn state(
        &self,
    ) -> Vec<NodeState<Self::NodeId, S, S::Message, <S::Message as Message>::Id, S::Event>> {
        let mut state =
            self.nodes
                .states()
                .iter()
                .map(|(peer_id, (executor, state, _))| NodeState {
                    id: peer_id,
                    state,
                    pending_events: std::iter::empty()
                        .chain(executor.pending_timer().iter().map(|timer_event| {
                            NodeEvent::Timer {
                                scheduled_time: timer_event.scheduled_tick,
                                trip_time: timer_event.tick,
                                event: &timer_event.event,
                            }
                        }))
                        .chain(executor.pending_messages().iter().map(|message_event| {
                            NodeEvent::Message {
                                tx_time: message_event.tx_tick,
                                rx_time: message_event.tick,
                                tx_peer: &message_event.from,
                                message: &message_event.t,
                            }
                        }))
                        .chain(
                            executor
                                .pending_acks()
                                .iter()
                                .map(|ack_event| NodeEvent::Ack {
                                    tx_time: ack_event.tx_tick,
                                    rx_time: ack_event.tick,
                                    tx_peer: &ack_event.from,
                                    message_id: &ack_event.t,
                                }),
                        )
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
