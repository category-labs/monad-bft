use std::time::Duration;

use monad_crypto::secp256k1::PubKey;
use monad_executor::{executor::mock::MockExecutor, mock_swarm::Nodes, Message, PeerId};

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

pub struct NodesSimulation<S, C, L>
where
    S: monad_executor::State,
    C: Fn() -> Vec<(PubKey, S::Config)>,
    L: Fn(&PeerId, &PeerId) -> Duration,
{
    init_configs_gen: C,
    init_compute_latency: L,
    max_tick: Duration,

    // TODO move stuff below into separate struct
    nodes: Nodes<S, L>,
    current_tick: Duration,
}

impl<S, C, L> NodesSimulation<S, C, L>
where
    S: monad_executor::State,
    MockExecutor<S>: Unpin,
    C: Fn() -> Vec<(PubKey, S::Config)>,
    L: Fn(&PeerId, &PeerId) -> Duration + Clone,
{
    pub fn new(configs_gen: C, compute_latency: L, max_tick: Duration) -> Self {
        let configs = configs_gen();
        Self {
            init_configs_gen: configs_gen,
            init_compute_latency: compute_latency.clone(),
            max_tick,

            nodes: Nodes::new(configs, compute_latency),
            current_tick: Duration::from_secs(0),
        }
    }

    fn reset(&mut self) {
        self.nodes = Nodes::new((self.init_configs_gen)(), self.init_compute_latency.clone());
        self.current_tick = self.min_tick();
    }
}

impl<S, C, L> Graph for NodesSimulation<S, C, L>
where
    S: monad_executor::State,
    MockExecutor<S>: Unpin,
    C: Fn() -> Vec<(PubKey, S::Config)>,
    L: Fn(&PeerId, &PeerId) -> Duration + Clone,
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
                .map(|(peer_id, (executor, state))| NodeState {
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
        self.max_tick
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
