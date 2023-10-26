use std::{cmp::Reverse, time::Duration};

use monad_crypto::secp256k1::PubKey;
use monad_executor::State;
use monad_executor_glue::{Identifiable, PeerId};
use monad_mock_swarm::{
    mock::{MockExecutor, RouterScheduler},
    mock_swarm::{Node, Nodes, UntilTerminator},
    swarm_relation::{SwarmRelation, SwarmStateType},
    transformer::ID,
};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_types::{Deserializable, Serializable};

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

pub struct NodeState<'s, Id, SWM: SwarmRelation, M> {
    pub id: &'s Id,
    pub state: &'s SwarmStateType<SWM>,

    pub pending_events: Vec<NodeEvent<'s, Id, M, <SWM::STATE as State>::Event>>,
}

pub trait Graph {
    type State;
    type Message;
    type MessageId;
    type Event;
    type NodeId;
    type Swarm: SwarmRelation;

    fn state(&self) -> Vec<NodeState<Self::NodeId, Self::Swarm, Self::Message>>;
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

pub trait SimulationConfig<S>
where
    S: SwarmRelation,
{
    fn max_tick(&self) -> Duration;
    fn nodes(
        &self,
    ) -> Vec<(
        ID,
        <S::STATE as State>::Config,
        S::LGRCFG,
        S::RSCFG,
        S::MPCFG,
        S::P,
        u64,
    )>;
}

pub struct NodesSimulation<S, C>
where
    S: SwarmRelation,
    C: SimulationConfig<S>,
{
    config: C,

    // TODO move stuff below into separate struct
    pub nodes: Nodes<S>,
    current_tick: Duration,
}

impl<S, C> NodesSimulation<S, C>
where
    S: SwarmRelation,
    S::Message: Identifiable,
    C: SimulationConfig<S>,

    MonadMessage<S::ST, S::SCT>: Deserializable<S::Message>,
    VerifiedMonadMessage<S::ST, S::SCT>: Serializable<<S::RS as RouterScheduler>::M>,

    MockExecutor<S>: Unpin,
    Node<S>: Send,
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

impl<S, C> Graph for NodesSimulation<S, C>
where
    S: SwarmRelation,
    S::Message: Identifiable,
    C: SimulationConfig<S>,

    MonadMessage<S::ST, S::SCT>: Deserializable<S::Message>,
    VerifiedMonadMessage<S::ST, S::SCT>: Serializable<<S::RS as RouterScheduler>::M>,

    MockExecutor<S>: Unpin,
    Node<S>: Send,
{
    type State = S::STATE;
    type Message = S::Message;
    type MessageId = <S::Message as Identifiable>::Id;
    type Event = <S::STATE as State>::Event;
    type NodeId = PeerId;
    type Swarm = S;

    fn state(&self) -> Vec<NodeState<Self::NodeId, Self::Swarm, Self::Message>> {
        let mut state = self
            .nodes
            .states()
            .iter()
            .map(|(peer_id, node)| NodeState {
                id: peer_id.get_peer_id(),
                state: &node.state,
                pending_events: node
                    .pending_inbound_messages
                    .iter()
                    .map(|Reverse((rx_time, message))| NodeEvent::Message {
                        tx_time: message.from_tick,
                        rx_time: *rx_time,
                        tx_peer: message.from.get_peer_id(),
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
        let term = UntilTerminator::new().until_tick(tick);

        while self.nodes.step_until(&term).is_some() {}
        self.current_tick = tick;
    }
}
