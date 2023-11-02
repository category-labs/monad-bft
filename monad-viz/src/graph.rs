use std::{cmp::Reverse, time::Duration};

use monad_block_sync::BlockSyncProcess;
use monad_consensus_state::ConsensusProcess;
use monad_consensus_types::{
    message_signature::MessageSignature, signature_collection::SignatureCollection,
};
use monad_crypto::secp256k1::PubKey;
use monad_executor::State;
use monad_executor_glue::PeerId;
use monad_mock_swarm::{
    mock::MockExecutor,
    mock_swarm::{Node, Nodes, UntilTerminator},
    swarm_relation::SwarmRelation,
    transformer::ID,
};
use monad_state::MonadState;
use monad_validator::{leader_election::LeaderElection, validator_set::ValidatorSetType};

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

pub struct NodeState<'s, Id, S: SwarmRelation, M> {
    pub id: &'s Id,
    pub state: &'s S::State,

    pub pending_events: Vec<NodeEvent<'s, Id, M, <S::State as State>::Event>>,
}

pub trait Graph {
    type State;
    type InboundMessage;
    type NodeId;
    type Swarm: SwarmRelation;

    fn state(&self) -> Vec<NodeState<Self::NodeId, Self::Swarm, Self::InboundMessage>>;
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
        <S::State as State>::Config,
        S::LoggerConfig,
        S::RouterSchedulerConfig,
        S::MempoolConfig,
        S::Pipeline,
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

impl<S, C, CT, ST, SCT, VT, LT, BST> NodesSimulation<S, C>
where
    S: SwarmRelation<
        State = MonadState<CT, ST, SCT, VT, LT, BST>,
        TransportMessage = <S as SwarmRelation>::InboundMessage,
    >,
    C: SimulationConfig<S>,

    CT: ConsensusProcess<SCT> + PartialEq + Eq,
    ST: MessageSignature,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
    LT: LeaderElection,
    BST: BlockSyncProcess<SCT, VT>,

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

impl<S, C, CT, ST, SCT, VT, LT, BST> Graph for NodesSimulation<S, C>
where
    S: SwarmRelation<
        State = MonadState<CT, ST, SCT, VT, LT, BST>,
        TransportMessage = <S as SwarmRelation>::InboundMessage,
    >,
    C: SimulationConfig<S>,

    CT: ConsensusProcess<SCT> + PartialEq + Eq,
    ST: MessageSignature,
    SCT: SignatureCollection,
    VT: ValidatorSetType,
    LT: LeaderElection,
    BST: BlockSyncProcess<SCT, VT>,

    MockExecutor<S>: Unpin,
    Node<S>: Send,
{
    type State = S::State;
    type InboundMessage = S::InboundMessage;
    type NodeId = PeerId;
    type Swarm = S;

    fn state(&self) -> Vec<NodeState<Self::NodeId, Self::Swarm, Self::InboundMessage>> {
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
