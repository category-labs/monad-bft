use std::{ops::Deref, time::Duration};

use async_graphql::{Context, NewType, Object, Union};
use monad_crypto::certificate_signature::{CertificateSignaturePubKey, PubKey};
use monad_executor_glue::{
    AsyncStateVerifyEvent, BlockSyncEvent, ConsensusEvent, MempoolEvent, MonadEvent, ValidatorEvent,
};
use monad_mock_swarm::{
    node::Node,
    swarm_relation::{DebugSwarmRelation, SwarmRelation},
};
use monad_transformer::ID;
use monad_types::NodeId;

use crate::simulation::Simulation;

type SwarmRelationType = DebugSwarmRelation;
type SignatureType = <SwarmRelationType as SwarmRelation>::SignatureType;
type SignatureCollectionType = <SwarmRelationType as SwarmRelation>::SignatureCollectionType;
type TransportMessage = <SwarmRelationType as SwarmRelation>::TransportMessage;
type MonadEventType = MonadEvent<SignatureType, SignatureCollectionType>;

#[derive(NewType)]
struct GraphQLNodeId(String);
impl<P: PubKey> From<&NodeId<P>> for GraphQLNodeId {
    fn from(node_id: &NodeId<P>) -> Self {
        Self(hex::encode(node_id.pubkey().bytes()))
    }
}
impl<P: PubKey> TryFrom<GraphQLNodeId> for NodeId<P> {
    type Error = &'static str;
    fn try_from(node_id: GraphQLNodeId) -> Result<Self, Self::Error> {
        let bytes = hex::decode(node_id.0).map_err(|_| "failed to parse hex")?;
        Ok(Self::new(
            P::from_bytes(&bytes).map_err(|_| "invalid pubkey")?,
        ))
    }
}

#[derive(NewType)]
pub struct GraphQLTimestamp(i32);
impl GraphQLTimestamp {
    pub fn new(duration: Duration) -> Self {
        Self(duration.as_millis().try_into().unwrap())
    }
}

pub struct GraphQLSimulation(pub *const Simulation);

unsafe impl Send for GraphQLSimulation {}
unsafe impl Sync for GraphQLSimulation {}

impl Deref for GraphQLSimulation {
    type Target = Simulation;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.0 }
    }
}

#[derive(Default)]
pub struct GraphQLRoot;

#[Object]
impl GraphQLRoot {
    async fn current_tick<'ctx>(&self, ctx: &Context<'ctx>) -> Option<GraphQLTimestamp> {
        let simulation = ctx.data_unchecked::<GraphQLSimulation>();
        Some(GraphQLTimestamp::new(simulation.current_tick))
    }

    async fn next_tick<'ctx>(&self, ctx: &Context<'ctx>) -> Option<GraphQLTimestamp> {
        let simulation = ctx.data_unchecked::<GraphQLSimulation>();
        let tick = simulation.swarm.peek_tick()?;
        Some(GraphQLTimestamp::new(tick))
    }

    async fn nodes<'ctx>(
        &self,
        ctx: &Context<'ctx>,
        node_id: Option<GraphQLNodeId>,
    ) -> async_graphql::Result<Vec<GraphQLNode<'ctx>>> {
        let simulation = ctx.data_unchecked::<GraphQLSimulation>();
        if let Some(id) = node_id {
            let swarm_id = ID::new(id.try_into()?);
            let node = simulation
                .swarm
                .states()
                .get(&swarm_id)
                .ok_or("unknown node_id")?;
            Ok(vec![GraphQLNode(node)])
        } else {
            Ok(simulation
                .swarm
                .states()
                .values()
                .map(GraphQLNode)
                .collect())
        }
    }

    async fn event_log<'ctx>(&self, ctx: &Context<'ctx>) -> Vec<GraphQLEventLogEntry<'ctx>> {
        let simulation = ctx.data_unchecked::<GraphQLSimulation>();
        simulation
            .event_log
            .iter()
            .map(|(tick, id, event)| GraphQLEventLogEntry { tick, id, event })
            .collect()
    }
}

struct GraphQLNode<'s>(&'s Node<DebugSwarmRelation>);
#[Object]
impl<'s> GraphQLNode<'s> {
    async fn id(&self) -> GraphQLNodeId {
        self.0.id.get_peer_id().into()
    }
    async fn pending_messages(&self) -> Vec<GraphQLPendingMessage<'s>> {
        self.0
            .pending_inbound_messages
            .iter()
            .flat_map(|(rx_tick, messages)| {
                messages.iter().map(|message| GraphQLPendingMessage {
                    from: &message.from,
                    from_tick: &message.from_tick,
                    rx_tick,
                    message: &message.message,
                })
            })
            .collect()
    }
}

struct GraphQLPendingMessage<'s> {
    from: &'s ID<CertificateSignaturePubKey<SignatureType>>,
    from_tick: &'s Duration,
    rx_tick: &'s Duration,
    message: &'s TransportMessage,
}

#[Object]
impl<'s> GraphQLPendingMessage<'s> {
    async fn from_id(&self) -> GraphQLNodeId {
        self.from.get_peer_id().into()
    }
    async fn from_tick(&self) -> GraphQLTimestamp {
        GraphQLTimestamp::new(*self.from_tick)
    }
    async fn rx_tick(&self) -> GraphQLTimestamp {
        GraphQLTimestamp::new(*self.rx_tick)
    }
    async fn size(&self) -> i64 {
        self.message.len().try_into().unwrap()
    }
}

struct GraphQLEventLogEntry<'s> {
    tick: &'s Duration,
    id: &'s ID<CertificateSignaturePubKey<SignatureType>>,
    event: &'s MonadEventType,
}
#[Object]
impl<'s> GraphQLEventLogEntry<'s> {
    async fn tick(&self) -> GraphQLTimestamp {
        GraphQLTimestamp::new(*self.tick)
    }
    async fn id(&self) -> GraphQLNodeId {
        self.id.get_peer_id().into()
    }
    async fn event(&self) -> GraphQLMonadEvent<'s> {
        self.event.into()
    }
}

#[derive(Union)]
enum GraphQLMonadEvent<'s> {
    ConsensusEvent(GraphQLConsensusEvent<'s>),
    BlockSyncEvent(GraphQLBlockSyncEvent<'s>),
    ValidatorEvent(GraphQLValidatorEvent<'s>),
    MempoolEvent(GraphQLMempoolEvent<'s>),
    AsyncStateVerifyEvent(GraphQLAsyncStateVerifyEvent<'s>),
}

impl<'s> From<&'s MonadEventType> for GraphQLMonadEvent<'s> {
    fn from(event: &'s MonadEventType) -> Self {
        match event {
            MonadEvent::ConsensusEvent(event) => Self::ConsensusEvent(GraphQLConsensusEvent(event)),
            MonadEvent::BlockSyncEvent(event) => Self::BlockSyncEvent(GraphQLBlockSyncEvent(event)),
            MonadEvent::ValidatorEvent(event) => Self::ValidatorEvent(GraphQLValidatorEvent(event)),
            MonadEvent::MempoolEvent(event) => Self::MempoolEvent(GraphQLMempoolEvent(event)),
            MonadEvent::AsyncStateVerifyEvent(event) => {
                Self::AsyncStateVerifyEvent(GraphQLAsyncStateVerifyEvent(event))
            }
        }
    }
}

struct GraphQLConsensusEvent<'s>(&'s ConsensusEvent<SignatureType, SignatureCollectionType>);
#[Object]
impl<'s> GraphQLConsensusEvent<'s> {
    async fn debug(&self) -> String {
        format!("{:?}", self.0)
    }
}

struct GraphQLBlockSyncEvent<'s>(&'s BlockSyncEvent<SignatureCollectionType>);
#[Object]
impl<'s> GraphQLBlockSyncEvent<'s> {
    async fn debug(&self) -> String {
        format!("{:?}", self.0)
    }
}

struct GraphQLValidatorEvent<'s>(&'s ValidatorEvent<SignatureCollectionType>);
#[Object]
impl<'s> GraphQLValidatorEvent<'s> {
    async fn debug(&self) -> String {
        format!("{:?}", self.0)
    }
}

struct GraphQLMempoolEvent<'s>(&'s MempoolEvent<SignatureCollectionType>);
#[Object]
impl<'s> GraphQLMempoolEvent<'s> {
    async fn debug(&self) -> String {
        format!("{:?}", self.0)
    }
}

struct GraphQLAsyncStateVerifyEvent<'s>(&'s AsyncStateVerifyEvent<SignatureCollectionType>);
#[Object]
impl<'s> GraphQLAsyncStateVerifyEvent<'s> {
    async fn debug(&self) -> String {
        format!("{:?}", self.0)
    }
}
