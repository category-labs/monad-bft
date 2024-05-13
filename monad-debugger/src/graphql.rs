use std::{
    fmt::{Debug, Formatter},
    ops::Deref,
    time::Duration,
};

use async_graphql::{Context, NewType, Object, Union};
use monad_consensus::{
    messages::consensus_message::{ConsensusMessage, ProtocolMessage},
    validation::signing::{Unvalidated, Unverified},
};
use monad_consensus_types::{
    block::{Block, BlockType},
    metrics::Metrics,
    quorum_certificate::QuorumCertificate,
    timeout::{HighQcRoundSigColTuple, TimeoutCertificate, TimeoutInfo},
};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, PubKey},
    NopSignature,
};
use monad_executor_glue::{
    AsyncStateVerifyEvent, BlockSyncEvent, ConsensusEvent, MempoolEvent, MetricsEvent, MonadEvent,
    ValidatorEvent,
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
    async fn metrics(&self) -> GraphQLMetrics<'s> {
        GraphQLMetrics(self.0.state.metrics())
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

struct GraphQLMetrics<'s>(&'s Metrics);
#[Object]
impl<'s> GraphQLMetrics<'s> {
    async fn consensus_created_qc(&self) -> u32 {
        self.0.consensus_events.created_qc.try_into().unwrap()
    }
    async fn consensus_local_timeout(&self) -> u32 {
        self.0.consensus_events.local_timeout.try_into().unwrap()
    }
    async fn consensus_handle_proposal(&self) -> u32 {
        self.0.consensus_events.handle_proposal.try_into().unwrap()
    }
    async fn consensus_failed_txn_validation(&self) -> u32 {
        self.0
            .consensus_events
            .failed_txn_validation
            .try_into()
            .unwrap()
    }
    async fn consensus_invalid_proposal_round_leader(&self) -> u32 {
        self.0
            .consensus_events
            .invalid_proposal_round_leader
            .try_into()
            .unwrap()
    }
    async fn consensus_out_of_order_proposals(&self) -> u32 {
        self.0
            .consensus_events
            .out_of_order_proposals
            .try_into()
            .unwrap()
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
    MetricsEvent(GraphQLMetricsEvent<'s>),
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
            MonadEventType::MetricsEvent(event) => Self::MetricsEvent(GraphQLMetricsEvent(event)),
        }
    }
}

struct WrappedUnverified<'a>(
    pub &'a Unverified<SignatureType, Unvalidated<ConsensusMessage<SignatureCollectionType>>>,
);

impl<'a> Debug for WrappedUnverified<'a> {
    #[allow(deprecated)]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let message = self.0.get_obj_unsafe();
        f.debug_struct("ConsensusMessage")
            .field("version", &message.version)
            .field("message", &WrappedProtocolMessage(&message.message))
            .finish()
    }
}

struct WrappedProtocolMessage<'a>(pub &'a ProtocolMessage<SignatureCollectionType>);

impl<'a> Debug for WrappedProtocolMessage<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            ProtocolMessage::Proposal(p) => {
                let mut ds = f.debug_struct("Proposal");
                let mut d = ds.field("block", &WrappedBlock(&p.block));
                if p.last_round_tc.is_some() {
                    d = d.field(
                        "last_round_tc",
                        &WrappedTimeoutCertificate(p.last_round_tc.as_ref().unwrap()),
                    );
                }
                d.finish()
            }
            ProtocolMessage::Vote(v) => f
                .debug_struct("Vote")
                .field("vote", &v.vote)
                .finish_non_exhaustive(),
            ProtocolMessage::Timeout(t) => {
                let mut ds = f.debug_struct("Timeout");
                let mut d = ds.field("round", &t.timeout.tminfo.round).field(
                    "high_qc",
                    &WrappedQuorumCertificate(&t.timeout.tminfo.high_qc),
                );
                if t.timeout.last_round_tc.is_some() {
                    d = d.field(
                        "last_round_tc",
                        &WrappedTimeoutCertificate(t.timeout.last_round_tc.as_ref().unwrap()),
                    );
                }
                d.finish()
            }
        }
    }
}

struct WrappedBlock<'a>(pub &'a Block<SignatureCollectionType>);
impl<'a> Debug for WrappedBlock<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("author", &self.0.author.to_string())
            .field("round", &self.0.round)
            .field("qc", &WrappedQuorumCertificate(&self.0.qc))
            .field("id", &self.0.get_id())
            .field("txn_payload_len", &self.0.payload.txns.bytes().len())
            .field("seq_num", &self.0.payload.seq_num)
            .field(
                "execution_state_root",
                &self.0.payload.header.state_root.0.to_string(),
            )
            .finish_non_exhaustive()
    }
}
struct WrappedTimeoutCertificate<'a>(pub &'a TimeoutCertificate<SignatureCollectionType>);
impl<'a> Debug for WrappedTimeoutCertificate<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TC")
            .field("round", &self.0.round)
            .field(
                "high_qc_rounds",
                &self
                    .0
                    .high_qc_rounds
                    .iter()
                    .map(|qc| qc.high_qc_round.qc_round.0)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}
struct WrappedHighQcRoundSigColTuple<'a>(pub &'a HighQcRoundSigColTuple<SignatureCollectionType>);
impl<'a> Debug for WrappedHighQcRoundSigColTuple<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("")
            .field(&self.0.high_qc_round)
            .field(&self.0.sigs)
            .finish()
    }
}

struct WrappedTimeoutInfo<'a>(pub &'a TimeoutInfo<SignatureCollectionType>);

impl<'a> Debug for WrappedTimeoutInfo<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let info = self.0;
        f.debug_struct("TimeoutInfo")
            .field("round", &info.round)
            .field("high_qc", &WrappedQuorumCertificate(&info.high_qc))
            .finish()
    }
}

struct WrappedQuorumCertificate<'a>(pub &'a QuorumCertificate<SignatureCollectionType>);
impl<'a> Debug for WrappedQuorumCertificate<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let qc = self.0;
        let mut debug_struct = f.debug_struct("QC");
        let d = debug_struct.field("vote", &qc.info.vote);
        if !qc.signatures.sigs.is_empty() {
            d.field(
                "sigs",
                &qc.signatures
                    .sigs
                    .iter()
                    .map(WrappedNopSignature)
                    .collect::<Vec<_>>(),
            )
            .field("signature_hash", &qc.get_hash().to_string())
        } else {
            d
        }
        .finish_non_exhaustive()
    }
}

struct WrappedNopSignature<'a>(pub &'a NopSignature);
impl<'a> Debug for WrappedNopSignature<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NopSignature")
            .field("pubkey", &self.0.pubkey.to_string())
            .field("id", &self.0.id)
            .finish()
    }
}

struct GraphQLConsensusEvent<'s>(&'s ConsensusEvent<SignatureType, SignatureCollectionType>);
impl<'s> Debug for GraphQLConsensusEvent<'s> {
    #[allow(deprecated)]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            ConsensusEvent::Message {
                sender,
                unverified_message,
            } => f
                .debug_struct("Message")
                .field("sender", &sender.to_string())
                // .field("version", &unverified_message.get_obj_unsafe().version)
                .field(
                    "message",
                    &WrappedProtocolMessage(&unverified_message.get_obj_unsafe().message),
                )
                .finish(),
            _ => writeln!(f, "{:?}", self.0),
        }
    }
}

#[Object]
impl<'s> GraphQLConsensusEvent<'s> {
    async fn debug(&self) -> String {
        format!("{:#?}", self)
    }
}

struct GraphQLBlockSyncEvent<'s>(&'s BlockSyncEvent<SignatureCollectionType>);
#[Object]
impl<'s> GraphQLBlockSyncEvent<'s> {
    async fn debug(&self) -> String {
        format!("{:#?}", self.0)
    }
}

struct GraphQLValidatorEvent<'s>(&'s ValidatorEvent<SignatureCollectionType>);
#[Object]
impl<'s> GraphQLValidatorEvent<'s> {
    async fn debug(&self) -> String {
        format!("{:#?}", self.0)
    }
}

struct GraphQLMempoolEvent<'s>(&'s MempoolEvent);
#[Object]
impl<'s> GraphQLMempoolEvent<'s> {
    async fn debug(&self) -> String {
        format!("{:#?}", self.0)
    }
}

struct GraphQLAsyncStateVerifyEvent<'s>(&'s AsyncStateVerifyEvent<SignatureCollectionType>);
#[Object]
impl<'s> GraphQLAsyncStateVerifyEvent<'s> {
    async fn debug(&self) -> String {
        format!("{:#?}", self.0)
    }
}

struct GraphQLMetricsEvent<'s>(&'s MetricsEvent);

#[Object]
impl<'s> GraphQLMetricsEvent<'s> {
    async fn debug(&self) -> String {
        format!("{:#?}", self.0)
    }
}
