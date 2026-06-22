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
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
    ops::Deref,
    time::Duration,
};

use async_graphql::{Context, NewType, Object, Union};
use bytes::Bytes;
use monad_consensus_types::{
    block::ConsensusBlockHeader, metrics::Metrics, payload::ConsensusBlockBodyId,
};
use monad_crypto::certificate_signature::{CertificateSignaturePubKey, PubKey};
use monad_executor_glue::{
    BlockSyncEvent, ConfigEvent, ConsensusEvent, ControlPanelEvent, MempoolEvent, MonadEvent,
    StateSyncEvent, ValidatorEvent,
};
use monad_mock_swarm::{
    node::Node,
    swarm_relation::{DebugSwarmRelation, SwarmRelation},
};
use monad_transformer::ID;
use monad_types::{
    BlockId, NodeId, Round, SeqNum, Serializable, GENESIS_ROUND, GENESIS_SEQ_NUM,
};
use monad_updaters::ledger::MockableLedger;

use crate::{
    graphql::message::GraphQLMonadMessage,
    network::{
        EffectiveLinkRule, NetworkCommand, NetworkCommandAction, NetworkConfig, NetworkNodeState,
    },
    simulation::Simulation,
};

type SwarmRelationType = DebugSwarmRelation;
type SignatureType = <SwarmRelationType as SwarmRelation>::SignatureType;
type SignatureCollectionType = <SwarmRelationType as SwarmRelation>::SignatureCollectionType;
type ExecutionProtocolType = <SwarmRelationType as SwarmRelation>::ExecutionProtocolType;
type TransportMessage = <SwarmRelationType as SwarmRelation>::TransportMessage;
type MonadEventType = MonadEvent<SignatureType, SignatureCollectionType, ExecutionProtocolType>;
type BlockHeaderType = ConsensusBlockHeader<SignatureType, SignatureCollectionType, ExecutionProtocolType>;

mod message;

pub(crate) fn block_id_string(block_id: &BlockId) -> String {
    hex::encode(block_id.0.as_ref())
}

pub(crate) fn block_body_id_string(body_id: &ConsensusBlockBodyId) -> String {
    hex::encode(body_id.0.as_ref())
}

pub(crate) struct GraphQLBlockRef {
    id: BlockId,
    parent_id: BlockId,
    body_id: ConsensusBlockBodyId,
    seq_num: SeqNum,
    round: Round,
    author: NodeId<CertificateSignaturePubKey<SignatureType>>,
}

impl GraphQLBlockRef {
    pub(crate) fn from_header(header: &BlockHeaderType) -> Self {
        Self {
            id: header.get_id(),
            parent_id: header.get_parent_id(),
            body_id: header.block_body_id,
            seq_num: header.seq_num,
            round: header.block_round,
            author: header.author,
        }
    }
}

#[Object]
impl GraphQLBlockRef {
    async fn id(&self) -> String {
        block_id_string(&self.id)
    }

    async fn parent_id(&self) -> String {
        block_id_string(&self.parent_id)
    }

    async fn body_id(&self) -> String {
        block_body_id_string(&self.body_id)
    }

    async fn seq_num(&self) -> GraphQLSeqNum {
        GraphQLSeqNum::new(self.seq_num)
    }

    async fn round(&self) -> GraphQLRound {
        GraphQLRound::new(self.round)
    }

    async fn author_id(&self) -> GraphQLNodeId {
        (&self.author).into()
    }
}

struct GraphQLLedgerBlock {
    id: BlockId,
    parent_id: Option<BlockId>,
    body_id: Option<ConsensusBlockBodyId>,
    seq_num: SeqNum,
    round: Round,
    author: Option<NodeId<CertificateSignaturePubKey<SignatureType>>>,
    coherent: bool,
    finalized: bool,
    children_ids: Vec<BlockId>,
}

#[Object]
impl GraphQLLedgerBlock {
    async fn id(&self) -> String {
        block_id_string(&self.id)
    }

    async fn parent_id(&self) -> Option<String> {
        self.parent_id.as_ref().map(block_id_string)
    }

    async fn body_id(&self) -> Option<String> {
        self.body_id.as_ref().map(block_body_id_string)
    }

    async fn seq_num(&self) -> GraphQLSeqNum {
        GraphQLSeqNum::new(self.seq_num)
    }

    async fn round(&self) -> GraphQLRound {
        GraphQLRound::new(self.round)
    }

    async fn author_id(&self) -> Option<GraphQLNodeId> {
        self.author.as_ref().map(Into::into)
    }

    async fn coherent(&self) -> bool {
        self.coherent
    }

    async fn finalized(&self) -> bool {
        self.finalized
    }

    async fn children_ids(&self) -> Vec<String> {
        self.children_ids.iter().map(block_id_string).collect()
    }
}

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

#[derive(NewType)]
pub struct GraphQLRound(i64);
impl GraphQLRound {
    pub fn new(round: Round) -> Self {
        Self(round.0.try_into().unwrap())
    }
}

#[derive(NewType)]
pub struct GraphQLSeqNum(i64);
impl GraphQLSeqNum {
    pub fn new(seq_num: SeqNum) -> Self {
        Self(seq_num.0.try_into().unwrap())
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

    async fn current_leader<'ctx>(&self, ctx: &Context<'ctx>) -> Option<GraphQLNodeId> {
        let simulation = ctx.data_unchecked::<GraphQLSimulation>();
        let live_states = simulation.swarm.states().values().filter_map(|node| {
            if node
                .outbound_pipeline
                .is_outbound_blocked(simulation.current_tick, node.id.get_peer_id())
            {
                return None; // Skip blocked nodes
            }
            let consensus = node.state.consensus()?;
            Some((
                node.state.leader_election(),
                node.state.validators_epoch_mapping(),
                node.state.epoch_manager(),
                consensus,
            ))
        });
        let mut max_round = GENESIS_ROUND;
        let mut leader = None;
        for (election, validators, epoch_manager, state) in live_states {
            if state.get_current_round() <= max_round {
                continue;
            }
            max_round = state.get_current_round();

            let leader_round = state.get_current_round();
            let Some(leader_epoch) = epoch_manager.get_epoch(leader_round) else {
                continue;
            };
            let Some(val_set) = validators.get_val_set(&leader_epoch) else {
                continue;
            };
            leader = Some(election.get_leader(leader_round, val_set.get_members()));
        }
        leader.as_ref().map(Into::into)
    }

    async fn next_leader<'ctx>(&self, ctx: &Context<'ctx>) -> Option<GraphQLNodeId> {
        let simulation = ctx.data_unchecked::<GraphQLSimulation>();
        let live_states = simulation.swarm.states().values().filter_map(|node| {
            if node
                .outbound_pipeline
                .is_outbound_blocked(simulation.current_tick, node.id.get_peer_id())
            {
                return None; // Skip blocked nodes
            }
            let consensus = node.state.consensus()?;
            Some((
                node.state.leader_election(),
                node.state.validators_epoch_mapping(),
                node.state.epoch_manager(),
                consensus,
            ))
        });
        let mut max_round = GENESIS_ROUND;
        let mut leader = None;
        for (election, validators, epoch_manager, state) in live_states {
            if state.get_current_round() <= max_round {
                continue;
            }
            max_round = state.get_current_round();

            let leader_round = state.get_current_round() + Round(1);
            let Some(leader_epoch) = epoch_manager.get_epoch(leader_round) else {
                continue;
            };
            let Some(val_set) = validators.get_val_set(&leader_epoch) else {
                continue;
            };
            leader = Some(election.get_leader(leader_round, val_set.get_members()));
        }
        leader.as_ref().map(Into::into)
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

    async fn network_config<'ctx>(&self, ctx: &Context<'ctx>) -> GraphQLNetworkConfig<'ctx> {
        let simulation = ctx.data_unchecked::<GraphQLSimulation>();
        GraphQLNetworkConfig(&simulation.network_config)
    }

    async fn network_command_log<'ctx>(
        &self,
        ctx: &Context<'ctx>,
    ) -> Vec<GraphQLNetworkCommand<'ctx>> {
        let simulation = ctx.data_unchecked::<GraphQLSimulation>();
        simulation
            .network_command_log
            .iter()
            .map(GraphQLNetworkCommand)
            .collect()
    }
}

struct GraphQLNetworkConfig<'s>(&'s NetworkConfig);

#[Object]
impl GraphQLNetworkConfig<'_> {
    async fn default_latency(&self) -> GraphQLTimestamp {
        GraphQLTimestamp::new(self.0.default_latency())
    }

    async fn links(&self) -> Vec<GraphQLNetworkLink> {
        self.0
            .effective_link_rules()
            .into_iter()
            .map(GraphQLNetworkLink)
            .collect()
    }

    async fn nodes(&self) -> Vec<GraphQLNetworkNode> {
        self.0
            .nodes()
            .into_iter()
            .map(GraphQLNetworkNode)
            .collect()
    }
}

struct GraphQLNetworkLink(EffectiveLinkRule);

#[Object]
impl GraphQLNetworkLink {
    async fn from_id(&self) -> GraphQLNodeId {
        (&self.0.from).into()
    }

    async fn to_id(&self) -> GraphQLNodeId {
        (&self.0.to).into()
    }

    async fn latency(&self) -> GraphQLTimestamp {
        GraphQLTimestamp::new(self.0.latency)
    }

    async fn dropped(&self) -> bool {
        self.0.dropped
    }

    async fn offline(&self) -> bool {
        self.0.offline
    }

    async fn overridden(&self) -> bool {
        self.0.overridden
    }
}

struct GraphQLNetworkNode(NetworkNodeState);

#[Object]
impl GraphQLNetworkNode {
    async fn id(&self) -> GraphQLNodeId {
        (&self.0.id).into()
    }

    async fn x(&self) -> i32 {
        self.0.x
    }

    async fn y(&self) -> i32 {
        self.0.y
    }

    async fn online(&self) -> bool {
        self.0.online
    }
}

struct GraphQLNetworkCommand<'s>(&'s NetworkCommand);

#[Object]
impl GraphQLNetworkCommand<'_> {
    async fn tick(&self) -> GraphQLTimestamp {
        GraphQLTimestamp::new(self.0.tick)
    }

    async fn sequence(&self) -> i64 {
        self.0.sequence.try_into().unwrap()
    }

    async fn kind(&self) -> &'static str {
        match &self.0.action {
            NetworkCommandAction::SetDefaultLatency { .. } => "SET_DEFAULT_LATENCY",
            NetworkCommandAction::SetLinkLatency { .. } => "SET_LINK_LATENCY",
            NetworkCommandAction::SetLinkDropped { .. } => "SET_LINK_DROPPED",
            NetworkCommandAction::ClearLinkRule { .. } => "CLEAR_LINK_RULE",
            NetworkCommandAction::SetNodePosition { .. } => "SET_NODE_POSITION",
            NetworkCommandAction::SetNodeOnline { .. } => "SET_NODE_ONLINE",
        }
    }

    async fn from_id(&self) -> Option<GraphQLNodeId> {
        match &self.0.action {
            NetworkCommandAction::SetLinkLatency { from, .. }
            | NetworkCommandAction::SetLinkDropped { from, .. }
            | NetworkCommandAction::ClearLinkRule { from, .. } => Some(from.into()),
            NetworkCommandAction::SetDefaultLatency { .. }
            | NetworkCommandAction::SetNodePosition { .. }
            | NetworkCommandAction::SetNodeOnline { .. } => None,
        }
    }

    async fn to_id(&self) -> Option<GraphQLNodeId> {
        match &self.0.action {
            NetworkCommandAction::SetLinkLatency { to, .. }
            | NetworkCommandAction::SetLinkDropped { to, .. }
            | NetworkCommandAction::ClearLinkRule { to, .. } => Some(to.into()),
            NetworkCommandAction::SetDefaultLatency { .. }
            | NetworkCommandAction::SetNodePosition { .. }
            | NetworkCommandAction::SetNodeOnline { .. } => None,
        }
    }

    async fn node_id(&self) -> Option<GraphQLNodeId> {
        match &self.0.action {
            NetworkCommandAction::SetNodePosition { node, .. }
            | NetworkCommandAction::SetNodeOnline { node, .. } => Some(node.into()),
            NetworkCommandAction::SetDefaultLatency { .. }
            | NetworkCommandAction::SetLinkLatency { .. }
            | NetworkCommandAction::SetLinkDropped { .. }
            | NetworkCommandAction::ClearLinkRule { .. } => None,
        }
    }

    async fn latency(&self) -> Option<GraphQLTimestamp> {
        match &self.0.action {
            NetworkCommandAction::SetDefaultLatency { latency }
            | NetworkCommandAction::SetLinkLatency { latency, .. } => {
                Some(GraphQLTimestamp::new(*latency))
            }
            NetworkCommandAction::SetLinkDropped { .. }
            | NetworkCommandAction::ClearLinkRule { .. }
            | NetworkCommandAction::SetNodePosition { .. }
            | NetworkCommandAction::SetNodeOnline { .. } => None,
        }
    }

    async fn dropped(&self) -> Option<bool> {
        match &self.0.action {
            NetworkCommandAction::SetLinkDropped { dropped, .. } => Some(*dropped),
            NetworkCommandAction::SetDefaultLatency { .. }
            | NetworkCommandAction::SetLinkLatency { .. }
            | NetworkCommandAction::ClearLinkRule { .. }
            | NetworkCommandAction::SetNodePosition { .. }
            | NetworkCommandAction::SetNodeOnline { .. } => None,
        }
    }

    async fn x(&self) -> Option<i32> {
        match &self.0.action {
            NetworkCommandAction::SetNodePosition { x, .. } => Some(*x),
            NetworkCommandAction::SetDefaultLatency { .. }
            | NetworkCommandAction::SetLinkLatency { .. }
            | NetworkCommandAction::SetLinkDropped { .. }
            | NetworkCommandAction::ClearLinkRule { .. }
            | NetworkCommandAction::SetNodeOnline { .. } => None,
        }
    }

    async fn y(&self) -> Option<i32> {
        match &self.0.action {
            NetworkCommandAction::SetNodePosition { y, .. } => Some(*y),
            NetworkCommandAction::SetDefaultLatency { .. }
            | NetworkCommandAction::SetLinkLatency { .. }
            | NetworkCommandAction::SetLinkDropped { .. }
            | NetworkCommandAction::ClearLinkRule { .. }
            | NetworkCommandAction::SetNodeOnline { .. } => None,
        }
    }

    async fn online(&self) -> Option<bool> {
        match &self.0.action {
            NetworkCommandAction::SetNodeOnline { online, .. } => Some(*online),
            NetworkCommandAction::SetDefaultLatency { .. }
            | NetworkCommandAction::SetLinkLatency { .. }
            | NetworkCommandAction::SetLinkDropped { .. }
            | NetworkCommandAction::ClearLinkRule { .. }
            | NetworkCommandAction::SetNodePosition { .. } => None,
        }
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
    async fn root(&self) -> GraphQLSeqNum {
        let Some(state) = self.0.state.consensus() else {
            return GraphQLSeqNum::new(GENESIS_SEQ_NUM);
        };
        let root = state.blocktree().root().seq_num;
        GraphQLSeqNum::new(root)
    }

    async fn ledger_blocks(&self, limit: Option<i32>) -> Vec<GraphQLLedgerBlock> {
        let Some(state) = self.0.state.consensus() else {
            return Vec::new();
        };
        let blocktree = state.blocktree();
        let root = blocktree.root();
        let mut blocks_by_id = BTreeMap::new();

        for block in self.0.executor.ledger().get_finalized_blocks().values() {
            blocks_by_id.insert(
                block.get_id(),
                GraphQLLedgerBlock {
                    id: block.get_id(),
                    parent_id: Some(block.get_parent_id()),
                    body_id: Some(block.get_body_id()),
                    seq_num: block.get_seq_num(),
                    round: block.get_block_round(),
                    author: Some(*block.get_author()),
                    coherent: true,
                    finalized: true,
                    children_ids: Vec::new(),
                },
            );
        }

        for (block_id, entry) in blocktree.tree().iter() {
            let block = &entry.validated_block;
            if let Some(existing_block) = blocks_by_id.get_mut(block_id) {
                existing_block.coherent |= entry.is_coherent;
                continue;
            }

            blocks_by_id.insert(
                *block_id,
                GraphQLLedgerBlock {
                    id: *block_id,
                    parent_id: Some(block.get_parent_id()),
                    body_id: Some(block.get_body_id()),
                    seq_num: block.get_seq_num(),
                    round: block.get_block_round(),
                    author: Some(*block.get_author()),
                    coherent: entry.is_coherent,
                    finalized: false,
                    children_ids: Vec::new(),
                },
            );
        }

        blocks_by_id
            .entry(root.block_id)
            .and_modify(|block| {
                block.coherent = true;
                block.finalized = true;
            })
            .or_insert(GraphQLLedgerBlock {
                id: root.block_id,
                parent_id: None,
                body_id: None,
                seq_num: root.seq_num,
                round: root.round,
                author: None,
                coherent: true,
                finalized: true,
                children_ids: Vec::new(),
            });

        let mut children_by_parent: BTreeMap<BlockId, Vec<BlockId>> = BTreeMap::new();
        for block in blocks_by_id.values() {
            if let Some(parent_id) = block.parent_id {
                if blocks_by_id.contains_key(&parent_id) {
                    children_by_parent
                        .entry(parent_id)
                        .or_default()
                        .push(block.id);
                }
            }
        }

        for children in children_by_parent.values_mut() {
            children.sort_by_key(|child_id| {
                blocks_by_id
                    .get(child_id)
                    .map(|block| (block.seq_num, block.round, block.id))
            });
        }

        for (parent_id, children) in children_by_parent {
            if let Some(parent) = blocks_by_id.get_mut(&parent_id) {
                parent.children_ids = children;
            }
        }

        let mut blocks: Vec<_> = blocks_by_id.into_values().collect();
        blocks.sort_by_key(|block| (block.seq_num, block.round, block.id));
        if let Some(limit) = limit.and_then(|limit| usize::try_from(limit).ok()) {
            if limit == 0 {
                return Vec::new();
            }
            let root_block = blocks
                .iter()
                .position(|block| block.id == root.block_id)
                .map(|index| blocks.remove(index));
            let non_root_limit = limit.saturating_sub(usize::from(root_block.is_some()));
            if blocks.len() > non_root_limit {
                blocks = blocks.split_off(blocks.len() - non_root_limit);
            }
            if let Some(root_block) = root_block {
                blocks.push(root_block);
            }
        }
        blocks.sort_by_key(|block| (block.seq_num, block.round, block.id));
        blocks
    }

    async fn is_blocked<'ctx>(&self, ctx: &Context<'ctx>) -> bool {
        let simulation = ctx.data_unchecked::<GraphQLSimulation>();
        self.0
            .outbound_pipeline
            .is_outbound_blocked(simulation.current_tick, self.0.id.get_peer_id())
    }

    async fn current_round(&self) -> GraphQLRound {
        let Some(state) = self.0.state.consensus() else {
            return GraphQLRound::new(GENESIS_ROUND);
        };
        let round = state.get_current_round();
        GraphQLRound::new(round)
    }

    async fn round_timer_started_at(&self) -> Option<GraphQLTimestamp> {
        let round_timeout = self.0.executor.next_round_timeout()?;
        Some(GraphQLTimestamp::new(round_timeout.was_scheduled_at))
    }
    async fn round_timer_ends_at(&self) -> Option<GraphQLTimestamp> {
        let round_timeout = self.0.executor.next_round_timeout()?;
        Some(GraphQLTimestamp::new(round_timeout.times_out_at))
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
                    message_nonce: &message.nonce,
                })
            })
            .collect()
    }
}

struct GraphQLMetrics<'s>(&'s Metrics);
#[Object]
impl GraphQLMetrics<'_> {
    async fn consensus_created_qc(&self) -> u32 {
        self.0.consensus_events.created_qc.get().try_into().unwrap()
    }
    async fn consensus_local_timeout(&self) -> u32 {
        self.0
            .consensus_events
            .local_timeout
            .get()
            .try_into()
            .unwrap()
    }
    async fn consensus_handle_proposal(&self) -> u32 {
        self.0
            .consensus_events
            .handle_proposal
            .get()
            .try_into()
            .unwrap()
    }
    async fn consensus_failed_txn_validation(&self) -> u32 {
        self.0
            .consensus_events
            .failed_txn_validation
            .get()
            .try_into()
            .unwrap()
    }
    async fn consensus_invalid_proposal_round_leader(&self) -> u32 {
        self.0
            .consensus_events
            .invalid_proposal_round_leader
            .get()
            .try_into()
            .unwrap()
    }
    async fn consensus_out_of_order_proposals(&self) -> u32 {
        self.0
            .consensus_events
            .out_of_order_proposals
            .get()
            .try_into()
            .unwrap()
    }
}

struct GraphQLPendingMessage<'s> {
    from: &'s ID<CertificateSignaturePubKey<SignatureType>>,
    from_tick: &'s Duration,
    rx_tick: &'s Duration,
    message: &'s TransportMessage,
    message_nonce: &'s usize,
}

#[Object]
impl<'s> GraphQLPendingMessage<'s> {
    async fn id(&self) -> i64 {
        let mut s = DefaultHasher::new();
        (self.from, self.from_tick, self.rx_tick, self.message_nonce).hash(&mut s);
        s.finish() as i64
    }
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
        let serialized: Bytes = self.message.serialize();
        serialized.len() as i64
    }
    async fn message(&self) -> GraphQLMonadMessage<'s> {
        self.message.into()
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
    ControlPanelEvent(GraphQLControlPanelEvent<'s>),
    TimestampEvent(GraphQLTimestampEvent),
    StateSyncEvent(GraphQLStateSyncEvent<'s>),
    ConfigEvent(GraphQLConfigEvent<'s>),
}

impl<'s> From<&'s MonadEventType> for GraphQLMonadEvent<'s> {
    fn from(event: &'s MonadEventType) -> Self {
        match event {
            MonadEvent::ConsensusEvent(event) => Self::ConsensusEvent(GraphQLConsensusEvent(event)),
            MonadEvent::BlockSyncEvent(event) => Self::BlockSyncEvent(GraphQLBlockSyncEvent(event)),
            MonadEvent::ValidatorEvent(event) => Self::ValidatorEvent(GraphQLValidatorEvent(event)),
            MonadEvent::MempoolEvent(event) => Self::MempoolEvent(GraphQLMempoolEvent(event)),
            MonadEvent::ControlPanelEvent(event) => {
                Self::ControlPanelEvent(GraphQLControlPanelEvent(event))
            }
            MonadEvent::TimestampUpdateEvent(event) => {
                Self::TimestampEvent(GraphQLTimestampEvent(*event as u64)) // TODO: this is wrong but protobuf is not used in
                                                                           // protocol and will be deleted
            }
            MonadEvent::StateSyncEvent(event) => Self::StateSyncEvent(GraphQLStateSyncEvent(event)),
            MonadEvent::ConfigEvent(event) => Self::ConfigEvent(GraphQLConfigEvent(event)),
            MonadEvent::SecondaryRaptorcastPeersUpdate { .. } => todo!(),
        }
    }
}

struct GraphQLConsensusEvent<'s>(
    &'s ConsensusEvent<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
);
#[Object]
impl GraphQLConsensusEvent<'_> {
    async fn debug(&self) -> String {
        format!("{:?}", self.0)
    }
}

struct GraphQLBlockSyncEvent<'s>(
    &'s BlockSyncEvent<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
);
#[Object]
impl GraphQLBlockSyncEvent<'_> {
    async fn debug(&self) -> String {
        format!("{:?}", self.0)
    }
}

struct GraphQLValidatorEvent<'s>(&'s ValidatorEvent<SignatureCollectionType>);
#[Object]
impl GraphQLValidatorEvent<'_> {
    async fn debug(&self) -> String {
        format!("{:?}", self.0)
    }
}

struct GraphQLMempoolEvent<'s>(
    &'s MempoolEvent<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
);
#[Object]
impl GraphQLMempoolEvent<'_> {
    async fn debug(&self) -> String {
        format!("{:?}", self.0)
    }
}

struct GraphQLControlPanelEvent<'s>(&'s ControlPanelEvent<SignatureType>);

#[Object]
impl GraphQLControlPanelEvent<'_> {
    async fn debug(&self) -> String {
        format!("{:?}", self.0)
    }
}

struct GraphQLTimestampEvent(u64);

#[Object]
impl GraphQLTimestampEvent {
    async fn debug(&self) -> String {
        format!("{:?}", self.0)
    }
}

struct GraphQLStateSyncEvent<'s>(
    &'s StateSyncEvent<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
);
#[Object]
impl GraphQLStateSyncEvent<'_> {
    async fn debug(&self) -> String {
        format!("{:?}", self.0)
    }
}

struct GraphQLConfigEvent<'s>(&'s ConfigEvent<SignatureType, SignatureCollectionType>);

#[Object]
impl GraphQLConfigEvent<'_> {
    async fn debug(&self) -> String {
        format!("{:?}", self.0)
    }
}
