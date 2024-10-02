use auto_impl::auto_impl;
use monad_crypto::{
    certificate_signature::PubKey,
    hasher::{Hashable, Hasher, HasherType},
};
use monad_state_backend::{InMemoryState, StateBackend, StateBackendError};
use monad_types::{BlockId, EnumDiscriminant, Epoch, NodeId, Round, SeqNum};
use zerocopy::AsBytes;

use crate::{
    payload::{ExecutionProtocol, Payload, PayloadId, TransactionPayload},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    state_root_hash::StateRootHash,
};

/// This trait represents a consensus block
pub trait BlockType<SCT: SignatureCollection>: Clone + PartialEq + Eq {
    type NodeIdPubKey: PubKey;
    type TxnHash: PartialEq + Eq + std::hash::Hash;
    /// Unique hash for the block
    fn get_id(&self) -> BlockId;

    /// Round in which this block was proposed
    fn get_round(&self) -> Round;

    /// Epoch in which this block was proposed
    fn get_epoch(&self) -> Epoch;

    /// Node which proposed this block
    fn get_author(&self) -> NodeId<Self::NodeIdPubKey>;

    /// returns the BlockId for the block referenced by
    /// the QC contained in this block
    fn get_parent_id(&self) -> BlockId;

    /// returns the Round for the block referenced by
    /// the QC contained in this block
    fn get_parent_round(&self) -> Round;

    /// Sequence number when this block was proposed
    fn get_seq_num(&self) -> SeqNum;

    /// State root hash included in the block
    fn get_state_root(&self) -> StateRootHash;

    /// get list of all txn hashes in this block
    fn get_txn_hashes(&self) -> Vec<Self::TxnHash>;

    fn is_empty_block(&self) -> bool;

    fn get_txn_list_len(&self) -> usize;

    /// get a reference to the block's QC
    fn get_qc(&self) -> &QuorumCertificate<SCT>;

    fn get_timestamp(&self) -> u64;

    fn get_unvalidated_block(self) -> Block<SCT>;

    fn get_unvalidated_block_ref(&self) -> &Block<SCT>;

    fn get_full_block(self) -> FullBlock<SCT>;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum BlockKind {
    Executable,
    Null,
}

impl Hashable for BlockKind {
    fn hash(&self, state: &mut impl Hasher) {
        match self {
            BlockKind::Executable => {
                EnumDiscriminant(1).hash(state);
            }
            BlockKind::Null => {
                EnumDiscriminant(2).hash(state);
            }
        }
    }
}

/// structure of the consensus block
/// the payload field is used to carry the data of the block
/// which is agnostic to the actual protocol of consensus
#[derive(Clone)]
pub struct Block<SCT: SignatureCollection> {
    /// proposer of this block
    pub author: NodeId<SCT::NodeIdPubKey>,

    /// Epoch this block was proposed in
    pub epoch: Epoch,

    /// round this block was proposed in
    pub round: Round,

    /// data related to the execution side of the protocol
    pub execution: ExecutionProtocol,

    /// identifier for the transaction payload of this block
    pub payload_id: PayloadId,

    pub block_kind: BlockKind,

    /// Certificate of votes for the parent block
    pub qc: QuorumCertificate<SCT>,

    pub timestamp: u64,

    /// Unique hash used to identify the block
    id: BlockId,
}

impl<SCT: SignatureCollection> PartialEq for Block<SCT> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl<SCT: SignatureCollection> Eq for Block<SCT> {}

impl<SCT: SignatureCollection> std::fmt::Debug for Block<SCT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("author", &self.author)
            .field("epoch", &self.epoch)
            .field("round", &self.round)
            .field("timestamp", &self.timestamp)
            .field("qc", &self.qc)
            .field("id", &self.id)
            .field("payload_id", &self.payload_id)
            .field("block_kind", &self.block_kind)
            .field("seq_num", &self.execution.seq_num)
            .field("execution_state_root", &self.execution.state_root)
            .finish_non_exhaustive()
    }
}

impl<SCT: SignatureCollection> Hashable for Block<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        self.id.hash(state);
    }
}

impl<SCT: SignatureCollection> Block<SCT> {
    pub fn calculate_block_id(
        author: NodeId<SCT::NodeIdPubKey>,
        timestamp: u64,
        epoch: Epoch,
        round: Round,
        execution: &ExecutionProtocol,
        payload_id: PayloadId,
        block_kind: BlockKind,
        qc: &QuorumCertificate<SCT>,
    ) -> BlockId {
        let mut _block_hash_span = tracing::trace_span!("block_hash_span").entered();
        let mut state = HasherType::new();
        author.hash(&mut state);
        state.update(timestamp.as_bytes());
        state.update(epoch.as_bytes());
        state.update(round.as_bytes());
        execution.hash(&mut state);
        state.update(payload_id.0.as_bytes());
        block_kind.hash(&mut state);
        state.update(qc.get_block_id().0.as_bytes());
        state.update(qc.get_hash().as_bytes());

        BlockId(state.hash())
    }

    // FIXME &QuorumCertificate -> QuorumCertificate
    pub fn new(
        author: NodeId<SCT::NodeIdPubKey>,
        timestamp: u64,
        epoch: Epoch,
        round: Round,
        execution: &ExecutionProtocol,
        payload_id: PayloadId,
        block_kind: BlockKind,
        qc: &QuorumCertificate<SCT>,
    ) -> Self {
        Self {
            author,
            timestamp,
            epoch,
            round,
            execution: execution.clone(),
            payload_id,
            block_kind,
            qc: qc.clone(),
            id: Self::calculate_block_id(
                author, timestamp, epoch, round, execution, payload_id, block_kind, qc,
            ),
        }
    }

    /// Check if the block is a consensus protocol empty block. Note there's a
    /// distinction between a block with no transactions
    /// `TransactionPayload::List(FullTransactionList::empty())` and a consensus
    /// protocol empty block `TransactionPayload::Empty`
    pub fn is_empty_block(&self) -> bool {
        matches!(self.block_kind, BlockKind::Null)
    }
}

// TODO, this should be removed, we don't need to implement BlockType for Block because we won't
// use this as a validatedBlock type going forward in the passthru policy, FullBlock can be used
// there
impl<SCT: SignatureCollection> BlockType<SCT> for Block<SCT> {
    type NodeIdPubKey = SCT::NodeIdPubKey;
    type TxnHash = ();

    fn get_id(&self) -> BlockId {
        self.id
    }

    fn get_round(&self) -> Round {
        self.round
    }

    fn get_epoch(&self) -> Epoch {
        self.epoch
    }

    fn get_author(&self) -> NodeId<Self::NodeIdPubKey> {
        self.author
    }

    fn get_parent_id(&self) -> BlockId {
        self.qc.get_block_id()
    }

    fn get_parent_round(&self) -> Round {
        self.qc.get_round()
    }

    fn get_seq_num(&self) -> SeqNum {
        self.execution.seq_num
    }

    fn get_state_root(&self) -> StateRootHash {
        self.execution.state_root
    }

    fn get_txn_hashes(&self) -> Vec<Self::TxnHash> {
        vec![]
    }

    fn is_empty_block(&self) -> bool {
        self.is_empty_block()
    }

    fn get_txn_list_len(&self) -> usize {
        /*
        match &self.payload.txns {
            TransactionPayload::List(list) => list.bytes().len(),
            TransactionPayload::Null => 0,
        }
        */
        0
    }

    fn get_qc(&self) -> &QuorumCertificate<SCT> {
        &self.qc
    }

    fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    fn get_unvalidated_block(self) -> Block<SCT> {
        self
    }

    fn get_unvalidated_block_ref(&self) -> &Block<SCT> {
        self
    }

    fn get_full_block(self) -> FullBlock<SCT> {
        todo!();
    }
}

#[derive(Debug, PartialEq)]
pub enum BlockPolicyError {
    BlockNotCoherent,
    StateBackendError(StateBackendError),
}

impl From<StateBackendError> for BlockPolicyError {
    fn from(err: StateBackendError) -> Self {
        Self::StateBackendError(err)
    }
}

/// Trait that represents how inner contents of a block should be validated
#[auto_impl(Box)]
pub trait BlockPolicy<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    type ValidatedBlock: Sized
        + Clone
        + PartialEq
        + Eq
        + std::fmt::Debug
        + BlockType<SCT, NodeIdPubKey = SCT::NodeIdPubKey>
        + Hashable
        + Send;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        state_backend: &SBT,
    ) -> Result<(), BlockPolicyError>;

    // TODO delete this function, pass recently committed blocks to check_coherency instead
    // This way, BlockPolicy doesn't need to be mutated
    fn update_committed_block(&mut self, block: &Self::ValidatedBlock);

    // TODO delete this function, pass recently committed blocks to check_coherency instead
    // This way, BlockPolicy doesn't need to be mutated
    fn reset(&mut self, last_delay_committed_blocks: Vec<&Self::ValidatedBlock>);
}

/// A block policy which does not validate the inner contents of the block
#[derive(Copy, Clone, Default)]
pub struct PassthruBlockPolicy;

impl<SCT> BlockPolicy<SCT, InMemoryState> for PassthruBlockPolicy
where
    SCT: SignatureCollection,
{
    type ValidatedBlock = FullBlock<SCT>;

    fn check_coherency(
        &self,
        _: &Self::ValidatedBlock,
        _: Vec<&Self::ValidatedBlock>,
        _: &InMemoryState,
    ) -> Result<(), BlockPolicyError> {
        Ok(())
    }

    fn update_committed_block(&mut self, _: &Self::ValidatedBlock) {}
    fn reset(&mut self, _: Vec<&Self::ValidatedBlock>) {}
}

#[derive(Debug, Clone)]
pub struct FullBlock<SCT: SignatureCollection> {
    pub block: Block<SCT>,
    pub payload: Payload,
}

impl<SCT: SignatureCollection> PartialEq for FullBlock<SCT> {
    fn eq(&self, other: &Self) -> bool {
        self.block.id == other.block.id
    }
}
impl<SCT: SignatureCollection> Eq for FullBlock<SCT> {}

impl<SCT: SignatureCollection> BlockType<SCT> for FullBlock<SCT> {
    type NodeIdPubKey = SCT::NodeIdPubKey;
    type TxnHash = ();

    fn get_id(&self) -> BlockId {
        self.block.id
    }

    fn get_round(&self) -> Round {
        self.block.round
    }

    fn get_epoch(&self) -> Epoch {
        self.block.epoch
    }

    fn get_author(&self) -> NodeId<Self::NodeIdPubKey> {
        self.block.author
    }

    fn get_parent_id(&self) -> BlockId {
        self.block.qc.get_block_id()
    }

    fn get_parent_round(&self) -> Round {
        self.block.qc.get_round()
    }

    fn get_seq_num(&self) -> SeqNum {
        self.block.execution.seq_num
    }

    fn get_state_root(&self) -> StateRootHash {
        self.block.execution.state_root
    }

    fn get_txn_hashes(&self) -> Vec<Self::TxnHash> {
        vec![]
    }

    fn is_empty_block(&self) -> bool {
        self.block.is_empty_block()
    }

    fn get_txn_list_len(&self) -> usize {
        match &self.payload.txns {
            TransactionPayload::List(list) => list.bytes().len(),
            TransactionPayload::Null => 0,
        }
    }

    fn get_qc(&self) -> &QuorumCertificate<SCT> {
        &self.block.qc
    }

    fn get_timestamp(&self) -> u64 {
        self.block.timestamp
    }

    fn get_unvalidated_block(self) -> Block<SCT> {
        self.block
    }

    fn get_unvalidated_block_ref(&self) -> &Block<SCT> {
        &self.block
    }

    fn get_full_block(self) -> FullBlock<SCT> {
        self
    }
}

impl<SCT: SignatureCollection> Hashable for FullBlock<SCT> {
    fn hash(&self, state: &mut impl Hasher) {
        self.block.id.hash(state);
        self.payload.hash(state);
    }
}
