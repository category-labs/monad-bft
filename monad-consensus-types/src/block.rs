use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use auto_impl::auto_impl;
use bytes::Bytes;
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
    },
    hasher::{Hashable, Hasher, HasherType},
};
use monad_state_backend::{InMemoryState, StateBackend, StateBackendError};
use monad_types::{BlockId, DontCare, Epoch, NodeId, Round, SeqNum};
use reth_primitives::Header;
use std::{fmt::Debug, ops::Deref};

use crate::{
    block_validator::BlockValidationError,
    checkpoint::RootInfo,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId, RoundSignature},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};

/// Represent a range of blocks the last of which is `last_block_id` and includes
/// all blocks upto to `root_seq_num`
/// For a valid block range, the seq num of block `last_block_id` >= `root_seq_num`
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct BlockRange {
    pub last_block_id: BlockId,
    pub max_blocks: SeqNum,
}

impl Hashable for BlockRange {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

/// structure of the consensus block
/// the payload field is used to carry the data of the block
/// which is agnostic to the actual protocol of consensus
#[derive(Clone, RlpDecodable, RlpEncodable)]
pub struct ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    /// round this block was proposed in
    pub round: Round,
    /// Epoch this block was proposed in
    pub epoch: Epoch,
    /// Certificate of votes for the parent block
    pub qc: QuorumCertificate<SCT>,
    /// proposer of this block
    pub author: NodeId<CertificateSignaturePubKey<ST>>,

    pub seq_num: SeqNum,
    pub timestamp_ns: u128,
    // This is SCT::SignatureType because SCT signatures are guaranteed to be deterministic
    pub round_signature: RoundSignature<SCT::SignatureType>,

    /// data related to the execution side of the protocol
    pub delayed_execution_results: Vec<EPT::FinalizedHeader>,
    pub execution_inputs: EPT::ProposedHeader,
    /// identifier for the transaction payload of this block
    pub block_body_id: ConsensusBlockBodyId,
}

impl<ST, SCT, EPT> ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn get_id(&self) -> BlockId {
        let mut hasher = HasherType::new();
        hasher.update(alloy_rlp::encode(self));
        BlockId(hasher.hash())
    }

    pub fn get_parent_id(&self) -> BlockId {
        self.qc.get_block_id()
    }
}

impl<ST, SCT, EPT> PartialEq for ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn eq(&self, other: &Self) -> bool {
        self.get_id() == other.get_id()
    }
}
impl<ST, SCT, EPT> Eq for ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
}

impl<ST, SCT, EPT> Debug for ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("author", &self.author)
            .field("epoch", &self.epoch)
            .field("round", &self.round)
            .field("block_body_id", &self.block_body_id)
            .field("qc", &self.qc)
            .field("seq_num", &self.seq_num)
            .field("timestamp_ns", &self.timestamp_ns)
            .field("id", &self.get_id())
            .finish_non_exhaustive()
    }
}

impl<ST, SCT, EPT> ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    // FIXME &QuorumCertificate -> QuorumCertificate
    pub fn new(
        author: NodeId<SCT::NodeIdPubKey>,
        epoch: Epoch,
        round: Round,
        delayed_execution_results: Vec<EPT::FinalizedHeader>,
        execution_inputs: EPT::ProposedHeader,
        block_body_id: ConsensusBlockBodyId,
        qc: QuorumCertificate<SCT>,
        seq_num: SeqNum,
        timestamp_ns: u128,
        round_signature: RoundSignature<SCT::SignatureType>,
    ) -> Self {
        Self {
            author,
            epoch,
            round,
            delayed_execution_results,
            execution_inputs,
            block_body_id,
            qc,
            seq_num,
            timestamp_ns,
            round_signature,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum BlockPolicyError {
    BlockNotCoherent,
    StateBackendError(StateBackendError),
    TimestampError,
}

impl From<StateBackendError> for BlockPolicyError {
    fn from(err: StateBackendError) -> Self {
        Self::StateBackendError(err)
    }
}

/// Trait that represents how inner contents of a block should be validated
#[auto_impl(Box)]
pub trait BlockPolicy<ST, SCT, EPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    SBT: StateBackend,
{
    type ValidatedBlock: Sized
        + Clone
        + PartialEq
        + Eq
        + Debug
        + Send
        + Deref<Target = ConsensusFullBlock<ST, SCT, EPT>>;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        blocktree_root: RootInfo,
        state_backend: &SBT,
    ) -> Result<(), BlockPolicyError>;

    // TODO delete this function, pass recently committed blocks to check_coherency instead
    // This way, BlockPolicy doesn't need to be mutated
    fn update_committed_block(&mut self, block: &Self::ValidatedBlock);

    // TODO delete this function, pass recently committed blocks to check_coherency instead
    // This way, BlockPolicy doesn't need to be mutated
    fn reset(&mut self, last_delay_non_null_committed_blocks: Vec<&Self::ValidatedBlock>);
}

/// A block policy which does not validate the inner contents of the block
#[derive(Copy, Clone, Default)]
pub struct PassthruBlockPolicy;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassthruWrappedBlock<ST, SCT, EPT>(pub ConsensusFullBlock<ST, SCT, EPT>)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol;

impl<ST, SCT, EPT> Deref for PassthruWrappedBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Target = ConsensusFullBlock<ST, SCT, EPT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<ST, SCT, EPT> BlockPolicy<ST, SCT, EPT, InMemoryState> for PassthruBlockPolicy
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type ValidatedBlock = PassthruWrappedBlock<ST, SCT, EPT>;

    fn check_coherency(
        &self,
        block: &Self::ValidatedBlock,
        extending_blocks: Vec<&Self::ValidatedBlock>,
        blocktree_root: RootInfo,
        _: &InMemoryState,
    ) -> Result<(), BlockPolicyError> {
        // check coherency against the block being extended or against the root of the blocktree if
        // there is no extending branch
        let (extending_seq_num, extending_timestamp) =
            if let Some(extended_block) = extending_blocks.last() {
                (extended_block.get_seq_num(), extended_block.get_timestamp())
            } else {
                (blocktree_root.seq_num, 0) //TODO: add timestamp to RootInfo
            };

        if block.get_seq_num() != extending_seq_num + SeqNum(1) {
            return Err(BlockPolicyError::BlockNotCoherent);
        }

        if block.get_timestamp() <= extending_timestamp {
            // timestamps must be monotonically increasing
            return Err(BlockPolicyError::TimestampError);
        }
        Ok(())
    }

    fn update_committed_block(&mut self, _: &Self::ValidatedBlock) {}
    fn reset(&mut self, _: Vec<&Self::ValidatedBlock>) {}
}

#[derive(Debug, Clone)]
pub struct ConsensusFullBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    header: ConsensusBlockHeader<ST, SCT, EPT>,
    body: ConsensusBlockBody<EPT>,
}

impl<ST, SCT, EPT> PartialEq for ConsensusFullBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn eq(&self, other: &Self) -> bool {
        self.header.get_id() == other.header.get_id()
    }
}
impl<ST, SCT, EPT> Eq for ConsensusFullBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
}

impl<ST, SCT, EPT> ConsensusFullBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        header: ConsensusBlockHeader<ST, SCT, EPT>,
        body: ConsensusBlockBody<EPT>,
    ) -> Result<Self, BlockValidationError> {
        if body.get_id() != header.block_body_id {
            return Err(BlockValidationError::HeaderPayloadMismatchError);
        }
        Ok(Self { header, body })
    }

    pub fn header(&self) -> &ConsensusBlockHeader<ST, SCT, EPT> {
        &self.header
    }
    pub fn body(&self) -> &ConsensusBlockBody<EPT> {
        &self.body
    }

    pub fn get_parent_id(&self) -> BlockId {
        self.header.qc.get_block_id()
    }
    pub fn get_id(&self) -> BlockId {
        self.header.get_id()
    }
    pub fn get_body_id(&self) -> ConsensusBlockBodyId {
        self.header.block_body_id
    }
    pub fn get_round(&self) -> Round {
        self.header.round
    }
    pub fn get_parent_round(&self) -> Round {
        self.header.qc.get_round()
    }
    pub fn get_qc(&self) -> &QuorumCertificate<SCT> {
        &self.header.qc
    }
    pub fn get_epoch(&self) -> Epoch {
        self.header.epoch
    }
    pub fn get_seq_num(&self) -> SeqNum {
        self.header.seq_num
    }
    pub fn get_timestamp(&self) -> u128 {
        self.header.timestamp_ns
    }
    pub fn get_author(&self) -> &NodeId<CertificateSignaturePubKey<ST>> {
        &self.header.author
    }

    pub fn get_execution_results(&self) -> &Vec<EPT::FinalizedHeader> {
        &self.header.delayed_execution_results
    }

    pub fn split(self) -> (ConsensusBlockHeader<ST, SCT, EPT>, ConsensusBlockBody<EPT>) {
        (self.header, self.body)
    }
}

pub trait ExecutionProtocol:
    Debug + Clone + PartialEq + Eq + Send + Sync + Unpin + Encodable + Decodable + 'static
{
    /// inputs to execution
    type ProposedHeader: Debug
        + Clone
        + PartialEq
        + Eq
        + Send
        + Sync
        + Unpin
        + Encodable
        + Decodable;
    type Body: Debug + Clone + PartialEq + Eq + Send + Sync + Unpin + Encodable + Decodable;

    /// output of execution
    type FinalizedHeader: FinalizedHeader;
}

pub trait FinalizedHeader:
    Debug + Clone + PartialEq + Eq + Send + Sync + Unpin + Encodable + Decodable
{
    fn seq_num(&self) -> SeqNum;
}

pub trait MockableFinalizedHeader: Sized {
    fn from_seq_num(seq_num: SeqNum) -> Self;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposedExecutionResult<EPT>
where
    EPT: ExecutionProtocol,
{
    pub block_id: BlockId,
    pub seq_num: SeqNum,
    pub round: Round,
    pub result: EPT::FinalizedHeader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionResult<EPT>
where
    EPT: ExecutionProtocol,
{
    Proposed(ProposedExecutionResult<EPT>),
    Finalized(SeqNum, EPT::FinalizedHeader),
}

impl<EPT> ExecutionResult<EPT>
where
    EPT: ExecutionProtocol,
{
    pub fn seq_num(&self) -> SeqNum {
        match self {
            Self::Proposed(proposed) => proposed.seq_num,
            Self::Finalized(seq_num, _) => *seq_num,
        }
    }
}

pub struct ProposedExecutionInputs<EPT>
where
    EPT: ExecutionProtocol,
{
    pub header: EPT::ProposedHeader,
    pub body: EPT::Body,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct MockExecutionProtocol {}

impl ExecutionProtocol for MockExecutionProtocol {
    type ProposedHeader = MockExecutionProposedHeader;
    type Body = MockExecutionBody;
    type FinalizedHeader = MockExecutionFinalizedHeader;
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Default)]
pub struct MockExecutionProposedHeader {}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable, Default)]
pub struct MockExecutionBody {
    pub data: Bytes,
}
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct MockExecutionFinalizedHeader {
    number: SeqNum,
}
impl FinalizedHeader for MockExecutionFinalizedHeader {
    fn seq_num(&self) -> SeqNum {
        self.number
    }
}

impl MockableFinalizedHeader for MockExecutionFinalizedHeader {
    fn from_seq_num(seq_num: SeqNum) -> Self {
        Self { number: seq_num }
    }
}
