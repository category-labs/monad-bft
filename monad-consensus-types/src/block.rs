use std::fmt::Debug;

use monad_crypto::hasher::{Hashable, Hasher, HasherType};
use monad_types::{BlockId, NodeId, Round, SeqNum};
use zerocopy::AsBytes;

use crate::{
    block_validator::BlockValidator, payload::Payload, quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
};

/// This trait represents a consensus block
pub trait BlockType: Clone + PartialEq + Eq {
    /// Unique hash for the block
    fn get_id(&self) -> BlockId;

    /// Round in which this block was proposed
    fn get_round(&self) -> Round;

    /// Node which proposed this block
    fn get_author(&self) -> NodeId;

    /// returns the BlockId for the block referenced by
    /// the QC contained in this block
    fn get_parent_id(&self) -> BlockId;

    /// returns the Round for the block referenced by
    /// the QC contained in this block
    fn get_parent_round(&self) -> Round;

    /// Sequence number when this block was proposed
    fn get_seq_num(&self) -> SeqNum;
}

/// structure of the consensus block
/// the payload field is used to carry the data of the block
/// which is agnostic to the actual protocol of consensus
#[derive(Clone)]
pub struct Block<T> {
    /// proposer of this block
    pub author: NodeId,

    /// round this block was proposed in
    pub round: Round,

    /// protocol agnostic data for the blockchain
    pub payload: Payload,

    /// Certificate of votes for this block
    pub qc: QuorumCertificate<T>,

    /// Unique hash used to identify the block
    id: BlockId,
}

impl<T> PartialEq for Block<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl<T> Eq for Block<T> {}

impl<T: std::fmt::Debug> std::fmt::Debug for Block<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("author", &self.author)
            .field("round", &self.round)
            .field("qc_info", &self.qc.info)
            .field("qc", &self.qc)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<T: SignatureCollection> Hashable for Block<T> {
    fn hash(&self, state: &mut impl Hasher) {
        self.id.hash(state);
    }
}

impl<T: SignatureCollection> Block<T> {
    // FIXME &QuorumCertificate -> QuorumCertificate
    pub fn new(author: NodeId, round: Round, payload: &Payload, qc: &QuorumCertificate<T>) -> Self {
        Self {
            author,
            round,
            payload: payload.clone(),
            qc: qc.clone(),
            id: {
                let mut _block_hash_span = tracing::info_span!("block_hash_span").entered();
                let mut state = HasherType::new();
                state.update(author.0.bytes());
                state.update(round.as_bytes());
                payload.hash(&mut state);
                state.update(qc.get_block_id().0.as_bytes());
                state.update(qc.get_hash().as_bytes());

                BlockId(state.hash())
            },
        }
    }

    /// Try to create a Block from an UnverifiedBlock, verifying
    /// with the TransactionValidator
    pub fn try_from_unverified(
        unverified: UnverifiedBlock<T>,
        validator: &impl BlockValidator,
    ) -> Option<Self> {
        validator
            .validate(&unverified.0.payload.txns)
            .then_some(unverified.0)
    }
}

impl<T: SignatureCollection> BlockType for Block<T> {
    fn get_id(&self) -> BlockId {
        self.id
    }

    fn get_round(&self) -> Round {
        self.round
    }

    fn get_author(&self) -> NodeId {
        self.author
    }

    fn get_parent_id(&self) -> BlockId {
        self.qc.get_block_id()
    }

    fn get_parent_round(&self) -> Round {
        self.qc.get_round()
    }

    fn get_seq_num(&self) -> SeqNum {
        self.payload.seq_num
    }
}

/// A block alongside the list of RLP encoded full transactions
/// The transactions have not been verified
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnverifiedBlock<T>(pub Block<T>);

impl<T> UnverifiedBlock<T> {
    pub fn new(block: Block<T>) -> Self {
        Self(block)
    }
}

impl<T> From<Block<T>> for UnverifiedBlock<T> {
    fn from(value: Block<T>) -> Self {
        Self(value)
    }
}

impl<T: SignatureCollection> Hashable for UnverifiedBlock<T> {
    fn hash(&self, state: &mut impl Hasher) {
        self.0.hash(state);
    }
}
