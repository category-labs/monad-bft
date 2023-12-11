use monad_crypto::hasher::{Hashable, Hasher, HasherType};
use monad_types::{BlockId, NodeId, Round, SeqNum};
use zerocopy::AsBytes;

use crate::{
    payload::{FullTransactionList, Payload},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    transaction_validator::TransactionValidator,
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
                qc.info.vote.id.hash(&mut state);
                state.update(qc.get_hash().as_bytes());

                BlockId(state.hash())
            },
        }
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
        self.qc.info.vote.id
    }

    fn get_parent_round(&self) -> Round {
        self.qc.info.vote.round
    }

    fn get_seq_num(&self) -> SeqNum {
        self.payload.seq_num
    }
}

/// A block alongside the list of RLP encoded full transactions
/// The transactions have not been verified
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnverifiedFullBlock<T> {
    pub block: Block<T>,
    pub full_txs: FullTransactionList,
}

impl<T> UnverifiedFullBlock<T> {
    pub fn new(block: Block<T>, full_txs: FullTransactionList) -> Self {
        Self { block, full_txs }
    }
}

impl<T> From<FullBlock<T>> for UnverifiedFullBlock<T> {
    fn from(value: FullBlock<T>) -> Self {
        Self {
            block: value.block,
            full_txs: value.full_txs,
        }
    }
}

impl<T: SignatureCollection> Hashable for UnverifiedFullBlock<T> {
    fn hash(&self, state: &mut impl Hasher) {
        self.block.hash(state);
        state.update(self.full_txs.bytes());
    }
}

/// A block alongside the list of RLP encoded full transactions
/// The transaction are verified on creation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullBlock<T> {
    block: Block<T>,
    full_txs: FullTransactionList,
}

impl<T> FullBlock<T> {
    /// Create a FullBlock from a Block and list of full transactions
    /// takes in a TransactionValidator to validate the list of transactions
    pub fn from_block(
        block: Block<T>,
        full_txs: FullTransactionList,
        validator: &impl TransactionValidator,
    ) -> Option<Self> {
        validator
            .validate(&block.payload.txns, &full_txs)
            .then_some(Self { block, full_txs })
    }

    /// Try to create a FullBlock from an UnverifiedFullBlock, verifying
    /// with the TransactionValidator
    pub fn try_from_unverified(
        unverified: UnverifiedFullBlock<T>,
        validator: &impl TransactionValidator,
    ) -> Option<Self> {
        validator
            .validate(&unverified.block.payload.txns, &unverified.full_txs)
            .then_some(Self {
                block: unverified.block,
                full_txs: unverified.full_txs,
            })
    }

    pub fn get_block(&self) -> &Block<T> {
        &self.block
    }

    pub fn get_full_txs(&self) -> &FullTransactionList {
        &self.full_txs
    }

    pub fn split(self) -> (Block<T>, FullTransactionList) {
        (self.block, self.full_txs)
    }
}

impl<T: SignatureCollection> BlockType for FullBlock<T> {
    fn get_id(&self) -> BlockId {
        self.block.get_id()
    }

    fn get_round(&self) -> Round {
        self.block.get_round()
    }

    fn get_author(&self) -> NodeId {
        self.block.get_author()
    }

    fn get_parent_id(&self) -> BlockId {
        self.block.get_parent_id()
    }

    fn get_parent_round(&self) -> Round {
        self.block.get_parent_round()
    }

    fn get_seq_num(&self) -> SeqNum {
        self.block.get_seq_num()
    }
}
