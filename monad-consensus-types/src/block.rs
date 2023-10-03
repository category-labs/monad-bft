use monad_types::{BlockId, Hash as HashType, NodeId, Round};
use zerocopy::AsBytes;

use crate::{
    payload::{FullTransactionList, Payload},
    quorum_certificate::QuorumCertificate,
    signature_collection::SignatureCollection,
    transaction_validator::TransactionValidator,
    validation::{Hashable, Hasher},
};

pub trait BlockType: Clone + PartialEq + Eq {
    fn get_id(&self) -> BlockId;
    fn get_round(&self) -> Round;
    fn get_author(&self) -> NodeId;

    fn get_parent_id(&self) -> BlockId;
    fn get_parent_round(&self) -> Round;

    fn get_seq_num(&self) -> u64;
}

#[derive(Clone)]
pub struct Block<T> {
    pub author: NodeId,
    pub round: Round,
    pub payload: Payload,
    pub qc: QuorumCertificate<T>,
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
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(self.author.0.bytes());
        state.update(self.round.as_bytes());
        self.payload.hash(state);
        state.update(self.qc.info.vote.id.0.as_bytes());
        state.update(self.qc.get_hash().as_bytes());
    }
}

impl<T: SignatureCollection> Block<T> {
    pub fn new<H: Hasher>(
        author: NodeId,
        round: Round,
        payload: &Payload,
        qc: &QuorumCertificate<T>,
    ) -> Self {
        let mut b = Block {
            author,
            round,
            payload: payload.clone(),
            qc: qc.clone(),
            id: BlockId(HashType([0x00_u8; 32])),
        };
        // FIXME make this less jank
        b.id = BlockId(H::hash_object(&b));
        b
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

    fn get_seq_num(&self) -> u64 {
        self.payload.seq_num
    }
}

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
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.block.hash(state);
        state.update(self.full_txs.0.as_bytes());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullBlock<T> {
    block: Block<T>,
    full_txs: FullTransactionList,
}

impl<T> FullBlock<T> {
    pub fn from_block(
        block: Block<T>,
        full_txs: FullTransactionList,
        validator: &impl TransactionValidator,
    ) -> Option<Self> {
        validator
            .validate(&block.payload.txns, &full_txs)
            .then_some(Self { block, full_txs })
    }

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

    fn get_seq_num(&self) -> u64 {
        self.block.get_seq_num()
    }
}
