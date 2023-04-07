use crate::{
    types::{block::Block, signature::SignatureCollection, voting::VoteInfo},
    validation::hashing::Hasher,
    Hash,
};

#[derive(Copy, Clone, Debug, Default)]
pub struct LedgerCommitInfo {
    pub commit_state_hash: Option<Hash>,
    pub vote_info_hash: Hash,
}

impl LedgerCommitInfo {
    pub fn new<H: Hasher>(commit_state_hash: Option<Hash>, vote_info: &VoteInfo) -> Self {
        LedgerCommitInfo {
            commit_state_hash,
            vote_info_hash: H::hash_object(vote_info),
        }
    }
}

pub trait Ledger {
    type Signatures: SignatureCollection;

    fn new() -> Self;
    fn add_blocks(&mut self, blocks: Vec<Block<Self::Signatures>>);
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryLedger<T: SignatureCollection> {
    blockchain: Vec<Block<T>>,
}

impl<T: SignatureCollection> Ledger for InMemoryLedger<T> {
    type Signatures = T;

    fn new() -> Self {
        InMemoryLedger {
            blockchain: Default::default(),
        }
    }

    fn add_blocks(&mut self, blocks: Vec<Block<Self::Signatures>>) {
        self.blockchain.extend(blocks);
    }
}
