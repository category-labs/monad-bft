use monad_types::Hash;

use crate::{
    types::{block::Block, signature::SignatureCollection, voting::VoteInfo},
    validation::hashing::{Hashable, Hasher},
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

impl Hashable for &LedgerCommitInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(&self.vote_info_hash);
        if let Some(x) = self.commit_state_hash.as_ref() {
            state.update(x);
        }
    }
}

pub trait Ledger {
    type Signatures: SignatureCollection;

    fn new() -> Self;
    fn add_blocks(&mut self, blocks: Vec<Block<Self::Signatures>>);
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryLedger<T> {
    blockchain: Vec<Block<T>>,
}

impl<T: SignatureCollection> InMemoryLedger<T> {
    pub fn get_blocks(&self) -> &Vec<Block<T>> {
        &self.blockchain
    }
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
