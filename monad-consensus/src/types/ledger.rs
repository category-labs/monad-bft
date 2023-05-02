use monad_types::Hash;

use crate::{
    types::{block::Block, signature::SignatureCollection, voting::VoteInfo},
    validation::hashing::{Hashable, Hasher},
};

use tracing::{event, Level};

#[derive(Copy, Clone, Default, PartialEq, Eq)]
pub struct LedgerCommitInfo {
    pub commit_state_hash: Option<Hash>,
    pub vote_info_hash: Hash,
}

impl std::fmt::Debug for LedgerCommitInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.commit_state_hash {
            Some(c) => write!(
                f,
                "commit: {:02x}{:02x}..{:02x}{:02x} ",
                c[0], c[1], c[30], c[31]
            ),
            None => write!(f, "commit: []"),
        }?;

        write!(
            f,
            "vote: {:02x}{:02x}..{:02x}{:02x}",
            self.vote_info_hash[0],
            self.vote_info_hash[1],
            self.vote_info_hash[30],
            self.vote_info_hash[31]
        )?;
        Ok(())
    }
}

impl LedgerCommitInfo {
    pub fn new<H: Hasher>(commit_state_hash: Option<Hash>, vote_info: &VoteInfo) -> Self {
        LedgerCommitInfo {
            commit_state_hash,
            vote_info_hash: H::hash_object(vote_info),
        }
    }
}

impl Hashable for LedgerCommitInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(self.vote_info_hash);
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
        event!(
            Level::DEBUG,
            num_blocks = blocks.len(),
            "appending to ledger"
        );
        self.blockchain.extend(blocks);
    }
}
