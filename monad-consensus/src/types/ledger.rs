use crate::types::voting::*;
use crate::validation::hashing::Hasher;
use crate::*;

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
