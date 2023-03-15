use crate::types::voting::*;
use crate::*;

#[derive(Copy, Clone, Debug, Default)]
pub struct LedgerCommitInfo {
    pub commit_state_hash: Option<Hash>,
    pub vote_info_hash: Hash,
}

impl LedgerCommitInfo {
    pub fn new(commit_state_hash: Option<Hash>, vote_info: &VoteInfo) -> Self {
        LedgerCommitInfo {
            commit_state_hash,
            vote_info_hash: vote_info.get_hash(),
        }
    }
}
