use monad_crypto::hasher::{Hash, Hashable, Hasher};

use crate::voting::VoteInfo;

/// Data related to blocks which satisfy the commit condition
/// Used in votes to be voted on by Nodes
#[derive(Copy, Clone, Default, PartialEq, Eq)]
pub struct LedgerCommitInfo {
    /// Hash related to a block which passes the commit condition
    /// In our case, will be the BlockId of a block which passes the commit condition
    /// Is None if no commit is expected to happen when creating a QC from this vote
    pub commit_state_hash: Option<Hash>,
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

        Ok(())
    }
}

impl LedgerCommitInfo {
    pub fn new<H: Hasher>(commit_state_hash: Option<Hash>, vote_info: &VoteInfo) -> Self {
        LedgerCommitInfo { commit_state_hash }
    }

    pub fn empty() -> Self {
        LedgerCommitInfo {
            commit_state_hash: None,
        }
    }
}

impl Hashable for LedgerCommitInfo {
    fn hash(&self, state: &mut impl Hasher) {
        if let Some(x) = self.commit_state_hash.as_ref() {
            state.update(x);
        }
    }
}
