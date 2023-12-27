use monad_crypto::hasher::{Hash, Hashable, Hasher, HasherType};

use crate::voting::VoteInfo;

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
    pub fn new(commit_state_hash: Option<Hash>, vote_info: &VoteInfo) -> Self {
        LedgerCommitInfo {
            commit_state_hash,
            vote_info_hash: HasherType::hash_object(vote_info),
        }
    }
}

impl Hashable for LedgerCommitInfo {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.vote_info_hash);
        if let Some(x) = self.commit_state_hash.as_ref() {
            state.update(x);
        }
    }
}
