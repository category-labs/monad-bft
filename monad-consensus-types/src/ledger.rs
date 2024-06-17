use monad_crypto::hasher::{Hashable, Hasher};
use serde::{Deserialize, Serialize};
use zerocopy::AsBytes;

#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, AsBytes, Serialize, Deserialize)]
pub enum CommitResult {
    NoCommit,
    Commit,
}

impl AsRef<[u8]> for CommitResult {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl CommitResult {
    pub fn is_commitable(&self) -> bool {
        match self {
            CommitResult::Commit => true,
            CommitResult::NoCommit => false,
        }
    }
}

impl Hashable for CommitResult {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.as_bytes());
    }
}
