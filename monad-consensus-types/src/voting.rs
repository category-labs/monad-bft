use std::collections::BTreeMap;

use monad_types::*;
use zerocopy::AsBytes;

use crate::{
    certificate_signature::CertificateKeyPair,
    ledger::LedgerCommitInfo,
    validation::{Hashable, Hasher},
};

pub struct ValidatorMapping<VKT: CertificateKeyPair> {
    pub map: BTreeMap<NodeId, VKT::PubKeyType>,
}

impl<VKT: CertificateKeyPair> ValidatorMapping<VKT> {
    pub fn new(iter: impl IntoIterator<Item = (NodeId, VKT::PubKeyType)>) -> Self {
        Self {
            map: iter.into_iter().collect(),
        }
    }
}

impl<VKT: CertificateKeyPair> IntoIterator for ValidatorMapping<VKT> {
    type Item = (NodeId, VKT::PubKeyType);
    type IntoIter = std::collections::btree_map::IntoIter<NodeId, VKT::PubKeyType>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Vote {
    pub vote_info: VoteInfo,
    pub ledger_commit_info: LedgerCommitInfo,
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct VoteInfo {
    pub id: BlockId,
    pub round: Round,
    pub parent_id: BlockId,
    pub parent_round: Round,
}

impl std::fmt::Debug for VoteInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VoteInfo")
            .field("id", &self.id)
            .field("r", &self.round)
            .field("pid", &self.parent_id)
            .field("pr", &self.parent_round)
            .finish()
    }
}

impl Hashable for VoteInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(self.id.0.as_bytes());
        state.update(self.round.as_bytes());
        state.update(self.parent_id.0.as_bytes());
        state.update(self.parent_round.as_bytes());
    }
}

#[cfg(test)]
mod test {
    use monad_types::{BlockId, Hash, Round};
    use sha2::Digest;

    use super::VoteInfo;
    use crate::validation::{Hasher, Sha256Hash};

    pub fn hash_vote(v: &VoteInfo) -> Hash {
        let mut hasher = sha2::Sha256::new();
        hasher.update(v.id.0);
        hasher.update(v.round);
        hasher.update(v.parent_id.0);
        hasher.update(v.parent_round);

        Hash(hasher.finalize().into())
    }

    #[test]
    fn voteinfo_hash() {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let h1 = Sha256Hash::hash_object(&vi);
        let h2 = hash_vote(&vi);

        assert_eq!(h1, h2);
    }
}
