use std::collections::BTreeMap;

use monad_crypto::hasher::{Hashable, Hasher};
use monad_types::*;
use zerocopy::AsBytes;

use crate::{certificate_signature::CertificateKeyPair, ledger::LedgerCommitInfo};

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

impl std::fmt::Debug for Vote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vote")
            .field("vote_info", &self.vote_info)
            .field("ledger_commit_info", &self.ledger_commit_info)
            .finish()
    }
}

impl Hashable for Vote {
    fn hash(&self, state: &mut impl Hasher) {
        self.vote_info.hash(state);
        self.ledger_commit_info.hash(state)
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct VoteInfo {
    pub id: BlockId,
    pub round: Round,
    pub parent_id: BlockId,
    pub parent_round: Round,
    pub seq_num: SeqNum,
}

impl std::fmt::Debug for VoteInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VoteInfo")
            .field("id", &self.id)
            .field("r", &self.round)
            .field("pid", &self.parent_id)
            .field("pr", &self.parent_round)
            .field("sn", &self.seq_num)
            .finish()
    }
}

impl Hashable for VoteInfo {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.id.0.as_bytes());
        state.update(self.round.as_bytes());
        state.update(self.parent_id.0.as_bytes());
        state.update(self.parent_round.as_bytes());
        state.update(self.seq_num.as_bytes());
    }
}

#[cfg(test)]
mod test {
    use monad_crypto::hasher::{Hash, Hasher, HasherType};
    use monad_types::{BlockId, Round, SeqNum};
    use zerocopy::AsBytes;

    use super::VoteInfo;

    #[test]
    fn voteinfo_hash() {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let mut hasher = HasherType::new();
        hasher.update(vi.id.0);
        hasher.update(vi.round);
        hasher.update(vi.parent_id.0);
        hasher.update(vi.parent_round);
        hasher.update(vi.seq_num.as_bytes());

        let h1 = hasher.hash();
        let h2 = HasherType::hash_object(&vi);

        assert_eq!(h1, h2);
    }
}
