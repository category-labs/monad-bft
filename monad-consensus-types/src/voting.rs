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
        self.ledger_commit_info.hash(state)
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct VoteInfo {
    pub id: BlockId,
    pub round: Round,
    pub parent_id: BlockId,
    pub parent_round: Round,
    pub seq_num: u64,
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
    use monad_types::{BlockId, Round};
    use test_case::test_case;
    use zerocopy::AsBytes;

    use super::VoteInfo;
    use crate::{ledger::LedgerCommitInfo, voting::Vote};

    #[test]
    fn voteinfo_hash() {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
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

    #[test_case(None ; "None commit_state")]
    #[test_case(Some(Default::default()) ; "Some commit_state")]
    fn vote_hash(cs: Option<Hash>) {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: 0,
        };

        let vi_hash = HasherType::hash_object(&vi);

        let lci = LedgerCommitInfo {
            commit_state_hash: cs,
            vote_info_hash: vi_hash,
        };

        let v = Vote {
            vote_info: vi,
            ledger_commit_info: lci,
        };

        let mut hasher = HasherType::new();
        hasher.update(vi_hash);
        if let Some(cs) = cs {
            hasher.update(cs);
        }

        let h1 = hasher.hash();
        let h2 = HasherType::hash_object(&v);

        assert_eq!(h1, h2);
    }
}
