use std::collections::BTreeMap;

use monad_crypto::{
    certificate_signature::{CertificateKeyPair, PubKey},
    hasher::{Hashable, Hasher},
};
use monad_types::*;
use serde::{Deserialize, Serialize};
use zerocopy::AsBytes;

use crate::ledger::CommitResult;

/// Map validator NodeId to its Certificate PubKey
pub struct ValidatorMapping<PT: PubKey, VKT: CertificateKeyPair> {
    pub map: BTreeMap<NodeId<PT>, VKT::PubKeyType>,
}

impl<PT: PubKey, VKT: CertificateKeyPair> ValidatorMapping<PT, VKT> {
    pub fn new(iter: impl IntoIterator<Item = (NodeId<PT>, VKT::PubKeyType)>) -> Self {
        Self {
            map: iter.into_iter().collect(),
        }
    }
}

impl<PT: PubKey, VKT: CertificateKeyPair> IntoIterator for ValidatorMapping<PT, VKT> {
    type Item = (NodeId<PT>, VKT::PubKeyType);
    type IntoIter = std::collections::btree_map::IntoIter<NodeId<PT>, VKT::PubKeyType>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}

/// Vote for consensus proposals
#[derive(Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vote {
    /// contents of the vote over which the QC is eventually formed
    pub vote_info: VoteInfo,
    /// commit decision
    pub ledger_commit_info: CommitResult,
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

#[derive(Copy, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct VoteInfo {
    /// id of the proposed block
    pub id: BlockId,
    /// round of the proposed block
    pub round: Round,
    /// parent block id of the proposed block
    pub parent_id: BlockId,
    /// parent round of the proposed block
    pub parent_round: Round,
    /// seqnum of the proposed block
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
        self.id.hash(state);
        state.update(self.round.as_bytes());
        self.parent_id.hash(state);
        state.update(self.parent_round.as_bytes());
        state.update(self.seq_num.as_bytes());
    }
}

#[cfg(test)]
mod test {
    use monad_crypto::hasher::{Hash, Hashable, Hasher, HasherType};
    use monad_types::{BlockId, Round, SeqNum};
    use test_case::test_case;
    use zerocopy::AsBytes;

    use super::VoteInfo;
    use crate::{ledger::CommitResult, voting::Vote};

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

    #[test_case(CommitResult::NoCommit ; "NoCommit")]
    #[test_case(CommitResult::Commit ; "Commit")]
    fn vote_hash(cr: CommitResult) {
        let vi = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let v = Vote {
            vote_info: vi,
            ledger_commit_info: cr,
        };

        let mut hasher = HasherType::new();
        vi.hash(&mut hasher);
        cr.hash(&mut hasher);

        let h1 = hasher.hash();
        let h2 = HasherType::hash_object(&v);

        assert_eq!(h1, h2);
    }
}
