use std::collections::BTreeMap;

use monad_crypto::{
    certificate_signature::{CertificateKeyPair, PubKey},
    hasher::{Hashable, Hasher},
};
use monad_types::*;
use serde::{Deserialize, Serialize};
use zerocopy::AsBytes;

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
    /// id of the proposed block
    pub id: BlockId,
    /// epoch of the proposed block
    pub epoch: Epoch,
    /// round of the proposed block
    pub round: Round,
    /// parent block id of the proposed block
    pub parent_id: BlockId,
    /// parent round of the proposed block
    pub parent_round: Round,
}

impl std::fmt::Debug for Vote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vote")
            .field("id", &self.id)
            .field("epoch", &self.epoch)
            .field("r", &self.round)
            .field("pid", &self.parent_id)
            .field("pr", &self.parent_round)
            .finish()
    }
}

impl Hashable for Vote {
    fn hash(&self, state: &mut impl Hasher) {
        self.id.hash(state);
        state.update(self.epoch.as_bytes());
        state.update(self.round.as_bytes());
        self.parent_id.hash(state);
        state.update(self.parent_round.as_bytes());
    }
}

impl DontCare for Vote {
    fn dont_care() -> Self {
        Self {
            id: BlockId(Hash([0x0_u8; 32])),
            epoch: Epoch(1),
            round: Round(0),
            parent_id: BlockId(Hash([0x0_u8; 32])),
            parent_round: Round(0),
        }
    }
}

#[cfg(test)]
mod test {
    use monad_crypto::hasher::{Hash, Hasher, HasherType};
    use monad_types::{BlockId, Epoch, Round};

    use super::Vote;

    #[test]
    fn vote_hash() {
        let vi = Vote {
            id: BlockId(Hash([0x00_u8; 32])),
            epoch: Epoch(1),
            round: Round(0),
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
        };

        let mut hasher = HasherType::new();
        hasher.update(vi.id.0);
        hasher.update(vi.epoch);
        hasher.update(vi.round);
        hasher.update(vi.parent_id.0);
        hasher.update(vi.parent_round);

        let h1 = hasher.hash();
        let h2 = HasherType::hash_object(&vi);

        assert_eq!(h1, h2);
    }
}
