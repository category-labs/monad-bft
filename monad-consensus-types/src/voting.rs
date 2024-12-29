use std::collections::BTreeMap;

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, PubKey},
    hasher::{Hashable, Hasher},
};
use monad_types::*;
use serde::{Deserialize, Serialize};

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
#[derive(Copy, Clone, PartialEq, Eq, Serialize, Deserialize, RlpDecodable, RlpEncodable)]
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
    /// seqnum of the proposed block
    pub seq_num: SeqNum,
    /// timestamp of the proposed block
    pub timestamp: u64,
    /// version info of consensus protocol that voted on the proposed block
    pub version: MonadVersion,
}

impl Hashable for Vote {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(alloy_rlp::encode(self));
    }
}

impl std::fmt::Debug for Vote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vote")
            .field("id", &self.id)
            .field("epoch", &self.epoch)
            .field("r", &self.round)
            .field("pid", &self.parent_id)
            .field("pr", &self.parent_round)
            .field("sn", &self.seq_num)
            .field("ts", &self.timestamp)
            .finish()
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
            seq_num: SeqNum(0),
            timestamp: 0,
            version: MonadVersion::version(),
        }
    }
}
