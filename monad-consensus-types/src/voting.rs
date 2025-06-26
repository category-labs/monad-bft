use std::collections::BTreeMap;

use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_crypto::certificate_signature::{CertificateKeyPair, PubKey};
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
#[derive(Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, RlpDecodable, RlpEncodable)]
pub struct Vote {
    /// id of the proposed block
    pub id: BlockId,
    /// the round that this vote is for
    pub round: Round,
    /// the epoch of the round that this vote is for
    pub epoch: Epoch,
    /// round the block was first proposed in
    /// if proposal_round != block_round, the proposal is a reproposal
    pub block_round: Round,
}

impl std::fmt::Debug for Vote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vote")
            .field("id", &self.id)
            .field("epoch", &self.epoch)
            .field("round", &self.round)
            .field("block_round", &self.block_round)
            .finish()
    }
}

impl DontCare for Vote {
    fn dont_care() -> Self {
        Self {
            id: BlockId(Hash([0x0_u8; 32])),
            epoch: Epoch(1),
            round: Round(0),
            block_round: Round(0),
        }
    }
}
