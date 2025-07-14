use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_types::*;
use serde::{Deserialize, Serialize};

/// Vote for consensus proposals
#[derive(Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, RlpDecodable, RlpEncodable)]
pub struct Vote {
    /// id of the proposed block
    pub id: BlockId,
    /// round of the proposed block
    pub round: Round,
    /// epoch of the proposed block
    pub epoch: Epoch,
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
