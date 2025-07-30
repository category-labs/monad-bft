use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_types::*;
use serde::{Deserialize, Serialize};

/// Vote for consensus proposals
#[derive(Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, RlpDecodable, RlpEncodable)]
pub struct Vote {
    /// id of the proposed block
    pub id: BlockId,
    /// the round that this vote is for
    pub round: Round,
    /// the epoch of the round that this vote is for
    pub epoch: Epoch,
}

impl std::fmt::Debug for Vote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vote")
            .field("id", &self.id)
            .field("epoch", &self.epoch)
            .field("round", &self.round)
            .finish()
    }
}

impl DontCare for Vote {
    fn dont_care() -> Self {
        Self {
            id: BlockId(Hash([0x0_u8; 32])),
            epoch: Epoch(1),
            round: Round(0),
        }
    }
}
