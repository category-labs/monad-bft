use monad_types::{BlockId, Epoch, Round, SeqNum};
use serde::{Deserialize, Serialize};

use crate::{
    quorum_certificate::QuorumCertificate, signature_collection::SignatureCollection,
    validator_data::ValidatorSetDataWithEpoch,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RootInfo {
    pub round: Round,
    pub seq_num: SeqNum,
    pub epoch: Epoch,
    pub block_id: BlockId,
    pub timestamp_ns: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint<SCT: SignatureCollection> {
    pub root: BlockId,
    // TODO high_round?
    #[serde(bound(
        serialize = "SCT: SignatureCollection",
        deserialize = "SCT: SignatureCollection",
    ))]
    pub high_qc: QuorumCertificate<SCT>,

    // TODO can we get rid of this by including an epoch_start_block_id in every block?
    #[serde(bound(
        serialize = "SCT: SignatureCollection",
        deserialize = "SCT: SignatureCollection",
    ))]
    pub validator_sets: Vec<ValidatorSetDataWithEpoch<SCT>>,
}
