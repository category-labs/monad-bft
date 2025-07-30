use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_types::{BlockId, Epoch, ExecutionProtocol, Round, SeqNum};
use serde::{Deserialize, Serialize};

use crate::{signature_collection::SignatureCollection, tip::ConsensusTip, RoundCertificate};

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
#[serde(bound(serialize = "", deserialize = ""))]
pub struct Checkpoint<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub root: BlockId,
    pub high_certificate: RoundCertificate<ST, SCT, EPT>,

    pub maybe_high_tip: Option<ConsensusTip<ST, SCT, EPT>>,

    // TODO can we get rid of this by including an epoch_start_block_id in every block?
    pub validator_sets: Vec<LockedEpoch>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockedEpoch {
    /// Validator set are active for this epoch
    pub epoch: Epoch,
    /// By the end of epoch - 1, the next epoch is scheduled to start on round. Otherwise, it's left empty
    pub round: Option<Round>,
}
