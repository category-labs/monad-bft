use monad_consensus_types::signature_collection::SignatureCollection;
use monad_types::{epoch_manager::EpochManager, NodeId, Round};

use crate::{validator_set::ValidatorSetType, validators_epoch_map::ValidatorsEpochMapping};

// VotingPower is i64
pub trait LeaderElection {
    fn new() -> Self;
    fn get_leader<VT, SCT>(
        &self,
        round: Round,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VT, SCT>,
    ) -> NodeId
    where
        VT: ValidatorSetType,
        SCT: SignatureCollection;
}
