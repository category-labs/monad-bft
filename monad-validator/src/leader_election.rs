use monad_consensus_types::signature_collection::SignatureCollection;
use monad_types::{NodeId, Round};

use crate::validator_set::{ValidatorSetType, ValidatorsEpochMapping};

// VotingPower is i64
pub trait LeaderElection {
    fn new() -> Self;
    fn get_leader<VT, SCT>(
        &self,
        round: Round,
        epoch_length: Round,
        validators_epoch_mapping: &ValidatorsEpochMapping<VT, SCT>,
    ) -> NodeId
    where
        VT: ValidatorSetType,
        SCT: SignatureCollection;
}
