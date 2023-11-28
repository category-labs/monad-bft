use monad_types::{NodeId, Round};

use crate::validator_set::{ValidatorSetMapping, ValidatorSetType};

// VotingPower is i64
pub trait LeaderElection {
    fn new() -> Self;
    fn get_leader<VT>(
        &self,
        round: Round,
        epoch_length: Round,
        validator_sets: &ValidatorSetMapping<VT>,
    ) -> NodeId
    where
        VT: ValidatorSetType;
}
