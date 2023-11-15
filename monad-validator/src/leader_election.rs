use monad_types::{NodeId, Round};

use crate::validator_set::ValidatorSetType;

// VotingPower is i64
pub trait LeaderElection {
    fn new() -> Self;
    fn get_leader<VT>
    (
        &self,
        round: Round,
        epoch_length: Round,
        validator_set: &VT,
        upcoming_validator_set: &VT,
     ) -> NodeId
    where
        VT: ValidatorSetType;
}
