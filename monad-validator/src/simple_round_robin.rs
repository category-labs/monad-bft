use monad_types::{NodeId, Round};

use crate::validator_set::ValidatorSetType;

use super::leader_election::LeaderElection;

pub struct SimpleRoundRobin {}
impl LeaderElection for SimpleRoundRobin {
    fn new() -> Self {
        Self {}
    }

    fn get_leader<VT>(
        &self,
        round: Round,
        epoch_length: Round,
        validator_set: &VT,
        upcoming_validator_set: &VT,
    ) -> NodeId
    where
        VT: ValidatorSetType,
    {
        let round_epoch = round.get_epoch_num(epoch_length);
        let validator_list = validator_set.get_list();
        let upcoming_validator_list = upcoming_validator_set.get_list();

        let node = if round_epoch == validator_set.get_epoch() {
            validator_list[round.0 as usize % validator_list.len()]
        } else if round_epoch == upcoming_validator_set.get_epoch() {
            upcoming_validator_list[round.0 as usize % upcoming_validator_list.len()]
        } else {
            // TODO: probably reached here to get leader from previous epoch
            panic!("unknown leader")
        };

        node
    }
}
