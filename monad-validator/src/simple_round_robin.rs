use monad_types::{NodeId, Round};

use crate::validator_set::{ValidatorSetMapping, ValidatorSetType};

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
        validator_sets: &ValidatorSetMapping<VT>,
    ) -> NodeId
    where
        VT: ValidatorSetType,
    {
        let round_epoch = round.get_epoch_num(epoch_length);

        if let Some(validator_set) = validator_sets.get(&round_epoch) {
            let validator_list = validator_set.get_list();
            validator_list[round.0 as usize % validator_list.len()]
        } else {
            // TODO: fix panic
            panic!(
                "validator set for epoch #{} not in validator set mapping",
                round_epoch.0
            )
        }
    }
}
