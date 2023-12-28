use monad_consensus_types::signature_collection::SignatureCollection;
use monad_epoch::epoch_manager::EpochManager;
use monad_types::{NodeId, Round};

use crate::{
    leader_election::LeaderElection, validator_set::ValidatorSetType,
    validators_epoch_map::ValidatorsEpochMapping,
};

pub struct SimpleRoundRobin {}
impl LeaderElection for SimpleRoundRobin {
    fn new() -> Self {
        Self {}
    }

    fn get_leader<VT, SCT>(
        &self,
        round: Round,
        epoch_manager: &EpochManager,
        val_epoch_map: &ValidatorsEpochMapping<VT, SCT>,
    ) -> NodeId
    where
        VT: ValidatorSetType,
        SCT: SignatureCollection,
    {
        let round_epoch = epoch_manager.get_epoch(round);

        if let Some(validator_set) = val_epoch_map.get_val_set(&round_epoch) {
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
