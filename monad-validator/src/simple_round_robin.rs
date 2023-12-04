use monad_consensus_types::signature_collection::SignatureCollection;
use monad_types::{NodeId, Round};

use super::leader_election::LeaderElection;
use crate::validator_set::{ValidatorSetType, ValidatorsEpochMapping};

pub struct SimpleRoundRobin {}
impl LeaderElection for SimpleRoundRobin {
    fn new() -> Self {
        Self {}
    }

    fn get_leader<VT, SCT>(
        &self,
        round: Round,
        epoch_length: Round,
        validators_epoch_mapping: &ValidatorsEpochMapping<VT, SCT>,
    ) -> NodeId
    where
        VT: ValidatorSetType,
        SCT: SignatureCollection,
    {
        let round_epoch = round.get_epoch_num(epoch_length);

        if let Some(validator_set) = validators_epoch_mapping.get_val_set(&round_epoch) {
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
