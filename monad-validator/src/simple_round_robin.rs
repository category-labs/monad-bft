use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Round, Stake};

use crate::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    validator_set::{ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

#[derive(Clone)]
pub struct SimpleRoundRobin<PT: PubKey> {
    validators: Vec<(NodeId<PT>, Stake)>,
}

impl<PT: PubKey> SimpleRoundRobin<PT> {
    /// Only useful for testing
    pub fn seed<VTF: ValidatorSetTypeFactory<NodeIdPubKey = PT>, SCT: SignatureCollection>(
        epoch_manager: &EpochManager,
        validators_epoch_mapping: &ValidatorsEpochMapping<VTF, SCT>,
    ) -> SimpleRoundRobin<PT> {
        let mut election = SimpleRoundRobin::<PT>::default();
        election.update(
            validators_epoch_mapping
                .get_val_set(&epoch_manager.get_epoch(Round(0)))
                .unwrap()
                .get_members()
                .clone()
                .into_iter()
                .collect::<Vec<(NodeId<PT>, Stake)>>(),
        );
        election
    }
}

impl<PT: PubKey> Default for SimpleRoundRobin<PT> {
    fn default() -> Self {
        Self {
            validators: Default::default(),
        }
    }
}

impl<PT: PubKey> LeaderElection for SimpleRoundRobin<PT> {
    type NodeIdPubKey = PT;

    fn update(&mut self, stakes: Vec<(NodeId<Self::NodeIdPubKey>, Stake)>) {
        self.validators = stakes
    }

    fn get_leader(&self, round: Round) -> NodeId<Self::NodeIdPubKey> {
        self.validators[round.0 as usize % self.validators.len()].0
    }
}
