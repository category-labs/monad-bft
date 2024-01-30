use std::collections::BTreeMap;

use monad_consensus_types::{
    signature_collection::SignatureCollection, validator_data::ValidatorData,
};
use monad_crypto::certificate_signature::PubKey;
use monad_types::{Epoch, NodeId, Round, Stake};

pub type UpdateValidators<SCT> = (ValidatorData<SCT>, Epoch);

// VotingPower is i64
pub trait LeaderElection {
    type NodeIdPubKey: PubKey;
    type NodeSignatureCollection: SignatureCollection;

    fn get_schedule(&self) -> Vec<NodeId<Self::NodeIdPubKey>>;

    fn update(&mut self, event: &UpdateValidators<Self::NodeSignatureCollection>);

    fn get_leader(
        &self,
        round: Round,
        epoch: Epoch,
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> NodeId<Self::NodeIdPubKey>;
}

impl<T: LeaderElection + ?Sized> LeaderElection for Box<T> {
    type NodeIdPubKey = T::NodeIdPubKey;
    type NodeSignatureCollection = T::NodeSignatureCollection;

    fn get_schedule(&self) -> Vec<NodeId<Self::NodeIdPubKey>> {
        (**self).get_schedule()
    }

    fn update(&mut self, event: &UpdateValidators<Self::NodeSignatureCollection>) {
        (**self).update(event)
    }

    fn get_leader(
        &self,
        round: Round,
        epoch: Epoch,
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> NodeId<Self::NodeIdPubKey> {
        (**self).get_leader(round, epoch, validators)
    }
}
