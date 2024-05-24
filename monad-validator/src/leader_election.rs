use std::collections::BTreeMap;

use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Round, Stake};

// VotingPower is i64
pub trait LeaderElection {
    type NodeIdPubKey: PubKey;
    fn get_leader(
        &self,
        round: Round,
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> NodeId<Self::NodeIdPubKey>;
}

impl<T: LeaderElection + ?Sized> LeaderElection for Box<T> {
    type NodeIdPubKey = T::NodeIdPubKey;

    fn get_leader(
        &self,
        round: Round,
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> NodeId<Self::NodeIdPubKey> {
        (**self).get_leader(round, validators)
    }
}
