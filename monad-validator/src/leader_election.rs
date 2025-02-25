use std::collections::BTreeMap;

use auto_impl::auto_impl;
use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Round, Stake};

// VotingPower is i64
#[auto_impl(Box)]
pub trait LeaderElection {
    type NodeIdPubKey: PubKey;

    fn get_leader(
        &self,
        round: Round,
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> NodeId<Self::NodeIdPubKey>;
}
