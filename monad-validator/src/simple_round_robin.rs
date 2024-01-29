use std::{collections::BTreeMap, marker::PhantomData};

use monad_crypto::certificate_signature::PubKey;
use monad_types::{Epoch, NodeId, Round, Stake};
use tracing::info;

use crate::leader_election::LeaderElection;

#[derive(Clone)]
pub struct SimpleRoundRobin<PT>(PhantomData<PT>);
impl<PT> Default for SimpleRoundRobin<PT> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<PT: PubKey> LeaderElection for SimpleRoundRobin<PT> {
    type NodeIdPubKey = PT;
    fn get_leader(
        &self,
        round: Round,
        _epoch: Epoch,
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> NodeId<PT> {
        let validators: Vec<_> = validators.keys().collect();
        info!(
            "get_leader round: {:?}, leader index: {:?}",
            round,
            round.0 as usize % validators.len()
        );

        *validators[round.0 as usize % validators.len()]
    }
}
