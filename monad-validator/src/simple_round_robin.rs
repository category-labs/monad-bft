use std::{collections::BTreeMap, marker::PhantomData};

use alloy_primitives::U256;
use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Round, Stake};

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
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> NodeId<PT> {
        let validators: Vec<_> = validators
            .iter()
            .filter_map(|(node_id, stake)| (*stake != Stake(U256::ZERO)).then_some(node_id))
            .collect();
        *validators[round.0 as usize % validators.len()]
    }
}
