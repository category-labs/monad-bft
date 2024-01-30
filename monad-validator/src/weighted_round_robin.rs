use std::{collections::BTreeMap, marker::PhantomData};

use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::PubKey;
use monad_types::{Epoch, NodeId, Round, Stake};

use crate::leader_election::{LeaderElection, UpdateValidators};

pub type Schedule<PT> = Vec<NodeId<PT>>;
#[derive(Clone)]
pub struct WeightedRoundRobin<PT, SCT>
where
    PT: PubKey,
    SCT: SignatureCollection,
{
    schedule: Schedule<PT>,
    phantom_data: PhantomData<SCT>,
}

impl<PT: PubKey, SCT: SignatureCollection> Default for WeightedRoundRobin<PT, SCT> {
    fn default() -> Self {
        Self {
            schedule: vec![],
            phantom_data: PhantomData,
        }
    }
}

impl<PT: PubKey, SCT: SignatureCollection> WeightedRoundRobin<PT, SCT> {
    pub fn compute_schedule(stakes: Vec<(NodeId<PT>, Stake)>) -> Schedule<PT> {
        if let Some(max_stake) = stakes.iter().map(|(_, stake)| stake.0).max() {
            (1i64..=max_stake)
                .flat_map(|c| {
                    stakes.iter().filter_map(
                        move |(validator, stake)| {
                            if c <= stake.0 {
                                Some(*validator)
                            } else {
                                None
                            }
                        },
                    )
                })
                .collect::<_>()
        } else {
            vec![]
        }
    }
}

impl<PT: PubKey, SCT: SignatureCollection<NodeIdPubKey = PT>> LeaderElection
    for WeightedRoundRobin<PT, SCT>
{
    type NodeIdPubKey = PT;
    type NodeSignatureCollection = SCT;

    fn get_schedule(&self) -> Vec<NodeId<Self::NodeIdPubKey>> {
        self.schedule.clone()
    }

    fn update(&mut self, event: &UpdateValidators<Self::NodeSignatureCollection>) {
        self.schedule = WeightedRoundRobin::<PT, SCT>::compute_schedule(event.0.get_stakes());
    }

    fn get_leader(
        &self,
        round: Round,
        _epoch: Epoch,
        validators: &BTreeMap<NodeId<Self::NodeIdPubKey>, Stake>,
    ) -> NodeId<PT> {
        self.schedule[round.0 as usize % self.schedule.len()]
    }
}
