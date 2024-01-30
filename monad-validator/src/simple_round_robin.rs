use std::{collections::BTreeMap, marker::PhantomData};

use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::PubKey;
use monad_types::{Epoch, NodeId, Round, Stake};
use tracing::info;

use crate::leader_election::{LeaderElection, UpdateValidators};

#[derive(Clone)]
pub struct SimpleRoundRobin<PT: PubKey, SCT> {
    schedule: Vec<NodeId<PT>>,
    phantom_data: PhantomData<SCT>,
}

impl<PT: PubKey, SCT: SignatureCollection> Default for SimpleRoundRobin<PT, SCT> {
    fn default() -> Self {
        Self {
            schedule: vec![],
            phantom_data: PhantomData,
        }
    }
}

impl<PT: PubKey, SCT: SignatureCollection<NodeIdPubKey = PT>> LeaderElection
    for SimpleRoundRobin<PT, SCT>
{
    type NodeIdPubKey = PT;
    type NodeSignatureCollection = SCT;

    fn get_schedule(&self) -> Vec<NodeId<Self::NodeIdPubKey>> {
        self.schedule.clone()
    }

    fn update(&mut self, event: &UpdateValidators<Self::NodeSignatureCollection>) {
        self.schedule = event
            .0
            .clone()
            .0
            .into_iter()
            .map(|(node_id, _, _)| node_id)
            .collect::<Vec<_>>();
    }

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
