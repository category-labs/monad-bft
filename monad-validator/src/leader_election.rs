use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Round, Stake};

// VotingPower is i64
pub trait LeaderElection {
    type NodeIdPubKey: PubKey;

    fn update(&mut self, stakes: Vec<(NodeId<Self::NodeIdPubKey>, Stake)>);

    fn get_schedule(&self) -> Vec<NodeId<Self::NodeIdPubKey>>;

    fn get_leader(&self, round: Round) -> NodeId<Self::NodeIdPubKey>;
}

impl<T: LeaderElection + ?Sized> LeaderElection for Box<T> {
    type NodeIdPubKey = T::NodeIdPubKey;

    fn update(&mut self, stakes: Vec<(NodeId<Self::NodeIdPubKey>, Stake)>) {
        (**self).update(stakes)
    }

    fn get_schedule(&self) -> Vec<NodeId<Self::NodeIdPubKey>> {
        (**self).get_schedule()
    }

    fn get_leader(&self, round: Round) -> NodeId<Self::NodeIdPubKey> {
        (**self).get_leader(round)
    }
}
