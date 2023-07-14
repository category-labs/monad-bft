use monad_types::{NodeId, Round};

// VotingPower is i64
pub trait LeaderElection {
    fn new() -> Self;
    fn get_leader(&self, round: Round, validator_list: &[NodeId]) -> NodeId;
}
