use monad_types::{NodeId, Round};

use super::leader_election::LeaderElection;

pub struct SimpleRoundRobin {}
impl LeaderElection for SimpleRoundRobin {
    fn new() -> Self {
        Self {}
    }

    fn get_leader(&self, round: Round, validator_list: &[NodeId]) -> NodeId {
        validator_list[round.0 as usize % validator_list.len()]
    }
}
