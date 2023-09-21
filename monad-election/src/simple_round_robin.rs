use monad_types::{NodeId, Round};
use monad_validator::validator_set::ValidatorSetType;

use super::leader_election::LeaderElection;

pub struct SimpleRoundRobin {}

impl LeaderElection for SimpleRoundRobin {
    // round robin ignores all evidence

    fn get_leader<VT>(&self, round: Round, valset: &VT) -> Option<NodeId>
    where
        VT: ValidatorSetType,
    {
        Some(valset.get_list()[round.0 as usize % valset.len()])
    }
}
