use monad_types::{NodeId, Round};

// VotingPower is i64
pub trait LeaderElection {
    fn new() -> Self;
    fn start_new_epoch(&mut self, voting_powers: Vec<(NodeId, i64)>);
    fn increment_view(&mut self, view: Round);
    fn get_leader(&self) -> &NodeId;
    fn update_voting_power(&mut self, addr: &NodeId, new_voting_power: i64) -> bool;
}
