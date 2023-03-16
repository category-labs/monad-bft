use super::validator::Address;

// VotingPower is i64
pub trait LeaderElection {
    fn new() -> Self;
    fn start_new_epoch(&mut self, voting_powers: Vec<(Address, i64)>);
    fn increment_view(&mut self, view: i64);
    fn get_leader(&self) -> &Address;
    fn update_voting_power(&mut self, addr: &Address, new_voting_power: i64) -> bool;
}
