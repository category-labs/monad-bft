use monad_crypto::secp256k1::PubKey;

// VotingPower is i64
pub trait LeaderElection {
    fn new() -> Self;
    fn start_new_epoch(&mut self, voting_powers: Vec<(PubKey, i64)>);
    fn increment_view(&mut self, view: i64);
    fn get_leader(&self) -> &PubKey;
    fn update_voting_power(&mut self, addr: &PubKey, new_voting_power: i64) -> bool;
}
