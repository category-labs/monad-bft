use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_validator::leader_election::LeaderElection;

pub struct MockLeaderElection {
    leader: PubKey,
}

impl LeaderElection for MockLeaderElection {
    fn new() -> Self {
        let key: [u8; 32] = [128; 32];
        let keypair = KeyPair::from_slice(&key).unwrap();
        let leader = keypair.pubkey().clone();
        MockLeaderElection { leader }
    }

    fn start_new_epoch(&mut self, _voting_powers: Vec<(monad_crypto::secp256k1::PubKey, i64)>) {}

    fn increment_view(&mut self, _view: i64) {}

    fn get_leader(&self) -> &PubKey {
        &self.leader
    }

    fn update_voting_power(
        &mut self,
        _addr: &monad_crypto::secp256k1::PubKey,
        _new_voting_power: i64,
    ) -> bool {
        true
    }
}
