use monad_crypto::secp256k1::KeyPair;
use monad_types::{NodeId, Round, Stake};
use monad_validator::leader_election::LeaderElection;

pub struct MockLeaderElection {
    leader: NodeId,
}

impl LeaderElection for MockLeaderElection {
    fn new() -> Self {
        let mut key: [u8; 32] = [128; 32];
        let keypair = KeyPair::from_bytes(&mut key).unwrap();
        let leader = keypair.pubkey();
        MockLeaderElection {
            leader: NodeId(leader),
        }
    }

    fn start_new_epoch(&mut self, _voting_powers: Vec<(NodeId, Stake)>) {}

    fn increment_view(&mut self, _view: Round) {}

    fn get_leader(&self) -> &NodeId {
        &self.leader
    }

    fn update_voting_power(&mut self, _addr: &NodeId, _new_voting_power: Stake) -> bool {
        true
    }
}
