use monad_crypto::secp256k1::KeyPair;
use monad_types::{NodeId, Round};
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

    fn get_leader(&self, _round: Round, _validator_list: &[NodeId]) -> NodeId {
        self.leader
    }
}
