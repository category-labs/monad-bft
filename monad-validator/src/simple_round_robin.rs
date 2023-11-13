use monad_types::{NodeId, Round, Epoch, EPOCH_LENGTH};

use super::leader_election::LeaderElection;

pub struct SimpleRoundRobin {}
impl LeaderElection for SimpleRoundRobin {
    fn new() -> Self {
        Self {}
    }

    fn get_leader(&self, round: Round, validator_list: &[NodeId], val_epoch: Epoch,
        upcoming_validator_list: &[NodeId], upcoming_val_epoch: Epoch) -> NodeId {
        let round_epoch = Epoch((round.0 + EPOCH_LENGTH - 1) / EPOCH_LENGTH);

        let node = if round_epoch == val_epoch {
            validator_list[round.0 as usize % validator_list.len()]
        } else if round_epoch == upcoming_val_epoch {
            upcoming_validator_list[round.0 as usize % upcoming_validator_list.len()]
        } else {
            // TODO: probably reached here to get leader from previous epoch
            panic!("unknown leader")
        };

        node
    }
}
