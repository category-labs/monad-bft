use monad_types::{Epoch, NodeId, Round};

// VotingPower is i64
pub trait LeaderElection {
    fn new() -> Self;
    fn get_leader(&self, round: Round, validator_list: &[NodeId], val_epoch: Epoch,
        upcoming_validator_list: &[NodeId], upcoming_val_epoch: Epoch) -> NodeId;
}
