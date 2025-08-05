use monad_types::{Epoch, Round, SeqNum};
use thiserror::Error;

/// Defines errors that can occur within the consensus state machine.
#[derive(Error, Debug, PartialEq)]
pub enum ConsensusError {
    /// Returned when the node's state is too far behind the network's high_qc to sync.
    /// This is considered a fatal, unrecoverable state that requires a restart.
    #[error("Node is too far behind to sync. Root: {root_seq_num:?}, High QC: {high_qc_seq_num:?}")]
    FatalOutOfSync {
        root_seq_num: SeqNum,
        high_qc_seq_num: SeqNum,
    },

    /// Returned when processing a message intended for a round in an unknown epoch.
    #[error("Epoch not found for round {0:?}")]
    EpochNotFound(Round),

    /// Returned when the validator set for a given epoch cannot be found.
    #[error("Validator set not found for epoch {0:?}")]
    ValidatorSetNotFound(Epoch),

    // Other error cases can be added here as more `expect()` calls are refactored.
}