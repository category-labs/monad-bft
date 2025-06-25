use crate::ffi::monad_exec_block_start;

/// Result produced by a block builder while attempting to reassemble a block from execution events.
pub type BlockBuilderResult<T> = Result<T, BlockBuilderError>;

/// Error produced by a block builder while attempting to reassemble a block from execution events.
pub enum BlockBuilderError {
    /// The block was rejected by execution.
    Rejected,

    /// The payload expired while reading the event.
    PayloadExpired,

    /// The block cannot be correctly reassembled.
    ///
    /// This variant is produced when the [`ExecutedBlockBuilder`](crate::ExecutedBlockBuilder) is
    /// not given the entire sequential stream of event descriptors containing execution events from
    /// some block's initial [`monad_exec_block_start`] event to the final
    /// [`monad_exec_block_end`](crate::ffi::monad_exec_block_end) event. This can occur if
    /// the user skips, duplicates, or reorders event descriptors or, even worse, provides event
    /// descriptors from different event rings. The most likely of these scenarios arises if the
    /// upstream event ring gaps and
    /// [`ExecutedBlockBuilder::reset`](crate::ExecutedBlockBuilder::reset) is not called during the
    /// user's event recovery phase.
    ImplicitDrop {
        /// Start event of the block being reassembled.
        block: monad_exec_block_start,
        /// Explains why the block could not be reassembled.
        reassembly_error: ReassemblyError,
    },
}

/// Error produced by [`ExecutedBlockBuilder`](crate::ExecutedBlockBuilder) while attempting to
/// reassemble a block from execution events.
#[derive(Clone, Debug)]
pub enum ReassemblyError {
    /// A block header event was received while the current block was still being reassembled.
    ///
    /// It is guaranteed by monad-execution that all events for a given block begin with a
    /// [`monad_exec_block_start`] and end with a
    /// [`monad_exec_block_end`](crate::ffi::monad_exec_block_end), and that events for
    /// different blocks do not interleave. This implies that processing another block header before
    /// processing the previous block's block result means the block builder missed an event.
    UnterminatedBlock {
        /// The unexpected block header.
        unexpected_header: monad_exec_block_start,
    },

    /// Transaction with the given index is missing
    MissingTransaction {
        /// The missing transaction index.
        index: u64,
    },

    /// Transaction with the given index does not have the expected number
    /// of logs
    MissingTransactionLog {
        /// Missing index
        index: u64,
        /// The log count expected from the transaction receipt.
        expected_log_count: usize,
        /// The actual number of log events received.
        actual_log_count: usize,
    },
}
