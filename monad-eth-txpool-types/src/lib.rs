use std::collections::HashSet;

use alloy_primitives::{TxHash, B256};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EthTxPoolEvent {
    /// The tx was inserted into the txpool's (pending/tracked) tx list.
    Insert {
        tx_hash: B256,
        owned: bool,
        tracked: bool,
    },

    /// A tx with a higher fee replaced an exixting tx.
    ///
    /// Note: The new tx is always placed in the same tx list as the original, preserving promotion
    /// status.
    Replace {
        old_tx_hash: B256,
        new_tx_hash: B256,
        new_owned: bool,
        tracked: bool,
    },

    /// The tx was dropped for the attached reason.
    Drop {
        tx_hash: B256,
        reason: EthTxPoolDropReason,
    },

    /// The tx was promoted from the txpool's pending tx list to it's tracked tx list.
    Promoted { tx_hash: B256 },

    /// The tx was committed and is thus finalized.
    Commit { tx_hash: B256 },

    /// The tx timed out and was evicted.
    Evict {
        tx_hash: B256,
        reason: EthTxPoolEvictReason,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EthTxPoolDropReason {
    NotWellFormed,
    NonceTooLow,
    FeeTooLow,
    InsufficientBalance,
    PoolFull,
    ExistingHigherPriority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EthTxPoolEvictReason {
    Expired,
}

#[derive(Serialize, Deserialize)]
pub struct EthTxPoolSnapshot {
    pub pending: HashSet<TxHash>,
    pub tracked: HashSet<TxHash>,
}
