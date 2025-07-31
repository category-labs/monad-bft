// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashSet;

use alloy_primitives::{Address, TxHash, B256};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EthTxPoolEvent {
    /// The tx was inserted into the txpool's (pending/tracked) tx list.
    Insert {
        tx_hash: B256,
        address: Address,
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

// allow for more fine grain debugging if needed
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionError {
    InvalidChainId,
    MaxPriorityFeeTooHigh,
    InitCodeLimitExceeded,
    GasLimitTooLow,
    GasLimitTooHigh,
    UnsupportedTransactionType,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EthTxPoolDropReason {
    NotWellFormed(TransactionError),
    InvalidSignature,
    NonceTooLow,
    FeeTooLow,
    InsufficientBalance,
    ExistingHigherPriority,
    PoolFull,
    PoolNotReady,
    Internal(EthTxPoolInternalDropReason),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EthTxPoolInternalDropReason {
    StateBackendError,
    NotReady,
}

impl EthTxPoolDropReason {
    pub fn as_user_string(&self) -> String {
        match self {
            EthTxPoolDropReason::NotWellFormed(err) => match err {
                TransactionError::InvalidChainId => "Invalid chain ID",
                TransactionError::MaxPriorityFeeTooHigh => "Max priority fee too high",
                TransactionError::InitCodeLimitExceeded => "Init code size limit exceeded",
                TransactionError::GasLimitTooLow => "Gas limit too low",
                TransactionError::GasLimitTooHigh => "Exceeds block gas limit",
                TransactionError::UnsupportedTransactionType => {
                    "EIP4844 and EIP7702 transactions unsupported"
                }
            },
            EthTxPoolDropReason::InvalidSignature => "Transaction signature is invalid",
            EthTxPoolDropReason::NonceTooLow => "Transaction nonce too low",
            EthTxPoolDropReason::FeeTooLow => "Transaction fee too low",
            EthTxPoolDropReason::InsufficientBalance => "Signer had insufficient balance",
            EthTxPoolDropReason::PoolFull => "Transaction pool is full",
            EthTxPoolDropReason::ExistingHigherPriority => {
                "Another transaction has higher priority"
            }
            EthTxPoolDropReason::PoolNotReady => "Transaction pool is not ready",
            EthTxPoolDropReason::Internal(_) => "Internal error",
        }
        .to_owned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EthTxPoolEvictReason {
    Expired,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EthTxPoolSnapshot {
    pub pending: HashSet<TxHash>,
    pub tracked: HashSet<TxHash>,
}
