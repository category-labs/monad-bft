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

use alloy_primitives::{Address, TxHash};
use monad_eth_block_policy::validation::StaticValidationError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EthTxPoolEvent {
    pub tx_hash: TxHash,
    pub action: EthTxPoolEventType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum EthTxPoolEventType {
    /// The tx was inserted into the txpool.
    Insert { address: Address, owned: bool },

    /// The tx was committed and is thus finalized.
    Commit,

    /// The tx was dropped for the attached reason.
    Drop { reason: EthTxPoolDropReason },

    /// The tx timed out and was evicted.
    Evict { reason: EthTxPoolEvictReason },
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EthTxPoolDropReason {
    NotWellFormed(StaticValidationError),
    InvalidSignature,
    NonceTooLow,
    FeeTooLow,
    InsufficientBalance,
    ExistingHigherPriority,
    ReplacedByHigherPriority { replacement: TxHash },
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
                StaticValidationError::InvalidChainId { .. } => "Invalid chain ID",
                StaticValidationError::MaxPriorityOverMaxFee { .. } => "Max priority fee too high",
                StaticValidationError::InitCodeLimitExceeded { .. } => {
                    "Init code size limit exceeded"
                }
                StaticValidationError::EncodedLengthLimitExceeded => {
                    "Encoded length limit exceeded"
                }
                StaticValidationError::GasLimitUnderFloorDataGas { .. } => "Gas limit too low",
                StaticValidationError::GasLimitUnderIntrinsicGas { .. } => "Gas limit too low",
                StaticValidationError::GasLimitOverTFMGasLimit { .. } => {
                    "Exceeds transaction gas limit"
                }
                StaticValidationError::GasLimitOverProposalGasLimit { .. } => {
                    "Exceeds transaction gas limit"
                }
                StaticValidationError::InvalidSignature => "Invalid transaction signature",
                StaticValidationError::UnsupportedTransactionType => "Unsupported transaction type",
                StaticValidationError::AuthorizationListEmpty => "EIP7702 authorization list empty",
                StaticValidationError::AuthorizationListLengthLimitExceeded => {
                    "EIP7702 authorization list length limit exceeded"
                }
            },
            EthTxPoolDropReason::InvalidSignature => "Transaction signature is invalid",
            EthTxPoolDropReason::NonceTooLow => "Transaction nonce too low",
            EthTxPoolDropReason::FeeTooLow => "Transaction fee too low",
            EthTxPoolDropReason::InsufficientBalance => "Signer had insufficient balance",
            EthTxPoolDropReason::PoolFull => "Transaction pool is full",
            EthTxPoolDropReason::ExistingHigherPriority => {
                "An existing transaction had higher priority"
            }
            EthTxPoolDropReason::ReplacedByHigherPriority { .. } => {
                "A newer transaction had higher priority"
            }
            EthTxPoolDropReason::PoolNotReady => "Transaction pool is not ready",
            EthTxPoolDropReason::Internal(_) => "Internal error",
        }
        .to_owned()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EthTxPoolEvictReason {
    Expired,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EthTxPoolSnapshot {
    pub txs: HashSet<TxHash>,
}
