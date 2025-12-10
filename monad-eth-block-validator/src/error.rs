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

use alloy_primitives::{B256, B64};
use monad_consensus_types::payload::ConsensusBlockBodyId;
use monad_eth_block_policy::validation::StaticValidationError;
use monad_eth_types::{Ommer, Withdrawal};
use monad_system_calls::validator::SystemTransactionValidationError;

#[derive(Debug)]
pub enum EthBlockValidationError {
    HeaderError(HeaderError),
    PayloadError(PayloadError),
    SystemTxnError(SystemTransactionValidationError),
    TxnError(TxnError),
}

#[derive(Debug)]
pub enum HeaderError {
    HeaderPayloadMismatch {
        expected_body_id: ConsensusBlockBodyId,
        actual: ConsensusBlockBodyId,
    },
    RandaoError,
    NonEmptyOmmersHash(B256),
    InvalidTransactionsRoot {
        expected: B256,
        actual: B256,
    },
    NonEmptyWithdrawalsRoot(B256),
    NonZeroDifficulty(u64),
    InvalidHeaderNumber {
        expected: u64,
        actual: u64,
    },
    InvalidGasLimit {
        expected: u64,
        actual: u64,
    },
    InvalidTimestamp {
        consensus_header_timestamp: u128,
        eth_header_timestamp: u128,
    },
    InvalidRoundSignatureHash {
        expected: B256,
        actual: B256,
    },
    InvalidBaseFee {
        consensus_header_base_fee: u64,
        eth_header_base_fee: u64,
    },
    NonEmptyHeaderNonce(B64),
    NonEmptyExtraData(B256),
    NonZeroBlockGasUsed(u64),
    NonZeroExcessBlobGas(u64),
    NonEmptyParentBeaconRoot(B256),
    InvalidRequestsHash {
        expected: Option<[u8; 32]>,
        actual: Option<[u8; 32]>,
    },
}

impl From<HeaderError> for EthBlockValidationError {
    fn from(value: HeaderError) -> Self {
        Self::HeaderError(value)
    }
}

#[derive(Debug)]
pub enum PayloadError {
    NonEmptyOmmers(Vec<Ommer>),
    NonEmptyWithdrawals(Vec<Withdrawal>),
    ExceededNumTxnLimit { num_txs: usize },
    ExceededBlockGasLimit { total_gas: u64 },
    ExceededBlockSizeLimit { txs_size: usize },
}

impl From<PayloadError> for EthBlockValidationError {
    fn from(value: PayloadError) -> Self {
        Self::PayloadError(value)
    }
}

#[derive(Debug)]
pub enum TxnError {
    SignerRecoveryError(monad_secp::Error),
    StaticValidationError(StaticValidationError),
    MaxFeeLessThanBaseFee,
    InvalidSystemAccountAuthorization,
    NonceOverflow,
    InvalidNonce,
}

impl From<TxnError> for EthBlockValidationError {
    fn from(value: TxnError) -> Self {
        Self::TxnError(value)
    }
}

impl From<SystemTransactionValidationError> for EthBlockValidationError {
    fn from(value: SystemTransactionValidationError) -> Self {
        Self::SystemTxnError(value)
    }
}
