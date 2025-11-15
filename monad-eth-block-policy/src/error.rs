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

use monad_state_backend::StateBackendError;
use monad_system_calls::validator::SystemTransactionValidationError;

#[derive(Debug, PartialEq)]
pub enum EthBlockPolicyError {
    InvalidSeqNum,
    StateBackendError(StateBackendError),
    TimestampError,
    ExecutionResultMismatch,
    BaseFeeError,
    BlockPolicyBlockValidatorError(EthBlockPolicyBlockValidatorError),
    InvalidNonce,
    SystemTransactionError(SystemTransactionValidationError),
}

#[derive(Debug, PartialEq)]
pub enum EthBlockPolicyBlockValidatorError {
    AccountBalanceMissing,
    InsufficientBalance,
    InsufficientReserveBalance,
}

impl From<EthBlockPolicyBlockValidatorError> for EthBlockPolicyError {
    fn from(err: EthBlockPolicyBlockValidatorError) -> Self {
        Self::BlockPolicyBlockValidatorError(err)
    }
}

impl From<StateBackendError> for EthBlockPolicyError {
    fn from(err: StateBackendError) -> Self {
        Self::StateBackendError(err)
    }
}

impl From<SystemTransactionValidationError> for EthBlockPolicyError {
    fn from(err: SystemTransactionValidationError) -> Self {
        Self::SystemTransactionError(err)
    }
}
