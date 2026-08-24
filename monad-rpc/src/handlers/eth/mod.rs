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

use monad_ethcall::{EthCallError, EthCallResult, EthCallSuccess};

pub mod account;
pub mod block;
pub mod call;
pub mod gas;
pub mod simulate;
pub mod txn;

#[derive(Clone, Debug)]
pub enum CallResult {
    Success(SuccessCallResult),
    Failure(FailureCallResult),
    Revert(RevertCallResult), // only used for trace
}

#[derive(Clone, Debug, Default)]
pub struct SuccessCallResult {
    pub gas_used: u64,
    pub gas_refund: u64,
    // We interpret this as rlp encoded CallFrames for debug_traceCall
    pub output_data: Box<[u8]>,
}

impl From<EthCallSuccess> for SuccessCallResult {
    fn from(result: EthCallSuccess) -> Self {
        Self {
            gas_used: result.gas_used,
            gas_refund: result.gas_refund,
            output_data: result.output_data,
        }
    }
}

impl From<SuccessCallResult> for CallResult {
    fn from(result: SuccessCallResult) -> Self {
        Self::Success(result)
    }
}

impl From<EthCallSuccess> for CallResult {
    fn from(result: EthCallSuccess) -> Self {
        Self::Success(result.into())
    }
}

#[derive(Clone, Debug, Default)]
pub struct FailureCallResult {
    pub error_code: EthCallResult,
    pub gas_used: u64,
    pub gas_refund: u64,
    pub message: String,
    pub data: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct RevertCallResult {
    pub trace: Box<[u8]>,
}

impl From<EthCallError> for CallResult {
    fn from(error: EthCallError) -> Self {
        match error {
            EthCallError::Failure {
                error_code,
                gas_used,
                gas_refund,
                message,
                data,
            } => Self::Failure(FailureCallResult {
                error_code,
                gas_used,
                gas_refund,
                message,
                data,
            }),
            EthCallError::GasLimitTooHigh => Self::Failure(FailureCallResult {
                error_code: EthCallResult::OtherError,
                gas_used: 0,
                gas_refund: 0,
                message: String::from("gas limit too high"),
                data: None,
            }),
            EthCallError::InternalError => Self::Failure(FailureCallResult {
                error_code: EthCallResult::OtherError,
                gas_used: 0,
                gas_refund: 0,
                message: String::from("internal eth_call error"),
                data: None,
            }),
            EthCallError::Other { message } => Self::Failure(FailureCallResult {
                error_code: EthCallResult::OtherError,
                gas_used: 0,
                gas_refund: 0,
                message,
                data: None,
            }),
            EthCallError::ReserveBalanceViolation {
                gas_used,
                gas_refund,
            } => Self::Failure(FailureCallResult {
                error_code: EthCallResult::ReserveBalanceViolation,
                gas_used,
                gas_refund,
                message: String::from("reserve balance violation"),
                data: None,
            }),
            EthCallError::Trace { trace } => Self::Revert(RevertCallResult { trace }),
        }
    }
}

impl From<FailureCallResult> for crate::types::jsonrpc::JsonRpcError {
    fn from(error: FailureCallResult) -> Self {
        match error.error_code {
            EthCallResult::ExecutionError => {
                Self::eth_call_execution_revert(error.message, error.data)
            }
            EthCallResult::InsufficientBalance => Self::insufficient_funds(),
            _ => Self::eth_call_error(error.message, error.data),
        }
    }
}

#[cfg(test)]
mod test {
    use monad_ethcall::{EthCallError, EthCallResult};

    use super::{CallResult, FailureCallResult};
    use crate::types::jsonrpc::JsonRpcError;

    #[test]
    fn test_insufficient_balance_rejection_maps_to_insufficient_funds() {
        let rejection = EthCallError::Failure {
            error_code: EthCallResult::InsufficientBalance,
            gas_used: 0,
            gas_refund: 0,
            message: "insufficient balance".to_string(),
            data: None,
        };

        let CallResult::Failure(failure) = CallResult::from(rejection.clone()) else {
            panic!("validation rejection must convert to CallResult::Failure");
        };
        let err: JsonRpcError = failure.into();
        assert_eq!(err, JsonRpcError::insufficient_funds());
        assert_eq!(err.code, -32000);

        let err: JsonRpcError = rejection.into();
        assert_eq!(err, JsonRpcError::insufficient_funds());

        let err: JsonRpcError = FailureCallResult {
            error_code: EthCallResult::OtherError,
            message: "bad nonce".to_string(),
            ..Default::default()
        }
        .into();
        assert_eq!(err.code, -32603);

        let err: JsonRpcError = FailureCallResult {
            error_code: EthCallResult::ExecutionError,
            message: "execution reverted".to_string(),
            ..Default::default()
        }
        .into();
        assert_eq!(err.code, 3);
    }
}
