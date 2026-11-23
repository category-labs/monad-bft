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

//! Access rules of the domain RPC.
//!
//! [`super::auth`] establishes who is calling; this module decides what that account is
//! served. A caller is either an operator of the domain, who is exempt from every rule but
//! that of a call, or any other account, which is served the [`Policy::Open`] methods and its
//! own data:
//!
//! | Methods | Rule |
//! |---|---|
//! | `eth_getBalance`, `eth_getTransactionCount` | the requested account is the caller |
//! | `eth_getTransactionByHash` | the caller sent the transaction |
//! | `eth_getTransactionReceipt` | the caller sent the transaction; its logs are removed |
//! | `eth_getLogs` | the filter has the caller as the only value of a topic position |
//! | `eth_call`, `eth_createAccessList`, `eth_estimateGas` | the caller is the sender of the call |
//!
//! A call has its caller as its sender and runs on the domain's own state, without state
//! overrides, for an operator as for any other account. What it may reach from its sender is
//! decided where it executes.
//!
//! A log filter may narrow its result by contract and by other topics. `eth_getLogs` reads a
//! block only if the block's bloom filter matches the whole filter, so the time a response
//! takes can tell a caller whether those other values occur in the logs of a block that names
//! it.

use std::collections::BTreeMap;

use alloy_consensus::ReceiptEnvelope;
use alloy_primitives::{Address, Bloom};
use alloy_rpc_types::{Filter, TransactionReceipt};
use serde::de::IgnoredAny;
use serde_json::value::{to_raw_value, RawValue};

use crate::types::jsonrpc::{JsonRpcError, JsonRpcResultExt, RequestParams};

/// JSON-RPC error code of a request the authenticated account is not allowed to make.
pub const FORBIDDEN_ERROR_CODE: i32 = -32007;

fn raw_json<T: serde::Serialize>(value: &T) -> Result<Box<RawValue>, JsonRpcError> {
    to_raw_value(value).map_err(|err| JsonRpcError::internal_error(err.to_string()))
}

fn forbidden(message: String) -> JsonRpcError {
    JsonRpcError {
        code: FORBIDDEN_ERROR_CODE,
        message: format!("Forbidden: {message}"),
        data: None,
    }
}

/// The account a request is served on behalf of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Caller {
    /// Account recovered from the access token.
    pub signer: Address,
    /// Whether `signer` is an operator of the domain.
    pub operator: bool,
}

/// Number of parameters of a call ahead of its state overrides: the transaction and the block.
const CALL_PARAMS_BEFORE_OVERRIDES: usize = 2;

/// Who a method is served to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    /// Every authenticated account.
    Open,
    /// Every authenticated account, for its own data only.
    OwnData,
    /// Every authenticated account, for a call it is the sender of.
    OwnCall,
    /// Operators of the domain.
    OperatorOnly,
}

impl Caller {
    pub fn require_operator(&self) -> Result<(), JsonRpcError> {
        if self.operator {
            Ok(())
        } else {
            Err(forbidden(format!(
                "{} is not an operator of the domain",
                self.signer
            )))
        }
    }

    /// `account` must be the caller.
    pub fn require_account(&self, account: Address) -> Result<(), JsonRpcError> {
        if self.operator || account == self.signer {
            Ok(())
        } else {
            Err(forbidden(format!(
                "{account} is not the authenticated account {}",
                self.signer
            )))
        }
    }

    /// The caller must be the sender of the transaction that `item` is, or is derived from. A
    /// missing item passes through.
    pub fn require_sender<T>(
        &self,
        item: Option<T>,
        sender: impl FnOnce(&T) -> Address,
    ) -> Result<Option<T>, JsonRpcError> {
        match item {
            Some(item) if !self.operator && sender(&item) != self.signer => {
                Err(forbidden(format!(
                    "the transaction was not sent by the authenticated account {}",
                    self.signer
                )))
            }
            item => Ok(item),
        }
    }

    /// `filter` must match only logs that have the caller as a topic: the caller is the only
    /// value of one of its topic positions.
    pub fn require_own_logs(&self, filter: &Filter) -> Result<(), JsonRpcError> {
        let caller = self.signer.into_word();
        if self.operator
            || filter
                .topics
                .iter()
                .any(|t| t.len() == 1 && t.contains(&caller))
        {
            Ok(())
        } else {
            Err(forbidden(format!(
                "the log filter must have the authenticated account {} as the only value of a topic position",
                self.signer
            )))
        }
    }

    /// The parameters of a call, `[transaction, block, state overrides]`, as the caller may
    /// make it: without state overrides, and with the caller as its sender. An operator is no
    /// exception.
    pub fn own_call(&self, params: RequestParams<'_>) -> Result<Box<RawValue>, JsonRpcError> {
        // Parameters are kept as raw JSON, so that nothing but the sender is rewritten.
        let mut params: Vec<Box<RawValue>> = serde_json::from_str(params.get()).invalid_params()?;
        for overrides in params.iter().skip(CALL_PARAMS_BEFORE_OVERRIDES) {
            let overrides: Option<BTreeMap<String, IgnoredAny>> =
                serde_json::from_str(overrides.get()).invalid_params()?;
            if overrides.is_some_and(|overrides| !overrides.is_empty()) {
                return Err(forbidden(
                    "state overrides are not served on a private domain".into(),
                ));
            }
        }
        let transaction = params
            .first_mut()
            .ok_or_else(JsonRpcError::invalid_params)?;
        let mut fields: BTreeMap<String, Box<RawValue>> =
            serde_json::from_str(transaction.get()).invalid_params()?;
        fields.insert("from".into(), raw_json(&self.signer)?);
        *transaction = raw_json(&fields)?;
        raw_json(&params)
    }

    /// Removes the logs of `receipt`.
    pub fn remove_logs(&self, receipt: &mut TransactionReceipt) {
        if self.operator {
            return;
        }
        let (ReceiptEnvelope::Legacy(inner)
        | ReceiptEnvelope::Eip2930(inner)
        | ReceiptEnvelope::Eip1559(inner)
        | ReceiptEnvelope::Eip4844(inner)
        | ReceiptEnvelope::Eip7702(inner)) = &mut receipt.inner;
        inner.receipt.logs.clear();
        inner.logs_bloom = Bloom::ZERO;
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::Address;
    use alloy_rpc_types::Filter;
    use serde_json::{json, value::to_raw_value, Value};

    use super::{Caller, FORBIDDEN_ERROR_CODE};
    use crate::types::jsonrpc::{ErrorCode, JsonRpcError, RequestParams};

    const SIGNER: Address = Address::repeat_byte(0x11);
    const OTHER: Address = Address::repeat_byte(0x22);

    const USER: Caller = Caller {
        signer: SIGNER,
        operator: false,
    };
    const OPERATOR: Caller = Caller {
        signer: SIGNER,
        operator: true,
    };

    fn assert_forbidden<T: std::fmt::Debug>(result: Result<T, JsonRpcError>) {
        let err = result.unwrap_err();
        assert_eq!(err.code, FORBIDDEN_ERROR_CODE);
        assert!(err.message.starts_with("Forbidden: "), "{err:?}");
    }

    #[test]
    fn only_operators_pass_as_operators() {
        assert!(OPERATOR.require_operator().is_ok());
        assert_forbidden(USER.require_operator());
    }

    #[test]
    fn account_must_be_the_caller() {
        assert!(USER.require_account(SIGNER).is_ok());
        assert_forbidden(USER.require_account(OTHER));
        assert!(OPERATOR.require_account(OTHER).is_ok());
    }

    #[test]
    fn sender_must_be_the_caller() {
        let sender = |sender: &Address| *sender;
        assert_eq!(USER.require_sender(Some(SIGNER), sender), Ok(Some(SIGNER)));
        assert_eq!(USER.require_sender(None, sender), Ok(None));
        assert_forbidden(USER.require_sender(Some(OTHER), sender));
        assert_eq!(
            OPERATOR.require_sender(Some(OTHER), sender),
            Ok(Some(OTHER))
        );
    }

    /// The parameters `caller` may make the call `params` with.
    fn own_call(caller: Caller, params: Value) -> Result<Value, JsonRpcError> {
        let params = to_raw_value(&params).unwrap();
        caller
            .own_call(RequestParams::new(&params))
            .map(|params| serde_json::from_str(params.get()).unwrap())
    }

    #[test]
    fn caller_is_the_sender_of_its_calls() {
        let call = |from: Option<Address>| json!([{"from": from, "to": OTHER}, "latest"]);
        for caller in [USER, OPERATOR] {
            for from in [None, Some(OTHER)] {
                assert_eq!(own_call(caller, call(from)).unwrap(), call(Some(SIGNER)));
            }
        }
    }

    #[test]
    fn calls_take_no_state_overrides() {
        let call = |overrides: Value| json!([{"to": OTHER}, "latest", overrides]);
        let overrides = json!({ (OTHER.to_string()): {"balance": "0x1"} });
        for caller in [USER, OPERATOR] {
            assert!(own_call(caller, call(json!({}))).is_ok());
            assert_forbidden(own_call(caller, call(overrides.clone())));
        }
    }

    #[test]
    fn call_parameters_are_positional() {
        for params in [json!({"transaction": {"to": OTHER}}), json!([])] {
            let err = own_call(USER, params).unwrap_err();
            assert_eq!(err.code, ErrorCode::InvalidParams as i32);
        }
    }

    #[test]
    fn log_filter_must_pin_the_caller_in_a_topic() {
        let signer = SIGNER.into_word();
        let other = OTHER.into_word();

        for filter in [
            Filter::new().topic1(signer),
            Filter::new().topic3(signer),
            Filter::new().address(OTHER).topic1(other).topic2(signer),
        ] {
            assert!(USER.require_own_logs(&filter).is_ok(), "{filter:?}");
        }

        // No topic, another account, a set that also matches another account, and the caller
        // as the address of the contract.
        for filter in [
            Filter::new(),
            Filter::new().topic1(other),
            Filter::new().topic1(vec![signer, other]),
            Filter::new().address(SIGNER),
        ] {
            assert_forbidden(USER.require_own_logs(&filter));
            assert!(OPERATOR.require_own_logs(&filter).is_ok(), "{filter:?}");
        }
    }
}
