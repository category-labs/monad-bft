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
//! served. A caller is either an operator of the domain, who is exempt from every rule, or
//! any other account, which is served the [`Policy::Open`] methods and its own data:
//!
//! | Methods | Rule |
//! |---|---|
//! | `eth_getBalance`, `eth_getTransactionCount` | the requested account is the caller |
//! | `eth_getTransactionByHash` | the caller sent the transaction |
//! | `eth_getTransactionReceipt` | the caller sent the transaction; its logs are removed |

use alloy_consensus::ReceiptEnvelope;
use alloy_primitives::{Address, Bloom};
use alloy_rpc_types::TransactionReceipt;

use crate::types::jsonrpc::JsonRpcError;

/// JSON-RPC error code of a request the authenticated account is not allowed to make.
pub const FORBIDDEN_ERROR_CODE: i32 = -32007;

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

/// Who a method is served to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    /// Every authenticated account.
    Open,
    /// Every authenticated account, for its own data only.
    OwnData,
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

    use super::{Caller, FORBIDDEN_ERROR_CODE};
    use crate::types::jsonrpc::JsonRpcError;

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
}
