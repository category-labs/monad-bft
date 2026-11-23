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
//! served. A caller is either an operator of the domain, who is served every method, or any
//! other account, which is served the [`Policy::Open`] methods only.

use alloy_primitives::Address;

use crate::types::jsonrpc::JsonRpcError;

/// JSON-RPC error code of a request the authenticated account is not allowed to make.
pub const FORBIDDEN_ERROR_CODE: i32 = -32007;

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
    /// Operators of the domain.
    OperatorOnly,
}

impl Caller {
    /// Whether the caller is served a method with `policy`.
    pub fn authorize(&self, policy: Policy) -> Result<(), JsonRpcError> {
        match policy {
            Policy::Open => Ok(()),
            Policy::OperatorOnly if self.operator => Ok(()),
            Policy::OperatorOnly => Err(JsonRpcError {
                code: FORBIDDEN_ERROR_CODE,
                message: format!(
                    "Forbidden: {} is not an operator of the domain",
                    self.signer
                ),
                data: None,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::Address;

    use super::{Caller, Policy, FORBIDDEN_ERROR_CODE};

    #[test]
    fn operator_only_methods_are_forbidden_to_other_accounts() {
        let signer = Address::repeat_byte(0x11);
        let operator = Caller {
            signer,
            operator: true,
        };
        let user = Caller {
            signer,
            operator: false,
        };

        assert!(operator.authorize(Policy::Open).is_ok());
        assert!(operator.authorize(Policy::OperatorOnly).is_ok());
        assert!(user.authorize(Policy::Open).is_ok());

        let err = user.authorize(Policy::OperatorOnly).unwrap_err();
        assert_eq!(err.code, FORBIDDEN_ERROR_CODE);
        assert!(err.message.starts_with("Forbidden: "), "{err:?}");
    }
}
