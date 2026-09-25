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

//! Per-method access rules on the `/domain/{chain_id}` RPC route.
//!
//! [`crate::auth`] establishes *who* is calling; this module decides *what* that account may
//! see. A caller is either a domain operator, who is unrestricted, or an ordinary account
//! that is only served its own data:
//!
//! | Methods | Rule |
//! |---|---|
//! | `eth_getBalance`, `eth_getTransactionCount`, `txpool_statusByAddress` | requested account is the signer |
//! | `eth_getTransactionByHash`, `eth_getTransactionReceipt` | transaction sender is the signer |
//! | `eth_call`, `eth_estimateGas`, `eth_createAccessList`, `eth_fillTransaction` | `from` is replaced by the signer; state overrides are rejected |
//! | `eth_getLogs` | the filter pins the signer in at least one topic position |
//! | block, code, storage, debug, simulation and admin methods | operators only |
//! | chain metadata, fee queries and transaction submission | any authenticated account |
//!
//! Every enabled method is assigned exactly one [`DomainPolicy`] in the dispatcher's
//! policy table, so adding a method forces a decision.

use std::collections::{HashMap, HashSet};

use alloy_primitives::Address;
use alloy_rpc_types::Filter;
use monad_ethcall::overrides::StateOverrideSet;

use crate::{handlers::eth::call::CallRequest, types::jsonrpc::JsonRpcError};

/// Operator accounts of each private domain, keyed by domain chain id.
pub type DomainOperators = HashMap<u64, HashSet<Address>>;

/// The account a domain request is served on behalf of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DomainCaller {
    /// Account recovered from the access token.
    pub signer: Address,
    /// Whether `signer` is a configured operator of the domain. Operators are exempt
    /// from every per-account rule.
    pub operator: bool,
}

/// How a method is gated on domain routes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DomainPolicy {
    /// Reveals no account-specific data; served to every authenticated caller.
    Open,
    /// Reveals domain-wide data; served to operators only.
    OperatorOnly,
    /// Served to a caller for their own data only. The method's dispatcher enforces the rule
    /// with the [`DomainCaller`] helpers.
    OwnDataOnly,
}

impl DomainCaller {
    pub fn new(signer: Address, operators: Option<&HashSet<Address>>) -> Self {
        Self {
            signer,
            operator: operators.is_some_and(|operators| operators.contains(&signer)),
        }
    }

    /// Gate applied before a method runs.
    pub fn authorize(&self, policy: DomainPolicy) -> Result<(), JsonRpcError> {
        match policy {
            DomainPolicy::Open | DomainPolicy::OwnDataOnly => Ok(()),
            DomainPolicy::OperatorOnly => self.require_operator(),
        }
    }

    pub fn require_operator(&self) -> Result<(), JsonRpcError> {
        if self.operator {
            Ok(())
        } else {
            Err(JsonRpcError::forbidden(
                "method is restricted to domain operators",
            ))
        }
    }

    /// The requested account must be the signer.
    pub fn require_account(&self, account: Address) -> Result<(), JsonRpcError> {
        if self.operator || account == self.signer {
            Ok(())
        } else {
            Err(JsonRpcError::forbidden(format!(
                "account {account} is not the authenticated account"
            )))
        }
    }

    /// A transaction, or anything derived from one, may only be returned to its sender. A
    /// missing item passes through unchanged.
    pub fn require_sender<T>(
        &self,
        item: Option<T>,
        sender: impl FnOnce(&T) -> Address,
    ) -> Result<Option<T>, JsonRpcError> {
        match item {
            Some(item) if !self.operator && sender(&item) != self.signer => Err(
                JsonRpcError::forbidden("transaction was not sent by the authenticated account"),
            ),
            item => Ok(item),
        }
    }

    /// Simulations run as the signer, whatever `from` the request names.
    pub fn run_as_signer(&self, request: &mut CallRequest) {
        if !self.operator {
            request.from = Some(self.signer);
        }
    }

    /// [`Self::run_as_signer`] for methods that also accept state overrides, which are
    /// rejected.
    pub fn restrict_call(
        &self,
        request: &mut CallRequest,
        state_overrides: &StateOverrideSet,
    ) -> Result<(), JsonRpcError> {
        if !self.operator && !state_overrides.is_empty() {
            return Err(JsonRpcError::forbidden(
                "state overrides are not allowed on domain routes",
            ));
        }
        self.run_as_signer(request);
        Ok(())
    }

    /// A log filter must pin the signer, as a single value, in at least one topic position so
    /// it can only match logs that reference the signer there.
    pub fn require_logs_filter_pins_signer(&self, filter: &Filter) -> Result<(), JsonRpcError> {
        if self.operator {
            return Ok(());
        }
        let signer_topic = self.signer.into_word();
        let pinned = filter
            .topics
            .iter()
            .any(|topic| topic.len() == 1 && topic.contains(&signer_topic));
        if pinned {
            Ok(())
        } else {
            Err(JsonRpcError::forbidden(
                "log filter must pin the authenticated account in a topic position",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::address;
    use monad_ethcall::overrides::StateOverrideObject;

    use super::*;
    use crate::types::jsonrpc::FORBIDDEN_ERROR_CODE;

    const SIGNER: Address = address!("1111111111111111111111111111111111111111");
    const OTHER: Address = address!("2222222222222222222222222222222222222222");

    fn user() -> DomainCaller {
        DomainCaller {
            signer: SIGNER,
            operator: false,
        }
    }

    fn operator() -> DomainCaller {
        DomainCaller {
            signer: SIGNER,
            operator: true,
        }
    }

    fn assert_forbidden<T: std::fmt::Debug>(result: Result<T, JsonRpcError>) {
        let err = result.expect_err("expected a forbidden error");
        assert_eq!(err.code, FORBIDDEN_ERROR_CODE, "{err:?}");
        assert!(err.message.starts_with("Forbidden: "), "{err:?}");
    }

    #[test]
    fn operator_flag_comes_from_configured_set() {
        let operators = HashSet::from([SIGNER]);
        assert!(DomainCaller::new(SIGNER, Some(&operators)).operator);
        assert!(!DomainCaller::new(OTHER, Some(&operators)).operator);
        assert!(!DomainCaller::new(SIGNER, None).operator);
    }

    #[test]
    fn only_operator_only_policy_gates_up_front() {
        assert!(user().authorize(DomainPolicy::Open).is_ok());
        assert!(user().authorize(DomainPolicy::OwnDataOnly).is_ok());
        assert_forbidden(user().authorize(DomainPolicy::OperatorOnly));
        assert!(operator().authorize(DomainPolicy::OperatorOnly).is_ok());
    }

    #[test]
    fn account_must_be_signer() {
        assert!(user().require_account(SIGNER).is_ok());
        assert_forbidden(user().require_account(OTHER));
        assert!(operator().require_account(OTHER).is_ok());
    }

    #[test]
    fn sender_must_be_signer() {
        assert_eq!(
            user()
                .require_sender(Some(SIGNER), |sender| *sender)
                .unwrap(),
            Some(SIGNER)
        );
        assert_forbidden(user().require_sender(Some(OTHER), |sender| *sender));
        assert_eq!(
            user()
                .require_sender(None::<Address>, |sender| *sender)
                .unwrap(),
            None
        );
        assert_eq!(
            operator()
                .require_sender(Some(OTHER), |sender| *sender)
                .unwrap(),
            Some(OTHER)
        );
    }

    #[test]
    fn calls_run_as_signer_without_state_overrides() {
        let mut request = CallRequest {
            from: Some(OTHER),
            ..CallRequest::default()
        };
        user().run_as_signer(&mut request);
        assert_eq!(request.from, Some(SIGNER));

        let mut request = CallRequest {
            from: Some(OTHER),
            ..CallRequest::default()
        };
        operator().run_as_signer(&mut request);
        assert_eq!(request.from, Some(OTHER));

        let mut request = CallRequest {
            from: Some(OTHER),
            ..CallRequest::default()
        };
        user()
            .restrict_call(&mut request, &StateOverrideSet::default())
            .unwrap();
        assert_eq!(request.from, Some(SIGNER));

        let mut request = CallRequest::default();
        user()
            .restrict_call(&mut request, &StateOverrideSet::default())
            .unwrap();
        assert_eq!(request.from, Some(SIGNER));

        let overrides = StateOverrideSet::from([(OTHER, StateOverrideObject::default())]);
        assert_forbidden(user().restrict_call(&mut request, &overrides));

        let mut request = CallRequest {
            from: Some(OTHER),
            ..CallRequest::default()
        };
        operator().restrict_call(&mut request, &overrides).unwrap();
        assert_eq!(request.from, Some(OTHER));
    }

    #[test]
    fn log_filter_must_pin_signer_in_a_topic() {
        let signer_topic = SIGNER.into_word();
        let other_topic = OTHER.into_word();

        assert!(user()
            .require_logs_filter_pins_signer(&Filter::new().topic1(signer_topic))
            .is_ok());
        assert!(user()
            .require_logs_filter_pins_signer(&Filter::new().topic3(signer_topic))
            .is_ok());
        assert!(user()
            .require_logs_filter_pins_signer(
                &Filter::new()
                    .address(OTHER)
                    .topic1(other_topic)
                    .topic2(signer_topic)
            )
            .is_ok());

        // No topics, another account, a set that also matches other accounts, or the signer
        // only in the address filter are all rejected.
        assert_forbidden(user().require_logs_filter_pins_signer(&Filter::new()));
        assert_forbidden(
            user().require_logs_filter_pins_signer(&Filter::new().topic1(other_topic)),
        );
        assert_forbidden(user().require_logs_filter_pins_signer(
            &Filter::new().topic1(vec![signer_topic, other_topic]),
        ));
        assert_forbidden(user().require_logs_filter_pins_signer(&Filter::new().address(SIGNER)));

        assert!(operator()
            .require_logs_filter_pins_signer(&Filter::new())
            .is_ok());
    }
}
