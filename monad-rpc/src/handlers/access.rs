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
//! | `eth_getTransactionByHash` | transaction sender is the signer |
//! | `eth_getTransactionReceipt`, `eth_sendRawTransactionSync` | transaction sender is the signer; logs it may not read are removed |
//! | `eth_call`, `eth_estimateGas`, `eth_createAccessList`, `eth_fillTransaction` | `from` is replaced by the signer; state overrides are rejected |
//! | `eth_getLogs` | the filter names only contracts whose logs the signer may read, or pins the signer in at least one topic position |
//! | block, code, storage, debug, simulation and admin methods | operators only |
//! | chain metadata, fee queries and transaction submission | any authenticated account |
//!
//! The signer may read every log of a contract whose DomainSpoke policy owner it is or whose
//! `logReaders` include it, and any other log that has the signer as a topic.
//!
//! Every enabled method is assigned exactly one [`DomainPolicy`] in the dispatcher's
//! policy table, so adding a method forces a decision.

use std::collections::{HashMap, HashSet};

use alloy_primitives::{keccak256, logs_bloom, Address, B256};
use alloy_rpc_types::{Filter, Log, TransactionReceipt};
use monad_eth_types::AccountKey;
use monad_ethcall::overrides::StateOverrideSet;
use monad_triedb_utils::triedb_env::Triedb;

use crate::{handlers::eth::call::CallRequest, types::jsonrpc::JsonRpcError};

/// Operator accounts of each private domain, keyed by domain chain id.
pub type DomainOperators = HashMap<u64, HashSet<Address>>;

/// DomainSpoke storage slots of `policyOwners` and `logReaders`.
const SPOKE_POLICY_OWNERS_SLOT: B256 = B256::with_last_byte(2);
const SPOKE_LOG_READERS_SLOT: B256 = B256::with_last_byte(6);

/// Storage slot of `mapping[key]` for a mapping with an address key declared at `slot`.
fn mapping_slot(key: Address, slot: B256) -> B256 {
    keccak256([key.into_word().0, slot.0].concat())
}

/// Whether `reader` may read every log `contract` emitted on `domain`: it is the contract's
/// policy owner in the domain's `spoke` or one of its `logReaders`, at the latest block.
pub async fn is_log_reader(
    triedb: &impl Triedb,
    domain: Address,
    spoke: Address,
    contract: Address,
    reader: Address,
) -> Result<bool, String> {
    if reader.is_zero() {
        return Ok(false);
    }
    let block_key = triedb.get_latest_proposed_block_key();
    let read = |slot: B256| {
        triedb.get_storage_at_by_key(block_key, AccountKey::domain(domain, spoke), slot.0)
    };
    if read(mapping_slot(contract, SPOKE_POLICY_OWNERS_SLOT)).await? == reader.into_word().0 {
        return Ok(true);
    }
    let allowed = read(mapping_slot(
        reader,
        mapping_slot(contract, SPOKE_LOG_READERS_SLOT),
    ))
    .await?;
    Ok(allowed != [0; 32])
}

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

    /// The filter names only `readable` contracts, or pins the signer as the single value of a
    /// topic position.
    pub fn require_logs_filter_access(
        &self,
        filter: &Filter,
        readable: &HashSet<Address>,
    ) -> Result<(), JsonRpcError> {
        let own_contracts =
            !filter.address.is_empty() && filter.address.iter().all(|a| readable.contains(a));
        if self.operator || own_contracts || self.pins_signer(filter) {
            Ok(())
        } else {
            Err(JsonRpcError::forbidden(
                "log filter must name only contracts whose logs the authenticated account may read, or pin it in a topic position",
            ))
        }
    }

    pub fn pins_signer(&self, filter: &Filter) -> bool {
        let signer_topic = self.signer.into_word();
        filter
            .topics
            .iter()
            .any(|topic| topic.len() == 1 && topic.contains(&signer_topic))
    }

    /// Whether the signer may read `log`: its contract is in `readable` or the signer is one of
    /// its topics.
    pub fn may_read_log(&self, log: &Log, readable: &HashSet<Address>) -> bool {
        self.operator
            || readable.contains(&log.address())
            || log.topics().contains(&self.signer.into_word())
    }

    /// Removes the logs the signer may not read and rebuilds the bloom from the rest.
    pub fn redact_receipt(&self, receipt: &mut TransactionReceipt, readable: &HashSet<Address>) {
        if let Some(inner) = receipt.inner.as_receipt_with_bloom_mut() {
            inner
                .receipt
                .logs
                .retain(|log| self.may_read_log(log, readable));
            inner.logs_bloom = logs_bloom(inner.receipt.logs.iter().map(|log| &log.inner));
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{Receipt, ReceiptEnvelope};
    use alloy_primitives::{address, b256};
    use monad_eth_testutil::make_representable_domain;
    use monad_ethcall::overrides::StateOverrideObject;
    use monad_triedb_utils::mock_triedb::MockTriedb;

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
        let none = HashSet::new();

        for filter in [
            Filter::new().topic1(signer_topic),
            Filter::new().topic3(signer_topic),
            Filter::new()
                .address(OTHER)
                .topic1(other_topic)
                .topic2(signer_topic),
        ] {
            assert!(user().require_logs_filter_access(&filter, &none).is_ok());
        }

        // No topics, another account, a set that also matches other accounts, or the signer
        // only in the address filter are all rejected.
        for filter in [
            Filter::new(),
            Filter::new().topic1(other_topic),
            Filter::new().topic1(vec![signer_topic, other_topic]),
            Filter::new().address(SIGNER),
        ] {
            assert_forbidden(user().require_logs_filter_access(&filter, &none));
        }

        assert!(operator()
            .require_logs_filter_access(&Filter::new(), &none)
            .is_ok());
    }

    #[test]
    fn log_filter_may_name_only_readable_contracts() {
        const CONTRACT: Address = address!("3333333333333333333333333333333333333333");
        let readable = HashSet::from([CONTRACT, OTHER]);

        for filter in [
            Filter::new().address(CONTRACT),
            Filter::new().address(vec![CONTRACT, OTHER]),
            Filter::new().address(CONTRACT).topic1(OTHER.into_word()),
        ] {
            assert!(user()
                .require_logs_filter_access(&filter, &readable)
                .is_ok());
        }

        // Every named contract must be readable; naming none matches every contract.
        for filter in [
            Filter::new().address(vec![CONTRACT, SIGNER]),
            Filter::new().topic1(OTHER.into_word()),
        ] {
            assert_forbidden(user().require_logs_filter_access(&filter, &readable));
        }
    }

    #[test]
    fn receipt_keeps_only_readable_logs() {
        const CONTRACT: Address = address!("3333333333333333333333333333333333333333");
        let log = |address: Address, topics: Vec<B256>| Log {
            inner: alloy_primitives::Log::new_unchecked(address, topics, Default::default()),
            ..Default::default()
        };
        let own_contract = log(CONTRACT, vec![B256::ZERO]);
        let names_signer = log(OTHER, vec![B256::ZERO, SIGNER.into_word()]);
        let hidden = log(OTHER, vec![B256::ZERO, OTHER.into_word()]);
        let receipt = TransactionReceipt {
            inner: ReceiptEnvelope::Eip1559(
                Receipt {
                    status: true.into(),
                    cumulative_gas_used: 0,
                    logs: vec![own_contract.clone(), hidden.clone(), names_signer.clone()],
                }
                .with_bloom(),
            ),
            transaction_hash: B256::ZERO,
            transaction_index: None,
            block_hash: None,
            block_number: None,
            gas_used: 0,
            effective_gas_price: 0,
            blob_gas_used: None,
            blob_gas_price: None,
            from: SIGNER,
            to: None,
            contract_address: None,
        };
        let readable = HashSet::from([CONTRACT]);

        let mut redacted = receipt.clone();
        user().redact_receipt(&mut redacted, &readable);
        let kept = vec![own_contract, names_signer];
        assert_eq!(redacted.inner.logs(), kept.as_slice());
        assert_eq!(
            *redacted.inner.logs_bloom(),
            logs_bloom(kept.iter().map(|log| &log.inner))
        );

        let mut unredacted = receipt.clone();
        operator().redact_receipt(&mut unredacted, &HashSet::new());
        assert_eq!(unredacted, receipt);
    }

    #[tokio::test]
    async fn log_readers_come_from_spoke_storage() {
        let domain = make_representable_domain(1);
        let spoke = address!("5fbdb2315678afecb367f032d93f642f64180aa3");
        let contract = address!("3333333333333333333333333333333333333333");
        let outsider = address!("4444444444444444444444444444444444444444");
        // Solidity's slots of policyOwners[contract] and logReaders[contract][OTHER].
        let owner_slot = b256!("c042e0f5eb0064bf1d9722ec18e39f827a2cf491b49d2d4c34a5ecf7a61b44b7");
        let reader_slot = b256!("b9e4a128383cc3e46eea650eb352c18ce6fca35b0a98251e055df30242b5c869");
        let mut triedb = MockTriedb::default();
        let account = AccountKey::domain(domain, spoke);
        triedb.set_storage_at_by_key(account, owner_slot.0, SIGNER.into_word().0);
        triedb.set_storage_at_by_key(account, reader_slot.0, B256::with_last_byte(1).0);

        for (reader, expected) in [
            (SIGNER, true),
            (OTHER, true),
            (outsider, false),
            (Address::ZERO, false),
        ] {
            let allowed = is_log_reader(&triedb, domain, spoke, contract, reader).await;
            assert_eq!(allowed, Ok(expected), "{reader}");
        }
        let other_domain = make_representable_domain(2);
        let allowed = is_log_reader(&triedb, other_domain, spoke, contract, SIGNER).await;
        assert_eq!(allowed, Ok(false));
    }
}
