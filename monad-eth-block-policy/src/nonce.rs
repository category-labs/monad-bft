use std::collections::BTreeMap;

use monad_eth_types::{Balance, EthAccount, EthAddress, Nonce};

use crate::{AccountNonceRetrievable, StateBackend};

#[derive(Debug, Clone)]
pub struct InMemoryState {
    account_nonces: BTreeMap<EthAddress, Nonce>,
    /// InMemoryState doesn't have access to an execution engine. It returns
    /// `max_reserve_balance` as the balance every time so txn reserve balance
    /// will pass if the sum doesn't exceed the max reserve
    max_reserve_balance: Balance,
}

impl Default for InMemoryState {
    fn default() -> Self {
        Self {
            account_nonces: Default::default(),
            max_reserve_balance: Balance::MAX,
        }
    }
}

impl InMemoryState {
    pub fn new(
        existing_nonces: impl IntoIterator<Item = (EthAddress, Nonce)>,
        max_reserve_balance: Balance,
    ) -> Self {
        Self {
            account_nonces: existing_nonces.into_iter().collect(),
            max_reserve_balance,
        }
    }

    pub fn update_committed_nonces<'a>(&mut self, block: impl AccountNonceRetrievable) {
        let nonces = block.get_account_nonces();

        for (address, account_nonce) in nonces {
            self.account_nonces.insert(address, account_nonce);
        }
    }
}

impl StateBackend for InMemoryState {
    fn get_account(&self, eth_address: &EthAddress, _block: u64) -> Option<EthAccount> {
        if let Some(nonce) = self.account_nonces.get(eth_address) {
            return Some(EthAccount {
                nonce: *nonce,
                balance: self.max_reserve_balance,
                code_hash: None,
            });
        }
        None
    }
}
