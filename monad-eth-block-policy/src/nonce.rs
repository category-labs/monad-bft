use std::collections::BTreeMap;

use monad_eth_types::{Balance, EthAccount, EthAddress, Nonce};
use monad_types::{SeqNum, GENESIS_SEQ_NUM};
use tracing::trace;

use crate::StateBackend;

#[derive(Debug, Clone)]
pub struct InMemoryState {
    account_nonces: BTreeMap<EthAddress, Nonce>,
    /// InMemoryState doesn't have access to an execution engine. It returns
    /// `max_reserve_balance` as the balance every time so txn reserve balance
    /// will pass if the sum doesn't exceed the max reserve
    max_reserve_balance: Balance,
    last_state: u64,
}

impl Default for InMemoryState {
    fn default() -> Self {
        Self {
            account_nonces: Default::default(),
            max_reserve_balance: Balance::MAX,
            last_state: GENESIS_SEQ_NUM.0,
        }
    }
}

impl InMemoryState {
    pub fn new(
        existing_nonces: impl IntoIterator<Item = (EthAddress, Nonce)>,
        max_reserve_balance: Balance,
        last_state: u64,
    ) -> Self {
        Self {
            account_nonces: existing_nonces.into_iter().collect(),
            max_reserve_balance,
            last_state,
        }
    }

    // new_account_nonces is the changeset of nonces from a given block
    // if account A's last tx nonce in a block is N, then new_account_nonces should include A=N+1
    // this is because N+1 is the next valid nonce for A
    pub fn update_committed_nonces(
        &mut self,
        seq_num: SeqNum,
        new_account_nonces: BTreeMap<EthAddress, Nonce>,
    ) {
        for (address, account_nonce) in new_account_nonces {
            self.account_nonces.insert(address, account_nonce);
        }
        self.last_state = seq_num.0;
    }
}

impl StateBackend for InMemoryState {
    fn get_account(&self, block: u64, eth_address: &EthAddress) -> Option<EthAccount> {
        assert!(block <= self.last_state);
        if let Some(nonce) = self.account_nonces.get(eth_address) {
            return Some(EthAccount {
                nonce: *nonce,
                balance: self.max_reserve_balance,
                code_hash: None,
            });
        }
        None
    }

    fn is_available(&self, block: u64) -> bool {
        trace!(?self.last_state,?block,"InMemoryState is_available");
        self.last_state >= block
    }
}
