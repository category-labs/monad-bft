use std::collections::BTreeMap;

use monad_consensus_types::{block::BlockType, signature_collection::SignatureCollection};
use monad_eth_types::{Balance, EthAccount, EthAddress, Nonce};
use monad_types::GENESIS_SEQ_NUM;
use tracing::trace;

use crate::{AccountNonceRetrievable, StateBackend};

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

    pub fn update_committed_nonces<SCT: SignatureCollection>(
        &mut self,
        block: impl AccountNonceRetrievable + BlockType<SCT>,
    ) {
        let nonces = block.get_account_nonces();

        for (address, account_nonce) in nonces {
            self.account_nonces.insert(address, account_nonce);
        }
        self.last_state = block.get_seq_num().0;
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
