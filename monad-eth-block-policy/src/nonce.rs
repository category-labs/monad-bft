use std::collections::BTreeMap;

use monad_consensus_types::state::StateBackend;
use monad_eth_types::{EthAddress, Nonce};

#[derive(Debug, Default)]
pub struct InMemoryState {
    account_nonces: BTreeMap<EthAddress, Nonce>,
}

impl InMemoryState {
    pub fn new(existing_nonces: impl IntoIterator<Item = (EthAddress, Nonce)>) -> Self {
        Self {
            account_nonces: existing_nonces.into_iter().collect(),
        }
    }
}

impl StateBackend for InMemoryState {
    fn get_account_nonce(&self, eth_address: &EthAddress, _block_number: u64) -> Option<Nonce> {
        self.account_nonces.get(eth_address).copied()
    }

    fn update_committed_nonces<'a>(
        &mut self,
        nonces: impl IntoIterator<Item = (&'a EthAddress, &'a Nonce)>,
    ) {
        for (&address, &account_nonce) in nonces {
            self.account_nonces.insert(address, account_nonce);
        }
    }
}
