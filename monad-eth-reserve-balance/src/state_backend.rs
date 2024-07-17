use std::sync::{Arc, Mutex};

use monad_eth_types::{EthAccount, EthAddress};

/// Backend provider of account data: balance and nonce
pub trait StateBackend {
    fn get_account(&self, block: u64, eth_address: &EthAddress) -> Option<EthAccount>;
    fn is_available(&self, block: u64) -> bool;
}

#[derive(Debug, Default, Clone)]
pub struct NopStateBackend;

impl StateBackend for NopStateBackend {
    fn get_account(&self, _block: u64, _eth_address: &EthAddress) -> Option<EthAccount> {
        None
    }

    fn is_available(&self, _block: u64) -> bool {
        true
    }
}

impl<T: StateBackend> StateBackend for Arc<Mutex<T>> {
    fn get_account(&self, block: u64, eth_address: &EthAddress) -> Option<EthAccount> {
        let state = self.lock().unwrap();
        state.get_account(block, eth_address)
    }

    fn is_available(&self, block: u64) -> bool {
        let state = self.lock().unwrap();
        state.is_available(block)
    }
}
