use monad_eth_types::{EthAddress, Nonce};
use monad_triedb::Handle as TriedbHandle;
use tracing::debug;

/// Backend provider of account nonce and balance
pub trait StateBackend {
    fn get_account_nonce(&self, eth_address: &EthAddress, block_number: u64) -> Option<Nonce>;
    fn update_committed_nonces<'a>(
        &mut self,
        nonces: impl IntoIterator<Item = (&'a EthAddress, &'a Nonce)>,
    );
}

#[derive(Debug, Default, Clone)]
pub struct NopStateBackend;

impl StateBackend for NopStateBackend {
    fn get_account_nonce(&self, _eth_address: &EthAddress, _block_number: u64) -> Option<Nonce> {
        None
    }

    fn update_committed_nonces<'a>(
        &mut self,
        _nonces: impl IntoIterator<Item = (&'a EthAddress, &'a Nonce)>,
    ) {
    }
}

impl StateBackend for TriedbHandle {
    fn get_account_nonce(&self, eth_address: &EthAddress, block_number: u64) -> Option<Nonce> {
        debug!("reading account nonce from triedb");
        self.get_account(eth_address.as_ref(), block_number)
            .map(|account| account.nonce)
    }

    fn update_committed_nonces<'a>(
        &mut self,
        _nonces: impl IntoIterator<Item = (&'a EthAddress, &'a Nonce)>,
    ) {
    }
}
