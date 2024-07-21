use monad_eth_types::{EthAccount, EthAddress, Nonce};

/// Backend provider of account nonce
pub trait StateBackend {
    fn get_account(&self, eth_address: &EthAddress, block: u64) -> Option<EthAccount>;

    // FIXME: to deprecate
    fn update_committed_nonces<'a>(
        &mut self,
        nonces: impl IntoIterator<Item = (&'a EthAddress, &'a Nonce)>,
    );
}

#[derive(Debug, Default, Clone)]
pub struct NopStateBackend;

impl StateBackend for NopStateBackend {
    fn get_account(&self, _eth_address: &EthAddress, _block: u64) -> Option<EthAccount> {
        None
    }

    fn update_committed_nonces<'a>(
        &mut self,
        _nonces: impl IntoIterator<Item = (&'a EthAddress, &'a Nonce)>,
    ) {
    }
}

// impl StateBackend for TriedbHandle {
//     fn get_account(&self, eth_address: &EthAddress, block: u64) -> Option<EthAccount> {
//         self.get_account(eth_address.as_ref(), block)
//     }

//     fn update_committed_nonces<'a>(
//         &mut self,
//         _nonces: impl IntoIterator<Item = (&'a EthAddress, &'a Nonce)>,
//     ) {
//     }
// }
