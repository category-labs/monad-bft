use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

use decode::rlp_decode_storage_slot;
use futures::{executor::block_on, future::join_all, FutureExt};
use monad_eth_types::{EthAccount, EthAddress};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_triedb::Handle;
use monad_types::SeqNum;
use tracing::{debug, warn};

use crate::{
    decode::rlp_decode_account,
    key::{create_addr_key, create_code_key, create_receipt_key, create_storage_at_key},
};

pub mod decode;
pub mod key;

const MAX_TRIEDB_ASYNC_POLLS: u32 = 320_000;

#[derive(Clone)]
pub struct TriedbReader {
    handle: Handle,
}

impl TriedbReader {
    pub fn try_new(triedb_path: &Path) -> Option<Self> {
        Handle::try_new(triedb_path).map(|handle| Self { handle })
    }

    pub fn get_latest_block(&self) -> u64 {
        self.handle.latest_block()
    }

    pub fn get_account(&self, eth_address: &[u8; 20], block_id: u64) -> Option<EthAccount> {
        let (triedb_key, key_len_nibbles) = create_addr_key(eth_address);

        let result = self.handle.read(&triedb_key, key_len_nibbles, block_id);

        let Some(account_rlp) = result else {
            debug!("account {:?} not found at {:?}", eth_address, block_id);
            return None;
        };

        rlp_decode_account(account_rlp)
    }

    pub fn get_accounts_async<'a>(
        &self,
        eth_addresses: impl Iterator<Item = &'a EthAddress>,
        block_id: u64,
    ) -> Option<Vec<Option<EthAccount>>> {
        // Counter which is updated when TrieDB processes a single async read to completion
        let completed_counter = Arc::new(AtomicUsize::new(0));

        let mut num_accounts = 0;
        let eth_account_receivers = eth_addresses.map(|eth_address| {
            num_accounts += 1;
            let (triedb_key, key_len_nibbles) = create_addr_key(eth_address.as_ref());
            self.handle
                .read_async(
                    triedb_key.as_ref(),
                    key_len_nibbles,
                    block_id,
                    completed_counter.clone(),
                )
                .map(|receiver_result| {
                    // Receiver should not fail
                    let maybe_rlp_account = receiver_result.expect("receiver can't be canceled");
                    // RLP decode the received account value
                    maybe_rlp_account.and_then(rlp_decode_account)
                })
        });

        // Join all futures of receivers
        let eth_account_receivers = join_all(eth_account_receivers);

        let mut poll_count = 0;
        // Poll TrieDB until the completed_counter reaches num_accounts
        while completed_counter.load(SeqCst) < num_accounts && poll_count < MAX_TRIEDB_ASYNC_POLLS {
            self.handle.triedb_poll();
            poll_count += 1;
        }
        // TrieDB should have completed processing all the async callbacks at this point
        if completed_counter.load(SeqCst) != num_accounts {
            warn!(
                "TrieDB poll count went over limit for {} lookups",
                num_accounts
            );
            return None;
        }

        Some(block_on(eth_account_receivers))
    }

    pub fn get_storage_at(
        &self,
        eth_address: &[u8; 20],
        storage_key: &[u8; 32],
        block_id: u64,
    ) -> Option<[u8; 32]> {
        let (triedb_key, key_len_nibbles) = create_storage_at_key(eth_address, storage_key);

        let result = self.handle.read(&triedb_key, key_len_nibbles, block_id);

        let Some(storage_rlp) = result else {
            debug!("storage {:?} not found at {:?}", eth_address, block_id);
            return None;
        };

        rlp_decode_storage_slot(storage_rlp)
    }

    pub fn get_code(&self, code_hash: &[u8; 32], block_id: u64) -> Option<Vec<u8>> {
        let (triedb_key, key_len_nibbles) = create_code_key(code_hash);

        self.handle.read(&triedb_key, key_len_nibbles, block_id)
    }

    pub fn get_receipt(&self, txn_index: u64, block_id: u64) -> Option<Vec<u8>> {
        let (triedb_key, key_len_nibbles) = create_receipt_key(txn_index);

        self.handle.read(&triedb_key, key_len_nibbles, block_id)
    }
}

impl StateBackend for TriedbReader {
    fn get_account_statuses<'a>(
        &self,
        block: SeqNum,
        eth_addresses: impl Iterator<Item = &'a EthAddress>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let latest = self.raw_read_latest_block();
        if latest < block {
            // latest < block
            return Err(StateBackendError::NotAvailableYet);
        }
        // block <= latest

        let earliest = self.raw_read_earliest_block();
        if block < earliest {
            // block < earliest
            return Err(StateBackendError::NeverAvailable);
        }
        // block >= earliest

        let Some(statuses) = self.get_accounts_async(eth_addresses, block.0) else {
            // TODO: Use a more descriptive error
            return Err(StateBackendError::NotAvailableYet);
        };

        // all accounts are now guaranteed to be fully consistent and correct
        Ok(statuses)
    }

    fn raw_read_account(&self, block: SeqNum, address: &EthAddress) -> Option<EthAccount> {
        self.get_account(address.as_ref(), block.0)
    }

    fn raw_read_earliest_block(&self) -> SeqNum {
        SeqNum(self.handle.earliest_block())
    }

    fn raw_read_latest_block(&self) -> SeqNum {
        SeqNum(self.handle.latest_block())
    }
}
