use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

use alloy_consensus::Header;
use alloy_primitives::Address;
use alloy_rlp::Decodable;
use futures::{channel::oneshot, executor::block_on, future::join_all, FutureExt};
use key::Version;
use monad_eth_types::{EthAccount, EthHeader};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_triedb::TriedbHandle;
use monad_types::SeqNum;
use tracing::{debug, warn};

use crate::{
    decode::rlp_decode_account,
    key::{create_triedb_key, KeyInput},
};

pub mod decode;
pub mod key;
pub mod metrics;
pub mod triedb_env;

const MAX_TRIEDB_ASYNC_POLLS: usize = 640_000;

#[derive(Clone)]
pub struct TriedbReader {
    handle: TriedbHandle,
}

impl TriedbReader {
    pub fn try_new(triedb_path: &Path) -> Option<Self> {
        TriedbHandle::try_new(triedb_path).map(|handle| Self { handle })
    }

    pub fn get_latest_finalized_block(&self) -> Option<SeqNum> {
        let latest_finalized = self.handle.latest_finalized_block()?;
        Some(SeqNum(latest_finalized))
    }

    pub fn get_earliest_finalized_block(&self) -> Option<SeqNum> {
        let earliest_finalized = self.handle.earliest_finalized_block()?;
        Some(SeqNum(earliest_finalized))
    }

    pub fn get_account_finalized(
        &self,
        seq_num: &SeqNum,
        eth_address: &[u8; 20],
    ) -> Option<EthAccount> {
        self.get_account(seq_num, Version::Finalized, eth_address)
    }

    pub fn get_finalized_eth_header(&self, seq_num: &SeqNum) -> Option<EthHeader> {
        if self
            .raw_read_latest_block()
            .is_some_and(|latest_finalized| seq_num <= &latest_finalized)
        {
            let (triedb_key, key_len_nibbles) =
                create_triedb_key(Version::Finalized, KeyInput::BlockHeader);
            let eth_header_bytes = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0)?;
            let mut rlp_buf = eth_header_bytes.as_slice();
            let block_header = Header::decode(&mut rlp_buf).expect("invalid rlp eth header");

            Some(EthHeader(block_header))
        } else {
            None
        }
    }

    pub fn get_account(
        &self,
        seq_num: &SeqNum,
        version: Version,
        eth_address: &[u8; 20],
    ) -> Option<EthAccount> {
        let (triedb_key, key_len_nibbles) =
            create_triedb_key(version, KeyInput::Address(eth_address));

        let result = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0);

        let Some(account_rlp) = result else {
            debug!(?seq_num, ?eth_address, "account not found");
            return None;
        };

        rlp_decode_account(account_rlp)
    }

    pub fn get_accounts_async<'a>(
        &self,
        seq_num: &SeqNum,
        version: Version,
        eth_addresses: impl Iterator<Item = &'a Address>,
    ) -> Option<Vec<Option<EthAccount>>> {
        // Counter which is updated when TrieDB processes a single async read to completion
        let completed_counter = Arc::new(AtomicUsize::new(0));
        let mut num_accounts = 0;
        let eth_account_receivers = eth_addresses.map(|eth_address| {
            num_accounts += 1;
            let (triedb_key, key_len_nibbles) =
                create_triedb_key(version, KeyInput::Address(eth_address.as_ref()));
            let (sender, receiver) = oneshot::channel();
            self.handle.read_async(
                triedb_key.as_ref(),
                key_len_nibbles,
                seq_num.0,
                completed_counter.clone(),
                sender,
            );
            receiver.map(|receiver_result| {
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
        // TODO: if initiating the reads had an error, this call to triedb_poll will not terminate.
        //       wrap this loop in a timeout so consensus doesn't get stuck
        while completed_counter.load(SeqCst) < num_accounts && poll_count < MAX_TRIEDB_ASYNC_POLLS {
            // blocking = true => wait to do more work
            // max_completions = usize::MAX => process as many completions before returning
            poll_count += self.handle.triedb_poll(true, usize::MAX);
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
}

impl StateBackend for TriedbReader {
    fn get_account_statuses<'a>(
        &self,
        block: SeqNum,
        eth_addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let Some(latest) = self.raw_read_latest_block() else {
            return Err(StateBackendError::NotAvailableYet);
        };
        if latest < block {
            // latest < block
            return Err(StateBackendError::NotAvailableYet);
        }
        // block <= latest

        let Some(statuses) = self.get_accounts_async(&block, Version::Finalized, eth_addresses)
        else {
            // TODO: Use a more descriptive error
            return Err(StateBackendError::NotAvailableYet);
        };

        let earliest = self
            .raw_read_earliest_block()
            .expect("earliest must exist if latest does");
        if block < earliest {
            // block < earliest
            return Err(StateBackendError::NeverAvailable);
        }

        // all accounts are now guaranteed to be fully consistent and correct
        Ok(statuses)
    }

    fn raw_read_account(&self, block: SeqNum, address: &Address) -> Option<EthAccount> {
        self.get_account_finalized(&block, address.as_ref())
    }

    fn raw_read_earliest_block(&self) -> Option<SeqNum> {
        self.get_earliest_finalized_block()
    }

    fn raw_read_latest_block(&self) -> Option<SeqNum> {
        self.get_latest_finalized_block()
    }
}
