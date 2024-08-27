use std::{
    cmp::Ordering,
    ffi::CString,
    path::Path,
    ptr::{null, null_mut},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

use alloy_primitives::{keccak256, B256};
use alloy_rlp::Decodable;
use futures::{
    channel::oneshot::{self, Receiver, Sender},
    executor::block_on,
    future::join_all,
    FutureExt,
};
use key::create_addr_key;
use monad_eth_types::{EthAccount, EthAddress};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;
use tracing::{debug, error, warn};

pub mod key;

mod bindings {
    include!(concat!(env!("OUT_DIR"), "/triedb.rs"));
}

const STATE_NIBBLE: u8 = 0x0;
const MAX_TRIEDB_ASYNC_POLLS: u32 = 320_000;

// TODO: rename to TriedbHandle
#[derive(Clone, Debug)]
pub struct Handle {
    db_ptr: *mut bindings::triedb,
}

pub struct SenderContext {
    sender: Sender<Option<Vec<u8>>>,
    completed_counter: Arc<AtomicUsize>,
}

/// # Safety
/// This should be used only as a callback for async TrieDB calls
///
/// This function is called by TrieDB once it proceses a single read async call
pub unsafe extern "C" fn read_async_callback(
    value_ptr: *const u8,
    value_len: i32,
    sender_context: *mut std::ffi::c_void,
) {
    // Unwrap the sender context struct
    let sender_context = unsafe { Box::from_raw(sender_context as *mut SenderContext) };
    // Increment the completed counter
    sender_context.completed_counter.fetch_add(1, SeqCst);

    let result = match value_len.cmp(&0) {
        Ordering::Less => None,
        Ordering::Equal => Some(Vec::new()),
        Ordering::Greater => {
            let value =
                unsafe { std::slice::from_raw_parts(value_ptr, value_len as usize).to_vec() };
            unsafe { bindings::triedb_finalize(value_ptr) };
            Some(value)
        }
    };

    // Send the retrieved result through the channel
    let sender_result = sender_context.sender.send(result);
    assert!(sender_result.is_ok());
}

impl Handle {
    pub fn rlp_decode_account(account_rlp: Vec<u8>) -> Option<EthAccount> {
        let mut buf = account_rlp.as_slice();
        let Ok(mut buf) = alloy_rlp::Header::decode_bytes(&mut buf, true) else {
            error!("rlp decode failed: {:?}", buf);
            return None;
        };

        // address (currently not needed)
        let Ok(_) = <[u8; 20]>::decode(&mut buf) else {
            error!("rlp address decode failed: {:?}", buf);
            return None;
        };

        // account incarnation decode (currently not needed)
        let Ok(_) = u64::decode(&mut buf) else {
            error!("rlp incarnation decode failed: {:?}", buf);
            return None;
        };

        let Ok(nonce) = u64::decode(&mut buf) else {
            error!("rlp nonce decode failed: {:?}", buf);

            return None;
        };
        let Ok(balance) = u128::decode(&mut buf) else {
            error!("rlp balance decode failed: {:?}", buf);
            return None;
        };

        let code_hash = if buf.is_empty() {
            None
        } else {
            match <[u8; 32]>::decode(&mut buf) {
                Ok(x) => Some(x),
                Err(e) => {
                    error!("rlp code_hash decode failed: {:?}", e);
                    return None;
                }
            }
        };

        Some(EthAccount {
            nonce,
            balance,
            code_hash: code_hash.map(B256::from),
        })
    }

    pub fn try_new(dbdir_path: &Path) -> Option<Self> {
        let path = CString::new(dbdir_path.to_str().expect("invalid path"))
            .expect("failed to create CString");

        let mut db_ptr = null_mut();

        let result = unsafe { bindings::triedb_open(path.as_c_str().as_ptr(), &mut db_ptr) };

        if result != 0 {
            debug!("triedb try_new error result: {}", result);
            return None;
        }

        Some(Self { db_ptr })
    }

    fn create_addr_key(addr: &EthAddress) -> (Vec<u8>, u8) {
        let mut key_nibbles: Vec<u8> = vec![];

        let state_nibble = 0_u8;

        key_nibbles.push(state_nibble);

        let hashed_addr = keccak256(addr);
        for byte in &hashed_addr {
            key_nibbles.push(*byte >> 4);
            key_nibbles.push(*byte & 0xF);
        }

        let num_nibbles: u8 = key_nibbles.len().try_into().expect("key too big");
        if num_nibbles % 2 != 0 {
            key_nibbles.push(0);
        }
        let key: Vec<_> = key_nibbles
            .chunks(2)
            .map(|chunk| (chunk[0] << 4) | chunk[1])
            .collect();

        (key, num_nibbles)
    }

    pub fn read(&self, key: &[u8], key_len_nibbles: u8, block_id: u64) -> Option<Vec<u8>> {
        let mut value_ptr = null();
        assert!(key_len_nibbles < u8::MAX - 1); // make sure doesn't overflow
        assert!((key_len_nibbles as usize + 1) / 2 <= key.len());
        let result = unsafe {
            bindings::triedb_read(
                self.db_ptr,
                key.as_ptr(),
                key_len_nibbles,
                &mut value_ptr,
                block_id,
            )
        };
        if result == -1 {
            return None;
        }

        if result == 0 {
            return Some(Vec::new());
        }

        // check that there's no unexpected error
        assert!(result > 0);

        let value_len = result.try_into().unwrap();
        let value = unsafe {
            let value = std::slice::from_raw_parts(value_ptr, value_len).to_vec();
            bindings::triedb_finalize(value_ptr);
            value
        };

        Some(value)
    }

    fn triedb_poll(&self) {
        let _io_count = unsafe { bindings::triedb_poll(self.db_ptr, true, usize::MAX) };
    }

    /// This is used to make an async read call to TrieDB.
    /// It creates a oneshot channel and Boxes its sender and the completed_counter
    /// into a context struct and passes it to TrieDB. When TrieDB completes processing
    /// the call, it will call the `read_async_callback` which will unwrap the context
    /// struct, increment the completed_counter, and send the retrieved TrieDB value
    /// through the channel.
    /// The user needs to poll TrieDB using the `triedb_poll` function to pump the async
    /// reads and wait on the returned receiver for the value.
    /// NOTE: the returned receiver must be resolved before key is dropped
    fn read_async(
        &self,
        key: &[u8],
        key_len_nibbles: u8,
        block_id: u64,
        completed_counter: Arc<AtomicUsize>,
    ) -> Receiver<Option<Vec<u8>>> {
        assert!(key_len_nibbles < u8::MAX - 1); // make sure doesn't overflow
        assert!((key_len_nibbles as usize + 1) / 2 <= key.len());

        // Create a channel to send the value from TrieDB
        let (sender, receiver) = oneshot::channel();
        // Wrap the sender and completed_counter in a context struct
        let sender_context = Box::new(SenderContext {
            sender,
            completed_counter,
        });

        unsafe {
            // Convert the struct into a raw pointer which will be sent to the callback function
            let sender_context_ptr = Box::into_raw(sender_context);

            bindings::triedb_async_read(
                self.db_ptr,
                key.as_ptr(),
                key_len_nibbles,
                block_id,
                Some(read_async_callback), // TrieDB read async callback
                sender_context_ptr as *mut std::ffi::c_void,
            );
        }

        receiver
    }

    pub fn get_state_root(&self, block_id: u64) -> Option<Vec<u8>> {
        let key: Vec<u8> = vec![STATE_NIBBLE];
        let mut value_ptr = null();
        let result = unsafe {
            bindings::triedb_read_data(self.db_ptr, key.as_ptr(), 1, &mut value_ptr, block_id)
        };
        if result == -1 {
            return None;
        }

        if result == 0 {
            return Some(Vec::new());
        }

        // check that there's no unexpected error
        assert_eq!(result, 32);

        let value_len = result.try_into().unwrap();
        let value = unsafe {
            let value = std::slice::from_raw_parts(value_ptr, value_len).to_vec();
            bindings::triedb_finalize(value_ptr);
            value
        };

        Some(value)
    }

    pub fn earliest_block(&self) -> u64 {
        unsafe { bindings::triedb_earliest_block(self.db_ptr) }
    }

    pub fn latest_block(&self) -> u64 {
        unsafe { bindings::triedb_latest_block(self.db_ptr) }
    }

    pub fn get_account(&self, eth_address: &[u8; 20], block_id: u64) -> Option<EthAccount> {
        let (triedb_key, key_len_nibbles) = create_addr_key(eth_address);

        let result = self.read(&triedb_key, key_len_nibbles, block_id);

        let Some(account_rlp) = result else {
            debug!("account {:?} not found at {:?}", eth_address, block_id);
            return None;
        };

        Handle::rlp_decode_account(account_rlp)
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
            self.read_async(
                triedb_key.as_ref(),
                key_len_nibbles,
                block_id,
                completed_counter.clone(),
            )
            .map(|receiver_result| {
                // Receiver should not fail
                let maybe_rlp_account = receiver_result.expect("receiver can't be canceled");
                // RLP decode the received account value
                maybe_rlp_account.and_then(Handle::rlp_decode_account)
            })
        });

        // Join all futures of receivers
        let eth_account_receivers = join_all(eth_account_receivers);

        let mut poll_count = 0;
        // Poll TrieDB until the completed_counter reaches num_accounts
        while completed_counter.load(SeqCst) < num_accounts && poll_count < MAX_TRIEDB_ASYNC_POLLS {
            self.triedb_poll();
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
}

impl Drop for Handle {
    fn drop(&mut self) {
        let result = unsafe { bindings::triedb_close(self.db_ptr) };
        assert_eq!(result, 0);
    }
}

impl StateBackend for Handle {
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
        SeqNum(self.earliest_block())
    }

    fn raw_read_latest_block(&self) -> SeqNum {
        SeqNum(self.latest_block())
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::Handle;

    #[test]
    fn read() {
        let handle = Handle::try_new(Path::new("/dummy")).unwrap();

        // this key is hardcoded into mock triedb
        let result = handle.read(&[1, 2, 3], 6, 0);
        assert_eq!(result, Some(vec![4, 5, 6]));

        // this key is hardcoded into mock triedb
        let result = handle.read(&[7, 8, 9], 6, 0);
        assert_eq!(result, Some(vec![10, 11, 12]));

        let result = handle.read(&[0], 2, 0);
        assert_eq!(result, None);
    }

    #[test]
    #[should_panic]
    fn read_invalid() {
        let handle = Handle::try_new(Path::new("/dummy")).unwrap();

        // too many nibbles
        let _ = handle.read(&[1, 2, 3], 7, 0);
    }

    #[test]
    fn read_latest_block() {
        let handle = Handle::try_new(Path::new("/dummy")).unwrap();

        // this value is hardcoded into mock triedb
        let result = handle.latest_block();
        assert_eq!(result, 20);
    }
}
