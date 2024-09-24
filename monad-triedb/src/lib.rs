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

use alloy_primitives::private::alloy_rlp::Decodable;
use futures::channel::oneshot::{self, Receiver, Sender};
use tracing::debug;

#[allow(dead_code, non_camel_case_types, non_upper_case_globals)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/triedb.rs"));
}

const STATE_NIBBLE: u8 = 0x0;

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

#[derive(Debug)]
pub struct Storage {
    data: std::sync::Mutex<Vec<([u8; 32], [u8; 32])>>,
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn callback(context: *mut std::ffi::c_void, value_ptr: *const u8, value_len: usize) {
    let storage = unsafe { &*(context as *mut Storage) };

    let value = unsafe {
        let value = std::slice::from_raw_parts(value_ptr, value_len).to_vec();
        value
    };

    // Expected storage value: rlp::list(rlp::zeroless_view(key), rlp::zeroless_view(value)
    let Ok(res) = <Vec<alloy_primitives::U256>>::decode(&mut value.as_slice()) else {
        return;
    };

    if res.len() != 2 {
        return;
    }

    let mut res = res.into_iter();

    let mut key_value = [0_u8; 32];
    for (byte, key) in res
        .next()
        .expect("expect key in rlp list")
        .to_be_bytes_vec()
        .into_iter()
        .zip(key_value.iter_mut())
    {
        *key = byte;
    }

    let mut storage_value = [0_u8; 32];
    for (byte, storage) in res
        .next()
        .expect("expect value in rlp list")
        .to_be_bytes_vec()
        .into_iter()
        .zip(storage_value.iter_mut())
    {
        *storage = byte;
    }

    storage
        .data
        .lock()
        .expect("acquire lock")
        .push((key_value, storage_value));
}

impl Handle {
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

    pub fn traverse(
        &self,
        key: &[u8],
        key_len_nibbles: u8,
        block_id: u64,
    ) -> Option<Vec<([u8; 32], [u8; 32])>> {
        assert!(key_len_nibbles < u8::MAX - 1);
        assert!((key_len_nibbles as usize + 1) / 2 <= key.len());

        let storage = Arc::new(Storage {
            data: std::sync::Mutex::new(Vec::new()),
        });

        let result = unsafe {
            let context = std::sync::Arc::into_raw(storage) as *mut std::ffi::c_void;
            bindings::triedb_traverse_state(
                self.db_ptr,
                key.as_ptr(),
                key_len_nibbles,
                block_id,
                context,
                Some(callback),
            );

            std::sync::Arc::from_raw(context as *const Storage)
        };

        let mut storage_slots = Vec::new();

        match result.data.lock().ok() {
            Some(data) => {
                for (key, value) in data.iter() {
                    storage_slots.push((*key, *value));
                }
            }
            None => return None,
        }

        Some(storage_slots)
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

    /// This is used to make an async read call to TrieDB.
    /// It creates a oneshot channel and Boxes its sender and the completed_counter
    /// into a context struct and passes it to TrieDB. When TrieDB completes processing
    /// the call, it will call the `read_async_callback` which will unwrap the context
    /// struct, increment the completed_counter, and send the retrieved TrieDB value
    /// through the channel.
    /// The user needs to poll TrieDB using the `triedb_poll` function to pump the async
    /// reads and wait on the returned receiver for the value.
    /// NOTE: the returned receiver must be resolved before key is dropped
    pub fn read_async(
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

    /// Used to pump async reads in TrieDB
    /// if blocking is true, the thread will sleep atleast until 1 completion is availabe to process
    /// if blocking is false, poll will return if no completion is available to process
    /// max_completions is used as a bound for maximum completions to process in this poll
    ///
    /// Returns the number of completions processed
    /// NOTE: could call poll internally: number of calls to this functions != number of completions processed
    pub fn triedb_poll(&self, blocking: bool, max_completions: usize) -> usize {
        unsafe { bindings::triedb_poll(self.db_ptr, blocking, max_completions) }
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
}

impl Drop for Handle {
    fn drop(&mut self) {
        let result = unsafe { bindings::triedb_close(self.db_ptr) };
        assert_eq!(result, 0);
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
