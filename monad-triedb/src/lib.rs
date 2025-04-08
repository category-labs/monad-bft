use std::{
    ffi::CString, fmt::{Debug, Formatter}, iter::Copied, path::Path, ptr::{null, null_mut}, sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    }
};

use futures::channel::oneshot::Sender;
use tracing::{debug, error};

#[allow(dead_code, non_camel_case_types, non_upper_case_globals)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/triedb.rs"));
}

#[derive(Clone, Debug)]
pub struct TriedbHandle {
    db_ptr: *mut bindings::triedb,
}

pub struct SenderContext {
    sender: Sender<Option<TriedbVec>>,
    completed_counter: Arc<AtomicUsize>,

    // The strong count of this dummy Arc<> reflects the total number of currently executing
    // (concurrent) requests, and this number is used by upstream code to maintain request
    // backpressure.  When this request completes, this Arc<> is implicitly dropped, which
    // causes the concurrent request count to be decremented.
    #[allow(dead_code)]
    concurrency_tracker: Arc<()>,
}

#[derive(Debug)]
pub struct TraverseContext {
    // values in traversal order
    data: std::sync::Mutex<Vec<TraverseEntry>>,
    sender: Sender<Option<Vec<TraverseEntry>>>,

    // The strong count of this dummy Arc<> reflects the total number of currently executing
    // (concurrent) requests, and this number is used by upstream code to maintain request
    // backpressure.  When this request completes, this Arc<> is implicitly dropped, which
    // causes the concurrent request count to be decremented.
    #[allow(dead_code)]
    concurrency_tracker: Arc<()>,
}

#[derive(Debug)]
pub struct TraverseEntry {
    pub key: TriedbVec,
    pub value: TriedbVec,
}

/// TriedbVec is a wrapper around a buffer returned by triedb.
pub struct TriedbVec {
    value_ptr: *const u8,
    value_len: usize,
}

unsafe impl Send for TriedbVec {}

impl TriedbVec {
    /// # Safety
    /// Internal buffer must be properly initialized and have a valid length.
    unsafe fn from_response(value_ptr: *const u8, value_len: usize) -> Self {
        Self {
            value_ptr,
            value_len,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.as_ref()
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.as_ref().to_vec()
    }

    pub fn len(&self) -> usize {
        self.value_len
    }
}

impl Drop for TriedbVec {
    fn drop(&mut self) {
        unsafe { bindings::triedb_finalize(self.value_ptr) };
    }
}

impl AsRef<[u8]> for TriedbVec {
    fn as_ref(&self) -> &[u8] {
        if self.value_len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.value_ptr, self.value_len) }
        }
    }
}

impl Debug for TriedbVec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.as_ref())
    }
}

impl<'a> IntoIterator for &'a TriedbVec {
    type Item = u8;
    // it is likely never useful to iterate over refs to bytes
    type IntoIter = Copied<std::slice::Iter<'a, u8>>;
    
    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter().copied()
    }
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
    let tvec = if value_len < 0 {
        None
    } else {
        Some(unsafe { TriedbVec::from_response(value_ptr, value_len as usize) })
    };
    let _ = sender_context.sender.send(tvec);
}

/// # Safety
/// This is used as a callback when traversing the transaction or receipt trie
pub unsafe extern "C" fn traverse_callback(
    op_kind: bindings::triedb_async_traverse_callback,
    context: *mut std::ffi::c_void,
    key_ptr: *const u8,
    key_len: usize,
    value_ptr: *const u8,
    value_len: usize,
) {
    let traverse_context = unsafe { Box::from_raw(context as *mut TraverseContext) };

    if op_kind
        == bindings::triedb_async_traverse_callback_triedb_async_traverse_callback_finished_early
    {
        let _ = traverse_context.sender.send(None);
        // traverse_context is freed here, because we don't call Box::into_raw
        return;
    }
    if op_kind
        == bindings::triedb_async_traverse_callback_triedb_async_traverse_callback_finished_normally
    {
        // completed
        let mut lock = traverse_context.data.lock().expect("mutex poisoned");
        let _ = traverse_context
            .sender
            .send(Some(std::mem::take(&mut *lock)));
        // traverse_context is freed here, because we don't call Box::into_raw
        return;
    }
    assert_eq!(
        op_kind,
        bindings::triedb_async_traverse_callback_triedb_async_traverse_callback_value
    );

    let key = unsafe { TriedbVec::from_response(key_ptr, key_len) };
    let value = unsafe { TriedbVec::from_response(value_ptr, value_len) };

    {
        let mut lock = traverse_context.data.lock().expect("mutex poisoned");
        lock.push(TraverseEntry { key, value });
    }

    // prevent Box<TraverseContext> from dropping
    let _ = Box::into_raw(traverse_context);
}

impl TriedbHandle {
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

    pub fn read(&self, key: &[u8], key_len_nibbles: u8, block_id: u64) -> Option<Vec<u8>> {
        let mut value_ptr = null();
        // make sure doesn't overflow
        if key_len_nibbles >= u8::MAX - 1 {
            error!("Key length nibbles exceeds maximum allowed value");
            return None;
        }
        if (key_len_nibbles as usize + 1) / 2 > key.len() {
            error!("Key length is insufficient for the given nibbles");
            return None;
        }

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
        if result <= 0 {
            error!("Unexpected result from triedb_read: {}", result);
            return None;
        }

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
        sender: Sender<Option<TriedbVec>>,
        concurrency_tracker: Arc<()>,
    ) {
        // make sure doesn't overflow
        if key_len_nibbles >= u8::MAX - 1 {
            error!("Key length nibbles exceeds maximum allowed value");
            return;
        }
        if (key_len_nibbles as usize + 1) / 2 > key.len() {
            error!("Key length is insufficient for the given nibbles");
            return;
        }

        // Wrap the sender and completed_counter in a context struct
        let sender_context = Box::new(SenderContext {
            sender,
            completed_counter,
            concurrency_tracker,
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
    }

    /// Used to pump async reads in TrieDB
    /// if blocking is true, the thread will sleep at least until 1 completion is available to process
    /// if blocking is false, poll will return if no completion is available to process
    /// max_completions is used as a bound for maximum completions to process in this poll
    ///
    /// Returns the number of completions processed
    /// NOTE: could call poll internally: number of calls to this functions != number of completions processed
    pub fn triedb_poll(&self, blocking: bool, max_completions: usize) -> usize {
        unsafe { bindings::triedb_poll(self.db_ptr, blocking, max_completions) }
    }

    pub fn traverse_triedb_async(
        &self,
        key: &[u8],
        key_len_nibbles: u8,
        block_id: u64,
        sender: Sender<Option<Vec<TraverseEntry>>>,
        concurrency_tracker: Arc<()>,
    ) {
        // make sure doesn't overflow
        if key_len_nibbles >= u8::MAX - 1 {
            error!("Key length nibbles exceeds maximum allowed value");
            return;
        }
        if (key_len_nibbles as usize + 1) / 2 > key.len() {
            error!("Key length is insufficient for the given nibbles");
            return;
        }

        let traverse_context = Box::new(TraverseContext {
            data: std::sync::Mutex::new(Default::default()),
            sender,
            concurrency_tracker,
        });

        unsafe {
            let context = Box::into_raw(traverse_context) as *mut std::ffi::c_void;
            bindings::triedb_async_traverse(
                self.db_ptr,
                key.as_ptr(),
                key_len_nibbles,
                block_id,
                context,
                Some(traverse_callback),
            );
        };
    }

    pub fn traverse_triedb_sync(
        &self,
        key: &[u8],
        key_len_nibbles: u8,
        block_id: u64,
        sender: Sender<Option<Vec<TraverseEntry>>>,
    ) {
        // make sure doesn't overflow
        if key_len_nibbles >= u8::MAX - 1 {
            error!("Key length nibbles exceeds maximum allowed value");
            return;
        }
        if (key_len_nibbles as usize + 1) / 2 > key.len() {
            error!("Key length is insufficient for the given nibbles");
            return;
        }

        let traverse_context = Box::new(TraverseContext {
            data: std::sync::Mutex::new(Default::default()),
            sender,
            concurrency_tracker: Arc::new(()),
        });

        unsafe {
            let context = Box::into_raw(traverse_context) as *mut std::ffi::c_void;
            // sync result is already handled by traverse_callback
            let _result = bindings::triedb_traverse(
                self.db_ptr,
                key.as_ptr(),
                key_len_nibbles,
                block_id,
                context,
                Some(traverse_callback),
            );
        };
    }

    pub fn range_get_triedb_async(
        &self,
        prefix_key: &[u8],
        prefix_key_len_nibbles: u8,
        min_key: &[u8],
        min_key_len_nibbles: u8,
        max_key: &[u8],
        max_key_len_nibbles: u8,
        block_id: u64,
        sender: Sender<Option<Vec<TraverseEntry>>>,
        concurrency_tracker: Arc<()>,
    ) {
        // make sure doesn't overflow
        if min_key_len_nibbles >= u8::MAX - 1 {
            error!("Min key length nibbles exceeds maximum allowed value");
            return;
        }
        if (min_key_len_nibbles as usize + 1) / 2 > min_key.len() {
            error!("Min key length is insufficient for the given nibbles");
            return;
        }
        if max_key_len_nibbles >= u8::MAX - 1 {
            error!("Max key length nibbles exceeds maximum allowed value");
            return;
        }
        if (max_key_len_nibbles as usize + 1) / 2 > max_key.len() {
            error!("Max key length is insufficient for the given nibbles");
            return;
        }

        let traverse_context = Box::new(TraverseContext {
            data: std::sync::Mutex::new(Default::default()),
            sender,
            concurrency_tracker,
        });

        unsafe {
            let context = Box::into_raw(traverse_context) as *mut std::ffi::c_void;
            bindings::triedb_async_ranged_get(
                self.db_ptr,
                prefix_key.as_ptr(),
                prefix_key_len_nibbles,
                min_key.as_ptr(),
                min_key_len_nibbles,
                max_key.as_ptr(),
                max_key_len_nibbles,
                block_id,
                context,
                Some(traverse_callback),
            );
        };
    }

    pub fn latest_voted_block(&self) -> Option<u64> {
        let maybe_latest_voted_block = unsafe { bindings::triedb_latest_voted_block(self.db_ptr) };
        if maybe_latest_voted_block == u64::MAX {
            None
        } else {
            Some(maybe_latest_voted_block)
        }
    }

    pub fn latest_voted_round(&self) -> Option<u64> {
        let maybe_latest_voted_round = unsafe { bindings::triedb_latest_voted_round(self.db_ptr) };
        if maybe_latest_voted_round == u64::MAX {
            None
        } else {
            Some(maybe_latest_voted_round)
        }
    }

    pub fn latest_finalized_block(&self) -> Option<u64> {
        let maybe_latest_finalized_block =
            unsafe { bindings::triedb_latest_finalized_block(self.db_ptr) };
        if maybe_latest_finalized_block == u64::MAX {
            None
        } else {
            Some(maybe_latest_finalized_block)
        }
    }

    pub fn latest_verified_block(&self) -> Option<u64> {
        let maybe_latest_verified_block =
            unsafe { bindings::triedb_latest_verified_block(self.db_ptr) };
        if maybe_latest_verified_block == u64::MAX {
            None
        } else {
            Some(maybe_latest_verified_block)
        }
    }

    pub fn earliest_finalized_block(&self) -> Option<u64> {
        let maybe_earliest_finalized_block =
            unsafe { bindings::triedb_earliest_finalized_block(self.db_ptr) };
        if maybe_earliest_finalized_block == u64::MAX {
            None
        } else {
            Some(maybe_earliest_finalized_block)
        }
    }
}

impl Drop for TriedbHandle {
    fn drop(&mut self) {
        let result = unsafe { bindings::triedb_close(self.db_ptr) };
        if result != 0 {
            error!("Unexpected result from triedb close: {}", result);
        }
    }
}
