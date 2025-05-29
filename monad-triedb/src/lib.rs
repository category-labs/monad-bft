use std::{
    mem::take,
    path::PathBuf,
    sync::{atomic::AtomicUsize, Arc},
};

use futures::channel::oneshot::Sender;

#[allow(dead_code, non_camel_case_types, non_upper_case_globals)]
mod bindings {
    include!("../../monad-cxx/monad-execution/libs/triedb-driver/rust/monad_triedb_driver.rs");
}

pub type MonadCResult<T> = bindings::MonadCResult<T>;

pub type TraverseEntry = bindings::TraverseEntry;

#[derive(Debug)]
struct ReadAsyncSenderWrapper {
    sender: Option<Sender<MonadCResult<Vec<u8>>>>,
}

impl bindings::ReadAsyncSender for ReadAsyncSenderWrapper {
    fn send(&mut self, result: MonadCResult<Vec<u8>>) {
        // Send the retrieved result through the channel
        let sender = take(&mut self.sender);
        let _ = sender.unwrap().send(result);
    }
}

#[derive(Debug)]
struct TraverseAsyncSenderWrapper {
    sender: Option<Sender<MonadCResult<Vec<TraverseEntry>>>>,
}

impl bindings::TraverseAsyncSender for TraverseAsyncSenderWrapper {
    fn send(&mut self, result: MonadCResult<Vec<TraverseEntry>>) {
        // Send the retrieved result through the channel
        let sender = take(&mut self.sender);
        let _ = sender.unwrap().send(result);
    }
}

#[derive(Clone, Debug, Default)]
pub struct TriedbHandle {
    db: bindings::TriedbHandle,
}

impl TriedbHandle {
    #[allow(dead_code)]
    pub fn new(dbdirpaths: &[PathBuf]) -> MonadCResult<TriedbHandle> {
        Ok(TriedbHandle {
            db: bindings::TriedbHandle::new(dbdirpaths)?,
        })
    }

    #[allow(dead_code)]
    pub fn read(&self, key: &[u8], key_len_nibbles: u8, block_id: u64) -> MonadCResult<Vec<u8>> {
        self.db.read(key, key_len_nibbles, block_id)
    }

    #[allow(dead_code)]
    pub fn read_async(
        &self,
        key: &[u8],
        key_len_nibbles: u8,
        block_id: u64,
        completed_counter: Arc<AtomicUsize>,
        sender: Sender<MonadCResult<Vec<u8>>>,
        concurrency_tracker: Arc<()>,
    ) {
        self.db.read_async(
            key,
            key_len_nibbles,
            block_id,
            completed_counter,
            Box::new(ReadAsyncSenderWrapper {
                sender: Some(sender),
            }),
            concurrency_tracker,
        )
    }

    #[allow(dead_code)]
    pub fn triedb_poll(&self, blocking: bool, max_completions: usize) -> MonadCResult<usize> {
        self.db.triedb_poll(blocking, max_completions)
    }

    pub fn traverse_triedb_async(
        &self,
        key: &[u8],
        key_len_nibbles: u8,
        block_id: u64,
        sender: Sender<MonadCResult<Vec<TraverseEntry>>>,
        concurrency_tracker: Arc<()>,
    ) {
        self.db.traverse_triedb_async(
            key,
            key_len_nibbles,
            block_id,
            Box::new(TraverseAsyncSenderWrapper {
                sender: Some(sender),
            }),
            concurrency_tracker,
        )
    }

    #[allow(dead_code)]
    pub fn traverse_triedb_sync(
        &self,
        key: &[u8],
        key_len_nibbles: u8,
        block_id: u64,
        sender: Sender<MonadCResult<Vec<TraverseEntry>>>,
    ) {
        self.db.traverse_triedb_sync(
            key,
            key_len_nibbles,
            block_id,
            Box::new(TraverseAsyncSenderWrapper {
                sender: Some(sender),
            }),
        )
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
        sender: Sender<MonadCResult<Vec<TraverseEntry>>>,
        concurrency_tracker: Arc<()>,
    ) {
        self.db.range_get_triedb_async(
            prefix_key,
            prefix_key_len_nibbles,
            min_key,
            min_key_len_nibbles,
            max_key,
            max_key_len_nibbles,
            block_id,
            Box::new(TraverseAsyncSenderWrapper {
                sender: Some(sender),
            }),
            concurrency_tracker,
        )
    }

    #[allow(dead_code)]
    pub fn latest_voted_block(&self) -> MonadCResult<u64> {
        self.db.latest_voted_block()
    }

    pub fn latest_voted_round(&self) -> MonadCResult<u64> {
        self.db.latest_voted_round()
    }

    pub fn latest_finalized_block(&self) -> MonadCResult<u64> {
        self.db.latest_finalized_block()
    }

    pub fn latest_verified_block(&self) -> MonadCResult<u64> {
        self.db.latest_verified_block()
    }

    pub fn earliest_finalized_block(&self) -> MonadCResult<u64> {
        self.db.earliest_finalized_block()
    }
}
