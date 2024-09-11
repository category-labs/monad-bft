use std::{
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant},
};

use futures::channel::oneshot::Receiver;
use monad_triedb::Handle as TriedbHandle;
use monad_triedb_utils::{
    decode::{rlp_decode_account, rlp_decode_storage_slot},
    key::{create_addr_key, create_code_key, create_receipt_key, create_storage_at_key},
};
use tokio::sync::oneshot;
use tracing::error;

type EthAddress = [u8; 20];
type EthStorageKey = [u8; 32];
type EthCodeHash = [u8; 32];

enum TriedbRequest {
    BlockNumberRequest(BlockNumberRequest),
    AsyncRequest(AsyncRequest),
}

struct BlockNumberRequest {
    // a sender to send the block number back to the request handler
    request_sender: oneshot::Sender<u64>,
}

// struct that is sent from the request handler to the polling thread
struct AsyncRequest {
    // a sender for the polling thread to send the result back to the request handler
    // after polling is completed
    request_sender: oneshot::Sender<Receiver<Option<Vec<u8>>>>,
    // counter which is updated when TrieDB processes a single async read to completion
    completed_counter: Arc<AtomicUsize>,
    // triedb_key and key_len_nibbles are used to read items from triedb
    triedb_key: Vec<u8>,
    key_len_nibbles: u8,
    // block number
    block_tag: BlockTags,
}

#[derive(Debug)]
pub enum TriedbResult {
    Null,
    Error,
    // (nonce, balance, code_hash)
    Account(u64, u128, [u8; 32]),
    Storage([u8; 32]),
    Code(Vec<u8>),
    Receipt(Vec<u8>),
    BlockNum(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockTags {
    Number(u64),
    Latest,
}

impl Default for BlockTags {
    fn default() -> Self {
        Self::Latest
    }
}

fn polling_thread(triedb_path: PathBuf, receiver: mpsc::Receiver<TriedbRequest>) {
    // create a new triedb handle for the polling thread
    let triedb_handle: Rc<TriedbHandle> =
        Rc::new(TriedbHandle::try_new(&triedb_path).expect("triedb should exist in path"));

    // create a polling interval of 200 microseconds
    let poll_interval = Duration::from_micros(200);
    let mut last_poll = Instant::now();

    loop {
        if last_poll.elapsed() >= poll_interval {
            triedb_handle.triedb_poll(false, usize::MAX);
            last_poll = Instant::now();
        }

        // spin on receiver
        match receiver.try_recv() {
            Ok(triedb_request) => {
                let triedb_handle_clone = Rc::clone(&triedb_handle);

                match triedb_request {
                    TriedbRequest::BlockNumberRequest(block_num_request) => {
                        let block_num = triedb_handle_clone.latest_block();
                        let _ = block_num_request.request_sender.send(block_num);
                    }
                    TriedbRequest::AsyncRequest(async_request) => {
                        // Process the request directly in this thread
                        process_request(triedb_handle_clone, async_request);
                    }
                }
            }
            Err(_) => {
                // no message received, continue spinning
                continue;
            }
        }
    }
}

fn process_request(triedb_handle: Rc<TriedbHandle>, async_request: AsyncRequest) {
    // Parse block tag
    let block_num = match async_request.block_tag {
        BlockTags::Number(q) => q,
        BlockTags::Latest => triedb_handle.latest_block(),
    };

    let future_result = triedb_handle.read_async(
        &async_request.triedb_key,
        async_request.key_len_nibbles,
        block_num,
        async_request.completed_counter,
    );

    // send read async future back to original thread
    let _ = async_request.request_sender.send(future_result);
}

#[derive(Clone)]
pub struct TriedbEnv {
    triedb_path: PathBuf,
    mpsc_sender: mpsc::SyncSender<TriedbRequest>, // sender for tasks
}

impl TriedbEnv {
    pub fn new(triedb_path: &Path) -> Self {
        // create a mpsc channel where sender are incoming requests, and the receiver is the triedb poller
        let (sender, receiver) = mpsc::sync_channel::<TriedbRequest>(10000);

        // spawn the polling thread in a dedicated thread
        let triedb_path_cloned = triedb_path.to_path_buf();
        thread::spawn(move || {
            polling_thread(triedb_path_cloned, receiver);
        });

        Self {
            triedb_path: triedb_path.to_path_buf(),
            mpsc_sender: sender,
        }
    }

    pub async fn get_latest_block(&self) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::BlockNumberRequest(BlockNumberRequest {
                request_sender,
            }))
        {
            error!("Polling thread channel full: {e}");
            return TriedbResult::Error;
        }

        match request_receiver.await {
            Ok(block_num) => TriedbResult::BlockNum(block_num),
            Err(e) => {
                error!("Error when receiving response from polling thread: {e}");
                TriedbResult::Error
            }
        }
    }

    pub async fn get_account(&self, addr: EthAddress, block_tag: BlockTags) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_addr_key(&addr);
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_tag,
            }))
        {
            error!("Polling thread channel full: {e}");
            return TriedbResult::Error;
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => match result.await {
                Ok(result) => {
                    // sanity check to ensure completed_counter is equal to 1
                    if completed_counter.load(SeqCst) != 1 {
                        error!("Unexpected completed_counter value");
                        return TriedbResult::Error;
                    }

                    match result {
                        Some(triedb_result) => rlp_decode_account(triedb_result)
                            .map(|account| {
                                TriedbResult::Account(
                                    account.nonce,
                                    account.balance,
                                    account.code_hash.map_or([0u8; 32], |bytes| bytes.0),
                                )
                            })
                            .unwrap_or_else(|| {
                                error!("Decoding account error");
                                TriedbResult::Error
                            }),
                        None => TriedbResult::Null,
                    }
                }
                Err(e) => {
                    error!("Error awaiting result: {e}");
                    TriedbResult::Error
                }
            },
            Err(e) => {
                error!("Error when receiving response from polling thread: {e}");
                TriedbResult::Error
            }
        }
    }

    pub async fn get_storage_at(
        &self,
        addr: EthAddress,
        at: EthStorageKey,
        block_tag: BlockTags,
    ) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_storage_at_key(&addr, &at);
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_tag,
            }))
        {
            error!("Polling thread channel full: {e}");
            return TriedbResult::Error;
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => match result.await {
                Ok(result) => {
                    // sanity check to ensure completed_counter is equal to 1
                    if completed_counter.load(SeqCst) != 1 {
                        error!("Unexpected completed_counter value");
                        return TriedbResult::Error;
                    }

                    match result {
                        Some(triedb_result) => rlp_decode_storage_slot(triedb_result)
                            .map(TriedbResult::Storage)
                            .unwrap_or_else(|| {
                                error!("Decoding storage slot error");
                                TriedbResult::Error
                            }),
                        None => TriedbResult::Null,
                    }
                }
                Err(e) => {
                    error!("Error awaiting result: {e}");
                    TriedbResult::Error
                }
            },
            Err(e) => {
                error!("Error when receiving response from polling thread: {e}");
                TriedbResult::Error
            }
        }
    }

    pub async fn get_code(&self, code_hash: EthCodeHash, block_tag: BlockTags) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_code_key(&code_hash);
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_tag,
            }))
        {
            error!("Polling thread channel full: {e}");
            return TriedbResult::Error;
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => match result.await {
                Ok(result) => {
                    // sanity check to ensure completed_counter is equal to 1
                    if completed_counter.load(SeqCst) != 1 {
                        error!("Unexpected completed_counter value");
                        return TriedbResult::Error;
                    }

                    match result {
                        Some(code) => TriedbResult::Code(code),
                        None => TriedbResult::Null,
                    }
                }
                Err(e) => {
                    error!("Error awaiting result: {e}");
                    TriedbResult::Error
                }
            },
            Err(e) => {
                error!("Error when receiving response from polling thread: {e}");
                TriedbResult::Error
            }
        }
    }

    pub async fn get_receipt(&self, txn_index: u64, block_num: u64) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_receipt_key(txn_index);
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_tag: BlockTags::Number(block_num),
            }))
        {
            error!("Polling thread channel full: {e}");
            return TriedbResult::Error;
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => match result.await {
                Ok(result) => {
                    // sanity check to ensure completed_counter is equal to 1
                    if completed_counter.load(SeqCst) != 1 {
                        error!("Unexpected completed_counter value");
                        return TriedbResult::Error;
                    }

                    match result {
                        Some(receipt) => TriedbResult::Receipt(receipt),
                        None => TriedbResult::Null,
                    }
                }
                Err(e) => {
                    error!("Error awaiting result: {e}");
                    TriedbResult::Error
                }
            },
            Err(e) => {
                error!("Error when receiving response from polling thread: {e}");
                TriedbResult::Error
            }
        }
    }

    pub fn path(&self) -> PathBuf {
        self.triedb_path.clone()
    }
}
