use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        mpsc, Arc,
    },
    thread,
};

use futures::channel::oneshot;
use monad_triedb::TriedbHandle;
use monad_triedb_utils::{
    decode::{
        rlp_decode_account, rlp_decode_block_num, rlp_decode_storage_slot,
        rlp_decode_transaction_location,
    },
    key::{
        create_addr_key, create_block_hash_key, create_block_header_key, create_code_key,
        create_receipt_key, create_storage_at_key, create_transaction_hash_key,
        create_transaction_key,
    },
};
use reth_primitives::TransactionSigned;
use tracing::{error, warn};

use crate::{
    eth_json_types::{BlockTags, Quantity},
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

const MAX_CONCURRENT_TRIEDB_REQUESTS: usize = 10_000;

type EthAddress = [u8; 20];
type EthStorageKey = [u8; 32];
type EthCodeHash = [u8; 32];
type EthTxHash = [u8; 32];
type EthBlockHash = [u8; 32];

enum TriedbRequest {
    BlockNumberRequest(BlockNumberRequest),
    TransactionsRequest(TransactionsRequest),
    AsyncRequest(AsyncRequest),
}

struct BlockNumberRequest {
    // a sender to send the block number back to the request handler
    request_sender: oneshot::Sender<u64>,
}

struct TransactionsRequest {
    // a sender for the polling thread to send the result back to the request handler
    request_sender: oneshot::Sender<Option<Vec<Vec<u8>>>>,
    // triedb_key and key_len_nibbles are used to read items from triedb
    triedb_key: Vec<u8>,
    key_len_nibbles: u8,
    // block number
    block_tag: BlockTags,
}

// struct that is sent from the request handler to the polling thread
struct AsyncRequest {
    // a sender for the polling thread to send the result back to the request handler
    // after polling is completed
    request_sender: oneshot::Sender<Option<Vec<u8>>>,
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
    Transaction(TransactionSigned),
    BlockTransactions(Vec<TransactionSigned>),
    BlockHeader(Vec<u8>),
    // (block_num, tx_index)
    TransactionLocation(u64, u64),
}

fn polling_thread(triedb_path: PathBuf, receiver: mpsc::Receiver<TriedbRequest>) {
    // create a new triedb handle for the polling thread
    let triedb_handle: TriedbHandle =
        TriedbHandle::try_new(&triedb_path).expect("triedb should exist in path");

    loop {
        triedb_handle.triedb_poll(false, usize::MAX);

        // spin on receiver
        match receiver.try_recv() {
            Ok(triedb_request) => {
                match triedb_request {
                    TriedbRequest::BlockNumberRequest(block_num_request) => {
                        let block_num = triedb_handle.latest_block();
                        let _ = block_num_request.request_sender.send(block_num);
                    }
                    TriedbRequest::TransactionsRequest(transaction_request) => {
                        // Parse block tag
                        let block_num = match transaction_request.block_tag {
                            BlockTags::Number(q) => q.0,
                            BlockTags::Latest => triedb_handle.latest_block(),
                        };
                        let rlp_encoded_transactions = triedb_handle.get_transactions(
                            &transaction_request.triedb_key,
                            transaction_request.key_len_nibbles,
                            block_num,
                        );
                        let _ = transaction_request
                            .request_sender
                            .send(rlp_encoded_transactions);
                    }
                    TriedbRequest::AsyncRequest(async_request) => {
                        // Process the request directly in this thread
                        process_request(&triedb_handle, async_request);
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

fn process_request(triedb_handle: &TriedbHandle, async_request: AsyncRequest) {
    // Parse block tag
    let block_num = match async_request.block_tag {
        BlockTags::Number(q) => q.0,
        BlockTags::Latest => triedb_handle.latest_block(),
    };

    // read_async will send back a future to request_receiver of oneshot channel
    triedb_handle.read_async(
        &async_request.triedb_key,
        async_request.key_len_nibbles,
        block_num,
        async_request.completed_counter,
        async_request.request_sender,
    );
}

pub trait Triedb {
    fn get_latest_block(&self) -> impl std::future::Future<Output = TriedbResult> + Send;
    fn get_receipt(
        &self,
        txn_index: u64,
        block_id: u64,
    ) -> impl std::future::Future<Output = TriedbResult> + Send;
    fn get_account(
        &self,
        addr: EthAddress,
        block_tag: BlockTags,
    ) -> impl std::future::Future<Output = TriedbResult> + Send;
    fn get_storage_at(
        &self,
        addr: EthAddress,
        at: EthStorageKey,
        block_tag: BlockTags,
    ) -> impl std::future::Future<Output = TriedbResult> + Send;
    fn get_code(
        &self,
        code_hash: EthCodeHash,
        block_tag: BlockTags,
    ) -> impl std::future::Future<Output = TriedbResult> + Send;
    fn get_transaction(
        &self,
        txn_index: u64,
        block_num: u64,
    ) -> impl std::future::Future<Output = TriedbResult> + Send;
    fn get_transactions(
        &self,
        block_num: u64,
    ) -> impl std::future::Future<Output = TriedbResult> + Send;
    fn get_block_header(
        &self,
        block_num: u64,
    ) -> impl std::future::Future<Output = TriedbResult> + Send;
    fn get_transaction_location_by_hash(
        &self,
        tx_hash: EthTxHash,
    ) -> impl std::future::Future<Output = TriedbResult> + Send;
    fn get_block_number_by_hash(
        &self,
        block_hash: EthBlockHash,
    ) -> impl std::future::Future<Output = TriedbResult> + Send;
}

pub trait TriedbPath {
    fn path(&self) -> PathBuf;
}

#[derive(Clone)]
pub struct TriedbEnv {
    triedb_path: PathBuf,
    mpsc_sender: mpsc::SyncSender<TriedbRequest>, // sender for tasks
}

impl TriedbEnv {
    pub fn new(triedb_path: &Path) -> Self {
        // create a mpsc channel where sender are incoming requests, and the receiver is the triedb poller
        let (sender, receiver) =
            mpsc::sync_channel::<TriedbRequest>(MAX_CONCURRENT_TRIEDB_REQUESTS);

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
}

impl TriedbPath for TriedbEnv {
    fn path(&self) -> PathBuf {
        self.triedb_path.clone()
    }
}

impl Triedb for TriedbEnv {
    async fn get_latest_block(&self) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        if let Err(e) =
            self.mpsc_sender
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

    async fn get_account(&self, addr: EthAddress, block_tag: BlockTags) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_addr_key(&addr);
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
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
        }
    }

    async fn get_storage_at(
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
        }
    }

    async fn get_code(&self, code_hash: EthCodeHash, block_tag: BlockTags) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_code_key(&code_hash);
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
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
        }
    }

    async fn get_receipt(&self, txn_index: u64, block_num: u64) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_receipt_key(txn_index);
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_tag: BlockTags::Number(Quantity(block_num)),
            }))
        {
            error!("Polling thread channel full: {e}");
            return TriedbResult::Error;
        }

        // await the result using request_receiver
        match request_receiver.await {
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
        }
    }

    async fn get_transaction(&self, txn_index: u64, block_num: u64) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_transaction_key(Some(txn_index));
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_tag: BlockTags::Number(Quantity(block_num)),
            }))
        {
            error!("Polling thread channel full: {e}");
            return TriedbResult::Error;
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return TriedbResult::Error;
                }

                match result {
                    Some(rlp_transaction) => {
                        match TransactionSigned::decode_enveloped(&mut rlp_transaction.as_slice()) {
                            Ok(transaction) => TriedbResult::Transaction(transaction),
                            Err(e) => {
                                warn!("Failed to decode RLP transaction: {e}");
                                TriedbResult::Error
                            }
                        }
                    }
                    None => TriedbResult::Null,
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                TriedbResult::Error
            }
        }
    }

    async fn get_transactions(&self, block_num: u64) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        // txn_index set to None to indiciate return all transactions
        let (triedb_key, key_len_nibbles) = create_transaction_key(None);

        if let Err(e) = self
            .mpsc_sender
            .try_send(TriedbRequest::TransactionsRequest(TransactionsRequest {
                request_sender,
                triedb_key,
                key_len_nibbles,
                block_tag: BlockTags::Number(Quantity(block_num)),
            }))
        {
            error!("Polling thread channel full: {e}");
            return TriedbResult::Error;
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => match result {
                Some(rlp_transactions) => {
                    let signed_transactions = rlp_transactions
                        .iter()
                        .filter_map(|rlp_transaction| {
                            TransactionSigned::decode_enveloped(&mut rlp_transaction.as_slice())
                                .map_err(|e| {
                                    warn!("Failed to decode RLP transaction: {e}");
                                    TriedbResult::Error
                                })
                                .ok()
                        })
                        .collect();
                    TriedbResult::BlockTransactions(signed_transactions)
                }
                None => TriedbResult::Null,
            },
            Err(e) => {
                error!("Error awaiting result: {e}");
                TriedbResult::Error
            }
        }
    }

    async fn get_block_header(&self, block_num: u64) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_block_header_key();
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_tag: BlockTags::Number(Quantity(block_num)),
            }))
        {
            error!("Polling thread channel full: {e}");
            return TriedbResult::Error;
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return TriedbResult::Error;
                }

                match result {
                    Some(block_header) => TriedbResult::BlockHeader(block_header),
                    None => TriedbResult::Null,
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                TriedbResult::Error
            }
        }
    }

    async fn get_transaction_location_by_hash(&self, tx_hash: EthTxHash) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_transaction_hash_key(&tx_hash);
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_tag: BlockTags::Latest,
            }))
        {
            error!("Polling thread channel full: {e}");
            return TriedbResult::Error;
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return TriedbResult::Error;
                }

                match result {
                    Some(rlp_output) => rlp_decode_transaction_location(rlp_output)
                        .map(|(block_num, tx_index)| {
                            TriedbResult::TransactionLocation(block_num, tx_index)
                        })
                        .unwrap_or_else(|| {
                            error!("Decoding transaction location error");
                            TriedbResult::Error
                        }),
                    None => TriedbResult::Null,
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                TriedbResult::Error
            }
        }
    }

    async fn get_block_number_by_hash(&self, block_hash: EthBlockHash) -> TriedbResult {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_block_hash_key(&block_hash);
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_tag: BlockTags::Latest,
            }))
        {
            error!("Polling thread channel full: {e}");
            return TriedbResult::Error;
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return TriedbResult::Error;
                }

                match result {
                    Some(rlp_output) => rlp_decode_block_num(rlp_output)
                        .map(TriedbResult::BlockNum)
                        .unwrap_or_else(|| {
                            error!("Decoding block number error");
                            TriedbResult::Error
                        }),
                    None => TriedbResult::Null,
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                TriedbResult::Error
            }
        }
    }
}

pub async fn get_block_num_from_tag<T: Triedb>(
    triedb_env: &T,
    tag: BlockTags,
) -> JsonRpcResult<u64> {
    match tag {
        BlockTags::Number(n) => Ok(n.0),
        BlockTags::Latest => {
            let result = triedb_env.get_latest_block().await;
            let TriedbResult::BlockNum(n) = result else {
                return Err(JsonRpcError::internal_error(
                    "could not get latest block from triedb".to_string(),
                ));
            };
            Ok(n)
        }
    }
}
