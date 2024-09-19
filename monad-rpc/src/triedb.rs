use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        mpsc, Arc,
    },
    thread,
};

use alloy_primitives::FixedBytes;
use alloy_rlp::Decodable;
use futures::channel::oneshot;
use monad_triedb::TriedbHandle;
use monad_triedb_utils::{
    decode::{
        rlp_decode_account, rlp_decode_block_num, rlp_decode_storage_slot,
        rlp_decode_transaction_location,
    },
    key::{create_triedb_key, KeyInput},
};
use reth_primitives::{keccak256, Header, ReceiptWithBloom, TransactionSigned};
use tracing::{error, warn};

use crate::{
    eth_json_types::{BlockTags, Quantity},
    hex,
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
    TraverseRequest(TraverseRequest),
    AsyncRequest(AsyncRequest),
}

struct BlockNumberRequest {
    // a sender to send the block number back to the request handler
    request_sender: oneshot::Sender<u64>,
}

struct TraverseRequest {
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

#[derive(Debug, Clone)]
pub struct Account {
    pub nonce: u64,
    pub balance: u128,
    pub code_hash: [u8; 32],
}

#[derive(Debug, Clone, Default)]
pub struct BlockHeader {
    pub hash: FixedBytes<32>,
    pub header: Header,
}

#[derive(Debug, Clone)]
pub struct TransactionLocation {
    pub tx_index: u64,
    pub block_num: u64,
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
                    TriedbRequest::TraverseRequest(traverse_request) => {
                        // Parse block tag
                        let block_num = match traverse_request.block_tag {
                            BlockTags::Number(q) => q.0,
                            BlockTags::Latest => triedb_handle.latest_block(),
                        };
                        let rlp_encoded_data = triedb_handle.traverse_triedb(
                            &traverse_request.triedb_key,
                            traverse_request.key_len_nibbles,
                            block_num,
                        );
                        let _ = traverse_request.request_sender.send(rlp_encoded_data);
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
    fn get_latest_block(
        &self,
    ) -> impl std::future::Future<Output = Result<u64, JsonRpcError>> + Send;
    fn get_account(
        &self,
        addr: EthAddress,
        block_tag: BlockTags,
    ) -> impl std::future::Future<Output = Result<Account, JsonRpcError>> + Send;
    fn get_storage_at(
        &self,
        addr: EthAddress,
        at: EthStorageKey,
        block_tag: BlockTags,
    ) -> impl std::future::Future<Output = Result<String, JsonRpcError>> + Send;
    fn get_code(
        &self,
        code_hash: EthCodeHash,
        block_tag: BlockTags,
    ) -> impl std::future::Future<Output = Result<String, JsonRpcError>> + Send;
    fn get_receipt(
        &self,
        txn_index: u64,
        block_id: u64,
    ) -> impl std::future::Future<Output = Result<Option<ReceiptWithBloom>, JsonRpcError>> + Send;
    fn get_receipts(
        &self,
        block_id: u64,
    ) -> impl std::future::Future<Output = Result<Vec<ReceiptWithBloom>, JsonRpcError>> + Send;
    fn get_transaction(
        &self,
        txn_index: u64,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<TransactionSigned>, JsonRpcError>> + Send;
    fn get_transactions(
        &self,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Vec<TransactionSigned>, JsonRpcError>> + Send;
    fn get_block_header(
        &self,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<BlockHeader>, JsonRpcError>> + Send;
    fn get_transaction_location_by_hash(
        &self,
        tx_hash: EthTxHash,
    ) -> impl std::future::Future<Output = Result<Option<TransactionLocation>, JsonRpcError>> + Send;
    fn get_block_number_by_hash(
        &self,
        block_hash: EthBlockHash,
    ) -> impl std::future::Future<Output = Result<Option<u64>, JsonRpcError>> + Send;

    fn get_call_frame(
        &self,
        txn_index: u64,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, JsonRpcError>> + Send;

    fn get_call_frames(
        &self,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Vec<Vec<u8>>, JsonRpcError>> + Send;
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
    async fn get_latest_block(&self) -> Result<u64, JsonRpcError> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        if let Err(e) =
            self.mpsc_sender
                .try_send(TriedbRequest::BlockNumberRequest(BlockNumberRequest {
                    request_sender,
                }))
        {
            error!("Polling thread channel full: {e}");
            return Err(JsonRpcError::internal_error(
                "could not get latest block from triedb".to_string(),
            ));
        }

        match request_receiver.await {
            Ok(block_num) => Ok(block_num),
            Err(e) => {
                error!("Error when receiving response from polling thread: {e}");
                Err(JsonRpcError::internal_error(
                    "could not get latest block from triedb".to_string(),
                ))
            }
        }
    }

    async fn get_account(
        &self,
        addr: EthAddress,
        block_tag: BlockTags,
    ) -> Result<Account, JsonRpcError> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_triedb_key(KeyInput::Address(&addr));
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
            return Err(JsonRpcError::internal_error(
                "error reading from db due to rate limit".into(),
            ));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(JsonRpcError::internal_error("error reading from db".into()));
                }

                match result {
                    Some(triedb_result) => rlp_decode_account(triedb_result)
                        .map(|account| {
                            Ok(Account {
                                nonce: account.nonce,
                                balance: account.balance,
                                code_hash: account.code_hash.map_or([0u8; 32], |bytes| bytes.0),
                            })
                        })
                        .unwrap_or_else(|| {
                            error!("Decoding account error");
                            Err(JsonRpcError::internal_error("error reading from db".into()))
                        }),
                    None => Ok(Account {
                        nonce: 0,
                        balance: 0,
                        code_hash: [0u8; 32],
                    }),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(JsonRpcError::internal_error("error reading from db".into()))
            }
        }
    }

    async fn get_storage_at(
        &self,
        addr: EthAddress,
        at: EthStorageKey,
        block_tag: BlockTags,
    ) -> Result<String, JsonRpcError> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_triedb_key(KeyInput::Storage(&addr, &at));
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
            return Err(JsonRpcError::internal_error(
                "error reading from db due to rate limit".into(),
            ));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(JsonRpcError::internal_error("error reading from db".into()));
                }

                match result {
                    Some(triedb_result) => rlp_decode_storage_slot(triedb_result)
                        .map(|storage_slot| Ok(hex::encode(&storage_slot)))
                        .unwrap_or_else(|| {
                            error!("Decoding storage slot error");
                            Err(JsonRpcError::internal_error("error reading from db".into()))
                        }),
                    None => Ok(
                        "0x0000000000000000000000000000000000000000000000000000000000000000"
                            .to_string(),
                    ),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(JsonRpcError::internal_error("error reading from db".into()))
            }
        }
    }

    async fn get_code(
        &self,
        code_hash: EthCodeHash,
        block_tag: BlockTags,
    ) -> Result<String, JsonRpcError> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_triedb_key(KeyInput::CodeHash(&code_hash));
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
            return Err(JsonRpcError::internal_error(
                "error reading from db due to rate limit".into(),
            ));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(JsonRpcError::internal_error("error reading from db".into()));
                }

                match result {
                    Some(code) => Ok(hex::encode(&code)),
                    None => Ok("0x".to_string()),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(JsonRpcError::internal_error("error reading from db".into()))
            }
        }
    }

    async fn get_receipt(
        &self,
        receipt_index: u64,
        block_num: u64,
    ) -> Result<Option<ReceiptWithBloom>, JsonRpcError> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) =
            create_triedb_key(KeyInput::ReceiptIndex(Some(receipt_index)));
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
            return Err(JsonRpcError::internal_error(
                "error reading from db due to rate limit".into(),
            ));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(JsonRpcError::internal_error("error reading from db".into()));
                }

                match result {
                    Some(rlp_receipt) => {
                        let mut rlp_buf = rlp_receipt.as_slice();
                        let receipt = ReceiptWithBloom::decode(&mut rlp_buf).map_err(|e| {
                            JsonRpcError::internal_error(format!("decode receipt failed: {}", e))
                        })?;
                        Ok(Some(receipt))
                    }
                    None => Ok(None),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(JsonRpcError::internal_error("error reading from db".into()))
            }
        }
    }

    async fn get_receipts(&self, block_num: u64) -> Result<Vec<ReceiptWithBloom>, JsonRpcError> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        // receipt_index set to None to indiciate return all receipts
        let (triedb_key, key_len_nibbles) = create_triedb_key(KeyInput::ReceiptIndex(None));

        if let Err(e) = self
            .mpsc_sender
            .try_send(TriedbRequest::TraverseRequest(TraverseRequest {
                request_sender,
                triedb_key,
                key_len_nibbles,
                block_tag: BlockTags::Number(Quantity(block_num)),
            }))
        {
            error!("Polling thread channel full: {e}");
            return Err(JsonRpcError::internal_error(
                "error reading from db due to rate limit".into(),
            ));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => match result {
                Some(rlp_receipts) => {
                    let receipts = rlp_receipts
                        .iter()
                        .filter_map(|rlp_receipt| {
                            ReceiptWithBloom::decode(&mut rlp_receipt.as_slice())
                                .map_err(|e| {
                                    error!("Failed to decode RLP receipt: {e}");
                                    JsonRpcError::internal_error("error decoding receipt".into())
                                })
                                .ok()
                        })
                        .collect();
                    Ok(receipts)
                }
                None => Ok(vec![]),
            },
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(JsonRpcError::internal_error("error reading from db".into()))
            }
        }
    }

    async fn get_transaction(
        &self,
        txn_index: u64,
        block_num: u64,
    ) -> Result<Option<TransactionSigned>, JsonRpcError> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_triedb_key(KeyInput::TxIndex(Some(txn_index)));
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
            return Err(JsonRpcError::internal_error(
                "error reading from db due to rate limit".into(),
            ));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(JsonRpcError::internal_error("error reading from db".into()));
                }

                match result {
                    Some(rlp_transaction) => {
                        match TransactionSigned::decode_enveloped(&mut rlp_transaction.as_slice()) {
                            Ok(transaction) => Ok(Some(transaction)),
                            Err(e) => {
                                warn!("Failed to decode RLP transaction: {e}");
                                Err(JsonRpcError::internal_error(
                                    "error decoding transaction".into(),
                                ))
                            }
                        }
                    }
                    None => Ok(None),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(JsonRpcError::internal_error("error reading from db".into()))
            }
        }
    }

    async fn get_transactions(
        &self,
        block_num: u64,
    ) -> Result<Vec<TransactionSigned>, JsonRpcError> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        // txn_index set to None to indiciate return all transactions
        let (triedb_key, key_len_nibbles) = create_triedb_key(KeyInput::TxIndex(None));

        if let Err(e) = self
            .mpsc_sender
            .try_send(TriedbRequest::TraverseRequest(TraverseRequest {
                request_sender,
                triedb_key,
                key_len_nibbles,
                block_tag: BlockTags::Number(Quantity(block_num)),
            }))
        {
            error!("Polling thread channel full: {e}");
            return Err(JsonRpcError::internal_error(
                "error reading from db due to rate limit".into(),
            ));
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
                                    error!("Failed to decode RLP transaction: {e}");
                                    JsonRpcError::internal_error(
                                        "error decoding transaction".into(),
                                    )
                                })
                                .ok()
                        })
                        .collect();
                    Ok(signed_transactions)
                }
                None => Ok(vec![]),
            },
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(JsonRpcError::internal_error("error reading from db".into()))
            }
        }
    }

    async fn get_block_header(&self, block_num: u64) -> Result<Option<BlockHeader>, JsonRpcError> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_triedb_key(KeyInput::BlockHeader);
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
            return Err(JsonRpcError::internal_error(
                "error reading from db due to rate limit".into(),
            ));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(JsonRpcError::internal_error("error reading from db".into()));
                }

                match result {
                    Some(rlp_block_header) => {
                        let block_hash = keccak256(&rlp_block_header);
                        let mut rlp_buf = rlp_block_header.as_slice();
                        let block_header = Header::decode(&mut rlp_buf).map_err(|e| {
                            JsonRpcError::internal_error(format!(
                                "decode block header failed: {}",
                                e
                            ))
                        })?;
                        Ok(Some(BlockHeader {
                            hash: block_hash,
                            header: block_header,
                        }))
                    }
                    None => Ok(None),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(JsonRpcError::internal_error("error reading from db".into()))
            }
        }
    }

    async fn get_transaction_location_by_hash(
        &self,
        tx_hash: EthTxHash,
    ) -> Result<Option<TransactionLocation>, JsonRpcError> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_triedb_key(KeyInput::TxHash(&tx_hash));
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
            return Err(JsonRpcError::internal_error(
                "error reading from db due to rate limit".into(),
            ));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(JsonRpcError::internal_error("error reading from db".into()));
                }

                match result {
                    Some(rlp_output) => rlp_decode_transaction_location(rlp_output)
                        .map(|(block_num, tx_index)| {
                            Ok(Some(TransactionLocation {
                                tx_index,
                                block_num,
                            }))
                        })
                        .unwrap_or_else(|| {
                            error!("Decoding transaction location error");
                            Err(JsonRpcError::internal_error("error reading from db".into()))
                        }),
                    None => Ok(None),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(JsonRpcError::internal_error("error reading from db".into()))
            }
        }
    }

    async fn get_block_number_by_hash(
        &self,
        block_hash: EthBlockHash,
    ) -> Result<Option<u64>, JsonRpcError> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_triedb_key(KeyInput::BlockHash(&block_hash));
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
            return Err(JsonRpcError::internal_error(
                "error reading from db due to rate limit".into(),
            ));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(JsonRpcError::internal_error("error reading from db".into()));
                }

                match result {
                    Some(rlp_output) => rlp_decode_block_num(rlp_output)
                        .map(|block_num| Ok(Some(block_num)))
                        .unwrap_or_else(|| {
                            error!("Decoding block number error");
                            Err(JsonRpcError::internal_error("error reading from db".into()))
                        }),
                    None => Ok(None),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(JsonRpcError::internal_error("error reading from db".into()))
            }
        }
    }

    async fn get_call_frame(
        &self,
        txn_index: u64,
        block_num: u64,
    ) -> Result<Option<Vec<u8>>, JsonRpcError> {
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_triedb_key(KeyInput::CallFrame(Some(txn_index)));
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
            return Err(JsonRpcError::internal_error(
                "error reading from db due to rate limit".into(),
            ));
        }

        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(JsonRpcError::internal_error("error reading from db".into()));
                }

                Ok(result)
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(JsonRpcError::internal_error("error reading from db".into()))
            }
        }
    }

    async fn get_call_frames(&self, block_num: u64) -> Result<Vec<Vec<u8>>, JsonRpcError> {
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_triedb_key(KeyInput::CallFrame(None));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::TraverseRequest(TraverseRequest {
                request_sender,
                triedb_key,
                key_len_nibbles,
                block_tag: BlockTags::Number(Quantity(block_num)),
            }))
        {
            error!("Polling thread channel full: {e}");
            return Err(JsonRpcError::internal_error(
                "error reading from db due to rate limit".into(),
            ));
        }

        match request_receiver.await {
            Ok(result) => match result {
                Some(rlp_call_frames) => Ok(rlp_call_frames),
                None => Ok(vec![]),
            },
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(JsonRpcError::internal_error("error reading from db".into()))
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
        BlockTags::Latest => triedb_env.get_latest_block().await,
    }
}
