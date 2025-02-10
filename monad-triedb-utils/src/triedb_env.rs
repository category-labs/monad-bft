use std::{
    fmt::Debug,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        mpsc, Arc,
    },
    thread,
    time::Duration,
};

use alloy_consensus::{Header, ReceiptEnvelope, TxEnvelope};
use alloy_primitives::{keccak256, Address, FixedBytes};
use alloy_rlp::{encode_list, BytesMut, Decodable, Encodable};
use futures::channel::oneshot;
use monad_triedb::{TraverseEntry, TriedbHandle};
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use crate::{
    decode::{
        rlp_decode_account, rlp_decode_block_num, rlp_decode_storage_slot,
        rlp_decode_transaction_location,
    },
    key::{create_triedb_key, KeyInput, Version},
};

pub type EthAddress = [u8; 20];
pub type EthStorageKey = [u8; 32];
pub type EthCodeHash = [u8; 32];
pub type EthTxHash = [u8; 32];
pub type EthBlockHash = [u8; 32];

enum TriedbRequest {
    SyncRequest(SyncRequest),
    AsyncRequest(AsyncRequest),
    AsyncTraverseRequest(AsyncTraverseRequest),
}

enum SyncRequest {
    BlockNumberRequest(BlockNumberRequest),
}

struct BlockNumberRequest {
    // a sender to send the block number back to the request handler
    request_sender: oneshot::Sender<u64>,
}

struct AsyncTraverseRequest {
    // a sender for the polling thread to send the result back to the request handler
    request_sender: oneshot::Sender<Option<Vec<TraverseEntry>>>,
    // triedb_key and key_len_nibbles are used to read items from triedb
    triedb_key: Vec<u8>,
    key_len_nibbles: u8,
    block_num: u64,
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
    block_num: u64,
}

#[derive(Debug, Clone, Default)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptWithLogIndex {
    pub receipt: ReceiptEnvelope,
    pub starting_log_index: u64,
}

impl Encodable for ReceiptWithLogIndex {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let mut encoded_receipt: BytesMut = BytesMut::new();
        self.receipt.encode(&mut encoded_receipt);
        let enc: [&dyn Encodable; 2] = [&encoded_receipt, &self.starting_log_index];
        encode_list::<_, dyn Encodable>(&enc, out);
    }
}

impl Decodable for ReceiptWithLogIndex {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let alloy_rlp::Header {
            list,
            payload_length: _,
        } = alloy_rlp::Header::decode(buf)?;
        if !list {
            return Err(alloy_rlp::Error::UnexpectedString);
        }

        let alloy_rlp::Header {
            list,
            payload_length: _,
        } = alloy_rlp::Header::decode(buf)?;
        if list {
            return Err(alloy_rlp::Error::UnexpectedList);
        }
        let receipt = ReceiptEnvelope::decode(buf)?;
        let starting_log_index = u64::decode(buf)?;

        Ok(ReceiptWithLogIndex {
            receipt,
            starting_log_index,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxEnvelopeWithSender {
    pub tx: TxEnvelope,
    pub sender: Address,
}

impl Encodable for TxEnvelopeWithSender {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        let mut encoded_tx: BytesMut = BytesMut::new();
        self.tx.encode(&mut encoded_tx);
        let enc: [&dyn Encodable; 2] = [&encoded_tx, &self.sender];
        encode_list::<_, dyn Encodable>(&enc, out);
    }
}

impl Decodable for TxEnvelopeWithSender {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let alloy_rlp::Header {
            list,
            payload_length: _,
        } = alloy_rlp::Header::decode(buf)?;
        if !list {
            return Err(alloy_rlp::Error::UnexpectedString);
        }

        let alloy_rlp::Header {
            list,
            payload_length: _,
        } = alloy_rlp::Header::decode(buf)?;
        if list {
            return Err(alloy_rlp::Error::UnexpectedList);
        }
        let tx = TxEnvelope::decode(buf)?;
        let sender = Address::decode(buf)?;

        Ok(TxEnvelopeWithSender { tx, sender })
    }
}

const MAX_NOP_POLL_BEFORE_SLEEP: usize = 1_000_000;
const MAX_POLL_COMPLETIONS: usize = usize::MAX;

fn polling_thread(triedb_path: PathBuf, receiver: mpsc::Receiver<TriedbRequest>) {
    // create a new triedb handle for the polling thread
    let triedb_handle: TriedbHandle =
        TriedbHandle::try_new(&triedb_path).expect("triedb should exist in path");

    let mut num_nop_poll = 0;
    loop {
        if num_nop_poll >= MAX_NOP_POLL_BEFORE_SLEEP {
            std::thread::sleep(Duration::from_millis(1));
        }

        let new_completions = triedb_handle.triedb_poll(false, MAX_POLL_COMPLETIONS);
        if new_completions == 0 {
            num_nop_poll += 1;
        } else {
            num_nop_poll = 0;
        }

        if let Ok(triedb_request) = receiver.try_recv() {
            num_nop_poll = 0;
            match triedb_request {
                TriedbRequest::SyncRequest(sync_request) => match sync_request {
                    SyncRequest::BlockNumberRequest(block_num_request) => {
                        let block_num = triedb_handle.latest_finalized_block().unwrap_or_default();
                        let _ = block_num_request.request_sender.send(block_num);
                    }
                },
                TriedbRequest::AsyncTraverseRequest(traverse_request) => {
                    triedb_handle.traverse_triedb_async(
                        &traverse_request.triedb_key,
                        traverse_request.key_len_nibbles,
                        traverse_request.block_num,
                        traverse_request.request_sender,
                    );
                }
                TriedbRequest::AsyncRequest(async_request) => {
                    // Process the request directly in this thread
                    // read_async will send back a future to request_receiver of oneshot channel
                    triedb_handle.read_async(
                        &async_request.triedb_key,
                        async_request.key_len_nibbles,
                        async_request.block_num,
                        async_request.completed_counter,
                        async_request.request_sender,
                    );
                }
            }
        }
    }
}

pub trait Triedb: Debug {
    fn get_latest_block(&self) -> impl std::future::Future<Output = Result<u64, String>> + Send;
    fn get_account(
        &self,
        addr: EthAddress,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Account, String>> + Send;
    fn get_storage_at(
        &self,
        addr: EthAddress,
        at: EthStorageKey,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<String, String>> + Send;
    fn get_code(
        &self,
        code_hash: EthCodeHash,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<String, String>> + Send;
    fn get_receipt(
        &self,
        txn_index: u64,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<ReceiptWithLogIndex>, String>> + Send;
    fn get_receipts(
        &self,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Vec<ReceiptWithLogIndex>, String>> + Send + Sync;
    fn get_transaction(
        &self,
        txn_index: u64,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<TxEnvelopeWithSender>, String>> + Send;
    fn get_transactions(
        &self,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Vec<TxEnvelopeWithSender>, String>> + Send + Sync;
    fn get_block_header(
        &self,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<BlockHeader>, String>> + Send + Sync;
    fn get_transaction_location_by_hash(
        &self,
        tx_hash: EthTxHash,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<TransactionLocation>, String>> + Send;
    fn get_block_number_by_hash(
        &self,
        block_hash: EthBlockHash,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<u64>, String>> + Send;

    fn get_call_frame(
        &self,
        txn_index: u64,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, String>> + Send;

    fn get_call_frames(
        &self,
        block_num: u64,
    ) -> impl std::future::Future<Output = Result<Vec<Vec<u8>>, String>> + Send;
}

pub trait TriedbPath {
    fn path(&self) -> PathBuf;
}

#[derive(Clone)]
pub struct TriedbEnv {
    triedb_path: PathBuf,
    mpsc_sender: mpsc::SyncSender<TriedbRequest>, // sender for tasks
}

impl std::fmt::Debug for TriedbEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TriedbEnv")
            .field("path", &self.triedb_path)
            .finish()
    }
}

impl TriedbEnv {
    pub fn new(triedb_path: &Path, max_concurrent_triedb_reads: usize) -> Self {
        // create a mpsc channel where sender are incoming requests, and the receiver is the triedb poller
        let (sender, receiver) = mpsc::sync_channel::<TriedbRequest>(max_concurrent_triedb_reads);

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
    #[tracing::instrument(level = "debug")]
    async fn get_latest_block(&self) -> Result<u64, String> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        if let Err(e) =
            self.mpsc_sender
                .try_send(TriedbRequest::SyncRequest(SyncRequest::BlockNumberRequest(
                    BlockNumberRequest { request_sender },
                )))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("could not get latest block from triedb"));
        }

        match request_receiver.await {
            Ok(block_num) => Ok(block_num),
            Err(e) => {
                error!("Error when receiving response from polling thread: {e}");
                Err(String::from("could not get latest block from triedb"))
            }
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_account(&self, addr: EthAddress, block_num: u64) -> Result<Account, String> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::Address(&addr));
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_num,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(String::from("error reading from db"));
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
                            Err(String::from("error reading from db"))
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
                Err(String::from("error reading from db"))
            }
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_storage_at(
        &self,
        addr: EthAddress,
        at: EthStorageKey,
        block_num: u64,
    ) -> Result<String, String> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::Storage(&addr, &at));
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_num,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(String::from("error reading from db"));
                }

                match result {
                    Some(triedb_result) => rlp_decode_storage_slot(triedb_result)
                        .map(|storage_slot| Ok(format!("0x{}", hex::encode(storage_slot))))
                        .unwrap_or_else(|| {
                            error!("Decoding storage slot error");
                            Err(String::from("error reading from db"))
                        }),
                    None => Ok(
                        "0x0000000000000000000000000000000000000000000000000000000000000000"
                            .to_string(),
                    ),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_code(&self, code_hash: EthCodeHash, block_num: u64) -> Result<String, String> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::CodeHash(&code_hash));
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_num,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(String::from("error reading from db"));
                }

                match result {
                    Some(code) => Ok(format!("0x{}", hex::encode(code))),
                    None => Ok("0x".to_string()),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_receipt(
        &self,
        receipt_index: u64,
        block_num: u64,
    ) -> Result<Option<ReceiptWithLogIndex>, String> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) = create_triedb_key(
            Version::Finalized,
            KeyInput::ReceiptIndex(Some(receipt_index)),
        );
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_num,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(String::from("error reading from db"));
                }

                match result {
                    Some(rlp_receipt) => {
                        let mut rlp_buf = rlp_receipt.as_slice();
                        let receipt = ReceiptWithLogIndex::decode(&mut rlp_buf)
                            .map_err(|e| format!("decode receipt failed: {}", e))?;
                        Ok(Some(receipt))
                    }
                    None => Ok(None),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        }
    }

    async fn get_receipts(&self, block_num: u64) -> Result<Vec<ReceiptWithLogIndex>, String> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        // receipt_index set to None to indiciate return all receipts
        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::ReceiptIndex(None));

        if let Err(e) = self
            .mpsc_sender
            .try_send(TriedbRequest::AsyncTraverseRequest(AsyncTraverseRequest {
                request_sender,
                triedb_key,
                key_len_nibbles,
                block_num,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(Some(rlp_receipts)) => {
                let mut receipts = rlp_receipts
                    .into_iter()
                    .map(|TraverseEntry { key, value }| {
                        let idx: usize = alloy_rlp::decode_exact(key)?;
                        let receipt: ReceiptWithLogIndex = alloy_rlp::decode_exact(value)?;

                        Ok((idx, receipt))
                    })
                    .collect::<Result<Vec<_>, alloy_rlp::Error>>()
                    .map_err(|err| {
                        error!(?err, "error decoding result from db");
                        String::from("error decoding from db")
                    })?;
                receipts.sort_by_key(|(idx, _)| *idx);
                // check that indices are consecutive and start with 0
                if !receipts
                    .iter()
                    .map(|(idx, _)| idx)
                    .zip(0..)
                    .all(|(&i, j)| i == j)
                {
                    return Err(format!(
                        "receipts missing from db, indices={:?}",
                        receipts.iter().map(|(idx, _)| idx).collect::<Vec<_>>()
                    ));
                }
                Ok(receipts.into_iter().map(|(_, receipt)| receipt).collect())
            }
            Ok(None) => {
                error!("Error traversing db");
                Err(String::from("error traversing db"))
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_transaction(
        &self,
        txn_index: u64,
        block_num: u64,
    ) -> Result<Option<TxEnvelopeWithSender>, String> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::TxIndex(Some(txn_index)));
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_num,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(String::from("error reading from db"));
                }

                match result {
                    Some(rlp_transaction) => {
                        match TxEnvelopeWithSender::decode(&mut rlp_transaction.as_slice()) {
                            Ok(transaction) => Ok(Some(transaction)),
                            Err(e) => {
                                warn!("Failed to decode RLP transaction: {e}");
                                Err(String::from("error decoding transaction"))
                            }
                        }
                    }
                    None => Ok(None),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_transactions(&self, block_num: u64) -> Result<Vec<TxEnvelopeWithSender>, String> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        // txn_index set to None to indiciate return all transactions
        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::TxIndex(None));

        if let Err(e) = self
            .mpsc_sender
            .try_send(TriedbRequest::AsyncTraverseRequest(AsyncTraverseRequest {
                request_sender,
                triedb_key,
                key_len_nibbles,
                block_num,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(Some(rlp_transactions)) => {
                let mut transactions = rlp_transactions
                    .into_iter()
                    .map(|TraverseEntry { key, value }| {
                        let idx: usize = alloy_rlp::decode_exact(key)?;
                        let transaction: TxEnvelopeWithSender = alloy_rlp::decode_exact(value)?;

                        Ok((idx, transaction))
                    })
                    .collect::<Result<Vec<_>, alloy_rlp::Error>>()
                    .map_err(|err| {
                        error!(?err, "error decoding result from db");
                        String::from("error decoding from db")
                    })?;
                transactions.sort_by_key(|(idx, _)| *idx);
                // check that indices are consecutive and start with 0
                if !transactions
                    .iter()
                    .map(|(idx, _)| idx)
                    .zip(0..)
                    .all(|(&i, j)| i == j)
                {
                    return Err(format!(
                        "transactions missing from db, indices={:?}",
                        transactions.iter().map(|(idx, _)| idx).collect::<Vec<_>>()
                    ));
                }
                Ok(transactions
                    .into_iter()
                    .map(|(_, transaction)| transaction)
                    .collect())
            }
            Ok(None) => {
                error!("Error traversing db");
                Err(String::from("error traversing db"))
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_block_header(&self, block_num: u64) -> Result<Option<BlockHeader>, String> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::BlockHeader);
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_num,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(String::from("error reading from db"));
                }

                match result {
                    Some(rlp_block_header) => {
                        let block_hash = keccak256(&rlp_block_header);
                        let mut rlp_buf = rlp_block_header.as_slice();
                        let block_header = Header::decode(&mut rlp_buf)
                            .map_err(|e| format!("decode block header failed: {}", e))?;
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
                Err(String::from("error reading from db"))
            }
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_transaction_location_by_hash(
        &self,
        tx_hash: EthTxHash,
        block_num: u64,
    ) -> Result<Option<TransactionLocation>, String> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::TxHash(&tx_hash));
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_num,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(String::from("error reading from db"));
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
                            Err(String::from("error reading from db"))
                        }),
                    None => Ok(None),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_block_number_by_hash(
        &self,
        block_hash: EthBlockHash,
        block_num: u64,
    ) -> Result<Option<u64>, String> {
        // create a one shot channel to retrieve the triedb result from the polling thread
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::BlockHash(&block_hash));
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_num,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        // await the result using request_receiver
        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(String::from("error reading from db"));
                }

                match result {
                    Some(rlp_output) => rlp_decode_block_num(rlp_output)
                        .map(|block_num| Ok(Some(block_num)))
                        .unwrap_or_else(|| {
                            error!("Decoding block number error");
                            Err(String::from("error reading from db"))
                        }),
                    None => Ok(None),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_call_frame(
        &self,
        txn_index: u64,
        block_num: u64,
    ) -> Result<Option<Vec<u8>>, String> {
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::CallFrame(Some(txn_index)));
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_num,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        match request_receiver.await {
            Ok(result) => {
                // sanity check to ensure completed_counter is equal to 1
                if completed_counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(String::from("error reading from db"));
                }

                Ok(result)
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_call_frames(&self, block_num: u64) -> Result<Vec<Vec<u8>>, String> {
        let (request_sender, request_receiver) = oneshot::channel();

        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Finalized, KeyInput::CallFrame(None));

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncTraverseRequest(AsyncTraverseRequest {
                request_sender,
                triedb_key,
                key_len_nibbles,
                block_num,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        match request_receiver.await {
            Ok(Some(rlp_call_frames)) => {
                let mut call_frames = rlp_call_frames
                    .into_iter()
                    .map(|TraverseEntry { key, value }| {
                        let idx: usize = alloy_rlp::decode_exact(key)?;

                        Ok((idx, value))
                    })
                    .collect::<Result<Vec<_>, alloy_rlp::Error>>()
                    .map_err(|err| {
                        error!(?err, "error decoding result from db");
                        String::from("error decoding from db")
                    })?;
                call_frames.sort_by_key(|(idx, _)| *idx);
                // check that indices are consecutive and start with 0
                if !call_frames
                    .iter()
                    .map(|(idx, _)| idx)
                    .zip(0..)
                    .all(|(&i, j)| i == j)
                {
                    return Err(format!(
                        "call frames missing from db, indices={:?}",
                        call_frames.iter().map(|(idx, _)| idx).collect::<Vec<_>>()
                    ));
                }
                Ok(call_frames
                    .into_iter()
                    .map(|(_, call_frame)| call_frame)
                    .collect())
            }
            Ok(None) => {
                error!("Error traversing db");
                Err(String::from("error traversing db"))
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        }
    }
}
