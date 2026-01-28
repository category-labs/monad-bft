// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::BTreeMap,
    fmt::Debug,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use alloy_consensus::{Header, ReceiptEnvelope, TxEnvelope};
use alloy_primitives::{keccak256, Address, FixedBytes, U256};
use alloy_rlp::{encode_list, BytesMut, Decodable, Encodable};
use futures::{channel::oneshot, FutureExt};
use monad_eth_types::AccountCodeOrHash;
use monad_triedb::{TraverseEntry, TriedbHandle};
use monad_types::{BlockId, Hash, SeqNum};
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use crate::{
    decode::{
        rlp_decode_account, rlp_decode_block_num, rlp_decode_storage_slot,
        rlp_decode_transaction_location,
    },
    key::{create_range_key, create_triedb_key, KeyInput, Version},
};

pub type EthAddress = [u8; 20];
pub type EthStorageKey = [u8; 32];
pub type EthCodeHash = [u8; 32];
pub type EthTxHash = [u8; 32];
pub type EthBlockHash = [u8; 32];

enum TriedbRequest {
    AsyncRangeGetRequest(RangeGetRequest),
    AsyncRequest(AsyncRequest),
    AsyncTraverseRequest(TraverseRequest),
}

struct RangeGetRequest {
    // a sender for the polling thread to send the result back to the request handler
    request_sender: oneshot::Sender<Option<Vec<TraverseEntry>>>,
    // prefix key is used to get the root of the subtrie
    prefix_key: Vec<u8>,
    prefix_key_len_nibbles: u8,
    // min key is inclusive in the range we want to retrieve
    min_triedb_key: Vec<u8>,
    min_key_len_nibbles: u8,
    // max key is not inclusive in the range we want to retrieve
    max_triedb_key: Vec<u8>,
    max_key_len_nibbles: u8,
    block_key: BlockKey,
}

struct TraverseRequest {
    // a sender for the polling thread to send the result back to the request handler
    request_sender: oneshot::Sender<Option<Vec<TraverseEntry>>>,
    // triedb_key and key_len_nibbles are used to read items from triedb
    triedb_key: Vec<u8>,
    key_len_nibbles: u8,
    block_key: BlockKey,
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
    block_key: BlockKey,
}

#[derive(Debug, Clone, Default)]
pub struct Account {
    pub nonce: u64,
    pub balance: U256,
    pub code_or_hash: AccountCodeOrHash,
}

#[derive(Debug, Clone, Default)]
pub struct BlockHeader {
    pub hash: FixedBytes<32>,
    pub header: Header,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
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

const MAX_QUEUE_BEFORE_POLL: usize = 100;
const MAX_POLL_COMPLETIONS: usize = usize::MAX;
const META_POLL_INTERVAL: Duration = Duration::from_millis(5);

fn get_latest_voted_block_key(triedb_handle: &TriedbHandle) -> Option<ProposedBlockKey> {
    let latest_voted_id_before = BlockId(Hash(triedb_handle.latest_voted_block_id()?));
    let latest_voted_seq_num = SeqNum(triedb_handle.latest_voted_block()?);
    let latest_voted_id_after = BlockId(Hash(triedb_handle.latest_voted_block_id()?));
    if latest_voted_id_before != latest_voted_id_after {
        return None;
    }
    Some(ProposedBlockKey(
        latest_voted_seq_num,
        latest_voted_id_before,
    ))
}

fn polling_thread(
    tokio_handle: tokio::runtime::Handle,
    triedb_path: PathBuf,
    node_lru_max_mem: u64,
    meta: Arc<Mutex<TriedbEnvMeta>>,
    receiver_read: mpsc::Receiver<TriedbRequest>,
    max_async_read_concurrency: usize,
    receiver_traverse: mpsc::Receiver<TriedbRequest>,
    max_async_traverse_concurrency: usize,
) {
    // create a new triedb handle for the polling thread
    let triedb_handle: TriedbHandle =
        TriedbHandle::try_new(&triedb_path, node_lru_max_mem).expect("triedb should exist in path");

    let triedb_async_read_concurrency_tracker: Arc<()> = Arc::new(());
    let triedb_async_traverse_concurrency_tracker: Arc<()> = Arc::new(());

    let mut last_meta_updated = Instant::now();
    let (mut last_finalized, mut last_voted) = {
        let meta = meta.lock().expect("poller mutex poisoned");
        (meta.latest_finalized, meta.latest_voted)
    };

    loop {
        if last_meta_updated.elapsed() > META_POLL_INTERVAL {
            let latest_finalized = FinalizedBlockKey(SeqNum(
                triedb_handle.latest_finalized_block().unwrap_or_default(),
            ));
            let mut latest_voted = BlockKey::Finalized(latest_finalized);
            for _ in 0..3 {
                if let Some(voted) = get_latest_voted_block_key(&triedb_handle) {
                    latest_voted = BlockKey::Proposed(voted);
                    break;
                }
                // retry in case of a race
            }

            last_meta_updated = Instant::now();

            let finalized_is_updated = last_finalized != latest_finalized;
            let voted_is_updated = last_voted != latest_voted;
            if finalized_is_updated || voted_is_updated {
                if finalized_is_updated {
                    last_finalized = latest_finalized;
                    populate_cache(
                        &tokio_handle,
                        &triedb_handle,
                        meta.clone(),
                        BlockKey::Finalized(latest_finalized),
                    );
                }
                if voted_is_updated {
                    last_voted = latest_voted;
                    if let BlockKey::Proposed(latest_voted) = latest_voted {
                        populate_cache(
                            &tokio_handle,
                            &triedb_handle,
                            meta.clone(),
                            BlockKey::Proposed(latest_voted),
                        );
                    }
                }

                let mut meta = meta.lock().expect("triedb poller mutex poisoned");
                meta.latest_finalized = latest_finalized;
                meta.latest_voted = latest_voted;
                while meta
                    .voted_proposals
                    .first_key_value()
                    .is_some_and(|(seq_num, _)| seq_num <= &latest_finalized.0)
                {
                    meta.voted_proposals.pop_first();
                }
                if let BlockKey::Proposed(ProposedBlockKey(voted_seq_num, voted_block_id)) =
                    latest_voted
                {
                    meta.voted_proposals.insert(voted_seq_num, voted_block_id);
                }
            }
        }

        triedb_handle.triedb_poll(false, MAX_POLL_COMPLETIONS);

        // get next request, or sleep for 1ms
        // prioritise async reads over async traversals
        // if we have neither reads nor traversals waiting, wait up to 1ms for an async read
        let mut maybe_request = None;
        if maybe_request.is_none()
            && Arc::strong_count(&triedb_async_read_concurrency_tracker)
                < max_async_read_concurrency
        {
            maybe_request = receiver_read.try_recv().ok();
        }
        if maybe_request.is_none()
            && Arc::strong_count(&triedb_async_traverse_concurrency_tracker)
                < max_async_traverse_concurrency
        {
            maybe_request = receiver_traverse.try_recv().ok();
        }
        if maybe_request.is_none() {
            std::thread::sleep(Duration::from_millis(1));
        }

        let mut num_queued = 0_usize;
        while let Some(triedb_request) = maybe_request {
            match triedb_request {
                TriedbRequest::AsyncRangeGetRequest(range_request) => {
                    triedb_handle.range_get_triedb_async(
                        &range_request.prefix_key,
                        range_request.prefix_key_len_nibbles,
                        &range_request.min_triedb_key,
                        range_request.min_key_len_nibbles,
                        &range_request.max_triedb_key,
                        range_request.max_key_len_nibbles,
                        range_request.block_key.seq_num().0,
                        range_request.request_sender,
                        triedb_async_read_concurrency_tracker.clone(),
                    );
                }
                TriedbRequest::AsyncTraverseRequest(traverse_request) => {
                    triedb_handle.traverse_triedb_async(
                        &traverse_request.triedb_key,
                        traverse_request.key_len_nibbles,
                        traverse_request.block_key.seq_num().0,
                        traverse_request.request_sender,
                        triedb_async_traverse_concurrency_tracker.clone(),
                    );
                }
                TriedbRequest::AsyncRequest(async_request) => {
                    // Process the request directly in this thread
                    // read_async will send back a future to request_receiver of oneshot channel
                    triedb_handle.read_async(
                        &async_request.triedb_key,
                        async_request.key_len_nibbles,
                        async_request.block_key.seq_num().0,
                        async_request.completed_counter,
                        async_request.request_sender,
                        triedb_async_read_concurrency_tracker.clone(),
                    );
                }
            }
            num_queued += 1;
            if num_queued > MAX_QUEUE_BEFORE_POLL {
                break;
            }
            // check for any other outstanding async read requests to queue up before polling
            maybe_request = receiver_read.try_recv().ok();
        }
    }
}

fn populate_cache(
    tokio_handle: &tokio::runtime::Handle,
    handle: &TriedbHandle,
    meta: Arc<Mutex<TriedbEnvMeta>>,
    block_key: BlockKey,
) {
    let tx_receiver = {
        let (tx_sender, tx_receiver) = oneshot::channel();

        // txn_index set to None to indicate return all transactions
        let (triedb_key, key_len_nibbles) =
            create_triedb_key(block_key.into(), KeyInput::TxIndex(None));

        handle.traverse_triedb_async(
            &triedb_key,
            key_len_nibbles,
            block_key.seq_num().0,
            tx_sender,
            // don't track concurrency
            Arc::new(()),
        );

        tx_receiver.map(move |maybe_tx| match maybe_tx {
            Ok(Some(rlp_transactions)) => parse_rlp_entries(rlp_transactions),
            Ok(None) => {
                error!(
                    ?block_key,
                    "Error traversing db while populating triedb cache txs"
                );
                Err(String::from("error traversing db"))
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        })
    };

    let receipt_receiver = {
        let (receipt_sender, receipt_receiver) = oneshot::channel();

        // receipt_index set to None to indicate return all receipts
        let (triedb_key, key_len_nibbles) =
            create_triedb_key(block_key.into(), KeyInput::ReceiptIndex(None));

        handle.traverse_triedb_async(
            &triedb_key,
            key_len_nibbles,
            block_key.seq_num().0,
            receipt_sender,
            // don't track concurrency
            Arc::new(()),
        );

        receipt_receiver.map(move |maybe_receipts| match maybe_receipts {
            Ok(Some(rlp_receipts)) => parse_rlp_entries(rlp_receipts),
            Ok(None) => {
                error!(
                    ?block_key,
                    "Error traversing db while populating triedb cache receipts"
                );
                Err(String::from("error traversing db"))
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        })
    };

    tokio_handle.spawn(async move {
        let transactions = tx_receiver.await;
        let receipts = receipt_receiver.await;

        if let (Ok(txs), Ok(rcpts)) = (transactions, receipts) {
            let transactions = Arc::new(txs);
            let receipts = Arc::new(rcpts);
            let mut meta = meta.lock().expect("mutex poisoned");
            meta.cache_manager
                .update_cache(block_key, transactions, receipts);
        }
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FinalizedBlockKey(pub SeqNum);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
// Note that SeqNum needs to be first, because this implements Ord/PartialOrd
pub struct ProposedBlockKey(pub SeqNum, pub BlockId);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKey {
    Finalized(FinalizedBlockKey),
    Proposed(ProposedBlockKey),
}

impl BlockKey {
    pub fn seq_num(&self) -> &SeqNum {
        match self {
            BlockKey::Finalized(FinalizedBlockKey(seq_num)) => seq_num,
            BlockKey::Proposed(ProposedBlockKey(seq_num, _)) => seq_num,
        }
    }

    pub fn block_id(&self) -> Option<&BlockId> {
        match self {
            BlockKey::Finalized(_) => None,
            BlockKey::Proposed(ProposedBlockKey(_, block_id)) => Some(block_id),
        }
    }
}

impl From<BlockKey> for Option<[u8; 32]> {
    fn from(key: BlockKey) -> Self {
        match key {
            BlockKey::Finalized(_) => None,
            BlockKey::Proposed(ProposedBlockKey(_, block_id)) => Some(block_id.0 .0),
        }
    }
}

impl From<BlockKey> for Version {
    fn from(key: BlockKey) -> Self {
        match key {
            BlockKey::Finalized(FinalizedBlockKey(_)) => Self::Finalized,
            BlockKey::Proposed(ProposedBlockKey(_, block_id)) => Self::Proposal(block_id),
        }
    }
}

pub trait Triedb: Debug {
    fn get_latest_finalized_block_key(&self) -> FinalizedBlockKey;
    /// returns a FinalizedBlockKey if latest_voted doesn't exist
    fn get_latest_voted_block_key(&self) -> BlockKey;
    /// returns None if block number was never known to be valid
    /// a known-to-be-finalized BlockKey will never return None
    ///
    /// the following sequence is safe:
    /// 1. get_block_key(block_num)
    ///    - if None, return Err
    /// 2. get_account(block_num)
    /// 3. get_state_availability(block_num)
    ///    - if None, return Err early
    fn get_block_key(&self, block_num: SeqNum) -> Option<BlockKey>;
    /// returns whether block number is available in triedb
    fn get_state_availability(
        &self,
        key: BlockKey,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;

    fn get_account(
        &self,
        key: BlockKey,
        addr: EthAddress,
    ) -> impl std::future::Future<Output = Result<Account, String>> + Send;
    fn get_storage_at(
        &self,
        key: BlockKey,
        addr: EthAddress,
        at: EthStorageKey,
    ) -> impl std::future::Future<Output = Result<String, String>> + Send;
    fn get_code_db(
        &self,
        key: BlockKey,
        code_hash: EthCodeHash,
    ) -> impl std::future::Future<Output = Result<String, String>> + Send;
    fn get_receipt(
        &self,
        key: BlockKey,
        txn_index: u64,
    ) -> impl std::future::Future<Output = Result<Option<ReceiptWithLogIndex>, String>> + Send;
    fn get_receipts(
        &self,
        key: BlockKey,
    ) -> impl std::future::Future<Output = Result<Vec<ReceiptWithLogIndex>, String>> + Send + Sync;
    fn get_transaction(
        &self,
        key: BlockKey,
        txn_index: u64,
    ) -> impl std::future::Future<Output = Result<Option<TxEnvelopeWithSender>, String>> + Send;
    fn get_transactions(
        &self,
        key: BlockKey,
    ) -> impl std::future::Future<Output = Result<Vec<TxEnvelopeWithSender>, String>> + Send + Sync;
    fn get_block_header(
        &self,
        key: BlockKey,
    ) -> impl std::future::Future<Output = Result<Option<BlockHeader>, String>> + Send + Sync;
    fn get_transaction_location_by_hash(
        &self,
        key: BlockKey,
        tx_hash: EthTxHash,
    ) -> impl std::future::Future<Output = Result<Option<TransactionLocation>, String>> + Send;
    fn get_block_number_by_hash(
        &self,
        key: BlockKey,
        block_hash: EthBlockHash,
    ) -> impl std::future::Future<Output = Result<Option<u64>, String>> + Send;

    fn get_call_frame(
        &self,
        key: BlockKey,
        txn_index: u64,
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, String>> + Send;

    fn get_call_frames(
        &self,
        key: BlockKey,
    ) -> impl std::future::Future<Output = Result<Vec<Vec<u8>>, String>> + Send;
}

pub trait TriedbPath {
    fn path(&self) -> PathBuf;
}

#[derive(Clone)]
pub struct TriedbEnv {
    triedb_path: PathBuf,
    mpsc_sender: mpsc::SyncSender<TriedbRequest>, // sender for tasks
    mpsc_sender_traverse: mpsc::SyncSender<TriedbRequest>,

    meta: Arc<Mutex<TriedbEnvMeta>>,
}

struct TriedbEnvMeta {
    latest_finalized: FinalizedBlockKey,
    latest_voted: BlockKey,
    voted_proposals: BTreeMap<SeqNum, BlockId>,

    cache_manager: CacheManager,
}

#[derive(Clone)]
struct BlockCache {
    transactions: Arc<Vec<TxEnvelopeWithSender>>,
    receipts: Arc<Vec<ReceiptWithLogIndex>>,
}

struct CacheManager {
    finalized_cache: BTreeMap<FinalizedBlockKey, BlockCache>,
    voted_cache: BTreeMap<ProposedBlockKey, BlockCache>,
    max_finalized_block_cache_len: usize,
    max_voted_block_cache_len: usize,
}

impl CacheManager {
    fn new(max_finalized_block_cache_len: usize, max_voted_block_cache_len: usize) -> Self {
        Self {
            finalized_cache: Default::default(),
            voted_cache: Default::default(),
            max_finalized_block_cache_len,
            max_voted_block_cache_len,
        }
    }

    fn get_cache(&self, key: &BlockKey) -> Option<BlockCache> {
        match key {
            BlockKey::Finalized(finalized) => self.finalized_cache.get(finalized).cloned(),
            BlockKey::Proposed(voted) => self.voted_cache.get(voted).cloned(),
        }
    }

    fn update_cache(
        &mut self,
        block_key: BlockKey,
        transactions: Arc<Vec<TxEnvelopeWithSender>>,
        receipts: Arc<Vec<ReceiptWithLogIndex>>,
    ) {
        match block_key {
            BlockKey::Finalized(finalized) => {
                self.finalized_cache.insert(
                    finalized,
                    BlockCache {
                        transactions,
                        receipts,
                    },
                );
                while self.finalized_cache.len() > self.max_finalized_block_cache_len {
                    self.finalized_cache.pop_first();
                }
            }
            BlockKey::Proposed(proposed) => {
                self.voted_cache.insert(
                    proposed,
                    BlockCache {
                        transactions,
                        receipts,
                    },
                );
                while self.voted_cache.len() > self.max_voted_block_cache_len {
                    self.voted_cache.pop_first();
                }
            }
        }
    }
}

impl std::fmt::Debug for TriedbEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TriedbEnv")
            .field("path", &self.triedb_path)
            .finish()
    }
}

impl TriedbEnv {
    pub fn new(
        triedb_path: &Path,
        node_lru_max_mem: u64,
        max_buffered_read_requests: usize,
        max_async_read_concurrency: usize,
        max_buffered_traverse_requests: usize,
        max_async_traverse_concurrency: usize,
        max_finalized_block_cache_len: usize,
        max_voted_block_cache_len: usize,
    ) -> Self {
        let latest_finalized = {
            let triedb_handle: TriedbHandle = TriedbHandle::try_new(triedb_path, node_lru_max_mem)
                .expect("triedb should exist in path");
            SeqNum(triedb_handle.latest_finalized_block().unwrap_or_default())
        };
        let meta = Arc::new(Mutex::new(TriedbEnvMeta {
            latest_finalized: FinalizedBlockKey(latest_finalized),
            latest_voted: BlockKey::Finalized(FinalizedBlockKey(latest_finalized)),
            voted_proposals: Default::default(),
            cache_manager: CacheManager::new(
                max_finalized_block_cache_len,
                max_voted_block_cache_len,
            ),
        }));

        // create mpsc channels where sender are incoming requests, and the receiver is the triedb poller
        let (sender_read, receiver_read) =
            mpsc::sync_channel::<TriedbRequest>(max_buffered_read_requests);
        let (sender_traverse, receiver_traverse) =
            mpsc::sync_channel::<TriedbRequest>(max_buffered_traverse_requests);

        // spawn the polling thread in a dedicated thread
        let meta_cloned = meta.clone();
        let triedb_path_cloned = triedb_path.to_path_buf();
        let tokio_handle = tokio::runtime::Handle::current();

        thread::Builder::new()
            .name("monad-rpc-poll".into())
            .spawn(move || {
                polling_thread(
                    tokio_handle,
                    triedb_path_cloned,
                    node_lru_max_mem,
                    meta_cloned,
                    receiver_read,
                    max_async_read_concurrency,
                    receiver_traverse,
                    max_async_traverse_concurrency,
                );
            })
            .expect("failed to spawn rpc poll");

        Self {
            triedb_path: triedb_path.to_path_buf(),
            mpsc_sender: sender_read,
            mpsc_sender_traverse: sender_traverse,
            meta,
        }
    }

    fn get_block_cache(&self, key: &BlockKey) -> Option<BlockCache> {
        self.meta
            .lock()
            .expect("mutex poisoned")
            .cache_manager
            .get_cache(key)
    }

    fn send_async_request(
        &self,
        block_key: BlockKey,
        key_input: KeyInput,
    ) -> Result<(oneshot::Receiver<Option<Vec<u8>>>, Arc<AtomicUsize>), String> {
        let (request_sender, request_receiver) = oneshot::channel();
        let (triedb_key, key_len_nibbles) = create_triedb_key(block_key.into(), key_input);
        let completed_counter = Arc::new(AtomicUsize::new(0));

        if let Err(e) = self
            .mpsc_sender
            .try_send(TriedbRequest::AsyncRequest(AsyncRequest {
                request_sender,
                completed_counter: completed_counter.clone(),
                triedb_key,
                key_len_nibbles,
                block_key,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        Ok((request_receiver, completed_counter))
    }

    async fn handle_async_result<T, F>(
        receiver: oneshot::Receiver<Option<Vec<u8>>>,
        counter: Arc<AtomicUsize>,
        decoder: F,
    ) -> Result<Option<T>, String>
    where
        F: FnOnce(Vec<u8>) -> Result<T, String>,
    {
        match receiver.await {
            Ok(result) => {
                if counter.load(SeqCst) != 1 {
                    error!("Unexpected completed_counter value");
                    return Err(String::from("error reading from db"));
                }

                match result {
                    Some(data) => decoder(data).map(Some),
                    None => Ok(None),
                }
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        }
    }

    async fn handle_async_request<T, F>(
        &self,
        block_key: BlockKey,
        key_input: KeyInput<'_>,
        decoder: F,
    ) -> Result<Option<T>, String>
    where
        F: FnOnce(Vec<u8>) -> Result<T, String>,
    {
        let (receiver, counter) = self.send_async_request(block_key, key_input)?;
        TriedbEnv::handle_async_result(receiver, counter, decoder).await
    }

    fn send_traverse_request(
        &self,
        block_key: BlockKey,
        key_input: KeyInput,
    ) -> Result<oneshot::Receiver<Option<Vec<TraverseEntry>>>, String> {
        let (request_sender, request_receiver) = oneshot::channel();
        let (triedb_key, key_len_nibbles) = create_triedb_key(block_key.into(), key_input);

        if let Err(e) = self
            .mpsc_sender_traverse
            .try_send(TriedbRequest::AsyncTraverseRequest(TraverseRequest {
                request_sender,
                triedb_key,
                key_len_nibbles,
                block_key,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        Ok(request_receiver)
    }

    async fn handle_traverse_result<T>(
        receiver: oneshot::Receiver<Option<Vec<TraverseEntry>>>,
        parser: impl FnOnce(Vec<TraverseEntry>) -> Result<Vec<T>, String>,
    ) -> Result<Vec<T>, String> {
        match receiver.await {
            Ok(Some(entries)) => parser(entries),
            Ok(None) => {
                error!("Error traversing db while traversing triedb result");
                Err(String::from("error traversing db"))
            }
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        }
    }

    fn send_async_range_request(
        &self,
        block_key: BlockKey,
        key_input: KeyInput,
        txn_index: u64,
        txn_count: u64,
    ) -> Result<oneshot::Receiver<Option<Vec<TraverseEntry>>>, String> {
        let (request_sender, request_receiver) = oneshot::channel();
        let (prefix_key, prefix_key_len_nibbles) = create_triedb_key(block_key.into(), key_input);
        let (min_triedb_key, min_key_len_nibbles) = create_range_key(txn_index);
        let (max_triedb_key, max_key_len_nibbles) = create_range_key(txn_index + txn_count);

        if let Err(e) = self
            .mpsc_sender
            .clone()
            .try_send(TriedbRequest::AsyncRangeGetRequest(RangeGetRequest {
                request_sender,
                prefix_key,
                prefix_key_len_nibbles,
                min_triedb_key,
                min_key_len_nibbles,
                max_triedb_key,
                max_key_len_nibbles,
                block_key,
            }))
        {
            warn!("Polling thread channel full: {e}");
            return Err(String::from("error reading from db due to rate limit"));
        }

        Ok(request_receiver)
    }

    async fn handle_async_range_result<T, F>(
        receiver: oneshot::Receiver<Option<Vec<TraverseEntry>>>,
        decoder: F,
    ) -> Result<Option<T>, String>
    where
        F: FnOnce(Vec<TraverseEntry>) -> Result<T, String>,
    {
        match receiver.await {
            Ok(result) => match result {
                Some(data) => decoder(data).map(Some),
                None => Ok(None),
            },
            Err(e) => {
                error!("Error awaiting result: {e}");
                Err(String::from("error reading from db"))
            }
        }
    }

    async fn handle_async_range_request<T, F>(
        &self,
        block_key: BlockKey,
        key_input: KeyInput<'_>,
        txn_index: u64,
        txn_count: u64,
        decoder: F,
    ) -> Result<Option<T>, String>
    where
        F: FnOnce(Vec<TraverseEntry>) -> Result<T, String>,
    {
        let receiver = self.send_async_range_request(block_key, key_input, txn_index, txn_count)?;
        TriedbEnv::handle_async_range_result(receiver, decoder).await
    }

    async fn handle_traverse_request<T>(
        &self,
        block_key: BlockKey,
        key_input: KeyInput<'_>,
        parser: impl FnOnce(Vec<TraverseEntry>) -> Result<Vec<T>, String>,
    ) -> Result<Vec<T>, String> {
        let receiver = self.send_traverse_request(block_key, key_input)?;
        TriedbEnv::handle_traverse_result(receiver, parser).await
    }
}

impl TriedbPath for TriedbEnv {
    fn path(&self) -> PathBuf {
        self.triedb_path.clone()
    }
}

impl Triedb for TriedbEnv {
    fn get_latest_finalized_block_key(&self) -> FinalizedBlockKey {
        let meta = self.meta.lock().expect("mutex poisoned");
        meta.latest_finalized
    }
    fn get_latest_voted_block_key(&self) -> BlockKey {
        let meta = self.meta.lock().expect("mutex poisoned");
        meta.latest_voted
    }
    fn get_block_key(&self, seq_num: SeqNum) -> Option<BlockKey> {
        let meta = self.meta.lock().expect("mutex poisoned");
        if let Some(&voted_block_id) = meta.voted_proposals.get(&seq_num) {
            // there's an unfinalized, voted proposal with this seq_num
            Some(BlockKey::Proposed(ProposedBlockKey(
                seq_num,
                voted_block_id,
            )))
        } else if seq_num <= meta.latest_finalized.0 {
            // this seq_num is finalized
            Some(BlockKey::Finalized(FinalizedBlockKey(seq_num)))
        } else {
            // get_block_key must return a state that was valid at some point
            // thus, it's not safe to default to finalized
            None
        }
    }

    async fn get_state_availability(&self, block_key: BlockKey) -> Result<bool, String> {
        match self
            .handle_async_request(block_key, KeyInput::State, Ok)
            .await?
        {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_account(&self, block_key: BlockKey, addr: EthAddress) -> Result<Account, String> {
        match self
            .handle_async_request(block_key, KeyInput::Address(&addr), |data| {
                rlp_decode_account(data).ok_or_else(|| String::from("Decoding account error"))
            })
            .await?
        {
            Some(account) => Ok(Account {
                nonce: account.nonce,
                balance: account.balance,
                code_or_hash: account.code_or_hash,
            }),
            None => Ok(Account::default()),
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_storage_at(
        &self,
        block_key: BlockKey,
        addr: EthAddress,
        at: EthStorageKey,
    ) -> Result<String, String> {
        match self
            .handle_async_request(block_key, KeyInput::Storage(&addr, &at), |data| {
                rlp_decode_storage_slot(data)
                    .ok_or_else(|| String::from("Decoding storage slot error"))
            })
            .await?
        {
            Some(storage_slot) => Ok(format!("0x{}", hex::encode(storage_slot))),
            None => Ok(
                "0x0000000000000000000000000000000000000000000000000000000000000000".to_string(),
            ),
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_code_db(
        &self,
        block_key: BlockKey,
        code_hash: EthCodeHash,
    ) -> Result<String, String> {
        match self
            .handle_async_request(block_key, KeyInput::CodeHash(&code_hash), |data| {
                Ok(format!("0x{}", hex::encode(data)))
            })
            .await?
        {
            Some(code) => Ok(code),
            None => Ok("0x".to_string()),
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_receipt(
        &self,
        block_key: BlockKey,
        receipt_index: u64,
    ) -> Result<Option<ReceiptWithLogIndex>, String> {
        if let Some(cache) = self.get_block_cache(&block_key) {
            return Ok(cache.receipts.get(receipt_index as usize).cloned());
        }

        self.handle_async_request(
            block_key,
            KeyInput::ReceiptIndex(Some(receipt_index)),
            |data| {
                let mut rlp_buf = data.as_slice();
                let receipt = ReceiptWithLogIndex::decode(&mut rlp_buf)
                    .map_err(|e| format!("decode receipt failed: {}", e))?;
                Ok(receipt)
            },
        )
        .await
    }

    async fn get_receipts(&self, block_key: BlockKey) -> Result<Vec<ReceiptWithLogIndex>, String> {
        if let Some(receipts) = self.get_block_cache(&block_key) {
            // TODO avoid copy here?
            return Ok((*receipts.receipts).clone());
        }

        self.handle_traverse_request(block_key, KeyInput::ReceiptIndex(None), parse_rlp_entries)
            .await
    }

    #[tracing::instrument(level = "debug")]
    async fn get_transaction(
        &self,
        block_key: BlockKey,
        txn_index: u64,
    ) -> Result<Option<TxEnvelopeWithSender>, String> {
        if let Some(cache) = self.get_block_cache(&block_key) {
            return Ok(cache.transactions.get(txn_index as usize).cloned());
        }

        self.handle_async_request(block_key, KeyInput::TxIndex(Some(txn_index)), |data| {
            let mut rlp_buf = data.as_slice();
            let transaction = TxEnvelopeWithSender::decode(&mut rlp_buf)
                .map_err(|e| format!("decode transaction failed: {}", e))?;
            Ok(transaction)
        })
        .await
    }

    #[tracing::instrument(level = "debug")]
    async fn get_transactions(
        &self,
        block_key: BlockKey,
    ) -> Result<Vec<TxEnvelopeWithSender>, String> {
        if let Some(txs) = self.get_block_cache(&block_key) {
            // TODO avoid copy here?
            return Ok((*txs.transactions).clone());
        }

        self.handle_traverse_request(block_key, KeyInput::TxIndex(None), parse_rlp_entries)
            .await
    }

    #[tracing::instrument(level = "debug")]
    async fn get_block_header(&self, block_key: BlockKey) -> Result<Option<BlockHeader>, String> {
        self.handle_async_request(block_key, KeyInput::BlockHeader, |data| {
            let mut rlp_buf = data.as_slice();
            let block_header = Header::decode(&mut rlp_buf)
                .map_err(|e| format!("decode block header failed: {}", e))?;
            Ok(BlockHeader {
                hash: keccak256(&data),
                header: block_header,
            })
        })
        .await
    }

    #[tracing::instrument(level = "debug")]
    async fn get_transaction_location_by_hash(
        &self,
        block_key: BlockKey,
        tx_hash: EthTxHash,
    ) -> Result<Option<TransactionLocation>, String> {
        match self
            .handle_async_request(block_key, KeyInput::TxHash(&tx_hash), |data| {
                rlp_decode_transaction_location(data)
                    .ok_or_else(|| String::from("decode transaction location error"))
            })
            .await?
        {
            Some((block_num, tx_index)) => Ok(Some(TransactionLocation {
                block_num,
                tx_index,
            })),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_block_number_by_hash(
        &self,
        block_key: BlockKey,
        block_hash: EthBlockHash,
    ) -> Result<Option<u64>, String> {
        self.handle_async_request(block_key, KeyInput::BlockHash(&block_hash), |data| {
            rlp_decode_block_num(data).ok_or_else(|| String::from("decode block number error"))
        })
        .await
    }

    #[tracing::instrument(level = "debug")]
    async fn get_call_frame(
        &self,
        block_key: BlockKey,
        txn_index: u64,
    ) -> Result<Option<Vec<u8>>, String> {
        self.handle_async_range_request(
            block_key,
            KeyInput::CallFrame,
            txn_index,
            1,
            |rlp_call_frames| {
                if rlp_call_frames.is_empty() {
                    return Ok(Vec::new());
                }

                let grouped_frames = parse_call_frames(rlp_call_frames)?;

                // we should only have one transaction index that is equivalent to txn_index
                if grouped_frames.len() != 1 {
                    warn!("Incorrect key length");
                    return Err(String::from("error decoding from db"));
                }

                match grouped_frames.into_iter().next() {
                    Some((idx, chunks)) => {
                        if idx as u64 != txn_index {
                            warn!("Incorrect transaction index");
                            return Err(String::from("error decoding from db"));
                        }
                        let complete_call_frame = process_call_frame_chunks(chunks)?;
                        Ok(complete_call_frame)
                    }
                    None => Err(String::from("error decoding from db")),
                }
            },
        )
        .await
        .map(|v| match v {
            Some(v) if v.is_empty() => None,
            Some(v) => Some(v),
            None => None,
        })
    }

    #[tracing::instrument(level = "debug")]
    async fn get_call_frames(&self, block_key: BlockKey) -> Result<Vec<Vec<u8>>, String> {
        self.handle_traverse_request(block_key, KeyInput::CallFrame, |rlp_call_frames| {
            // txn_index => (chunk_index, rlp_call_frame)
            let grouped_frames = parse_call_frames(rlp_call_frames)?;

            // check that transaction indices are consecutive and start with 0
            if !grouped_frames.keys().copied().zip(0..).all(|(i, j)| i == j) {
                return Err(format!(
                    "call frames missing from db, transaction indices={:?}",
                    grouped_frames.keys().collect::<Vec<_>>()
                ));
            }
            let call_frames = grouped_frames
                .into_iter()
                .map(|(txn_idx, chunks)| {
                    process_call_frame_chunks(chunks)
                        .map_err(|e| format!("chunks missing for transaction {}: {}", txn_idx, e))
                })
                .collect::<Result<Vec<_>, String>>()?;

            Ok(call_frames)
        })
        .await
    }
}

fn parse_rlp_entries<T>(rlp_entries: Vec<TraverseEntry>) -> Result<Vec<T>, String>
where
    T: alloy_rlp::Decodable,
{
    let mut entries = rlp_entries
        .into_iter()
        .map(|TraverseEntry { key, value }| {
            let idx: usize = alloy_rlp::decode_exact(key)?;
            let entry: T = alloy_rlp::decode_exact(value)?;

            Ok((idx, entry))
        })
        .collect::<Result<Vec<_>, alloy_rlp::Error>>()
        .map_err(|err| {
            error!(?err, "error decoding result from db");
            String::from("error decoding from db")
        })?;
    entries.sort_by_key(|(idx, _)| *idx);
    // check that indices are consecutive and start with 0
    if !entries
        .iter()
        .map(|(idx, _)| idx)
        .zip(0..)
        .all(|(&i, j)| i == j)
    {
        return Err(format!(
            "entries missing from db, indices={:?}",
            entries.iter().map(|(idx, _)| idx).collect::<Vec<_>>()
        ));
    }
    Ok(entries.into_iter().map(|(_, entry)| entry).collect())
}

fn parse_call_frames(
    entries: Vec<TraverseEntry>,
) -> Result<BTreeMap<u32, BTreeMap<u8, Vec<u8>>>, String> {
    let mut grouped_frames: BTreeMap<u32, BTreeMap<u8, Vec<u8>>> = BTreeMap::new();

    for TraverseEntry { key, value } in entries {
        // decode the key as a tuple of (txn_index, chunk_index)
        if key.len() != 5 {
            warn!("Incorrect key length");
            return Err(String::from("error decoding from db"));
        }

        // first 4 bytes is txn_index
        let txn_index = u32::from_be_bytes(key[0..4].try_into().unwrap_or_default());
        // 5th byte is chunk_index
        let chunk_index = key[4];

        grouped_frames
            .entry(txn_index)
            .or_default()
            .insert(chunk_index, value);
    }

    Ok(grouped_frames)
}

fn process_call_frame_chunks(chunks: BTreeMap<u8, Vec<u8>>) -> Result<Vec<u8>, String> {
    // check that chunk indices are consecutive and start with 0
    if !chunks.keys().copied().zip(0..).all(|(i, j)| i == j) {
        return Err(format!(
            "chunk indices={:?}",
            chunks.keys().collect::<Vec<_>>()
        ));
    }

    // concatenate chunks in order
    Ok(chunks.into_values().flatten().collect())
}
