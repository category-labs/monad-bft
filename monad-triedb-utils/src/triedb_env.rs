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

use alloy_consensus::Header;
use alloy_primitives::{keccak256, U256, KECCAK256_EMPTY};
use alloy_rlp::Decodable;
use auto_impl::auto_impl;
use futures::{channel::oneshot, FutureExt};
use monad_eth_types::{
    BlockHeader, EthAccount, EthAddress, EthBlockHash, EthCode, EthCodeHash, EthStorageKey,
    EthStorageSlot, EthTxHash, ReceiptWithLogIndex, TransactionLocation, TxEnvelopeWithSender,
};
use monad_triedb::{
    compute_page_key, compute_slot_offset, decode_storage_page_slot, BlobCategory, BlockGuard,
    KvHandle, TraverseEntry, TriedbHandle,
};
use monad_types::{BlockId, Hash, SeqNum};
use tracing::{error, warn};

use crate::{
    decode::{
        rlp_decode_account, rlp_decode_block_num, rlp_decode_storage_slot,
        rlp_decode_transaction_location,
    },
    key::{create_range_key, create_triedb_key, KeyInput, Version},
};

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

fn get_latest_proposed_block_key(triedb_handle: &TriedbHandle) -> Option<ProposedBlockKey> {
    let latest_proposed_id_before = BlockId(Hash(triedb_handle.latest_proposed_block_id()?));
    let latest_proposed_seq_num = SeqNum(triedb_handle.latest_proposed_block()?);
    let latest_proposed_id_after = BlockId(Hash(triedb_handle.latest_proposed_block_id()?));
    if latest_proposed_id_before != latest_proposed_id_after {
        return None;
    }
    Some(ProposedBlockKey(
        latest_proposed_seq_num,
        latest_proposed_id_before,
    ))
}

/// The three block-tag keys from KV's own cursors, or `None` if KV could not
/// give a stable snapshot -- in which case the caller stays on triedb for this
/// tick.
///
/// KV is the read authority, so taking the tags from it keeps what RPC believes
/// exists consistent with what it can actually read: a key derived from KV
/// always pins. KV's cursors can lag triedb's by a block (each is published in
/// its own part of the commit), which shows up as the newest block being
/// reported a moment later, not as a wrong answer.
///
/// The defaults mirror the triedb path: voted falls back to finalized, and
/// proposed to voted, when consensus has not stamped them.
fn kv_tag_keys(kv: &KvHandle) -> Option<(FinalizedBlockKey, BlockKey, BlockKey)> {
    let tags = kv.tags()?;
    let finalized = FinalizedBlockKey(SeqNum(tags.finalized?));
    let proposed_key = |pair: Option<(u64, [u8; 32])>| {
        pair.map(|(block, id)| {
            BlockKey::Proposed(ProposedBlockKey(SeqNum(block), BlockId(Hash(id))))
        })
    };
    let voted = proposed_key(tags.voted).unwrap_or(BlockKey::Finalized(finalized));
    let proposed = proposed_key(tags.proposed).unwrap_or(voted);
    Some((finalized, voted, proposed))
}

fn polling_thread(
    tokio_handle: tokio::runtime::Handle,
    triedb_path: PathBuf,
    node_lru_max_mem: u64,
    kv: Option<Arc<KvHandle>>,
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
    let (mut last_finalized, mut last_voted, mut last_proposed) = {
        let meta = meta.lock().expect("poller mutex poisoned");
        (
            meta.latest_finalized,
            meta.latest_voted,
            meta.latest_proposed,
        )
    };

    loop {
        if last_meta_updated.elapsed() > META_POLL_INTERVAL {
            // KV serves the tags when it is configured; its snapshot is already
            // internally consistent, so it needs none of the re-read retries
            // the triedb path below uses to catch a torn (block, id) pair.
            let kv_keys = kv.as_deref().and_then(kv_tag_keys);
            let (latest_finalized, latest_voted, latest_proposed) = match kv_keys {
                Some(keys) => keys,
                None => {
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

                    let mut latest_proposed = latest_voted;
                    for _ in 0..3 {
                        if let Some(proposed) = get_latest_proposed_block_key(&triedb_handle) {
                            latest_proposed = BlockKey::Proposed(proposed);
                            break;
                        }
                        // retry in case of a race
                    }
                    (latest_finalized, latest_voted, latest_proposed)
                }
            };

            last_meta_updated = Instant::now();

            let finalized_is_updated = last_finalized != latest_finalized;
            let voted_is_updated = last_voted != latest_voted;
            let proposed_is_updated = last_proposed != latest_proposed;
            if finalized_is_updated || voted_is_updated || proposed_is_updated {
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

                if proposed_is_updated {
                    last_proposed = latest_proposed;
                    if let BlockKey::Proposed(_) = latest_proposed {
                        populate_cache(
                            &tokio_handle,
                            &triedb_handle,
                            meta.clone(),
                            latest_proposed,
                        );
                    }
                }

                let mut meta = meta.lock().expect("triedb poller mutex poisoned");
                meta.latest_finalized = latest_finalized;
                meta.latest_voted = latest_voted;
                meta.latest_proposed = latest_proposed;
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

/// A [`BlockKey`] together with the KV pin that keeps that block's data alive.
///
/// KV reclaims a block's pages once it falls out of the retained window, so a
/// request that reads KV must hold a pin for as long as it is reading. The pin
/// is taken once, where the block is resolved, and then passed to each read --
/// so a read physically cannot be issued without one being in scope.
///
/// `guard` is `None` when there is no KV to pin: triedb-only deployments and
/// the mock. KV-backed reads fall back to triedb in that case.
#[derive(Debug, Clone)]
pub struct PinnedBlock {
    key: BlockKey,
    guard: Option<Arc<BlockGuard>>,
}

impl PinnedBlock {
    /// Unpinned: reads are served by triedb.
    pub fn unpinned(key: BlockKey) -> Self {
        Self { key, guard: None }
    }

    pub fn new(key: BlockKey, guard: Option<Arc<BlockGuard>>) -> Self {
        Self { key, guard }
    }

    pub fn key(&self) -> BlockKey {
        self.key
    }

    pub fn seq_num(&self) -> &SeqNum {
        self.key.seq_num()
    }

    /// The live pin, if this block is held in KV.
    pub fn guard(&self) -> Option<&BlockGuard> {
        self.guard.as_deref()
    }
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

#[auto_impl(Arc)]
pub trait Triedb: Debug {
    fn get_latest_finalized_block_key(&self) -> FinalizedBlockKey;
    /// returns a FinalizedBlockKey if latest_voted doesn't exist
    fn get_latest_voted_block_key(&self) -> BlockKey;
    fn get_latest_proposed_block_key(&self) -> BlockKey;
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

    /// Pin `key` for the duration of a request, so KV cannot reclaim that
    /// block's data while it is being read. Call this once, where the block is
    /// resolved, and pass the result to the state reads below.
    ///
    /// Always succeeds: with no KV configured, or if the block is not held in
    /// KV, the result is unpinned and those reads fall back to triedb.
    fn pin_block(&self, key: BlockKey) -> PinnedBlock;

    /// returns whether block number is available in triedb
    fn get_state_availability(
        &self,
        block: &PinnedBlock,
    ) -> impl std::future::Future<Output = Result<bool, String>> + Send;

    fn get_account(
        &self,
        block: &PinnedBlock,
        addr: EthAddress,
    ) -> impl std::future::Future<Output = Result<EthAccount, String>> + Send;
    fn get_storage_at(
        &self,
        block: &PinnedBlock,
        addr: EthAddress,
        at: EthStorageKey,
    ) -> impl std::future::Future<Output = Result<EthStorageSlot, String>> + Send;
    fn get_code(
        &self,
        block: &PinnedBlock,
        code_hash: EthCodeHash,
    ) -> impl std::future::Future<Output = Result<EthCode, String>> + Send;
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

#[auto_impl(Arc)]
pub trait TriedbPath {
    fn path(&self) -> PathBuf;
}

#[derive(Clone)]
pub struct TriedbEnv {
    triedb_path: PathBuf,
    mpsc_sender: mpsc::SyncSender<TriedbRequest>, // sender for tasks
    mpsc_sender_traverse: mpsc::SyncSender<TriedbRequest>,

    meta: Arc<Mutex<TriedbEnvMeta>>,

    // True if the primary timeline is page-encoded (Monad state machine). Read
    // once at open; safe because the primary's encoding only changes via an
    // offline promote (node stopped, then restarted), so a running RPC re-reads
    // it on the next open.
    page_encoded: bool,

    // KV read store, when one is configured ($KVDB_IMAGE). State reads are
    // served from here under a pin; everything else still goes to triedb.
    // `None` leaves every read on triedb, so this is inert unless enabled.
    kv: Option<Arc<KvHandle>>,
}

struct TriedbEnvMeta {
    latest_finalized: FinalizedBlockKey,
    latest_voted: BlockKey,
    latest_proposed: BlockKey,
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

/// The KV read store named by `$KVDB_IMAGE`, if one is configured and opens.
/// The path is the `.kvhdr` sidecar; execution must already be running, since
/// opening attaches the hazard segment and metadata it owns. Absent or
/// unopenable means every read stays on triedb.
fn kv_from_env() -> Option<Arc<KvHandle>> {
    let image = std::env::var("KVDB_IMAGE").ok()?;
    match KvHandle::try_new(Path::new(&image)) {
        Some(handle) => Some(Arc::new(handle)),
        None => {
            warn!(
                image,
                "KVDB_IMAGE set but the KV store did not open; \
                 serving all reads from triedb"
            );
            None
        }
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
        Self::with_kv(
            triedb_path,
            node_lru_max_mem,
            max_buffered_read_requests,
            max_async_read_concurrency,
            max_buffered_traverse_requests,
            max_async_traverse_concurrency,
            max_finalized_block_cache_len,
            max_voted_block_cache_len,
            kv_from_env(),
        )
    }

    /// As [`Self::new`], with the KV store given explicitly rather than taken
    /// from the environment. Passing `None` gives an environment that is wholly
    /// on triedb, **including its own poller** -- which is what makes an A/B
    /// comparison of the block tags possible, since those are served out of
    /// poller state rather than read per call.
    #[allow(clippy::too_many_arguments)]
    pub fn with_kv(
        triedb_path: &Path,
        node_lru_max_mem: u64,
        max_buffered_read_requests: usize,
        max_async_read_concurrency: usize,
        max_buffered_traverse_requests: usize,
        max_async_traverse_concurrency: usize,
        max_finalized_block_cache_len: usize,
        max_voted_block_cache_len: usize,
        kv: Option<Arc<KvHandle>>,
    ) -> Self {
        let triedb_handle: TriedbHandle = TriedbHandle::try_new(triedb_path, node_lru_max_mem)
            .expect("triedb should exist in path");
        let page_encoded = triedb_handle.is_page_encoded();
        let latest_finalized = FinalizedBlockKey(SeqNum(
            triedb_handle.latest_finalized_block().unwrap_or_default(),
        ));

        let meta = Arc::new(Mutex::new(TriedbEnvMeta {
            latest_finalized,
            latest_voted: BlockKey::Finalized(latest_finalized),
            latest_proposed: BlockKey::Finalized(latest_finalized),
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
        let kv_cloned = kv.clone();

        thread::Builder::new()
            .name("monad-rpc-poll".into())
            .spawn(move || {
                polling_thread(
                    tokio_handle,
                    triedb_path_cloned,
                    node_lru_max_mem,
                    kv_cloned,
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
            page_encoded,
            kv,
        }
    }

    fn get_block_cache(&self, key: &BlockKey) -> Option<BlockCache> {
        self.meta
            .lock()
            .expect("mutex poisoned")
            .cache_manager
            .get_cache(key)
    }

    /// Pin `key` for the duration of one per-block read, or `None` if KV cannot
    /// serve it (not configured, or the block is outside KV's retained window)
    /// -- in which case that read stays on triedb.
    ///
    /// Unlike the state reads, the per-block reads take their pin here instead
    /// of receiving one from the caller: each returns its whole result in a
    /// single call, so a pin held for that call is all they need. Threading a
    /// request-long pin in would mean putting it through the data-source trait
    /// that also fronts the archive backends, which have no notion of one.
    fn kv_pin(&self, key: BlockKey) -> Option<BlockGuard> {
        kv_protect(self.kv.as_ref()?, key)
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
    fn get_latest_proposed_block_key(&self) -> BlockKey {
        let meta = self.meta.lock().expect("mutex poisoned");
        meta.latest_proposed
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
        } else if seq_num == *meta.latest_proposed.seq_num() {
            Some(meta.latest_proposed)
        } else {
            // get_block_key must return a state that was valid at some point
            // thus, it's not safe to default to finalized
            None
        }
    }

    fn pin_block(&self, key: BlockKey) -> PinnedBlock {
        let Some(kv) = self.kv.as_ref() else {
            return PinnedBlock::unpinned(key);
        };
        // A failed pin is not an error: the block may have fallen out of KV's
        // retained window, which just means triedb serves this read.
        PinnedBlock::new(key, kv_protect(kv, key).map(Arc::new))
    }

    async fn get_state_availability(&self, block: &PinnedBlock) -> Result<bool, String> {
        // A live pin is itself proof the block's state is present in KV.
        if block.guard().is_some() {
            return Ok(true);
        }
        match self
            .handle_async_request(block.key(), KeyInput::State, Ok)
            .await?
        {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_account(
        &self,
        block: &PinnedBlock,
        addr: EthAddress,
    ) -> Result<EthAccount, String> {
        if let Some(guard) = block.guard() {
            // KV stores a fixed record, so there is no RLP on this path.
            // A missing account reads as the default, matching the triedb path.
            return Ok(guard
                .account(&addr)
                .map(|account| EthAccount {
                    nonce: account.nonce,
                    balance: U256::from_be_bytes(account.balance_be),
                    // KV always stores a hash; keccak256("") is how it records
                    // "no code", which is the triedb path's None.
                    code_hash: (account.code_hash != KECCAK256_EMPTY.0)
                        .then_some(account.code_hash),
                    is_delegated: false,
                })
                .unwrap_or_default());
        }
        self.handle_async_request(block.key(), KeyInput::Address(&addr), |data| {
            rlp_decode_account(data).ok_or_else(|| String::from("Decoding account error"))
        })
        .await
        .map(Option::unwrap_or_default)
    }

    #[tracing::instrument(level = "debug")]
    async fn get_storage_at(
        &self,
        block: &PinnedBlock,
        addr: EthAddress,
        at: EthStorageKey,
    ) -> Result<EthStorageSlot, String> {
        if let Some(guard) = block.guard() {
            // KV takes the raw slot key: whether the leaf is a flat record or a
            // page, locating the slot inside it happens on the C++ side, so the
            // page geometry stays in one place. An unset slot reads as zero,
            // which is the same answer triedb gives.
            return Ok(guard.storage(&addr, &at));
        }
        let block_key = block.key();
        if self.page_encoded {
            // Page-encoded: storage is keyed by keccak(page_key) where
            // page_key = slot >> 7, and the leaf is an encoded page. Look up the
            // page key and extract the slot at its offset within the page. The
            // page geometry (the >> 7 shift and the in-page offset) and the page
            // decode all live in C++ so the format has one source of truth.
            let page_key = compute_page_key(at);
            let offset = compute_slot_offset(at);
            self.handle_async_request(
                block_key,
                KeyInput::Storage(&addr, &page_key),
                move |data| {
                    decode_storage_page_slot(&data, offset)
                        .ok_or_else(|| String::from("Decoding storage page error"))
                },
            )
            .await
            .map(Option::unwrap_or_default)
        } else {
            self.handle_async_request(block_key, KeyInput::Storage(&addr, &at), |data| {
                rlp_decode_storage_slot(data)
                    .ok_or_else(|| String::from("Decoding storage slot error"))
            })
            .await
            .map(Option::unwrap_or_default)
        }
    }

    #[tracing::instrument(level = "debug")]
    async fn get_code(
        &self,
        block: &PinnedBlock,
        code_hash: EthCodeHash,
    ) -> Result<EthCode, String> {
        // Code is content-addressed and never reclaimed, so the pin is not
        // needed to read it -- but it does tell us KV is serving this request.
        if block.guard().is_some() {
            if let Some(kv) = self.kv.as_ref() {
                return Ok(kv.code(&code_hash).unwrap_or_default());
            }
        }
        self.handle_async_request(block.key(), KeyInput::CodeHash(&code_hash), Ok)
            .await
            .map(Option::unwrap_or_default)
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

        if let Some(guard) = self.kv_pin(block_key) {
            // Past the last receipt is a miss, as it is on triedb -- and so is
            // every index of a block that has no receipts at all.
            return match guard.table_blob(BlobCategory::Receipts, receipt_index as u32) {
                Some(blob) => Ok(Some(kv_decode(&blob, "receipt")?)),
                None => Ok(None),
            };
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

        if let Some(guard) = self.kv_pin(block_key) {
            return kv_read_table(guard, BlobCategory::Receipts)
                .await?
                .iter()
                .map(|blob| kv_decode(blob, "receipt"))
                .collect();
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

        if let Some(guard) = self.kv_pin(block_key) {
            return match guard.table_blob(BlobCategory::Transactions, txn_index as u32) {
                Some(blob) => Ok(Some(kv_decode(&blob, "transaction")?)),
                None => Ok(None),
            };
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

        if let Some(guard) = self.kv_pin(block_key) {
            return kv_read_table(guard, BlobCategory::Transactions)
                .await?
                .iter()
                .map(|blob| kv_decode(blob, "transaction"))
                .collect();
        }

        self.handle_traverse_request(block_key, KeyInput::TxIndex(None), parse_rlp_entries)
            .await
    }

    #[tracing::instrument(level = "debug")]
    async fn get_block_header(&self, block_key: BlockKey) -> Result<Option<BlockHeader>, String> {
        if let Some(guard) = self.kv_pin(block_key) {
            // Every block execution commits carries a header blob, so the fall
            // through below is only reachable for a store whose base image was
            // bulk-built without blobs.
            if let Some(data) = guard.block_blob(BlobCategory::Header) {
                return Ok(Some(BlockHeader {
                    hash: keccak256(&data),
                    header: kv_decode(&data, "block header")?,
                }));
            }
        }

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
        // Takes no block pin. The index is a COW tree whose superseded roots
        // ARE reclaimed, so the walk does need protection -- but it is a
        // transient hazard taken inside the C++ resolve (pin the index root,
        // confirm it is still current, walk, release), not the block pin, since
        // the index is global rather than versioned at `block_key` and only
        // names a block rather than reading its data.
        //
        // It covers what KV has finalized and still retains, so it can lag
        // triedb but never lead it: a hit is the answer, a miss falls through
        // to triedb rather than being reported as "no such tx".
        if let Some(kv) = self.kv.as_ref() {
            if let Some((block_num, tx_index)) = kv.resolve_tx_hash(&tx_hash) {
                return Ok(Some(TransactionLocation {
                    block_num,
                    tx_index: tx_index.into(),
                }));
            }
        }

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
        // Same as the tx-hash index: hazard-protected inside the C++ resolve
        // rather than by a block pin, a hit is authoritative, and a miss defers
        // to triedb, which also holds the proposals KV has not finalized yet.
        if let Some(kv) = self.kv.as_ref() {
            if let Some(block_num) = kv.resolve_block_hash(&block_hash) {
                return Ok(Some(block_num));
            }
        }

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
        if let Some(guard) = self.kv_pin(block_key) {
            // KV keeps each frame whole, so there are no chunks to reassemble.
            // A transaction with no frame has no blob, which is the empty
            // result the triedb path reports as None.
            return Ok(guard
                .table_blob(BlobCategory::CallFrames, txn_index as u32)
                .filter(|frame| !frame.is_empty()));
        }

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
        if let Some(guard) = self.kv_pin(block_key) {
            // The table is indexed by transaction, so the entries are already
            // the consecutive-from-zero sequence the triedb path has to check
            // for.
            return kv_read_table(guard, BlobCategory::CallFrames).await;
        }

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

/// Pin one block-map entry, whether it is finalized or still undecided. An
/// undecided height can hold several proposals, so KV needs the proposal id to
/// say which; the finalized entry at a height is unique and takes none.
fn kv_protect(kv: &Arc<KvHandle>, key: BlockKey) -> Option<BlockGuard> {
    match key {
        BlockKey::Finalized(FinalizedBlockKey(seq_num)) => kv.try_protect_block(seq_num.0, None),
        BlockKey::Proposed(ProposedBlockKey(seq_num, block_id)) => {
            kv.try_protect_block(seq_num.0, Some(&block_id.0 .0))
        }
    }
}

/// A whole KV table, read off the async runtime.
///
/// KV reads are synchronous `pread`s, so a table's worth of them in a row would
/// park an async worker for as long as they take, where the triedb path awaits
/// and yields. The blocking pool is what that work belongs on. It is ONE
/// handoff for the whole table rather than one per entry, which is why the
/// single-blob reads stay inline: for those a handoff would cost more than the
/// read it protects.
async fn kv_read_table(
    guard: BlockGuard,
    category: BlobCategory,
) -> Result<Vec<Vec<u8>>, String> {
    tokio::task::spawn_blocking(move || kv_table_entries(&guard, category))
        .await
        .map_err(|err| format!("kv {category:?} read task failed: {err}"))
}

/// Every entry of a KV table category, in index order.
///
/// An absent category is an EMPTY result, not a missing one: the pin proves KV
/// holds this block, and execution only writes a table when it has entries, so
/// "no table" means zero entries (a block with no transactions, no ommers).
/// An entry with no blob is empty for the same reason -- KV stores an empty
/// payload as "no blob" -- which is the value for a category whose entries may
/// legitimately be empty, such as call frames.
fn kv_table_entries(guard: &BlockGuard, category: BlobCategory) -> Vec<Vec<u8>> {
    if !guard.blob_present(category) {
        return Vec::new();
    }
    let count = guard.table_count(category).unwrap_or(0);
    (0..count)
        .map(|i| guard.table_blob(category, i as u32).unwrap_or_default())
        .collect()
}

/// Decode one KV blob. Execution hands KV the identical buffer it writes to
/// triedb, so these are the same DB-format encodings the triedb paths decode.
fn kv_decode<T>(bytes: &[u8], what: &str) -> Result<T, String>
where
    T: Decodable,
{
    T::decode(&mut &bytes[..]).map_err(|err| format!("decode {what} from kv failed: {err}"))
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

// KVDB_PROTO: A/B the per-block reads. Every read that KV can serve must give
// the same answer triedb gives for the same block, so the two paths are run
// side by side over the blocks both stores still hold.
#[cfg(test)]
mod kv_ab_tests {
    use super::*;

    /// Env-gated on a live store, since KV is only readable while execution is
    /// running (it owns the hazard segment these reads publish into):
    ///
    /// ```text
    /// KVDB_AB_TRIEDB=/dev/triedb KVDB_IMAGE=<img>.kvhdr KVDB_DEVICE=<img>.img \
    ///   cargo test -p monad-triedb-utils kv_matches_triedb -- --nocapture
    /// ```
    #[tokio::test(flavor = "multi_thread")]
    async fn kv_matches_triedb_per_block_reads() {
        let Ok(triedb_path) = std::env::var("KVDB_AB_TRIEDB") else {
            eprintln!("KVDB_AB_TRIEDB unset; skipping the KV A/B test");
            return;
        };
        assert!(
            std::env::var("KVDB_IMAGE").is_ok(),
            "KVDB_AB_TRIEDB is set but KVDB_IMAGE is not: there would be no KV side"
        );

        // Two independent environments over the same triedb, each with its own
        // poller: one with KV, one wholly on triedb. Separate pollers are what
        // make the block tags comparable -- those are served out of poller
        // state, so a shared poller would have both sides reporting whichever
        // store it happened to read.
        //
        // Cache lengths of zero keep the block cache empty, so every read below
        // reaches a store instead of being answered out of memory.
        let env = |kv| {
            TriedbEnv::with_kv(
                Path::new(&triedb_path),
                1 << 28,
                1024,
                128,
                1024,
                128,
                0,
                0,
                kv,
            )
        };
        let kv = kv_from_env().expect("KVDB_IMAGE is set but the KV store did not open");
        let kv_env = env(Some(kv.clone()));
        let td_env = env(None);

        // Wait for both pollers to publish a tick, rather than sleeping a fixed
        // interval: each opens its own TriedbHandle first, so the first tick is
        // much slower than META_POLL_INTERVAL. Reading too early gives the
        // constructor's default (everything Finalized) and silently compares
        // nothing -- which is exactly what a fixed 20ms sleep did.
        // Readiness is measured against each env's OWN source, so it is not
        // circular with the comparison below: if the store has a proposed head,
        // an env that has ticked reports Proposed rather than the constructor's
        // Finalized default. Comparing the two envs to each other would be
        // trivially true before either had ticked.
        let raw = TriedbHandle::try_new(Path::new(&triedb_path), 1 << 28)
            .expect("triedb opens for the readiness check");
        let expect_proposed = raw.latest_proposed_block().is_some();
        let ready = |e: &TriedbEnv| {
            matches!(e.get_latest_proposed_block_key(), BlockKey::Proposed(_)) == expect_proposed
        };
        let mut waited = Duration::ZERO;
        while !(ready(&kv_env) && ready(&td_env)) && waited < Duration::from_secs(5) {
            thread::sleep(META_POLL_INTERVAL);
            waited += META_POLL_INTERVAL;
        }
        assert!(
            ready(&kv_env) && ready(&td_env),
            "pollers did not publish a tick within {waited:?}"
        );
        eprintln!("pollers ready after {waited:?} (proposed head present: {expect_proposed})");

        let finalized = kv.finalized_block().expect("KV has no finalized tip");
        let earliest = kv.earliest_block().expect("KV has no retained floor");
        assert!(earliest <= finalized, "KV window is inverted");
        eprintln!("kv window: [{earliest}, {finalized}]");

        // Block tags, KV-derived against triedb-derived. Both envs poll the
        // same triedb, so the only difference is where the cursors came from.
        {
            let kv_fin = kv_env.get_latest_finalized_block_key();
            let td_fin = td_env.get_latest_finalized_block_key();
            let kv_voted = kv_env.get_latest_voted_block_key();
            let td_voted = td_env.get_latest_voted_block_key();
            let kv_prop = kv_env.get_latest_proposed_block_key();
            let td_prop = td_env.get_latest_proposed_block_key();
            eprintln!(
                "tags: finalized kv={:?} td={:?} | voted kv={kv_voted:?} td={td_voted:?} \
                 | proposed kv={kv_prop:?} td={td_prop:?}",
                kv_fin.0, td_fin.0
            );

            // The two stores publish in different parts of the same commit, so
            // KV may trail triedb by the block in flight -- but it must never
            // be ahead, and never further behind than that.
            assert!(
                kv_fin.0 <= td_fin.0 && td_fin.0 .0 - kv_fin.0 .0 <= 1,
                "kv finalized {kv_fin:?} vs triedb {td_fin:?}: not within one block"
            );
            assert_eq!(
                kv_fin.0 .0, finalized,
                "the tag KV reports disagrees with its own cursor"
            );

            // The (block, id) heads must agree exactly. Asserting this is the
            // whole point of the two-poller setup: without it the test passes
            // while the two sides report different heads.
            assert_eq!(kv_voted, td_voted, "voted head");
            assert_eq!(kv_prop, td_prop, "proposed head");

            // Whatever KV names as a head must be pinnable -- that is the point
            // of taking the tags from KV rather than triedb.
            for (name, key) in [("voted", kv_voted), ("proposed", kv_prop)] {
                assert!(
                    kv_env.kv_pin(key).is_some(),
                    "kv named {key:?} as the {name} head but cannot pin it"
                );
            }

            // A height at or below the finalized tip must resolve the same way
            // from either side.
            for block in [earliest, (earliest + finalized) / 2, finalized] {
                assert_eq!(
                    kv_env.get_block_key(SeqNum(block)),
                    td_env.get_block_key(SeqNum(block)),
                    "block key for {block}"
                );
            }
        }

        // Stay a few blocks below the tip: execution is committing while this
        // runs, and the two stores need not have finalized the same block yet.
        // Sample from the top of the window rather than across all of it: the
        // two stores retain different depths and prune on their own schedules,
        // so the recently finalized blocks are the region both still hold.
        let top = finalized.saturating_sub(4).max(earliest);
        let bottom = top.saturating_sub(200).max(earliest);
        let step = ((top - bottom) / 8).max(1);
        let mut blocks = 0usize;
        let mut txs = 0usize;

        for block in (bottom..=top).step_by(step as usize) {
            let key = BlockKey::Finalized(FinalizedBlockKey(SeqNum(block)));

            // The triedb side is the reference. If it no longer holds the
            // block, there is nothing to compare against: the two stores are
            // pruned on their own schedules.
            let td_header = td_env.get_block_header(key).await.expect("triedb header");
            let Some(td_header) = td_header else {
                eprintln!("block {block}: not in triedb, skipping");
                continue;
            };
            // Everything below must actually be served by KV. Without this the
            // comparison would still pass with KV falling back to triedb on
            // both sides, which is exactly what it is meant to detect.
            {
                let guard = kv_env.kv_pin(key).expect("kv did not pin the block");
                // A block KV holds with no header at all is a bulk-built base
                // image block, which predates blob writing -- the store's
                // retained floor when it was seeded rather than replayed.
                if !guard.blob_present(BlobCategory::Header) {
                    eprintln!("block {block}: bulk-built base image, no blobs, skipping");
                    continue;
                }
                for category in [
                    BlobCategory::Receipts,
                    BlobCategory::Transactions,
                    BlobCategory::CallFrames,
                ] {
                    assert!(
                        guard.blob_present(category),
                        "block {block}: kv has no {category:?}, so that read fell back to triedb"
                    );
                }
            }

            let kv_header = kv_env
                .get_block_header(key)
                .await
                .expect("kv header")
                .expect("kv is missing a header triedb has");
            // BlockHeader has no PartialEq, so compare its parts.
            assert_eq!(
                (kv_header.hash, &kv_header.header),
                (td_header.hash, &td_header.header),
                "block {block}: header"
            );

            // The block-hash index must map the header's hash back to it.
            assert_eq!(
                kv_env
                    .get_block_number_by_hash(key, td_header.hash.0)
                    .await
                    .expect("kv block-hash lookup"),
                Some(block),
                "block {block}: block-hash index"
            );

            let td_txs = td_env.get_transactions(key).await.expect("triedb txs");
            assert_eq!(
                kv_env.get_transactions(key).await.expect("kv txs"),
                td_txs,
                "block {block}: transactions"
            );

            let td_receipts = td_env.get_receipts(key).await.expect("triedb receipts");
            assert_eq!(
                kv_env.get_receipts(key).await.expect("kv receipts"),
                td_receipts,
                "block {block}: receipts"
            );

            let td_frames = td_env.get_call_frames(key).await.expect("triedb frames");
            assert_eq!(
                kv_env.get_call_frames(key).await.expect("kv frames"),
                td_frames,
                "block {block}: call frames"
            );

            // Indexed reads, including one past the end, which must miss on
            // both sides rather than being served differently.
            for i in 0..=td_txs.len() as u64 {
                assert_eq!(
                    kv_env.get_transaction(key, i).await.expect("kv tx"),
                    td_env.get_transaction(key, i).await.expect("triedb tx"),
                    "block {block}: transaction {i}"
                );
                assert_eq!(
                    kv_env.get_receipt(key, i).await.expect("kv receipt"),
                    td_env.get_receipt(key, i).await.expect("triedb receipt"),
                    "block {block}: receipt {i}"
                );
                assert_eq!(
                    kv_env.get_call_frame(key, i).await.expect("kv frame"),
                    td_env.get_call_frame(key, i).await.expect("triedb frame"),
                    "block {block}: call frame {i}"
                );
            }

            // The tx-hash index must place every transaction at its own
            // position in this block.
            for (i, tx) in td_txs.iter().enumerate() {
                let hash = *tx.tx.tx_hash();
                assert_eq!(
                    kv_env
                        .get_transaction_location_by_hash(key, hash.0)
                        .await
                        .expect("kv tx-hash lookup"),
                    Some(TransactionLocation {
                        block_num: block,
                        tx_index: i as u64,
                    }),
                    "block {block}: tx-hash index for tx {i}"
                );
            }

            blocks += 1;
            txs += td_txs.len();
            eprintln!("block {block}: {} txs ok", td_txs.len());
        }

        assert!(blocks > 0, "no block was held by both stores");
        eprintln!("kv A/B ok: {blocks} blocks, {txs} txs");

        // An undecided block pins by its proposal id, and only by the right
        // one: a height can carry several proposals, so an id-blind pin would
        // be free to return a sibling's root.
        match kv_env.get_latest_proposed_block_key() {
            BlockKey::Proposed(ProposedBlockKey(seq_num, block_id)) => {
                let key = BlockKey::Proposed(ProposedBlockKey(seq_num, block_id));
                assert!(
                    kv_env.kv_pin(key).is_some(),
                    "kv did not pin proposal {seq_num:?} {block_id:?}"
                );
                let mut wrong = block_id.0 .0;
                wrong[0] ^= 1;
                assert!(
                    kv_env
                        .kv
                        .as_ref()
                        .unwrap()
                        .try_protect_block(seq_num.0, Some(&wrong))
                        .is_none(),
                    "kv pinned a proposal id that does not exist"
                );
                eprintln!("proposal pin ok at {seq_num:?}");
            }
            BlockKey::Finalized(_) => {
                eprintln!("no undecided block in triedb; proposal pin not exercised");
            }
        }
    }
}
