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
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
};

use alloy_rlp::{Decodable, RlpDecodable, RlpEncodable};
use bytes::Bytes;
use futures::{
    lock::Mutex,
    stream::{StreamExt, TryStreamExt},
};
use tokio::sync::Semaphore;
use zstd::dict::DecoderDictionary;

use crate::{
    engine::{
        bitmap::{
            decode_fragment_blob, decode_open_streams_chunk, decode_page, decode_page_counts,
            BitmapPageCounts, BitmapTables, DecodedBitmapFragment, DecodedBitmapPage,
        },
        digest::ChainDigest,
        family::{Family, BLOCK_BLOB_TABLE},
        primary_dir::{
            decode_bucket, decode_fragment, PrimaryDirBucket, PrimaryDirFragment, PrimaryDirTables,
        },
        query::row_cache::RowCaches,
        row_codec::{
            decode_row_frame, should_sample_row, RowCodec, RowCodecState, DICT_VERSION_NONE,
            ROW_ZSTD_LEVEL,
        },
    },
    error::{MonadChainDataError, Result},
    external::ExternalBlobReader,
    ingest_types::{FinalizedBlock, Hash32},
    primitives::{
        records::{BlockBlobHeader, BlockRecord, PublicationState},
        EvmBlockHeader,
    },
    store::{
        BlobStore, BlobTable, BlobWriteOp, CacheConfig, CachedKvTable, CachedScannableKvTable,
        MetaStore, MetaWriteOp, SessionFuture, TableId, WriteSession,
    },
    txs::TxHashIndexTable,
};

/// Epoch-based dictionary lifecycle. Block `n` has dict version `n / epoch_blocks`;
/// version `V >= 1` is trained from the first `sample_span_blocks` blocks of epoch `V - 1`
/// and must be published before any block of epoch `V` is written. Epoch 0 is plain.
#[derive(Debug, Clone, Copy)]
pub struct DictConfig {
    /// Blocks per epoch; the epoch number doubles as the dict version.
    pub epoch_blocks: u64,
    /// Leading blocks of an epoch sampled to train the next epoch's dictionary.
    /// Clamped to `epoch_blocks` at read time.
    pub sample_span_blocks: u64,
    /// Maximum trained dictionary size in bytes.
    pub max_dict_size_bytes: usize,
    /// Minimum row samples to train a non-empty dictionary; below this the
    /// family publishes the empty-dict sentinel.
    pub min_training_samples: usize,
}

impl Default for DictConfig {
    fn default() -> Self {
        Self {
            epoch_blocks: 1_000_000,
            sample_span_blocks: 900_000,
            max_dict_size_bytes: 112 * 1024,
            min_training_samples: 4_096,
        }
    }
}

impl DictConfig {
    /// `sample_span_blocks` with the documented read-time clamp applied. Every
    /// consumer of the span (training and the prewarm trigger) goes through
    /// this, so they agree on the corpus size by construction.
    pub fn clamped_sample_span(&self) -> u64 {
        self.sample_span_blocks.min(self.epoch_blocks)
    }

    /// Clamped training sample range `[start, end)` for version `V >= 1`,
    /// drawn from the leading blocks of epoch `V - 1`.
    pub fn training_range(&self, version: u32) -> (u64, u64) {
        let prev_epoch = u64::from(version - 1);
        let start = prev_epoch * self.epoch_blocks;
        (start, start + self.clamped_sample_span())
    }
}

const BLOCK_BLOB_COALESCE_TARGET_BYTES: usize = 512 * 1024;
const BLOCK_METADATA_TABLE_ID: TableId = TableId::new("block_metadata");

/// Every logical kv/scan table name the engine declares, so backends that map
/// logical tables to physical resources (dynamo `PerLogicalTable`) can
/// provision/validate the complete set. A unit test below asserts this list
/// matches the declared `TableId`s exactly, so drift fails loudly.
pub const ALL_LOGICAL_TABLE_NAMES: &[&str] = &[
    "publication_state",
    "ingest_snapshot",
    "block_metadata",
    "block_evm_header",
    "block_hash_to_number_index",
    "tx_hash_index",
    "log_dict_by_version",
    "log_dir_by_block",
    "log_dir_bucket",
    "log_bitmap_by_block",
    "log_bitmap_page_blob",
    "log_bitmap_page_counts",
    "log_open_bitmap_stream",
    "log_seal_chain",
    "tx_dict_by_version",
    "tx_dir_by_block",
    "tx_dir_bucket",
    "tx_bitmap_by_block",
    "tx_bitmap_page_blob",
    "tx_bitmap_page_counts",
    "tx_open_bitmap_stream",
    "tx_seal_chain",
    "trace_dict_by_version",
    "trace_dir_by_block",
    "trace_dir_bucket",
    "trace_bitmap_by_block",
    "trace_bitmap_page_blob",
    "trace_bitmap_page_counts",
    "trace_open_bitmap_stream",
    "trace_seal_chain",
];

/// Per-family row-codec dictionary state: write-side per-version codecs,
/// read-side decoder cache, and single-flight training locks.
pub struct DictManager {
    /// Write-side codecs keyed by `(family, version)`; version-0 plain is
    /// pre-installed, epochs `>= 1` arrive via `install_version`.
    codecs: RwLock<HashMap<(Family, u32), Arc<RowCodecState>>>,
    decoders: RwLock<HashMap<(Family, u32), Arc<DecoderDictionary<'static>>>>,
    /// Per-family single-flight gate collapsing concurrent `ensure_epoch_dict`
    /// callers onto one training run.
    train_locks: BTreeMap<Family, Mutex<()>>,
    config: DictConfig,
}

impl DictManager {
    fn new(config: DictConfig) -> Self {
        let mut codecs = HashMap::new();
        let mut train_locks = BTreeMap::new();
        for family in Family::ALL {
            codecs.insert(
                (family, DICT_VERSION_NONE),
                Arc::new(RowCodecState::bootstrap()),
            );
            train_locks.insert(family, Mutex::new(()));
        }
        Self {
            codecs: RwLock::new(codecs),
            decoders: RwLock::new(HashMap::new()),
            train_locks,
            config,
        }
    }

    pub fn config(&self) -> &DictConfig {
        &self.config
    }

    /// The single-flight training lock for one family.
    fn train_lock(&self, family: Family) -> &Mutex<()> {
        self.train_locks
            .get(&family)
            .expect("family registered at construction")
    }

    /// Write-side codec for `(family, version)`, if installed.
    pub fn write_codec(&self, family: Family, version: u32) -> Option<RowCodec> {
        self.codecs
            .read()
            .expect("dict codecs lock poisoned")
            .get(&(family, version))
            .map(|state| state.snapshot())
    }

    pub fn has_codec(&self, family: Family, version: u32) -> bool {
        self.codecs
            .read()
            .expect("dict codecs lock poisoned")
            .contains_key(&(family, version))
    }

    /// Installs the write-side codec for a published dictionary version.
    /// Empty `dict_bytes` is the plain-frames sentinel; non-empty bytes also
    /// seed the read-side decoder cache.
    pub fn install_version(&self, family: Family, version: u32, dict_bytes: &[u8]) {
        let state = if dict_bytes.is_empty() {
            Arc::new(RowCodecState::plain(version))
        } else {
            Arc::new(RowCodecState::with_dictionary(
                version,
                dict_bytes,
                ROW_ZSTD_LEVEL,
            ))
        };
        self.codecs
            .write()
            .expect("dict codecs lock poisoned")
            .insert((family, version), state);
        if !dict_bytes.is_empty() {
            self.insert_decoder(
                family,
                version,
                Arc::new(DecoderDictionary::copy(dict_bytes)),
            );
        }
    }

    /// Pre-inserts a decoder dictionary into the read cache.
    fn insert_decoder(
        &self,
        family: Family,
        version: u32,
        decoder: Arc<DecoderDictionary<'static>>,
    ) {
        self.decoders
            .write()
            .expect("dict decoder cache poisoned")
            .insert((family, version), decoder);
    }

    fn cached_decoder(
        &self,
        family: Family,
        version: u32,
    ) -> Option<Arc<DecoderDictionary<'static>>> {
        self.decoders
            .read()
            .expect("dict decoder cache poisoned")
            .get(&(family, version))
            .cloned()
    }
}

pub struct Tables<M: MetaStore, B: BlobStore> {
    meta_store: M,
    blob_store: B,
    blocks: BlockTables<M>,
    tx_hash_index: TxHashIndexTable<M>,
    /// Raw shared-per-block blob table. Bytes are never cached; the
    /// decoded-row caches above the decode layer absorb repeat reads.
    block_blobs: BlobTable<B>,
    /// Read-only access to the external archive's objects, for blocks whose
    /// headers carry `ENCODING_EXTERNAL_V1`. `None` on stores that never
    /// ingested external payloads; reading an external block without it is a
    /// hard error.
    external_payload: Option<Arc<dyn ExternalBlobReader>>,
    /// The shared blob-read limiter (also installed on `block_blobs`), held
    /// here so external archive reads obey the same process-global cap.
    blob_io: Arc<Semaphore>,
    families: BTreeMap<Family, FamilyTables<M>>,
    /// Per-family decoded-row caches; see [`crate::engine::query::row_cache`].
    row_caches: RowCaches,
    dicts: DictManager,
    /// Process-global, byte-weighted budget bounding concurrent stage-2 decode
    /// across all queries; see `family_runner::materialize_permits_for_bytes`.
    materialize_budget: Arc<Semaphore>,
    /// Query-runtime limits; the semaphores above are sized from it at construction.
    query: QueryRuntimeConfig,
}

/// Tunable, process-global limits on the read path. Defaults suit a fast
/// local backend; high-latency backends (S3/Dynamo) should raise
/// `blob_io_concurrency` and `materialize_concurrency` so sparse queries fan
/// out toward a single round-trip.
#[derive(Debug, Clone, Copy)]
pub struct QueryRuntimeConfig {
    /// Global cap on concurrent backend blob reads across every query path.
    pub blob_io_concurrency: usize,
    /// Per-request ceiling on blocks materialized concurrently in stage 2
    /// (the `buffered` width).
    pub materialize_concurrency: usize,
    /// Per-request stage-1 fan-out: concurrent page intersections (bitmap
    /// fetches + AND). Independent of the stage-2 fan-out — different
    /// backends (bitmap pages vs block blobs).
    pub page_intersect_concurrency: usize,
    /// Total permits in the process-global, byte-weighted stage-2 decode budget.
    pub materialize_budget_permits: usize,
    /// Bytes per decode-budget permit. A block acquires
    /// `clamp(decode_bytes / this, 1, materialize_budget_permits)` permits, so
    /// tiny blocks fan out while a huge region runs ~exclusively.
    pub materialize_permit_bytes: usize,
    /// Maximum gap (compressed bytes) between two requested frames sharing one
    /// coalesced range read; raise for high-latency backends where a
    /// round-trip costs more than the over-read.
    pub materialize_span_max_gap_bytes: usize,
    /// Maximum length of one coalesced read span.
    pub materialize_span_max_bytes: usize,
    /// When a batch coalesces into more spans than this AND the region is
    /// within `materialize_whole_region_max_bytes`, the batch reads the whole
    /// family region in one request instead of per-span ranges.
    pub materialize_whole_region_span_threshold: usize,
    /// Largest region the dense-selection whole-region read will pull.
    pub materialize_whole_region_max_bytes: usize,
}

impl Default for QueryRuntimeConfig {
    fn default() -> Self {
        Self {
            // Primary knob. Materialization is point-gets + small range
            // reads, so a wide fan-out is cheap on fast local backends and
            // necessary on high-latency ones (mainnet-1 bench: 64 cut warm
            // analytic pages ~8x vs 8).
            materialize_concurrency: 64,
            page_intersect_concurrency: 64,
            // Aggregate ceilings that only bind once the fan-out is raised.
            blob_io_concurrency: 1024,
            materialize_budget_permits: 1024,
            // 32 KiB/permit: tiny blocks take one permit; a 40 MiB region
            // clamps to the full budget and runs ~exclusively.
            materialize_permit_bytes: 32 * 1024,
            materialize_span_max_gap_bytes: 16 * 1024,
            materialize_span_max_bytes: 512 * 1024,
            materialize_whole_region_span_threshold: 8,
            materialize_whole_region_max_bytes: 8 * 1024 * 1024,
        }
    }
}

impl QueryRuntimeConfig {
    /// Clamps to ≥ 1 so a zero from config can't wedge a semaphore or
    /// divide-by-zero the permit math.
    fn sanitized(self) -> Self {
        Self {
            blob_io_concurrency: self.blob_io_concurrency.max(1),
            materialize_concurrency: self.materialize_concurrency.max(1),
            page_intersect_concurrency: self.page_intersect_concurrency.max(1),
            materialize_budget_permits: self.materialize_budget_permits.max(1),
            materialize_permit_bytes: self.materialize_permit_bytes.max(1),
            // Zero is meaningful for the span knobs (per-frame reads /
            // never-whole-region), so they are not clamped.
            materialize_span_max_gap_bytes: self.materialize_span_max_gap_bytes,
            materialize_span_max_bytes: self.materialize_span_max_bytes,
            materialize_whole_region_span_threshold: self.materialize_whole_region_span_threshold,
            materialize_whole_region_max_bytes: self.materialize_whole_region_max_bytes,
        }
    }
}

impl<M: MetaStore, B: BlobStore> Tables<M, B> {
    /// Default-configured constructor; see [`Self::with_all_configs`].
    pub fn new(meta_store: M, blob_store: B) -> Self {
        Self::with_all_configs(
            meta_store,
            blob_store,
            CacheConfig::default(),
            DictConfig::default(),
            QueryRuntimeConfig::default(),
        )
    }

    /// Full constructor threading every operator-tuned config (cache sizes,
    /// dict lifecycle, read-path limits).
    pub fn with_all_configs(
        meta_store: M,
        blob_store: B,
        cache: CacheConfig,
        dict_config: DictConfig,
        query: QueryRuntimeConfig,
    ) -> Self {
        let query = query.sanitized();
        let mut families = BTreeMap::new();
        for family in Family::ALL {
            families.insert(family, FamilyTables::new(meta_store.clone(), family, cache));
        }
        // Every blob read goes through `block_blobs`, so this cap is
        // process-global across all blob-read paths.
        let blob_io = Arc::new(Semaphore::new(query.blob_io_concurrency));
        Self {
            blocks: BlockTables::new(meta_store.clone(), cache),
            tx_hash_index: TxHashIndexTable::new(meta_store.clone(), cache),
            block_blobs: blob_store
                .table(BLOCK_BLOB_TABLE)
                .with_io_limit(Arc::clone(&blob_io)),
            external_payload: None,
            blob_io,
            meta_store,
            blob_store,
            families,
            row_caches: RowCaches::new(cache.row_cache_bytes),
            dicts: DictManager::new(dict_config),
            materialize_budget: Arc::new(Semaphore::new(query.materialize_budget_permits)),
            query,
        }
    }

    /// Attaches the external archive reader serving `ENCODING_EXTERNAL_V1`
    /// payload reads (see [`crate::external`]).
    pub fn with_external_payload_reader(mut self, reader: Arc<dyn ExternalBlobReader>) -> Self {
        self.external_payload = Some(reader);
        self
    }

    /// Process-global stage-2 decode budget (see field docs).
    pub(crate) fn materialize_budget(&self) -> &Arc<Semaphore> {
        &self.materialize_budget
    }

    /// The sanitized read-path limits this instance was constructed with;
    /// see the [`QueryRuntimeConfig`] field docs for each knob.
    pub fn query_config(&self) -> &QueryRuntimeConfig {
        &self.query
    }

    pub fn row_caches(&self) -> &RowCaches {
        &self.row_caches
    }

    pub fn meta_store(&self) -> &M {
        &self.meta_store
    }

    pub fn blob_store(&self) -> &B {
        &self.blob_store
    }

    pub fn blocks(&self) -> &BlockTables<M> {
        &self.blocks
    }

    pub fn tx_hash_index(&self) -> &TxHashIndexTable<M> {
        &self.tx_hash_index
    }

    /// Raw byte-range read of one family's payload object over an absolute
    /// `[start, end_exclusive)` span. Native headers resolve through the
    /// physical key into the chain-data blob store (coalesced blocks share an
    /// object); external headers range-read the archive object named by their
    /// key through the attached [`ExternalBlobReader`].
    pub async fn read_block_blob_header_range(
        &self,
        block_number: u64,
        header: &BlockBlobHeader,
        start: usize,
        end_exclusive: usize,
    ) -> Result<Option<Bytes>> {
        if header.is_external() {
            let reader =
                self.external_payload
                    .as_deref()
                    .ok_or(MonadChainDataError::MissingData(
                        "external payload block but no archive reader configured",
                    ))?;
            if header.physical_key.is_empty() {
                return Err(MonadChainDataError::Decode(
                    "external payload header missing its archive key",
                ));
            }
            let _permit = self
                .blob_io
                .acquire()
                .await
                .expect("blob io-limit semaphore is never closed");
            return reader
                .read_range(&header.physical_key, start, end_exclusive)
                .await;
        }
        let default_key = block_number_key(block_number);
        self.block_blobs
            .read_range(header.physical_key_or(&default_key), start, end_exclusive)
            .await
    }

    /// Reads this family's whole compressed region for `block_number` in one
    /// uncached range read. The buffer starts at the family's `base_offset`,
    /// so callers slice frames with family-relative offsets (`header.offsets[i]`).
    pub async fn read_block_blob_region(
        &self,
        block_number: u64,
        header: &BlockBlobHeader,
    ) -> Result<Option<Bytes>> {
        let (region_start, region_end) = header.region_range();
        self.read_block_blob_header_range(block_number, header, region_start, region_end)
            .await
    }

    pub fn stage_block_blob(
        &self,
        w: &mut WriteSession<'_, M, B>,
        block_number: u64,
        combined: Vec<u8>,
    ) {
        if combined.is_empty() {
            return;
        }

        let key = block_number_key(block_number);
        w.put_blob(&self.block_blobs, &key, Bytes::from(combined));
    }

    /// Table set for a family. Panics if unregistered — the `Family` enum is
    /// closed, so that is a programmer error.
    pub fn family(&self, family: Family) -> &FamilyTables<M> {
        self.families
            .get(&family)
            .expect("family registered at construction")
    }

    pub fn dicts(&self) -> &DictManager {
        &self.dicts
    }

    /// Resolves the decoder dictionary for `(family, version)`, populating the
    /// read cache from the durable dict table on a miss. `None` means plain
    /// frames (version 0 or the empty-dict sentinel). Absent bytes for a
    /// version `>= 1` are a hard error: blocks are written only after their
    /// epoch dictionary is durably published.
    pub async fn block_decoder(
        &self,
        family: Family,
        dict_version: u32,
    ) -> Result<Option<Arc<DecoderDictionary<'static>>>> {
        if dict_version == DICT_VERSION_NONE {
            return Ok(None);
        }
        if let Some(decoder) = self.dicts.cached_decoder(family, dict_version) {
            return Ok(Some(decoder));
        }
        let bytes = self.family(family).load_dict(dict_version).await?.ok_or(
            MonadChainDataError::MissingData("missing row-codec dictionary for block"),
        )?;
        if bytes.is_empty() {
            // Empty-dict sentinel: this epoch's frames are plain.
            return Ok(None);
        }
        let decoder = Arc::new(DecoderDictionary::copy(&bytes));
        self.dicts
            .insert_decoder(family, dict_version, Arc::clone(&decoder));
        Ok(Some(decoder))
    }

    /// Decodes a single compressed row frame written under `dict_version`.
    pub async fn decode_block_row(
        &self,
        family: Family,
        dict_version: u32,
        frame: &[u8],
    ) -> Result<Bytes> {
        let decoder = self.block_decoder(family, dict_version).await?;
        decode_row_frame(decoder.as_ref(), frame)
    }

    /// Ensures `version`'s dictionary is durably published and its write-side
    /// codec installed; idempotent and single-flight. Adopts an already
    /// published dict, else trains one from epoch `version - 1`'s blocks.
    /// Operates over `&self` so the query service and the ingest engine's
    /// codec resolver share the logic — see [`crate::ingest::resolver`].
    pub async fn ensure_epoch_dict(&self, family: Family, version: u32) -> Result<()> {
        // Version 0 is the plain bootstrap; its codec is pre-installed.
        if version == DICT_VERSION_NONE {
            return Ok(());
        }
        let dicts = self.dicts();
        if dicts.has_codec(family, version) {
            return Ok(());
        }
        let _g = dicts.train_lock(family).lock().await;
        if dicts.has_codec(family, version) {
            return Ok(());
        }

        // Adopt a durably-published dict (e.g. from a peer or prior run);
        // `Some(empty)` is the plain sentinel.
        if let Some(bytes) = self.family(family).load_dict(version).await? {
            dicts.install_version(family, version, &bytes);
            return Ok(());
        }

        let samples = self.read_back_training_samples(family, version).await?;
        let dict_bytes: Vec<u8> = if samples.len() >= dicts.config().min_training_samples {
            let max_size = dicts.config().max_dict_size_bytes;
            // CPU-bound training runs on rayon so the executor thread isn't blocked.
            let (tx, rx) = futures::channel::oneshot::channel();
            rayon::spawn(move || {
                let _ = tx.send(zstd::dict::from_samples(&samples, max_size));
            });
            let trained = rx
                .await
                .map_err(|_| MonadChainDataError::Backend("dict training canceled".into()))?;
            match trained {
                Ok(bytes) if !bytes.is_empty() => bytes,
                // Failed or empty training falls back to the empty-dict (plain) sentinel.
                _ => Vec::new(),
            }
        } else {
            // Too few samples: publish the empty-dict sentinel.
            Vec::new()
        };

        // Persist durably BEFORE installing the codec, so a block written under
        // `version` always has its dict (or sentinel) present on the read path.
        let dict_bytes = Bytes::from(dict_bytes);
        let dict_for_write = dict_bytes.clone();
        self.with_writes(|w| {
            Box::pin(async move {
                self.family(family).stage_dict(w, version, dict_for_write);
                Ok(())
            })
        })
        .await?;
        dicts.install_version(family, version, &dict_bytes);
        Ok(())
    }

    /// Ensures the epoch dictionaries for all three families at `version`.
    pub async fn ensure_epoch_dicts(&self, version: u32) -> Result<()> {
        for family in Family::ALL {
            self.ensure_epoch_dict(family, version).await?;
        }
        Ok(())
    }

    /// Collects a deterministic training corpus from the leading blocks of
    /// epoch `version - 1`: strided block selection seeded by `version` (no
    /// RNG), rows filtered via [`should_sample_row`], so the corpus is reproducible.
    async fn read_back_training_samples(
        &self,
        family: Family,
        version: u32,
    ) -> Result<Vec<Vec<u8>>> {
        const TARGET_SAMPLE_BLOCKS: u64 = 256;

        let (start, end) = self.dicts().config().training_range(version);
        let span = end.saturating_sub(start);
        if span == 0 {
            return Ok(Vec::new());
        }
        let count = span.min(TARGET_SAMPLE_BLOCKS);
        let stride = (span / count).max(1);
        let offset = u64::from(version) % stride;

        let family_tables = self.family(family);
        let mut samples: Vec<Vec<u8>> = Vec::new();
        for i in 0..count {
            let block_number = start + offset + i * stride;
            if block_number >= end {
                break;
            }
            let Some(header) = family_tables.load_blob_header(block_number).await? else {
                continue;
            };
            // External blocks hold uncompressed archive containers, never
            // zstd frames — they contribute nothing to a dict corpus (and the
            // external engine never trains; this guards mixed stores).
            if header.is_external() || header.offsets.len() < 2 {
                continue;
            }
            let Some(region) = self.read_block_blob_region(block_number, &header).await? else {
                continue;
            };
            let row_count = header.offsets.len() - 1;
            for idx in 0..row_count {
                if !should_sample_row(idx, row_count) {
                    continue;
                }
                let frame_start = header.offsets[idx] as usize;
                let frame_end = header.offsets[idx + 1] as usize;
                if frame_start > frame_end || frame_end > region.len() {
                    return Err(MonadChainDataError::Decode(
                        "invalid row frame range while sampling for dict training",
                    ));
                }
                let frame = &region[frame_start..frame_end];
                let raw = self
                    .decode_block_row(family, header.dict_version, frame)
                    .await?;
                samples.push(raw.to_vec());
            }
        }
        Ok(samples)
    }

    /// Runs `f` against a fresh [`WriteSession`], coalesces block-blob writes,
    /// and applies the staged meta and blob ops concurrently.
    pub async fn with_writes<'a, F>(&'a self, f: F) -> Result<()>
    where
        F: for<'s> FnOnce(&'s mut WriteSession<'a, M, B>) -> SessionFuture<'s>,
    {
        let mut session = WriteSession::new(self);
        f(&mut session).await?;
        let mut meta_ops = session.take_meta();
        let mut blob_ops = session.take_blob();
        coalesce_block_blob_writes(&mut meta_ops, &mut blob_ops)?;
        futures::try_join!(
            self.meta_store.apply_writes(meta_ops),
            self.blob_store.apply_writes(blob_ops),
        )?;
        Ok(())
    }

    /// Drains the (hits, misses) counters for every cached wrapper; each call
    /// resets them, so consumers see per-window deltas.
    pub fn take_cache_window_stats(&self) -> Vec<(&'static str, u64, u64)> {
        let mut out = Vec::new();
        self.blocks.collect_window_stats(&mut out);
        self.tx_hash_index.collect_window_stats(&mut out);
        self.row_caches.collect_window_stats(&mut out);
        for fam in self.families.values() {
            fam.collect_window_stats(&mut out);
        }
        out
    }
}

/// Pushes one (name, hits, misses) tuple, skipping all-zero rows.
fn push_window_stats(
    out: &mut Vec<(&'static str, u64, u64)>,
    name: &'static str,
    (hits, misses): (u64, u64),
) {
    if hits != 0 || misses != 0 {
        out.push((name, hits, misses));
    }
}

/// Collects this table's cache window stats into `out`.
pub(crate) fn collect_kv_stats<M: MetaStore, V>(
    out: &mut Vec<(&'static str, u64, u64)>,
    table: &CachedKvTable<M, V>,
) {
    push_window_stats(out, table.table_id().as_str(), table.take_window_stats());
}

/// Collects this table's cache window stats into `out`.
pub(crate) fn collect_scan_stats<M: MetaStore, V>(
    out: &mut Vec<(&'static str, u64, u64)>,
    table: &CachedScannableKvTable<M, V>,
) {
    push_window_stats(out, table.table_id().as_str(), table.take_window_stats());
}

/// Max concurrent point gets after a keys-only fragment scan; turns a cold
/// open page/bucket from N serial round-trips into ~one.
pub(crate) const FRAGMENT_GET_CONCURRENCY: usize = 32;

/// Keys-only scan of `partition` followed by concurrent point gets of every
/// key, each missing value surfacing `missing`. Uses `buffered` (not
/// `buffer_unordered`) so the values preserve the scan's key order, which the
/// fragment-compaction callers rely on.
pub(crate) async fn scan_get_all<M, V>(
    table: &CachedScannableKvTable<M, V>,
    partition: &[u8],
    missing: &'static str,
) -> Result<Vec<V>>
where
    M: MetaStore,
    V: Clone + Send + Sync + 'static,
{
    let keys = table.scan_keys(partition).await?;
    futures::stream::iter(keys)
        .map(|clustering| async move {
            table
                .get(partition, &clustering)
                .await?
                .ok_or(MonadChainDataError::MissingData(missing))
        })
        .buffered(FRAGMENT_GET_CONCURRENCY)
        .try_collect()
        .await
}

/// Accumulates consecutive block-blob writes into one coalesced object,
/// recording each source key's physical `(key, offset)` in `locators` so the
/// matching metadata headers can be rewritten afterward.
struct Coalescer {
    group: Vec<BlobWriteOp>,
    group_len: usize,
    out: Vec<BlobWriteOp>,
    locators: BTreeMap<Vec<u8>, (Vec<u8>, u64)>,
}

impl Coalescer {
    fn new(capacity: usize) -> Self {
        Self {
            group: Vec::new(),
            group_len: 0,
            out: Vec::with_capacity(capacity),
            locators: BTreeMap::new(),
        }
    }

    /// Concatenates the current group into one physical object. A lone op is
    /// emitted unchanged: its header's empty physical key already falls back to
    /// the block-number key, so it needs no locator rewrite.
    fn flush(&mut self) {
        let group_len = std::mem::take(&mut self.group_len);
        match self.group.len() {
            0 => {}
            1 => self.out.push(self.group.pop().expect("group has one item")),
            _ => {
                let first_key = &self.group.first().expect("non-empty group").key;
                let last_key = &self.group.last().expect("non-empty group").key;
                let mut physical_key = Vec::with_capacity(1 + first_key.len() + last_key.len());
                physical_key.push(b'c');
                physical_key.extend_from_slice(first_key);
                physical_key.extend_from_slice(last_key);

                let mut combined = Vec::with_capacity(group_len);
                for op in self.group.drain(..) {
                    // usize -> u64 is infallible on every supported target.
                    let offset = combined.len() as u64;
                    self.locators.insert(op.key, (physical_key.clone(), offset));
                    combined.extend_from_slice(&op.value);
                }
                self.out.push(BlobWriteOp {
                    table: BLOCK_BLOB_TABLE,
                    key: physical_key,
                    value: Bytes::from(combined),
                });
            }
        }
    }

    /// Adds a block-blob op to the current group, first flushing if adding it
    /// would push a non-empty group past the coalesce target (a single
    /// oversized op still starts a group rather than being emitted alone).
    fn push(&mut self, op: BlobWriteOp) {
        debug_assert_eq!(op.table, BLOCK_BLOB_TABLE);
        if !self.group.is_empty()
            && self.group_len.saturating_add(op.value.len()) > BLOCK_BLOB_COALESCE_TARGET_BYTES
        {
            self.flush();
        }
        self.group_len = self.group_len.saturating_add(op.value.len());
        self.group.push(op);
    }
}

fn coalesce_block_blob_writes(
    meta_ops: &mut [MetaWriteOp],
    blob_ops: &mut Vec<BlobWriteOp>,
) -> Result<()> {
    let block_blob_ops = blob_ops
        .iter()
        .filter(|op| op.table == BLOCK_BLOB_TABLE)
        .count();
    if block_blob_ops <= 1 {
        return Ok(());
    }

    let mut coalescer = Coalescer::new(blob_ops.len());
    for op in std::mem::take(blob_ops) {
        if op.table == BLOCK_BLOB_TABLE {
            coalescer.push(op);
        } else {
            coalescer.flush();
            coalescer.out.push(op);
        }
    }
    coalescer.flush();

    for op in meta_ops {
        let MetaWriteOp::Put { table, key, value } = op else {
            continue;
        };
        if *table != BLOCK_METADATA_TABLE_ID {
            continue;
        }
        let Some((physical_key, physical_base_offset)) = coalescer.locators.get(key) else {
            continue;
        };
        let mut metadata = BlockMetadataRecord::decode(value)?;
        for header in [
            &mut metadata.log_header,
            &mut metadata.tx_header,
            &mut metadata.trace_header,
        ] {
            rewrite_block_blob_header(header, physical_key.clone(), *physical_base_offset)?;
        }
        *value = metadata.encode();
    }

    *blob_ops = coalescer.out;
    Ok(())
}

fn rewrite_block_blob_header(
    header_bytes: &mut Bytes,
    physical_key: Vec<u8>,
    physical_base_offset: u64,
) -> Result<()> {
    let mut header = BlockBlobHeader::decode(header_bytes.as_ref())?;
    // External blocks stage no block-blob write, so the coalescer never maps
    // their metadata key to a locator; reaching here would clobber the
    // archive key with a chain-data physical key.
    if header.is_external() {
        return Err(MonadChainDataError::Decode(
            "external payload header cannot be repointed by the blob coalescer",
        ));
    }
    header.physical_key = physical_key;
    header.physical_base_offset = physical_base_offset;
    *header_bytes = Bytes::from(header.encode());
    Ok(())
}

fn block_number_key(block_number: u64) -> [u8; 8] {
    block_number.to_be_bytes()
}

#[cfg(test)]
impl<M: MetaStore, B: BlobStore> Tables<M, B> {
    /// Test seed: durably write one sealed 10k directory bucket summary.
    pub(crate) async fn seed_dir_bucket(
        &self,
        family: Family,
        bucket_start: u64,
        bucket: &PrimaryDirBucket,
    ) {
        self.with_writes(|w| {
            Box::pin(async move {
                self.family(family)
                    .dir()
                    .stage_bucket(w, bucket_start, bucket);
                Ok(())
            })
        })
        .await
        .expect("seed dir bucket");
    }

    /// Test seed: durably write one block's primary-directory fragments.
    pub(crate) async fn seed_dir_fragment(
        &self,
        family: Family,
        block_number: u64,
        first_primary_id: u64,
        count: u32,
    ) {
        self.with_writes(|w| {
            Box::pin(async move {
                self.family(family).dir().stage_block_fragment(
                    w,
                    block_number,
                    first_primary_id,
                    count,
                );
                Ok(())
            })
        })
        .await
        .expect("seed dir fragment");
    }
}

#[derive(Debug)]
pub struct PublicationTables<M: MetaStore> {
    meta_store: M,
}

impl<M: MetaStore> PublicationTables<M> {
    pub const PUBLICATION_STATE_TABLE: TableId = TableId::new("publication_state");
    pub const PUBLICATION_STATE_KEY: &[u8] = b"state";

    /// Public so a binary can build the `Arc<PublicationTables>` the ingest
    /// engine requires without a `MonadChainDataService`.
    pub fn new(meta_store: M) -> Self {
        Self { meta_store }
    }

    pub async fn load_state(&self) -> Result<Option<PublicationState>> {
        let Some(bytes) = self
            .meta_store
            .get(Self::PUBLICATION_STATE_TABLE, Self::PUBLICATION_STATE_KEY)
            .await?
        else {
            return Ok(None);
        };

        Ok(Some(PublicationState::decode(&bytes)?))
    }

    pub async fn load_published_head(&self) -> Result<Option<u64>> {
        Ok(self
            .load_state()
            .await?
            .map(|state| state.indexed_finalized_head))
    }

    /// The published head usable for queries, or `None` when nothing is
    /// queryable yet. Head 0 is the acquire-before-first-publish sentinel (a
    /// writer claimed ownership but nothing is finalized); real heads start
    /// at 1 since ingest starts at block 1, so 0 maps to `None` like a
    /// never-written store.
    pub async fn queryable_head(&self) -> Result<Option<u64>> {
        Ok(self.load_published_head().await?.filter(|head| *head > 0))
    }

    /// Read the authoritative published head. A missing row is a cold start.
    pub async fn published_head(&self) -> Result<u64> {
        Ok(self.load_published_head().await?.unwrap_or(0))
    }

    /// Publish the new reader-visible head, carrying the standby row-chain
    /// digest forward.
    pub async fn publish(&self, new_head: u64, head_row_chain: ChainDigest) -> Result<()> {
        self.store_state(PublicationState {
            indexed_finalized_head: new_head,
            head_row_chain,
        })
        .await
    }

    pub async fn store_state(&self, state: PublicationState) -> Result<()> {
        self.meta_store
            .put(
                Self::PUBLICATION_STATE_TABLE,
                Self::PUBLICATION_STATE_KEY,
                Bytes::from(state.encode()),
            )
            .await
    }
}

pub struct BlockTables<M: MetaStore> {
    block_metadata: CachedKvTable<M, BlockRecord>,
    evm_header: CachedKvTable<M>,
    block_hash_to_number_index: CachedKvTable<M, u64>,
}

/// Per-block metadata row: the `block_record` plus the three per-family blob
/// headers. The bulky `EvmBlockHeader` lives in a separate table so hot
/// `load_record` reads don't pay ~500 B they never decode.
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
struct BlockMetadataRecord {
    block_record: Bytes,
    log_header: Bytes,
    tx_header: Bytes,
    trace_header: Bytes,
}

impl BlockMetadataRecord {
    fn encode(&self) -> Bytes {
        Bytes::from(alloy_rlp::encode(self))
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid block metadata rlp"))
    }
}

/// Cache decoder for the `BlockTables` view of the block-metadata row; runs
/// once per miss, so cache hits skip both RLP decodes.
fn decode_block_record(bytes: Bytes) -> Result<BlockRecord> {
    let metadata = BlockMetadataRecord::decode(&bytes)?;
    BlockRecord::decode(&metadata.block_record)
}

/// Per-family cache decoder yielding the decoded `BlockBlobHeader`, shared as
/// an `Arc` so a hit clones a refcount rather than the offsets vector; `pick`
/// selects which family's header field to decode.
fn decode_family_header(
    bytes: Bytes,
    pick: fn(&BlockMetadataRecord) -> &Bytes,
) -> Result<Arc<BlockBlobHeader>> {
    let metadata = BlockMetadataRecord::decode(&bytes)?;
    Ok(Arc::new(BlockBlobHeader::decode(pick(&metadata))?))
}

/// Decodes the hash-to-number index value: an 8-byte big-endian block number.
fn decode_block_number(bytes: Bytes) -> Result<u64> {
    let be: [u8; 8] = bytes
        .as_ref()
        .try_into()
        .map_err(|_| MonadChainDataError::Decode("invalid block_hash_to_number_index value"))?;
    Ok(u64::from_be_bytes(be))
}

impl<M: MetaStore> BlockTables<M> {
    pub const BLOCK_METADATA_TABLE: TableId = BLOCK_METADATA_TABLE_ID;
    pub const BLOCK_EVM_HEADER_TABLE: TableId = TableId::new("block_evm_header");
    pub const BLOCK_HASH_TO_NUMBER_INDEX_TABLE: TableId =
        TableId::new("block_hash_to_number_index");

    fn new(meta_store: M, cache: CacheConfig) -> Self {
        Self {
            block_metadata: CachedKvTable::new(
                meta_store.table(Self::BLOCK_METADATA_TABLE),
                cache.block_header_cache_bytes,
                decode_block_record,
            ),
            // Cold path (only `load_block` reads it); identity decode.
            evm_header: CachedKvTable::new(
                meta_store.table(Self::BLOCK_EVM_HEADER_TABLE),
                cache.block_header_cache_bytes,
                Ok,
            ),
            block_hash_to_number_index: CachedKvTable::new(
                meta_store.table(Self::BLOCK_HASH_TO_NUMBER_INDEX_TABLE),
                cache.block_hash_to_number_cache_bytes,
                decode_block_number,
            ),
        }
    }

    pub async fn load_record(&self, block_number: u64) -> Result<Option<BlockRecord>> {
        let key = block_number_key(block_number);
        self.block_metadata.get(&key).await
    }

    pub fn stage_metadata<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        block_number: u64,
        block_record: &BlockRecord,
        evm_header: &EvmBlockHeader,
        log_header: Bytes,
        tx_header: Bytes,
        trace_header: Bytes,
    ) {
        let key = block_number_key(block_number);
        let metadata = BlockMetadataRecord {
            block_record: Bytes::from(block_record.encode()),
            log_header,
            tx_header,
            trace_header,
        };
        w.put(&self.block_metadata, &key, metadata.encode());
        w.put(
            &self.evm_header,
            &key,
            Bytes::from(alloy_rlp::encode(evm_header)),
        );
    }

    pub async fn load_header(&self, block_number: u64) -> Result<Option<EvmBlockHeader>> {
        let key = block_number_key(block_number);
        let Some(bytes) = self.evm_header.get(&key).await? else {
            return Ok(None);
        };
        let header = EvmBlockHeader::decode(&mut bytes.as_ref())
            .map_err(|_| MonadChainDataError::Decode("invalid block header rlp"))?;
        Ok(Some(header))
    }

    /// Resolves a block hash to its block number. `Some(n)` does not guarantee
    /// block `n` is fully published — the index entry lands before the
    /// publication head advances, so follow-up loads may hit `MissingData`.
    pub async fn block_number_by_hash(&self, block_hash: &Hash32) -> Result<Option<u64>> {
        self.block_hash_to_number_index
            .get(block_hash.as_slice())
            .await
    }

    pub fn stage_hash_index<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        block_hash: &Hash32,
        block_number: u64,
    ) {
        w.put(
            &self.block_hash_to_number_index,
            block_hash.as_slice(),
            Bytes::copy_from_slice(&block_number.to_be_bytes()),
        );
    }

    pub(crate) fn collect_window_stats(&self, out: &mut Vec<(&'static str, u64, u64)>) {
        collect_kv_stats(out, &self.block_metadata);
        collect_kv_stats(out, &self.evm_header);
        collect_kv_stats(out, &self.block_hash_to_number_index);
    }

    pub async fn validate_continuity(
        &self,
        block: &FinalizedBlock,
        current_head: Option<u64>,
    ) -> Result<Option<BlockRecord>> {
        let Some(head) = current_head else {
            if block.block_number() != 1 {
                return Err(MonadChainDataError::InvalidBlock(
                    "first ingested block must be block 1 in the first pass",
                ));
            }
            return Ok(None);
        };

        if block.block_number() != head + 1 {
            return Err(MonadChainDataError::InvalidBlock(
                "block_number must extend the published head contiguously",
            ));
        }

        let previous = self
            .load_record(head)
            .await?
            .ok_or(MonadChainDataError::MissingData(
                "missing previous block record",
            ))?;
        if previous.block_hash != block.parent_hash() {
            return Err(MonadChainDataError::InvalidBlock(
                "parent_hash must match the previous published block",
            ));
        }
        Ok(Some(previous))
    }
}

#[derive(Clone)]
pub struct FamilyTables<M: MetaStore> {
    family: Family,
    block_metadata: CachedKvTable<M, Arc<BlockBlobHeader>>,
    dict_by_version: CachedKvTable<M>,
    dir: PrimaryDirTables<M>,
    bitmap: BitmapTables<M>,
    /// Standby seal-chain rows (`span_start` -> chained seal digest). Read
    /// only at recovery and by verification tooling — uncached.
    seal_chain: CachedKvTable<M, Hash32>,
}

/// Cache decoder for one seal-chain row: exactly 32 raw digest bytes.
fn decode_seal_chain(bytes: Bytes) -> Result<Hash32> {
    let raw: [u8; 32] = bytes
        .as_ref()
        .try_into()
        .map_err(|_| MonadChainDataError::Decode("invalid seal chain value"))?;
    Ok(Hash32::from(raw))
}

impl<M: MetaStore> FamilyTables<M> {
    fn new(meta_store: M, family: Family, cache: CacheConfig) -> Self {
        let ids = family.table_ids();
        let header_decoder: fn(Bytes) -> Result<Arc<BlockBlobHeader>> = match family {
            Family::Log => |b| decode_family_header(b, |m| &m.log_header),
            Family::Tx => |b| decode_family_header(b, |m| &m.tx_header),
            Family::Trace => |b| decode_family_header(b, |m| &m.trace_header),
        };
        Self {
            family,
            block_metadata: CachedKvTable::new(
                meta_store.table(BlockTables::<M>::BLOCK_METADATA_TABLE),
                cache.block_header_cache_bytes,
                header_decoder,
            ),
            // Uncached: `DictManager` memoizes decoders, so this table is read
            // at most once per (family, version) — a cache here could never
            // see a second lookup to hit.
            dict_by_version: CachedKvTable::new(meta_store.table(ids.dict_by_version), 0, Ok),
            dir: PrimaryDirTables::new(
                CachedScannableKvTable::new(
                    meta_store.scannable_table(ids.dir_by_block),
                    cache.dir_by_block_cache_bytes,
                    decode_fragment,
                ),
                CachedKvTable::new(
                    meta_store.table(ids.dir_bucket),
                    cache.dir_bucket_cache_bytes,
                    decode_bucket,
                ),
            ),
            bitmap: BitmapTables::new(
                CachedScannableKvTable::new(
                    meta_store.scannable_table(ids.bitmap_by_block),
                    cache.bitmap_by_block_cache_bytes,
                    decode_fragment_blob,
                ),
                CachedKvTable::new(
                    meta_store.table(ids.bitmap_page_blob),
                    cache.bitmap_page_blob_cache_bytes,
                    decode_page,
                ),
                CachedKvTable::new(
                    meta_store.table(ids.bitmap_page_counts),
                    cache.bitmap_page_counts_cache_bytes,
                    decode_page_counts,
                ),
                CachedScannableKvTable::new(
                    meta_store.scannable_table(ids.open_bitmap_stream),
                    cache.open_bitmap_stream_cache_bytes,
                    decode_open_streams_chunk,
                ),
            ),
            seal_chain: CachedKvTable::new(meta_store.table(ids.seal_chain), 0, decode_seal_chain),
        }
    }

    pub fn dir(&self) -> &PrimaryDirTables<M> {
        &self.dir
    }

    pub fn bitmap(&self) -> &BitmapTables<M> {
        &self.bitmap
    }

    pub fn family(&self) -> Family {
        self.family
    }

    /// Loads the decoded per-block blob header for this family from the read
    /// cache. `None` means the block's metadata row is absent.
    pub async fn load_blob_header(
        &self,
        block_number: u64,
    ) -> Result<Option<Arc<BlockBlobHeader>>> {
        let key = block_number_key(block_number);
        self.block_metadata.get(&key).await
    }

    /// Loads the raw bytes of the row-codec dictionary for `version`, or
    /// `None` if no such version has been persisted.
    pub async fn load_dict(&self, version: u32) -> Result<Option<Bytes>> {
        self.dict_by_version.get(&version.to_be_bytes()).await
    }

    /// Stages a row-codec dictionary version into the durable write path.
    pub fn stage_dict<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        version: u32,
        dict_bytes: Bytes,
    ) {
        w.put(&self.dict_by_version, &version.to_be_bytes(), dict_bytes);
    }

    pub async fn load_bucket_fragments(
        &self,
        bucket_start: u64,
    ) -> Result<Vec<PrimaryDirFragment>> {
        self.dir.load_bucket_fragments(bucket_start).await
    }

    /// The open bucket's fragments folded through `published_head` via the
    /// shared incremental fold (zero store reads while the head is unchanged).
    pub(crate) async fn load_open_bucket_fold(
        &self,
        bucket_start: u64,
        published_head: u64,
    ) -> Result<Arc<Vec<PrimaryDirFragment>>> {
        self.dir
            .load_open_bucket_fold(bucket_start, published_head)
            .await
    }

    pub async fn load_bucket(&self, bucket_start: u64) -> Result<Option<PrimaryDirBucket>> {
        self.dir.load_bucket(bucket_start).await
    }

    pub async fn load_bitmap_fragments(
        &self,
        stream_id: &str,
        page_start: u64,
    ) -> Result<Vec<Arc<DecodedBitmapFragment>>> {
        self.bitmap.load_fragments(stream_id, page_start).await
    }

    /// The open page's bitmap folded through `published_head` via the shared
    /// incremental fold (zero store reads while the head is unchanged).
    pub(crate) async fn load_open_bitmap_page_fold(
        &self,
        stream_id: &str,
        page_start: u64,
        published_head: u64,
    ) -> Result<Arc<DecodedBitmapPage>> {
        self.bitmap
            .load_open_page_fold(stream_id, page_start, published_head)
            .await
    }

    /// Loads a compacted bitmap page for one sealed stream page, decoded
    /// (metadata + roaring bitmap) and served from the read cache.
    pub async fn load_bitmap_page_artifact(
        &self,
        stream_id: &str,
        page_start: u64,
    ) -> Result<Option<Arc<DecodedBitmapPage>>> {
        self.bitmap.load_page_artifact(stream_id, page_start).await
    }

    /// Loads one stream's page-count manifest for one sealed page group.
    pub async fn load_bitmap_page_counts(
        &self,
        stream_id: &str,
        group_start: u64,
    ) -> Result<Option<BitmapPageCounts>> {
        self.bitmap.load_page_counts(stream_id, group_start).await
    }

    /// Loads the open stream inventory for one frontier page.
    pub async fn load_open_bitmap_streams(&self, page_start: u64) -> Result<Vec<String>> {
        self.bitmap.load_open_streams(page_start).await
    }

    /// Loads the chained seal digest persisted when span `span_start` sealed.
    /// `None` for spans sealed before the seal chain existed (or never sealed).
    pub async fn load_seal_chain(&self, span_start: u64) -> Result<Option<Hash32>> {
        self.seal_chain.get(&span_start.to_be_bytes()).await
    }

    /// Stages span `span_start`'s chained seal digest; callers stage it in the
    /// SAME write session as the span's sealed artifacts.
    pub fn stage_seal_chain<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        span_start: u64,
        value: Hash32,
    ) {
        w.put(
            &self.seal_chain,
            &span_start.to_be_bytes(),
            Bytes::copy_from_slice(value.as_slice()),
        );
    }

    pub(crate) fn collect_window_stats(&self, out: &mut Vec<(&'static str, u64, u64)>) {
        // The per-family header cache shares its `TableId` ("block_metadata")
        // with `BlockTables`' record cache, so report it under the family-
        // prefixed label declared with this family's table ids.
        push_window_stats(
            out,
            self.family.table_ids().block_metadata_cache_label,
            self.block_metadata.take_window_stats(),
        );
        collect_kv_stats(out, &self.dict_by_version);
        collect_scan_stats(out, self.dir.fragments_cache());
        collect_kv_stats(out, self.dir.buckets_cache());
        collect_scan_stats(out, self.bitmap.fragments_cache());
        collect_kv_stats(out, self.bitmap.page_blobs_cache());
        collect_kv_stats(out, self.bitmap.page_counts_cache());
        collect_scan_stats(out, self.bitmap.open_streams_cache());
        collect_kv_stats(out, &self.seal_chain);
    }
}

#[cfg(test)]
mod tests {
    use super::PublicationTables;
    use crate::{engine::digest::EMPTY_DIGEST, store::InMemoryMetaStore};

    // NOTE: `all_logical_table_names_match_declared_table_ids` (the catalog ⇄
    // declared-`TableId` consistency check) lives in monad-chain-data: it also
    // references `ingest::snapshot::SnapshotStore`, which is a chain-data type,
    // so the engine layer can't host it.

    #[tokio::test]
    async fn publication_tables_read_absent_and_publish_head() {
        let tables = PublicationTables::new(InMemoryMetaStore::default());

        assert_eq!(tables.published_head().await.unwrap(), 0);

        tables.publish(7, EMPTY_DIGEST).await.unwrap();
        assert_eq!(tables.published_head().await.unwrap(), 7);

        tables.publish(9, EMPTY_DIGEST).await.unwrap();
        assert_eq!(tables.published_head().await.unwrap(), 9);
    }
}
