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
    fmt,
    sync::{Arc, RwLock},
    time::Duration,
};

use alloy_rlp::{Decodable, RlpDecodable, RlpEncodable};
use bytes::Bytes;
use tokio::sync::Semaphore;
use zstd::dict::DecoderDictionary;

use crate::{
    engine::{
        bitmap::{BitmapBlob, BitmapTables, DecodedBitmapPage},
        family::{Family, BLOCK_BLOB_TABLE},
        primary_dir::{PrimaryDirBucket, PrimaryDirFragment, PrimaryDirTables},
        row_codec::{
            decode_row_frame, should_sample_row, RowCodec, RowCodecState, DICT_VERSION_NONE,
            ROW_ZSTD_LEVEL,
        },
    },
    error::{MonadChainDataError, Result},
    family::{FinalizedBlock, Hash32},
    primitives::{
        state::{BlockBlobHeader, BlockRecord, PublicationState},
        EvmBlockHeader,
    },
    store::{
        BlobStore, BlobTable, BlobWriteOp, BlockRegionCache, CacheConfig, CachedKvTable,
        CachedScannableTable, MetaStore, MetaWriteOp, ScannableTableId, SessionFuture, TableId,
        WriteSession,
    },
    txs::TxHashIndexTable,
};

/// Deterministic, epoch-based dictionary lifecycle parameters.
///
/// The dictionary version of block `n` is `n / epoch_blocks`; that epoch
/// number *is* the version (still stamped on the header). Version `V >= 1` is
/// trained from a sample of the first `sample_span` blocks of epoch `V - 1`
/// and must be published before any block of epoch `V` is written. Epoch 0 is
/// the version-0 plain bootstrap.
#[derive(Debug, Clone, Copy)]
pub struct DictConfig {
    /// Blocks per epoch; the epoch number doubles as the dict version.
    pub epoch_blocks: u64,
    /// How many leading blocks of an epoch are sampled to train the next
    /// epoch's dictionary. Clamped to `epoch_blocks` at read time.
    pub sample_span: u64,
    /// Maximum trained dictionary size in bytes.
    pub max_dict_size_bytes: usize,
    /// Minimum number of collected row samples before a non-empty dictionary
    /// is trained; below this a family publishes the empty-dict sentinel.
    pub min_training_samples: usize,
}

impl Default for DictConfig {
    fn default() -> Self {
        Self {
            epoch_blocks: 1_000_000,
            sample_span: 900_000,
            max_dict_size_bytes: 112 * 1024,
            min_training_samples: 4_096,
        }
    }
}

impl DictConfig {
    /// Clamped training sample range `[start, end)` for version `V >= 1`,
    /// drawn from the leading blocks of epoch `V - 1`.
    pub fn training_range(&self, version: u32) -> (u64, u64) {
        let prev_epoch = u64::from(version - 1);
        let start = prev_epoch * self.epoch_blocks;
        let span = self.sample_span.min(self.epoch_blocks);
        (start, start + span)
    }
}

const BLOCK_BLOB_COALESCE_TARGET_BYTES: usize = 512 * 1024;
const BLOCK_METADATA_TABLE_ID: TableId = TableId::new("block_metadata");

/// Holds the per-family row-codec dictionary state: a write-side per-version
/// codec map (version-0 plain pre-installed), a read-side decoder cache, and
/// per-family single-flight training locks. Lives on [`Tables`] so both ingest
/// and the materializers can reach it.
pub struct DictManager {
    /// Write-side prepared codecs keyed by `(family, version)`. The version-0
    /// plain codec is pre-installed for every family; epochs `>= 1` are
    /// installed by `install_version` once their dictionary is published.
    codecs: RwLock<HashMap<(Family, u32), Arc<RowCodecState>>>,
    decoders: RwLock<HashMap<(Family, u32), Arc<DecoderDictionary<'static>>>>,
    /// Per-family single-flight gate so concurrent `ensure_epoch_dict` callers
    /// (plan path + background pre-train) collapse onto one training run.
    train_locks: BTreeMap<Family, futures::lock::Mutex<()>>,
    config: DictConfig,
}

impl DictManager {
    fn new(config: DictConfig) -> Self {
        let mut codecs = HashMap::new();
        let mut train_locks = BTreeMap::new();
        for family in [Family::Log, Family::Tx, Family::Trace] {
            codecs.insert(
                (family, DICT_VERSION_NONE),
                Arc::new(RowCodecState::bootstrap()),
            );
            train_locks.insert(family, futures::lock::Mutex::new(()));
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
    pub fn train_lock(&self, family: Family) -> &futures::lock::Mutex<()> {
        self.train_locks
            .get(&family)
            .expect("family registered at construction")
    }

    /// Returns the write-side codec for `(family, version)` if it has been
    /// installed, else `None`.
    pub fn write_codec(&self, family: Family, version: u32) -> Option<RowCodec> {
        self.codecs
            .read()
            .expect("dict codecs lock poisoned")
            .get(&(family, version))
            .map(|state| state.snapshot())
    }

    /// Whether a write-side codec for `(family, version)` is installed.
    pub fn has_codec(&self, family: Family, version: u32) -> bool {
        self.codecs
            .read()
            .expect("dict codecs lock poisoned")
            .contains_key(&(family, version))
    }

    /// Installs the write-side codec for a published dictionary version. Empty
    /// `dict_bytes` is the sentinel "this epoch uses plain frames" — a plain
    /// codec stamped with `version`. Non-empty bytes build a dictionary codec
    /// and also seed the read-side decoder cache.
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
    pub fn insert_decoder(
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

#[derive(Debug, Clone, Default)]
pub struct MetaBlobTimings {
    pub meta: Duration,
    pub blob: Duration,
    pub writes: WriteOpCounts,
}

#[derive(Debug, Clone, Default)]
pub struct StagedWrites {
    pub meta_ops: Vec<MetaWriteOp>,
    pub blob_ops: Vec<BlobWriteOp>,
    pub counts: WriteOpCounts,
}

impl StagedWrites {
    pub fn merge(&mut self, mut other: Self) {
        self.meta_ops.append(&mut other.meta_ops);
        self.blob_ops.append(&mut other.blob_ops);
        self.counts.merge(&other.counts);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteOpCount {
    pub ops: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteOpCounts {
    by_table: BTreeMap<String, WriteOpCount>,
}

impl WriteOpCounts {
    pub fn from_writes(meta_ops: &[MetaWriteOp], blob_ops: &[BlobWriteOp]) -> Self {
        let mut counts = Self::default();
        for op in meta_ops {
            match op {
                MetaWriteOp::Put { table, value, .. } => {
                    counts.add(format!("kv:{}", table.as_str()), value.len());
                }
                MetaWriteOp::ScanPut { table, value, .. } => {
                    counts.add(format!("scan:{}", table.as_str()), value.len());
                }
            }
        }
        for BlobWriteOp { table, value, .. } in blob_ops {
            counts.add(format!("blob:{}", table.as_str()), value.len());
        }
        counts
    }

    pub fn merge(&mut self, other: &Self) {
        for (table, count) in &other.by_table {
            let entry = self.by_table.entry(table.clone()).or_default();
            entry.ops = entry.ops.saturating_add(count.ops);
            entry.bytes = entry.bytes.saturating_add(count.bytes);
        }
    }

    pub fn total_ops(&self) -> u64 {
        self.by_table.values().map(|count| count.ops).sum()
    }

    /// Write-op count for one prefixed table key (e.g. `scan:log_open_bitmap_stream`,
    /// `kv:block_metadata`), or 0 if the table saw no writes. Used by telemetry
    /// drill-downs and by tests asserting per-table write behavior.
    pub fn ops_for_table(&self, table: &str) -> u64 {
        self.by_table.get(table).map_or(0, |count| count.ops)
    }

    pub fn total_bytes(&self) -> u64 {
        self.by_table.values().map(|count| count.bytes).sum()
    }

    pub fn top_by_ops(&self, limit: usize) -> WriteOpTopList<'_> {
        WriteOpTopList {
            counts: self,
            limit,
            sort_by_bytes: false,
        }
    }

    pub fn top_by_bytes(&self, limit: usize) -> WriteOpTopList<'_> {
        WriteOpTopList {
            counts: self,
            limit,
            sort_by_bytes: true,
        }
    }

    fn add(&mut self, table: String, bytes: usize) {
        let entry = self.by_table.entry(table).or_default();
        entry.ops = entry.ops.saturating_add(1);
        entry.bytes = entry.bytes.saturating_add(bytes as u64);
    }
}

pub struct WriteOpTopList<'a> {
    counts: &'a WriteOpCounts,
    limit: usize,
    sort_by_bytes: bool,
}

impl fmt::Display for WriteOpTopList<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entries: Vec<_> = self.counts.by_table.iter().collect();
        if self.sort_by_bytes {
            entries.sort_by(|a, b| b.1.bytes.cmp(&a.1.bytes).then_with(|| a.0.cmp(b.0)));
        } else {
            entries.sort_by(|a, b| b.1.ops.cmp(&a.1.ops).then_with(|| a.0.cmp(b.0)));
        }
        for (idx, (table, count)) in entries.into_iter().take(self.limit).enumerate() {
            if idx > 0 {
                f.write_str(",")?;
            }
            write!(f, "{}:{}ops/{}bytes", table, count.ops, count.bytes)?;
        }
        Ok(())
    }
}

pub struct Tables<M: MetaStore, B: BlobStore> {
    meta_store: M,
    blob_store: B,
    blocks: BlockTables<M>,
    tx_hash_index: TxHashIndexTable<M>,
    /// Raw shared-per-block blob table. Writes go here directly (the region
    /// cache is read-populated only); range reads on a region-cache miss also
    /// go here.
    block_blobs: BlobTable<B>,
    /// In-memory cache of compressed per-(family, block) regions, serving both
    /// point and full-scan reads. See [`BlockRegionCache`].
    block_regions: BlockRegionCache<B>,
    /// Whether the region cache has a non-zero budget. When disabled, point
    /// materialization must stay on the single-frame range-read path even if
    /// the region would otherwise be small enough to cache.
    block_region_cache_enabled: bool,
    /// Compressed-region size cap: regions larger than this are read uncached
    /// (full scans read the whole region; point reads do a single-frame range
    /// read) so a giant object is never pulled into RAM. From
    /// [`CacheConfig::block_region_max_bytes`].
    block_region_max_bytes: usize,
    families: BTreeMap<Family, FamilyTables<M>>,
    dicts: DictManager,
    /// Process-global, byte-weighted budget bounding concurrent stage-2 block
    /// materialization (decode) across all queries. Shared by the query runner;
    /// see `family_runner::block_materialize_permits`.
    materialize_budget: Arc<Semaphore>,
    /// Tunable query-runtime limits (fan-out + budgets). Read by the query
    /// runner; the semaphores above are sized from it at construction.
    query: QueryRuntimeConfig,
}

/// Tunable, process-global limits on the read path. Defaults are moderate —
/// safe for a fast local backend, where wide fan-out is pure scheduling
/// overhead. High-latency backends (S3/Dynamo) should raise
/// `blob_io_concurrency` and `materialize_concurrency` so a sparse query (≈ 1
/// record/block over many blocks) fans its reads out toward a single
/// round-trip; the budgets still bound aggregate load across concurrent queries.
#[derive(Debug, Clone, Copy)]
pub struct QueryRuntimeConfig {
    /// Global cap on concurrent backend blob reads, shared by the block-blob
    /// table and the region cache (bounds reads across every query path).
    pub blob_io_concurrency: usize,
    /// Per-request ceiling on blocks materialized concurrently in stage 2 (the
    /// `buffered` width). The block-read fan-out for a single query.
    pub materialize_concurrency: usize,
    /// Total permits in the process-global, byte-weighted stage-2 decode budget.
    pub materialize_budget_permits: usize,
    /// Bytes per decode-budget permit. A block acquires
    /// `clamp(decode_bytes / this, 1, materialize_budget_permits)` permits, so
    /// tiny blocks fan out while a huge region runs ~exclusively.
    pub materialize_permit_bytes: usize,
}

impl Default for QueryRuntimeConfig {
    fn default() -> Self {
        Self {
            // `materialize_concurrency` is the single primary knob. Default low:
            // safe for a fast local backend (wide fan-out is pure scheduling
            // overhead there). Raise it (toward the hundreds/thousand) for a
            // high-latency backend so a sparse query reaches ≈ one round-trip.
            materialize_concurrency: 8,
            // High ceilings that only engage once `materialize_concurrency` is
            // raised: at the default fan-out neither binds. They cap aggregate
            // backend reads / concurrent decode bytes across all queries.
            blob_io_concurrency: 1024,
            materialize_budget_permits: 1024,
            // 32 KiB/permit: a tiny block takes one permit (fans out to the
            // budget) while a 40 MiB trace region takes >1000 → clamped to the
            // budget → runs exclusively.
            materialize_permit_bytes: 32 * 1024,
        }
    }
}

impl QueryRuntimeConfig {
    /// Clamps every field to a sane minimum (≥ 1 permit/byte) so a zero from
    /// config can never wedge a semaphore or divide-by-zero the permit math.
    fn sanitized(self) -> Self {
        Self {
            blob_io_concurrency: self.blob_io_concurrency.max(1),
            materialize_concurrency: self.materialize_concurrency.max(1),
            materialize_budget_permits: self.materialize_budget_permits.max(1),
            materialize_permit_bytes: self.materialize_permit_bytes.max(1),
        }
    }
}

impl<M: MetaStore, B: BlobStore> Tables<M, B> {
    pub fn new(meta_store: M, blob_store: B) -> Self {
        Self::with_cache_config(meta_store, blob_store, CacheConfig::default())
    }

    pub fn with_cache_config(meta_store: M, blob_store: B, cache: CacheConfig) -> Self {
        Self::with_configs(meta_store, blob_store, cache, DictConfig::default())
    }

    /// Constructor threading a [`DictConfig`] (e.g. tests exercising the
    /// epoch-based dictionary lifecycle); uses default [`QueryRuntimeConfig`].
    pub fn with_configs(
        meta_store: M,
        blob_store: B,
        cache: CacheConfig,
        dict_config: DictConfig,
    ) -> Self {
        Self::with_all_configs(
            meta_store,
            blob_store,
            cache,
            dict_config,
            QueryRuntimeConfig::default(),
        )
    }

    /// Full constructor also threading the [`QueryRuntimeConfig`] read-path
    /// limits (fan-out + budgets). The reader-open path passes operator config;
    /// all other callers get the defaults via the constructors above.
    pub fn with_all_configs(
        meta_store: M,
        blob_store: B,
        cache: CacheConfig,
        dict_config: DictConfig,
        query: QueryRuntimeConfig,
    ) -> Self {
        let query = query.sanitized();
        let mut families = BTreeMap::new();
        families.insert(
            Family::Log,
            FamilyTables::new(meta_store.clone(), Family::Log, cache),
        );
        families.insert(
            Family::Tx,
            FamilyTables::new(meta_store.clone(), Family::Tx, cache),
        );
        families.insert(
            Family::Trace,
            FamilyTables::new(meta_store.clone(), Family::Trace, cache),
        );
        // Shared, process-global read limiter for the block-blob backend, held
        // by both the raw block-blob table and the region cache so the cap is
        // global across every blob-read path.
        let blob_io = Arc::new(Semaphore::new(query.blob_io_concurrency));
        Self {
            blocks: BlockTables::new(meta_store.clone(), cache),
            tx_hash_index: TxHashIndexTable::new(meta_store.clone(), cache),
            block_blobs: blob_store
                .table(BLOCK_BLOB_TABLE)
                .with_io_limit(Arc::clone(&blob_io)),
            block_regions: BlockRegionCache::new(
                blob_store
                    .table(BLOCK_BLOB_TABLE)
                    .with_io_limit(Arc::clone(&blob_io)),
                cache.block_region_cache_bytes,
            ),
            block_region_cache_enabled: cache.block_region_cache_bytes > 0,
            block_region_max_bytes: cache.block_region_max_bytes,
            meta_store,
            blob_store,
            families,
            dicts: DictManager::new(dict_config),
            materialize_budget: Arc::new(Semaphore::new(query.materialize_budget_permits)),
            query,
        }
    }

    /// The process-global stage-2 decode budget (see field docs).
    pub(crate) fn materialize_budget(&self) -> &Arc<Semaphore> {
        &self.materialize_budget
    }

    /// Per-request stage-2 materialization fan-out (the `buffered` width).
    pub(crate) fn materialize_concurrency(&self) -> usize {
        self.query.materialize_concurrency
    }

    /// Total permits in the decode budget — the clamp ceiling for a block's
    /// per-block permit weight.
    pub(crate) fn materialize_budget_permits(&self) -> usize {
        self.query.materialize_budget_permits
    }

    /// Bytes per decode-budget permit (the permit-weight unit).
    pub(crate) fn materialize_permit_bytes(&self) -> usize {
        self.query.materialize_permit_bytes
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

    /// Raw byte-range read of the shared per-block blob object, bypassing the
    /// region cache. Used for the large-region point-read fallback and as the
    /// single backend fetch the region cache coalesces concurrent misses onto.
    pub async fn read_block_blob_range(
        &self,
        block_number: u64,
        start: usize,
        end_exclusive: usize,
    ) -> Result<Option<alloy_primitives::Bytes>> {
        let key = block_number_key(block_number);
        Ok(self
            .block_blobs
            .read_range(&key, start, end_exclusive)
            .await?
            .map(Into::into))
    }

    pub async fn read_block_blob_header_range(
        &self,
        block_number: u64,
        header: &BlockBlobHeader,
        start: usize,
        end_exclusive: usize,
    ) -> Result<Option<alloy_primitives::Bytes>> {
        let default_key = block_number_key(block_number);
        Ok(self
            .block_blobs
            .read_range(header.physical_key(&default_key), start, end_exclusive)
            .await?
            .map(Into::into))
    }

    /// Returns this family's WHOLE compressed region for `block_number`,
    /// serving it from the [`BlockRegionCache`] (populating on miss when the
    /// region is small enough to admit). Callers slice frames out with
    /// FAMILY-RELATIVE offsets (`header.offsets[i]`), which line up exactly
    /// because the returned buffer starts at the family's `base_offset`.
    ///
    /// A region larger than [`CacheConfig::block_region_max_bytes`] is read uncached (the
    /// caller still gets the bytes, we just never resident-cache a giant
    /// object). The full-scan read paths always go through here.
    pub async fn read_block_blob_region(
        &self,
        family: Family,
        block_number: u64,
        header: &BlockBlobHeader,
    ) -> Result<Option<Bytes>> {
        let (region_start, region_end) = header.region_range();
        let region_len = region_end.saturating_sub(region_start);
        if !self.block_region_cache_enabled || region_len > self.block_region_max_bytes {
            // Too big to cache: read it directly without admitting it.
            let default_key = block_number_key(block_number);
            return Ok(self
                .block_blobs
                .read_range(header.physical_key(&default_key), region_start, region_end)
                .await?
                .map(Into::into));
        }
        let default_key = block_number_key(block_number);
        self.block_regions
            .get_region(
                family.slot(),
                block_number,
                header.physical_key(&default_key).to_vec(),
                region_start,
                region_end,
            )
            .await
    }

    /// Returns the single compressed frame for row `idx_in_block` of `family`.
    ///
    /// On a region-cache hit (or for a small uncached region we choose to
    /// admit) this slices the frame out of the cached region using
    /// FAMILY-RELATIVE offsets. For a LARGE region (above
    /// [`CacheConfig::block_region_max_bytes`]) it falls back to a single-frame ABSOLUTE
    /// range read so a one-shot point read never pulls a huge region into RAM.
    pub async fn read_block_blob_frame(
        &self,
        family: Family,
        block_number: u64,
        header: &BlockBlobHeader,
        idx_in_block: usize,
    ) -> Result<Option<Bytes>> {
        let (region_start, region_end) = header.region_range();
        let region_len = region_end.saturating_sub(region_start);
        if !self.block_region_cache_enabled || region_len > self.block_region_max_bytes {
            // Bandwidth-frugal large-region path: one absolute single-frame
            // read, no caching.
            let (start, end) = header.abs_range(idx_in_block);
            let default_key = block_number_key(block_number);
            return Ok(self
                .block_blobs
                .read_range(header.physical_key(&default_key), start, end)
                .await?
                .map(Into::into));
        }
        // Small region: fetch (or hit) the whole region via the cache, then
        // slice the row's frame with family-relative offsets. region-relative
        // offset = abs offset - base_offset, and the cached buffer starts at
        // base_offset, so `header.offsets[idx]` indexes the buffer directly.
        let default_key = block_number_key(block_number);
        let Some(region) = self
            .block_regions
            .get_region(
                family.slot(),
                block_number,
                header.physical_key(&default_key).to_vec(),
                region_start,
                region_end,
            )
            .await?
        else {
            return Ok(None);
        };
        let start = header.offsets[idx_in_block] as usize;
        let end = header.offsets[idx_in_block + 1] as usize;
        if start > end || end > region.len() {
            return Err(MonadChainDataError::Decode(
                "invalid block blob frame range",
            ));
        }
        Ok(Some(region.slice(start..end)))
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

    /// Returns the table set for a family. Panics if the family was not
    /// registered at construction; the `Family` enum is closed, so missing
    /// a variant here is a programmer error, not a data error.
    pub fn family(&self, family: Family) -> &FamilyTables<M> {
        self.families
            .get(&family)
            .expect("family registered at construction")
    }

    /// Per-family row-codec dictionary manager (write-side per-version codecs,
    /// read-side decoder cache, single-flight training locks).
    pub fn dicts(&self) -> &DictManager {
        &self.dicts
    }

    /// Resolves the decoder dictionary for `(family, version)`, populating the
    /// read cache from the durable dict table on a miss. Returns `None` for a
    /// plain frame: version 0, or a version `>= 1` that published the
    /// empty-dict sentinel. A version `>= 1` whose bytes are *absent* (never
    /// published) is a hard error — an invariant violation, since every block
    /// is written only after its epoch dictionary is durably published.
    async fn resolve_decoder(
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

    /// Decodes a single compressed row frame for `family` written under
    /// `dict_version`. The materializers' point-read path calls this after a
    /// byte-range read returns the frame.
    pub async fn decode_block_row(
        &self,
        family: Family,
        dict_version: u32,
        frame: &[u8],
    ) -> Result<Bytes> {
        let decoder = self.resolve_decoder(family, dict_version).await?;
        decode_row_frame(decoder.as_ref(), frame)
    }

    /// Resolves the decoder dictionary once for a whole-block (full-scan)
    /// decode pass. Returns `None` for version 0 (plain frames).
    pub async fn block_decoder(
        &self,
        family: Family,
        dict_version: u32,
    ) -> Result<Option<Arc<DecoderDictionary<'static>>>> {
        self.resolve_decoder(family, dict_version).await
    }

    /// Ensures `version`'s dictionary for one family is durably published and its
    /// write-side codec installed, blocking until ready. Idempotent + single-
    /// flight: the fast path returns if already installed; otherwise it adopts a
    /// durably-published dict, or trains one from epoch `version - 1`'s blocks.
    ///
    /// Operates purely over `&self` (no service/reader needed) so both the query
    /// service and the standalone ingest engine's codec resolver drive the same
    /// logic over the same `Tables` — see [`crate::ingest_resolver`].
    pub async fn ensure_epoch_dict(&self, family: Family, version: u32) -> Result<()> {
        // Version 0 is the implicit plain bootstrap; its codec is pre-installed.
        if version == DICT_VERSION_NONE {
            return Ok(());
        }
        let dicts = self.dicts();
        // Fast path: already installed.
        if dicts.has_codec(family, version) {
            return Ok(());
        }
        // Single-flight gate + double-check.
        let _g = dicts.train_lock(family).lock().await;
        if dicts.has_codec(family, version) {
            return Ok(());
        }

        // Durable dict already published (e.g. a peer/standby wrote it, or a
        // prior run): adopt it. `Some(empty)` is the plain sentinel.
        if let Some(bytes) = self.family(family).load_dict(version).await? {
            dicts.install_version(family, version, &bytes);
            return Ok(());
        }

        // Otherwise train from epoch `version - 1`'s leading blocks.
        let samples = self.read_back_training_samples(family, version).await?;
        let dict_bytes: Vec<u8> = if samples.len() >= dicts.config().min_training_samples {
            let max_size = dicts.config().max_dict_size_bytes;
            // `from_samples` is CPU-bound; run it on rayon's pool and await the
            // result so the async executor thread isn't blocked.
            let (tx, rx) = futures::channel::oneshot::channel();
            rayon::spawn(move || {
                let _ = tx.send(zstd::dict::from_samples(&samples, max_size));
            });
            let trained = rx
                .await
                .map_err(|_| MonadChainDataError::Backend("dict training canceled".into()))?;
            match trained {
                // A non-empty trained dictionary; otherwise fall back to the
                // empty-dict sentinel (this epoch uses plain frames).
                Ok(bytes) if !bytes.is_empty() => bytes,
                _ => Vec::new(),
            }
        } else {
            // Too few samples: publish the empty-dict sentinel.
            Vec::new()
        };

        // Persist durably BEFORE installing the codec, so a block written under
        // `version` always has its dict (or sentinel) present on the read path.
        let dict_for_write = Bytes::from(dict_bytes.clone());
        self.with_writes(|w| {
            let dict_for_write = dict_for_write.clone();
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
        for family in [Family::Log, Family::Tx, Family::Trace] {
            self.ensure_epoch_dict(family, version).await?;
        }
        Ok(())
    }

    /// Collects a deterministic training corpus for `(family, version)` from the
    /// leading blocks of epoch `version - 1`. Block selection is strided (seeded
    /// by `version`, no RNG) so the corpus is reproducible. At most
    /// `TARGET_SAMPLE_BLOCKS` blocks are sampled, each contributing up to
    /// `PER_BLOCK_SAMPLE_CAP` rows via [`should_sample_row`]. Sampled blocks
    /// belong to epoch `version - 1`, whose dictionary was published earlier, so
    /// `decode_block_row` just works.
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
            let Some(header) = family_tables.load_block_header(block_number).await? else {
                continue;
            };
            if header.offsets.len() < 2 {
                continue;
            }
            let Some(region) = self
                .read_block_blob_region(family, block_number, &header)
                .await?
            else {
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

    pub async fn with_writes<'a, F>(&'a self, f: F) -> Result<()>
    where
        F: for<'s> FnOnce(&'s mut WriteSession<'a, M, B>) -> SessionFuture<'s>,
    {
        let (result, _timings) = self.with_writes_timed(f).await;
        result
    }

    /// Like [`Self::with_writes`] but returns the meta/blob apply durations
    /// separately so callers preserving per-phase instrumentation can
    /// attribute commit latency. On closure error or backend error the
    /// returned durations reflect work actually performed; zero when the
    /// closure short-circuited the flush.
    pub async fn with_writes_timed<'a, F>(&'a self, f: F) -> (Result<()>, MetaBlobTimings)
    where
        F: for<'s> FnOnce(&'s mut WriteSession<'a, M, B>) -> SessionFuture<'s>,
    {
        let mut session = WriteSession::new(self);
        let closure_result = f(&mut session).await;
        if let Err(e) = closure_result {
            return (Err(e), MetaBlobTimings::default());
        }
        let mut meta_ops = session.take_meta();
        let mut blob_ops = session.take_blob();
        if let Err(e) = coalesce_block_blob_writes(&mut meta_ops, &mut blob_ops) {
            return (Err(e), MetaBlobTimings::default());
        }
        let writes = WriteOpCounts::from_writes(&meta_ops, &blob_ops);
        let meta_apply = async {
            let t = std::time::Instant::now();
            self.meta_store.apply_writes(meta_ops).await?;
            Ok::<_, crate::error::MonadChainDataError>(t.elapsed())
        };
        let blob_apply = async {
            let t = std::time::Instant::now();
            self.blob_store.apply_writes(blob_ops).await?;
            Ok::<_, crate::error::MonadChainDataError>(t.elapsed())
        };
        match futures::try_join!(meta_apply, blob_apply) {
            Ok((meta_elapsed, blob_elapsed)) => (
                Ok(()),
                MetaBlobTimings {
                    meta: meta_elapsed,
                    blob: blob_elapsed,
                    writes,
                },
            ),
            Err(e) => (Err(e), MetaBlobTimings::default()),
        }
    }

    /// Stages write operations without applying them. The read caches are
    /// read-populated only, so staging touches no cache and there is nothing
    /// to roll back if the writes are later applied, retried, or abandoned by
    /// a pipelined caller.
    pub async fn stage_writes<'a, F>(&'a self, f: F) -> Result<StagedWrites>
    where
        F: for<'s> FnOnce(&'s mut WriteSession<'a, M, B>) -> SessionFuture<'s>,
    {
        let mut session = WriteSession::new(self);
        let closure_result = f(&mut session).await;
        closure_result?;
        let mut meta_ops = session.take_meta();
        let mut blob_ops = session.take_blob();
        coalesce_block_blob_writes(&mut meta_ops, &mut blob_ops)?;
        let counts = WriteOpCounts::from_writes(&meta_ops, &blob_ops);
        Ok(StagedWrites {
            meta_ops,
            blob_ops,
            counts,
        })
    }

    pub async fn apply_staged_writes_timed(&self, writes: StagedWrites) -> Result<MetaBlobTimings> {
        let counts = writes.counts;
        let meta_apply = async {
            let t = std::time::Instant::now();
            self.meta_store.apply_writes(writes.meta_ops).await?;
            Ok::<_, crate::error::MonadChainDataError>(t.elapsed())
        };
        let blob_apply = async {
            let t = std::time::Instant::now();
            self.blob_store.apply_writes(writes.blob_ops).await?;
            Ok::<_, crate::error::MonadChainDataError>(t.elapsed())
        };
        let (meta, blob) = futures::try_join!(meta_apply, blob_apply)?;
        Ok(MetaBlobTimings {
            meta,
            blob,
            writes: counts,
        })
    }

    /// Drains and returns the (hits, misses) counters for every cached
    /// wrapper, tagged with its logical table name. Each call resets the
    /// counters so consumers see per-window deltas.
    pub fn take_cache_window_stats(&self) -> Vec<(&'static str, u64, u64)> {
        let mut out = Vec::new();
        self.blocks.collect_window_stats(&mut out);
        self.tx_hash_index.collect_window_stats(&mut out);
        // Per-family region-cache hit/miss, tagged with a stable per-family
        // label so the log line stays legible.
        for (slot, (h, m)) in self
            .block_regions
            .take_window_stats()
            .into_iter()
            .enumerate()
        {
            if h == 0 && m == 0 {
                continue;
            }
            let name = match Family::from_slot(slot) {
                Some(Family::Log) => "block_region:log",
                Some(Family::Tx) => "block_region:tx",
                Some(Family::Trace) => "block_region:trace",
                None => "block_region:?",
            };
            out.push((name, h, m));
        }
        for fam in self.families.values() {
            fam.collect_window_stats(&mut out);
        }
        out
    }
}

/// Pushes one (name, hits, misses) tuple per cached wrapper, skipping rows
/// where both counters are zero so the per-window log line stays compact.
pub(crate) fn collect_kv_stats<M: MetaStore, V>(
    out: &mut Vec<(&'static str, u64, u64)>,
    table: &CachedKvTable<M, V>,
) where
    V: Clone + Send + Sync + 'static,
{
    let (h, m) = table.take_window_stats();
    if h != 0 || m != 0 {
        out.push((table.table_id().as_str(), h, m));
    }
}

pub(crate) fn collect_scan_stats<M: MetaStore, V>(
    out: &mut Vec<(&'static str, u64, u64)>,
    table: &CachedScannableTable<M, V>,
) where
    V: Clone + Send + Sync + 'static,
{
    let (h, m) = table.take_window_stats();
    if h != 0 || m != 0 {
        out.push((table.table_id().as_str(), h, m));
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

    let mut locators: BTreeMap<Vec<u8>, (Vec<u8>, u64)> = BTreeMap::new();
    let mut out = Vec::with_capacity(blob_ops.len());
    let mut group: Vec<BlobWriteOp> = Vec::new();
    let mut group_len = 0usize;

    let flush_group = |group: &mut Vec<BlobWriteOp>,
                       group_len: &mut usize,
                       out: &mut Vec<BlobWriteOp>,
                       locators: &mut BTreeMap<Vec<u8>, (Vec<u8>, u64)>|
     -> Result<()> {
        if group.is_empty() {
            return Ok(());
        }
        if group.len() == 1 {
            let op = group.pop().expect("group has one item");
            locators.insert(op.key.clone(), (op.key.clone(), 0));
            out.push(op);
            *group_len = 0;
            return Ok(());
        }

        let first_key = group.first().expect("non-empty group").key.clone();
        let last_key = group.last().expect("non-empty group").key.clone();
        let mut physical_key = Vec::with_capacity(1 + first_key.len() + last_key.len());
        physical_key.push(b'c');
        physical_key.extend_from_slice(&first_key);
        physical_key.extend_from_slice(&last_key);

        let mut combined = Vec::with_capacity(*group_len);
        for op in group.drain(..) {
            let offset = u64::try_from(combined.len()).map_err(|_| {
                MonadChainDataError::Decode("coalesced block blob offset overflows u64")
            })?;
            locators.insert(op.key, (physical_key.clone(), offset));
            combined.extend_from_slice(&op.value);
        }
        out.push(BlobWriteOp {
            table: BLOCK_BLOB_TABLE,
            key: physical_key,
            value: Bytes::from(combined),
        });
        *group_len = 0;
        Ok(())
    };

    for op in std::mem::take(blob_ops) {
        if op.table != BLOCK_BLOB_TABLE {
            flush_group(&mut group, &mut group_len, &mut out, &mut locators)?;
            out.push(op);
            continue;
        }
        if !group.is_empty()
            && group_len.saturating_add(op.value.len()) > BLOCK_BLOB_COALESCE_TARGET_BYTES
        {
            flush_group(&mut group, &mut group_len, &mut out, &mut locators)?;
        }
        group_len = group_len.saturating_add(op.value.len());
        group.push(op);
    }
    flush_group(&mut group, &mut group_len, &mut out, &mut locators)?;

    for op in meta_ops {
        let MetaWriteOp::Put { table, key, value } = op else {
            continue;
        };
        if *table != BLOCK_METADATA_TABLE_ID {
            continue;
        }
        let Some((physical_key, physical_base_offset)) = locators.get(key) else {
            continue;
        };
        let mut metadata = BlockMetadataRecord::decode(value)?;
        rewrite_block_blob_header(
            &mut metadata.log_header,
            physical_key.clone(),
            *physical_base_offset,
        )?;
        rewrite_block_blob_header(
            &mut metadata.tx_header,
            physical_key.clone(),
            *physical_base_offset,
        )?;
        rewrite_block_blob_header(
            &mut metadata.trace_header,
            physical_key.clone(),
            *physical_base_offset,
        )?;
        *value = metadata.encode();
    }

    *blob_ops = out;
    Ok(())
}

fn rewrite_block_blob_header(
    header_bytes: &mut Bytes,
    physical_key: Vec<u8>,
    physical_base_offset: u64,
) -> Result<()> {
    let mut header = BlockBlobHeader::decode(header_bytes.as_ref())?;
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

    /// Builds a publication handle over `meta_store`. Exposed so a binary can
    /// construct the `Arc<PublicationTables>` the branchless ingest engine
    /// (`crate::ingest_controller`) requires, alongside the `Arc<Tables>`
    /// built from the same store — no `MonadChainDataService` needed for ingest.
    pub fn new(meta_store: M) -> Self {
        Self { meta_store }
    }

    /// Loads the current publication state. Readers that only want the head can
    /// use [`Self::load_published_head`].
    pub async fn load_state(&self) -> Result<Option<PublicationState>> {
        let Some(bytes) = self
            .meta_store
            .get(Self::PUBLICATION_STATE_TABLE, Self::PUBLICATION_STATE_KEY)
            .await?
        else {
            return Ok(None);
        };

        match PublicationState::decode(&bytes) {
            Ok(state) => Ok(Some(state)),
            Err(_) => {
                // One-shot migration for the pre-single-writer five-field row.
                // Preserve the live head/checksum and rewrite this key in the
                // new two-field format so future startups take the fast path.
                let state = PublicationState::decode_legacy(&bytes)?;
                self.store_state(state).await?;
                Ok(Some(state))
            }
        }
    }

    pub async fn load_published_head(&self) -> Result<Option<u64>> {
        Ok(self
            .load_state()
            .await?
            .map(|state| state.indexed_finalized_head))
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

/// The per-block metadata row. Holds only what the hot path needs: the
/// `block_record` (window/continuity/recovery via `load_record`) and the three
/// per-family blob headers (read by the family materializers). The bulky
/// `EvmBlockHeader` is stored separately (`evm_header` table) because it is read
/// only when serving a full block to a client (`load_block`), so bundling it
/// here made every `load_record` pay for ~500 B it never decodes.
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

/// Cache decoder for the `BlockTables` view of the block-metadata row: yields
/// the decoded `BlockRecord` (the only projection that view serves). Runs once
/// per miss inside the cache fetch, so window/continuity/recovery reads that
/// hit the cache skip both RLP decodes.
fn decode_block_record(bytes: Bytes) -> Result<BlockRecord> {
    let metadata = BlockMetadataRecord::decode(&bytes)?;
    BlockRecord::decode(&metadata.block_record)
}

/// Cache decoders for the per-family `FamilyTables` view: each yields the
/// fully decoded `BlockBlobHeader` for one family, shared as an `Arc` so a hit
/// clones a refcount rather than the offsets vector. The hot materializers read
/// this on every block they touch; caching it decoded removes a per-block RLP
/// decode from each query.
fn decode_log_header(bytes: Bytes) -> Result<Arc<BlockBlobHeader>> {
    let metadata = BlockMetadataRecord::decode(&bytes)?;
    Ok(Arc::new(BlockBlobHeader::decode(&metadata.log_header)?))
}

fn decode_tx_header(bytes: Bytes) -> Result<Arc<BlockBlobHeader>> {
    let metadata = BlockMetadataRecord::decode(&bytes)?;
    Ok(Arc::new(BlockBlobHeader::decode(&metadata.tx_header)?))
}

fn decode_trace_header(bytes: Bytes) -> Result<Arc<BlockBlobHeader>> {
    let metadata = BlockMetadataRecord::decode(&bytes)?;
    Ok(Arc::new(BlockBlobHeader::decode(&metadata.trace_header)?))
}

/// Cache decoder for the block-hash-to-number index: an 8-byte big-endian
/// block number.
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
            block_metadata: CachedKvTable::new_decoded(
                meta_store.table(Self::BLOCK_METADATA_TABLE),
                cache.block_header_entries,
                decode_block_record,
            ),
            // Cold path (only `load_block` reads it); sized off the same knob.
            evm_header: CachedKvTable::new(
                meta_store.table(Self::BLOCK_EVM_HEADER_TABLE),
                cache.block_header_entries,
            ),
            block_hash_to_number_index: CachedKvTable::new_decoded(
                meta_store.table(Self::BLOCK_HASH_TO_NUMBER_INDEX_TABLE),
                cache.block_hash_to_number_entries,
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

    /// Resolves a block hash to its block number via the hash-to-number index.
    /// A returned `Some(n)` is not by itself a guarantee that block `n` is
    /// fully published — the index entry is written before the publication
    /// head advances, so a hash hit may name a block whose record/header is
    /// not yet visible. Callers that turn the number into a record/header
    /// load should expect `MissingData` if `n > published_head` or if the
    /// follow-up loads fail.
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
        match current_head {
            None => {
                if block.block_number() != 1 {
                    return Err(crate::error::MonadChainDataError::InvalidRequest(
                        "first ingested block must be block 1 in the first pass",
                    ));
                }
                Ok(None)
            }
            Some(head) => {
                if block.block_number() != head + 1 {
                    return Err(crate::error::MonadChainDataError::InvalidRequest(
                        "block_number must extend the published head contiguously",
                    ));
                }

                let previous = self.load_record(head).await?.ok_or(
                    crate::error::MonadChainDataError::MissingData("missing previous block record"),
                )?;
                if previous.block_hash != block.parent_hash() {
                    return Err(crate::error::MonadChainDataError::InvalidRequest(
                        "parent_hash must match the previous published block",
                    ));
                }
                Ok(Some(previous))
            }
        }
    }
}

#[derive(Clone)]
pub struct FamilyTables<M: MetaStore> {
    family: Family,
    block_metadata: CachedKvTable<M, Arc<BlockBlobHeader>>,
    dict_by_version: CachedKvTable<M>,
    dir: PrimaryDirTables<M>,
    bitmap: BitmapTables<M>,
}

impl<M: MetaStore> FamilyTables<M> {
    fn new(meta_store: M, family: Family, cache: CacheConfig) -> Self {
        let ids = family.table_ids();
        let header_decoder: fn(Bytes) -> Result<Arc<BlockBlobHeader>> = match family {
            Family::Log => decode_log_header,
            Family::Tx => decode_tx_header,
            Family::Trace => decode_trace_header,
        };
        Self {
            family,
            block_metadata: CachedKvTable::new_decoded(
                meta_store.table(BlockTables::<M>::BLOCK_METADATA_TABLE),
                cache.block_header_entries,
                header_decoder,
            ),
            // The dict-by-version table is left uncached: the hot decode path
            // memoizes decoders in `DictManager` (unbounded), so this table is
            // read at most once per (family, version) per process — only by the
            // `ensure_epoch_dict` adoption probe and a cold `resolve_decoder`
            // miss. Caching it would also be the lone metadata cache still live
            // on the writer (every other metadata cache is disabled during
            // ingest), where the mint path's check-then-write would otherwise
            // strand a negative-cached miss now that writes never seed caches.
            dict_by_version: CachedKvTable::new(meta_store.table(ids.dict_by_version), 0),
            dir: PrimaryDirTables::new(
                CachedScannableTable::new_decoded(
                    meta_store.scannable_table(ids.dir_by_block),
                    cache.dir_by_block_entries,
                    crate::engine::primary_dir::decode_fragment,
                ),
                CachedKvTable::new_decoded(
                    meta_store.table(ids.dir_bucket),
                    cache.dir_bucket_entries,
                    crate::engine::primary_dir::decode_bucket,
                ),
            ),
            bitmap: BitmapTables::new(
                CachedScannableTable::new_decoded_weighted(
                    meta_store.scannable_table(ids.bitmap_by_block),
                    cache.bitmap_by_block_cache_bytes,
                    crate::engine::bitmap::decode_fragment_blob,
                    crate::engine::bitmap::weigh_fragment,
                ),
                CachedKvTable::new_decoded_weighted(
                    meta_store.table(ids.bitmap_page_blob),
                    cache.bitmap_page_blob_cache_bytes,
                    crate::engine::bitmap::decode_page,
                    crate::engine::bitmap::weigh_page,
                ),
                CachedKvTable::new_decoded(
                    meta_store.table(ids.bitmap_page_counts),
                    cache.bitmap_page_counts_entries,
                    crate::engine::bitmap::decode_page_counts,
                ),
                CachedScannableTable::new_decoded(
                    meta_store.scannable_table(ids.open_bitmap_stream),
                    cache.open_bitmap_stream_entries,
                    crate::engine::bitmap::decode_open_streams_chunk,
                ),
            ),
        }
    }

    pub fn dir(&self) -> &PrimaryDirTables<M> {
        &self.dir
    }

    pub fn bitmap(&self) -> &BitmapTables<M> {
        &self.bitmap
    }

    pub fn bitmap_by_block_table(&self) -> ScannableTableId {
        self.bitmap.fragments_table()
    }

    pub fn family(&self) -> Family {
        self.family
    }

    /// Loads the decoded per-block blob header for this family, served from the
    /// read cache (decoded once per miss). `None` means the block's metadata row
    /// is absent.
    pub async fn load_block_header(
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

    pub async fn load_bucket(&self, bucket_start: u64) -> Result<Option<PrimaryDirBucket>> {
        self.dir.load_bucket(bucket_start).await
    }

    pub async fn load_bitmap_fragments(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Vec<Arc<BitmapBlob>>> {
        self.bitmap
            .load_fragments(stream_id, page_start_local)
            .await
    }

    /// Loads a compacted bitmap page for one sealed stream page, decoded
    /// (metadata + roaring bitmap) and served from the read cache.
    pub async fn load_bitmap_page_artifact(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Option<Arc<DecodedBitmapPage>>> {
        self.bitmap
            .load_page_artifact(stream_id, page_start_local)
            .await
    }

    /// Loads the sealed-shard page-count manifest for one stream.
    pub async fn load_bitmap_page_counts(
        &self,
        stream_id: &str,
    ) -> Result<Option<crate::engine::bitmap::BitmapPageCounts>> {
        self.bitmap.load_page_counts(stream_id).await
    }

    /// Loads the open stream inventory for one frontier page.
    pub async fn load_open_bitmap_streams(&self, global_page_start: u64) -> Result<Vec<String>> {
        self.bitmap.load_open_streams(global_page_start).await
    }

    pub(crate) fn collect_window_stats(&self, out: &mut Vec<(&'static str, u64, u64)>) {
        collect_kv_stats(out, &self.dict_by_version);
        collect_scan_stats(out, self.dir.fragments_cache());
        collect_kv_stats(out, self.dir.buckets_cache());
        collect_scan_stats(out, self.bitmap.fragments_cache());
        collect_kv_stats(out, self.bitmap.page_blobs_cache());
        collect_kv_stats(out, self.bitmap.page_counts_cache());
        collect_scan_stats(out, self.bitmap.open_streams_cache());
    }
}
