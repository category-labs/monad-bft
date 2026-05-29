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
use zstd::dict::DecoderDictionary;

use crate::{
    engine::{
        bitmap::{BitmapFragmentWrite, BitmapPageArtifact, BitmapPageMeta, BitmapTables},
        family::Family,
        primary_dir::{PrimaryDirBucket, PrimaryDirFragment, PrimaryDirTables},
        row_codec::{decode_row_frame, RowCodec, RowCodecState, DICT_VERSION_NONE, ROW_ZSTD_LEVEL},
    },
    error::{MonadChainDataError, Result},
    family::{FinalizedBlock, Hash32},
    primitives::{
        state::{BlockRecord, PublicationState},
        EvmBlockHeader,
    },
    store::{
        BlobStore, BlobWriteOp, CacheConfig, CachedBlobTable, CachedKvTable, CachedScannableTable,
        CasOutcome, CasVersion, MetaStore, MetaStoreCas, MetaWriteOp, PublicationCasParams,
        ScannableTableId, SessionFuture, TableId, WriteSession,
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
    /// The dictionary version (== epoch number) for a given block number.
    pub fn version_for_block(&self, block_number: u64) -> u32 {
        (block_number / self.epoch_blocks) as u32
    }

    /// Clamped training sample range `[start, end)` for version `V >= 1`,
    /// drawn from the leading blocks of epoch `V - 1`.
    pub fn training_range(&self, version: u32) -> (u64, u64) {
        let prev_epoch = u64::from(version - 1);
        let start = prev_epoch * self.epoch_blocks;
        let span = self.sample_span.min(self.epoch_blocks);
        (start, start + span)
    }
}

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

    pub fn add_cas_count(&mut self, table: TableId, bytes: usize) {
        self.counts.add_cas(table, bytes);
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

    pub fn add_cas(&mut self, table: TableId, bytes: usize) {
        self.add(format!("cas:{}", table.as_str()), bytes);
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
    families: BTreeMap<Family, FamilyTables<M, B>>,
    dicts: DictManager,
}

impl<M: MetaStore, B: BlobStore> Tables<M, B> {
    pub fn new(meta_store: M, blob_store: B) -> Self {
        Self::with_cache_config(meta_store, blob_store, CacheConfig::default())
    }

    pub fn with_cache_config(meta_store: M, blob_store: B, cache: CacheConfig) -> Self {
        Self::with_configs(meta_store, blob_store, cache, DictConfig::default())
    }

    /// Full constructor letting callers (notably tests) thread a small
    /// [`DictConfig`] so the epoch-based dictionary lifecycle can be exercised
    /// over a handful of blocks.
    pub fn with_configs(
        meta_store: M,
        blob_store: B,
        cache: CacheConfig,
        dict_config: DictConfig,
    ) -> Self {
        let mut families = BTreeMap::new();
        families.insert(
            Family::Log,
            FamilyTables::new(meta_store.clone(), blob_store.clone(), Family::Log, cache),
        );
        families.insert(
            Family::Tx,
            FamilyTables::new(meta_store.clone(), blob_store.clone(), Family::Tx, cache),
        );
        families.insert(
            Family::Trace,
            FamilyTables::new(meta_store.clone(), blob_store.clone(), Family::Trace, cache),
        );
        Self {
            blocks: BlockTables::new(meta_store.clone(), cache),
            tx_hash_index: TxHashIndexTable::new(meta_store.clone(), cache),
            meta_store,
            blob_store,
            families,
            dicts: DictManager::new(dict_config),
        }
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

    /// Returns the table set for a family. Panics if the family was not
    /// registered at construction; the `Family` enum is closed, so missing
    /// a variant here is a programmer error, not a data error.
    pub fn family(&self, family: Family) -> &FamilyTables<M, B> {
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
            session.invalidate_populated();
            return (Err(e), MetaBlobTimings::default());
        }
        let meta_ops = session.take_meta();
        let blob_ops = session.take_blob();
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
            Err(e) => {
                session.invalidate_populated();
                (Err(e), MetaBlobTimings::default())
            }
        }
    }

    /// Stages write operations without applying them. Any cache entries
    /// populated while staging are evicted before returning because the writes
    /// may be applied later, retried, or abandoned by a pipelined caller.
    pub async fn stage_writes<'a, F>(&'a self, f: F) -> Result<StagedWrites>
    where
        F: for<'s> FnOnce(&'s mut WriteSession<'a, M, B>) -> SessionFuture<'s>,
    {
        let mut session = WriteSession::new(self);
        let closure_result = f(&mut session).await;
        if let Err(e) = closure_result {
            session.invalidate_populated();
            return Err(e);
        }
        let meta_ops = session.take_meta();
        let blob_ops = session.take_blob();
        session.invalidate_populated();
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

    /// Stages writes, flushes meta and blob artifacts, and only then attempts
    /// the publication CAS. A CAS conflict therefore means the staged
    /// artifacts may already be durable; callers must make those writes
    /// idempotent and retry-safe. On closure or flush error, populated cache
    /// entries are invalidated because the durable state may not match them.
    pub async fn with_writes_and_cas<'a, F>(
        &'a self,
        cas: PublicationCasParams,
        f: F,
    ) -> Result<CasOutcome>
    where
        M: MetaStoreCas,
        F: for<'s> FnOnce(&'s mut WriteSession<'a, M, B>) -> SessionFuture<'s>,
    {
        self.with_writes_and_cas_timed(cas, f)
            .await
            .map(|(outcome, _timings)| outcome)
    }

    pub async fn with_writes_and_cas_timed<'a, F>(
        &'a self,
        cas: PublicationCasParams,
        f: F,
    ) -> Result<(CasOutcome, MetaBlobTimings)>
    where
        M: MetaStoreCas,
        F: for<'s> FnOnce(&'s mut WriteSession<'a, M, B>) -> SessionFuture<'s>,
    {
        let mut session = WriteSession::new(self);
        let closure_result = f(&mut session).await;
        if let Err(e) = closure_result {
            session.invalidate_populated();
            return Err(e);
        }
        let meta_ops = session.take_meta();
        let blob_ops = session.take_blob();
        let mut writes = WriteOpCounts::from_writes(&meta_ops, &blob_ops);
        writes.add_cas(cas.table, cas.value.len());
        let meta_start = std::time::Instant::now();
        let blob_start = std::time::Instant::now();
        match futures::try_join!(
            async {
                self.meta_store.apply_writes(meta_ops).await?;
                Ok::<_, crate::error::MonadChainDataError>(meta_start.elapsed())
            },
            async {
                self.blob_store.apply_writes(blob_ops).await?;
                Ok::<_, crate::error::MonadChainDataError>(blob_start.elapsed())
            },
        ) {
            Ok((meta_elapsed, blob_elapsed)) => {
                let outcome = self
                    .meta_store
                    .cas_put(cas.table, &cas.key, cas.expected, cas.value)
                    .await?;
                return Ok((
                    outcome,
                    MetaBlobTimings {
                        meta: meta_elapsed,
                        blob: blob_elapsed,
                        writes,
                    },
                ));
            }
            Err(e) => {
                session.invalidate_populated();
                return Err(e);
            }
        }
    }

    /// Drains and returns the (hits, misses) counters for every cached
    /// wrapper, tagged with its logical table name. Each call resets the
    /// counters so consumers see per-window deltas.
    pub fn take_cache_window_stats(&self) -> Vec<(&'static str, u64, u64)> {
        let mut out = Vec::new();
        self.blocks.collect_window_stats(&mut out);
        self.tx_hash_index.collect_window_stats(&mut out);
        for fam in self.families.values() {
            fam.collect_window_stats(&mut out);
        }
        out
    }
}

/// Pushes one (name, hits, misses) tuple per cached wrapper, skipping rows
/// where both counters are zero so the per-window log line stays compact.
pub(crate) fn collect_kv_stats<M: MetaStore>(
    out: &mut Vec<(&'static str, u64, u64)>,
    table: &CachedKvTable<M>,
) {
    let (h, m) = table.take_window_stats();
    if h != 0 || m != 0 {
        out.push((table.table_id().as_str(), h, m));
    }
}

pub(crate) fn collect_scan_stats<M: MetaStore>(
    out: &mut Vec<(&'static str, u64, u64)>,
    table: &CachedScannableTable<M>,
) {
    let (h, m) = table.take_window_stats();
    if h != 0 || m != 0 {
        out.push((table.table_id().as_str(), h, m));
    }
}

pub(crate) fn collect_blob_stats<B: BlobStore>(
    out: &mut Vec<(&'static str, u64, u64)>,
    table: &CachedBlobTable<B>,
) {
    let (h, m) = table.take_window_stats();
    if h != 0 || m != 0 {
        out.push((table.table_id().as_str(), h, m));
    }
}

fn block_number_key(block_number: u64) -> [u8; 8] {
    block_number.to_be_bytes()
}

pub struct PublicationTables<M: MetaStoreCas> {
    meta_store: M,
}

impl<M: MetaStoreCas> PublicationTables<M> {
    pub const PUBLICATION_STATE_TABLE: TableId = TableId::new("publication_state");
    pub const PUBLICATION_STATE_KEY: &[u8] = b"state";

    pub(crate) fn new(meta_store: M) -> Self {
        Self { meta_store }
    }

    /// Loads the current publication state along with its CAS version.
    /// Writers must thread the version into the matching [`Self::cas_advance`]
    /// call; readers that only want the head can use [`Self::load_published_head`].
    pub async fn load_state(&self) -> Result<Option<(CasVersion, PublicationState)>> {
        let Some((version, bytes)) = self
            .meta_store
            .cas_get(Self::PUBLICATION_STATE_TABLE, Self::PUBLICATION_STATE_KEY)
            .await?
        else {
            return Ok(None);
        };

        Ok(Some((version, PublicationState::decode(&bytes)?)))
    }

    pub async fn load_published_head(&self) -> Result<Option<u64>> {
        Ok(self
            .load_state()
            .await?
            .map(|(_, state)| state.indexed_finalized_head))
    }

    /// Atomically advances the publication state. `expected` must be the
    /// version returned by the load that produced `next`'s preconditions —
    /// writer+standby failover is the case this guards. On version mismatch
    /// this returns `Err(FencedOut)` rather than [`CasOutcome::Conflict`]
    /// so callers can `?` through ingest paths.
    pub async fn cas_advance(
        &self,
        expected: Option<CasVersion>,
        next: PublicationState,
    ) -> Result<()> {
        let outcome = self
            .meta_store
            .cas_put(
                Self::PUBLICATION_STATE_TABLE,
                Self::PUBLICATION_STATE_KEY,
                expected,
                Bytes::from(next.encode()),
            )
            .await?;
        self.cas_outcome_into_result(outcome).await
    }

    /// Folds a [`CasOutcome`] from a publication-state write into the
    /// service-level `Result`. Centralized so the Phase B path in
    /// `MonadChainDataService::ingest_blocks` and the Phase-B-skipped path
    /// in [`Self::cas_advance`] map `Conflict` to `FencedOut` identically.
    pub(crate) async fn cas_outcome_into_result(&self, outcome: CasOutcome) -> Result<()> {
        match outcome {
            CasOutcome::Applied { .. } => Ok(()),
            CasOutcome::Conflict { .. } => {
                let current_head = self.load_published_head().await?;
                Err(MonadChainDataError::FencedOut { current_head })
            }
        }
    }
}

pub struct BlockTables<M: MetaStore> {
    block_metadata: CachedKvTable<M>,
    block_records: CachedKvTable<M>,
    block_headers: CachedKvTable<M>,
    block_hash_to_number_index: CachedKvTable<M>,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
struct BlockMetadataRecord {
    block_record: Bytes,
    evm_header: Bytes,
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

impl<M: MetaStore> BlockTables<M> {
    pub const BLOCK_METADATA_TABLE: TableId = TableId::new("block_metadata");
    pub const BLOCK_RECORD_TABLE: TableId = TableId::new("block_record");
    pub const BLOCK_HEADER_TABLE: TableId = TableId::new("block_header");
    pub const BLOCK_HASH_TO_NUMBER_INDEX_TABLE: TableId =
        TableId::new("block_hash_to_number_index");

    fn new(meta_store: M, cache: CacheConfig) -> Self {
        Self {
            block_metadata: CachedKvTable::new(
                meta_store.table(Self::BLOCK_METADATA_TABLE),
                cache.block_header_entries,
            ),
            block_records: CachedKvTable::new(
                meta_store.table(Self::BLOCK_RECORD_TABLE),
                cache.block_record_entries,
            ),
            block_headers: CachedKvTable::new(
                meta_store.table(Self::BLOCK_HEADER_TABLE),
                cache.block_header_entries,
            ),
            block_hash_to_number_index: CachedKvTable::new(
                meta_store.table(Self::BLOCK_HASH_TO_NUMBER_INDEX_TABLE),
                cache.block_hash_to_number_entries,
            ),
        }
    }

    async fn load_metadata(&self, block_number: u64) -> Result<Option<BlockMetadataRecord>> {
        let key = block_number_key(block_number);
        let Some(bytes) = self.block_metadata.get(&key).await? else {
            return Ok(None);
        };
        BlockMetadataRecord::decode(&bytes).map(Some)
    }

    pub async fn load_record(&self, block_number: u64) -> Result<Option<BlockRecord>> {
        if let Some(metadata) = self.load_metadata(block_number).await? {
            return BlockRecord::decode(&metadata.block_record).map(Some);
        }
        let key = block_number_key(block_number);
        let Some(bytes) = self.block_records.get(&key).await? else {
            return Ok(None);
        };
        Ok(Some(BlockRecord::decode(&bytes)?))
    }

    pub async fn store_record(&self, block_number: u64, block_record: &BlockRecord) -> Result<()> {
        let key = block_number_key(block_number);
        self.block_records
            .put(&key, Bytes::from(block_record.encode()))
            .await?;
        Ok(())
    }

    pub fn stage_record<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        block_number: u64,
        block_record: &BlockRecord,
    ) {
        let key = block_number_key(block_number);
        w.put(
            &self.block_records,
            &key,
            Bytes::from(block_record.encode()),
        );
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
            evm_header: Bytes::from(alloy_rlp::encode(evm_header)),
            log_header,
            tx_header,
            trace_header,
        };
        w.put(&self.block_metadata, &key, metadata.encode());
    }

    pub async fn load_header(&self, block_number: u64) -> Result<Option<EvmBlockHeader>> {
        if let Some(metadata) = self.load_metadata(block_number).await? {
            let header = EvmBlockHeader::decode(&mut metadata.evm_header.as_ref())
                .map_err(|_| MonadChainDataError::Decode("invalid block header rlp"))?;
            return Ok(Some(header));
        }
        let key = block_number_key(block_number);
        let Some(bytes) = self.block_headers.get(&key).await? else {
            return Ok(None);
        };
        let header = EvmBlockHeader::decode(&mut bytes.as_ref())
            .map_err(|_| MonadChainDataError::Decode("invalid block header rlp"))?;
        Ok(Some(header))
    }

    pub async fn store_header(&self, block_number: u64, header: &EvmBlockHeader) -> Result<()> {
        let key = block_number_key(block_number);
        self.block_headers
            .put(&key, Bytes::from(alloy_rlp::encode(header)))
            .await?;
        Ok(())
    }

    pub fn stage_header<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        block_number: u64,
        header: &EvmBlockHeader,
    ) {
        let key = block_number_key(block_number);
        w.put(
            &self.block_headers,
            &key,
            Bytes::from(alloy_rlp::encode(header)),
        );
    }

    /// Resolves a block hash to its block number via the hash-to-number index.
    /// A returned `Some(n)` is not by itself a guarantee that block `n` is
    /// fully published — the index entry is written before the publication
    /// head advances, so a hash hit may name a block whose record/header is
    /// not yet visible. Callers that turn the number into a record/header
    /// load should expect `MissingData` if `n > published_head` or if the
    /// follow-up loads fail.
    pub async fn block_number_by_hash(&self, block_hash: &Hash32) -> Result<Option<u64>> {
        let Some(value) = self
            .block_hash_to_number_index
            .get(block_hash.as_slice())
            .await?
        else {
            return Ok(None);
        };
        let bytes: [u8; 8] = value
            .as_ref()
            .try_into()
            .map_err(|_| MonadChainDataError::Decode("invalid block_hash_to_number_index value"))?;
        Ok(Some(u64::from_be_bytes(bytes)))
    }

    pub async fn store_hash_index(&self, block_hash: &Hash32, block_number: u64) -> Result<()> {
        self.block_hash_to_number_index
            .put(
                block_hash.as_slice(),
                Bytes::copy_from_slice(&block_number.to_be_bytes()),
            )
            .await?;
        Ok(())
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
        collect_kv_stats(out, &self.block_records);
        collect_kv_stats(out, &self.block_headers);
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

pub struct FamilyTables<M: MetaStore, B: BlobStore> {
    family: Family,
    block_metadata: CachedKvTable<M>,
    block_headers: CachedKvTable<M>,
    block_blobs: CachedBlobTable<B>,
    dict_by_version: CachedKvTable<M>,
    dir: PrimaryDirTables<M>,
    bitmap: BitmapTables<M>,
}

impl<M: MetaStore, B: BlobStore> FamilyTables<M, B> {
    fn new(meta_store: M, blob_store: B, family: Family, cache: CacheConfig) -> Self {
        let ids = family.table_ids();
        Self {
            family,
            block_metadata: CachedKvTable::new(
                meta_store.table(BlockTables::<M>::BLOCK_METADATA_TABLE),
                cache.block_header_entries,
            ),
            block_headers: CachedKvTable::new(
                meta_store.table(ids.block_header),
                cache.block_header_entries,
            ),
            block_blobs: CachedBlobTable::new(
                blob_store.table(ids.block_blob),
                cache.block_blob_entries,
            ),
            // Dictionaries are tiny and rarely minted; a small cache suffices.
            dict_by_version: CachedKvTable::new(meta_store.table(ids.dict_by_version), 16),
            dir: PrimaryDirTables::new(
                CachedScannableTable::new(
                    meta_store.scannable_table(ids.dir_by_block),
                    cache.dir_by_block_entries,
                ),
                CachedKvTable::new(meta_store.table(ids.dir_bucket), cache.dir_bucket_entries),
            ),
            bitmap: BitmapTables::new(
                CachedScannableTable::new(
                    meta_store.scannable_table(ids.bitmap_by_block),
                    cache.bitmap_by_block_entries,
                ),
                CachedKvTable::new(
                    meta_store.table(ids.bitmap_page_meta),
                    cache.bitmap_page_meta_entries,
                ),
                CachedKvTable::new(
                    meta_store.table(ids.bitmap_page_blob),
                    cache.bitmap_page_blob_entries,
                ),
                CachedKvTable::new(
                    meta_store.table(ids.bitmap_page_counts),
                    cache.bitmap_page_counts_entries,
                ),
                CachedScannableTable::new(
                    meta_store.scannable_table(ids.open_bitmap_stream),
                    cache.open_bitmap_stream_entries,
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

    /// Loads the raw per-block header bytes for this family. The codec is
    /// the consumer's responsibility; the engine treats the value as opaque.
    pub async fn load_block_header(&self, block_number: u64) -> Result<Option<Bytes>> {
        let key = block_number_key(block_number);
        if let Some(bytes) = self.block_metadata.get(&key).await? {
            let metadata = BlockMetadataRecord::decode(&bytes)?;
            return Ok(Some(match self.family {
                Family::Log => metadata.log_header,
                Family::Tx => metadata.tx_header,
                Family::Trace => metadata.trace_header,
            }));
        }
        let key = block_number_key(block_number);
        self.block_headers.get(&key).await
    }

    pub async fn store_block_header(&self, block_number: u64, bytes: Bytes) -> Result<()> {
        let key = block_number_key(block_number);
        self.block_headers.put(&key, bytes).await?;
        Ok(())
    }

    pub async fn load_block_blob(
        &self,
        block_number: u64,
    ) -> Result<Option<alloy_primitives::Bytes>> {
        let key = block_number_key(block_number);
        Ok(self.block_blobs.get(&key).await?.map(Into::into))
    }

    pub async fn read_block_blob_range(
        &self,
        block_number: u64,
        start: usize,
        end_exclusive: usize,
    ) -> Result<Option<bytes::Bytes>> {
        let key = block_number_key(block_number);
        self.block_blobs
            .read_range(&key, start, end_exclusive)
            .await
    }

    pub async fn store_block_blob(&self, block_number: u64, block_log_blob: Vec<u8>) -> Result<()> {
        let key = block_number_key(block_number);
        self.block_blobs
            .put(&key, Bytes::from(block_log_blob))
            .await?;
        Ok(())
    }

    pub fn stage_block_blob(
        &self,
        w: &mut WriteSession<'_, M, B>,
        block_number: u64,
        block_log_blob: Vec<u8>,
    ) {
        let key = block_number_key(block_number);
        w.put_blob(&self.block_blobs, &key, Bytes::from(block_log_blob));
    }

    pub fn stage_block_header(
        &self,
        w: &mut WriteSession<'_, M, B>,
        block_number: u64,
        bytes: Bytes,
    ) {
        let key = block_number_key(block_number);
        w.put(&self.block_headers, &key, bytes);
    }

    /// Loads the raw bytes of the row-codec dictionary for `version`, or
    /// `None` if no such version has been persisted.
    pub async fn load_dict(&self, version: u32) -> Result<Option<Bytes>> {
        self.dict_by_version.get(&version.to_be_bytes()).await
    }

    /// Stages a row-codec dictionary version into the durable write path.
    pub fn stage_dict(&self, w: &mut WriteSession<'_, M, B>, version: u32, dict_bytes: Bytes) {
        w.put(&self.dict_by_version, &version.to_be_bytes(), dict_bytes);
    }

    pub async fn load_bucket_fragments(
        &self,
        bucket_start: u64,
    ) -> Result<Vec<PrimaryDirFragment>> {
        self.dir.load_bucket_fragments(bucket_start).await
    }

    pub(crate) async fn list_bucket_fragments_for_rebuild(
        &self,
        bucket_start: u64,
        published_head: u64,
    ) -> Result<BTreeMap<u64, Bytes>> {
        self.dir
            .list_bucket_fragments_for_rebuild(bucket_start, published_head)
            .await
    }

    pub async fn load_bucket(&self, bucket_start: u64) -> Result<Option<PrimaryDirBucket>> {
        self.dir.load_bucket(bucket_start).await
    }

    pub async fn store_bitmap_fragment(
        &self,
        fragment: &BitmapFragmentWrite,
        block_number: u64,
    ) -> Result<()> {
        self.bitmap.store_fragment(fragment, block_number).await
    }

    pub async fn load_bitmap_fragments(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Vec<Bytes>> {
        self.bitmap
            .load_fragments(stream_id, page_start_local)
            .await
    }

    pub(crate) async fn list_bitmap_fragments_for_rebuild(
        &self,
        stream_id: &str,
        page_start_local: u32,
        published_head: u64,
    ) -> Result<BTreeMap<u64, Bytes>> {
        self.bitmap
            .list_fragments_for_rebuild(stream_id, page_start_local, published_head)
            .await
    }

    /// Loads the compacted page metadata for one sealed stream page.
    pub async fn load_bitmap_page_meta(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Option<BitmapPageMeta>> {
        self.bitmap
            .load_page_meta(stream_id, page_start_local)
            .await
    }

    pub async fn store_bitmap_page_meta(
        &self,
        stream_id: &str,
        page_start_local: u32,
        meta: &BitmapPageMeta,
    ) -> Result<()> {
        self.bitmap
            .store_page_meta(stream_id, page_start_local, meta)
            .await
    }

    /// Loads a compacted bitmap page artifact for one sealed stream page.
    pub async fn load_bitmap_page_artifact(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Option<BitmapPageArtifact>> {
        self.bitmap
            .load_page_artifact(stream_id, page_start_local)
            .await
    }

    pub async fn store_bitmap_page_artifact(
        &self,
        stream_id: &str,
        page_start_local: u32,
        artifact: &BitmapPageArtifact,
    ) -> Result<()> {
        self.bitmap
            .store_page_artifact(stream_id, page_start_local, artifact)
            .await
    }

    /// Loads the compacted bitmap blob for one sealed stream page.
    pub async fn load_bitmap_page_blob(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Option<Bytes>> {
        self.bitmap
            .load_page_blob(stream_id, page_start_local)
            .await
    }

    pub async fn store_bitmap_page_blob(
        &self,
        stream_id: &str,
        page_start_local: u32,
        bitmap_blob: Bytes,
    ) -> Result<()> {
        self.bitmap
            .store_page_blob(stream_id, page_start_local, bitmap_blob)
            .await
    }

    /// Loads the sealed-shard page-count manifest for one stream.
    pub async fn load_bitmap_page_counts(
        &self,
        stream_id: &str,
    ) -> Result<Option<crate::engine::bitmap::BitmapPageCounts>> {
        self.bitmap.load_page_counts(stream_id).await
    }

    pub async fn store_bitmap_page_counts(
        &self,
        stream_id: &str,
        counts: &crate::engine::bitmap::BitmapPageCounts,
    ) -> Result<()> {
        self.bitmap.store_page_counts(stream_id, counts).await
    }

    /// Loads the open stream inventory for one frontier page.
    pub async fn load_open_bitmap_streams(&self, global_page_start: u64) -> Result<Vec<String>> {
        self.bitmap.load_open_streams(global_page_start).await
    }

    /// Records any newly touched streams in the open inventory for one page.
    ///
    /// This is intentionally append-only in the current slice so replay can
    /// never lose open-stream membership through a partial delete+rewrite.
    pub async fn record_open_bitmap_streams(
        &self,
        global_page_start: u64,
        streams: &std::collections::BTreeSet<String>,
    ) -> Result<()> {
        self.bitmap
            .record_open_streams(global_page_start, streams)
            .await
    }

    pub(crate) fn collect_window_stats(&self, out: &mut Vec<(&'static str, u64, u64)>) {
        collect_kv_stats(out, &self.block_headers);
        collect_kv_stats(out, &self.dict_by_version);
        collect_blob_stats(out, &self.block_blobs);
        collect_scan_stats(out, self.dir.fragments_cache());
        collect_kv_stats(out, self.dir.buckets_cache());
        collect_scan_stats(out, self.bitmap.fragments_cache());
        collect_kv_stats(out, self.bitmap.page_meta_cache());
        collect_kv_stats(out, self.bitmap.page_blobs_cache());
        collect_kv_stats(out, self.bitmap.page_counts_cache());
        collect_scan_stats(out, self.bitmap.open_streams_cache());
    }
}
