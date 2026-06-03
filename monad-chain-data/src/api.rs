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
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::lock::Mutex;
use rayon::prelude::*;

use crate::{
    blocks::{
        execute_query_blocks, load_blocks_by_numbers, QueryBlocksRequest, QueryBlocksResponse,
    },
    engine::{
        authority::{
            LeaseAuthority, ReadOnlyAuthority, WriteAuthority, WriteContinuity, WriteSession,
        },
        bitmap::{global_page_start, local_page_start, stream_page_global_start},
        digest::{self, ArtifactChecksum, EMPTY_CHECKSUM},
        family::Family,
        ingest::persist::PhaseAFragmentWriteFilter,
        open_index::{
            insert_bitmap_set, insert_directory_set, OpenIndexStats, OpenIndexes, OpenIndexesDelta,
            OpenIndexesEviction,
        },
        primary_dir::{bucket_start, fragment_bucket_starts, PrimaryDirFragment},
        query::family_runner::IndexedFamilyQuery,
        row_codec::{should_sample_row, RowCodec, DICT_VERSION_NONE},
        tables::{DictConfig, PublicationTables, StagedWrites, Tables, WriteOpCounts},
    },
    error::{MonadChainDataError, Result},
    family::{FinalizedBlock, Hash32},
    logs::{
        execute_block_scan_query, execute_indexed_log_query, LogIngestPlan, QueryLogsRequest,
        QueryLogsResponse,
    },
    primitives::{
        limits::QueryLimits,
        range::ResolvedBlockWindow,
        state::{BlockBlobHeader, BlockRecord, LogId, TraceId, TxId},
    },
    store::{BlobStore, CacheConfig, MetaStoreCas},
    traces::{
        execute_block_scan_trace_query, execute_indexed_trace_query, QueryTracesRequest,
        QueryTracesResponse, TraceIngestPlan,
    },
    transfers::{
        execute_block_scan_transfer_query, execute_indexed_transfer_query, QueryTransfersRequest,
        QueryTransfersResponse,
    },
    txs::{
        execute_block_scan_tx_query, execute_indexed_tx_query, load_txs_by_positions,
        QueryTransactionsRequest, QueryTransactionsResponse, TxEntry, TxIngestPlan, TxMaterializer,
    },
};

/// Fixed upper bound on the RLP-encoded width of a `PublicationState` row
/// (a 4-field list: three `u64`s, each at most 9 bytes, plus a 16-byte
/// `session_id` blob at 17 bytes, plus a 1-byte short-list header). Used only
/// for write-volume accounting; the exact bytes are formed at publish time.
const PUBLICATION_STATE_ENCODED_BYTES: usize = 1 + 9 * 3 + 17;

/// Callback yielding the latest observed upstream finalized block number — the
/// lease clock. Returns `None` when the upstream head is unknown, which forces
/// the authority to fail closed (no acquire / renew / publish). Reader-only
/// services default to `|| None`.
pub type ObserveUpstream = Arc<dyn Fn() -> Option<u64> + Send + Sync>;

/// The write-authority a service runs under. Kept as an enum (rather than a
/// third generic on [`MonadChainDataService`]) so existing
/// `MonadChainDataService<M, B>` call sites — fixtures, tests, the bin — keep
/// compiling unchanged.
///
/// - `ReadOnly` — never takes ownership; `new_reader_only`.
/// - `SingleWriter` — the historical "exactly one process writes" path used by
///   `new` / `with_cache_config`: a plain version-CAS on the published head
///   (no ownership, no lease clock, no acquire-time row write). This keeps the
///   pre-lease behavior every fixture/test encodes, and crucially does *not*
///   conflate single-writer with the lease machinery — there is no shared
///   default identity to silently collide on.
/// - `Lease` — multi-node leader/standby coordination; the explicit
///   `new_reader_writer*` constructors.
enum ServiceAuthority<M: MetaStoreCas> {
    ReadOnly(ReadOnlyAuthority),
    SingleWriter,
    Lease(LeaseAuthority<M>),
}

pub struct MonadChainDataService<M: MetaStoreCas, B: BlobStore> {
    tables: Tables<M, B>,
    publication: PublicationTables<M>,
    limits: QueryLimits,
    open_indexes: OpenIndexes,
    planning_state: Mutex<Option<IngestPlanningState>>,
    authority: ServiceAuthority<M>,
    observe_upstream: ObserveUpstream,
    /// Test-only counter of `ensure_open_indexes_rebuilt` invocations, used by
    /// the recovery-gating test to prove `Reacquired` rebuilds and `Continuous`
    /// skips. Not present in non-test builds.
    #[cfg(test)]
    recovery_runs: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Debug, Clone)]
struct IngestPlanningState {
    head: Option<u64>,
    previous: Option<BlockRecord>,
}

struct StagedBlock {
    log_plan: LogIngestPlan,
    tx_plan: TxIngestPlan,
    trace_plan: TraceIngestPlan,
    block_record: BlockRecord,
}

#[derive(Clone, Copy)]
struct StageAPlanStarts {
    log: LogId,
    tx: TxId,
    trace: TraceId,
}

struct StageAPlanOutput {
    staged: StagedBlock,
    /// Standalone (un-chained) content digest for this block; chained into the
    /// running checksum in the sequential staging loop.
    block_content_digest: ArtifactChecksum,
}

/// Computes the standalone content digest for one planned block: the logical
/// (uncompressed, zstd-independent) artifacts the block would write. Folds the
/// three per-family content digests with the block identity + EVM header. See
/// [`crate::engine::digest`]. The running chain value is layered on top in the
/// sequential staging loop.
fn block_content_digest(
    block: &FinalizedBlock,
    log_plan: &LogIngestPlan,
    tx_plan: &TxIngestPlan,
    trace_plan: &TraceIngestPlan,
) -> ArtifactChecksum {
    let log_fc = digest::family_content_digest(
        log_plan.log_window,
        log_plan.block_log_header.dict_version,
        log_plan.rows_digest,
        &log_plan.bitmap_fragments,
    );
    let tx_fc = digest::family_content_digest(
        tx_plan.tx_window,
        tx_plan.block_tx_header.dict_version,
        tx_plan.rows_digest,
        &tx_plan.bitmap_fragments,
    );
    let trace_fc = digest::family_content_digest(
        trace_plan.trace_window,
        trace_plan.block_trace_header.dict_version,
        trace_plan.rows_digest,
        &trace_plan.bitmap_fragments,
    );
    digest::block_content_digest(
        block.block_number(),
        &block.block_hash(),
        &block.parent_hash(),
        &alloy_rlp::encode(&block.header),
        log_fc,
        tx_fc,
        trace_fc,
    )
}

fn family_ranges<F>(staged: &[StagedBlock], pick: F) -> Vec<(u64, u64)>
where
    F: Fn(&StagedBlock) -> crate::primitives::state::FamilyWindowRecord,
{
    staged
        .iter()
        .map(|s| {
            let w = pick(s);
            let from = w.first_primary_id.as_u64();
            let to = w
                .next_primary_id_exclusive()
                .map(|p| p.as_u64())
                .unwrap_or(from);
            (from, to)
        })
        .collect()
}

fn append_family_phase_a_delta(
    delta: &mut OpenIndexesDelta,
    family: Family,
    block_number: u64,
    window: crate::primitives::state::FamilyWindowRecord,
    bitmap_fragments: &[crate::engine::bitmap::BitmapFragmentWrite],
) -> Result<()> {
    let first_primary_id = window.first_primary_id.as_u64();
    // Empty blocks mint no ids, so a fragment for them resolves nothing and is
    // pure write/memory amplification. Skip them: the compacted bucket's sentinel
    // (the next id-producing block's first id, or the bucket's final frontier)
    // already closes the id range across any empty blocks in between.
    if window.count > 0 {
        for bucket in fragment_bucket_starts(first_primary_id, window.count) {
            let fragment = PrimaryDirFragment {
                block_number,
                first_primary_id,
                end_primary_id_exclusive: first_primary_id.saturating_add(u64::from(window.count)),
            };
            delta.directory_fragments.push((
                family,
                bucket,
                block_number,
                Bytes::from(fragment.encode()),
            ));
        }
    }
    for fragment in bitmap_fragments {
        let page_global_start =
            stream_page_global_start(&fragment.stream_id, fragment.page_start_local)?;
        delta.bitmap_fragments.push((
            family,
            fragment.stream_id.clone(),
            fragment.page_start_local,
            block_number,
            fragment.bitmap_blob.clone(),
        ));
        delta
            .bitmap_open_streams
            .push((family, page_global_start, fragment.stream_id.clone()));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestOutcome {
    pub indexed_finalized_head: u64,
    pub block_record: BlockRecord,
    pub written_logs: usize,
    pub written_txs: usize,
    pub written_traces: usize,
}

/// Result of [`MonadChainDataService::verify_artifact_checksums`]: whether a
/// standby's re-derived artifact-checksum chain matches what the primary
/// published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyOutcome {
    /// Every supplied block's re-derived running checksum matched the published
    /// [`BlockRecord`]. `through_block` is the highest verified block number
    /// (the prior published head when no blocks were supplied).
    Match { through_block: u64 },
    /// The first block whose published record was not found, so verification
    /// cannot proceed past it — typically the standby is ahead of the primary's
    /// published head. Blocks below `block_number` did match.
    Pending { block_number: u64 },
    /// The first block whose re-derived running checksum disagreed with the
    /// published record. Blocks below `block_number` matched.
    Mismatch {
        block_number: u64,
        /// The published (primary's) chained checksum for the block.
        published: ArtifactChecksum,
        /// The locally re-derived chained checksum.
        computed: ArtifactChecksum,
    },
}

/// Per-batch timing breakdown emitted by [`MonadChainDataService::ingest_blocks`].
/// All durations are milliseconds. Data writes are applied before the
/// publication CAS; `commit_b_ms` is retained as the post-write/CAS overhead
/// bucket for existing ingest logging.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestBatchTimings {
    pub stage_a_ms: u64,
    pub phase_a_write_counts: WriteOpCounts,
    pub phase_b_write_counts: WriteOpCounts,
    pub commit_a_meta_ms: u64,
    pub commit_a_blob_ms: u64,
    pub reads_ms: u64,
    pub stage_b_ms: u64,
    pub commit_b_ms: u64,
    pub cas_ms: u64,
    pub phase_b_skipped: bool,
    pub blocks: usize,
}

#[derive(Debug, Clone)]
pub struct IngestPlan {
    pub outcomes: Vec<IngestOutcome>,
    pub timings: IngestBatchTimings,
    pub writes: StagedWrites,
    pub publication: PublicationAdvance,
}

/// The head a successfully-applied plan should publish. Under the
/// write-authority lease model the *bytes* of the published
/// [`crate::primitives::state::PublicationState`] row can only be formed at
/// publish time (they carry the owner/session/lease fields the authority
/// stamps inside the CAS), so the plan carries just the target head, its
/// chained artifact checksum, plus the CAS table/key it will be written under.
/// The authority builds the row in `apply_ingest_plan_with_retry`.
#[derive(Debug, Clone)]
pub struct PublicationAdvance {
    pub table: crate::store::TableId,
    pub key: Vec<u8>,
    pub new_head: u64,
    /// Chained artifact checksum at `new_head`, equal to the head block's
    /// [`BlockRecord::artifact_checksum`](crate::primitives::state::BlockRecord::artifact_checksum).
    pub head_artifact_checksum: ArtifactChecksum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoRetryPolicy {
    pub max_retries: Option<usize>,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl IoRetryPolicy {
    pub const fn no_retries() -> Self {
        Self {
            max_retries: Some(0),
            initial_backoff: Duration::from_millis(0),
            max_backoff: Duration::from_millis(0),
        }
    }

    pub const fn bounded(
        max_retries: usize,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Self {
        Self {
            max_retries: Some(max_retries),
            initial_backoff,
            max_backoff,
        }
    }

    pub const fn infinite(initial_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            max_retries: None,
            initial_backoff,
            max_backoff,
        }
    }
}

async fn retry_sleep(backoff: Duration) {
    futures_timer::Delay::new(backoff).await;
}

/// Number of blocks sampled from an epoch's leading range to build its
/// training corpus. Together with `PER_BLOCK_SAMPLE_CAP` this bounds the
/// corpus (`TARGET_SAMPLE_BLOCKS * PER_BLOCK_SAMPLE_CAP` rows at most), so a
/// dense epoch can't blow up memory during an off-thread training run.
const TARGET_SAMPLE_BLOCKS: u64 = 256;

impl<M: MetaStoreCas, B: BlobStore> MonadChainDataService<M, B> {
    pub fn new(meta_store: M, blob_store: B, limits: QueryLimits) -> Self {
        Self::with_cache_config(meta_store, blob_store, limits, CacheConfig::default())
    }

    /// Parallel constructor that lets the binary thread an operator-tuned
    /// `CacheConfig` through. Existing `new` keeps its three-arg shape so
    /// fixture call sites don't churn.
    ///
    /// C4 choice: `new` / `with_cache_config` stay **single-writer** — the
    /// pre-lease publish path ([`ServiceAuthority::SingleWriter`]): a plain
    /// version-CAS on the published head with no ownership, no lease clock, and
    /// no acquire-time row write. This preserves the historic "exactly one
    /// process writes" assumption every fixture/test encodes (they ingest
    /// through `new`) without conflating it with the lease machinery — there is
    /// no shared default identity for two such processes to silently clobber
    /// each other through; concurrent writers fence via the version-CAS exactly
    /// as they did before this layer existed. Multi-node coordination uses the
    /// explicit [`Self::new_reader_writer`] / [`Self::new_reader_only`]
    /// constructors, which wire a real upstream clock, owner identity, and role.
    pub fn with_cache_config(
        meta_store: M,
        blob_store: B,
        limits: QueryLimits,
        cache: CacheConfig,
    ) -> Self {
        Self::with_configs(meta_store, blob_store, limits, cache, DictConfig::default())
    }

    /// Full constructor that also threads a [`DictConfig`], letting tests run
    /// the epoch-based dictionary lifecycle over a handful of blocks (e.g.
    /// `epoch_blocks = 8`).
    pub fn with_configs(
        meta_store: M,
        blob_store: B,
        limits: QueryLimits,
        cache: CacheConfig,
        dict_config: DictConfig,
    ) -> Self {
        Self {
            publication: PublicationTables::new(meta_store.clone()),
            tables: Tables::with_configs(meta_store, blob_store, cache, dict_config),
            limits,
            open_indexes: OpenIndexes::default(),
            planning_state: Mutex::new(None),
            authority: ServiceAuthority::SingleWriter,
            observe_upstream: Arc::new(|| None),
            #[cfg(test)]
            recovery_runs: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Constructs a reader-only service. Queries read `load_published_head`
    /// observationally; the service never takes ownership and `observe_upstream`
    /// is `|| None`. Any ingest path errors with [`ReadOnlyMode`].
    ///
    /// [`ReadOnlyMode`]: MonadChainDataError::ReadOnlyMode
    pub fn new_reader_only(
        meta_store: M,
        blob_store: B,
        limits: QueryLimits,
        cache: CacheConfig,
    ) -> Self {
        Self {
            publication: PublicationTables::new(meta_store.clone()),
            tables: Tables::with_cache_config(meta_store, blob_store, cache),
            limits,
            open_indexes: OpenIndexes::default(),
            planning_state: Mutex::new(None),
            authority: ServiceAuthority::ReadOnly(ReadOnlyAuthority),
            observe_upstream: Arc::new(|| None),
            #[cfg(test)]
            recovery_runs: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Constructs a reader+writer service backed by a [`LeaseAuthority`].
    ///
    /// `owner_id` is the stable node identity (a restart reuses it but
    /// generates a fresh per-process `session_id`, so a restarted leader always
    /// takes the reacquire path and runs recovery). `lease_blocks` /
    /// `renew_threshold` are in upstream finalized blocks
    /// (`renew_threshold < lease_blocks`, `lease_blocks > 0` — asserted by the
    /// authority). `observe_upstream` yields the lease clock; returning `None`
    /// fails closed.
    pub fn new_reader_writer(
        meta_store: M,
        blob_store: B,
        limits: QueryLimits,
        owner_id: u64,
        lease_blocks: u64,
        renew_threshold: u64,
        observe_upstream: ObserveUpstream,
    ) -> Self {
        Self::new_reader_writer_with_cache_config(
            meta_store,
            blob_store,
            limits,
            CacheConfig::default(),
            owner_id,
            lease_blocks,
            renew_threshold,
            observe_upstream,
        )
    }

    /// [`Self::new_reader_writer`] with an explicit `CacheConfig`.
    #[allow(clippy::too_many_arguments)]
    pub fn new_reader_writer_with_cache_config(
        meta_store: M,
        blob_store: B,
        limits: QueryLimits,
        cache: CacheConfig,
        owner_id: u64,
        lease_blocks: u64,
        renew_threshold: u64,
        observe_upstream: ObserveUpstream,
    ) -> Self {
        let authority = LeaseAuthority::new(
            PublicationTables::new(meta_store.clone()),
            owner_id,
            lease_blocks,
            renew_threshold,
        );
        Self {
            publication: PublicationTables::new(meta_store.clone()),
            tables: Tables::with_cache_config(meta_store, blob_store, cache),
            limits,
            open_indexes: OpenIndexes::default(),
            planning_state: Mutex::new(None),
            authority: ServiceAuthority::Lease(authority),
            observe_upstream,
            #[cfg(test)]
            recovery_runs: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub fn tables(&self) -> &Tables<M, B> {
        &self.tables
    }

    pub fn publication(&self) -> &PublicationTables<M> {
        &self.publication
    }

    pub fn limits(&self) -> &QueryLimits {
        &self.limits
    }

    pub fn open_index_stats(&self) -> OpenIndexStats {
        self.open_indexes.stats()
    }

    /// The configured upstream-clock observation.
    fn observe_upstream(&self) -> Option<u64> {
        (self.observe_upstream)()
    }

    /// Begins a write-scoped authority session and returns its
    /// [`AuthorityState`] (head + continuity). The session is dropped here: per
    /// D1 option A we do not hold it across plan→apply; the cached lease lives
    /// on the authority and is revalidated inside the publish CAS. A
    /// reader-only service errors with [`ReadOnlyMode`].
    ///
    /// [`AuthorityState`]: crate::engine::authority::AuthorityState
    /// [`ReadOnlyMode`]: MonadChainDataError::ReadOnlyMode
    async fn authority_begin_write(
        &self,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<crate::engine::authority::AuthorityState> {
        match &self.authority {
            ServiceAuthority::ReadOnly(a) => {
                let session = a.begin_write(observed_upstream_finalized_block).await?;
                Ok(session.state())
            }
            // Single-writer has no lease/session: there is nothing to acquire.
            // It reports the current published head and a continuity that always
            // requires recovery on a cold cache (Fresh on an empty store, else
            // Reacquired), matching the pre-lease behavior of unconditionally
            // rebuilding open indexes from the published head on the first plan.
            ServiceAuthority::SingleWriter => {
                let head = self.publication.load_published_head().await?;
                let continuity = match head {
                    None => WriteContinuity::Fresh,
                    Some(_) => WriteContinuity::Reacquired,
                };
                Ok(crate::engine::authority::AuthorityState {
                    indexed_finalized_head: head.unwrap_or(0),
                    continuity,
                })
            }
            ServiceAuthority::Lease(a) => {
                let session = a.begin_write(observed_upstream_finalized_block).await?;
                Ok(session.state())
            }
        }
    }

    /// Publishes the new head through the active authority lease, revalidating
    /// and renewing inside the CAS (D1 option A). A reader-only service errors
    /// with [`ReadOnlyMode`].
    ///
    /// [`ReadOnlyMode`]: MonadChainDataError::ReadOnlyMode
    async fn authority_publish(
        &self,
        new_head: u64,
        head_artifact_checksum: ArtifactChecksum,
        observed_upstream_finalized_block: Option<u64>,
    ) -> Result<()> {
        match &self.authority {
            ServiceAuthority::ReadOnly(_) => Err(MonadChainDataError::ReadOnlyMode(
                "reader-only service cannot publish a new head",
            )),
            // Single-writer: the historical plain version-CAS publish. Load the
            // current row version, CAS the head forward against it; a stale
            // version (a concurrent writer moved the head) maps to `FencedOut`
            // via `cas_advance`. The lease fields are written as zeros — an
            // owner-less row — so the published bytes stay deterministic.
            ServiceAuthority::SingleWriter => {
                let expected = self
                    .publication
                    .load_state()
                    .await?
                    .map(|(version, _)| version);
                let next = crate::primitives::state::PublicationState {
                    indexed_finalized_head: new_head,
                    owner_id: 0,
                    session_id: [0u8; 16],
                    lease_valid_through_block: 0,
                    head_artifact_checksum,
                };
                self.publication.cas_advance(expected, next).await
            }
            ServiceAuthority::Lease(a) => {
                let result = a
                    .publish_current(
                        new_head,
                        head_artifact_checksum,
                        observed_upstream_finalized_block,
                    )
                    .await;
                // On any invalidating error (lease lost, fenced, or a lost
                // observation) drop the cached lease so the next begin_write
                // reacquires from the store — and thus runs recovery.
                if let Some(error) = result.as_ref().err() {
                    if LeaseAuthority::<M>::should_invalidate_cached_lease(error) {
                        a.clear_cached_lease().await;
                    }
                }
                result
            }
        }
    }

    async fn ensure_open_indexes_rebuilt(&self, current_head: Option<u64>) -> Result<()> {
        #[cfg(test)]
        self.recovery_runs
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.open_indexes.rebuilt_for_head() == Some(current_head) {
            return Ok(());
        }
        let Some(head) = current_head else {
            self.open_indexes
                .replace_rebuilt(None, OpenIndexesDelta::default());
            return Ok(());
        };
        let record = self.tables.blocks().load_record(head).await?.ok_or(
            MonadChainDataError::MissingData("missing published head block record"),
        )?;
        let mut delta = OpenIndexesDelta::default();
        self.rebuild_family_open_indexes(&mut delta, Family::Log, record.logs, head)
            .await?;
        self.rebuild_family_open_indexes(&mut delta, Family::Tx, record.txs, head)
            .await?;
        self.rebuild_family_open_indexes(&mut delta, Family::Trace, record.traces, head)
            .await?;
        self.open_indexes.replace_rebuilt(Some(head), delta);
        Ok(())
    }

    async fn rebuild_family_open_indexes(
        &self,
        delta: &mut OpenIndexesDelta,
        family: Family,
        window: crate::primitives::state::FamilyWindowRecord,
        published_head: u64,
    ) -> Result<()> {
        let next_primary_id = window
            .next_primary_id_exclusive()
            .map(|id| id.as_u64())
            .unwrap_or(window.first_primary_id.as_u64());
        let family_tables = self.tables.family(family);

        // Rebuild the current open directory bucket unconditionally — even when
        // the published head block itself minted no ids. The open bucket can hold
        // id-producing fragments from *earlier* blocks that a later batch will
        // seal; skipping the reload when the head happens to be empty would leave
        // the open index short those fragments and compact an incomplete summary.
        // Earlier buckets are sealed behind durable summaries, and `_for_rebuild`
        // already drops ahead-of-head fragments, so this loads exactly the open
        // bucket's published fragments (empty when the frontier is brand new).
        let open_bucket = bucket_start(next_primary_id);
        let directory_fragments = family_tables
            .list_bucket_fragments_for_rebuild(open_bucket, published_head)
            .await?;
        insert_directory_set(delta, family, open_bucket, directory_fragments);

        // Likewise, only the current open bitmap page needs fragment block
        // sets. Sealed pages have compacted page artifacts; ahead-of-head
        // fragments are ignored by the published-head filter in the scan.
        let open_page = global_page_start(next_primary_id);
        let page_start_local = local_page_start(open_page);
        let streams = family_tables.load_open_bitmap_streams(open_page).await?;
        for stream_id in streams {
            let fragments = family_tables
                .list_bitmap_fragments_for_rebuild(&stream_id, page_start_local, published_head)
                .await?;
            if fragments.is_empty() {
                continue;
            }
            delta
                .bitmap_open_streams
                .push((family, open_page, stream_id.clone()));
            insert_bitmap_set(delta, family, stream_id, page_start_local, fragments);
        }
        Ok(())
    }

    // The publication head is the commit boundary: every artifact written before
    // `cas_advance` is pre-publication state and must not be treated as valid by
    // readers until the head advances. Retry/recovery should derive the next block
    // from the published head and may overwrite any matching pre-publication
    // artifacts left by an interrupted ingest.
    //
    // The head row is CAS-protected so that during writer+standby failover the
    // losing writer sees `FencedOut` and steps down rather than racing the new
    // writer's publish.
    /// Persists one finalized block and advances the published head on success.
    pub async fn ingest_block(&self, block: FinalizedBlock) -> Result<IngestOutcome> {
        let (mut out, _timings) = self.ingest_blocks(vec![block]).await?;
        // `ingest_blocks` documents and tests an exact 1:1 input-to-outcome
        // contract, so a single-element input always yields exactly one
        // outcome. The `debug_assert_eq!` traps any future regression of
        // that invariant before the `expect` ever has to fire.
        debug_assert_eq!(
            out.len(),
            1,
            "ingest_blocks(vec![one]) returns exactly one outcome"
        );
        Ok(out
            .pop()
            .expect("ingest_blocks contract: one outcome per input block"))
    }

    /// Persists a contiguous run of finalized blocks as one ingest batch and
    /// advances the published head once on success. Reduces fjall WAL+commit
    /// overhead for backfill; live ingest with `batch_size = 1` reduces to
    /// the same on-disk shape as a single `ingest_block`.
    ///
    /// Returns exactly one [`IngestOutcome`] per input block, in input order.
    /// An empty input yields an empty `Vec` and is a no-op (no CAS advance).
    pub async fn ingest_blocks(
        &self,
        blocks: Vec<FinalizedBlock>,
    ) -> Result<(Vec<IngestOutcome>, IngestBatchTimings)> {
        let Some(plan) = self.plan_ingest_blocks(blocks).await? else {
            return Ok((Vec::new(), IngestBatchTimings::default()));
        };
        self.apply_ingest_plan(plan).await
    }

    pub async fn plan_ingest_blocks(
        &self,
        blocks: Vec<FinalizedBlock>,
    ) -> Result<Option<IngestPlan>> {
        if blocks.is_empty() {
            return Ok(None);
        }

        let mut timings = IngestBatchTimings {
            blocks: blocks.len(),
            ..IngestBatchTimings::default()
        };

        let block_tables = self.tables.blocks();
        let logs = self.tables.family(Family::Log);
        let txs = self.tables.family(Family::Tx);
        let traces = self.tables.family(Family::Trace);

        let mut planning_state = self.planning_state.lock().await;
        if planning_state.is_none() {
            // Cold cache: take (or renew) write authority to learn the
            // authoritative head and whether ownership transitioned. The lease
            // is cached in the authority; publish-time revalidation (D1 option
            // A) re-checks it inside the CAS rather than holding a guard across
            // plan→apply. The session itself is only read for its state here.
            let begin = self.authority_begin_write(self.observe_upstream()).await?;
            let current_head = match begin.indexed_finalized_head {
                // Head 0 is the sentinel for "nothing published yet" — block
                // numbers start at 1, so there is never a block-0 record. This
                // holds regardless of how this writer learned head 0: a Fresh
                // acquire that just wrote the row at head 0, OR a takeover
                // (Reacquired) of a predecessor that acquired the lease but died
                // before publishing block 1. Both must start clean, not try to
                // load a nonexistent block-0 record.
                0 => None,
                head => Some(head),
            };
            // Recovery runs only after an ownership transition (Fresh /
            // Reacquired); a Continuous renewal skips the open-index rebuild.
            if begin.continuity.requires_recovery() {
                self.ensure_open_indexes_rebuilt(current_head).await?;
            }
            let previous = match current_head {
                Some(head) => Some(block_tables.load_record(head).await?.ok_or(
                    MonadChainDataError::MissingData("missing published head block record"),
                )?),
                None => None,
            };
            *planning_state = Some(IngestPlanningState {
                head: current_head,
                previous,
            });
        }
        let state = planning_state
            .as_ref()
            .expect("planning state initialized above");
        match state.head {
            None => {
                if blocks[0].block_number() != 1 {
                    return Err(MonadChainDataError::InvalidRequest(
                        "first ingested block must be block 1 in the first pass",
                    ));
                }
            }
            Some(head) => {
                if blocks[0].block_number() != head + 1 {
                    return Err(MonadChainDataError::InvalidRequest(
                        "block_number must extend the planned head contiguously",
                    ));
                }
                let previous = state
                    .previous
                    .as_ref()
                    .ok_or(MonadChainDataError::MissingData(
                        "missing previous planned block record",
                    ))?;
                if previous.block_hash != blocks[0].parent_hash() {
                    return Err(MonadChainDataError::InvalidRequest(
                        "parent_hash must match the previous planned block",
                    ));
                }
            }
        }
        let first_previous = state.previous.clone();
        // Seed the artifact-checksum chain from the previously published head
        // (captured before `first_previous` is consumed below).
        let seed_checksum = first_previous
            .as_ref()
            .map(|record| record.artifact_checksum)
            .unwrap_or(EMPTY_CHECKSUM);

        let stage_a_start = Instant::now();
        let mut plan_starts: Vec<StageAPlanStarts> = Vec::with_capacity(blocks.len());
        let mut prev_record = first_previous;
        let mut prev_hash = blocks[0].parent_hash();
        for block in &blocks {
            // Lenient: log-only fixtures pass `txs: vec![]` alongside non-empty
            // `logs_by_tx`. When callers do provide txs, the count must line up
            // with the per-tx log grouping.
            if !block.txs.is_empty() && block.txs.len() != block.logs_by_tx.len() {
                return Err(MonadChainDataError::InvalidRequest(
                    "txs and logs_by_tx lengths must match when txs are provided",
                ));
            }
            if block.parent_hash() != prev_hash {
                return Err(MonadChainDataError::InvalidRequest(
                    "parent_hash must match the previous block in the batch",
                ));
            }

            let (next_log_id, next_tx_id, next_trace_id) = match &prev_record {
                Some(previous) => (
                    LogId::from(previous.logs.next_primary_id_exclusive()?),
                    TxId::from(previous.txs.next_primary_id_exclusive()?),
                    TraceId::from(previous.traces.next_primary_id_exclusive()?),
                ),
                None => (LogId::ZERO, TxId::ZERO, TraceId::ZERO),
            };
            plan_starts.push(StageAPlanStarts {
                log: next_log_id,
                tx: next_tx_id,
                trace: next_trace_id,
            });

            let log_count: u32 = block
                .logs_by_tx
                .iter()
                .map(Vec::len)
                .sum::<usize>()
                .try_into()
                .map_err(|_| MonadChainDataError::Decode("log count overflow"))?;
            let tx_count = u32::try_from(block.txs.len())
                .map_err(|_| MonadChainDataError::Decode("tx count overflow"))?;
            let trace_count = u32::try_from(block.traces.len())
                .map_err(|_| MonadChainDataError::Decode("trace count overflow"))?;

            let block_record = BlockRecord {
                block_number: block.block_number(),
                block_hash: block.block_hash(),
                parent_hash: block.parent_hash(),
                logs: crate::primitives::state::FamilyWindowRecord {
                    first_primary_id: next_log_id.into(),
                    count: log_count,
                },
                txs: crate::primitives::state::FamilyWindowRecord {
                    first_primary_id: next_tx_id.into(),
                    count: tx_count,
                },
                traces: crate::primitives::state::FamilyWindowRecord {
                    first_primary_id: next_trace_id.into(),
                    count: trace_count,
                },
                // This record only seeds the next block's primary-id windows
                // and parent-hash chain; its checksum is never read.
                artifact_checksum: EMPTY_CHECKSUM,
            };

            prev_hash = block_record.block_hash;
            prev_record = Some(block_record);
        }

        // Resolve the per-family row codec for every epoch this batch spans,
        // BEFORE the parallel plan build. `ensure_epoch_dict` is the
        // block-until-ready await point: it guarantees each epoch's dictionary
        // is durably published (and its write-side codec installed) before any
        // block of that epoch is framed. A batch is contiguous, so it usually
        // touches a single epoch.
        let epoch_blocks = self.tables.dicts().config().epoch_blocks;
        let first_epoch = blocks[0].block_number() / epoch_blocks;
        let last_epoch = blocks[blocks.len() - 1].block_number() / epoch_blocks;
        let mut epoch_codecs: std::collections::HashMap<u64, (RowCodec, RowCodec, RowCodec)> =
            std::collections::HashMap::new();
        for epoch in first_epoch..=last_epoch {
            let version = u32::try_from(epoch)
                .map_err(|_| MonadChainDataError::Decode("dict epoch/version overflow"))?;
            self.ensure_epoch_dicts(version).await?;
            let dicts = self.tables.dicts();
            let log_codec =
                dicts
                    .write_codec(Family::Log, version)
                    .ok_or(MonadChainDataError::MissingData(
                        "epoch log codec not installed",
                    ))?;
            let tx_codec =
                dicts
                    .write_codec(Family::Tx, version)
                    .ok_or(MonadChainDataError::MissingData(
                        "epoch tx codec not installed",
                    ))?;
            let trace_codec = dicts.write_codec(Family::Trace, version).ok_or(
                MonadChainDataError::MissingData("epoch trace codec not installed"),
            )?;
            epoch_codecs.insert(epoch, (log_codec, tx_codec, trace_codec));
        }
        let epoch_codecs_ref = &epoch_codecs;

        let planned: Vec<StageAPlanOutput> = blocks
            .par_iter()
            .zip(plan_starts.par_iter())
            .map(|(block, starts)| {
                let epoch = block.block_number() / epoch_blocks;
                let (log_codec_ref, tx_codec_ref, trace_codec_ref) = epoch_codecs_ref
                    .get(&epoch)
                    .expect("epoch codec resolved before par_iter");
                debug_assert_eq!(log_codec_ref.version() as u64, epoch);

                let mut log_plan = LogIngestPlan::build(block, starts.log, log_codec_ref)?;
                let mut tx_plan = TxIngestPlan::build(block, starts.tx, tx_codec_ref)?;
                let mut trace_plan = TraceIngestPlan::build(block, starts.trace, trace_codec_ref)?;

                let log_len = u32::try_from(log_plan.block_log_blob.len())
                    .map_err(|_| MonadChainDataError::Decode("block log blob too large"))?;
                let tx_len = u32::try_from(tx_plan.block_tx_blob.len())
                    .map_err(|_| MonadChainDataError::Decode("block tx blob too large"))?;
                let trace_base = log_len
                    .checked_add(tx_len)
                    .ok_or(MonadChainDataError::Decode(
                        "combined block blob base offset overflow",
                    ))?;
                log_plan.block_log_header.base_offset = 0;
                tx_plan.block_tx_header.base_offset = log_len;
                trace_plan.block_trace_header.base_offset = trace_base;

                // Content digest folds the uncompressed artifacts in parallel;
                // the chain over prior blocks is applied sequentially below.
                let block_content_digest =
                    block_content_digest(block, &log_plan, &tx_plan, &trace_plan);

                let block_record = BlockRecord {
                    block_number: block.block_number(),
                    block_hash: block.block_hash(),
                    parent_hash: block.parent_hash(),
                    logs: log_plan.log_window,
                    txs: tx_plan.tx_window,
                    traces: trace_plan.trace_window,
                    // Filled with the chained value in the sequential loop.
                    artifact_checksum: EMPTY_CHECKSUM,
                };

                Ok(StageAPlanOutput {
                    staged: StagedBlock {
                        log_plan,
                        tx_plan,
                        trace_plan,
                        block_record,
                    },
                    block_content_digest,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Seed the artifact-checksum chain from the previously published head
        // (or the empty seed for a fresh store), then fold each block's content
        // digest in contiguous order. This is the sequential dependency the
        // parallel plan build above cannot carry.
        let mut cumulative_checksum = seed_checksum;

        let mut staged: Vec<StagedBlock> = Vec::with_capacity(planned.len());
        for mut output in planned {
            cumulative_checksum = digest::chain(cumulative_checksum, output.block_content_digest);
            output.staged.block_record.artifact_checksum = cumulative_checksum;
            staged.push(output.staged);
        }

        timings.stage_a_ms = stage_a_start.elapsed().as_millis() as u64;

        let mut phase_a_delta = OpenIndexesDelta::default();
        for st in &staged {
            let block_number = st.block_record.block_number;
            append_family_phase_a_delta(
                &mut phase_a_delta,
                Family::Log,
                block_number,
                st.log_plan.log_window,
                &st.log_plan.bitmap_fragments,
            )?;
            append_family_phase_a_delta(
                &mut phase_a_delta,
                Family::Tx,
                block_number,
                st.tx_plan.tx_window,
                &st.tx_plan.bitmap_fragments,
            )?;
            append_family_phase_a_delta(
                &mut phase_a_delta,
                Family::Trace,
                block_number,
                st.trace_plan.trace_window,
                &st.trace_plan.bitmap_fragments,
            )?;
        }
        let last_staged = staged.last().expect("at least one block staged");
        let log_filter = PhaseAFragmentWriteFilter {
            open_bitmap_page: global_page_start(
                last_staged
                    .log_plan
                    .log_window
                    .next_primary_id_exclusive()?
                    .as_u64(),
            ),
        };
        let tx_filter = PhaseAFragmentWriteFilter {
            open_bitmap_page: global_page_start(
                last_staged
                    .tx_plan
                    .tx_window
                    .next_primary_id_exclusive()?
                    .as_u64(),
            ),
        };
        let trace_filter = PhaseAFragmentWriteFilter {
            open_bitmap_page: global_page_start(
                last_staged
                    .trace_plan
                    .trace_window
                    .next_primary_id_exclusive()?
                    .as_u64(),
            ),
        };

        // Phase A: stage every block's artifacts inside one write session so
        // the framework can fire meta + blob commits concurrently while
        // populating per-table caches for the Phase B compaction reads.
        let blocks_ref = &blocks;
        let staged_ref = &staged;
        let projected_open_indexes = self
            .open_indexes
            .projected_with_delta(phase_a_delta.clone());
        // Pre-batch (committed) inventory: the baseline for deciding which
        // open-bitmap-stream rows are genuinely new this batch. The projected
        // view above already folds in this batch's touched streams, so it can't
        // serve as that baseline.
        let committed_open_indexes = &self.open_indexes;
        let log_ranges = family_ranges(&staged, |s| s.log_plan.log_window);
        let tx_ranges = family_ranges(&staged, |s| s.tx_plan.tx_window);
        let trace_ranges = family_ranges(&staged, |s| s.trace_plan.trace_window);

        let log_touched_bitmap_per_block: Vec<_> = staged
            .iter()
            .map(|s| &s.log_plan.touched_bitmap_streams_by_page)
            .collect();
        let tx_touched_bitmap_per_block: Vec<_> = staged
            .iter()
            .map(|s| &s.tx_plan.touched_bitmap_streams_by_page)
            .collect();
        let trace_touched_bitmap_per_block: Vec<_> = staged
            .iter()
            .map(|s| &s.trace_plan.touched_bitmap_streams_by_page)
            .collect();

        let phase_a_stage = self.tables.stage_writes(|w| {
            Box::pin(async move {
                for (block, st) in blocks_ref.iter().zip(staged_ref.iter()) {
                    block_tables.stage_metadata(
                        w,
                        block.block_number(),
                        &st.block_record,
                        &block.header,
                        Bytes::from(st.log_plan.block_log_header.encode()),
                        Bytes::from(st.tx_plan.block_tx_header.encode()),
                        Bytes::from(st.trace_plan.block_trace_header.encode()),
                    );
                    let mut combined = st.log_plan.block_log_blob.clone();
                    combined.extend_from_slice(&st.tx_plan.block_tx_blob);
                    combined.extend_from_slice(&st.trace_plan.block_trace_blob);
                    w.tables()
                        .stage_block_blob(w, block.block_number(), combined);
                    logs.stage_indexed_family_ingest(
                        w,
                        block.block_number(),
                        st.log_plan.log_window,
                        &st.log_plan.bitmap_fragments,
                        log_filter,
                    )?;
                    txs.stage_indexed_family_ingest(
                        w,
                        block.block_number(),
                        st.tx_plan.tx_window,
                        &st.tx_plan.bitmap_fragments,
                        tx_filter,
                    )?;
                    traces.stage_indexed_family_ingest(
                        w,
                        block.block_number(),
                        st.trace_plan.trace_window,
                        &st.trace_plan.bitmap_fragments,
                        trace_filter,
                    )?;
                    block_tables.stage_hash_index(
                        w,
                        &st.block_record.block_hash,
                        block.block_number(),
                    );
                    for (tx_hash, location) in &st.tx_plan.hash_locations {
                        w.tables().tx_hash_index().stage_put(w, tx_hash, *location);
                    }
                }
                Ok(())
            })
        });

        let stage_b_plan = async {
            let reads_start = Instant::now();
            let plans = futures::try_join!(
                logs.plan_directory_compactions(&projected_open_indexes, &log_ranges),
                logs.plan_bitmap_compactions(
                    &projected_open_indexes,
                    committed_open_indexes,
                    &log_touched_bitmap_per_block,
                    &log_ranges
                ),
                txs.plan_directory_compactions(&projected_open_indexes, &tx_ranges),
                txs.plan_bitmap_compactions(
                    &projected_open_indexes,
                    committed_open_indexes,
                    &tx_touched_bitmap_per_block,
                    &tx_ranges
                ),
                traces.plan_directory_compactions(&projected_open_indexes, &trace_ranges),
                traces.plan_bitmap_compactions(
                    &projected_open_indexes,
                    committed_open_indexes,
                    &trace_touched_bitmap_per_block,
                    &trace_ranges
                ),
            )?;
            Ok::<_, MonadChainDataError>((plans, reads_start.elapsed().as_millis() as u64))
        };

        let (phase_a_writes_result, stage_b_plan_result) =
            futures::join!(phase_a_stage, stage_b_plan);
        let phase_a_writes = phase_a_writes_result?;
        timings.phase_a_write_counts = phase_a_writes.counts.clone();

        let (
            (
                log_dir_plan,
                log_bitmap_plan,
                tx_dir_plan,
                tx_bitmap_plan,
                trace_dir_plan,
                trace_bitmap_plan,
            ),
            reads_elapsed_ms,
        ) = stage_b_plan_result?;
        timings.reads_ms = reads_elapsed_ms;

        let phase_b_has_writes = !log_dir_plan.buckets.is_empty()
            || log_bitmap_plan.has_writes
            || !tx_dir_plan.buckets.is_empty()
            || tx_bitmap_plan.has_writes
            || !trace_dir_plan.buckets.is_empty()
            || trace_bitmap_plan.has_writes;

        let last = staged.last().expect("at least one block staged");
        let new_head = last.block_record.block_number;
        let publication = PublicationAdvance {
            table: PublicationTables::<M>::PUBLICATION_STATE_TABLE,
            key: PublicationTables::<M>::PUBLICATION_STATE_KEY.to_vec(),
            new_head,
            head_artifact_checksum: last.block_record.artifact_checksum,
        };

        let mut combined_writes = phase_a_writes;
        let mut phase_b_eviction = OpenIndexesEviction::default();
        if phase_b_has_writes {
            timings.phase_b_skipped = false;
            let log_dir = &log_dir_plan;
            let log_bitmap = &log_bitmap_plan;
            let tx_dir = &tx_dir_plan;
            let tx_bitmap = &tx_bitmap_plan;
            let trace_dir = &trace_dir_plan;
            let trace_bitmap = &trace_bitmap_plan;
            // `stage_b_ms` is the synchronous closure body — pure staging
            // into the session. The IO side applies the combined writes and
            // publishes with a standalone CAS after planning returns.
            let stage_b_ms_cell = std::sync::atomic::AtomicU64::new(0);
            let phase_b_writes = self
                .tables
                .stage_writes(|w| {
                    let stage_b_ms_cell = &stage_b_ms_cell;
                    Box::pin(async move {
                        let stage_b_start = Instant::now();
                        logs.stage_directory_compactions(w, log_dir);
                        logs.stage_bitmap_compactions(w, log_bitmap);
                        txs.stage_directory_compactions(w, tx_dir);
                        txs.stage_bitmap_compactions(w, tx_bitmap);
                        traces.stage_directory_compactions(w, trace_dir);
                        traces.stage_bitmap_compactions(w, trace_bitmap);
                        stage_b_ms_cell.store(
                            stage_b_start.elapsed().as_millis() as u64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        Ok(())
                    })
                })
                .await?;
            timings.stage_b_ms = stage_b_ms_cell.load(std::sync::atomic::Ordering::Relaxed);
            timings.phase_b_write_counts = phase_b_writes.counts.clone();
            phase_b_eviction.merge(log_dir_plan.eviction(Family::Log));
            phase_b_eviction.merge(log_bitmap_plan.eviction(Family::Log));
            phase_b_eviction.merge(tx_dir_plan.eviction(Family::Tx));
            phase_b_eviction.merge(tx_bitmap_plan.eviction(Family::Tx));
            phase_b_eviction.merge(trace_dir_plan.eviction(Family::Trace));
            phase_b_eviction.merge(trace_bitmap_plan.eviction(Family::Trace));
            combined_writes.merge(phase_b_writes);
        } else {
            timings.phase_b_skipped = true;
        }
        // The published row is stamped with the lease fields at publish time,
        // so its exact bytes are not known here. The encoded width is a small
        // fixed-shape RLP list (`PUBLICATION_STATE_ENCODED_BYTES`), used only
        // for write-volume accounting.
        timings
            .phase_b_write_counts
            .add_cas(publication.table, PUBLICATION_STATE_ENCODED_BYTES);
        combined_writes.add_cas_count(publication.table, PUBLICATION_STATE_ENCODED_BYTES);

        // Planning owns the ingest open-index state. Once a plan is emitted,
        // later plans should account for its open fragments even if the IO
        // worker has not published it yet. CAS failure tears down ingest, so
        // there is no rollback path for this speculative state.
        self.open_indexes.apply_delta(phase_a_delta);
        if phase_b_has_writes {
            self.open_indexes.apply_eviction(phase_b_eviction);
        }
        self.open_indexes.mark_rebuilt_for_head(Some(new_head));
        *planning_state = Some(IngestPlanningState {
            head: Some(new_head),
            previous: Some(last.block_record.clone()),
        });

        let outcomes = staged
            .into_iter()
            .map(|s| IngestOutcome {
                indexed_finalized_head: s.block_record.block_number,
                block_record: s.block_record,
                written_logs: s.log_plan.written_logs,
                written_txs: s.tx_plan.written_txs,
                written_traces: s.trace_plan.written_traces,
            })
            .collect();
        Ok(Some(IngestPlan {
            outcomes,
            timings,
            writes: combined_writes,
            publication,
        }))
    }

    /// Ensures the row-codec dictionary for `(family, version)` is published
    /// and its write-side codec installed, training it if necessary. This is
    /// the **block-until-ready** primitive of the epoch-based lifecycle: a
    /// block of epoch `V` is only ever framed after `ensure_epoch_dict(family,
    /// V)` returns, so the durable dict for `V` always exists before any block
    /// that references it.
    ///
    /// Single-flight: concurrent callers (the plan path and the binary's
    /// background pre-train task) serialize on the per-family
    /// [`DictManager::train_lock`] and double-check `has_codec`, so a given
    /// `(family, version)` is trained at most once.
    pub async fn ensure_epoch_dict(&self, family: Family, version: u32) -> Result<()> {
        // Version 0 is the implicit plain bootstrap; its codec is pre-installed.
        if version == DICT_VERSION_NONE {
            return Ok(());
        }
        let dicts = self.tables.dicts();
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
        if let Some(bytes) = self.tables.family(family).load_dict(version).await? {
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
        self.tables
            .with_writes(|w| {
                let dict_for_write = dict_for_write.clone();
                Box::pin(async move {
                    self.tables
                        .family(family)
                        .stage_dict(w, version, dict_for_write);
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

    /// Collects a deterministic training corpus for `(family, version)` from
    /// the leading blocks of epoch `version - 1`. Block selection is strided
    /// (seeded by `version`, no RNG) so the corpus is reproducible. At most
    /// [`TARGET_SAMPLE_BLOCKS`] blocks are sampled, each contributing up to
    /// `PER_BLOCK_SAMPLE_CAP` rows via [`should_sample_row`], which bounds the
    /// corpus. Sampled blocks belong to epoch `version - 1`, whose dictionary
    /// was published earlier, so `decode_block_row` just works.
    async fn read_back_training_samples(
        &self,
        family: Family,
        version: u32,
    ) -> Result<Vec<Vec<u8>>> {
        let dicts = self.tables.dicts();
        let (start, end) = dicts.config().training_range(version);
        let span = end.saturating_sub(start);
        if span == 0 {
            return Ok(Vec::new());
        }
        let count = span.min(TARGET_SAMPLE_BLOCKS);
        let stride = (span / count).max(1);
        let offset = u64::from(version) % stride;

        let family_tables = self.tables.family(family);
        let mut samples: Vec<Vec<u8>> = Vec::new();
        for i in 0..count {
            let block_number = start + offset + i * stride;
            if block_number >= end {
                break;
            }
            let Some(header_bytes) = family_tables.load_block_header(block_number).await? else {
                continue;
            };
            let header = BlockBlobHeader::decode(&header_bytes)?;
            if header.offsets.len() < 2 {
                continue;
            }
            let Some(region) = self
                .tables
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
                    .tables
                    .decode_block_row(family, header.dict_version, frame)
                    .await?;
                samples.push(raw.to_vec());
            }
        }
        Ok(samples)
    }

    pub async fn apply_ingest_plan(
        &self,
        plan: IngestPlan,
    ) -> Result<(Vec<IngestOutcome>, IngestBatchTimings)> {
        self.apply_ingest_plan_with_retry(plan, IoRetryPolicy::no_retries())
            .await
    }

    pub async fn apply_ingest_plan_with_retry(
        &self,
        plan: IngestPlan,
        retry: IoRetryPolicy,
    ) -> Result<(Vec<IngestOutcome>, IngestBatchTimings)> {
        let IngestPlan {
            outcomes,
            mut timings,
            writes,
            publication,
        } = plan;

        let commit_start = Instant::now();
        let mut attempts = 0usize;
        let mut backoff = retry.initial_backoff;
        let combined_timings = loop {
            match self.tables.apply_staged_writes_timed(writes.clone()).await {
                Ok(timings) => break timings,
                Err(e) => {
                    if retry.max_retries.is_some_and(|max| attempts >= max) {
                        return Err(e);
                    }
                    attempts = attempts.saturating_add(1);
                    if !backoff.is_zero() {
                        retry_sleep(backoff).await;
                        backoff = backoff.saturating_mul(2).min(retry.max_backoff);
                    }
                }
            }
        };
        timings.commit_a_meta_ms = combined_timings.meta.as_millis() as u64;
        timings.commit_a_blob_ms = combined_timings.blob.as_millis() as u64;
        // Publish through the write authority: it revalidates and renews the
        // lease inside the ownership-validating CAS (D1 option A), stamping the
        // owner/session/lease fields onto the head row. This replaces the old
        // inline load_state + cas_put. `LeaseLost` is surfaced distinctly from
        // a same-owner `FencedOut`; `authority_publish` clears the cached lease
        // on either so the next begin_write reacquires (and runs recovery).
        let cas_start = Instant::now();
        self.authority_publish(
            publication.new_head,
            publication.head_artifact_checksum,
            self.observe_upstream(),
        )
        .await?;
        timings.cas_ms = cas_start.elapsed().as_millis() as u64;
        let commit_elapsed_ms = commit_start.elapsed().as_millis() as u64;
        timings.commit_b_ms = commit_elapsed_ms
            .saturating_sub(timings.commit_a_meta_ms.max(timings.commit_a_blob_ms));
        Ok((outcomes, timings))
    }

    /// Re-derives the per-block artifact-checksum chain for a contiguous run of
    /// finalized `blocks` and compares each running value against the published
    /// [`BlockRecord`]. This is the standby verification primitive: it proves
    /// the node would have written byte-equivalent artifacts (modulo zstd
    /// framing — the digest folds *uncompressed* rows) WITHOUT acquiring the
    /// write lease or performing any writes.
    ///
    /// Batch-independent: the chain is seeded from the published predecessor
    /// record and folded one block at a time, so a standby may call this with
    /// any grouping regardless of how the primary batched ingest. Returns at
    /// the first block that mismatches ([`VerifyOutcome::Mismatch`]) or whose
    /// record is not yet published ([`VerifyOutcome::Pending`]).
    pub async fn verify_artifact_checksums(
        &self,
        blocks: &[FinalizedBlock],
    ) -> Result<VerifyOutcome> {
        let Some(first) = blocks.first() else {
            return Ok(VerifyOutcome::Match { through_block: 0 });
        };
        let block_tables = self.tables.blocks();

        // Seed the chain and the family primary-id frontier from the published
        // predecessor (or the empty/zero seed when verifying from block 1).
        let first_number = first.block_number();
        let (mut cumulative, mut next_log, mut next_tx, mut next_trace) = if first_number <= 1 {
            (EMPTY_CHECKSUM, LogId::ZERO, TxId::ZERO, TraceId::ZERO)
        } else {
            let prev = block_tables.load_record(first_number - 1).await?.ok_or(
                MonadChainDataError::MissingData("missing predecessor record for verification"),
            )?;
            (
                prev.artifact_checksum,
                LogId::from(prev.logs.next_primary_id_exclusive()?),
                TxId::from(prev.txs.next_primary_id_exclusive()?),
                TraceId::from(prev.traces.next_primary_id_exclusive()?),
            )
        };

        let epoch_blocks = self.tables.dicts().config().epoch_blocks;
        let mut last_verified = first_number.saturating_sub(1);
        for block in blocks {
            let epoch = block.block_number() / epoch_blocks;
            let version = u32::try_from(epoch)
                .map_err(|_| MonadChainDataError::Decode("dict epoch/version overflow"))?;
            self.ensure_epoch_dicts(version).await?;
            let dicts = self.tables.dicts();
            let log_codec =
                dicts
                    .write_codec(Family::Log, version)
                    .ok_or(MonadChainDataError::MissingData(
                        "epoch log codec not installed",
                    ))?;
            let tx_codec =
                dicts
                    .write_codec(Family::Tx, version)
                    .ok_or(MonadChainDataError::MissingData(
                        "epoch tx codec not installed",
                    ))?;
            let trace_codec = dicts.write_codec(Family::Trace, version).ok_or(
                MonadChainDataError::MissingData("epoch trace codec not installed"),
            )?;

            let log_plan = LogIngestPlan::build(block, next_log, &log_codec)?;
            let tx_plan = TxIngestPlan::build(block, next_tx, &tx_codec)?;
            let trace_plan = TraceIngestPlan::build(block, next_trace, &trace_codec)?;

            cumulative = digest::chain(
                cumulative,
                block_content_digest(block, &log_plan, &tx_plan, &trace_plan),
            );

            let number = block.block_number();
            match block_tables.load_record(number).await? {
                None => {
                    return Ok(VerifyOutcome::Pending {
                        block_number: number,
                    })
                }
                Some(published) if published.artifact_checksum != cumulative => {
                    return Ok(VerifyOutcome::Mismatch {
                        block_number: number,
                        published: published.artifact_checksum,
                        computed: cumulative,
                    });
                }
                Some(_) => {}
            }

            // Advance the family frontier for the next block.
            next_log = LogId::from(log_plan.log_window.next_primary_id_exclusive()?);
            next_tx = TxId::from(tx_plan.tx_window.next_primary_id_exclusive()?);
            next_trace = TraceId::from(trace_plan.trace_window.next_primary_id_exclusive()?);
            last_verified = number;
        }

        Ok(VerifyOutcome::Match {
            through_block: last_verified,
        })
    }

    /// Executes a finalized logs query over the current published head.
    /// The service's configured `QueryLimits` bound the request shape and
    /// the resolved block-range span.
    pub async fn query_logs(&self, request: QueryLogsRequest) -> Result<QueryLogsResponse> {
        self.limits.check_limit(request.envelope.limit)?;

        let head = self.load_published_head().await?;
        let window = ResolvedBlockWindow::resolve(
            &request.envelope,
            head,
            &self.limits,
            self.tables.blocks(),
        )
        .await?;

        let mut response = if request.filter.has_indexed_clause() {
            execute_indexed_log_query(&self.tables, &request, window, head).await?
        } else {
            execute_block_scan_query(&self.tables, &request, window).await?
        };

        if request.relations.blocks {
            response.blocks = Some(if response.logs.is_empty() {
                Vec::new()
            } else {
                load_blocks_by_numbers(
                    self.tables.blocks(),
                    response.logs.iter().map(|l| l.block_number),
                )
                .await?
            });
        }

        if request.relations.transactions {
            response.transactions = Some(
                load_txs_by_positions(
                    &self.tables,
                    response.logs.iter().map(|l| (l.block_number, l.tx_index)),
                )
                .await?,
            );
        }

        Ok(response)
    }

    /// Executes a finalized transactions query over the current published
    /// head. The service's configured `QueryLimits` bound the request
    /// shape and the resolved block-range span.
    pub async fn query_transactions(
        &self,
        request: QueryTransactionsRequest,
    ) -> Result<QueryTransactionsResponse> {
        self.limits.check_limit(request.envelope.limit)?;

        let head = self.load_published_head().await?;
        let window = ResolvedBlockWindow::resolve(
            &request.envelope,
            head,
            &self.limits,
            self.tables.blocks(),
        )
        .await?;

        let mut response = if request.filter.has_indexed_clause() {
            execute_indexed_tx_query(&self.tables, &request, window, head).await?
        } else {
            execute_block_scan_tx_query(&self.tables, &request, window).await?
        };

        if request.relations.blocks {
            response.blocks = Some(if response.txs.is_empty() {
                Vec::new()
            } else {
                load_blocks_by_numbers(
                    self.tables.blocks(),
                    response.txs.iter().map(|t| t.block_number),
                )
                .await?
            });
        }

        Ok(response)
    }

    /// Resolves a finalized transaction by hash. Returns `None` if the
    /// hash was never indexed. A hit in the index that fails to
    /// materialize inside the published range indicates a data-layer
    /// inconsistency and surfaces as `MissingData`; it is not silently
    /// flattened to `None`.
    pub async fn get_transaction(&self, tx_hash: Hash32) -> Result<Option<TxEntry>> {
        let Some(location) = self.tables.tx_hash_index().get(&tx_hash).await? else {
            return Ok(None);
        };
        let Some(head) = self.publication.load_published_head().await? else {
            return Ok(None);
        };
        if location.block_number > head {
            return Ok(None);
        }

        let materializer = TxMaterializer::new(&self.tables);
        let entry = materializer
            .load_record_at(location.block_number, location.tx_idx as usize)
            .await?;
        Ok(Some(entry))
    }

    /// Executes a finalized traces query over the current published head.
    /// The service's configured `QueryLimits` bound the request shape and
    /// the resolved block-range span.
    pub async fn query_traces(&self, request: QueryTracesRequest) -> Result<QueryTracesResponse> {
        self.limits.check_limit(request.envelope.limit)?;

        let head = self.load_published_head().await?;
        let window = ResolvedBlockWindow::resolve(
            &request.envelope,
            head,
            &self.limits,
            self.tables.blocks(),
        )
        .await?;

        let mut response = if request.filter.has_indexed_clause() {
            execute_indexed_trace_query(&self.tables, &request, window, head).await?
        } else {
            execute_block_scan_trace_query(&self.tables, &request, window).await?
        };

        if request.relations.blocks {
            response.blocks = Some(
                load_blocks_by_numbers(
                    self.tables.blocks(),
                    response.traces.iter().map(|t| t.block_number),
                )
                .await?,
            );
        }

        if request.relations.transactions {
            response.transactions = Some(
                load_txs_by_positions(
                    &self.tables,
                    response.traces.iter().map(|t| (t.block_number, t.tx_index)),
                )
                .await?,
            );
        }

        Ok(response)
    }

    /// Executes a finalized transfers query over the current published
    /// head. Transfers are a view over the trace family gated by the
    /// indexed `has_transfer` column; the response carries the projected
    /// `TransferEntry` rows and the same optional `blocks` /
    /// `transactions` relations as the traces query.
    pub async fn query_transfers(
        &self,
        request: QueryTransfersRequest,
    ) -> Result<QueryTransfersResponse> {
        self.limits.check_limit(request.envelope.limit)?;

        let head = self.load_published_head().await?;
        let window = ResolvedBlockWindow::resolve(
            &request.envelope,
            head,
            &self.limits,
            self.tables.blocks(),
        )
        .await?;

        let mut response = if request.filter.has_indexed_clause() {
            execute_indexed_transfer_query(&self.tables, &request, window, head).await?
        } else {
            execute_block_scan_transfer_query(&self.tables, &request, window).await?
        };

        if request.relations.blocks {
            response.blocks = Some(
                load_blocks_by_numbers(
                    self.tables.blocks(),
                    response.transfers.iter().map(|t| t.block_number),
                )
                .await?,
            );
        }

        if request.relations.transactions {
            response.transactions = Some(
                load_txs_by_positions(
                    &self.tables,
                    response
                        .transfers
                        .iter()
                        .map(|t| (t.block_number, t.tx_index)),
                )
                .await?,
            );
        }

        Ok(response)
    }

    /// Executes a finalized blocks query over the current published head.
    /// The service's configured `QueryLimits` bound the request shape and
    /// the resolved block-range span.
    pub async fn query_blocks(&self, request: QueryBlocksRequest) -> Result<QueryBlocksResponse> {
        self.limits.check_limit(request.envelope.limit)?;

        let head = self.load_published_head().await?;
        let window = ResolvedBlockWindow::resolve(
            &request.envelope,
            head,
            &self.limits,
            self.tables.blocks(),
        )
        .await?;

        execute_query_blocks(&self.tables, &request, window).await
    }

    async fn load_published_head(&self) -> Result<u64> {
        match self.publication.load_published_head().await? {
            // Head 0 is the acquire-before-first-publish sentinel: a lease
            // writer has claimed ownership (writing the row at head 0) but no
            // block is finalized yet. For a reader this is indistinguishable
            // from a never-written store — there are no finalized blocks — so it
            // reports "no published blocks" rather than resolving a range
            // against head 0 (which would surface a confusing "block range
            // starts above the published head"). Real published heads are always
            // >= 1, since ingest starts at block 1.
            None | Some(0) => Err(MonadChainDataError::MissingData("no published blocks")),
            Some(head) => Ok(head),
        }
    }
}

#[cfg(test)]
mod recovery_gating_tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::{
        family::FinalizedBlock,
        primitives::EvmBlockHeader,
        store::{InMemoryBlobStore, InMemoryMetaStore},
    };

    fn empty_block(number: u64, parent: crate::family::Hash32) -> FinalizedBlock {
        FinalizedBlock {
            header: EvmBlockHeader {
                number,
                parent_hash: parent,
                ..EvmBlockHeader::default()
            },
            logs_by_tx: vec![],
            txs: Vec::new(),
            traces: vec![],
        }
    }

    fn chain(n: u64) -> Vec<FinalizedBlock> {
        let mut blocks = Vec::with_capacity(n as usize);
        let mut parent = crate::family::Hash32::ZERO;
        for number in 1..=n {
            let block = empty_block(number, parent);
            parent = block.header.hash_slow();
            blocks.push(block);
        }
        blocks
    }

    fn writer(
        meta: InMemoryMetaStore,
        owner_id: u64,
        clock: std::sync::Arc<AtomicU64>,
        lease_blocks: u64,
        renew_threshold: u64,
    ) -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
        MonadChainDataService::new_reader_writer(
            meta,
            InMemoryBlobStore::default(),
            QueryLimits::UNLIMITED,
            owner_id,
            lease_blocks,
            renew_threshold,
            Arc::new(move || Some(clock.load(Ordering::SeqCst))),
        )
    }

    // Reacquired (takeover) runs `ensure_open_indexes_rebuilt`; Continuous
    // (in-window renewal) does not. The open-index rebuild signal
    // (`rebuilt_for_head`) is the observable.
    #[tokio::test(flavor = "current_thread")]
    async fn reacquire_runs_recovery_continuous_skips_it() {
        let meta = InMemoryMetaStore::default();
        let blocks = chain(4);

        // Node A (owner 1) acquires at observed=10 (lease through 13) and
        // publishes blocks 1..=2.
        let clock_a = Arc::new(AtomicU64::new(10));
        let node_a = writer(meta.clone(), 1, clock_a.clone(), 4, 1);
        node_a
            .ingest_blocks(blocks[..2].to_vec())
            .await
            .expect("A publishes 1..=2");
        assert_eq!(
            node_a.publication().load_published_head().await.unwrap(),
            Some(2)
        );

        // Node B (owner 2) observes 20 > 13: takeover → Reacquired → recovery
        // rebuilds open indexes from the published head (2).
        let clock_b = Arc::new(AtomicU64::new(20));
        let node_b = writer(meta.clone(), 2, clock_b.clone(), 4, 1);
        let runs_before = node_b.recovery_runs.load(Ordering::SeqCst);
        let plan = node_b
            .plan_ingest_blocks(blocks[2..3].to_vec())
            .await
            .expect("B plans block 3")
            .expect("B has a plan");
        assert_eq!(
            node_b.recovery_runs.load(Ordering::SeqCst),
            runs_before + 1,
            "Reacquired must run the open-index rebuild exactly once"
        );
        node_b.apply_ingest_plan(plan).await.expect("B publishes 3");

        // Same process B renews in-window (observe 21, lease now through 23):
        // Continuous → recovery is skipped. Drop the planning cache so the
        // cold-cache branch (the only place gating runs) is exercised again.
        *node_b.planning_state.lock().await = None;
        let runs_before = node_b.recovery_runs.load(Ordering::SeqCst);
        clock_b.store(21, Ordering::SeqCst);
        let plan = node_b
            .plan_ingest_blocks(blocks[3..4].to_vec())
            .await
            .expect("B plans block 4")
            .expect("B has a plan");
        assert_eq!(
            node_b.recovery_runs.load(Ordering::SeqCst),
            runs_before,
            "Continuous renewal must not re-run the open-index rebuild"
        );
        node_b.apply_ingest_plan(plan).await.expect("B publishes 4");
        assert_eq!(
            node_b.publication().load_published_head().await.unwrap(),
            Some(4)
        );
    }

    // A leader that acquires the lease (which writes the publication row at
    // head 0) but dies before publishing block 1 leaves head 0 in the store. A
    // standby taking over sees continuity `Reacquired` with head 0; it must
    // start clean (head 0 == nothing published) rather than try to load a
    // nonexistent block-0 record. Regression test for the over-narrow
    // `0 if Fresh` head-0 guard.
    #[tokio::test(flavor = "current_thread")]
    async fn takeover_before_first_publish_starts_clean() {
        let meta = InMemoryMetaStore::default();
        let blocks = chain(1);

        // Node A acquires the lease at observed=10 (writes head 0) by planning
        // block 1, then "dies" without applying — the plan is dropped, so no
        // head past 0 is ever published.
        let clock_a = Arc::new(AtomicU64::new(10));
        let node_a = writer(meta.clone(), 1, clock_a.clone(), 4, 1);
        let _plan = node_a
            .plan_ingest_blocks(blocks.clone())
            .await
            .expect("A plans block 1")
            .expect("A has a plan");
        // The acquire CAS wrote the row at head 0 even though nothing published.
        assert_eq!(
            node_a.publication().load_published_head().await.unwrap(),
            Some(0)
        );
        drop(node_a);

        // Node B observes 20 > A's lease bound (13): takeover with head still 0.
        // It must plan and publish block 1 cleanly, not error on a missing
        // block-0 record.
        let clock_b = Arc::new(AtomicU64::new(20));
        let node_b = writer(meta.clone(), 2, clock_b.clone(), 4, 1);
        let plan = node_b
            .plan_ingest_blocks(blocks)
            .await
            .expect("B plans block 1 after takeover")
            .expect("B has a plan");
        node_b.apply_ingest_plan(plan).await.expect("B publishes 1");
        assert_eq!(
            node_b.publication().load_published_head().await.unwrap(),
            Some(1)
        );
    }
}
