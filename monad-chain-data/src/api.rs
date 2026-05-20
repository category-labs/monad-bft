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

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::lock::Mutex;
use rayon::prelude::*;
use tracing::trace;

use crate::{
    blocks::{
        execute_query_blocks, load_blocks_by_numbers, QueryBlocksRequest, QueryBlocksResponse,
    },
    engine::{
        bitmap::{global_page_start, local_page_start, stream_page_global_start},
        family::Family,
        ingest::{persist::PhaseAFragmentWriteFilter, ReadPlanningTimings},
        open_index::{
            insert_bitmap_set, insert_directory_set, OpenIndexStats, OpenIndexes, OpenIndexesDelta,
            OpenIndexesEviction,
        },
        primary_dir::{bucket_start, fragment_bucket_starts, PrimaryDirFragment},
        query::family_runner::IndexedFamilyQuery,
        tables::{PublicationTables, StagedWrites, Tables, WriteOpCounts},
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
        state::{BlockRecord, LogId, TraceId, TxId},
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

pub struct MonadChainDataService<M: MetaStoreCas, B: BlobStore> {
    tables: Tables<M, B>,
    publication: PublicationTables<M>,
    limits: QueryLimits,
    open_indexes: OpenIndexes,
    planning_state: Mutex<Option<IngestPlanningState>>,
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
    log_plan_us: u64,
    tx_plan_us: u64,
    trace_plan_us: u64,
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

/// Per-batch timing breakdown emitted by [`MonadChainDataService::ingest_blocks`].
/// All durations are milliseconds. Data writes are applied before the
/// publication CAS; `commit_b_ms` is retained as the post-write/CAS overhead
/// bucket for existing ingest logging.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestBatchTimings {
    pub stage_a_ms: u64,
    pub stage_a_log_plan_us: u64,
    pub stage_a_tx_plan_us: u64,
    pub stage_a_trace_plan_us: u64,
    pub stage_a_delta_us: u64,
    pub stage_a_session_stage_us: u64,
    pub stage_a_bitmap_fragment_count: u64,
    pub stage_a_hash_location_count: u64,
    pub stage_a_dir_fragments_total: u64,
    pub stage_a_dir_fragments_written: u64,
    pub stage_a_dir_fragments_skipped: u64,
    pub stage_a_bitmap_fragments_total: u64,
    pub stage_a_bitmap_fragments_written: u64,
    pub stage_a_bitmap_fragments_skipped: u64,
    pub phase_a_write_counts: WriteOpCounts,
    pub phase_b_write_counts: WriteOpCounts,
    pub commit_a_meta_ms: u64,
    pub commit_a_blob_ms: u64,
    pub reads_ms: u64,
    pub reads_dir_list_ms: u64,
    pub reads_dir_get_ms: u64,
    pub reads_dir_list_count: u64,
    pub reads_dir_get_count: u64,
    pub reads_bitmap_open_streams_ms: u64,
    pub reads_bitmap_open_streams_us: u64,
    pub reads_bitmap_open_streams_count: u64,
    pub reads_bitmap_list_ms: u64,
    pub reads_bitmap_get_ms: u64,
    pub reads_bitmap_shape_ms: u64,
    pub reads_bitmap_shape_us: u64,
    pub reads_bitmap_union_us: u64,
    pub reads_bitmap_frontier_us: u64,
    pub reads_bitmap_open_write_us: u64,
    pub reads_bitmap_index_ms: u64,
    pub reads_bitmap_index_us: u64,
    pub reads_bitmap_compact_ms: u64,
    pub reads_bitmap_compact_us: u64,
    pub reads_bitmap_compact_wall_us: u64,
    pub reads_bitmap_list_count: u64,
    pub reads_bitmap_get_count: u64,
    pub reads_bitmap_fragment_count: u64,
    pub reads_bitmap_fragment_bytes: u64,
    pub reads_bitmap_compact_count: u64,
    pub reads_bitmap_frontier_stream_count: u64,
    pub reads_bitmap_union_page_count: u64,
    pub reads_bitmap_union_stream_count: u64,
    pub reads_bitmap_touched_page_count: u64,
    pub reads_bitmap_touched_stream_count: u64,
    pub reads_bitmap_final_open_stream_count: u64,
    pub reads_dir_index_ms: u64,
    pub reads_dir_index_us: u64,
    pub reads_dir_decode_ms: u64,
    pub reads_dir_decode_us: u64,
    pub reads_dir_fragment_count: u64,
    pub reads_unaccounted_ms: u64,
    pub reads_unaccounted_us: u64,
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

#[derive(Debug, Clone)]
pub struct PublicationAdvance {
    pub table: crate::store::TableId,
    pub key: Vec<u8>,
    pub value: Bytes,
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

impl<M: MetaStoreCas, B: BlobStore> MonadChainDataService<M, B> {
    pub fn new(meta_store: M, blob_store: B, limits: QueryLimits) -> Self {
        Self::with_cache_config(meta_store, blob_store, limits, CacheConfig::default())
    }

    /// Parallel constructor that lets the binary thread an operator-tuned
    /// `CacheConfig` through. Existing `new` keeps its three-arg shape so
    /// fixture call sites don't churn.
    pub fn with_cache_config(
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

    async fn ensure_open_indexes_rebuilt(&self, current_head: Option<u64>) -> Result<()> {
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

        // Rebuild only the current open directory bucket. Earlier buckets are
        // sealed behind durable summaries, and future/ahead-of-head fragments
        // are speculative write-before-CAS artifacts that retry will restage.
        if window.count > 0 {
            let open_bucket = bucket_start(next_primary_id);
            let directory_fragments = family_tables
                .list_bucket_fragments_for_rebuild(open_bucket, published_head)
                .await?;
            insert_directory_set(delta, family, open_bucket, directory_fragments);
        }

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
            let current_head = self.publication.load_published_head().await?;
            self.ensure_open_indexes_rebuilt(current_head).await?;
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
            };

            prev_hash = block_record.block_hash;
            prev_record = Some(block_record);
        }

        let planned: Vec<StageAPlanOutput> = blocks
            .par_iter()
            .zip(plan_starts.par_iter())
            .map(|(block, starts)| {
                let log_plan_start = Instant::now();
                let log_plan = LogIngestPlan::build(block, starts.log)?;
                let log_plan_us = log_plan_start.elapsed().as_micros() as u64;

                let tx_plan_start = Instant::now();
                let tx_plan = TxIngestPlan::build(block, starts.tx)?;
                let tx_plan_us = tx_plan_start.elapsed().as_micros() as u64;

                let trace_plan_start = Instant::now();
                let trace_plan = TraceIngestPlan::build(block, starts.trace)?;
                let trace_plan_us = trace_plan_start.elapsed().as_micros() as u64;

                let block_record = BlockRecord {
                    block_number: block.block_number(),
                    block_hash: block.block_hash(),
                    parent_hash: block.parent_hash(),
                    logs: log_plan.log_window,
                    txs: tx_plan.tx_window,
                    traces: trace_plan.trace_window,
                };

                Ok(StageAPlanOutput {
                    staged: StagedBlock {
                        log_plan,
                        tx_plan,
                        trace_plan,
                        block_record,
                    },
                    log_plan_us,
                    tx_plan_us,
                    trace_plan_us,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut staged: Vec<StagedBlock> = Vec::with_capacity(planned.len());
        for output in planned {
            timings.stage_a_log_plan_us = timings
                .stage_a_log_plan_us
                .saturating_add(output.log_plan_us);
            timings.stage_a_tx_plan_us =
                timings.stage_a_tx_plan_us.saturating_add(output.tx_plan_us);
            timings.stage_a_trace_plan_us = timings
                .stage_a_trace_plan_us
                .saturating_add(output.trace_plan_us);
            timings.stage_a_bitmap_fragment_count = timings
                .stage_a_bitmap_fragment_count
                .saturating_add(output.staged.log_plan.bitmap_fragments.len() as u64)
                .saturating_add(output.staged.tx_plan.bitmap_fragments.len() as u64)
                .saturating_add(output.staged.trace_plan.bitmap_fragments.len() as u64);
            timings.stage_a_hash_location_count = timings
                .stage_a_hash_location_count
                .saturating_add(output.staged.tx_plan.hash_locations.len() as u64);
            staged.push(output.staged);
        }

        timings.stage_a_ms = stage_a_start.elapsed().as_millis() as u64;

        let phase_a_delta_start = Instant::now();
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
        timings.stage_a_delta_us = phase_a_delta_start.elapsed().as_micros() as u64;

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
        let stage_a_ms_cell = std::sync::atomic::AtomicU64::new(0);
        let dir_fragments_total_cell = std::sync::atomic::AtomicU64::new(0);
        let dir_fragments_written_cell = std::sync::atomic::AtomicU64::new(0);
        let bitmap_fragments_total_cell = std::sync::atomic::AtomicU64::new(0);
        let bitmap_fragments_written_cell = std::sync::atomic::AtomicU64::new(0);
        let projected_open_indexes = self
            .open_indexes
            .projected_with_delta(phase_a_delta.clone());
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
            let stage_a_ms_cell = &stage_a_ms_cell;
            let dir_fragments_total_cell = &dir_fragments_total_cell;
            let dir_fragments_written_cell = &dir_fragments_written_cell;
            let bitmap_fragments_total_cell = &bitmap_fragments_total_cell;
            let bitmap_fragments_written_cell = &bitmap_fragments_written_cell;
            Box::pin(async move {
                let stage_a_session_stage_start = Instant::now();
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
                    let log_stats = logs.stage_indexed_family_ingest(
                        w,
                        block.block_number(),
                        st.log_plan.block_log_blob.clone(),
                        st.log_plan.log_window,
                        &st.log_plan.bitmap_fragments,
                        log_filter,
                    )?;
                    let tx_stats = txs.stage_indexed_family_ingest(
                        w,
                        block.block_number(),
                        st.tx_plan.block_tx_blob.clone(),
                        st.tx_plan.tx_window,
                        &st.tx_plan.bitmap_fragments,
                        tx_filter,
                    )?;
                    let trace_stats = traces.stage_indexed_family_ingest(
                        w,
                        block.block_number(),
                        st.trace_plan.block_trace_blob.clone(),
                        st.trace_plan.trace_window,
                        &st.trace_plan.bitmap_fragments,
                        trace_filter,
                    )?;
                    for stats in [log_stats, tx_stats, trace_stats] {
                        dir_fragments_total_cell.fetch_add(
                            stats.dir_fragments_total,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        dir_fragments_written_cell.fetch_add(
                            stats.dir_fragments_written,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        bitmap_fragments_total_cell.fetch_add(
                            stats.bitmap_fragments_total,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        bitmap_fragments_written_cell.fetch_add(
                            stats.bitmap_fragments_written,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                    block_tables.stage_hash_index(
                        w,
                        &st.block_record.block_hash,
                        block.block_number(),
                    );
                    for (tx_hash, location) in &st.tx_plan.hash_locations {
                        w.tables().tx_hash_index().stage_put(w, tx_hash, *location);
                    }
                }
                stage_a_ms_cell.store(
                    stage_a_session_stage_start.elapsed().as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                Ok(())
            })
        });

        let stage_b_plan = async {
            let reads_start = Instant::now();
            let plans = futures::try_join!(
                logs.plan_directory_compactions(&projected_open_indexes, &log_ranges),
                logs.plan_bitmap_compactions(
                    &projected_open_indexes,
                    &log_touched_bitmap_per_block,
                    &log_ranges
                ),
                txs.plan_directory_compactions(&projected_open_indexes, &tx_ranges),
                txs.plan_bitmap_compactions(
                    &projected_open_indexes,
                    &tx_touched_bitmap_per_block,
                    &tx_ranges
                ),
                traces.plan_directory_compactions(&projected_open_indexes, &trace_ranges),
                traces.plan_bitmap_compactions(
                    &projected_open_indexes,
                    &trace_touched_bitmap_per_block,
                    &trace_ranges
                ),
            )?;
            Ok::<_, MonadChainDataError>((
                plans,
                reads_start.elapsed().as_millis() as u64,
                reads_start.elapsed().as_micros() as u64,
            ))
        };

        let (phase_a_writes_result, stage_b_plan_result) =
            futures::join!(phase_a_stage, stage_b_plan);
        let phase_a_writes = phase_a_writes_result?;
        timings.stage_a_session_stage_us =
            stage_a_ms_cell.load(std::sync::atomic::Ordering::Relaxed);
        timings.stage_a_dir_fragments_total =
            dir_fragments_total_cell.load(std::sync::atomic::Ordering::Relaxed);
        timings.stage_a_dir_fragments_written =
            dir_fragments_written_cell.load(std::sync::atomic::Ordering::Relaxed);
        timings.stage_a_dir_fragments_skipped = timings
            .stage_a_dir_fragments_total
            .saturating_sub(timings.stage_a_dir_fragments_written);
        timings.stage_a_bitmap_fragments_total =
            bitmap_fragments_total_cell.load(std::sync::atomic::Ordering::Relaxed);
        timings.stage_a_bitmap_fragments_written =
            bitmap_fragments_written_cell.load(std::sync::atomic::Ordering::Relaxed);
        timings.stage_a_bitmap_fragments_skipped = timings
            .stage_a_bitmap_fragments_total
            .saturating_sub(timings.stage_a_bitmap_fragments_written);
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
            reads_elapsed_us,
        ) = stage_b_plan_result?;
        let mut read_plan_timings = ReadPlanningTimings::default();
        read_plan_timings.merge(log_dir_plan.timings);
        read_plan_timings.merge(log_bitmap_plan.timings);
        read_plan_timings.merge(tx_dir_plan.timings);
        read_plan_timings.merge(tx_bitmap_plan.timings);
        read_plan_timings.merge(trace_dir_plan.timings);
        read_plan_timings.merge(trace_bitmap_plan.timings);
        timings.reads_ms = reads_elapsed_ms;
        timings.reads_dir_list_ms = read_plan_timings.dir_list_ms;
        timings.reads_dir_get_ms = read_plan_timings.dir_get_ms;
        timings.reads_dir_list_count = read_plan_timings.dir_list_count;
        timings.reads_dir_get_count = read_plan_timings.dir_get_count;
        timings.reads_bitmap_open_streams_ms = read_plan_timings.bitmap_open_streams_ms;
        timings.reads_bitmap_open_streams_us = read_plan_timings.bitmap_open_streams_us;
        timings.reads_bitmap_open_streams_count = read_plan_timings.bitmap_open_streams_count;
        timings.reads_bitmap_list_ms = read_plan_timings.bitmap_list_ms;
        timings.reads_bitmap_get_ms = read_plan_timings.bitmap_get_ms;
        timings.reads_bitmap_shape_ms = read_plan_timings.bitmap_shape_ms;
        timings.reads_bitmap_shape_us = read_plan_timings.bitmap_shape_us;
        timings.reads_bitmap_union_us = read_plan_timings.bitmap_union_us;
        timings.reads_bitmap_frontier_us = read_plan_timings.bitmap_frontier_us;
        timings.reads_bitmap_open_write_us = read_plan_timings.bitmap_open_write_us;
        timings.reads_bitmap_index_ms = read_plan_timings.bitmap_index_ms;
        timings.reads_bitmap_index_us = read_plan_timings.bitmap_index_us;
        timings.reads_bitmap_compact_ms = read_plan_timings.bitmap_compact_ms;
        timings.reads_bitmap_compact_us = read_plan_timings.bitmap_compact_us;
        timings.reads_bitmap_compact_wall_us = read_plan_timings.bitmap_compact_wall_us;
        timings.reads_bitmap_list_count = read_plan_timings.bitmap_list_count;
        timings.reads_bitmap_get_count = read_plan_timings.bitmap_get_count;
        timings.reads_bitmap_fragment_count = read_plan_timings.bitmap_fragment_count;
        timings.reads_bitmap_fragment_bytes = read_plan_timings.bitmap_fragment_bytes;
        timings.reads_bitmap_compact_count = read_plan_timings.bitmap_compact_count;
        timings.reads_bitmap_frontier_stream_count = read_plan_timings.bitmap_frontier_stream_count;
        timings.reads_bitmap_union_page_count = read_plan_timings.bitmap_union_page_count;
        timings.reads_bitmap_union_stream_count = read_plan_timings.bitmap_union_stream_count;
        timings.reads_bitmap_touched_page_count = read_plan_timings.bitmap_touched_page_count;
        timings.reads_bitmap_touched_stream_count = read_plan_timings.bitmap_touched_stream_count;
        timings.reads_bitmap_final_open_stream_count =
            read_plan_timings.bitmap_final_open_stream_count;
        timings.reads_dir_index_ms = read_plan_timings.dir_index_ms;
        timings.reads_dir_index_us = read_plan_timings.dir_index_us;
        timings.reads_dir_decode_ms = read_plan_timings.dir_decode_ms;
        timings.reads_dir_decode_us = read_plan_timings.dir_decode_us;
        timings.reads_dir_fragment_count = read_plan_timings.dir_fragment_count;
        let accounted_reads_ms = timings
            .reads_dir_list_ms
            .saturating_add(timings.reads_dir_get_ms)
            .saturating_add(timings.reads_dir_index_ms)
            .saturating_add(timings.reads_dir_decode_ms)
            .saturating_add(timings.reads_bitmap_open_streams_ms)
            .saturating_add(timings.reads_bitmap_list_ms)
            .saturating_add(timings.reads_bitmap_get_ms)
            .saturating_add(timings.reads_bitmap_shape_ms)
            .saturating_add(timings.reads_bitmap_index_ms)
            .saturating_add(timings.reads_bitmap_compact_wall_us / 1_000);
        timings.reads_unaccounted_ms = timings.reads_ms.saturating_sub(accounted_reads_ms);
        let accounted_reads_us = timings
            .reads_dir_index_us
            .saturating_add(timings.reads_dir_decode_us)
            .saturating_add(timings.reads_bitmap_open_streams_us)
            .saturating_add(timings.reads_bitmap_union_us)
            .saturating_add(timings.reads_bitmap_shape_us)
            .saturating_add(timings.reads_bitmap_index_us)
            .saturating_add(timings.reads_bitmap_compact_wall_us);
        timings.reads_unaccounted_us = reads_elapsed_us.saturating_sub(accounted_reads_us);

        let phase_b_has_writes = !log_dir_plan.buckets.is_empty()
            || log_bitmap_plan.has_writes
            || !tx_dir_plan.buckets.is_empty()
            || tx_bitmap_plan.has_writes
            || !trace_dir_plan.buckets.is_empty()
            || trace_bitmap_plan.has_writes;

        let last = staged.last().expect("at least one block staged");
        let new_head = last.block_record.block_number;
        let next_state = crate::primitives::state::PublicationState {
            indexed_finalized_head: new_head,
        };
        let publication = PublicationAdvance {
            table: PublicationTables::<M>::PUBLICATION_STATE_TABLE,
            key: PublicationTables::<M>::PUBLICATION_STATE_KEY.to_vec(),
            value: Bytes::from(next_state.encode()),
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
        timings
            .phase_b_write_counts
            .add_cas(publication.table, publication.value.len());
        combined_writes.add_cas_count(publication.table, publication.value.len());

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
                        std::thread::sleep(backoff);
                        backoff = backoff.saturating_mul(2).min(retry.max_backoff);
                    }
                }
            }
        };
        timings.commit_a_meta_ms = combined_timings.meta.as_millis() as u64;
        timings.commit_a_blob_ms = combined_timings.blob.as_millis() as u64;
        let expected = self
            .publication
            .load_state()
            .await?
            .map(|(version, _)| version);
        let cas_start = Instant::now();
        let outcome = self
            .tables
            .meta_store()
            .cas_put(
                publication.table,
                &publication.key,
                expected,
                publication.value,
            )
            .await?;
        timings.cas_ms = cas_start.elapsed().as_millis() as u64;
        let commit_elapsed_ms = commit_start.elapsed().as_millis() as u64;
        timings.commit_b_ms = commit_elapsed_ms
            .saturating_sub(timings.commit_a_meta_ms.max(timings.commit_a_blob_ms));
        self.publication.cas_outcome_into_result(outcome).await?;
        Ok((outcomes, timings))
    }

    /// Executes a finalized logs query over the current published head.
    /// The service's configured `QueryLimits` bound the request shape and
    /// the resolved block-range span.
    pub async fn query_logs(&self, request: QueryLogsRequest) -> Result<QueryLogsResponse> {
        let query_start = Instant::now();
        trace!(family = "logs", ?request, "chain data query started");
        self.limits.check_limit(request.envelope.limit)?;

        let head = self.load_published_head().await?;
        let window = ResolvedBlockWindow::resolve(
            &request.envelope,
            head,
            &self.limits,
            self.tables.blocks(),
        )
        .await?;

        let path = if request.filter.has_indexed_clause() {
            "indexed"
        } else {
            "block_scan"
        };
        trace!(
            family = "logs",
            path,
            published_head = head,
            from_block = window.low.number,
            to_block = window.high.number,
            block_span = window.high.number - window.low.number + 1,
            "chain data query window resolved"
        );

        let mut response = if path == "indexed" {
            execute_indexed_log_query(&self.tables, &request, window).await?
        } else {
            execute_block_scan_query(&self.tables, &request, window).await?
        };

        if request.relations.blocks {
            response.blocks = Some(
                load_blocks_by_numbers(
                    self.tables.blocks(),
                    response.logs.iter().map(|l| l.block_number),
                )
                .await?,
            );
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

        trace!(
            family = "logs",
            path,
            result_count = response.logs.len(),
            blocks_count = response.blocks.as_ref().map_or(0, Vec::len),
            transactions_count = response.transactions.as_ref().map_or(0, Vec::len),
            cursor_block = response.span.cursor_block.number,
            elapsed_ms = query_start.elapsed().as_millis() as u64,
            "chain data query completed"
        );
        Ok(response)
    }

    /// Executes a finalized transactions query over the current published
    /// head. The service's configured `QueryLimits` bound the request
    /// shape and the resolved block-range span.
    pub async fn query_transactions(
        &self,
        request: QueryTransactionsRequest,
    ) -> Result<QueryTransactionsResponse> {
        let query_start = Instant::now();
        trace!(
            family = "transactions",
            ?request,
            "chain data query started"
        );
        self.limits.check_limit(request.envelope.limit)?;

        let head = self.load_published_head().await?;
        let window = ResolvedBlockWindow::resolve(
            &request.envelope,
            head,
            &self.limits,
            self.tables.blocks(),
        )
        .await?;

        let path = if request.filter.has_indexed_clause() {
            "indexed"
        } else {
            "block_scan"
        };
        trace!(
            family = "transactions",
            path,
            published_head = head,
            from_block = window.low.number,
            to_block = window.high.number,
            block_span = window.high.number - window.low.number + 1,
            "chain data query window resolved"
        );

        let mut response = if path == "indexed" {
            execute_indexed_tx_query(&self.tables, &request, window).await?
        } else {
            execute_block_scan_tx_query(&self.tables, &request, window).await?
        };

        if request.relations.blocks {
            response.blocks = Some(
                load_blocks_by_numbers(
                    self.tables.blocks(),
                    response.txs.iter().map(|t| t.block_number),
                )
                .await?,
            );
        }

        trace!(
            family = "transactions",
            path,
            result_count = response.txs.len(),
            blocks_count = response.blocks.as_ref().map_or(0, Vec::len),
            cursor_block = response.span.cursor_block.number,
            elapsed_ms = query_start.elapsed().as_millis() as u64,
            "chain data query completed"
        );
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
        let query_start = Instant::now();
        trace!(family = "traces", ?request, "chain data query started");
        self.limits.check_limit(request.envelope.limit)?;

        let head = self.load_published_head().await?;
        let window = ResolvedBlockWindow::resolve(
            &request.envelope,
            head,
            &self.limits,
            self.tables.blocks(),
        )
        .await?;

        let path = if request.filter.has_indexed_clause() {
            "indexed"
        } else {
            "block_scan"
        };
        trace!(
            family = "traces",
            path,
            published_head = head,
            from_block = window.low.number,
            to_block = window.high.number,
            block_span = window.high.number - window.low.number + 1,
            "chain data query window resolved"
        );

        let mut response = if path == "indexed" {
            execute_indexed_trace_query(&self.tables, &request, window).await?
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

        trace!(
            family = "traces",
            path,
            result_count = response.traces.len(),
            blocks_count = response.blocks.as_ref().map_or(0, Vec::len),
            transactions_count = response.transactions.as_ref().map_or(0, Vec::len),
            cursor_block = response.span.cursor_block.number,
            elapsed_ms = query_start.elapsed().as_millis() as u64,
            "chain data query completed"
        );
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
        let query_start = Instant::now();
        trace!(family = "transfers", ?request, "chain data query started");
        self.limits.check_limit(request.envelope.limit)?;

        let head = self.load_published_head().await?;
        let window = ResolvedBlockWindow::resolve(
            &request.envelope,
            head,
            &self.limits,
            self.tables.blocks(),
        )
        .await?;

        let path = if request.filter.has_indexed_clause() {
            "indexed"
        } else {
            "block_scan"
        };
        trace!(
            family = "transfers",
            path,
            published_head = head,
            from_block = window.low.number,
            to_block = window.high.number,
            block_span = window.high.number - window.low.number + 1,
            "chain data query window resolved"
        );

        let mut response = if path == "indexed" {
            execute_indexed_transfer_query(&self.tables, &request, window).await?
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

        trace!(
            family = "transfers",
            path,
            result_count = response.transfers.len(),
            blocks_count = response.blocks.as_ref().map_or(0, Vec::len),
            transactions_count = response.transactions.as_ref().map_or(0, Vec::len),
            cursor_block = response.span.cursor_block.number,
            elapsed_ms = query_start.elapsed().as_millis() as u64,
            "chain data query completed"
        );
        Ok(response)
    }

    /// Executes a finalized blocks query over the current published head.
    /// The service's configured `QueryLimits` bound the request shape and
    /// the resolved block-range span.
    pub async fn query_blocks(&self, request: QueryBlocksRequest) -> Result<QueryBlocksResponse> {
        let query_start = Instant::now();
        trace!(family = "blocks", ?request, "chain data query started");
        self.limits.check_limit(request.envelope.limit)?;

        let head = self.load_published_head().await?;
        let window = ResolvedBlockWindow::resolve(
            &request.envelope,
            head,
            &self.limits,
            self.tables.blocks(),
        )
        .await?;

        trace!(
            family = "blocks",
            path = "block_range",
            published_head = head,
            from_block = window.low.number,
            to_block = window.high.number,
            block_span = window.high.number - window.low.number + 1,
            "chain data query window resolved"
        );

        let response = execute_query_blocks(&self.tables, &request, window).await?;
        trace!(
            family = "blocks",
            path = "block_range",
            result_count = response.blocks.len(),
            cursor_block = response.span.cursor_block.number,
            elapsed_ms = query_start.elapsed().as_millis() as u64,
            "chain data query completed"
        );
        Ok(response)
    }

    async fn load_published_head(&self) -> Result<u64> {
        self.publication
            .load_published_head()
            .await?
            .ok_or(MonadChainDataError::MissingData("no published blocks"))
    }
}
