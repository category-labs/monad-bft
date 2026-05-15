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

use std::time::Instant;

use bytes::Bytes;

use crate::{
    blocks::{
        execute_query_blocks, load_blocks_by_numbers, QueryBlocksRequest, QueryBlocksResponse,
    },
    engine::{
        family::Family,
        query::family_runner::IndexedFamilyQuery,
        tables::{PublicationTables, Tables},
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
    store::{BlobStore, CasOutcome, MetaStoreCas, PublicationCasParams},
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
}

struct StagedBlock {
    log_plan: LogIngestPlan,
    tx_plan: TxIngestPlan,
    trace_plan: TraceIngestPlan,
    block_record: BlockRecord,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestOutcome {
    pub indexed_finalized_head: u64,
    pub block_record: BlockRecord,
    pub written_logs: usize,
    pub written_txs: usize,
    pub written_traces: usize,
}

/// Per-batch timing breakdown emitted by [`MonadChainDataService::ingest_blocks`].
/// All durations are milliseconds. `commit_b_ms` and `cas_ms` are mutually
/// exclusive: when Phase B is empty (`phase_b_skipped = true`) the head
/// advance runs through a standalone CAS and `commit_b_ms == 0`; otherwise
/// the CAS is folded into the Phase B commit and `cas_ms == 0`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestBatchTimings {
    pub stage_a_ms: u64,
    pub commit_a_meta_ms: u64,
    pub commit_a_blob_ms: u64,
    pub reads_ms: u64,
    pub stage_b_ms: u64,
    pub commit_b_ms: u64,
    pub cas_ms: u64,
    pub phase_b_skipped: bool,
    pub blocks: usize,
}

impl<M: MetaStoreCas, B: BlobStore> MonadChainDataService<M, B> {
    pub fn new(meta_store: M, blob_store: B, limits: QueryLimits) -> Self {
        Self {
            publication: PublicationTables::new(meta_store.clone()),
            tables: Tables::new(meta_store, blob_store),
            limits,
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
        debug_assert_eq!(out.len(), 1, "ingest_blocks(vec![one]) returns exactly one outcome");
        Ok(out.pop().expect("ingest_blocks contract: one outcome per input block"))
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
        if blocks.is_empty() {
            return Ok((Vec::new(), IngestBatchTimings::default()));
        }

        let mut timings = IngestBatchTimings {
            blocks: blocks.len(),
            ..IngestBatchTimings::default()
        };

        let block_tables = self.tables.blocks();
        let logs = self.tables.family(Family::Log);
        let txs = self.tables.family(Family::Tx);
        let traces = self.tables.family(Family::Trace);

        let current_state = self.publication.load_state().await?;
        let (expected_version, current_head) = match &current_state {
            Some((version, state)) => (Some(*version), Some(state.indexed_finalized_head)),
            None => (None, None),
        };
        let first_previous = block_tables
            .validate_continuity(&blocks[0], current_head)
            .await?;

        let stage_a_start = Instant::now();
        let mut staged: Vec<StagedBlock> = Vec::with_capacity(blocks.len());
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

            let log_plan = LogIngestPlan::build(block, next_log_id)?;
            let tx_plan = TxIngestPlan::build(block, next_tx_id)?;
            let trace_plan = TraceIngestPlan::build(block, next_trace_id)?;

            let block_record = BlockRecord {
                block_number: block.block_number(),
                block_hash: block.block_hash(),
                parent_hash: block.parent_hash(),
                logs: log_plan.log_window,
                txs: tx_plan.tx_window,
                traces: trace_plan.trace_window,
            };

            prev_hash = block_record.block_hash;
            prev_record = Some(block_record.clone());
            staged.push(StagedBlock {
                log_plan,
                tx_plan,
                trace_plan,
                block_record,
            });
        }

        timings.stage_a_ms = stage_a_start.elapsed().as_millis() as u64;

        // Phase A: stage every block's artifacts inside one write session so
        // the framework can fire meta + blob commits concurrently while
        // populating per-table caches for the Phase B compaction reads.
        let blocks_ref = &blocks;
        let staged_ref = &staged;
        let (phase_a_result, phase_a_timings) = self
            .tables
            .with_writes_timed(|w| {
                Box::pin(async move {
                    for (block, st) in blocks_ref.iter().zip(staged_ref.iter()) {
                        block_tables.stage_header(w, block.block_number(), &block.header);
                        logs.stage_indexed_family_ingest(
                            w,
                            block.block_number(),
                            st.log_plan.block_log_blob.clone(),
                            Bytes::from(st.log_plan.block_log_header.encode()),
                            st.log_plan.log_window,
                            &st.log_plan.bitmap_fragments,
                        );
                        txs.stage_indexed_family_ingest(
                            w,
                            block.block_number(),
                            st.tx_plan.block_tx_blob.clone(),
                            Bytes::from(st.tx_plan.block_tx_header.encode()),
                            st.tx_plan.tx_window,
                            &st.tx_plan.bitmap_fragments,
                        );
                        traces.stage_indexed_family_ingest(
                            w,
                            block.block_number(),
                            st.trace_plan.block_trace_blob.clone(),
                            Bytes::from(st.trace_plan.block_trace_header.encode()),
                            st.trace_plan.trace_window,
                            &st.trace_plan.bitmap_fragments,
                        );
                        block_tables.stage_hash_index(
                            w,
                            &st.block_record.block_hash,
                            block.block_number(),
                        );
                        for (tx_hash, location) in &st.tx_plan.hash_locations {
                            w.tables().tx_hash_index().stage_put(w, tx_hash, *location);
                        }
                        block_tables.stage_record(w, block.block_number(), &st.block_record);
                    }
                    Ok(())
                })
            })
            .await;
        phase_a_result?;
        timings.commit_a_meta_ms = phase_a_timings.meta.as_millis() as u64;
        timings.commit_a_blob_ms = phase_a_timings.blob.as_millis() as u64;

        let reads_start = Instant::now();
        let log_ranges = family_ranges(&staged, |s| s.log_plan.log_window);
        let tx_ranges = family_ranges(&staged, |s| s.tx_plan.tx_window);
        let trace_ranges = family_ranges(&staged, |s| s.trace_plan.trace_window);

        let log_bitmap_per_block: Vec<&[_]> = staged
            .iter()
            .map(|s| s.log_plan.bitmap_fragments.as_slice())
            .collect();
        let tx_bitmap_per_block: Vec<&[_]> = staged
            .iter()
            .map(|s| s.tx_plan.bitmap_fragments.as_slice())
            .collect();
        let trace_bitmap_per_block: Vec<&[_]> = staged
            .iter()
            .map(|s| s.trace_plan.bitmap_fragments.as_slice())
            .collect();

        let (
            log_dir_plan,
            log_bitmap_plan,
            tx_dir_plan,
            tx_bitmap_plan,
            trace_dir_plan,
            trace_bitmap_plan,
        ) = futures::try_join!(
            logs.plan_directory_compactions(&log_ranges),
            logs.plan_bitmap_compactions(&log_bitmap_per_block, &log_ranges),
            txs.plan_directory_compactions(&tx_ranges),
            txs.plan_bitmap_compactions(&tx_bitmap_per_block, &tx_ranges),
            traces.plan_directory_compactions(&trace_ranges),
            traces.plan_bitmap_compactions(&trace_bitmap_per_block, &trace_ranges),
        )?;
        timings.reads_ms = reads_start.elapsed().as_millis() as u64;

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

        if phase_b_has_writes {
            timings.phase_b_skipped = false;
            let cas = PublicationCasParams {
                table: PublicationTables::<M>::PUBLICATION_STATE_TABLE,
                key: PublicationTables::<M>::PUBLICATION_STATE_KEY.to_vec(),
                expected: expected_version,
                value: Bytes::from(next_state.encode()),
            };
            let log_dir = &log_dir_plan;
            let log_bitmap = &log_bitmap_plan;
            let tx_dir = &tx_dir_plan;
            let tx_bitmap = &tx_bitmap_plan;
            let trace_dir = &trace_dir_plan;
            let trace_bitmap = &trace_bitmap_plan;
            // `stage_b_ms` is the synchronous closure body — pure staging
            // into the session. `commit_b_ms` is the meta apply_writes_with_cas
            // + blob apply_writes that the framework fires after the closure
            // returns. Carve them apart inside the closure so the on-wire
            // timing fields keep the same meaning across the rewrite.
            let stage_b_ms_cell = std::sync::Mutex::new(0u64);
            let commit_b_start = Instant::now();
            let outcome: CasOutcome = self
                .tables
                .with_writes_and_cas(cas, |w| {
                    let stage_b_ms_cell = &stage_b_ms_cell;
                    Box::pin(async move {
                        let stage_b_start = Instant::now();
                        logs.stage_directory_compactions(w, log_dir);
                        logs.stage_bitmap_compactions(w, log_bitmap);
                        txs.stage_directory_compactions(w, tx_dir);
                        txs.stage_bitmap_compactions(w, tx_bitmap);
                        traces.stage_directory_compactions(w, trace_dir);
                        traces.stage_bitmap_compactions(w, trace_bitmap);
                        *stage_b_ms_cell.lock().expect("stage_b timing poisoned") =
                            stage_b_start.elapsed().as_millis() as u64;
                        Ok(())
                    })
                })
                .await?;
            let total_b_ms = commit_b_start.elapsed().as_millis() as u64;
            timings.stage_b_ms = *stage_b_ms_cell.lock().expect("stage_b timing poisoned");
            timings.commit_b_ms = total_b_ms.saturating_sub(timings.stage_b_ms);
            self.publication.cas_outcome_into_result(outcome).await?;
        } else {
            timings.phase_b_skipped = true;
            let cas_start = Instant::now();
            self.publication
                .cas_advance(expected_version, next_state)
                .await?;
            timings.cas_ms = cas_start.elapsed().as_millis() as u64;
        }

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
        Ok((outcomes, timings))
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
    pub async fn query_traces(
        &self,
        request: QueryTracesRequest,
    ) -> Result<QueryTracesResponse> {
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
        self.publication
            .load_published_head()
            .await?
            .ok_or(MonadChainDataError::MissingData("no published blocks"))
    }
}
