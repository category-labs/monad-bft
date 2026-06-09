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

use crate::{
    blocks::{
        execute_query_blocks, load_blocks_by_numbers, QueryBlocksRequest, QueryBlocksResponse,
    },
    engine::{
        query::family_runner::IndexedFamilyQuery,
        tables::{DictConfig, PublicationTables, QueryRuntimeConfig, Tables},
    },
    error::{MonadChainDataError, Result},
    family::Hash32,
    logs::{
        execute_block_scan_query, execute_indexed_log_query, QueryLogsRequest, QueryLogsResponse,
    },
    primitives::{limits::QueryLimits, range::ResolvedBlockWindow},
    store::{BlobStore, CacheConfig, MetaStore},
    traces::{
        execute_block_scan_trace_query, execute_indexed_trace_query, QueryTracesRequest,
        QueryTracesResponse,
    },
    transfers::{
        execute_block_scan_transfer_query, execute_indexed_transfer_query, QueryTransfersRequest,
        QueryTransfersResponse,
    },
    txs::{
        execute_block_scan_tx_query, execute_indexed_tx_query, load_txs_by_positions,
        QueryTransactionsRequest, QueryTransactionsResponse, TxEntry, TxMaterializer,
    },
};

/// Read-only query layer over the chain-data store. The branchless ingest engine
/// (`ingest_core`/`backfill`/`live`) is the only write path; this service reads
/// the published head observationally and never writes to the stores.
pub struct MonadChainDataService<M: MetaStore, B: BlobStore> {
    tables: Tables<M, B>,
    publication: PublicationTables<M>,
    limits: QueryLimits,
}

impl<M: MetaStore, B: BlobStore> MonadChainDataService<M, B> {
    /// Builds a read-only query/verify service over the given stores.
    pub fn new(meta_store: M, blob_store: B, limits: QueryLimits) -> Self {
        Self::with_cache_config(meta_store, blob_store, limits, CacheConfig::default())
    }

    /// [`Self::new`] threading an operator-tuned [`CacheConfig`].
    pub fn with_cache_config(
        meta_store: M,
        blob_store: B,
        limits: QueryLimits,
        cache: CacheConfig,
    ) -> Self {
        Self::with_configs(meta_store, blob_store, limits, cache, DictConfig::default())
    }

    /// [`Self::with_cache_config`] also threading a [`DictConfig`], letting tests
    /// run the epoch-based dictionary lifecycle over a handful of blocks (e.g.
    /// `epoch_blocks = 8`).
    /// [`Self::with_cache_config`] also threading a [`DictConfig`], letting tests
    /// run the epoch-based dictionary lifecycle over a handful of blocks (e.g.
    /// `epoch_blocks = 8`). Uses default [`QueryRuntimeConfig`].
    pub fn with_configs(
        meta_store: M,
        blob_store: B,
        limits: QueryLimits,
        cache: CacheConfig,
        dict_config: DictConfig,
    ) -> Self {
        Self::with_all_configs(
            meta_store,
            blob_store,
            limits,
            cache,
            dict_config,
            QueryRuntimeConfig::default(),
        )
    }

    /// Full constructor also threading operator-tuned [`QueryRuntimeConfig`]
    /// read-path limits (fan-out + budgets). Used by the reader-open path.
    pub fn with_all_configs(
        meta_store: M,
        blob_store: B,
        limits: QueryLimits,
        cache: CacheConfig,
        dict_config: DictConfig,
        query: QueryRuntimeConfig,
    ) -> Self {
        Self {
            publication: PublicationTables::new(meta_store.clone()),
            tables: Tables::with_all_configs(meta_store, blob_store, cache, dict_config, query),
            limits,
        }
    }

    /// Alias for [`Self::with_cache_config`]. The service is read-only, so this
    /// is retained only for call-site clarity at reader construction points.
    pub fn new_reader_only(
        meta_store: M,
        blob_store: B,
        limits: QueryLimits,
        cache: CacheConfig,
    ) -> Self {
        Self::with_cache_config(meta_store, blob_store, limits, cache)
    }

    /// [`Self::new_reader_only`] threading operator-tuned read-path limits.
    pub fn new_reader_with_query_config(
        meta_store: M,
        blob_store: B,
        limits: QueryLimits,
        cache: CacheConfig,
        query: QueryRuntimeConfig,
    ) -> Self {
        Self::with_all_configs(
            meta_store,
            blob_store,
            limits,
            cache,
            DictConfig::default(),
            query,
        )
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
            .load_record_at(
                location.block_number,
                location.tx_idx as usize,
                &crate::engine::query::family_runner::IndexedQueryStats::default(),
            )
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
