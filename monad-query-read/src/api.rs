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
        execute_query_blocks, load_blocks_by_numbers, Block, QueryBlocksRequest,
        QueryBlocksResponse,
    },
    engine::{
        digest::ChainDigest,
        family::PerFamily,
        query::family_runner::{
            run_family_query, IndexedFamilyQuery, IndexedQueryOutcome, IndexedQueryStats,
        },
        seal::{last_sealed_span, seal_boundary},
        tables::{DictConfig, PublicationTables, QueryRuntimeConfig, Tables},
    },
    error::{MonadChainDataError, Result},
    ingest_types::Hash32,
    logs::{LogMaterializer, QueryLogsRequest, QueryLogsResponse},
    primitives::{
        limits::{QueryEnvelope, QueryLimits},
        range::ResolvedBlockWindow,
        EvmBlockHeader,
    },
    store::{BlobStore, CacheConfig, MetaStore},
    traces::{QueryTracesRequest, QueryTracesResponse, TraceMaterializer},
    transfers::{QueryTransfersRequest, QueryTransfersResponse, TransferMaterializer},
    txs::{
        load_txs_by_positions, QueryTransactionsRequest, QueryTransactionsResponse, TxEntry,
        TxMaterializer,
    },
};

/// One family's seal-chain reading: the chained seal digest persisted when
/// `span_start` (the family's most recently sealed 64K id span) sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealChainPoint {
    pub span_start: u64,
    pub value: ChainDigest,
}

/// The standby verification digests at one block height: the `row_chain`
/// plus each family's last sealed span + seal chain.
/// Two replicas that ingested the same finalized stream must agree on every
/// field — see the recipe in [`crate::engine::digest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandbyDigests {
    pub block_number: u64,
    /// Chained per-block content digest through this block (row chain).
    pub row_chain: ChainDigest,
    /// Per-family seal chain at the last span sealed within this block's id
    /// frontier; `None` when nothing has sealed yet.
    pub seal_chains: PerFamily<Option<SealChainPoint>>,
}

/// Read-only query layer over the chain-data store. The branchless ingest engine
/// (`monad-query-write`) is the only write path; this service reads the published
/// head observationally and never writes to the stores.
pub struct MonadChainDataService<M: MetaStore, B: BlobStore> {
    tables: Tables<M, B>,
    publication: PublicationTables<M>,
    limits: QueryLimits,
}

impl<M: MetaStore, B: BlobStore> MonadChainDataService<M, B> {
    /// Builds a read-only query/verify service over the given stores with
    /// default configs; see [`Self::with_all_configs`].
    pub fn new(meta_store: M, blob_store: B, limits: QueryLimits) -> Self {
        Self::with_all_configs(
            meta_store,
            blob_store,
            limits,
            CacheConfig::default(),
            DictConfig::default(),
            QueryRuntimeConfig::default(),
        )
    }

    /// Full constructor threading every operator-tuned config: [`CacheConfig`]
    /// sizes, the [`DictConfig`] lifecycle (e.g. a small `epoch_blocks` for
    /// dictionary tests), and [`QueryRuntimeConfig`] read-path limits.
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

    /// Attaches the external archive reader serving `ENCODING_EXTERNAL_V1`
    /// payload reads (see [`crate::external`]); required to query a store
    /// ingested in external payload mode.
    pub fn with_external_payload_reader(
        mut self,
        reader: std::sync::Arc<dyn crate::external::ExternalBlobReader>,
    ) -> Self {
        self.tables = self.tables.with_external_payload_reader(reader);
        self
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

    /// Shared query prelude: checks `QueryLimits`, loads the published head,
    /// and resolves the envelope to a block window.
    async fn resolve_head_and_window(
        &self,
        envelope: &QueryEnvelope,
    ) -> Result<(u64, ResolvedBlockWindow)> {
        self.limits.check_limit(envelope.limit)?;
        let head = self.require_published_head().await?;
        let window =
            ResolvedBlockWindow::resolve(envelope, head, &self.limits, self.tables.blocks())
                .await?;
        Ok((head, window))
    }

    /// Shared query body: resolve the head/window prelude, then run the
    /// family's indexed/scan dispatch over it. Each public `query_*` wraps this
    /// with its own relation joins + response construction.
    async fn run_query<R: IndexedFamilyQuery<Meta = M, Blob = B>>(
        &self,
        runner: &R,
        filter: &R::Filter,
        envelope: &QueryEnvelope,
    ) -> Result<IndexedQueryOutcome<R::Record>> {
        let (head, window) = self.resolve_head_and_window(envelope).await?;
        run_family_query(runner, filter, window, head, envelope.order, envelope.limit).await
    }

    /// Joins the opt-in `blocks` / `transactions` relations for a page of
    /// records, given each record's `(block_number, tx_index)` position.
    async fn join_relations<I>(
        &self,
        want_blocks: bool,
        want_transactions: bool,
        positions: I,
    ) -> Result<(Option<Vec<Block>>, Option<Vec<TxEntry>>)>
    where
        I: Iterator<Item = (u64, u32)> + Clone,
    {
        let concurrency = self.tables.query_config().materialize_concurrency;
        let load_blocks = async {
            match want_blocks {
                true => load_blocks_by_numbers(
                    self.tables.blocks(),
                    positions.clone().map(|(block_number, _)| block_number),
                    concurrency,
                )
                .await
                .map(Some),
                false => Ok(None),
            }
        };
        let load_txs = async {
            match want_transactions {
                true => load_txs_by_positions(&self.tables, positions.clone())
                    .await
                    .map(Some),
                false => Ok(None),
            }
        };
        futures::try_join!(load_blocks, load_txs)
    }

    /// Executes a finalized logs query over the current published head,
    /// bounded by the configured `QueryLimits`.
    pub async fn query_logs(&self, request: QueryLogsRequest) -> Result<QueryLogsResponse> {
        let outcome = self
            .run_query(
                &LogMaterializer::new(&self.tables),
                &request.filter,
                &request.envelope,
            )
            .await?;

        let (blocks, transactions) = self
            .join_relations(
                request.relations.blocks,
                request.relations.transactions,
                outcome.records.iter().map(|l| (l.block_number, l.tx_index)),
            )
            .await?;

        Ok(QueryLogsResponse {
            logs: outcome.records,
            blocks,
            transactions,
            span: outcome.span,
        })
    }

    /// Executes a finalized transactions query over the current published
    /// head, bounded by the configured `QueryLimits`.
    pub async fn query_transactions(
        &self,
        request: QueryTransactionsRequest,
    ) -> Result<QueryTransactionsResponse> {
        let outcome = self
            .run_query(
                &TxMaterializer::new(&self.tables),
                &request.filter,
                &request.envelope,
            )
            .await?;

        // `TxsRelations` has no `transactions` relation: the records are txs.
        let blocks = if request.relations.blocks {
            Some(
                load_blocks_by_numbers(
                    self.tables.blocks(),
                    outcome.records.iter().map(|t| t.block_number),
                    self.tables.query_config().materialize_concurrency,
                )
                .await?,
            )
        } else {
            None
        };

        Ok(QueryTransactionsResponse {
            txs: outcome.records,
            blocks,
            span: outcome.span,
        })
    }

    /// Resolves a finalized transaction by hash, paired with its block's
    /// header (the context `eth_getTransactionByHash` responses need:
    /// timestamp and base fee); `None` if never indexed. An index hit that
    /// fails to materialize inside the published range is a data-layer
    /// inconsistency and surfaces as `MissingData`, not `None`.
    pub async fn get_transaction(
        &self,
        tx_hash: Hash32,
    ) -> Result<Option<(TxEntry, EvmBlockHeader)>> {
        let Some(location) = self.tables.tx_hash_index().get(&tx_hash).await? else {
            return Ok(None);
        };
        let Some(head) = self.publication.queryable_head().await? else {
            return Ok(None);
        };
        if location.block_number > head {
            return Ok(None);
        }

        let materializer = TxMaterializer::new(&self.tables);
        let entry = materializer
            .load_record_at(
                location.block_number,
                location.tx_index as usize,
                &IndexedQueryStats::default(),
            )
            .await?;
        let header = self
            .tables
            .blocks()
            .load_header(location.block_number)
            .await?
            .ok_or(MonadChainDataError::MissingData(
                "missing header for indexed transaction",
            ))?;
        Ok(Some((entry, header)))
    }

    /// Executes a finalized traces query over the current published head,
    /// bounded by the configured `QueryLimits`.
    pub async fn query_traces(&self, request: QueryTracesRequest) -> Result<QueryTracesResponse> {
        let outcome = self
            .run_query(
                &TraceMaterializer::new(&self.tables),
                &request.filter,
                &request.envelope,
            )
            .await?;

        let (blocks, transactions) = self
            .join_relations(
                request.relations.blocks,
                request.relations.transactions,
                outcome.records.iter().map(|t| (t.block_number, t.tx_index)),
            )
            .await?;

        Ok(QueryTracesResponse {
            traces: outcome.records,
            blocks,
            transactions,
            span: outcome.span,
        })
    }

    /// Executes a finalized transfers query. Transfers are a view over the
    /// trace family gated by the indexed `has_transfer` column, with the same
    /// optional `blocks`/`transactions` relations as the traces query.
    pub async fn query_transfers(
        &self,
        request: QueryTransfersRequest,
    ) -> Result<QueryTransfersResponse> {
        // Always routes indexed; see `TransferMaterializer::decode_scan_record`.
        let outcome = self
            .run_query(
                &TransferMaterializer::new(&self.tables),
                &request.filter,
                &request.envelope,
            )
            .await?;

        let (blocks, transactions) = self
            .join_relations(
                request.relations.blocks,
                request.relations.transactions,
                outcome.records.iter().map(|t| (t.block_number, t.tx_index)),
            )
            .await?;

        Ok(QueryTransfersResponse {
            transfers: outcome.records,
            blocks,
            transactions,
            span: outcome.span,
        })
    }

    /// Executes a finalized blocks query over the current published head,
    /// bounded by the configured `QueryLimits`.
    pub async fn query_blocks(&self, request: QueryBlocksRequest) -> Result<QueryBlocksResponse> {
        let (_head, window) = self.resolve_head_and_window(&request.envelope).await?;

        execute_query_blocks(&self.tables, &request, window).await
    }

    /// Standby verification probe: block `block_number`'s chained
    /// `row_chain` plus each family's `(last sealed span, seal chain)`
    /// derived from that block's id frontier. `None` when the block's record
    /// is absent.
    pub async fn standby_digests(&self, block_number: u64) -> Result<Option<StandbyDigests>> {
        let Some(record) = self.tables.blocks().load_record(block_number).await? else {
            return Ok(None);
        };
        let mut seal_chains = PerFamily::default();
        for (family, slot) in seal_chains.iter_mut() {
            let bound = family
                .window_in(&record)
                .next_primary_id_exclusive()?
                .as_u64();
            *slot = match last_sealed_span(seal_boundary(bound)) {
                Some(span_start) => self
                    .tables
                    .family(family)
                    .load_seal_chain(span_start)
                    .await?
                    .map(|value| SealChainPoint { span_start, value }),
                None => None,
            };
        }
        Ok(Some(StandbyDigests {
            block_number,
            row_chain: record.row_chain,
            seal_chains,
        }))
    }

    async fn require_published_head(&self) -> Result<u64> {
        // Range queries cannot resolve against a head-less store, so the
        // not-yet-queryable state errors instead of mapping to `None`.
        self.publication
            .queryable_head()
            .await?
            .ok_or(MonadChainDataError::MissingData("no published blocks"))
    }
}
