// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::sync::Arc;

#[cfg(any(feature = "dynamo", feature = "s3"))]
use eyre::Context;
use eyre::{bail, Result};
use tracing::{info, warn};

use crate::{
    blocks::{QueryBlocksRequest, QueryBlocksResponse},
    engine::{
        authority::HeadPublisher,
        tables::{PublicationTables, QueryRuntimeConfig, Tables},
    },
    ingest_controller::{run_ingest_controller, IngestRunConfig},
    ingest_core::{Prefetch, SnapshotStore},
    ingest_helpers::seed_snapshot_at,
    ingest_resolver::TablesCodecResolver,
    ingest_source::ChainDataIngestSource,
    logs::{QueryLogsRequest, QueryLogsResponse},
    primitives::state::BlockRecord,
    store::{BlobStore, CacheConfig, CacheField, MetaStore},
    traces::{QueryTracesRequest, QueryTracesResponse},
    transfers::{QueryTransfersRequest, QueryTransfersResponse},
    txs::{QueryTransactionsRequest, QueryTransactionsResponse},
    Hash32, QueryLimits,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataStoreConfig {
    pub meta: ChainDataMetaBackendConfig,
    pub blob: ChainDataBlobBackendConfig,
    /// Deprecated compatibility alias for `[cache].total_mib`.
    pub cache_mib: Option<usize>,
    /// Deprecated compatibility alias for `[cache].block_region_max_mib`.
    pub block_region_max_mib: Option<usize>,
    pub cache: ChainDataCacheConfig,
    /// Reader-side query-runtime limits (fan-out + decode budget).
    pub query: ChainDataQueryConfig,
    pub reader_only: bool,
}

impl Default for ChainDataStoreConfig {
    fn default() -> Self {
        Self {
            meta: ChainDataMetaBackendConfig::default(),
            blob: ChainDataBlobBackendConfig::default(),
            cache_mib: None,
            block_region_max_mib: None,
            cache: ChainDataCacheConfig::default(),
            query: ChainDataQueryConfig::default(),
            reader_only: false,
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataCacheConfig {
    /// Total cache budget in MiB. If absent, the caller's mode default applies:
    /// ingest resolves to 0 MiB, reader resolves to 2048 MiB.
    pub total_mib: Option<usize>,
    /// Per-region admission cap in MiB for the block-region cache. This is a
    /// cap, not part of the total budget.
    pub block_region_max_mib: Option<usize>,
    /// Optional per-table budget overrides in MiB. Unspecified tables receive
    /// their budget from the default ratio of the total budget.
    pub tables: ChainDataCacheTableBudgets,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataCacheTableBudgets {
    pub dir_by_block_mib: Option<usize>,
    pub dir_bucket_mib: Option<usize>,
    pub bitmap_by_block_mib: Option<usize>,
    pub bitmap_page_blob_mib: Option<usize>,
    pub bitmap_page_counts_mib: Option<usize>,
    pub open_bitmap_stream_mib: Option<usize>,
    pub block_header_mib: Option<usize>,
    pub block_hash_to_number_mib: Option<usize>,
    pub tx_hash_index_mib: Option<usize>,
    pub block_region_mib: Option<usize>,
}

/// Reader-side query-runtime limits (fan-out + decode budget). Each field
/// overrides the corresponding [`QueryRuntimeConfig`] default when set; absent
/// fields keep the default. Raise `blob_io_concurrency` /
/// `materialize_concurrency` for high-latency backends (S3/Dynamo) so sparse
/// queries fan their block reads toward a single round-trip; a fast local
/// backend is better served by the modest defaults.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataQueryConfig {
    pub blob_io_concurrency: Option<usize>,
    pub materialize_concurrency: Option<usize>,
    pub materialize_budget_permits: Option<usize>,
    /// Decode-budget permit unit, in KiB.
    pub materialize_permit_kib: Option<usize>,
}

impl ChainDataQueryConfig {
    fn to_runtime(&self) -> QueryRuntimeConfig {
        let d = QueryRuntimeConfig::default();
        QueryRuntimeConfig {
            blob_io_concurrency: self.blob_io_concurrency.unwrap_or(d.blob_io_concurrency),
            materialize_concurrency: self
                .materialize_concurrency
                .unwrap_or(d.materialize_concurrency),
            materialize_budget_permits: self
                .materialize_budget_permits
                .unwrap_or(d.materialize_budget_permits),
            materialize_permit_bytes: self
                .materialize_permit_kib
                .map(|kib| kib * 1024)
                .unwrap_or(d.materialize_permit_bytes),
        }
    }
}

impl ChainDataStoreConfig {
    pub fn validate(&self) -> Result<()> {
        if self.reader_only {
            bail!("chain-data embedded ingest cannot run with reader_only=true");
        }
        self.validate_backends()?;
        Ok(())
    }

    pub fn validate_reader(&self) -> Result<()> {
        self.validate_backends()
    }

    fn validate_backends(&self) -> Result<()> {
        self.meta.validate()?;
        self.blob.validate()?;
        Ok(())
    }
}

#[derive(Clone)]
pub enum ConfiguredChainDataReader {
    #[cfg(not(feature = "dynamo"))]
    Unavailable,
    #[cfg(all(feature = "dynamo", feature = "s3"))]
    DynamoS3(
        Arc<crate::MonadChainDataService<crate::store::DynamoMetaStore, crate::store::S3BlobStore>>,
    ),
    #[cfg(feature = "dynamo")]
    DynamoDynamo(
        Arc<
            crate::MonadChainDataService<
                crate::store::DynamoMetaStore,
                crate::store::DynamoBlobStore,
            >,
        >,
    ),
}

macro_rules! with_reader {
    ($self:expr, $service:ident => $body:expr) => {
        match $self {
            #[cfg(all(feature = "dynamo", feature = "s3"))]
            Self::DynamoS3($service) => $body,
            #[cfg(feature = "dynamo")]
            Self::DynamoDynamo($service) => $body,
            #[cfg(not(feature = "dynamo"))]
            Self::Unavailable => {
                unreachable!("configured chain-data reader has no enabled backend")
            }
        }
    };
}

impl ConfiguredChainDataReader {
    pub fn limits(&self) -> &QueryLimits {
        with_reader!(self, service => service.limits())
    }

    pub async fn load_published_head(&self) -> crate::error::Result<Option<u64>> {
        with_reader!(self, service => service.publication().load_published_head().await)
    }

    pub async fn block_number_by_hash(
        &self,
        block_hash: &Hash32,
    ) -> crate::error::Result<Option<u64>> {
        with_reader!(self, service => service.tables().blocks().block_number_by_hash(block_hash).await)
    }

    pub async fn load_block_record(
        &self,
        block_number: u64,
    ) -> crate::error::Result<Option<BlockRecord>> {
        with_reader!(self, service => service.tables().blocks().load_record(block_number).await)
    }

    pub async fn query_blocks(
        &self,
        request: QueryBlocksRequest,
    ) -> crate::error::Result<QueryBlocksResponse> {
        with_reader!(self, service => service.query_blocks(request).await)
    }

    pub async fn query_logs(
        &self,
        request: QueryLogsRequest,
    ) -> crate::error::Result<QueryLogsResponse> {
        with_reader!(self, service => service.query_logs(request).await)
    }

    pub async fn query_transactions(
        &self,
        request: QueryTransactionsRequest,
    ) -> crate::error::Result<QueryTransactionsResponse> {
        with_reader!(self, service => service.query_transactions(request).await)
    }

    pub async fn query_traces(
        &self,
        request: QueryTracesRequest,
    ) -> crate::error::Result<QueryTracesResponse> {
        with_reader!(self, service => service.query_traces(request).await)
    }

    pub async fn query_transfers(
        &self,
        request: QueryTransfersRequest,
    ) -> crate::error::Result<QueryTransfersResponse> {
        with_reader!(self, service => service.query_transfers(request).await)
    }

    pub fn take_cache_window_stats(&self) -> Vec<(&'static str, u64, u64)> {
        with_reader!(self, service => service.tables().take_cache_window_stats())
    }

    #[cfg(feature = "dynamo")]
    pub fn take_dynamo_meta_read_stats(&self) -> Option<crate::store::DynamoMetaReadStatsSnapshot> {
        match self {
            #[cfg(all(feature = "dynamo", feature = "s3"))]
            Self::DynamoS3(service) => Some(service.tables().meta_store().take_read_stats()),
            #[cfg(feature = "dynamo")]
            Self::DynamoDynamo(service) => Some(service.tables().meta_store().take_read_stats()),
        }
    }

    #[cfg(feature = "s3")]
    pub fn take_s3_read_stats(&self) -> Option<crate::store::S3ReadStatsSnapshot> {
        match self {
            #[cfg(not(feature = "dynamo"))]
            Self::Unavailable => None,
            #[cfg(all(feature = "dynamo", feature = "s3"))]
            Self::DynamoS3(service) => Some(service.tables().blob_store().take_read_stats()),
            #[cfg(feature = "dynamo")]
            Self::DynamoDynamo(_) => None,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ChainDataMetaBackendConfig {
    #[cfg(feature = "dynamo")]
    Dynamo(ChainDataDynamoMetaConfig),
    #[cfg(not(feature = "dynamo"))]
    Unavailable,
}

impl Default for ChainDataMetaBackendConfig {
    fn default() -> Self {
        #[cfg(feature = "dynamo")]
        {
            Self::Dynamo(ChainDataDynamoMetaConfig::default())
        }
        #[cfg(not(feature = "dynamo"))]
        {
            Self::Unavailable
        }
    }
}

impl ChainDataMetaBackendConfig {
    fn validate(&self) -> Result<()> {
        match self {
            #[cfg(feature = "dynamo")]
            Self::Dynamo(config) => config.validate(),
            #[cfg(not(feature = "dynamo"))]
            Self::Unavailable => bail!("chain-data configured storage requires the dynamo feature"),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ChainDataBlobBackendConfig {
    #[cfg(feature = "s3")]
    S3(ChainDataS3BlobConfig),
    #[cfg(feature = "dynamo")]
    Dynamo(ChainDataDynamoBlobConfig),
    #[cfg(not(any(feature = "s3", feature = "dynamo")))]
    Unavailable,
}

impl Default for ChainDataBlobBackendConfig {
    fn default() -> Self {
        #[cfg(feature = "s3")]
        {
            Self::S3(ChainDataS3BlobConfig::default())
        }
        #[cfg(all(not(feature = "s3"), feature = "dynamo"))]
        {
            Self::Dynamo(ChainDataDynamoBlobConfig::default())
        }
        #[cfg(not(any(feature = "s3", feature = "dynamo")))]
        {
            Self::Unavailable
        }
    }
}

impl ChainDataBlobBackendConfig {
    fn validate(&self) -> Result<()> {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(config) => config.validate(),
            #[cfg(feature = "dynamo")]
            Self::Dynamo(config) => config.validate(),
            #[cfg(not(any(feature = "s3", feature = "dynamo")))]
            Self::Unavailable => bail!("chain-data configured blob storage requires s3 or dynamo"),
        }
    }
}

#[cfg(feature = "s3")]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataS3BlobConfig {
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub profile: Option<String>,
    pub endpoint_urls: Vec<String>,
    pub prefix: String,
    pub force_path_style: bool,
    pub max_concurrency: usize,
    pub create_bucket: bool,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

#[cfg(feature = "s3")]
impl Default for ChainDataS3BlobConfig {
    fn default() -> Self {
        Self {
            bucket: None,
            region: None,
            profile: None,
            endpoint_urls: Vec::new(),
            prefix: String::new(),
            force_path_style: false,
            max_concurrency: 64,
            create_bucket: false,
            access_key_id: None,
            secret_access_key: None,
        }
    }
}

#[cfg(feature = "s3")]
impl ChainDataS3BlobConfig {
    fn validate(&self) -> Result<()> {
        if self.bucket.is_none() {
            bail!("chain-data s3 blob requires bucket");
        }
        if self.max_concurrency == 0 {
            bail!("chain-data s3 max_concurrency must be >= 1");
        }
        validate_pair(
            &self.access_key_id,
            &self.secret_access_key,
            "s3 access_key_id",
            "s3 secret_access_key",
        )
    }
}

#[cfg(feature = "dynamo")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChainDataDynamoTableLayoutConfig {
    Single,
    PerLogicalTable,
}

#[cfg(feature = "dynamo")]
impl Default for ChainDataDynamoTableLayoutConfig {
    fn default() -> Self {
        Self::Single
    }
}

#[cfg(feature = "dynamo")]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataDynamoMetaConfig {
    pub table: Option<String>,
    pub table_prefix: Option<String>,
    pub table_layout: ChainDataDynamoTableLayoutConfig,
    /// Single Alternator/DynamoDB endpoint (back-compat). Prefer `endpoint_urls`
    /// to drive multiple nodes.
    pub endpoint_url: Option<String>,
    /// Multiple endpoints (e.g. all Alternator nodes); requests round-robin
    /// across them to spread coordinator load. Takes precedence over
    /// `endpoint_url` when non-empty.
    pub endpoint_urls: Vec<String>,
    pub region: Option<String>,
    pub profile: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub create_table: bool,
    pub max_concurrency: usize,
    pub table_max_concurrency: usize,
    pub scylla_profile: bool,
    pub scylla_concurrency: usize,
    /// Desired max items per `BatchWriteItem`. Verified by a startup probe before
    /// use; if the backend rejects it the store falls back to 25. Unset →
    /// `ALTERNATOR_DEFAULT_BATCH_WRITE_ITEMS` (100) under `scylla_profile`, else
    /// 25. DynamoDB caps at 25; Alternator defaults to 100.
    pub batch_write_max_items: Option<usize>,
}

#[cfg(feature = "dynamo")]
impl Default for ChainDataDynamoMetaConfig {
    fn default() -> Self {
        Self {
            table: None,
            table_prefix: None,
            table_layout: ChainDataDynamoTableLayoutConfig::Single,
            endpoint_url: None,
            endpoint_urls: Vec::new(),
            region: None,
            profile: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            create_table: false,
            max_concurrency: 256,
            table_max_concurrency: 256,
            scylla_profile: false,
            scylla_concurrency: 256,
            batch_write_max_items: None,
        }
    }
}

/// Default `BatchWriteItem` item count to probe for under `scylla_profile` —
/// ScyllaDB Alternator's default `alternator_max_items_in_batch_write`.
#[cfg(feature = "dynamo")]
const ALTERNATOR_DEFAULT_BATCH_WRITE_ITEMS: usize = 100;
/// DynamoDB's fixed `BatchWriteItem` item cap (and the safe fallback).
#[cfg(feature = "dynamo")]
const DYNAMO_BATCH_WRITE_ITEMS: usize = 25;

#[cfg(feature = "dynamo")]
impl ChainDataDynamoMetaConfig {
    fn validate(&self) -> Result<()> {
        if self.effective_max_concurrency() == 0 || self.effective_table_max_concurrency() == 0 {
            bail!("chain-data dynamo concurrency must be >= 1");
        }
        validate_pair(
            &self.access_key_id,
            &self.secret_access_key,
            "dynamo access_key_id",
            "dynamo secret_access_key",
        )?;
        match self.effective_table_layout() {
            ChainDataDynamoTableLayoutConfig::Single => {
                if self.table_prefix.is_some() {
                    bail!("chain-data dynamo table_prefix requires per-logical-table layout and cannot be combined with scylla_profile");
                }
                if self.table.is_none() {
                    bail!("chain-data dynamo single table layout requires table");
                }
            }
            ChainDataDynamoTableLayoutConfig::PerLogicalTable => {
                if self.table.is_some() {
                    bail!("chain-data dynamo per-logical-table layout cannot use table");
                }
                if self.table_prefix.is_none() {
                    bail!("chain-data dynamo per-logical-table layout requires table_prefix");
                }
            }
        }
        Ok(())
    }

    /// Endpoints to use: `endpoint_urls` if set, else the single `endpoint_url`,
    /// else empty (default AWS resolver).
    fn effective_endpoint_urls(&self) -> Vec<String> {
        if !self.endpoint_urls.is_empty() {
            self.endpoint_urls.clone()
        } else {
            self.endpoint_url.clone().into_iter().collect()
        }
    }

    fn effective_table_layout(&self) -> ChainDataDynamoTableLayoutConfig {
        if self.scylla_profile {
            ChainDataDynamoTableLayoutConfig::Single
        } else {
            self.table_layout
        }
    }

    fn effective_max_concurrency(&self) -> usize {
        if self.scylla_profile {
            self.scylla_concurrency
        } else {
            self.max_concurrency
        }
    }

    fn effective_table_max_concurrency(&self) -> usize {
        if self.scylla_profile {
            self.scylla_concurrency
        } else {
            self.table_max_concurrency
        }
    }
}

#[cfg(feature = "dynamo")]
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataDynamoBlobConfig {
    pub table: Option<String>,
    pub endpoint_url: Option<String>,
    pub region: Option<String>,
    pub profile: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub create_table: bool,
    pub max_concurrency: usize,
    pub chunk_size: Option<usize>,
}

#[cfg(feature = "dynamo")]
impl Default for ChainDataDynamoBlobConfig {
    fn default() -> Self {
        Self {
            table: None,
            endpoint_url: None,
            region: None,
            profile: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            create_table: false,
            max_concurrency: 256,
            chunk_size: None,
        }
    }
}

#[cfg(feature = "dynamo")]
impl ChainDataDynamoBlobConfig {
    fn validate(&self) -> Result<()> {
        if self.table.is_none() {
            bail!("chain-data dynamo blob requires table");
        }
        if self.max_concurrency == 0 {
            bail!("chain-data dynamo blob max_concurrency must be >= 1");
        }
        validate_pair(
            &self.access_key_id,
            &self.secret_access_key,
            "dynamo access_key_id",
            "dynamo secret_access_key",
        )
    }
}

/// Cadence/range knobs for the branchless ingest engine
/// ([`run_configured_chain_data_engine_ingest`]). The two-track engine has its
/// own dials: pack coalescing size, flush/checkpoint cadence, and the tip-poll
/// interval — the engine owns its own producer, so there are no fetch/plan/
/// write-worker or autotune knobs.
///
/// There is deliberately no `start` knob: the begin block is always derived from
/// store state — the recovery snapshot's `last_block + 1` on a warm resume, or
/// genesis on an empty store. An absolute start cannot create a contiguous
/// store from the middle of the chain (the query layer assumes a gap-free range
/// up to the head), so the only valid begin is "wherever we left off". `end` and
/// `count` only bound how far *this run* goes from there.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataEngineConfig {
    /// Inclusive absolute end block for a bounded backfill: the engine catches
    /// the resume point up to here, then stops. Absent → follow the tip (live).
    /// Restart-safe: re-running with the same `end` resumes and finishes the gap.
    pub end: Option<u64>,
    /// Bounded backfill of this many blocks measured from the resume point
    /// (mutually exclusive with `end`). A convenience for one-shot runs; NOT
    /// restart-idempotent — a restart counts `count` blocks from the *new*
    /// resume point, so prefer `end` for anything that may be re-run.
    pub count: Option<u64>,
    /// UNSAFE test/fixture knob: seed a *fresh* store's recovery snapshot at this
    /// begin block so ingest starts here instead of genesis. On the first run it
    /// writes a snapshot with resume `block = begin - 1`; every subsequent run
    /// then resumes normally from the stored position. It is rejected if the
    /// store is already initialized (a snapshot exists) — remove this knob to
    /// resume. This deliberately breaks the gap-free-up-to-head invariant from
    /// the type-level doc comment: the store will have NO data below `begin`, so
    /// queries below it are invalid. Never use in production.
    pub unsafe_seed_begin: Option<u64>,
    /// Target packed-blob size before a data-track flush.
    pub pack_target_bytes: usize,
    /// Hard cap on blocks per packed blob.
    pub pack_max_blocks: usize,
    /// Adaptive `BatchFlush` interval scaled by distance to the uploaded tip:
    /// `max(distance / tip_lag_divisor, 1)`. At the tip it flushes every block and
    /// coarsens the farther behind it is; the published head trails the ingested
    /// frontier by at most ~1/`tip_lag_divisor` of the remaining distance. There is
    /// no cap — `OpenTail` memory is bounded by `seal` (continuous, frontier-driven),
    /// not by flush cadence — so this is purely a reader-freshness knob.
    pub tip_lag_divisor: u64,
    /// Snapshot `OpenState` every this many blocks (recovery resume point /
    /// fragment-replay bound).
    pub checkpoint_every_blocks: u64,
    /// Inter-track channel buffer depth.
    pub track_buffer: usize,
    /// Tip poll interval (ms): how often to re-check `get_latest` once caught up to
    /// the uploaded tip. Does not affect catch-up (which never polls mid-backlog) or
    /// lease liveness (separate refresher). Small ⇒ fresher head, cheap idle polls.
    pub poll_ms: u64,
    /// How many block fetches the producer runs at once. Each fetch is spawned, so
    /// this also bounds how many blocks decode in parallel across worker threads
    /// (decode is CPU-bound and would otherwise serialize on the one producer
    /// task). Overlaps (S3) GET latency that is otherwise fully serialized
    /// (throughput = 1 / per-block-fetch-latency). 1 == sequential. Matters during
    /// catch-up; at the steady tip the backlog is ~1 block so it has no effect.
    pub fetch_concurrency: usize,
    /// Ordered look-ahead buffer — max decoded-but-not-yet-ingested blocks queued
    /// ahead of the engine (clamped up to `fetch_concurrency`). A buffer deeper
    /// than `fetch_concurrency` lets fetchers keep running ahead so the engine
    /// never starves during a flush/checkpoint stall. Memory scales with this ×
    /// per-block size. The default is sized for a high-latency per-object source
    /// (e.g. S3) where deep look-ahead keeps all `fetch_concurrency` GETs in
    /// flight; shrink it for memory-constrained runs or tiny per-block sources.
    pub fetch_buffer: usize,
}

impl Default for ChainDataEngineConfig {
    fn default() -> Self {
        Self {
            end: None,
            count: None,
            unsafe_seed_begin: None,
            pack_target_bytes: 8 * 1024 * 1024,
            pack_max_blocks: 1000,
            // flush interval = max(distance/10, 1): every block at the tip,
            // coarsening to ~10% of the distance while catching up.
            tip_lag_divisor: 10,
            checkpoint_every_blocks: 10_000,
            track_buffer: 16,
            poll_ms: 50,
            fetch_concurrency: 100,
            fetch_buffer: 5000,
        }
    }
}

impl ChainDataEngineConfig {
    pub fn validate(&self) -> Result<()> {
        if self.end.is_some() && self.count.is_some() {
            bail!("chain-data engine end and count are mutually exclusive");
        }
        if matches!(self.count, Some(0)) {
            bail!("chain-data engine count must be >= 1");
        }
        if matches!(self.unsafe_seed_begin, Some(0)) {
            bail!("chain-data engine unsafe_seed_begin must be >= 1 (0 is the normal genesis cold start)");
        }
        if let (Some(begin), Some(end)) = (self.unsafe_seed_begin, self.end) {
            if end < begin {
                bail!("chain-data engine end ({end}) must be >= unsafe_seed_begin ({begin})");
            }
        }
        if self.pack_target_bytes == 0 {
            bail!("chain-data engine pack_target_bytes must be >= 1");
        }
        if self.pack_max_blocks == 0 {
            bail!("chain-data engine pack_max_blocks must be >= 1");
        }
        if self.checkpoint_every_blocks == 0 {
            bail!("chain-data engine checkpoint_every_blocks must be >= 1");
        }
        if self.track_buffer == 0 {
            bail!("chain-data engine track_buffer must be >= 1");
        }
        if self.fetch_concurrency == 0 {
            bail!("chain-data engine fetch_concurrency must be >= 1");
        }
        if self.fetch_buffer == 0 {
            bail!("chain-data engine fetch_buffer must be >= 1");
        }
        // The flush cadence and tip poll interval always apply.
        if self.tip_lag_divisor == 0 {
            bail!("chain-data engine tip_lag_divisor must be >= 1");
        }
        if self.poll_ms == 0 {
            bail!("chain-data engine poll_ms must be >= 1");
        }
        Ok(())
    }
}

/// Open the configured stores and drive the **branchless** ingest engine
/// ([`run_ingest_controller`]) over them. Handles store/backend
/// dispatch and cache resolution, then builds `Arc<Tables>` +
/// `Arc<HeadPublisher>` + [`SnapshotStore`] + [`TablesCodecResolver`] directly
/// from one meta/blob store pair — no [`MonadChainDataService`] on the ingest
/// write path. A bounded range (`end`/`count`) selects backfill; otherwise it
/// follows the tip.
pub async fn run_configured_chain_data_engine_ingest<S>(
    store_config: ChainDataStoreConfig,
    engine_config: ChainDataEngineConfig,
    source: S,
) -> Result<()>
where
    S: ChainDataIngestSource,
{
    store_config.validate()?;
    engine_config.validate()?;

    match store_config.meta.clone() {
        #[cfg(feature = "dynamo")]
        ChainDataMetaBackendConfig::Dynamo(meta_config) => {
            let meta_store = build_dynamo_meta_store(&meta_config).await?;
            let client = Some(meta_store.client());
            dispatch_blob_engine(&store_config, &engine_config, source, meta_store, client).await
        }
        #[cfg(not(feature = "dynamo"))]
        ChainDataMetaBackendConfig::Unavailable => {
            bail!("chain-data configured ingest requires the dynamo feature")
        }
    }
}

async fn dispatch_blob_engine<M, S>(
    store_config: &ChainDataStoreConfig,
    engine_config: &ChainDataEngineConfig,
    source: S,
    meta_store: M,
    #[allow(unused_variables)] dynamo_client: Option<SharedDynamoClient>,
) -> Result<()>
where
    M: MetaStore,
    S: ChainDataIngestSource,
{
    match store_config.blob.clone() {
        #[cfg(feature = "s3")]
        ChainDataBlobBackendConfig::S3(blob_config) => {
            let blob_store = build_s3_blob_store(&blob_config).await?;
            run_engine_with_store(store_config, engine_config, source, meta_store, blob_store).await
        }
        #[cfg(feature = "dynamo")]
        ChainDataBlobBackendConfig::Dynamo(blob_config) => {
            let blob_store = build_dynamo_blob_store(&blob_config, dynamo_client).await?;
            run_engine_with_store(store_config, engine_config, source, meta_store, blob_store).await
        }
        #[cfg(not(any(feature = "s3", feature = "dynamo")))]
        ChainDataBlobBackendConfig::Unavailable => {
            bail!("chain-data configured ingest requires s3 or dynamo blob storage")
        }
    }
}

async fn run_engine_with_store<M, B, S>(
    store_config: &ChainDataStoreConfig,
    engine_config: &ChainDataEngineConfig,
    source: S,
    meta_store: M,
    blob_store: B,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
    S: ChainDataIngestSource,
{
    let cache_config = resolve_cache_config(store_config, ChainDataCacheMode::Ingest);
    // One Arc<Tables> is shared by the engine (writes) and the resolver (dict
    // training reads back through the same caches). PublicationTables and the
    // SnapshotStore wrap the same meta store handle.
    let tables = Arc::new(Tables::with_cache_config(
        meta_store.clone(),
        blob_store,
        cache_config,
    ));
    let publisher = Arc::new(HeadPublisher::new(PublicationTables::new(
        meta_store.clone(),
    )));
    let snapshots = SnapshotStore::new(meta_store);
    let resolver = TablesCodecResolver::new(tables.clone());

    // UNSAFE test/fixture seeding: on a fresh store, write a recovery snapshot at
    // an explicit begin block so the controller resumes there instead of starting
    // at genesis. Refused once the store is initialized (a snapshot exists) — the
    // operator must remove the knob to resume from the stored position.
    if let Some(begin) = engine_config.unsafe_seed_begin {
        if snapshots.load().await?.is_some() {
            bail!(
                "chain-data unsafe_seed_begin ({begin}) is set but the store is already \
                 initialized (a recovery snapshot exists); remove unsafe_seed_begin to \
                 resume from the stored position"
            );
        }
        warn!(
            begin,
            "UNSAFE: seeding chain-data recovery snapshot at an explicit begin block; \
             the store will have NO data below this block (gap-free invariant violated) — \
             for tests/fixtures only"
        );
        seed_snapshot_at(&snapshots, begin).await?;
    }

    // Fetch parallelism is shared identically whether we catch up or follow the tip.
    let prefetch = Prefetch {
        concurrency: engine_config.fetch_concurrency,
        buffer: engine_config.fetch_buffer,
    };

    // One unified controller: always follow the tip. `end`/`count` only bound how
    // far this run goes (absent ⇒ follow forever); the begin block is derived from
    // store state inside the controller (resume snapshot's last_block + 1, or
    // genesis on an empty store).
    info!(
        end = engine_config.end,
        count = engine_config.count,
        "starting chain-data ingest (branchless engine)"
    );
    run_ingest_controller(
        source,
        tables,
        publisher,
        snapshots,
        resolver,
        IngestRunConfig {
            start: 0, // genesis cold-start floor; a warm resume overrides it
            stop_at: engine_config.end,
            count: engine_config.count,
            pack_target_bytes: engine_config.pack_target_bytes,
            pack_max_blocks: engine_config.pack_max_blocks,
            tip_lag_divisor: engine_config.tip_lag_divisor,
            checkpoint_every_blocks: engine_config.checkpoint_every_blocks,
            track_buffer: engine_config.track_buffer,
            poll_ms: engine_config.poll_ms,
        },
        prefetch,
    )
    .await
    .map_err(Into::into)
}

pub async fn open_configured_chain_data_reader(
    store_config: ChainDataStoreConfig,
    limits: QueryLimits,
) -> Result<ConfiguredChainDataReader> {
    store_config.validate_reader()?;
    match store_config.meta.clone() {
        #[cfg(feature = "dynamo")]
        ChainDataMetaBackendConfig::Dynamo(meta_config) => {
            let meta_store = build_dynamo_meta_store(&meta_config).await?;
            let client = Some(meta_store.client());
            dispatch_dynamo_reader_blob(&store_config, limits, meta_store, client).await
        }
        #[cfg(not(feature = "dynamo"))]
        ChainDataMetaBackendConfig::Unavailable => {
            bail!("chain-data configured reader requires the dynamo feature")
        }
    }
}

#[cfg(feature = "dynamo")]
async fn dispatch_dynamo_reader_blob(
    store_config: &ChainDataStoreConfig,
    limits: QueryLimits,
    meta_store: crate::store::DynamoMetaStore,
    #[allow(unused_variables)] dynamo_client: Option<SharedDynamoClient>,
) -> Result<ConfiguredChainDataReader> {
    let cache_config = resolve_cache_config(store_config, ChainDataCacheMode::Reader);
    let query_config = store_config.query.to_runtime();
    match store_config.blob.clone() {
        #[cfg(feature = "s3")]
        ChainDataBlobBackendConfig::S3(blob_config) => {
            let blob_store = build_s3_blob_store(&blob_config).await?;
            Ok(ConfiguredChainDataReader::DynamoS3(Arc::new(
                crate::MonadChainDataService::new_reader_with_query_config(
                    meta_store,
                    blob_store,
                    limits,
                    cache_config,
                    query_config,
                ),
            )))
        }
        ChainDataBlobBackendConfig::Dynamo(blob_config) => {
            let blob_store = build_dynamo_blob_store(&blob_config, dynamo_client).await?;
            Ok(ConfiguredChainDataReader::DynamoDynamo(Arc::new(
                crate::MonadChainDataService::new_reader_with_query_config(
                    meta_store,
                    blob_store,
                    limits,
                    cache_config,
                    query_config,
                ),
            )))
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ChainDataCacheMode {
    Ingest,
    Reader,
}

const READER_DEFAULT_CACHE_MIB: usize = 2048;

fn resolve_cache_config(
    store_config: &ChainDataStoreConfig,
    mode: ChainDataCacheMode,
) -> CacheConfig {
    let mode_default_mib = match mode {
        ChainDataCacheMode::Ingest => 0,
        ChainDataCacheMode::Reader => READER_DEFAULT_CACHE_MIB,
    };
    let legacy_total_mib = match (mode, store_config.cache_mib) {
        // Old archiver configs often wrote `cache_mib = 0` to keep ingest lean.
        // Reader mode has a separate default now, so do not let that legacy
        // ingest hint disable reader caches. Use `[cache].total_mib = 0` for an
        // explicit reader-side no-cache config.
        (ChainDataCacheMode::Reader, Some(0)) => None,
        (_, value) => value,
    };
    let total_mib = store_config
        .cache
        .total_mib
        .or(legacy_total_mib)
        .unwrap_or(mode_default_mib);

    let mut config = CacheConfig::from_total_mib(total_mib);
    apply_cache_table_overrides(&mut config, &store_config.cache.tables);

    let block_region_max_mib = store_config
        .cache
        .block_region_max_mib
        .or(store_config.block_region_max_mib);
    if let Some(max_mib) = block_region_max_mib {
        config.block_region_max_bytes = CacheConfig::budget_mib_to_bytes(max_mib);
    }
    config
}

fn apply_cache_table_overrides(config: &mut CacheConfig, overrides: &ChainDataCacheTableBudgets) {
    if let Some(mib) = overrides.dir_by_block_mib {
        config.dir_by_block_entries =
            CacheConfig::entries_for_budget_mib(CacheField::DirByBlock, mib);
    }
    if let Some(mib) = overrides.dir_bucket_mib {
        config.dir_bucket_entries = CacheConfig::entries_for_budget_mib(CacheField::DirBucket, mib);
    }
    if let Some(mib) = overrides.bitmap_by_block_mib {
        config.bitmap_by_block_cache_bytes = CacheConfig::budget_mib_to_bytes(mib);
    }
    if let Some(mib) = overrides.bitmap_page_blob_mib {
        config.bitmap_page_blob_cache_bytes = CacheConfig::budget_mib_to_bytes(mib);
    }
    if let Some(mib) = overrides.bitmap_page_counts_mib {
        config.bitmap_page_counts_entries =
            CacheConfig::entries_for_budget_mib(CacheField::BitmapPageCounts, mib);
    }
    if let Some(mib) = overrides.open_bitmap_stream_mib {
        config.open_bitmap_stream_entries =
            CacheConfig::entries_for_budget_mib(CacheField::OpenBitmapStream, mib);
    }
    if let Some(mib) = overrides.block_header_mib {
        config.block_header_entries =
            CacheConfig::entries_for_budget_mib(CacheField::BlockHeader, mib);
    }
    if let Some(mib) = overrides.block_hash_to_number_mib {
        config.block_hash_to_number_entries =
            CacheConfig::entries_for_budget_mib(CacheField::BlockHashToNumber, mib);
    }
    if let Some(mib) = overrides.tx_hash_index_mib {
        config.tx_hash_index_entries =
            CacheConfig::entries_for_budget_mib(CacheField::TxHashIndex, mib);
    }
    if let Some(mib) = overrides.block_region_mib {
        config.block_region_cache_bytes = CacheConfig::budget_mib_to_bytes(mib);
    }
}

#[cfg(feature = "s3")]
async fn build_s3_blob_store(config: &ChainDataS3BlobConfig) -> Result<crate::store::S3BlobStore> {
    use crate::store::{S3BlobStore, S3BlobStoreConfig, S3Credentials};

    let credentials = match (&config.access_key_id, &config.secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => Some(S3Credentials {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: None,
        }),
        _ => None,
    };
    let store_config = S3BlobStoreConfig {
        bucket: config.bucket.clone().expect("validated s3 bucket"),
        root_prefix: config.prefix.clone(),
        endpoint_urls: config.endpoint_urls.clone(),
        region: config.region.clone(),
        profile: config.profile.clone(),
        force_path_style: config.force_path_style,
        max_concurrency: config.max_concurrency,
        create_bucket: config.create_bucket,
        credentials,
    };
    S3BlobStore::new(store_config)
        .await
        .context("building chain-data S3 blob store")
}

#[cfg(feature = "dynamo")]
async fn build_dynamo_meta_store(
    config: &ChainDataDynamoMetaConfig,
) -> Result<crate::store::DynamoMetaStore> {
    use crate::store::{DynamoMetaStore, DynamoMetaStoreConfig, DynamoTableLayout};

    let table_layout = match config.effective_table_layout() {
        ChainDataDynamoTableLayoutConfig::Single => {
            DynamoTableLayout::single(config.table.clone().expect("validated dynamo table"))
        }
        ChainDataDynamoTableLayoutConfig::PerLogicalTable => DynamoTableLayout::PerLogicalTable {
            prefix: config
                .table_prefix
                .clone()
                .expect("validated dynamo table_prefix"),
        },
    };
    let credentials = dynamo_credentials(
        &config.access_key_id,
        &config.secret_access_key,
        &config.session_token,
    );
    let store_config = DynamoMetaStoreConfig {
        table_layout,
        endpoint_urls: config.effective_endpoint_urls(),
        region: config.region.clone(),
        profile: config.profile.clone(),
        batch_max_concurrency: config.effective_max_concurrency(),
        batch_table_max_concurrency: config.effective_table_max_concurrency(),
        // Start at the DynamoDB-safe limit; a startup probe raises it if the
        // backend (Alternator) accepts larger batches.
        batch_write_max_items: DYNAMO_BATCH_WRITE_ITEMS,
        credentials,
    };
    let store = DynamoMetaStore::new(store_config)
        .await
        .context("building chain-data Dynamo meta store")?;
    if config.create_table {
        store
            .create_table()
            .await
            .context("creating chain-data Dynamo meta table(s)")?;
    }
    store
        .validate_table()
        .await
        .context("validating chain-data Dynamo meta table(s)")?;
    // Probe the effective BatchWriteItem item limit (Alternator allows >25): one
    // delete-batch of non-existent keys, falling back to 25 if rejected. Larger
    // batches cut write round-trips proportionally.
    let batch_candidate = config
        .batch_write_max_items
        .unwrap_or(if config.scylla_profile {
            ALTERNATOR_DEFAULT_BATCH_WRITE_ITEMS
        } else {
            DYNAMO_BATCH_WRITE_ITEMS
        });
    let effective = store.discover_batch_write_limit(batch_candidate).await;
    info!(
        candidate = batch_candidate,
        effective, "resolved dynamo BatchWriteItem item limit"
    );
    Ok(store)
}

#[cfg(feature = "dynamo")]
type SharedDynamoClient = aws_sdk_dynamodb::Client;
#[cfg(not(feature = "dynamo"))]
type SharedDynamoClient = ();

#[cfg(feature = "dynamo")]
async fn build_dynamo_blob_store(
    config: &ChainDataDynamoBlobConfig,
    shared_client: Option<aws_sdk_dynamodb::Client>,
) -> Result<crate::store::DynamoBlobStore> {
    use crate::store::{DynamoBlobStore, DynamoBlobStoreConfig};

    let mut store_config =
        DynamoBlobStoreConfig::new(config.table.clone().expect("validated table"));
    store_config.endpoint_url = config.endpoint_url.clone();
    store_config.region = config.region.clone();
    store_config.profile = config.profile.clone();
    store_config.batch_max_concurrency = config.max_concurrency;
    store_config.credentials = dynamo_credentials(
        &config.access_key_id,
        &config.secret_access_key,
        &config.session_token,
    );
    if let Some(chunk_size) = config.chunk_size {
        store_config.chunk_size = chunk_size;
    }
    let store = match shared_client {
        Some(client) => DynamoBlobStore::new_with_client(
            client,
            store_config.table_name.clone(),
            store_config.batch_max_concurrency,
            store_config.chunk_size,
        ),
        None => DynamoBlobStore::new(store_config)
            .await
            .context("building chain-data Dynamo blob store")?,
    };
    if config.create_table {
        store
            .create_table()
            .await
            .context("creating chain-data Dynamo blob table")?;
    }
    store
        .validate_table()
        .await
        .context("validating chain-data Dynamo blob table")?;
    Ok(store)
}

#[cfg(feature = "dynamo")]
fn dynamo_credentials(
    access_key_id: &Option<String>,
    secret_access_key: &Option<String>,
    session_token: &Option<String>,
) -> Option<crate::store::DynamoCredentials> {
    match (access_key_id, secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => Some(crate::store::DynamoCredentials {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: session_token.clone(),
        }),
        _ => None,
    }
}

/// Render an optional credential for `Debug` without leaking its value, so a
/// config dump (e.g. the archiver's `info!(?args)` startup log) never persists
/// secrets to logs.
#[cfg(any(feature = "s3", feature = "dynamo"))]
fn redacted(value: &Option<String>) -> Option<&'static str> {
    value.as_ref().map(|_| "[REDACTED]")
}

#[cfg(feature = "s3")]
impl std::fmt::Debug for ChainDataS3BlobConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainDataS3BlobConfig")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("profile", &self.profile)
            .field("endpoint_urls", &self.endpoint_urls)
            .field("prefix", &self.prefix)
            .field("force_path_style", &self.force_path_style)
            .field("max_concurrency", &self.max_concurrency)
            .field("create_bucket", &self.create_bucket)
            .field("access_key_id", &redacted(&self.access_key_id))
            .field("secret_access_key", &redacted(&self.secret_access_key))
            .finish()
    }
}

#[cfg(feature = "dynamo")]
impl std::fmt::Debug for ChainDataDynamoMetaConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainDataDynamoMetaConfig")
            .field("table", &self.table)
            .field("table_prefix", &self.table_prefix)
            .field("table_layout", &self.table_layout)
            .field("endpoint_url", &self.endpoint_url)
            .field("endpoint_urls", &self.endpoint_urls)
            .field("region", &self.region)
            .field("profile", &self.profile)
            .field("access_key_id", &redacted(&self.access_key_id))
            .field("secret_access_key", &redacted(&self.secret_access_key))
            .field("session_token", &redacted(&self.session_token))
            .field("create_table", &self.create_table)
            .field("max_concurrency", &self.max_concurrency)
            .field("table_max_concurrency", &self.table_max_concurrency)
            .field("scylla_profile", &self.scylla_profile)
            .field("scylla_concurrency", &self.scylla_concurrency)
            .field("batch_write_max_items", &self.batch_write_max_items)
            .finish()
    }
}

#[cfg(feature = "dynamo")]
impl std::fmt::Debug for ChainDataDynamoBlobConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainDataDynamoBlobConfig")
            .field("table", &self.table)
            .field("endpoint_url", &self.endpoint_url)
            .field("region", &self.region)
            .field("profile", &self.profile)
            .field("access_key_id", &redacted(&self.access_key_id))
            .field("secret_access_key", &redacted(&self.secret_access_key))
            .field("session_token", &redacted(&self.session_token))
            .field("create_table", &self.create_table)
            .field("max_concurrency", &self.max_concurrency)
            .field("chunk_size", &self.chunk_size)
            .finish()
    }
}

#[cfg(any(feature = "s3", feature = "dynamo"))]
fn validate_pair(
    left: &Option<String>,
    right: &Option<String>,
    left_name: &'static str,
    right_name: &'static str,
) -> Result<()> {
    match (left, right) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(_), None) => {
            warn!("{left_name} was set without {right_name}");
            bail!("{left_name} requires {right_name}")
        }
        (None, Some(_)) => {
            warn!("{right_name} was set without {left_name}");
            bail!("{right_name} requires {left_name}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_cache_default_is_disabled() {
        let config =
            resolve_cache_config(&ChainDataStoreConfig::default(), ChainDataCacheMode::Ingest);
        assert_eq!(config.total_entries(), 0);
        assert_eq!(config.block_region_cache_bytes, 0);
    }

    #[test]
    fn reader_cache_default_is_two_gib_ratio_based() {
        let config =
            resolve_cache_config(&ChainDataStoreConfig::default(), ChainDataCacheMode::Reader);
        assert_eq!(config.block_region_cache_bytes, 1024 * 1024 * 1024);
        // 256/1024 of 2048 MiB, as a byte budget.
        assert_eq!(config.bitmap_by_block_cache_bytes, 512 * 1024 * 1024);
    }

    #[test]
    fn legacy_zero_cache_mib_does_not_disable_reader_default() {
        let store_config = ChainDataStoreConfig {
            cache_mib: Some(0),
            ..ChainDataStoreConfig::default()
        };
        let config = resolve_cache_config(&store_config, ChainDataCacheMode::Reader);
        assert_eq!(config.block_region_cache_bytes, 1024 * 1024 * 1024);
    }

    #[test]
    fn nested_zero_total_disables_reader_cache() {
        let store_config = ChainDataStoreConfig {
            cache: ChainDataCacheConfig {
                total_mib: Some(0),
                ..ChainDataCacheConfig::default()
            },
            ..ChainDataStoreConfig::default()
        };
        let config = resolve_cache_config(&store_config, ChainDataCacheMode::Reader);
        assert_eq!(config.total_entries(), 0);
        assert_eq!(config.block_region_cache_bytes, 0);
    }

    #[test]
    fn per_table_budget_overrides_ratio_budget() {
        let store_config = ChainDataStoreConfig {
            cache: ChainDataCacheConfig {
                total_mib: Some(2048),
                tables: ChainDataCacheTableBudgets {
                    block_region_mib: Some(64),
                    bitmap_page_blob_mib: Some(32),
                    ..ChainDataCacheTableBudgets::default()
                },
                ..ChainDataCacheConfig::default()
            },
            ..ChainDataStoreConfig::default()
        };
        let config = resolve_cache_config(&store_config, ChainDataCacheMode::Reader);
        assert_eq!(config.block_region_cache_bytes, 64 * 1024 * 1024);
        assert_eq!(config.bitmap_page_blob_cache_bytes, 32 * 1024 * 1024);
    }
}
