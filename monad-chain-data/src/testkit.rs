// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

//! Test utilities for downstream crates: populate a store by running the real
//! branchless ingest engine, then read it back through [`MonadChainDataService`].
//!
//! The production write path is the branchless engine (`ingest_core` driven by
//! `ingest_controller`), so tests should populate fixtures through it rather than
//! the removed `MonadChainDataService::ingest_block` path. The in-memory stores are
//! `Arc<RwLock>`-backed and clone-share their backing, so a reader built from
//! clones of the same stores observes the engine's writes (data, index
//! artifacts, and the published head) with no extra plumbing.

use std::{future::Future, sync::Arc};

use crate::{
    api::MonadChainDataService,
    engine::{
        authority::HeadPublisher,
        tables::{DictConfig, PublicationTables, Tables},
    },
    error::MonadChainDataError,
    family::FinalizedBlock,
    ingest_controller::{run_ingest_controller, IngestRunConfig},
    ingest_core::{Prefetch, SnapshotStore},
    ingest_resolver::TablesCodecResolver,
    ingest_source::ChainDataIngestSource,
    primitives::limits::QueryLimits,
    store::{BlobStore, CacheConfig, InMemoryBlobStore, InMemoryMetaStore, MetaStore},
};

/// A `Vec`-backed [`ChainDataIngestSource`] over blocks `[start, start + len)`.
/// `get_latest_uploaded` reports the last block for tip-following cadence.
#[derive(Clone)]
struct VecSource {
    blocks: Arc<Vec<FinalizedBlock>>,
    start: u64,
}

impl ChainDataIngestSource for VecSource {
    fn get_latest_uploaded(&self) -> impl Future<Output = eyre::Result<Option<u64>>> + Send {
        let latest = self.start + self.blocks.len() as u64 - 1;
        async move { Ok(Some(latest)) }
    }

    fn fetch_finalized_block(
        &self,
        block_number: u64,
    ) -> impl Future<Output = eyre::Result<FinalizedBlock>> + Send {
        let block = block_number
            .checked_sub(self.start)
            .and_then(|idx| self.blocks.get(idx as usize).cloned());
        async move { block.ok_or_else(|| eyre::eyre!("no block {block_number}")) }
    }
}

/// In-memory stores populated by the engine. Hold onto this so the backing
/// `Arc<RwLock>` maps stay alive; build readers from it via [`Self::reader`].
pub struct PopulatedStore {
    pub meta: InMemoryMetaStore,
    pub blob: InMemoryBlobStore,
    pub tables: Arc<Tables<InMemoryMetaStore, InMemoryBlobStore>>,
}

impl PopulatedStore {
    /// A read-only query service over the SAME stores the engine wrote to.
    pub fn reader(&self) -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
        self.reader_with_limits(QueryLimits::UNLIMITED)
    }

    /// A read-only query service with explicit [`QueryLimits`].
    pub fn reader_with_limits(
        &self,
        limits: QueryLimits,
    ) -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
        MonadChainDataService::new_reader_only(
            self.meta.clone(),
            self.blob.clone(),
            limits,
            CacheConfig::default(),
        )
    }
}

/// Backfill-cadence knobs for population. The defaults run the blocks through in
/// a single terminal flush, publishing `head == last_block`; tests that care
/// about intermediate flush/checkpoint cadence can override them.
/// Fetch parallelism for tests: >1 so fixtures exercise the ordered-prefetch path
/// (delivery order is preserved regardless).
pub const TEST_PREFETCH: Prefetch = Prefetch {
    concurrency: 4,
    buffer: 8,
};

pub fn populate_config(start: u64, end: u64) -> IngestRunConfig {
    IngestRunConfig {
        start,
        stop_at: Some(end),
        count: None,
        pack_target_bytes: 8 << 20,
        pack_max_blocks: 4096,
        // interval == distance to tip: few flushes over a small fixture, with the
        // terminal flush guaranteeing head == last_block.
        tip_lag_divisor: 1,
        checkpoint_every_blocks: 4096,
        track_buffer: 16,
        poll_ms: 1,
    }
}

/// Ingest `blocks` via the branchless backfill engine into fresh in-memory
/// stores and publish the head to the last block. `blocks` must be parent-linked
/// and contiguously numbered (the engine assigns ids sequentially and derives
/// the epoch from the block number).
///
/// Panics on an empty input or an ingest error — tests want a hard failure.
pub async fn populate_via_engine(blocks: Vec<FinalizedBlock>) -> PopulatedStore {
    try_populate_via_engine(blocks)
        .await
        .expect("backfill ingest")
}

/// Like [`populate_via_engine`] but with an explicit [`IngestRunConfig`] (e.g. to
/// exercise a specific flush/checkpoint cadence). `config.start` must match the
/// first block's number.
pub async fn populate_via_engine_with(
    blocks: Vec<FinalizedBlock>,
    config: IngestRunConfig,
) -> PopulatedStore {
    run_engine(blocks, config, DictConfig::default())
        .await
        .expect("backfill ingest")
}

/// Fallible variant of [`populate_via_engine`]: returns the engine's error
/// instead of panicking, for negative tests (e.g. an invalid block must abort
/// ingest rather than be silently accepted).
pub async fn try_populate_via_engine(
    blocks: Vec<FinalizedBlock>,
) -> Result<PopulatedStore, MonadChainDataError> {
    assert!(
        !blocks.is_empty(),
        "try_populate_via_engine needs at least one block"
    );
    let start = blocks.first().unwrap().header.number;
    let end = blocks.last().unwrap().header.number;
    run_engine(blocks, populate_config(start, end), DictConfig::default()).await
}

/// Like [`populate_via_engine`] but with a custom [`DictConfig`] so the
/// epoch-based dictionary lifecycle (small `epoch_blocks`) can be exercised.
pub async fn populate_via_engine_with_dict(
    blocks: Vec<FinalizedBlock>,
    dict: DictConfig,
) -> PopulatedStore {
    assert!(!blocks.is_empty(), "populate needs at least one block");
    let start = blocks.first().unwrap().header.number;
    let end = blocks.last().unwrap().header.number;
    run_engine(blocks, populate_config(start, end), dict)
        .await
        .expect("backfill ingest")
}

async fn run_engine(
    blocks: Vec<FinalizedBlock>,
    config: IngestRunConfig,
    dict: DictConfig,
) -> Result<PopulatedStore, MonadChainDataError> {
    let meta = InMemoryMetaStore::default();
    let blob = InMemoryBlobStore::default();
    let tables = Arc::new(Tables::with_configs(
        meta.clone(),
        blob.clone(),
        CacheConfig::default(),
        dict,
    ));
    let snapshots = SnapshotStore::new(meta.clone());
    let resolver = TablesCodecResolver::new(tables.clone());

    let publisher = Arc::new(HeadPublisher::new(PublicationTables::new(meta.clone())));

    let source = VecSource {
        blocks: Arc::new(blocks),
        start: config.start,
    };

    run_ingest_controller(
        source,
        tables.clone(),
        publisher,
        snapshots,
        resolver,
        config,
        TEST_PREFETCH,
    )
    .await?;

    Ok(PopulatedStore { meta, blob, tables })
}

/// Populate caller-owned stores via the branchless engine, publishing head = last block. The caller
/// keeps the store handles and builds its own read service over them — used by
/// backend-specific tests (durability across reopen, etc.).
pub async fn populate_stores<M, B>(
    meta: M,
    blob: B,
    blocks: Vec<FinalizedBlock>,
) -> Result<(), MonadChainDataError>
where
    M: MetaStore,
    B: BlobStore,
{
    assert!(
        !blocks.is_empty(),
        "populate_stores needs at least one block"
    );
    let start = blocks.first().unwrap().header.number;
    let end = blocks.last().unwrap().header.number;

    let tables = Arc::new(Tables::new(meta.clone(), blob.clone()));
    let snapshots = SnapshotStore::new(meta.clone());
    let resolver = TablesCodecResolver::new(tables.clone());
    let publisher = Arc::new(HeadPublisher::new(PublicationTables::new(meta.clone())));
    let source = VecSource {
        blocks: Arc::new(blocks),
        start,
    };

    run_ingest_controller(
        source,
        tables,
        publisher,
        snapshots,
        resolver,
        populate_config(start, end),
        TEST_PREFETCH,
    )
    .await
}
