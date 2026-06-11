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

//! Test utilities: populate a store through the real branchless ingest engine,
//! then read it back via [`MonadChainDataService`]. The in-memory stores
//! clone-share their `Arc<RwLock>` backing, so readers built from clones
//! observe the engine's writes with no extra plumbing.

use std::{future::Future, sync::Arc};

use crate::{
    api::MonadChainDataService,
    engine::tables::{DictConfig, PublicationTables, QueryRuntimeConfig, Tables},
    error::MonadChainDataError,
    ingest::{
        resolver::TablesCodecResolver, run_ingest, source::ChainDataIngestSource, IngestRunConfig,
        PackConfig, Prefetch, SignalPolicy, SnapshotStore,
    },
    ingest_types::FinalizedBlock,
    primitives::limits::QueryLimits,
    store::{CacheConfig, InMemoryBlobStore, InMemoryMetaStore},
};

/// A `Vec`-backed [`ChainDataIngestSource`] over blocks `[start, start + len)`.
/// `get_latest_uploaded` reports the last block for tip-following cadence.
#[derive(Clone)]
pub struct VecSource {
    blocks: Arc<Vec<FinalizedBlock>>,
    start: u64,
}

impl VecSource {
    pub fn new(blocks: Vec<FinalizedBlock>, start: u64) -> Self {
        Self {
            blocks: Arc::new(blocks),
            start,
        }
    }
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
        MonadChainDataService::new(self.meta.clone(), self.blob.clone(), limits)
    }
}

/// Fetch parallelism for tests: >1 so fixtures exercise the ordered-prefetch
/// path (delivery order is preserved regardless).
pub const TEST_PREFETCH: Prefetch = Prefetch {
    concurrency: 4,
    buffer: 8,
};

/// Backfill-cadence knobs for population; the defaults run the blocks through
/// few flushes, publishing `head == last_block`.
fn populate_config(start: u64, end: u64) -> IngestRunConfig {
    IngestRunConfig {
        start,
        end: Some(end),
        count: None,
        pack: PackConfig {
            target_bytes: 8 << 20,
            max_blocks: 4096,
        },
        // interval == distance to tip: few flushes over a small fixture; the
        // terminal flush guarantees head == last_block.
        policy: SignalPolicy {
            tip_lag_divisor: 1,
            checkpoint_every_blocks: 4096,
        },
        track_buffer: 16,
        poll_ms: 1,
    }
}

/// First/last block numbers of a fixture; panics on empty input — populate
/// helpers want a hard failure rather than a silently empty store.
fn block_bounds(blocks: &[FinalizedBlock]) -> (u64, u64) {
    assert!(!blocks.is_empty(), "populate needs at least one block");
    (
        blocks.first().unwrap().header.number,
        blocks.last().unwrap().header.number,
    )
}

/// Ingests `blocks` (which must be parent-linked and contiguously numbered)
/// into fresh in-memory stores, publishing head = last block.
/// Panics on empty input or ingest error — tests want a hard failure.
pub async fn populate_via_engine(blocks: Vec<FinalizedBlock>) -> PopulatedStore {
    try_populate_via_engine(blocks)
        .await
        .expect("backfill ingest")
}

/// Fallible variant of [`populate_via_engine`] for negative tests: returns the
/// engine's error instead of panicking.
pub async fn try_populate_via_engine(
    blocks: Vec<FinalizedBlock>,
) -> Result<PopulatedStore, MonadChainDataError> {
    let (start, end) = block_bounds(&blocks);
    run_engine(blocks, populate_config(start, end), DictConfig::default()).await
}

/// Like [`populate_via_engine`] but with a custom [`DictConfig`] so the
/// epoch-based dictionary lifecycle (small `epoch_blocks`) can be exercised.
pub async fn populate_via_engine_with_dict(
    blocks: Vec<FinalizedBlock>,
    dict: DictConfig,
) -> PopulatedStore {
    let (start, end) = block_bounds(&blocks);
    run_engine(blocks, populate_config(start, end), dict)
        .await
        .expect("backfill ingest")
}

/// Ingests `blocks` (parent-linked continuations of what `store` holds) into
/// the SAME stores via a fresh engine run, advancing the published head to
/// the new last block — the fixture for read paths that must observe a head
/// advance (e.g. the open-region fold caches).
pub async fn populate_more_via_engine(store: &PopulatedStore, blocks: Vec<FinalizedBlock>) {
    let (start, end) = block_bounds(&blocks);
    let meta = store.meta.clone();
    let blob = store.blob.clone();
    let snapshots = SnapshotStore::new(meta.clone(), blob.clone());
    let tables = Arc::new(Tables::with_all_configs(
        meta.clone(),
        blob.clone(),
        CacheConfig::default(),
        DictConfig::default(),
        QueryRuntimeConfig::default(),
    ));
    let resolver = TablesCodecResolver::new(tables.clone());
    let publisher = Arc::new(PublicationTables::new(meta.clone()));
    let source = VecSource::new(blocks, start);

    run_ingest(
        source,
        tables,
        publisher,
        snapshots,
        resolver,
        populate_config(start, end),
        TEST_PREFETCH,
    )
    .await
    .expect("continuation ingest");
}

/// Wires tables/snapshots/resolver/publisher over fresh in-memory stores and
/// drives the engine to completion, publishing head = last block.
async fn run_engine(
    blocks: Vec<FinalizedBlock>,
    config: IngestRunConfig,
    dict: DictConfig,
) -> Result<PopulatedStore, MonadChainDataError> {
    let meta = InMemoryMetaStore::default();
    let blob = InMemoryBlobStore::default();
    let snapshots = SnapshotStore::new(meta.clone(), blob.clone());
    let tables = Arc::new(Tables::with_all_configs(
        meta.clone(),
        blob.clone(),
        CacheConfig::default(),
        dict,
        QueryRuntimeConfig::default(),
    ));
    let resolver = TablesCodecResolver::new(tables.clone());
    let publisher = Arc::new(PublicationTables::new(meta.clone()));
    let source = VecSource::new(blocks, config.start);

    run_ingest(
        source,
        tables,
        publisher,
        snapshots,
        resolver,
        config,
        TEST_PREFETCH,
    )
    .await?;

    Ok(PopulatedStore { meta, blob })
}
