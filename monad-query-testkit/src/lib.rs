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

use monad_query_engine::tables::{DictConfig, PublicationTables, QueryRuntimeConfig, Tables};
use monad_query_errors::QueryError;
use monad_query_primitives::{limits::QueryLimits, ExternalBlobReader};
use monad_query_read::api::MonadChainDataService;
use monad_query_store::{
    BlobStore, CacheConfig, InMemoryBlobStore, InMemoryMetaStore, NullBlobStore,
};
use monad_query_types::ingest_types::FinalizedBlock;
use monad_query_write::ingest::{
    resolver::TablesCodecResolver, run_ingest, source::ChainDataIngestSource, IngestRunConfig,
    PackConfig, PayloadMode, Prefetch, SignalPolicy, SnapshotStore,
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
/// `B` is [`InMemoryBlobStore`] except for blob-less external fixtures
/// ([`populate_via_engine_external_no_blob`]), which run over
/// [`NullBlobStore`].
pub struct PopulatedStore<B: BlobStore = InMemoryBlobStore> {
    pub meta: InMemoryMetaStore,
    pub blob: B,
    /// External archive reader attached to every built service (population
    /// through [`populate_via_engine_external`]); `None` for native stores.
    pub external: Option<Arc<dyn ExternalBlobReader>>,
}

impl<B: BlobStore> PopulatedStore<B> {
    /// A read-only query service over the SAME stores the engine wrote to.
    pub fn reader(&self) -> MonadChainDataService<InMemoryMetaStore, B> {
        self.reader_with_limits(QueryLimits::UNLIMITED)
    }

    /// A read-only query service with explicit [`QueryLimits`].
    pub fn reader_with_limits(
        &self,
        limits: QueryLimits,
    ) -> MonadChainDataService<InMemoryMetaStore, B> {
        let service = MonadChainDataService::new(self.meta.clone(), self.blob.clone(), limits);
        match &self.external {
            Some(reader) => service.with_external_payload_reader(Arc::clone(reader)),
            None => service,
        }
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
fn populate_config(start: u64, end: u64, payload: PayloadMode) -> IngestRunConfig {
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
            checkpoints_enabled: true,
        },
        payload,
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
) -> Result<PopulatedStore, QueryError> {
    let (start, end) = block_bounds(&blocks);
    run_engine(
        blocks,
        populate_config(start, end, PayloadMode::Native),
        DictConfig::default(),
        None,
        InMemoryBlobStore::default(),
    )
    .await
}

/// Like [`populate_via_engine`] but with a custom [`DictConfig`] so the
/// epoch-based dictionary lifecycle (small `epoch_blocks`) can be exercised.
pub async fn populate_via_engine_with_dict(
    blocks: Vec<FinalizedBlock>,
    dict: DictConfig,
) -> PopulatedStore {
    let (start, end) = block_bounds(&blocks);
    run_engine(
        blocks,
        populate_config(start, end, PayloadMode::Native),
        dict,
        None,
        InMemoryBlobStore::default(),
    )
    .await
    .expect("backfill ingest")
}

/// External-payload variant of [`populate_via_engine`]: every block must
/// carry an [`monad_query_types::ExternalPayloadSpec`], and `external` must
/// serve the archive objects the specs point into. The reader is attached to
/// every service built from the returned store.
pub async fn try_populate_via_engine_external(
    blocks: Vec<FinalizedBlock>,
    external: Arc<dyn ExternalBlobReader>,
) -> Result<PopulatedStore, QueryError> {
    let (start, end) = block_bounds(&blocks);
    run_engine(
        blocks,
        populate_config(start, end, PayloadMode::ExternalArchive),
        DictConfig::default(),
        Some(external),
        InMemoryBlobStore::default(),
    )
    .await
}

/// Panicking variant of [`try_populate_via_engine_external`].
pub async fn populate_via_engine_external(
    blocks: Vec<FinalizedBlock>,
    external: Arc<dyn ExternalBlobReader>,
) -> PopulatedStore {
    try_populate_via_engine_external(blocks, external)
        .await
        .expect("external backfill ingest")
}

/// Blob-less external-payload population: the engine runs over
/// [`NullBlobStore`] with checkpoints disabled, mirroring an ingest assembly
/// whose `[store.blob]` is omitted. Any blob access errors the run (see
/// [`NullBlobStore`]), so a successful populate doubles as the proof that an
/// external-archive ingest issues ZERO blob-store calls.
pub async fn populate_via_engine_external_no_blob(
    blocks: Vec<FinalizedBlock>,
    external: Arc<dyn ExternalBlobReader>,
) -> PopulatedStore<NullBlobStore> {
    let (start, end) = block_bounds(&blocks);
    let mut config = populate_config(start, end, PayloadMode::ExternalArchive);
    config.policy.checkpoints_enabled = false;
    run_engine(
        blocks,
        config,
        DictConfig::default(),
        Some(external),
        NullBlobStore,
    )
    .await
    .expect("blob-less external backfill ingest")
}

/// Ingests `blocks` (parent-linked continuations of what `store` holds) into
/// the SAME stores via a fresh engine run, advancing the published head to
/// the new last block — the fixture for read paths that must observe a head
/// advance (e.g. the open-region fold caches).
pub async fn populate_more_via_engine(store: &PopulatedStore, blocks: Vec<FinalizedBlock>) {
    let (start, end) = block_bounds(&blocks);
    run_more_engine(
        store,
        blocks,
        populate_config(start, end, PayloadMode::Native),
    )
    .await;
}

/// Blob-less external continuation: a SECOND engine run over the same meta
/// store, with no checkpoint to resume from — the engine rebuilds its open
/// state from the fragments at the published head, then ingests the
/// continuation blocks (the restartability fixture for checkpoint-less
/// stores).
pub async fn populate_more_via_engine_external_no_blob(
    store: &PopulatedStore<NullBlobStore>,
    blocks: Vec<FinalizedBlock>,
) {
    let (start, end) = block_bounds(&blocks);
    let mut config = populate_config(start, end, PayloadMode::ExternalArchive);
    config.policy.checkpoints_enabled = false;
    run_more_engine(store, blocks, config).await;
}

/// Shared continuation body: wires a fresh engine over `store`'s existing
/// backing stores and drives it to completion.
async fn run_more_engine<B: BlobStore>(
    store: &PopulatedStore<B>,
    blocks: Vec<FinalizedBlock>,
    config: IngestRunConfig,
) {
    let meta = store.meta.clone();
    let blob = store.blob.clone();
    let snapshots = if config.policy.checkpoints_enabled {
        SnapshotStore::new(meta.clone(), blob.clone())
    } else {
        SnapshotStore::without_payloads(meta.clone(), blob.clone())
    };
    let mut tables = Tables::with_all_configs(
        meta.clone(),
        blob,
        CacheConfig::default(),
        DictConfig::default(),
        QueryRuntimeConfig::default(),
    );
    if let Some(reader) = &store.external {
        tables = tables.with_external_payload_reader(Arc::clone(reader));
    }
    let tables = Arc::new(tables);
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
    .await
    .expect("continuation ingest");
}

/// Wires tables/snapshots/resolver/publisher over a fresh in-memory meta
/// store (and the given blob store) and drives the engine to completion,
/// publishing head = last block.
async fn run_engine<B: BlobStore>(
    blocks: Vec<FinalizedBlock>,
    config: IngestRunConfig,
    dict: DictConfig,
    external: Option<Arc<dyn ExternalBlobReader>>,
    blob: B,
) -> Result<PopulatedStore<B>, QueryError> {
    let meta = InMemoryMetaStore::default();
    let snapshots = if config.policy.checkpoints_enabled {
        SnapshotStore::new(meta.clone(), blob.clone())
    } else {
        SnapshotStore::without_payloads(meta.clone(), blob.clone())
    };
    let mut tables = Tables::with_all_configs(
        meta.clone(),
        blob.clone(),
        CacheConfig::default(),
        dict,
        QueryRuntimeConfig::default(),
    );
    if let Some(reader) = &external {
        tables = tables.with_external_payload_reader(Arc::clone(reader));
    }
    let tables = Arc::new(tables);
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

    Ok(PopulatedStore {
        meta,
        blob,
        external,
    })
}
