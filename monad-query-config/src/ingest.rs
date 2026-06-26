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

//! The configured ingest entry point: backend dispatch, store assembly, and
//! handoff to the branchless engine (`run_ingest`).

#[cfg(any(feature = "dynamo", feature = "mongo"))]
use std::sync::Arc;

use eyre::{bail, Result};
#[cfg(any(feature = "dynamo", feature = "mongo"))]
use monad_query_engine::tables::{DictConfig, PublicationTables, QueryRuntimeConfig, Tables};
#[cfg(any(feature = "dynamo", feature = "mongo"))]
use monad_query_store::{BlobStore, MetaStore};
use monad_query_write::ingest::source::ChainDataIngestSource;
#[cfg(any(feature = "dynamo", feature = "mongo"))]
use monad_query_write::ingest::{
    resolver::TablesCodecResolver, run_ingest, snapshot::seed_snapshot_at, IngestRunConfig,
    PackConfig, Prefetch, SignalPolicy, SnapshotStore,
};
#[cfg(any(feature = "dynamo", feature = "mongo"))]
use tracing::{info, warn};

#[cfg(feature = "mongo")]
use super::build_mongo_meta_store;
#[cfg(all(feature = "s3", feature = "dynamo"))]
use super::build_s3_blob_store;
#[cfg(feature = "dynamo")]
use super::{build_dynamo_blob_store, build_dynamo_meta_store};
#[cfg(any(feature = "dynamo", feature = "mongo"))]
use super::{resolve_cache_config, ChainDataCacheMode};
#[cfg(feature = "dynamo")]
use super::{ChainDataBlobBackendConfig, SharedDynamoConnection};
use super::{ChainDataMetaBackendConfig, ChainDataStoreConfig};

/// Cadence/range knobs for the branchless ingest engine. There is deliberately
/// no `start` knob: the begin block is always derived from store state (resume
/// snapshot's `last_block + 1`, or genesis on an empty store) because the query
/// layer assumes a gap-free range up to the head; `end`/`count` only bound how
/// far this run goes from there.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataEngineConfig {
    /// Where row payloads live: `"native"` re-encodes rows into chain-data
    /// pack blobs; `"external-archive"` indexes the source's existing
    /// monad-archive objects (every fetched block must carry an external
    /// payload spec, and readers need `[store.archive]` access). A store must
    /// keep one mode for its whole life.
    pub payload: ChainDataPayloadConfig,
    /// Inclusive absolute end block for a bounded backfill: the engine catches
    /// the resume point up to here, then stops. Absent → follow the tip (live).
    /// Restart-safe: re-running with the same `end` resumes and finishes the gap.
    pub end: Option<u64>,
    /// Bounded backfill of this many blocks from the resume point (mutually
    /// exclusive with `end`). NOT restart-idempotent — a restart counts from
    /// the *new* resume point — so prefer `end` for anything re-runnable.
    pub count: Option<u64>,
    /// UNSAFE test/fixture knob: seed a *fresh* store's recovery snapshot so
    /// ingest starts at this block instead of genesis. Rejected once the store
    /// is initialized (a snapshot exists). Breaks the gap-free-up-to-head
    /// invariant — queries below `begin` are invalid. Never use in production.
    pub unsafe_seed_begin: Option<u64>,
    /// Target packed-blob size before a data-track flush.
    pub pack_target_bytes: usize,
    /// Hard cap on blocks per packed blob.
    pub pack_max_blocks: usize,
    /// Adaptive `BatchFlush` interval: `max(distance_to_tip / tip_lag_divisor, 1)`,
    /// i.e. every block at the tip, coarser while catching up. Purely a
    /// reader-freshness knob: `OpenTail` memory is bounded by `seal`, not flush
    /// cadence, so there is no cap.
    pub tip_lag_divisor: u64,
    /// Snapshot `OpenState` every this many blocks (recovery resume point /
    /// fragment-replay bound).
    pub checkpoint_every_blocks: u64,
    /// Inter-track channel buffer depth.
    pub track_buffer: usize,
    /// Tip poll interval (ms) for re-checking `get_latest` once caught up.
    /// Catch-up never polls mid-backlog.
    pub poll_ms: u64,
    /// Concurrent block fetches in the producer. Each fetch is spawned, so this
    /// also bounds parallel (CPU-bound) block decode. Only matters during
    /// catch-up; at the steady tip the backlog is ~1 block.
    pub fetch_concurrency: usize,
    /// Ordered look-ahead: max decoded-but-not-yet-ingested blocks queued ahead
    /// of the engine, so fetchers keep running through flush/checkpoint stalls.
    /// Memory scales with this × per-block size; shrink for constrained runs.
    pub fetch_buffer: usize,
}

/// Serde face of [`monad_query_write::ingest::PayloadMode`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChainDataPayloadConfig {
    #[default]
    Native,
    ExternalArchive,
}

impl From<ChainDataPayloadConfig> for monad_query_write::ingest::PayloadMode {
    fn from(config: ChainDataPayloadConfig) -> Self {
        match config {
            ChainDataPayloadConfig::Native => Self::Native,
            ChainDataPayloadConfig::ExternalArchive => Self::ExternalArchive,
        }
    }
}

impl Default for ChainDataEngineConfig {
    fn default() -> Self {
        Self {
            payload: ChainDataPayloadConfig::default(),
            end: None,
            count: None,
            unsafe_seed_begin: None,
            pack_target_bytes: 8 * 1024 * 1024,
            pack_max_blocks: 10_000,
            // max(distance/10, 1): every block at the tip, ~10% of the
            // distance while catching up.
            tip_lag_divisor: 10,
            checkpoint_every_blocks: 10_000,
            // Deep enough that a multi-second index seal burst doesn't convoy
            // the producer and data track behind it.
            track_buffer: 2048,
            poll_ms: 50,
            // Sized for high-latency per-object sources (3 GETs/block); the
            // semaphore only fills during catch-up.
            fetch_concurrency: 2000,
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
        for (name, value) in [
            ("pack_target_bytes", self.pack_target_bytes as u64),
            ("pack_max_blocks", self.pack_max_blocks as u64),
            ("checkpoint_every_blocks", self.checkpoint_every_blocks),
            ("track_buffer", self.track_buffer as u64),
            ("fetch_concurrency", self.fetch_concurrency as u64),
            ("fetch_buffer", self.fetch_buffer as u64),
            ("tip_lag_divisor", self.tip_lag_divisor),
            ("poll_ms", self.poll_ms),
        ] {
            if value == 0 {
                bail!("chain-data engine {name} must be >= 1");
            }
        }
        Ok(())
    }
}

/// Cross-checks the store/engine pair for a blob-less store (`[store.blob]`
/// omitted). Such a store cannot hold pack blobs (so the payload must be
/// external) or snapshot payloads (so the seed knob, which writes one, is
/// rejected; the checkpoint cadence is auto-disabled rather than rejected).
fn validate_blobless_ingest(
    store_config: &ChainDataStoreConfig,
    engine_config: &ChainDataEngineConfig,
) -> Result<()> {
    if store_config.blob.is_some() {
        return Ok(());
    }
    if engine_config.payload != ChainDataPayloadConfig::ExternalArchive {
        bail!(
            "chain-data ingest without [store.blob] requires engine.payload = \
             \"external-archive\": native-payload ingest writes pack blobs"
        );
    }
    if engine_config.unsafe_seed_begin.is_some() {
        bail!(
            "chain-data unsafe_seed_begin writes a seed snapshot payload to the blob \
             store and cannot be combined with an omitted [store.blob]"
        );
    }
    if store_config.archive.is_none() {
        bail!(
            "a store without [store.blob] keeps its row payloads in the monad-archive \
             objects and requires [store.archive] so readers can reach them"
        );
    }
    Ok(())
}

/// Opens the configured stores and drives the branchless ingest engine
/// (`run_ingest`) over them. A bounded range (`end`/`count`)
/// selects backfill; otherwise it follows the tip.
/// `external`: a pre-built external payload reader (monad-archive's
/// archive-format readers); required for mongo/dynamo `[store.archive]`
/// backends, `None` lets the S3 backend build from config.
pub async fn run_configured_chain_data_engine_ingest<S>(
    store_config: ChainDataStoreConfig,
    engine_config: ChainDataEngineConfig,
    source: S,
    external: Option<std::sync::Arc<dyn monad_query_primitives::ExternalBlobReader>>,
) -> Result<()>
where
    S: ChainDataIngestSource,
{
    store_config.validate_ingest()?;
    engine_config.validate()?;
    validate_blobless_ingest(&store_config, &engine_config)?;
    #[cfg(not(any(feature = "dynamo", feature = "mongo")))]
    let _ = (source, external);

    match &store_config.meta {
        #[cfg(feature = "dynamo")]
        ChainDataMetaBackendConfig::Dynamo(meta_config) => {
            let meta_store = build_dynamo_meta_store(meta_config).await?;
            let shared = Some(meta_store.shared_connection());
            dispatch_blob_engine(
                &store_config,
                &engine_config,
                source,
                meta_store,
                shared,
                external,
            )
            .await
        }
        // Mongo meta is blob-less only (validated), so it always runs the
        // external-archive engine over the loud NullBlobStore: checkpoints
        // are disabled and recovery rebuilds from fragments.
        #[cfg(feature = "mongo")]
        ChainDataMetaBackendConfig::Mongo(meta_config) => {
            let meta_store = build_mongo_meta_store(meta_config).await?;
            info!(
                "chain-data mongo meta store is blob-less; checkpoints are disabled — \
                 recovery rebuilds from fragments"
            );
            run_engine_with_store(
                &store_config,
                &engine_config,
                source,
                meta_store,
                monad_query_store::NullBlobStore,
                false,
                external,
            )
            .await
        }
        #[cfg(not(any(feature = "dynamo", feature = "mongo")))]
        ChainDataMetaBackendConfig::Unavailable => {
            bail!("chain-data configured ingest requires the dynamo or mongo feature")
        }
    }
}

#[cfg(feature = "dynamo")]
async fn dispatch_blob_engine<M, S>(
    store_config: &ChainDataStoreConfig,
    engine_config: &ChainDataEngineConfig,
    source: S,
    meta_store: M,
    dynamo_connection: Option<SharedDynamoConnection>,
    external: Option<std::sync::Arc<dyn monad_query_primitives::ExternalBlobReader>>,
) -> Result<()>
where
    M: MetaStore,
    S: ChainDataIngestSource,
{
    match &store_config.blob {
        #[cfg(feature = "s3")]
        Some(ChainDataBlobBackendConfig::S3(blob_config)) => {
            let blob_store = build_s3_blob_store(blob_config).await?;
            run_engine_with_store(
                store_config,
                engine_config,
                source,
                meta_store,
                blob_store,
                true,
                external,
            )
            .await
        }
        Some(ChainDataBlobBackendConfig::Dynamo(blob_config)) => {
            let blob_store = build_dynamo_blob_store(blob_config, dynamo_connection).await?;
            run_engine_with_store(
                store_config,
                engine_config,
                source,
                meta_store,
                blob_store,
                true,
                external,
            )
            .await
        }
        #[cfg(not(any(feature = "s3", feature = "dynamo")))]
        Some(ChainDataBlobBackendConfig::Unavailable) => {
            bail!("chain-data configured ingest requires s3 or dynamo blob storage")
        }
        // No blob store: external-archive payload (validated upstream) over the
        // loud NullBlobStore. Checkpoints persist their snapshot payload as a
        // blob object, so they are auto-disabled; recovery rebuilds the open
        // state from fragments at the published head.
        None => {
            info!(
                "chain-data blob store not configured ([store.blob] omitted); \
                 checkpoints are disabled — recovery rebuilds from fragments"
            );
            run_engine_with_store(
                store_config,
                engine_config,
                source,
                meta_store,
                monad_query_store::NullBlobStore,
                false,
                external,
            )
            .await
        }
    }
}

#[cfg(any(feature = "dynamo", feature = "mongo"))]
#[allow(clippy::too_many_arguments)]
async fn run_engine_with_store<M, B, S>(
    store_config: &ChainDataStoreConfig,
    engine_config: &ChainDataEngineConfig,
    source: S,
    meta_store: M,
    blob_store: B,
    checkpoints_enabled: bool,
    external: Option<std::sync::Arc<dyn monad_query_primitives::ExternalBlobReader>>,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
    S: ChainDataIngestSource,
{
    let cache_config = resolve_cache_config(store_config, ChainDataCacheMode::Ingest);
    // The snapshot store writes its payload blobs through the same blob backend
    // the engine packs blocks into (under its own blob table). A blob-less run
    // gets the payload-less variant, which also unwedges recovery from any
    // manifest a blob-backed past left behind.
    let snapshots = if checkpoints_enabled {
        SnapshotStore::new(meta_store.clone(), blob_store.clone())
    } else {
        SnapshotStore::without_payloads(meta_store.clone(), blob_store.clone())
    };
    // One Arc<Tables> is shared by the engine (writes) and the resolver (dict
    // training reads back through the same caches).
    let mut tables = Tables::with_all_configs(
        meta_store.clone(),
        blob_store,
        cache_config,
        DictConfig::default(),
        QueryRuntimeConfig::default(),
    );
    // Ingest never reads external payloads itself (dict training is disabled
    // in external mode), but attaching configured archive access keeps the
    // shared `Tables` fully wired for any read path it serves.
    if let Some(reader) =
        super::build_external_payload_reader(&store_config.archive, external).await?
    {
        tables = tables.with_external_payload_reader(reader);
    }
    let tables = Arc::new(tables);
    let publisher = Arc::new(PublicationTables::new(meta_store));
    let resolver = TablesCodecResolver::new(tables.clone());

    if let Some(begin) = engine_config.unsafe_seed_begin {
        if snapshots.is_initialized().await? {
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

    let prefetch = Prefetch {
        concurrency: engine_config.fetch_concurrency,
        buffer: engine_config.fetch_buffer,
    };

    // `end`/`count` only bound how far this run goes (absent ⇒ follow forever);
    // the engine derives the begin block from store state.
    info!(
        end = engine_config.end,
        count = engine_config.count,
        payload = ?engine_config.payload,
        "starting chain-data ingest (branchless engine)"
    );
    run_ingest(
        source,
        tables,
        publisher,
        snapshots,
        resolver,
        IngestRunConfig {
            start: 0, // genesis cold-start floor; a warm resume overrides it
            end: engine_config.end,
            count: engine_config.count,
            policy: SignalPolicy {
                tip_lag_divisor: engine_config.tip_lag_divisor,
                checkpoint_every_blocks: engine_config.checkpoint_every_blocks,
                checkpoints_enabled,
            },
            pack: PackConfig {
                target_bytes: engine_config.pack_target_bytes,
                max_blocks: engine_config.pack_max_blocks,
            },
            payload: engine_config.payload.into(),
            track_buffer: engine_config.track_buffer,
            poll_ms: engine_config.poll_ms,
        },
        prefetch,
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blobless_store() -> ChainDataStoreConfig {
        ChainDataStoreConfig {
            blob: None,
            #[cfg(feature = "s3")]
            archive: Some(crate::ChainDataArchiveBackendConfig::S3(
                crate::ChainDataArchiveS3Config::default(),
            )),
            ..ChainDataStoreConfig::default()
        }
    }

    /// Blob-less stores cannot hold pack blobs, so a native payload must be
    /// rejected at assembly with a clear message.
    #[test]
    fn blobless_ingest_rejects_native_payload() {
        let engine = ChainDataEngineConfig {
            payload: ChainDataPayloadConfig::Native,
            ..ChainDataEngineConfig::default()
        };
        let err = validate_blobless_ingest(&blobless_store(), &engine).unwrap_err();
        assert!(err.to_string().contains("external-archive"), "{err}");
    }

    /// `unsafe_seed_begin` writes a seed snapshot payload (a blob object), so
    /// it must be rejected on a blob-less store.
    #[test]
    fn blobless_ingest_rejects_unsafe_seed_begin() {
        let engine = ChainDataEngineConfig {
            payload: ChainDataPayloadConfig::ExternalArchive,
            unsafe_seed_begin: Some(7),
            ..ChainDataEngineConfig::default()
        };
        let err = validate_blobless_ingest(&blobless_store(), &engine).unwrap_err();
        assert!(err.to_string().contains("unsafe_seed_begin"), "{err}");
    }

    /// The supported blob-less shape: external-archive payload, no seed,
    /// archive access configured.
    #[cfg(feature = "s3")]
    #[test]
    fn blobless_ingest_accepts_external_payload() {
        let engine = ChainDataEngineConfig {
            payload: ChainDataPayloadConfig::ExternalArchive,
            ..ChainDataEngineConfig::default()
        };
        assert!(validate_blobless_ingest(&blobless_store(), &engine).is_ok());
    }

    /// Without [store.archive] a blob-less store could not serve its own row
    /// payloads; rejected at assembly rather than at first query.
    #[test]
    fn blobless_ingest_requires_archive_access() {
        let engine = ChainDataEngineConfig {
            payload: ChainDataPayloadConfig::ExternalArchive,
            ..ChainDataEngineConfig::default()
        };
        let store = ChainDataStoreConfig {
            blob: None,
            archive: None,
            ..ChainDataStoreConfig::default()
        };
        let err = validate_blobless_ingest(&store, &engine).unwrap_err();
        assert!(err.to_string().contains("[store.archive]"), "{err}");
    }

    /// With a blob backend configured the cross-check imposes nothing (native
    /// payload and the seed knob stay valid).
    #[cfg(feature = "dynamo")]
    #[test]
    fn configured_blob_store_imposes_no_payload_constraint() {
        let store = ChainDataStoreConfig {
            blob: Some(ChainDataBlobBackendConfig::Dynamo(
                crate::ChainDataDynamoBlobConfig::default(),
            )),
            ..ChainDataStoreConfig::default()
        };
        let engine = ChainDataEngineConfig {
            payload: ChainDataPayloadConfig::Native,
            unsafe_seed_begin: Some(7),
            ..ChainDataEngineConfig::default()
        };
        assert!(validate_blobless_ingest(&store, &engine).is_ok());
    }
}
