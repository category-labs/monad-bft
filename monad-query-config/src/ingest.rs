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
use monad_query_write::source::ChainDataIngestSource;
#[cfg(any(feature = "dynamo", feature = "mongo"))]
use monad_query_write::{
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ChainDataEngineConfig {
    pub payload: ChainDataPayloadConfig,
    pub end: Option<u64>,
    pub count: Option<u64>,
    pub unsafe_seed_begin: Option<u64>,
    pub pack_target_bytes: usize,
    pub pack_max_blocks: usize,
    pub tip_lag_divisor: u64,
    pub checkpoint_every_blocks: u64,
    pub track_buffer: usize,
    pub poll_ms: u64,
    pub fetch_concurrency: usize,
    pub fetch_buffer: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChainDataPayloadConfig {
    #[default]
    Native,
    ExternalArchive,
}

impl From<ChainDataPayloadConfig> for monad_query_write::PayloadMode {
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
            tip_lag_divisor: 10,
            checkpoint_every_blocks: 10_000,
            track_buffer: 2048,
            poll_ms: 50,
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
        #[cfg(feature = "mongo")]
        ChainDataMetaBackendConfig::Mongo(meta_config) => {
            let meta_store = build_mongo_meta_store(meta_config).await?;
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
        None => {
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
    let snapshots = if checkpoints_enabled {
        SnapshotStore::new(meta_store.clone(), blob_store.clone())
    } else {
        SnapshotStore::without_payloads(meta_store.clone(), blob_store.clone())
    };
    let mut tables = Tables::with_all_configs(
        meta_store.clone(),
        blob_store,
        cache_config,
        DictConfig::default(),
        QueryRuntimeConfig::default(),
    );
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
            start: 0,
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

    #[test]
    fn blobless_ingest_rejects_native_payload() {
        let engine = ChainDataEngineConfig {
            payload: ChainDataPayloadConfig::Native,
            ..ChainDataEngineConfig::default()
        };
        let err = validate_blobless_ingest(&blobless_store(), &engine).unwrap_err();
        assert!(err.to_string().contains("external-archive"), "{err}");
    }

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

    #[cfg(feature = "s3")]
    #[test]
    fn blobless_ingest_accepts_external_payload() {
        let engine = ChainDataEngineConfig {
            payload: ChainDataPayloadConfig::ExternalArchive,
            ..ChainDataEngineConfig::default()
        };
        assert!(validate_blobless_ingest(&blobless_store(), &engine).is_ok());
    }

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
