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

//! The chain-data ingest worker: feeds the queryX ingest engine from a
//! block-data source. Shared by `monad-archiver` and `monad-indexer`, whose
//! TOML configs both carry a `[chain_data_ingest]` section.

use eyre::{bail, Result};
use serde::Deserialize;

use crate::{chain_data_source::ArchiverChainDataSource, model::BlockDataReaderErased};

/// The `[chain_data_ingest]` config section: worker switch plus the
/// chain-data store/engine configs passed through verbatim.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ChainDataIngestConfig {
    pub enabled: bool,
    /// Index the archive's existing objects instead of writing payload blobs
    /// (requires an archive-backed `block_data_source`). Must agree with
    /// `engine.payload = "external-archive"` — validated at startup.
    pub index_archive_payload: bool,
    pub store: monad_query_config::ChainDataStoreConfig,
    pub engine: monad_query_config::ChainDataEngineConfig,
}

pub async fn chain_data_ingest_worker(
    block_data_source: BlockDataReaderErased,
    fallback_block_data_source: Option<BlockDataReaderErased>,
    config: ChainDataIngestConfig,
) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }
    config.store.validate_ingest()?;
    config.engine.validate()?;
    // The worker flag and the engine's payload mode express the same intent
    // at two layers; require them to agree so a half-edited config cannot
    // silently ingest in the wrong mode.
    let external_engine =
        config.engine.payload == monad_query_config::ChainDataPayloadConfig::ExternalArchive;
    if config.index_archive_payload != external_engine {
        bail!(
            "chain_data_ingest.index_archive_payload ({}) must match engine.payload ({:?})",
            config.index_archive_payload,
            config.engine.payload,
        );
    }
    let source = if config.index_archive_payload {
        ArchiverChainDataSource::external(block_data_source, fallback_block_data_source)?
    } else {
        ArchiverChainDataSource::native(block_data_source, fallback_block_data_source)
    };
    // Archive-format external readers (mongo/dynamo) are built here;
    // chain-data builds the S3 one from config itself.
    let external =
        crate::chain_data_external::build_archive_external_reader(&config.store.archive).await?;
    monad_query_config::run_configured_chain_data_engine_ingest(
        config.store,
        config.engine,
        source,
        external,
    )
    .await
}
