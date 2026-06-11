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

#![allow(async_fn_in_trait)]

use eyre::{bail, Result};
use monad_archive::{chain_data_source::ArchiverChainDataSource, model::BlockDataReaderErased};
use monad_chain_data::{run_configured_chain_data_engine_ingest, ChainDataPayloadConfig};

use super::cli::ArchiverChainDataIngestConfig;

pub async fn chain_data_ingest_worker(
    block_data_source: BlockDataReaderErased,
    config: ArchiverChainDataIngestConfig,
) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }
    config.store.validate_ingest()?;
    config.engine.validate()?;
    // The worker flag and the engine's payload mode express the same intent
    // at two layers; require them to agree so a half-edited config cannot
    // silently ingest in the wrong mode.
    let external_engine = config.engine.payload == ChainDataPayloadConfig::ExternalArchive;
    if config.index_archive_payload != external_engine {
        bail!(
            "chain_data_ingest.index_archive_payload ({}) must match engine.payload ({:?})",
            config.index_archive_payload,
            config.engine.payload,
        );
    }
    let source = if config.index_archive_payload {
        ArchiverChainDataSource::external(block_data_source)?
    } else {
        ArchiverChainDataSource::native(block_data_source)
    };
    run_configured_chain_data_engine_ingest(config.store, config.engine, source).await
}
