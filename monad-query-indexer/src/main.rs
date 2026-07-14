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

//! Standalone chain-data ingest binary: runs `chain_data_ingest_worker`
//! against a block-data source, alongside an unmodified `monad-archiver` /
//! `monad-indexer` deployment writing to the same chain-data store.

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use eyre::{eyre, Result, WrapErr};
use monad_archive::{
    cli::BlockDataReaderArgs,
    prelude::{warn, Metrics},
};
use monad_query_indexer::chain_data_ingest::{chain_data_ingest_worker, ChainDataIngestConfig};

#[derive(Debug, Parser)]
#[command(name = "monad-query-indexer", about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Source to read block data from for ingestion
    #[arg(long, env = "BLOCK_DATA_SOURCE", value_parser = clap::value_parser!(BlockDataReaderArgs))]
    block_data_source: Option<BlockDataReaderArgs>,

    /// If reading from --block-data-source fails, attempts to read from this
    /// optional fallback
    #[arg(long, env = "FALLBACK_BLOCK_DATA_SOURCE", value_parser = clap::value_parser!(BlockDataReaderArgs))]
    fallback_block_data_source: Option<BlockDataReaderArgs>,

    /// TOML config for the chain-data ingest worker: either the archiver
    /// daemon shape (a `[chain_data_ingest]` table) or a bare
    /// `ChainDataIngestConfig`.
    #[arg(long, env = "CHAIN_DATA_CONFIG")]
    chain_data_config: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the chain-data store's published head and exit.
    ChainDataHead {
        /// TOML config; only `[chain_data_ingest.store]` (or a bare
        /// `[store]` table) is read.
        #[arg(long, env = "CHAIN_DATA_CONFIG")]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Honor RUST_LOG (default info) like the archiver/indexer binaries.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    if let Some(Command::ChainDataHead { config }) = cli.command {
        return print_chain_data_head(&config).await;
    }

    let block_data_source = cli
        .block_data_source
        .ok_or_else(|| eyre!("--block-data-source is required"))?;
    let chain_data_config_path = cli
        .chain_data_config
        .ok_or_else(|| eyre!("--chain-data-config is required"))?;
    let config = load_chain_data_config(&chain_data_config_path)?;
    if !config.enabled {
        warn!(
            "chain-data ingest is disabled in {}; exiting",
            chain_data_config_path.display()
        );
        return Ok(());
    }

    // No otel wiring here; the co-located archiver/indexer already reports
    // source/sink metrics for this deployment.
    let metrics = Metrics::none();
    let source = block_data_source.build(&metrics).await?;
    let fallback = match cli.fallback_block_data_source {
        Some(fallback) => Some(fallback.build(&metrics).await?),
        None => None,
    };

    chain_data_ingest_worker(source, fallback, config).await
}

/// Accepts either the daemon shape (a `[chain_data_ingest]` table) or a bare
/// `ChainDataIngestConfig`, so an archiver/indexer's own config file can be
/// pointed at directly.
fn load_chain_data_config(path: &Path) -> Result<ChainDataIngestConfig> {
    #[derive(Default, serde::Deserialize)]
    #[serde(default)]
    struct DaemonShape {
        chain_data_ingest: Option<ChainDataIngestConfig>,
    }

    let contents = std::fs::read_to_string(path)
        .wrap_err_with(|| format!("failed to read chain-data config {}", path.display()))?;
    let daemon: DaemonShape = toml::from_str(&contents)
        .wrap_err_with(|| format!("failed to parse chain-data config {}", path.display()))?;
    match daemon.chain_data_ingest {
        Some(config) => Ok(config),
        None => toml::from_str(&contents).wrap_err_with(|| {
            format!(
                "config {} has neither a [chain_data_ingest] table nor a bare chain-data \
                 ingest config",
                path.display()
            )
        }),
    }
}

async fn print_chain_data_head(config: &Path) -> Result<()> {
    // Accepts both deployed config shapes: the archiver/indexer daemon TOML
    // ([chain_data_ingest.store]) and a bare reader TOML ([store]).
    #[derive(serde::Deserialize)]
    struct HeadToml {
        chain_data_ingest: Option<ChainDataIngestConfig>,
        store: Option<monad_query_config::ChainDataStoreConfig>,
    }
    let contents = std::fs::read_to_string(config)
        .wrap_err_with(|| format!("failed to read config {}", config.display()))?;
    let parsed: HeadToml = toml::from_str(&contents)?;
    let store = match (parsed.chain_data_ingest, parsed.store) {
        (Some(ingest), _) => ingest.store,
        (None, Some(store)) => store,
        (None, None) => {
            return Err(eyre!(
                "config {} has neither [chain_data_ingest.store] nor [store]",
                config.display()
            ))
        }
    };

    let external =
        monad_query_indexer::chain_data_external::build_archive_external_reader(&store.archive)
            .await?;
    let reader = monad_query_config::open_configured_chain_data_reader(
        store,
        monad_query_primitives::limits::QueryLimits::UNLIMITED,
        external,
    )
    .await?;
    match reader.load_published_head().await? {
        Some(head) => println!("published head: {head}"),
        None => println!("published head: none (nothing published yet)"),
    }
    Ok(())
}
