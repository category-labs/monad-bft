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

use std::{
    fs,
    path::{Path, PathBuf},
    process,
};

use clap::{ArgAction, Parser, Subcommand};
use eyre::{eyre, Context, Result};
use monad_archive::cli::{ArchiveArgs, BlockDataReaderArgs};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Cli {
    #[serde(default)]
    pub command: Option<Commands>,

    /// Source to read block data that will be indexed
    pub block_data_source: BlockDataReaderArgs,

    /// Where archive data is written to
    /// For aws: 'aws <bucket_name> <concurrent_requests>'
    pub archive_sink: ArchiveArgs,

    #[serde(default = "default_max_blocks_per_iteration")]
    pub max_blocks_per_iteration: u64,

    #[serde(default = "default_max_concurrent_blocks")]
    pub max_concurrent_blocks: usize,

    /// Resets the latest indexed entry
    #[serde(default)]
    pub reset_index: bool,

    /// Override block number to start at
    pub start_block: Option<u64>,

    /// Override block number to stop at
    pub stop_block: Option<u64>,

    /// Endpoint to push metrics to
    pub otel_endpoint: Option<String>,

    pub otel_replica_name_override: Option<String>,

    /// Maximum size of an encoded inline tx index entry
    /// If an entry is larger than this, it is stored as a reference pointing to
    /// the block level data store
    #[serde(default = "default_max_inline_encoded_len")]
    pub max_inline_encoded_len: usize,

    #[serde(default)]
    pub skip_connectivity_check: bool,

    /// Enable eth_getLogs indexing (disabled by default)
    #[serde(default)]
    pub enable_logs_indexing: bool,
}

impl Cli {
    pub fn parse() -> Self {
        Self::try_parse().unwrap_or_else(|err| {
            eprintln!("failed to load monad-indexer configuration: {err:?}");
            process::exit(2);
        })
    }

    pub fn try_parse() -> Result<Self> {
        CliArgs::parse().into_cli()
    }

    fn from_sources(
        config: Option<Cli>,
        overrides: CliOverrides,
        command: Option<Commands>,
    ) -> Result<Self> {
        let mut cli = match config {
            Some(mut cli) => {
                cli.apply_overrides(overrides);
                cli
            }
            None => Cli::from_overrides(overrides)?,
        };

        if let Some(command) = command {
            cli.command = Some(command);
        }

        Ok(cli)
    }

    fn from_overrides(overrides: CliOverrides) -> Result<Self> {
        let CliOverrides {
            block_data_source,
            archive_sink,
            max_blocks_per_iteration,
            max_concurrent_blocks,
            reset_index,
            start_block,
            stop_block,
            otel_endpoint,
            otel_replica_name_override,
            max_inline_encoded_len,
            skip_connectivity_check,
            enable_logs_indexing,
        } = overrides;

        Ok(Self {
            command: None,
            block_data_source: block_data_source
                .ok_or_else(|| eyre!("block_data_source must be provided via CLI or config"))?,
            archive_sink: archive_sink
                .ok_or_else(|| eyre!("archive_sink must be provided via CLI or config"))?,
            max_blocks_per_iteration: max_blocks_per_iteration
                .unwrap_or_else(default_max_blocks_per_iteration),
            max_concurrent_blocks: max_concurrent_blocks
                .unwrap_or_else(default_max_concurrent_blocks),
            reset_index: reset_index.unwrap_or(false),
            start_block,
            stop_block,
            otel_endpoint,
            otel_replica_name_override,
            max_inline_encoded_len: max_inline_encoded_len
                .unwrap_or_else(default_max_inline_encoded_len),
            skip_connectivity_check: skip_connectivity_check.unwrap_or(false),
            enable_logs_indexing: enable_logs_indexing.unwrap_or(false),
        })
    }

    fn apply_overrides(&mut self, overrides: CliOverrides) {
        let CliOverrides {
            block_data_source,
            archive_sink,
            max_blocks_per_iteration,
            max_concurrent_blocks,
            reset_index,
            start_block,
            stop_block,
            otel_endpoint,
            otel_replica_name_override,
            max_inline_encoded_len,
            skip_connectivity_check,
            enable_logs_indexing,
        } = overrides;

        if let Some(value) = block_data_source {
            self.block_data_source = value;
        }
        if let Some(value) = archive_sink {
            self.archive_sink = value;
        }
        if let Some(value) = max_blocks_per_iteration {
            self.max_blocks_per_iteration = value;
        }
        if let Some(value) = max_concurrent_blocks {
            self.max_concurrent_blocks = value;
        }
        if let Some(value) = reset_index {
            self.reset_index = value;
        }
        if let Some(value) = start_block {
            self.start_block = Some(value);
        }
        if let Some(value) = stop_block {
            self.stop_block = Some(value);
        }
        if let Some(value) = otel_endpoint {
            self.otel_endpoint = Some(value);
        }
        if let Some(value) = otel_replica_name_override {
            self.otel_replica_name_override = Some(value);
        }
        if let Some(value) = max_inline_encoded_len {
            self.max_inline_encoded_len = value;
        }
        if let Some(value) = skip_connectivity_check {
            self.skip_connectivity_check = value;
        }
        if let Some(value) = enable_logs_indexing {
            self.enable_logs_indexing = value;
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "monad-indexer", about, long_about = None)]
struct CliArgs {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to a TOML configuration file
    #[arg(long)]
    config: Option<PathBuf>,

    /// Source to read block data that will be indexed
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    block_data_source: Option<BlockDataReaderArgs>,

    /// Where archive data is written to
    /// For aws: 'aws <bucket_name> <concurrent_requests>'
    #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
    archive_sink: Option<ArchiveArgs>,

    #[arg(long)]
    max_blocks_per_iteration: Option<u64>,

    #[arg(long)]
    max_concurrent_blocks: Option<usize>,

    /// Resets the latest indexed entry
    #[arg(long, action = ArgAction::SetTrue)]
    reset_index: bool,

    /// Override block number to start at
    #[arg(long)]
    start_block: Option<u64>,

    /// Override block number to stop at
    #[arg(long)]
    stop_block: Option<u64>,

    /// Endpoint to push metrics to
    #[arg(long)]
    otel_endpoint: Option<String>,

    #[arg(long)]
    otel_replica_name_override: Option<String>,

    /// Maximum size of an encoded inline tx index entry
    /// If an entry is larger than this, it is stored as a reference pointing to
    /// the block level data store
    #[arg(long)]
    max_inline_encoded_len: Option<usize>,

    #[arg(long, action = ArgAction::SetTrue)]
    skip_connectivity_check: bool,

    /// Enable eth_getLogs indexing (disabled by default)
    #[arg(long, action = ArgAction::SetTrue)]
    enable_logs_indexing: bool,
}

impl CliArgs {
    fn into_cli(self) -> Result<Cli> {
        let (config_path, overrides, command) = self.into_parts();
        let config = match config_path {
            Some(path) => Some(load_config(&path)?),
            None => None,
        };
        Cli::from_sources(config, overrides, command)
    }

    fn into_parts(self) -> (Option<PathBuf>, CliOverrides, Option<Commands>) {
        let Self {
            command,
            config,
            block_data_source,
            archive_sink,
            max_blocks_per_iteration,
            max_concurrent_blocks,
            reset_index,
            start_block,
            stop_block,
            otel_endpoint,
            otel_replica_name_override,
            max_inline_encoded_len,
            skip_connectivity_check,
            enable_logs_indexing,
        } = self;

        let overrides = CliOverrides {
            block_data_source,
            archive_sink,
            max_blocks_per_iteration,
            max_concurrent_blocks,
            reset_index: bool_override(reset_index),
            start_block,
            stop_block,
            otel_endpoint,
            otel_replica_name_override,
            max_inline_encoded_len,
            skip_connectivity_check: bool_override(skip_connectivity_check),
            enable_logs_indexing: bool_override(enable_logs_indexing),
        };

        (config, overrides, command)
    }
}

#[derive(Debug, Subcommand, Deserialize)]
pub enum Commands {
    /// Migrate logs index
    MigrateLogs,
    /// Migrate capped collection to uncapped
    MigrateCapped {
        /// Database name
        #[arg(long)]
        db_name: String,
        /// Collection name to migrate
        #[arg(long)]
        coll_name: String,
        /// Batch size for copying
        #[arg(long, default_value_t = 2000)]
        batch_size: u32,
        /// Free space factor
        #[arg(long, default_value_t = 1.5)]
        free_factor: f64,
    },
}

#[derive(Debug, Default)]
struct CliOverrides {
    block_data_source: Option<BlockDataReaderArgs>,
    archive_sink: Option<ArchiveArgs>,
    max_blocks_per_iteration: Option<u64>,
    max_concurrent_blocks: Option<usize>,
    reset_index: Option<bool>,
    start_block: Option<u64>,
    stop_block: Option<u64>,
    otel_endpoint: Option<String>,
    otel_replica_name_override: Option<String>,
    max_inline_encoded_len: Option<usize>,
    skip_connectivity_check: Option<bool>,
    enable_logs_indexing: Option<bool>,
}

fn load_config(path: &Path) -> Result<Cli> {
    let contents = fs::read_to_string(path)
        .wrap_err_with(|| format!("failed to read config file {}", path.display()))?;
    toml::from_str(&contents)
        .wrap_err_with(|| format!("failed to parse config file {}", path.display()))
}

fn bool_override(value: bool) -> Option<bool> {
    value.then_some(true)
}

const fn default_max_blocks_per_iteration() -> u64 {
    50
}

const fn default_max_concurrent_blocks() -> usize {
    10
}

const fn default_max_inline_encoded_len() -> usize {
    350 * 1024
}
