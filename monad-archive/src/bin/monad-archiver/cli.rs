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

use clap::{ArgAction, Parser};
use eyre::{eyre, Context, Result};
use monad_archive::cli::{ArchiveArgs, BlockDataReaderArgs};
use serde::Deserialize;

/// Runtime configuration for the `monad-archiver` binary.
///
/// Values can come from either a `--config path/to/config.toml` file or from
/// CLI flags. When both are supplied, CLI arguments win. For example:
///
/// ```text
/// monad-archiver --config config.toml --max-blocks-per-iteration 50
/// ```
///
/// will load every field from `config.toml` and then replace the
/// `max_blocks_per_iteration` value with `50` before execution.
#[derive(Debug, Deserialize)]
pub struct Cli {
    /// Where blocks, receipts and traces are read from
    /// For triedb: 'triedb <triedb_path> <concurrent_requests>'
    pub block_data_source: BlockDataReaderArgs,

    /// If reading from --block-data-source fails, attempts to read from
    /// this optional fallback
    pub fallback_block_data_source: Option<BlockDataReaderArgs>,

    /// Where archive data is written to
    /// For aws: 'aws <bucket_name> <concurrent_requests>'
    pub archive_sink: ArchiveArgs,

    #[serde(default = "default_max_blocks_per_iteration")]
    pub max_blocks_per_iteration: u64,

    #[serde(default = "default_max_concurrent_blocks")]
    pub max_concurrent_blocks: usize,

    /// Override block number to start at
    pub start_block: Option<u64>,

    /// Override block number to stop at
    pub stop_block: Option<u64>,

    /// Skip bad blocks
    /// If set, archiver will skip blocks that fail to archive
    /// and log an error
    /// DO NOT ENABLE UNDER NORMAL OPERATION
    #[serde(default)]
    pub unsafe_skip_bad_blocks: bool,

    /// Path to folder containing bft blocks
    /// If set, archiver will upload these files to blob store provided in archive_sink
    pub bft_block_path: Option<PathBuf>,

    #[serde(default = "default_bft_block_poll_freq_secs")]
    pub bft_block_poll_freq_secs: u64,

    #[serde(default = "default_bft_block_min_age_secs")]
    pub bft_block_min_age_secs: u64,

    /// Path to forkpoint for checkpoint'ing
    /// If set, archiver will save a copy of this file every forkpoint_checkpoint_freq_secs
    pub forkpoint_path: Option<PathBuf>,

    #[serde(default = "default_forkpoint_checkpoint_freq_secs")]
    pub forkpoint_checkpoint_freq_secs: u64,

    #[serde(default)]
    pub additional_files_to_checkpoint: Vec<PathBuf>,

    #[serde(default = "default_additional_checkpoint_freq_secs")]
    pub additional_checkpoint_freq_secs: u64,

    pub otel_endpoint: Option<String>,

    pub otel_replica_name_override: Option<String>,

    #[serde(default)]
    pub skip_connectivity_check: bool,
}

impl Cli {
    pub fn parse() -> Self {
        Self::try_parse().unwrap_or_else(|err| {
            eprintln!("failed to load monad-archiver configuration: {err:?}");
            process::exit(2);
        })
    }

    pub fn try_parse() -> Result<Self> {
        CliArgs::parse().into_cli()
    }

    fn from_sources(config: Option<Cli>, overrides: CliOverrides) -> Result<Self> {
        match config {
            Some(mut cli) => {
                cli.apply_overrides(overrides);
                Ok(cli)
            }
            None => Cli::from_overrides(overrides),
        }
    }

    fn from_overrides(overrides: CliOverrides) -> Result<Self> {
        let CliOverrides {
            block_data_source,
            fallback_block_data_source,
            archive_sink,
            max_blocks_per_iteration,
            max_concurrent_blocks,
            start_block,
            stop_block,
            unsafe_skip_bad_blocks,
            bft_block_path,
            bft_block_poll_freq_secs,
            bft_block_min_age_secs,
            forkpoint_path,
            forkpoint_checkpoint_freq_secs,
            additional_files_to_checkpoint,
            additional_checkpoint_freq_secs,
            otel_endpoint,
            otel_replica_name_override,
            skip_connectivity_check,
        } = overrides;

        Ok(Self {
            block_data_source: block_data_source
                .ok_or_else(|| eyre!("block_data_source must be provided via CLI or config"))?,
            fallback_block_data_source,
            archive_sink: archive_sink
                .ok_or_else(|| eyre!("archive_sink must be provided via CLI or config"))?,
            max_blocks_per_iteration: max_blocks_per_iteration
                .unwrap_or_else(default_max_blocks_per_iteration),
            max_concurrent_blocks: max_concurrent_blocks
                .unwrap_or_else(default_max_concurrent_blocks),
            start_block,
            stop_block,
            unsafe_skip_bad_blocks: unsafe_skip_bad_blocks.unwrap_or(false),
            bft_block_path,
            bft_block_poll_freq_secs: bft_block_poll_freq_secs
                .unwrap_or_else(default_bft_block_poll_freq_secs),
            bft_block_min_age_secs: bft_block_min_age_secs
                .unwrap_or_else(default_bft_block_min_age_secs),
            forkpoint_path,
            forkpoint_checkpoint_freq_secs: forkpoint_checkpoint_freq_secs
                .unwrap_or_else(default_forkpoint_checkpoint_freq_secs),
            additional_files_to_checkpoint: additional_files_to_checkpoint.unwrap_or_default(),
            additional_checkpoint_freq_secs: additional_checkpoint_freq_secs
                .unwrap_or_else(default_additional_checkpoint_freq_secs),
            otel_endpoint,
            otel_replica_name_override,
            skip_connectivity_check: skip_connectivity_check.unwrap_or(false),
        })
    }

    fn apply_overrides(&mut self, overrides: CliOverrides) {
        if let Some(value) = overrides.block_data_source {
            self.block_data_source = value;
        }
        if let Some(value) = overrides.fallback_block_data_source {
            self.fallback_block_data_source = Some(value);
        }
        if let Some(value) = overrides.archive_sink {
            self.archive_sink = value;
        }
        if let Some(value) = overrides.max_blocks_per_iteration {
            self.max_blocks_per_iteration = value;
        }
        if let Some(value) = overrides.max_concurrent_blocks {
            self.max_concurrent_blocks = value;
        }
        if let Some(value) = overrides.start_block {
            self.start_block = Some(value);
        }
        if let Some(value) = overrides.stop_block {
            self.stop_block = Some(value);
        }
        if let Some(value) = overrides.unsafe_skip_bad_blocks {
            self.unsafe_skip_bad_blocks = value;
        }
        if let Some(value) = overrides.bft_block_path {
            self.bft_block_path = Some(value);
        }
        if let Some(value) = overrides.bft_block_poll_freq_secs {
            self.bft_block_poll_freq_secs = value;
        }
        if let Some(value) = overrides.bft_block_min_age_secs {
            self.bft_block_min_age_secs = value;
        }
        if let Some(value) = overrides.forkpoint_path {
            self.forkpoint_path = Some(value);
        }
        if let Some(value) = overrides.forkpoint_checkpoint_freq_secs {
            self.forkpoint_checkpoint_freq_secs = value;
        }
        if let Some(value) = overrides.additional_files_to_checkpoint {
            self.additional_files_to_checkpoint = value;
        }
        if let Some(value) = overrides.additional_checkpoint_freq_secs {
            self.additional_checkpoint_freq_secs = value;
        }
        if let Some(value) = overrides.otel_endpoint {
            self.otel_endpoint = Some(value);
        }
        if let Some(value) = overrides.otel_replica_name_override {
            self.otel_replica_name_override = Some(value);
        }
        if let Some(value) = overrides.skip_connectivity_check {
            self.skip_connectivity_check = value;
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "monad-archive", about, long_about = None)]
struct CliArgs {
    /// Path to a TOML configuration file
    #[arg(long)]
    config: Option<PathBuf>,

    /// Where blocks, receipts and traces are read from
    /// For triedb: 'triedb <triedb_path> <concurrent_requests>'
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    block_data_source: Option<BlockDataReaderArgs>,

    /// If reading from --block-data-source fails, attempts to read from
    /// this optional fallback
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    fallback_block_data_source: Option<BlockDataReaderArgs>,

    /// Where archive data is written to
    /// For aws: 'aws <bucket_name> <concurrent_requests>'
    #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
    archive_sink: Option<ArchiveArgs>,

    #[arg(long)]
    max_blocks_per_iteration: Option<u64>,

    #[arg(long)]
    max_concurrent_blocks: Option<usize>,

    /// Override block number to start at
    #[arg(long)]
    start_block: Option<u64>,

    /// Override block number to stop at
    #[arg(long)]
    stop_block: Option<u64>,

    /// Skip bad blocks
    /// If set, archiver will skip blocks that fail to archive
    /// and log an error
    /// DO NOT ENABLE UNDER NORMAL OPERATION
    #[arg(long, action = ArgAction::SetTrue)]
    unsafe_skip_bad_blocks: bool,

    /// Path to folder containing bft blocks
    /// If set, archiver will upload these files to blob store provided in archive_sink
    #[arg(long)]
    bft_block_path: Option<PathBuf>,

    #[arg(long)]
    bft_block_poll_freq_secs: Option<u64>,

    #[arg(long)]
    bft_block_min_age_secs: Option<u64>,

    /// Path to forkpoint for checkpoint'ing
    /// If set, archiver will save a copy of this file every forkpoint_checkpoint_freq_secs
    #[arg(long)]
    forkpoint_path: Option<PathBuf>,

    #[arg(long)]
    forkpoint_checkpoint_freq_secs: Option<u64>,

    #[arg(long, value_delimiter = ',', num_args = 1..)]
    additional_files_to_checkpoint: Option<Vec<PathBuf>>,

    #[arg(long)]
    additional_checkpoint_freq_secs: Option<u64>,

    #[arg(long)]
    otel_endpoint: Option<String>,

    #[arg(long)]
    otel_replica_name_override: Option<String>,

    #[arg(long, action = ArgAction::SetTrue)]
    skip_connectivity_check: bool,
}

impl CliArgs {
    fn into_cli(self) -> Result<Cli> {
        let (config_path, overrides) = self.into_parts();
        let config = match config_path {
            Some(path) => Some(load_config(&path)?),
            None => None,
        };
        Cli::from_sources(config, overrides)
    }

    fn into_parts(self) -> (Option<PathBuf>, CliOverrides) {
        let Self {
            config,
            block_data_source,
            fallback_block_data_source,
            archive_sink,
            max_blocks_per_iteration,
            max_concurrent_blocks,
            start_block,
            stop_block,
            unsafe_skip_bad_blocks,
            bft_block_path,
            bft_block_poll_freq_secs,
            bft_block_min_age_secs,
            forkpoint_path,
            forkpoint_checkpoint_freq_secs,
            additional_files_to_checkpoint,
            additional_checkpoint_freq_secs,
            otel_endpoint,
            otel_replica_name_override,
            skip_connectivity_check,
        } = self;

        let overrides = CliOverrides {
            block_data_source,
            fallback_block_data_source,
            archive_sink,
            max_blocks_per_iteration,
            max_concurrent_blocks,
            start_block,
            stop_block,
            unsafe_skip_bad_blocks: bool_override(unsafe_skip_bad_blocks),
            bft_block_path,
            bft_block_poll_freq_secs,
            bft_block_min_age_secs,
            forkpoint_path,
            forkpoint_checkpoint_freq_secs,
            additional_files_to_checkpoint,
            additional_checkpoint_freq_secs,
            otel_endpoint,
            otel_replica_name_override,
            skip_connectivity_check: bool_override(skip_connectivity_check),
        };

        (config, overrides)
    }
}

#[derive(Debug, Default)]
struct CliOverrides {
    block_data_source: Option<BlockDataReaderArgs>,
    fallback_block_data_source: Option<BlockDataReaderArgs>,
    archive_sink: Option<ArchiveArgs>,
    max_blocks_per_iteration: Option<u64>,
    max_concurrent_blocks: Option<usize>,
    start_block: Option<u64>,
    stop_block: Option<u64>,
    unsafe_skip_bad_blocks: Option<bool>,
    bft_block_path: Option<PathBuf>,
    bft_block_poll_freq_secs: Option<u64>,
    bft_block_min_age_secs: Option<u64>,
    forkpoint_path: Option<PathBuf>,
    forkpoint_checkpoint_freq_secs: Option<u64>,
    additional_files_to_checkpoint: Option<Vec<PathBuf>>,
    additional_checkpoint_freq_secs: Option<u64>,
    otel_endpoint: Option<String>,
    otel_replica_name_override: Option<String>,
    skip_connectivity_check: Option<bool>,
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

fn default_max_blocks_per_iteration() -> u64 {
    100
}

fn default_max_concurrent_blocks() -> usize {
    15
}

fn default_bft_block_poll_freq_secs() -> u64 {
    5
}

fn default_bft_block_min_age_secs() -> u64 {
    10
}

fn default_forkpoint_checkpoint_freq_secs() -> u64 {
    300
}

fn default_additional_checkpoint_freq_secs() -> u64 {
    300
}

#[cfg(test)]
mod tests {
    use std::{io::Write, path::PathBuf};

    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn toml_deserialization_with_all_fields() {
        let config = r#"
            max_blocks_per_iteration = 250
            max_concurrent_blocks = 32
            start_block = 5
            stop_block = 10
            unsafe_skip_bad_blocks = true
            bft_block_path = "/tmp/bft"
            bft_block_poll_freq_secs = 7
            bft_block_min_age_secs = 11
            forkpoint_path = "/tmp/fork"
            forkpoint_checkpoint_freq_secs = 123
            additional_files_to_checkpoint = ["/tmp/a", "/tmp/b"]
            additional_checkpoint_freq_secs = 456
            otel_endpoint = "http://otel"
            otel_replica_name_override = "special"
            skip_connectivity_check = true

            [block_data_source]
            type = "aws"
            bucket = "source-bucket"
            region = "us-east-1"
            concurrency = 99
            operation_timeout_secs = 12
            operation_attempt_timeout_secs = 12
            read_timeout_secs = 12

            [fallback_block_data_source]
            type = "mongodb"
            url = "mongodb://fallback:27017"
            db = "fallback-db"

            [archive_sink]
            type = "mongodb"
            url = "mongodb://sink:27017"
            db = "sink-db"
        "#;

        let cli: Cli = toml::from_str(config).expect("toml should deserialize");

        assert_eq!(cli.max_blocks_per_iteration, 250);
        assert_eq!(cli.max_concurrent_blocks, 32);
        assert_eq!(cli.start_block, Some(5));
        assert_eq!(cli.stop_block, Some(10));
        assert!(cli.unsafe_skip_bad_blocks);
        assert_eq!(cli.bft_block_path, Some(PathBuf::from("/tmp/bft")));
        assert_eq!(cli.bft_block_poll_freq_secs, 7);
        assert_eq!(cli.bft_block_min_age_secs, 11);
        assert_eq!(cli.forkpoint_path, Some(PathBuf::from("/tmp/fork")));
        assert_eq!(cli.forkpoint_checkpoint_freq_secs, 123);
        assert_eq!(cli.additional_files_to_checkpoint.len(), 2);
        assert_eq!(cli.additional_checkpoint_freq_secs, 456);
        assert_eq!(cli.otel_endpoint.as_deref(), Some("http://otel"));
        assert_eq!(cli.otel_replica_name_override.as_deref(), Some("special"));
        assert!(cli.skip_connectivity_check);

        match &cli.block_data_source {
            BlockDataReaderArgs::Aws(args) => {
                assert_eq!(args.bucket, "source-bucket");
                assert_eq!(args.region.as_deref(), Some("us-east-1"));
                assert_eq!(args.concurrency, 99);
                assert_eq!(args.operation_timeout_secs, 12);
            }
            _ => panic!("expected aws block data source"),
        }

        match cli
            .fallback_block_data_source
            .as_ref()
            .expect("fallback expected")
        {
            BlockDataReaderArgs::MongoDb(args) => {
                assert_eq!(args.url, "mongodb://fallback:27017");
                assert_eq!(args.db, "fallback-db");
            }
            _ => panic!("expected mongodb fallback source"),
        }

        match &cli.archive_sink {
            ArchiveArgs::MongoDb(args) => {
                assert_eq!(args.url, "mongodb://sink:27017");
                assert_eq!(args.db, "sink-db");
            }
            _ => panic!("expected mongodb sink"),
        }
    }

    #[test]
    fn toml_deserialization_uses_cli_defaults() {
        let config = r#"
            [block_data_source]
            type = "triedb"
            triedb_path = "/var/triedb"

            [archive_sink]
            type = "aws"
            bucket = "sink-bucket"
            concurrency = 50
        "#;

        let cli: Cli = toml::from_str(config).expect("toml should deserialize");

        assert_eq!(cli.max_blocks_per_iteration, 100);
        assert_eq!(cli.max_concurrent_blocks, 15);
        assert_eq!(cli.bft_block_poll_freq_secs, 5);
        assert_eq!(cli.bft_block_min_age_secs, 10);
        assert_eq!(cli.forkpoint_checkpoint_freq_secs, 300);
        assert_eq!(cli.additional_checkpoint_freq_secs, 300);
        assert!(cli.additional_files_to_checkpoint.is_empty());
        assert!(!cli.skip_connectivity_check);

        match &cli.block_data_source {
            BlockDataReaderArgs::Triedb(args) => {
                assert_eq!(args.triedb_path, "/var/triedb");
            }
            _ => panic!("expected triedb source"),
        }

        match &cli.archive_sink {
            ArchiveArgs::Aws(args) => {
                assert_eq!(args.bucket, "sink-bucket");
            }
            _ => panic!("expected aws sink"),
        }
    }

    #[test]
    fn config_flag_reads_toml_file() {
        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            r#"
            max_blocks_per_iteration = 222
            max_concurrent_blocks = 16

            [block_data_source]
            type = "aws"
            bucket = "from-config"
            concurrency = 20

            [archive_sink]
            type = "mongodb"
            url = "mongodb://sink"
            db = "config-db"
            "#
        )
        .unwrap();

        let cli =
            CliArgs::parse_from(["monad-archiver", "--config", file.path().to_str().unwrap()])
                .into_cli()
                .expect("config file should load");

        assert_eq!(cli.max_blocks_per_iteration, 222);
        assert_eq!(cli.max_concurrent_blocks, 16);
        match cli.block_data_source {
            BlockDataReaderArgs::Aws(args) => {
                assert_eq!(args.bucket, "from-config");
            }
            _ => panic!("expected aws"),
        }
        match cli.archive_sink {
            ArchiveArgs::MongoDb(args) => {
                assert_eq!(args.db, "config-db");
            }
            _ => panic!("expected mongodb"),
        }
    }

    #[test]
    fn cli_args_override_config_file() {
        let mut file = NamedTempFile::new().unwrap();
        write!(
            file,
            r#"
            max_blocks_per_iteration = 50

            [block_data_source]
            type = "aws"
            bucket = "config-bucket"

            [archive_sink]
            type = "mongodb"
            url = "mongodb://sink"
            db = "config-db"
            "#
        )
        .unwrap();

        let cli = CliArgs::parse_from([
            "monad-archiver",
            "--config",
            file.path().to_str().unwrap(),
            "--max-blocks-per-iteration",
            "123",
            "--block-data-source",
            "aws cli-bucket",
        ])
        .into_cli()
        .expect("cli overrides should succeed");

        assert_eq!(cli.max_blocks_per_iteration, 123);
        match cli.block_data_source {
            BlockDataReaderArgs::Aws(args) => {
                assert_eq!(args.bucket, "cli-bucket");
            }
            _ => panic!("expected aws"),
        }
    }
}
