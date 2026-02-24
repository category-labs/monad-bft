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

use std::process;

use clap::{error::ErrorKind, ArgAction, CommandFactory, Parser, Subcommand};
use eyre::{eyre, Result};
use monad_archive::cli::{ArchiveArgs, BlockDataReaderArgs};

#[derive(Debug)]
pub struct Cli {
    pub block_data_source: BlockDataReaderArgs,
    pub fallback_block_data_source: Option<BlockDataReaderArgs>,
    pub archive_sink: ArchiveArgs,
    pub async_backfill: bool,
    pub max_blocks_per_iteration: u64,
    pub max_concurrent_blocks: usize,
    pub reset_index: bool,
    pub stop_block: Option<u64>,
    pub otel_endpoint: Option<String>,
    pub otel_replica_name_override: Option<String>,
    pub max_inline_encoded_len: usize,
    pub skip_connectivity_check: bool,
    pub enable_logs_indexing: bool,
}

pub enum ParsedCli {
    Command { command: Commands, args: CliArgs },
    Daemon(Cli),
}

impl Cli {
    pub fn parse() -> ParsedCli {
        let mut args = match CliArgs::try_parse() {
            Ok(args) => args,
            Err(err) => match err.kind() {
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => {
                    let _ = err.print();
                    process::exit(0);
                }
                _ => {
                    eprintln!("failed to parse monad-indexer args: {err:?}");
                    process::exit(2);
                }
            },
        };

        if let Some(command) = std::mem::take(&mut args.command) {
            if let Err(err) = args.validate_command(&command) {
                let _ = err.print();
                process::exit(2);
            }
            return ParsedCli::Command { command, args };
        }

        match args.into_cli() {
            Ok(cli) => ParsedCli::Daemon(cli),
            Err(err) => {
                eprintln!("failed to load monad-indexer configuration: {err:?}");
                process::exit(2);
            }
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "monad-indexer", about, long_about = None)]
pub struct CliArgs {
    #[command(subcommand)]
    pub command: Option<Commands>,

    /// Source to read block data that will be indexed
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    pub block_data_source: Option<BlockDataReaderArgs>,

    /// If reading from --block-data-source fails, attempts to read from
    /// this optional fallback
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    pub fallback_block_data_source: Option<BlockDataReaderArgs>,

    /// Where archive data is written to
    /// For aws: 'aws <bucket_name> <concurrent_requests>'
    #[arg(long, global = true, value_parser = clap::value_parser!(ArchiveArgs))]
    pub archive_sink: Option<ArchiveArgs>,

    /// If set, indexer will perform an asynchronous backfill of the index
    /// This allows a second indexer to backfill a range while the first indexer is running
    #[arg(long, default_value_t = false)]
    pub async_backfill: bool,

    #[arg(long, default_value_t = 50)]
    pub max_blocks_per_iteration: u64,

    #[arg(long, default_value_t = 10)]
    pub max_concurrent_blocks: usize,

    /// Resets the latest indexed entry
    #[arg(long, default_value_t = false)]
    pub reset_index: bool,

    /// Override block number to stop at
    #[arg(long)]
    pub stop_block: Option<u64>,

    /// Endpoint to push metrics to
    #[arg(long)]
    pub otel_endpoint: Option<String>,

    #[arg(long)]
    pub otel_replica_name_override: Option<String>,

    /// Maximum size of an encoded inline tx index entry
    /// If an entry is larger than this, it is stored as a reference pointing to
    /// the block level data store
    #[arg(long, default_value_t = 350 * 1024)]
    pub max_inline_encoded_len: usize,

    #[arg(long, default_value_t = false)]
    pub skip_connectivity_check: bool,

    /// Enable eth_getLogs indexing (disabled by default)
    #[arg(long, default_value_t = false)]
    pub enable_logs_indexing: bool,
}

impl CliArgs {
    pub fn into_cli(self) -> Result<Cli> {
        let CliArgs {
            command: _,
            block_data_source,
            fallback_block_data_source,
            archive_sink,
            async_backfill,
            max_blocks_per_iteration,
            max_concurrent_blocks,
            reset_index,
            stop_block,
            otel_endpoint,
            otel_replica_name_override,
            max_inline_encoded_len,
            skip_connectivity_check,
            enable_logs_indexing,
        } = self;

        Ok(Cli {
            block_data_source: block_data_source
                .ok_or_else(|| eyre!("block_data_source must be provided"))?,
            fallback_block_data_source,
            archive_sink: archive_sink.ok_or_else(|| eyre!("archive_sink must be provided"))?,
            async_backfill,
            max_blocks_per_iteration,
            max_concurrent_blocks,
            reset_index,
            stop_block,
            otel_endpoint,
            otel_replica_name_override,
            max_inline_encoded_len,
            skip_connectivity_check,
            enable_logs_indexing,
        })
    }

    fn validate_command(&self, command: &Commands) -> std::result::Result<(), clap::Error> {
        match command {
            Commands::SetStartBlock { .. } | Commands::MigrateCapped { .. } => {
                if self.archive_sink.is_none() {
                    return Err(Self::missing_archive_sink_error());
                }
            }
            Commands::MigrateLogs { .. } | Commands::MigrateBftIndex { .. } => {}
        }
        Ok(())
    }

    fn missing_archive_sink_error() -> clap::Error {
        CliArgs::command().error(
            ErrorKind::MissingRequiredArgument,
            "The following required argument was not provided: --archive-sink <ARCHIVE_SINK>",
        )
    }
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Migrate logs index
    MigrateLogs {
        /// Block number to start migration from (default: 0)
        #[arg(long, default_value_t = 0)]
        start_block: u64,
        /// Block number to stop migration at (default: latest indexed)
        #[arg(long)]
        stop_block: Option<u64>,
    },
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
    /// Set the start block marker in the archive and exit.
    /// Use this instead of --start-block to safely configure the starting point.
    SetStartBlock {
        /// Block number to set as the latest indexed marker
        #[arg(long)]
        block: u64,

        /// Set the async-backfill marker instead of the primary marker
        #[arg(long, action = ArgAction::SetTrue)]
        async_backfill: bool,
    },
    /// Build seq_num -> block_id index from legacy bft_blocks/ archive data
    MigrateBftIndex {
        /// Source archive containing the legacy bft_blocks/ data
        #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
        source: ArchiveArgs,

        /// Sink archive to write index and markers to
        #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
        sink: ArchiveArgs,

        /// Number of sub-chains to index in parallel
        #[arg(long, default_value_t = 4)]
        concurrency: usize,

        /// Max blocks to index before persisting progress marker
        #[arg(long, default_value_t = 1000)]
        batch_size: usize,

        /// Skip copying header and body bytes to the new schema
        #[arg(long)]
        no_copy_data: bool,
    },
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use monad_archive::cli::{ArchiveArgs, AwsCliArgs, BlockDataReaderArgs};

    use super::*;

    fn make_aws_block_data_source() -> BlockDataReaderArgs {
        BlockDataReaderArgs::Aws(AwsCliArgs {
            bucket: "test-bucket".to_string(),
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            concurrency: 50,
            operation_timeout_secs: 10,
            operation_attempt_timeout_secs: 10,
            read_timeout_secs: 10,
        })
    }

    fn make_aws_archive_sink() -> ArchiveArgs {
        ArchiveArgs::Aws(AwsCliArgs {
            bucket: "sink-bucket".to_string(),
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            concurrency: 50,
            operation_timeout_secs: 10,
            operation_attempt_timeout_secs: 10,
            read_timeout_secs: 10,
        })
    }

    fn make_cli_args_with_defaults() -> CliArgs {
        CliArgs {
            command: None,
            block_data_source: None,
            fallback_block_data_source: None,
            archive_sink: None,
            async_backfill: false,
            max_blocks_per_iteration: 50,
            max_concurrent_blocks: 10,
            reset_index: false,
            stop_block: None,
            otel_endpoint: None,
            otel_replica_name_override: None,
            max_inline_encoded_len: 350 * 1024,
            skip_connectivity_check: false,
            enable_logs_indexing: false,
        }
    }

    #[test]
    fn into_cli_succeeds_with_required_fields() {
        let mut args = make_cli_args_with_defaults();
        args.block_data_source = Some(make_aws_block_data_source());
        args.archive_sink = Some(make_aws_archive_sink());

        let cli = args
            .into_cli()
            .expect("should succeed with required fields");
        assert!(matches!(cli.block_data_source, BlockDataReaderArgs::Aws(_)));
        assert!(matches!(cli.archive_sink, ArchiveArgs::Aws(_)));
    }

    #[test]
    fn into_cli_fails_without_block_data_source() {
        let mut args = make_cli_args_with_defaults();
        args.archive_sink = Some(make_aws_archive_sink());

        let err = args
            .into_cli()
            .expect_err("should fail without block_data_source");
        assert!(err
            .to_string()
            .contains("block_data_source must be provided"));
    }

    #[test]
    fn into_cli_fails_without_archive_sink() {
        let mut args = make_cli_args_with_defaults();
        args.block_data_source = Some(make_aws_block_data_source());

        let err = args
            .into_cli()
            .expect_err("should fail without archive_sink");
        assert!(err.to_string().contains("archive_sink must be provided"));
    }

    #[test]
    fn into_cli_preserves_optional_fields() {
        let mut args = make_cli_args_with_defaults();
        args.block_data_source = Some(make_aws_block_data_source());
        args.archive_sink = Some(make_aws_archive_sink());
        args.fallback_block_data_source = Some(make_aws_block_data_source());
        args.async_backfill = true;
        args.stop_block = Some(1000);
        args.otel_endpoint = Some("http://localhost:4317".to_string());

        let cli = args.into_cli().expect("should succeed");
        assert!(cli.fallback_block_data_source.is_some());
        assert!(cli.async_backfill);
        assert_eq!(cli.stop_block, Some(1000));
        assert_eq!(cli.otel_endpoint, Some("http://localhost:4317".to_string()));
    }

    #[test]
    fn clap_parses_set_start_block_with_global_archive_sink() {
        let args = CliArgs::try_parse_from([
            "monad-indexer",
            "--archive-sink",
            "aws sink-bucket",
            "set-start-block",
            "--block",
            "100",
        ])
        .expect("should parse set-start-block with global archive-sink");

        assert!(matches!(
            args.command,
            Some(Commands::SetStartBlock {
                block: 100,
                async_backfill: false
            })
        ));
        assert!(args.archive_sink.is_some());
    }

    #[test]
    fn clap_parses_set_start_block_with_archive_sink_after_subcommand() {
        let args = CliArgs::try_parse_from([
            "monad-indexer",
            "set-start-block",
            "--block",
            "100",
            "--archive-sink",
            "aws sink-bucket",
        ])
        .expect("should parse set-start-block with archive-sink after subcommand");

        assert!(matches!(
            args.command,
            Some(Commands::SetStartBlock {
                block: 100,
                async_backfill: false
            })
        ));
        assert!(args.archive_sink.is_some());
    }

    #[test]
    fn clap_parses_set_start_block_with_async_backfill() {
        let args = CliArgs::try_parse_from([
            "monad-indexer",
            "--archive-sink",
            "aws sink-bucket",
            "set-start-block",
            "--block",
            "200",
            "--async-backfill",
        ])
        .expect("should parse set-start-block with async-backfill");

        assert!(matches!(
            args.command,
            Some(Commands::SetStartBlock {
                block: 200,
                async_backfill: true
            })
        ));
    }

    #[test]
    fn clap_parses_daemon_mode_with_all_flags() {
        let args = CliArgs::try_parse_from([
            "monad-indexer",
            "--block-data-source",
            "aws source-bucket",
            "--archive-sink",
            "aws sink-bucket",
            "--max-blocks-per-iteration",
            "100",
            "--max-concurrent-blocks",
            "20",
        ])
        .expect("should parse daemon mode arguments");

        assert!(args.command.is_none());
        assert!(args.block_data_source.is_some());
        assert!(args.archive_sink.is_some());
        assert_eq!(args.max_blocks_per_iteration, 100);
        assert_eq!(args.max_concurrent_blocks, 20);
    }

    #[test]
    fn validate_set_start_block_requires_archive_sink() {
        let args = CliArgs::try_parse_from(["monad-indexer", "set-start-block", "--block", "100"]);

        let args = args.expect("clap should parse, validation happens later");
        let command = args.command.as_ref().expect("command should be set");
        let err = args
            .validate_command(command)
            .expect_err("validation should fail without archive-sink");
        assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn validate_migrate_capped_without_block_data_source() {
        let args = CliArgs::try_parse_from([
            "monad-indexer",
            "migrate-capped",
            "--db-name",
            "indexer-db",
            "--coll-name",
            "events",
            "--archive-sink",
            "aws sink-bucket",
        ])
        .expect("should parse migrate-capped without block-data-source");

        let command = args.command.as_ref().expect("command should be set");
        args.validate_command(command)
            .expect("validation should allow missing block-data-source");
        assert!(args.block_data_source.is_none());
    }

    #[test]
    fn validate_migrate_capped_requires_archive_sink() {
        let args = CliArgs::try_parse_from([
            "monad-indexer",
            "migrate-capped",
            "--db-name",
            "indexer-db",
            "--coll-name",
            "events",
        ])
        .expect("clap should parse, validation happens later");

        let command = args.command.as_ref().expect("command should be set");
        let err = args
            .validate_command(command)
            .expect_err("validation should fail without archive-sink");
        assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
    }
}
