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

use clap::{Parser, Subcommand};
use monad_archive::cli::ArchiveArgs;

#[derive(Debug, Parser)]
#[command(
    name = "monad-archive-checker",
    about = "Archive consistency checker for validating blockchain data across multiple replicas",
    long_about = "Archive consistency checker for validating blockchain data across multiple replicas.\n\n\
EXAMPLES:\n\n\
  # Start main checker with 3 replicas (runs continuously)\n\
  monad-archive-checker --bucket checker-state --region us-east-1 checker \\\n\
    --init-replicas 'aws archive-1 20,aws archive-2 20,aws archive-3 20'\n\n\
  # Inspect specific faults\n\
  monad-archive-checker --bucket checker-state inspector list-faults\n\
  monad-archive-checker --bucket checker-state inspector inspect-block 12345 --format all\n\n\
  # Fix faults by copying from good replicas (dry run first)\n\
  monad-archive-checker --bucket checker-state fault-fixer\n\
  monad-archive-checker --bucket checker-state fault-fixer --commit-changes --verify\n\n\
  # Run standalone rechecker to fix false positives (runs once and exits)\n\
  monad-archive-checker --bucket checker-state rechecker\n\n\
  # Run rechecker in worker mode (runs periodically)\n\
  monad-archive-checker --bucket checker-state rechecker --worker --recheck-freq-min 5\n\n\
  # Advanced: Recheck specific block range with dry run\n\
  monad-archive-checker --bucket checker-state rechecker \\\n\
    --start-block 1000 --end-block 5000 --dry-run\n\n\
  # Force recheck all chunks even without faults\n\
  monad-archive-checker --bucket checker-state rechecker \\\n\
    --start-block 0 --end-block 10000 --force-recheck --dry-run\n\n\
"
)]
pub struct Cli {
    #[command(subcommand)]
    pub mode: Mode,

    /// S3 bucket name for storing checker state
    #[arg(long)]
    pub bucket: String,

    /// AWS region
    #[arg(long)]
    pub region: Option<String>,

    #[arg(long)]
    pub otel_endpoint: Option<String>,

    #[arg(long)]
    pub otel_replica_name_override: Option<String>,

    #[arg(long)]
    pub max_compute_threads: Option<usize>,
}

#[derive(Subcommand, Debug)]
pub enum Mode {
    /// Main checker mode - continuously validates blocks across replicas
    Checker(CheckerArgs),
    /// Standalone rechecker - rechecks fault chunks from scratch (runs once by default)
    Rechecker(Rechecker),
    /// Repairs faults by copying data from good replicas
    FaultFixer(FaultFixerArgs),
    /// Inspects and analyzes fault data
    Inspector(InspectorArgs),
}

#[derive(Parser, Debug)]
pub struct CheckerArgs {
    /// Comma-separated list of replicas to check
    /// Format: 'aws bucket1 [concurrency1] [region1],aws bucket2 [concurrency2] [region2],...'
    #[arg(long, value_delimiter = ',', value_parser = clap::value_parser!(ArchiveArgs))]
    pub init_replicas: Option<Vec<ArchiveArgs>>,

    /// Flag to disable running rechecker worker to determine if existing faults still exist
    #[arg(long)]
    pub disable_rechecker: bool,

    /// The minimum difference between the block_num tip of the latest replica
    /// and the latest block to check
    /// E.g. if replica tips are 2000, 2100 and 2005, and  --min-lag-from-tip is 500,
    /// then we would check up to block_num max(2000,2100,2005) - 500 = 2100 - 500 = 1600
    #[arg(long, default_value_t = 1500)]
    pub min_lag_from_tip: u64,

    /// How frequently to recheck faults in minutes
    #[arg(long, default_value_t = 15.)]
    pub recheck_freq_min: f64,

    /// How many blocks to process in parallel
    #[arg(long, default_value_t = 20)]
    pub concurrency: usize,
}

#[derive(Parser, Debug)]
pub struct Rechecker {
    /// How frequently to recheck faults in minutes (only used with --worker)
    #[arg(long, default_value_t = 5.)]
    pub recheck_freq_min: f64,

    /// Dry run mode - only print differences without updating faults
    #[arg(long)]
    pub dry_run: bool,

    /// Optional start block to recheck (inclusive)
    #[arg(long)]
    pub start: Option<u64>,

    /// Optional end block to recheck (inclusive)
    #[arg(long)]
    pub end: Option<u64>,

    /// Force rechecking all chunks in range even if no faults are present
    #[arg(long)]
    pub force_recheck: bool,

    /// Run continuously as a worker (default is to run once and exit)
    #[arg(long)]
    pub worker: bool,

    /// How many blocks to process in parallel
    #[arg(long, default_value_t = 20)]
    pub concurrency: usize,
}

#[derive(Parser, Debug)]
pub struct FaultFixerArgs {
    /// Commit changes to replicas
    /// Otherwise runs in dry-run mode
    #[clap(long)]
    pub commit_changes: bool,

    /// Verify fixed blocks after repair
    #[clap(long)]
    pub verify: bool,

    /// Comma-separated list of specific replicas to fix (defaults to all)
    #[clap(long, value_delimiter = ',')]
    pub replicas: Option<Vec<String>>,

    /// Start block (inclusive)
    #[clap(long)]
    pub start: Option<u64>,

    /// End block (inclusive)
    #[clap(long)]
    pub end: Option<u64>,

    /// How many blocks to process in parallel
    #[arg(long, default_value_t = 20)]
    pub concurrency: usize,
}

#[derive(Parser, Debug)]
pub struct InspectorArgs {
    #[command(subcommand)]
    pub command: InspectorCommand,
}

#[derive(Subcommand, Debug)]
pub enum InspectorCommand {
    /// Show status summary of the checker model
    Status,

    /// List all fault ranges collapsed to start-end format
    ListFaults {
        /// Show summary with only first 10 ranges per fault type
        #[arg(long)]
        summary: bool,

        /// Start block (inclusive)
        #[arg(long)]
        start: Option<u64>,

        /// End block (inclusive)
        #[arg(long)]
        end: Option<u64>,

        /// Replica to list faults for
        #[arg(long)]
        replica: Option<String>,
    },

    /// Inspect a specific block across all replicas
    InspectBlock {
        /// Block number to inspect
        block_num: u64,

        /// Output format
        #[arg(long, default_value = "all")]
        format: InspectorOutputFormat,

        /// Show raw data
        #[arg(long, short = 'r')]
        raw: bool,
    },
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum InspectorOutputFormat {
    /// Show all replicas
    All,
    /// Show only replicas with faults
    FaultsOnly,
    /// Show only the good replica
    GoodOnly,
    /// Show summary statistics only
    Summary,
}
