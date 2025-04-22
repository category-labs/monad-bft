use std::path::PathBuf;

use clap::Parser;
use monad_archive::cli::{ArchiveArgs, BlockDataReaderArgs};

#[derive(Debug, Parser)]
#[command(name = "monad-archive", about, long_about = None)]
pub struct Cli {
    /// Where blocks, receipts and traces are read from
    /// For triedb: 'triedb <triedb_path> <concurrent_requests>'
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    pub block_data_source: BlockDataReaderArgs,

    /// If reading from --block-data-source fails, attempts to read from
    /// this optional fallback
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    pub fallback_block_data_source: Option<BlockDataReaderArgs>,

    /// Where archive data is written to
    /// For aws: 'aws <bucket_name> <concurrent_requests>'
    #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
    pub archive_sink: ArchiveArgs,

    #[arg(long, default_value_t = 100)]
    pub max_blocks_per_iteration: u64,

    #[arg(long, default_value_t = 15)]
    pub max_concurrent_blocks: usize,

    /// Override block number to start at
    #[arg(long)]
    pub start_block: Option<u64>,

    /// Override block number to stop at
    #[arg(long)]
    pub stop_block: Option<u64>,

    /// Skip bad blocks
    /// If set, archiver will skip blocks that fail to archive
    /// and log an error
    /// DO NOT ENABLE UNDER NORMAL OPERATION
    #[arg(long)]
    pub unsafe_skip_bad_blocks: bool,

    /// Path to folder containing bft blocks
    /// If set, archiver will upload these files to blob store provided in archive_sink
    #[arg(long)]
    pub bft_block_path: Option<PathBuf>,

    #[arg(long, default_value_t = 5)]
    pub bft_block_poll_freq_secs: u64,

    /// Path to wal for checkpoint'ing
    /// If set, archiver will save a copy of this file every wal_checkpoint_freq_secs
    #[arg(long)]
    pub wal_path: Option<PathBuf>,

    #[arg(long, default_value_t = 3600)]
    pub wal_checkpoint_freq_secs: u64,

    /// Path to forkpoint for checkpoint'ing
    /// If set, archiver will save a copy of this file every forkpoint_checkpoint_freq_secs
    #[arg(long)]
    pub forkpoint_path: Option<PathBuf>,

    #[arg(long, default_value_t = 300)]
    pub forkpoint_checkpoint_freq_secs: u64,

    #[arg(long, value_delimiter = ',')]
    pub additional_files_to_checkpoint: Vec<PathBuf>,

    #[arg(long, default_value_t = 300)]
    pub additional_checkpoint_freq_secs: u64,

    #[arg(long)]
    pub otel_endpoint: Option<String>,

    #[arg(long)]
    pub otel_replica_name_override: Option<String>,

    #[arg(long, default_value_t = false)]
    pub skip_connectivity_check: bool,
}
