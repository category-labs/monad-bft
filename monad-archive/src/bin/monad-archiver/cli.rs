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

    /// Where archive data is written to
    /// For aws: 'aws <bucket_name> <concurrent_requests>'
    #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
    pub archive_sink: ArchiveArgs,

    #[arg(long, default_value_t = 100)]
    pub max_blocks_per_iteration: u64,

    #[arg(long, default_value_t = 20)]
    pub max_concurrent_blocks: usize,

    /// Override block number to start at
    #[arg(long)]
    pub start_block: Option<u64>,

    /// Override block number to stop at
    #[arg(long)]
    pub stop_block: Option<u64>,

    /// Path to folder containing bft block bodies
    /// If set, archiver will upload these files to blob store provided in archive_sink
    #[arg(long)]
    pub bft_block_body_path: Option<PathBuf>,

    /// Override for bft header path
    /// Will default to `bft_block_path` otherwise
    #[arg(long)]
    pub bft_block_header_path: Option<PathBuf>,

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
}
