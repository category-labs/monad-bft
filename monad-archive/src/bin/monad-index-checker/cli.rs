use std::path::PathBuf;

use clap::Parser;
use monad_archive::cli::ArchiveArgs;

#[derive(Debug, Parser)]
#[command(name = "monad-index-checker", about, long_about = None)]
pub struct Cli {
    #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
    pub source: ArchiveArgs,

    #[arg(long)]
    pub checker_path: PathBuf,

    /// Set the max response size in bytes (the same as monad-rpc)
    #[arg(long, default_value_t = 25_000_000)]
    pub max_response_size: u32,

    /// Number of blocks to handle per loop iteration
    #[arg(long, default_value_t = 50)]
    pub max_blocks_per_iteration: u64,

    #[arg(long, default_value_t = 10)]
    pub concurrent_blocks: usize,

    /// Override block number to start at
    #[arg(long)]
    pub start_block: Option<u64>,

    /// Override block number to stop at
    #[arg(long)]
    pub stop_block: Option<u64>,

    #[arg(long)]
    pub otel_endpoint: Option<String>,
}
