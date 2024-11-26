use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "monad-archive-checker", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    pub checker_path: PathBuf,

    #[arg(long, value_delimiter = ',')]
    pub s3_buckets: Vec<String>,

    #[arg(long, value_delimiter = ',')]
    pub regions: Vec<String>,

    /// Set start checking block number
    #[arg(long, default_value_t = 0)]
    pub start_block_number: u64,

    /// TODO: Set the concurrent blocks
    #[arg(long, default_value_t = 10)]
    pub max_concurrent_blocks: usize,

    /// Set max block processed per iter
    #[arg(long, default_value_t = 200)]
    pub max_blocks_per_iteration: u64,

    /// Set max tolerated log for each bucket, should be much SMALLER than "max_blocks_per_iteration"
    #[arg(long, default_value_t = 25)]
    pub max_lag: u64,

    #[arg(long)]
    pub otel_endpoint: Option<String>,
}
