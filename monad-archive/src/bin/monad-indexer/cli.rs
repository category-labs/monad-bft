use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "monad-indexer", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    pub s3_bucket: String,

    #[arg(long)]
    pub region: Option<String>,

    #[arg(long)]
    pub db_table: String,

    /// Set the max response size in bytes (the same as monad-rpc)
    #[arg(long, default_value_t = 25_000_000)]
    pub max_response_size: u32,

    /// Set the concurrent connections
    #[arg(long, default_value_t = 5)]
    pub max_concurrent_connections: usize,

    /// Number of blocks to handle per loop iteration
    #[arg(long, default_value_t = 20)]
    pub max_blocks_per_iteration: u64,

    #[arg(long, default_value_t = false)]
    pub reset_index: bool,

    /// Block number to start at
    #[arg(long)]
    pub start_block: Option<u64>,

    #[arg(long)]
    pub otel_endpoint: Option<String>,
}
