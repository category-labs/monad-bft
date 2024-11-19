use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "monad-archive", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    pub triedb_path: PathBuf,

    #[arg(long)]
    pub s3_bucket: String,

    #[arg(long)]
    pub region: Option<String>,

    #[arg(long)]
    pub db_table: String,

    /// Set the max response size in bytes (the same as monad-rpc)
    #[arg(long, default_value_t = 25_000_000)]
    pub max_response_size: u32,

    /// Set the concurrent blocks
    #[arg(long, default_value_t = 10)]
    pub max_concurrent_blocks: usize,
}
