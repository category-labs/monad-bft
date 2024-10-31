use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "monad-archive", about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    pub triedb_path: Option<PathBuf>,

    #[arg(long)]
    pub s3_bucket: String,

    #[arg(long)]
    pub s3_region: Option<String>,

    /// Set the max response size in bytes (the same as monad-rpc)
    #[arg(long, default_value_t = 25_000_000)]
    pub max_response_size: u32,

    /// Set the concurrency level
    #[arg(long, default_value_t = 10)]
    pub max_concurrency_level: u32,
}
