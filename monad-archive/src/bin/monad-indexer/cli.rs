use clap::Parser;
use monad_archive::cli::{ArchiveArgs, BlockDataReaderArgs};

#[derive(Debug, Parser)]
#[command(name = "monad-indexer", about, long_about = None)]
pub struct Cli {
    /// Source to read block data that will be indexed
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    pub block_data_source: BlockDataReaderArgs,

    /// Where archive data is written to
    /// For aws: 'aws <bucket_name> <concurrent_requests>'
    #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
    pub archive_sink: ArchiveArgs,

    #[arg(long, default_value_t = 50)]
    pub max_blocks_per_iteration: u64,

    #[arg(long, default_value_t = 10)]
    pub max_concurrent_blocks: usize,

    /// Resets the latest indexed entry
    #[arg(long, default_value_t = false)]
    pub reset_index: bool,

    /// Override block number to start at
    #[arg(long)]
    pub start_block: Option<u64>,

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
}
