use clap::Parser;
use monad_archive::{ArchiveArgs, BlockDataReaderArgs};

#[derive(Debug, Parser)]
#[command(name = "monad-archive", about, long_about = None)]
pub struct Cli {
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    pub block_data_source: BlockDataReaderArgs,

    #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
    pub archive_sink: ArchiveArgs,

    #[arg(long, default_value_t = 100)]
    pub max_blocks_per_iteration: u64,

    #[arg(long, default_value_t = 20)]
    pub max_concurrent_blocks: usize,

    /// Override block number to start at
    #[arg(long)]
    pub start_block: Option<u64>,

    #[arg(long)]
    pub otel_endpoint: Option<String>,
}
