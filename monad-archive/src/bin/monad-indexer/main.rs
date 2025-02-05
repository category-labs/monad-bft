use clap::Parser;
use monad_archive::{prelude::*, workers::index_worker::index_worker};
use tracing::{info, Level};

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    info!(?args);

    let metrics = Metrics::new(
        args.otel_endpoint,
        "monad-indexer",
        args.archive_sink.replica_name(),
        Duration::from_secs(15),
    )?;

    let block_data_reader = args.block_data_source.build(&metrics).await?;
    let tx_index_archiver = args
        .archive_sink
        .build_index_archive(&metrics, args.max_inline_encoded_len)
        .await?;

    // for testing
    if args.reset_index {
        tx_index_archiver.update_latest_indexed(0).await?;
    }

    // tokio main should not await futures directly, so we spawn a worker
    tokio::spawn(index_worker(
        block_data_reader,
        tx_index_archiver,
        args.max_blocks_per_iteration,
        args.max_concurrent_blocks,
        metrics,
        args.start_block,
        args.stop_block,
        Duration::from_millis(100),
    ))
    .await
    .map_err(Into::into)
}
