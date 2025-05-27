use clap::Parser;
use monad_archive::{
    model::logs_index::LogsIndexArchiver, prelude::*, workers::index_worker::index_worker,
};
use tracing::{info, Level};

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    info!(?args, "Cli Arguments: ");

    let metrics = Metrics::new(
        args.otel_endpoint,
        "monad-indexer",
        args.otel_replica_name_override
            .unwrap_or_else(|| args.archive_sink.replica_name()),
        Duration::from_secs(15),
    )?;

    let block_data_reader = args.block_data_source.build(&metrics).await?;
    let tx_index_archiver = args
        .archive_sink
        .build_index_archive(&metrics, args.max_inline_encoded_len)
        .await?;

    let log_index_archiver = match &tx_index_archiver.index_store {
        KVStoreErased::MongoDbStorage(_storage) => {
            info!("Building log index archiver...");
            Some(
                LogsIndexArchiver::from_tx_index_archiver(&tx_index_archiver, 50, false)
                    .await
                    .wrap_err("Failed to create log index reader")?,
            )
        }
        _ => None,
    };
    info!("Log index archiver: {:?}", log_index_archiver.is_some());

    // Confirm connectivity
    if !args.skip_connectivity_check {
        block_data_reader
            .get_latest(LatestKind::Uploaded)
            .await
            .wrap_err("Cannot connect to block data source")?;
        tx_index_archiver
            .get_latest_indexed()
            .await
            .wrap_err("Cannot connect to archive sink")?;
    }

    // for testing
    if args.reset_index {
        tx_index_archiver.update_latest_indexed(0).await?;
    }

    // tokio main should not await futures directly, so we spawn a worker
    tokio::spawn(index_worker(
        block_data_reader,
        tx_index_archiver,
        log_index_archiver,
        args.max_blocks_per_iteration,
        args.max_concurrent_blocks,
        metrics,
        args.start_block,
        args.stop_block,
        Duration::from_millis(500),
    ))
    .await
    .map_err(Into::into)
}
