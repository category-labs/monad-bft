use futures::TryStreamExt;
use monad_archive::{model::logs_index::LogsIndexArchiver, prelude::*};

pub async fn run_migrate_logs(args: crate::cli::Cli) -> Result<()> {
    let metrics = Metrics::none();

    let block_data_reader = args.block_data_source.build(&metrics).await?;
    let tx_index_archiver = args
        .archive_sink
        .build_index_archive(&metrics, 350 * 1024)
        .await?;

    info!("Building log index archiver...");
    let log_index_archiver =
        LogsIndexArchiver::from_tx_index_archiver(&tx_index_archiver, 50, false)
            .await
            .wrap_err("Failed to create log index reader")?;

    let start_block = args.start_block.unwrap_or(0);
    // If stop block not set, query block data reader for latest
    let stop_block = if let Some(stop_block) = args.stop_block {
        stop_block
    } else {
        block_data_reader
            .get_latest(LatestKind::Indexed)
            .await?
            .ok_or_eyre("Latest block not found")?
    };

    // tokio main should not await futures directly, so we spawn a worker
    tokio::spawn(reindex_worker(
        block_data_reader,
        log_index_archiver,
        args.max_concurrent_blocks,
        start_block,
        stop_block,
    ))
    .await?
}

async fn reindex_worker(
    block_data_reader: impl BlockDataReader + Sync + Send,
    log_index: LogsIndexArchiver,
    max_concurrent_blocks: usize,
    start_block: u64,
    stop_block: u64,
) -> Result<()> {
    futures::stream::iter(start_block..=stop_block)
        .map(|block_num| {
            let block_data_reader = block_data_reader.clone();
            let log_index = log_index.clone();
            async move { handle_block(&block_data_reader, &log_index, block_num).await }
        })
        .buffer_unordered(max_concurrent_blocks)
        .try_collect::<()>()
        .await
}

async fn handle_block(
    block_data_reader: &(impl BlockDataReader + Send),
    log_index: &LogsIndexArchiver,
    block_num: u64,
) -> Result<()> {
    let BlockDataWithOffsets {
        block, receipts, ..
    } = block_data_reader
        .get_block_data_with_offsets(block_num)
        .await?;

    info!(
        num_txs = block.body.transactions.len(),
        block_num, "Indexing block..."
    );

    log_index.index_block(&block, &receipts).await?;
    Ok(())
}
