#![allow(async_fn_in_trait)]

use std::{ops::RangeInclusive, time::Instant};

use bft_block_archiver::bft_block_archive_worker;
use clap::Parser;
use eyre::Result;
use futures::{StreamExt, TryStreamExt};
use metrics::Metrics;
use monad_archive::*;
use tokio::{
    time::{sleep, Duration},
    try_join,
};
use tracing::{error, info, warn, Level};
use wal_checkpoint::wal_checkpoint_worker;

mod bft_block_archiver;
mod cli;
mod wal_checkpoint;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    info!(?args);

    let metrics = Metrics::new(
        args.otel_endpoint,
        "monad-archiver",
        Duration::from_secs(15),
    )?;

    let block_data_source = args.block_data_source.build(&metrics).await?;
    let archive_writer = args.archive_sink.build_block_data_archive(&metrics).await?;

    if let Some(path) = args.bft_block_body_path {
        info!("Spawning bft block archive worker...");
        tokio::spawn(bft_block_archive_worker(
            archive_writer.store.clone(),
            args.bft_block_header_path.unwrap_or(path.clone()),
            path,
            Duration::from_secs(args.bft_block_poll_freq_secs),
            metrics.clone(),
        ));
    }

    if let Some(path) = args.wal_path {
        info!("Spawning wal checkpoint worker...");
        tokio::spawn(wal_checkpoint_worker(
            archive_writer.store.clone(),
            path,
            Duration::from_secs(args.wal_checkpoint_freq_secs),
        ));
    }

    tokio::spawn(archive_worker(
        block_data_source,
        archive_writer,
        args.max_blocks_per_iteration,
        args.max_concurrent_blocks,
        args.start_block,
        metrics,
    ))
    .await
    .map_err(Into::into)
}

async fn archive_worker(
    block_data_source: (impl BlockDataReader + Sync),
    archive_writer: BlockDataArchive,
    max_blocks_per_iteration: u64,
    max_concurrent_blocks: usize,
    mut start_block_override: Option<u64>,
    metrics: Metrics,
) {
    // initialize starting block using either override or stored latest
    let mut start_block = match start_block_override.take() {
        Some(start_block) => start_block,
        None => {
            let latest_uploaded = archive_writer
                .get_latest(LatestKind::Uploaded)
                .await
                .unwrap_or(0);
            if latest_uploaded == 0 {
                0
            } else {
                latest_uploaded + 1
            }
        }
    };

    loop {
        sleep(Duration::from_millis(100)).await;

        // query latest
        let latest_source = match block_data_source.get_latest(LatestKind::Uploaded).await {
            Ok(number) => number,
            Err(e) => {
                warn!("Error getting latest source block: {e:?}");
                continue;
            }
        };

        let end_block = latest_source.min(start_block + max_blocks_per_iteration - 1);
        if end_block < start_block {
            info!(start_block, end_block, "Nothing to process");
            continue;
        }

        metrics.gauge("source_latest_block_num", latest_source);
        metrics.gauge("end_block_number", end_block);
        metrics.gauge("start_block_number", start_block);

        info!(
            start = start_block,
            end = end_block,
            latest_source,
            "Archiving group of blocks",
        );

        let latest_uploaded = archive_blocks(
            &block_data_source,
            start_block..=end_block,
            &archive_writer,
            max_concurrent_blocks,
        )
        .await;

        start_block = if latest_uploaded == 0 {
            0
        } else {
            latest_uploaded + 1
        };
    }
}

async fn archive_blocks(
    reader: &(impl BlockDataReader + Sync),
    range: RangeInclusive<u64>,
    archiver: &BlockDataArchive,
    concurrency: usize,
) -> u64 {
    let start = Instant::now();

    let res: Result<(), u64> = futures::stream::iter(range.clone())
        .map(|block_num: u64| async move {
            match archive_block(reader, block_num, archiver).await {
                Ok(_) => Ok(block_num),
                Err(e) => {
                    error!("Failed to handle block: {e:?}");
                    Err(block_num)
                }
            }
        })
        .buffered(concurrency)
        .then(|r| async move {
            if let Ok(block_num) = &r {
                if block_num % 10 == 0 {
                    checkpoint_latest(archiver, *block_num).await;
                }
            }
            r.map(|_| ())
        })
        .try_collect()
        .await;

    info!(
        elapsed = start.elapsed().as_millis(),
        start = range.start(),
        end = range.end(),
        "Finished archiving range",
    );

    let new_latest_uploaded = match res {
        Ok(()) => *range.end(),
        Err(err_block) => err_block.saturating_sub(1),
    };

    if new_latest_uploaded != 0 {
        checkpoint_latest(archiver, new_latest_uploaded).await;
    }

    new_latest_uploaded
}

async fn archive_block(
    reader: &impl BlockDataReader,
    block_num: u64,
    archiver: &BlockDataArchive,
) -> Result<()> {
    let mut num_txs = None;

    try_join!(
        async {
            // NOTE: is it ok to error out here? Previous behavior returned Ok(())
            // with a warn if the header was not found.
            // That seems incorrect, but should investigate
            let block = reader.get_block_by_number(block_num).await?;
            num_txs = Some(block.body.transactions.len());
            archiver.archive_block(block).await
        },
        async {
            let receipts = reader.get_block_receipts(block_num).await?;
            archiver.archive_receipts(receipts, block_num).await
        },
        async {
            let traces = reader.get_block_traces(block_num).await?;
            archiver.archive_traces(traces, block_num).await
        },
    )?;
    info!(block_num, num_txs, "Successfully archived block");
    Ok(())
}

async fn checkpoint_latest(archiver: &BlockDataArchive, block_num: u64) {
    match archiver
        .update_latest(block_num, LatestKind::Uploaded)
        .await
    {
        Ok(()) => info!(block_num, "Set latest uploaded checkpoint"),
        Err(e) => error!(block_num, "Failed to set latest uploaded block: {e:?}"),
    }
}
