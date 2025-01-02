#![allow(unused_imports)]

use std::{
    ops::{Range, RangeInclusive},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::hex::ToHexExt;
use archive_reader::{ArchiveReader, LatestKind::*};
use archive_tx_index::TxIndexArchiver;
use clap::Parser;
use eyre::Result;
use futures::{executor::block_on, future::join_all, StreamExt, TryStreamExt};
use metrics::Metrics;
use monad_archive::*;
use tokio::{join, sync::Semaphore, time::sleep, try_join};
use tracing::{error, info, warn, Level};

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    info!(?args);

    let metrics = Metrics::new(args.otel_endpoint, "monad-indexer", Duration::from_secs(15))?;

    let block_data_reader = args.block_data_source.build(&metrics).await?;
    let tx_index_archiver = args.archive_sink.build_index_archive(&metrics).await?;

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
    ))
    .await
    .map_err(Into::into)
}

async fn index_worker(
    block_data_reader: (impl BlockDataReader + Sync),
    indexer: TxIndexArchiver,
    max_blocks_per_iteration: u64,
    max_concurrent_blocks: usize,
    metrics: Metrics,
    start_block_override: Option<u64>,
) {
    // initialize starting block using either override or stored latest
    let mut start_block = match start_block_override {
        Some(start_block) => start_block,
        None => {
            let mut latest = indexer.get_latest_indexed().await.unwrap_or(0);
            if latest != 0 {
                latest += 1
            }
            latest
        }
    };

    loop {
        sleep(Duration::from_millis(100)).await;

        // query latest
        let latest_source = match block_data_reader.get_latest(Uploaded).await {
            Ok(number) => number,
            Err(e) => {
                warn!("Error getting latest uploaded block: {e:?}");
                continue;
            }
        };

        let end_block = latest_source.min(start_block + max_blocks_per_iteration - 1);

        if end_block < start_block {
            info!(start_block, end_block, latest_source, "Nothing to process");
            continue;
        }
        info!(
            start_block,
            end_block, latest_source, "Indexing group of blocks"
        );
        metrics.gauge("source_latest_block_num", latest_source);
        metrics.gauge("end_block_number", end_block);
        metrics.gauge("start_block_number", start_block);

        let latest_indexed = index_blocks(
            &block_data_reader,
            &indexer,
            start_block..=end_block,
            max_concurrent_blocks,
            &metrics,
        )
        .await;

        start_block = if latest_indexed == 0 {
            0
        } else {
            latest_indexed + 1
        };
    }
}

async fn index_blocks(
    block_data_reader: &impl BlockDataReader,
    indexer: &TxIndexArchiver,
    block_range: RangeInclusive<u64>,
    concurrency: usize,
    metrics: &Metrics,
) -> u64 {
    let start = Instant::now();

    let res: Result<usize, u64> = futures::stream::iter(block_range.clone())
        .map(|block_num: u64| async move {
            match handle_block(block_data_reader, indexer, block_num).await {
                Ok(num_txs) => Ok((num_txs, block_num)),
                Err(e) => {
                    error!("Failed to handle block: {e:?}");
                    Err(block_num)
                }
            }
        })
        .buffered(concurrency)
        .then(|r| async move {
            if let Ok((_, block_num)) = &r {
                if block_num % 10 == 0 {
                    checkpoint_latest(indexer, *block_num).await;
                }
            }
            r.map(|(num_txs, _)| num_txs)
        })
        .try_fold(0, |total_txs, block_txs| async move {
            Ok(total_txs + block_txs)
        })
        .await;

    let (num_txs_indexed, new_latest_indexed) = match res {
        Ok(num_txs) => (num_txs, *block_range.end()),
        Err(err_block) => (0, err_block - 1),
    };

    info!(
        elapsed = start.elapsed().as_millis(),
        start = block_range.start(),
        end = block_range.end(),
        num_txs_indexed,
        "Finished indexing range",
    );
    metrics.counter("txs_indexed", num_txs_indexed as u64);

    if new_latest_indexed != 0 {
        checkpoint_latest(indexer, new_latest_indexed).await;
    }

    new_latest_indexed
}

async fn handle_block(
    block_data_reader: &impl BlockDataReader,
    tx_index_archiver: &TxIndexArchiver,
    block_num: u64,
) -> Result<usize> {
    let (block, traces, receipts) = try_join!(
        block_data_reader.get_block_by_number(block_num),
        block_data_reader.get_block_traces(block_num),
        block_data_reader.get_block_receipts(block_num)
    )?;
    let num_txs = block.body.transactions.len();
    info!(num_txs, block_num, "Indexing block...");

    let first = block.body.transactions.first().cloned();
    let first_rx = receipts.first().cloned();
    let first_trace = traces.first().cloned();
    tx_index_archiver
        .index_block(block, traces, receipts)
        .await?;

    // check 1 key
    if let Some(tx) = first {
        let key = tx.tx_hash().encode_hex();
        match tx_index_archiver.store.get(tx.tx_hash()).await {
            Ok(Some(resp)) => {
                if resp.header_subset.block_number != block_num
                    || Some(&resp.receipt) != first_rx.as_ref()
                    || Some(&resp.trace) != first_trace.as_ref()
                {
                    warn!(key, block_num, ?resp, "Returned index not as expected");
                } else {
                    info!(key, block_num, "Index spot-check successful");
                }
            }
            Ok(None) => warn!(key, block_num, "No item found for key"),
            Err(e) => warn!(key, block_num, "Error while checking: {e}"),
        };
    }

    Ok(num_txs)
}

async fn checkpoint_latest(archiver: &TxIndexArchiver, block_num: u64) {
    match archiver.update_latest_indexed(block_num).await {
        Ok(()) => info!(block_num, "Set latest indexed checkpoint"),
        Err(e) => error!(block_num, "Failed to set latest indexed block: {e:?}"),
    }
}
