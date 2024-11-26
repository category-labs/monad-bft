#![allow(async_fn_in_trait, clippy::async_trait)]

use std::{sync::Arc, time::Instant};

use clap::Parser;
use eyre::{eyre, Result};
use futures::future::join_all;
use tokio::{
    sync::Semaphore,
    time::{sleep, Duration},
    try_join,
};
use tracing::{error, info, warn, Level};

use monad_archive::{
    archive_interface::{ArchiveReader, ArchiveWriter, LatestKind},
    metrics::Metrics,
    s3_archive::{get_aws_config, S3Archive, S3Bucket},
};
use monad_triedb_utils::triedb_env::{Triedb, TriedbEnv};
mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    let metrics = Metrics::new(
        args.otel_endpoint,
        "monad-archiver",
        Duration::from_secs(15),
    )?;

    let max_concurrent_blocks = args.max_concurrent_blocks;
    let concurrent_block_semaphore = Arc::new(Semaphore::new(max_concurrent_blocks));

    // This will spin off a polling thread
    let triedb = TriedbEnv::new(
        &args.triedb_path,
        args.triedb_max_concurrent_requests as usize,
    );

    // Construct s3 and dynamodb connections
    let sdk_config = get_aws_config(args.region).await;
    let s3_archive_writer =
        S3Archive::new(S3Bucket::new(args.s3_bucket, &sdk_config, metrics.clone())).await?;

    // Check for new blocks every 100 ms
    // Issue requests to triedb, poll data and push to relevant tables
    loop {
        sleep(Duration::from_millis(100)).await;

        let trie_db_block_number = triedb.get_latest_block().await.map_err(|e| eyre!("{e}"))?;
        metrics.gauge("triedb_block_number", trie_db_block_number);

        let latest_processed_block =
            (s3_archive_writer.get_latest(LatestKind::Uploaded).await).unwrap_or_default();
        metrics.gauge("latest_uploaded", latest_processed_block);

        if trie_db_block_number <= latest_processed_block {
            info!(
                "Nothing to process. S3 archive progress: {}, triedb progress: {}",
                latest_processed_block, trie_db_block_number
            );
            continue;
        }

        let start_block_number = if latest_processed_block == 0 {
            0
        } else {
            latest_processed_block + 1
        };

        let end_block_number =
            trie_db_block_number.min(start_block_number + args.max_blocks_per_iteration - 1);
        metrics.gauge("end_block_number", end_block_number);

        let start = Instant::now();
        info!(
            "Processing blocks from {} to {}, start time = {:?}",
            start_block_number, end_block_number, start
        );

        let join_handles = (start_block_number..=end_block_number).map(|current_block: u64| {
            let triedb = triedb.clone();
            let s3_archive_writer = s3_archive_writer.clone();

            let semaphore = concurrent_block_semaphore.clone();
            tokio::spawn(async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .expect("Got permit to execute a new block");
                handle_block(&triedb, current_block, &s3_archive_writer).await
            })
        });

        let block_results = join_all(join_handles).await;
        let mut current_join_block = start_block_number;
        for block_result in block_results {
            match block_result {
                Ok(Ok(())) => {
                    current_join_block += 1;
                }
                Ok(Err(e)) => {
                    error!(current_join_block, "Failure writing block, {e}");
                    break;
                }
                Err(e) => {
                    error!(current_join_block, "Failure writing block, {e}");
                    break;
                }
            }
        }

        if current_join_block != 0 {
            s3_archive_writer
                .update_latest(current_join_block - 1, LatestKind::Uploaded)
                .await?;
        }

        let duration = start.elapsed();
        info!("Time spent = {:?}", duration);
    }
}

async fn handle_block(
    triedb: &TriedbEnv,
    current_block: u64,
    s3_archive: &S3Archive,
) -> Result<()> {
    /*  Store Blocks */
    let block_header = match triedb
        .get_block_header(current_block)
        .await
        .map_err(|e| eyre!("{e}"))?
    {
        Some(header) => header,
        None => {
            warn!("Can't find block {} in triedb", current_block);
            return Ok(());
        }
    };
    let transactions = triedb
        .get_transactions(current_block)
        .await
        .map_err(|e| eyre!("{e}"))?;

    let f_block = s3_archive.archive_block(block_header, transactions, current_block);

    /* Store Receipts */
    let receipts = triedb
        .get_receipts(current_block)
        .await
        .map_err(|e| eyre!("{e}"))?;

    let f_receipt = s3_archive.archive_receipts(receipts, current_block);

    /* Store Traces */
    let traces: Vec<Vec<u8>> = triedb
        .get_call_frames(current_block)
        .await
        .map_err(|e| eyre!("{e}"))?;

    let f_trace = s3_archive.archive_traces(traces, current_block);

    match try_join!(f_block, f_receipt, f_trace) {
        Ok(_) => {
            info!("Successfully archived block {}", current_block);
        }
        Err(e) => {
            error!("Error archiving block {}: {:?}", current_block, e);
        }
    }

    Ok(())
}
