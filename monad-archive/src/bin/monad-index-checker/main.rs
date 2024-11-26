#![allow(unused_imports)]

use std::{
    io::Write,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use archive_interface::{ArchiveReader, ArchiveWriter, LatestKind::*};
use chrono::{
    format::{DelayedFormat, StrftimeItems},
    prelude::*,
};
use clap::Parser;
use dynamodb::{DynamoDBArchive, TxIndexReader, TxIndexedData};
use eyre::{Context, Result};
use fault::{get_timestamp, BlockCheckResult, Fault, FaultWriter};
use futures::{executor::block_on, future::join_all, stream, StreamExt};
use metrics::Metrics;
use monad_archive::*;
use s3_archive::{get_aws_config, S3Archive, S3Bucket};
use serde::{Deserialize, Serialize};
use tokio::{
    io::AsyncWriteExt,
    join,
    sync::{Mutex, Semaphore},
    time::sleep,
};
use tracing::{error, info, warn, Level};

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    let metrics = Metrics::new(args.otel_endpoint, "monad-indexer", Duration::from_secs(15))?;

    // Construct s3 and dynamodb connections
    let sdk_config = get_aws_config(args.region).await;
    let archive = S3Archive::new(S3Bucket::new(
        args.archive_bucket,
        &sdk_config,
        metrics.clone(),
    ))
    .await?
    .as_reader();
    let dynamodb_archive = DynamoDBArchive::new(
        args.db_table,
        &sdk_config,
        args.max_concurrent_connections,
        metrics.clone(),
    );

    let mut latest_checked = args.start_block.unwrap_or(0);

    let mut fault_writer = FaultWriter::new(&args.checker_path).await?;

    loop {
        sleep(Duration::from_millis(100)).await;
        let start = Instant::now();

        // get latest indexed and indexed from s3
        let latest_indexed = match archive.get_latest(Uploaded).await {
            Ok(number) => number,
            Err(e) => {
                warn!("Error getting latest uploaded block: {e:?}");
                continue;
            }
        };

        if latest_checked >= latest_indexed {
            info!(latest_checked, latest_indexed, "Nothing to process");
            continue;
        }

        // compute start and end block for this upload
        let start_block_num = if latest_checked == 0 {
            0
        } else {
            latest_checked + 1
        };
        let end_block_num = latest_indexed.min(start_block_num + args.max_blocks_per_iteration);

        info!(
            start_block_num,
            end_block_num, latest_indexed, "Spawning checker tasks for blocks"
        );

        if let Err(e) = handle_blocks(
            &archive,
            &dynamodb_archive,
            start_block_num,
            end_block_num,
            args.max_concurrent_connections,
            &mut fault_writer,
            &metrics,
        )
        .await
        {
            error!("Error handling blocks: {e:?}");
            continue;
        } else {
            latest_checked = end_block_num;
        }

        let duration = start.elapsed();
        info!("Time spent = {:?}", duration);
    }
}

async fn handle_blocks(
    archive: &(impl ArchiveReader + 'static),
    dynamodb: &DynamoDBArchive,
    start_block_num: u64,
    end_block_num: u64,
    concurrency: usize,
    fault_writer: &mut FaultWriter,
    metrics: &Metrics,
) -> Result<()> {
    let faults: Vec<_> = stream::iter(start_block_num..=end_block_num)
        .map(|block_num| async move {
            let archive = archive.clone();
            let dynamodb = dynamodb.clone();
            let check_result = tokio::spawn(handle_block(archive, dynamodb, block_num)).await;

            let check = match check_result {
                Ok(Ok(fault)) => fault,
                Ok(Err(e)) => {
                    error!("Encountered error handling block: {e:?}");
                    BlockCheckResult {
                        timestamp: get_timestamp(),
                        block_num,
                        faults: vec![Fault::ErrorChecking {
                            err: format!("{e:?}"),
                        }],
                    }
                }
                Err(e) => {
                    error!("Encountered error handling block: {e:?}");
                    BlockCheckResult {
                        timestamp: get_timestamp(),
                        block_num,
                        faults: vec![Fault::ErrorChecking {
                            err: format!("{e:?}"),
                        }],
                    }
                }
            };
            check
            // todo: write per item instead of after joining
            // e.g.:
            // fault_writer.write_fault(check).await
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    for block_check in &faults {
        if block_check.faults.len() > 0 {
            metrics.counter("faults_blocks_with_faults", 1);
        }
        for fault in &block_check.faults {
            match fault {
                Fault::ErrorChecking { .. } => metrics.counter("faults_error_checking", 1),
                Fault::MissingBlock => metrics.counter("faults_missing_blocks", 1),
                Fault::CorruptedBlock => metrics.counter("faults_corrupted_blocks", 1),
                Fault::MissingAllTxHash { num_txs } => {
                    metrics.counter("faults_blocks_missing_all_txhash", 1);
                    metrics.counter("faults_missing_txhash", *num_txs);
                }
                Fault::MissingTxhash { .. } => metrics.counter("faults_missing_txhash", 1),
                Fault::WrongBlockNumber { .. } => metrics.counter("faults_wrong_block_number", 1),
            }
        }
    }

    fault_writer.write_faults(&faults).await
}

async fn handle_block(
    archive: impl ArchiveReader,
    dynamodb: DynamoDBArchive,
    block_num: u64,
) -> Result<BlockCheckResult> {
    let block = archive.get_block_by_number(block_num).await?;
    info!(num_txs = block.body.len(), block_num, "Handling block");

    if block.body.is_empty() {
        return Ok(BlockCheckResult::valid(block_num));
    }

    let hashes = block
        .body
        .iter()
        .map(|tx| tx.hash().to_string())
        .collect::<Vec<_>>();

    let mut faults = dynamodb
        .batch_get_data(&hashes)
        .await?
        .into_iter()
        .zip(hashes.into_iter())
        .filter_map(|(resp, txhash)| match resp {
            None => Some(Fault::MissingTxhash { txhash }),
            Some(TxIndexedData {
                block_num: bnum, ..
            }) if bnum != block_num => Some(Fault::WrongBlockNumber {
                txhash,
                wrong_block_num: bnum,
            }),
            Some(_) => None,
        })
        .collect::<Vec<_>>();

    // reduce all txhash case
    if faults.len() == block.body.len()
        && faults
            .iter()
            .all(|f| matches!(f, Fault::MissingTxhash { .. }))
    {
        faults.clear();
        faults.push(Fault::MissingAllTxHash {
            num_txs: block.body.len() as u64,
        });
    }

    Ok(BlockCheckResult::new(block_num, faults))
}
