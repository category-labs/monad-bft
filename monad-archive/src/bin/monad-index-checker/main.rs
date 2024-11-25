#![allow(unused_imports)]

use std::{
    io::Write,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use archive_interface::{ArchiveWriterInterface, LatestKind::*};
use chrono::{
    format::{DelayedFormat, StrftimeItems},
    prelude::*,
};
use clap::Parser;
use dynamodb::DynamoDBArchive;
use eyre::{Context, Result};
use fault::{get_timestamp, BlockCheckResult, Fault, FaultWriter};
use futures::{executor::block_on, future::join_all, stream, StreamExt};
use monad_archive::*;
use s3_archive::{get_aws_config, S3ArchiveWriter, S3Bucket};
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

    // Construct s3 and dynamodb connections
    let sdk_config = get_aws_config(args.region).await;
    let archive = S3ArchiveWriter::new(S3Bucket::new(args.archive_bucket, &sdk_config)).await?;
    let dynamodb_archive =
        DynamoDBArchive::new(args.db_table, &sdk_config, args.max_concurrent_connections);

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
    archive: &S3ArchiveWriter,
    dynamodb: &DynamoDBArchive,
    start_block_num: u64,
    end_block_num: u64,
    concurrency: usize,
    fault_writer: &mut FaultWriter,
) -> Result<()> {
    let faults: Vec<_> = stream::iter(start_block_num..=end_block_num)
        .map(|block_num| async move {
            let archive = archive.clone();
            let dynamodb = dynamodb.clone();
            let check_result = tokio::spawn(handle_block(archive, dynamodb, block_num)).await;
            // let mut fault_writer = fault_writer.clone();

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
    fault_writer.write_faults(&faults).await
}

async fn handle_block(
    archive: S3ArchiveWriter,
    dynamodb: DynamoDBArchive,
    block_num: u64,
) -> Result<BlockCheckResult> {
    let block = archive.read_block(block_num).await?;
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
        .batch_get_block_nums(&hashes)
        .await?
        .into_iter()
        .zip(hashes.into_iter())
        .filter_map(|(resp_bnum, txhash)| match resp_bnum {
            None => Some(Fault::MissingTxhash { txhash }),
            Some(bnum) if bnum != block_num => Some(Fault::WrongBlockNumber {
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
        faults.push(Fault::MissingAllTxHash);
    }

    Ok(BlockCheckResult::new(block_num, faults))
}
