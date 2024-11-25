#![allow(unused_imports)]

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use archive_interface::{ArchiveReader, ArchiveWriter, LatestKind::*};
use clap::Parser;
use dynamodb::{DynamoDBArchive, TxIndexArchiver, TxIndexReader};
use eyre::Result;
use futures::{executor::block_on, future::join_all};
use monad_archive::*;
use s3_archive::{get_aws_config, S3Archive, S3Bucket};
use tokio::{join, sync::Semaphore, time::sleep, try_join};
use tracing::{error, info, warn, Level};

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();

    let concurrent_block_semaphore = Arc::new(Semaphore::new(args.max_concurrent_connections));

    // Construct s3 and dynamodb connections
    let sdk_config = get_aws_config(args.region).await;
    let dynamodb_archive =
        DynamoDBArchive::new(args.db_table, &sdk_config, args.max_concurrent_connections);
    let archive = S3Archive::new(S3Bucket::new(args.s3_bucket, &sdk_config)).await?;

    // for testing
    if args.reset_index {
        archive.update_latest(0, Indexed).await?;
    }

    {
        let latest_indexed = archive.get_latest(Indexed).await.unwrap_or(0);
        let latest_uploaded = archive.get_latest(Uploaded).await.unwrap_or(0);

        info!("Latest indexed block is : {latest_indexed}");
        info!("Latest uploaded block is : {latest_uploaded}");
    }
    let mut latest_indexed = if let Some(start_block) = args.start_block {
        start_block
    } else {
        archive.get_latest(Indexed).await.unwrap_or(0)
    };

    loop {
        sleep(Duration::from_millis(100)).await;
        let start = Instant::now();

        // get latest uploaded and indexed from s3
        let latest_uploaded = match archive.get_latest(Uploaded).await {
            Ok(number) => number,
            Err(e) => {
                warn!("Error getting latest uploaded block: {e:?}");
                continue;
            }
        };

        if latest_uploaded <= latest_indexed {
            info!(latest_indexed, latest_uploaded, "Nothing to process");
            continue;
        }

        // compute start and end block for this upload
        let start_block_number = if latest_indexed == 0 {
            0
        } else {
            latest_indexed + 1
        };
        let end_block_number =
            latest_uploaded.min(start_block_number + args.max_blocks_per_iteration);

        info!(
            start_block_number,
            end_block_number, latest_uploaded, "Spawning index uploads for blocks"
        );

        let join_handles = (start_block_number..=end_block_number).map(|current_block: u64| {
            let archive = archive.clone();
            let dynamodb_archive = dynamodb_archive.clone();

            let semaphore = concurrent_block_semaphore.clone();
            tokio::spawn(async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .expect("Got permit to execute a new block");
                handle_block(&archive, dynamodb_archive, current_block).await
            })
        });

        let mut num_txs_indexed = 0;
        let block_results = join_all(join_handles).await;
        let mut current_join_block = start_block_number;
        for block_result in block_results {
            // two match arm error conditions are similar but have different error types
            let on_err = |e: String| {
                let archive = archive.clone();
                async move {
                    if current_join_block != 0 {
                        info!(current_join_block, "Updating latest");
                        if let Err(e) = archive.update_latest(current_join_block - 1, Indexed).await
                        {
                            error!(
                                "Failed to update latest indexed s3 object, continuing. Error: {e:?}"
                            );
                            return false;
                        };
                    }
                    error!(
                        current_join_block,
                        latest_uploaded, start_block_number, "Error indexing block: {e:?}"
                    );
                    true
                }
            };
            match block_result {
                Ok(Ok(num_txs)) => {
                    current_join_block += 1;
                    num_txs_indexed += num_txs
                }
                Ok(Err(e)) => {
                    if !on_err(e.to_string()).await {
                        continue;
                    }
                }
                Err(e) => {
                    if !on_err(e.to_string()).await {
                        continue;
                    }
                }
            }
        }

        archive.update_latest(end_block_number, Indexed).await?;
        latest_indexed = end_block_number;

        let duration = start.elapsed();
        info!(num_txs_indexed, "Time spent = {:?}", duration);
    }
}

async fn handle_block(
    archive: &S3Archive,
    dynamodb: DynamoDBArchive,
    block_num: u64,
) -> Result<usize> {
    let (block, traces, receipts) = try_join!(
        archive.read_block(block_num),
        archive.get_block_traces(block_num),
        archive.get_block_receipts(block_num)
    )?;
    let num_txs = block.body.len();
    info!(num_txs, block_num, "Block");

    let first = block.body.first().cloned();
    dynamodb.index_block(block, traces, receipts).await?;

    // check 1 key
    if let Some(tx) = first {
        let key = tx.hash.to_string();
        tokio::spawn(async move {
            match dynamodb.get_data(&key).await {
                Ok(resp) => {
                    if resp.is_some() && resp.as_ref().unwrap().block_num == block_num {
                        info!(key, ?resp, "Check successful")
                    } else {
                        warn!(key, ?resp, "Returned mapping not as expected");
                    }
                }
                Err(e) => warn!("Error while checking: {e}"),
            };
        });
    }

    Ok(num_txs)
}
