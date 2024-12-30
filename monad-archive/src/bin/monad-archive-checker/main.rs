#![allow(async_fn_in_trait)]

use std::sync::Arc;

use alloy_consensus::ReceiptEnvelope;
use clap::Parser;
use eyre::Result;
use futures::{future::join_all, join};
use monad_archive::{
    archive_reader::LatestKind,
    fault::{BlockCheckResult, Fault, FaultWriter},
    metrics::Metrics,
    s3_archive::{get_aws_config, Block, S3Archive, S3Bucket},
};
use tokio::{
    sync::Semaphore,
    time::{sleep, Duration},
};
use tracing::{debug, error, info, warn, Level};

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();

    let metrics = Metrics::new(
        args.otel_endpoint,
        "monad-archive-checker",
        Duration::from_secs(15),
    )?;

    let mut s3_archive_readers = Vec::new();

    let s3_buckets = args.s3_buckets.clone();
    let regions = args.regions.clone();

    if s3_buckets.is_empty() {
        panic!("Need to specify at least 1 bucket");
    }

    if s3_buckets.len() != regions.len() {
        panic!(
            "Size of buckets and regions should be the same. Bucket size: {}, Regions size: {}",
            s3_buckets.len(),
            regions.len()
        );
    }

    if args.max_blocks_per_iteration == 0 {
        panic!("Max blocks per iteration can't be 0. Suggested value: 200");
    }

    let max_concurrent_blocks = args.max_concurrent_blocks;
    let concurrent_block_semaphore = Arc::new(Semaphore::new(max_concurrent_blocks));

    // Configure all archive checkers
    for (idx, bucket_name) in s3_buckets.iter().enumerate() {
        let config = get_aws_config(Some(args.regions[idx].clone())).await;
        let s3_archive_reader =
            S3Archive::new(S3Bucket::new(bucket_name.clone(), &config, metrics.clone()));

        s3_archive_readers.push(s3_archive_reader);
    }

    // Initialize fault writer
    let fault_writer = FaultWriter::new(&args.checker_path).await?;
    info!("Writing S3 checking result at {:?}", &args.checker_path);

    let mut start_block_number = args.start_block;
    info!(
        "Checking buckets {:?} from regions {:?}. Starting from block: {}",
        &s3_buckets, &regions, start_block_number
    );

    loop {
        let latest_block_number =
            latest_uploaded_block(&s3_archive_readers, &args.max_lag, &s3_buckets).await;
        let end_block_number =
            latest_block_number.min(start_block_number + args.max_blocks_per_iteration - 1);

        info!(
            "Start block: {}, end block: {}, latest block: {}",
            start_block_number, end_block_number, latest_block_number
        );

        if end_block_number <= start_block_number {
            info!("Nothing to do. Sleeping for 10s");
            sleep(Duration::from_secs(10)).await;
            continue;
        }

        let join_handles = (start_block_number..=end_block_number).map(|current_block: u64| {
            let mut fault_writer = fault_writer.clone();
            let s3_archive_readers = s3_archive_readers.clone();
            let metrics = metrics.clone();

            let semaphore = concurrent_block_semaphore.clone();

            tokio::spawn(async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .expect("Got permit to check a new block");
                let blocks_data = get_block_data(&s3_archive_readers, current_block).await;
                pairwise_check(&blocks_data, current_block, &mut fault_writer, metrics).await
            })
        });

        let block_check_results = join_all(join_handles).await;
        let mut current_block = start_block_number;
        for block_check_result in block_check_results {
            match block_check_result {
                Ok(block_faults_cnt) => {
                    if block_faults_cnt == 0 {
                        info!("Block {} is consistent across buckets", current_block);
                    } else {
                        error!(
                            "Block {} is inconsistent across buckets. Number of faults: {}",
                            current_block, block_faults_cnt
                        );
                    }
                }
                Err(e) => {
                    // TODO: should be abort here??
                    error!("Critical: Unable to join futures!, {e}");
                }
            }
            current_block += 1;
        }

        start_block_number = end_block_number;

        // sleep for 10s
        info!("Sleeping for 10s...");
        sleep(Duration::from_secs(10)).await;
    }
}

async fn latest_uploaded_block(
    s3_archive_readers: &[S3Archive],
    max_lag: &u64,
    s3_buckets: &[String],
) -> u64 {
    let latest_futures = s3_archive_readers.iter().map(|reader| async {
        match reader.get_latest(LatestKind::Uploaded).await {
            Ok(block_number) => block_number,
            Err(e) => {
                // This is not necessarily an error. It might be that the latest is not there yet
                warn!(
                    "Failed to get latest block number for bucket '{}': {:?}",
                    reader.bucket.bucket, e
                );
                0
            }
        }
    });

    let results = join_all(latest_futures).await;

    let max_block_number = results.iter().cloned().max().unwrap_or(0);
    let mut min_block_number = max_block_number;

    for (i, &bucket_latest_block) in results.iter().enumerate() {
        if bucket_latest_block + max_lag <= max_block_number {
            error!(
                "Bucket '{}' falling behind. Tip: {}, Current: {}",
                &s3_buckets[i], max_block_number, bucket_latest_block
            );
        } else {
            // We only update min when it's not too behind
            min_block_number = min_block_number.min(bucket_latest_block);
        }
    }

    min_block_number
}

struct BlockData {
    pub bucket: String,
    pub block: Option<Block>,
    pub receipts: Option<Vec<ReceiptEnvelope>>,
    pub traces: Option<Vec<Vec<u8>>>,
}

async fn get_block_data(s3_archive_readers: &[S3Archive], block_number: u64) -> Vec<BlockData> {
    let block_futures = s3_archive_readers.iter().map(|reader| {
        let bucket = reader.bucket.bucket.clone();
        async move {
            let (block_result, receipts_result, traces_result) = join!(
                reader.get_block_by_number(block_number),
                reader.get_block_receipts(block_number),
                reader.get_block_traces(block_number)
            );

            let block_data = BlockData {
                bucket: bucket.clone(),
                block: block_result.ok(),
                receipts: receipts_result.ok(),
                traces: traces_result.ok(),
            };

            if block_data.block.is_none() {
                error!(
                    "Failed to get block {} for bucket '{}'",
                    block_number, bucket
                );
            }
            if block_data.receipts.is_none() {
                error!(
                    "Failed to get receipts {} for bucket '{}'",
                    block_number, bucket
                );
            }
            if block_data.traces.is_none() {
                error!(
                    "Failed to get traces {} for bucket '{}'",
                    block_number, bucket
                );
            }

            block_data
        }
    });

    join_all(block_futures).await
}

async fn pairwise_check(
    blocks_data: &[BlockData],
    block_number: u64,
    fault_writer: &mut FaultWriter,
    metrics: Metrics,
) -> usize {
    let mut faults = Vec::new();

    let mut missing_block = Vec::new();
    let mut missing_receipts = Vec::new();
    let mut missing_traces = Vec::new();

    // checking for missing stuff
    for block_data in blocks_data {
        let bucket = &block_data.bucket;

        if block_data.block.is_none() {
            missing_block.push(bucket.clone());
        }

        if block_data.receipts.is_none() {
            missing_receipts.push(bucket.clone());
        }

        if block_data.traces.is_none() {
            missing_traces.push(bucket.clone());
        }
    }

    if !missing_block.is_empty() {
        faults.push(Fault::S3MissingBlock {
            buckets: missing_block,
        });
    }
    if !missing_receipts.is_empty() {
        faults.push(Fault::S3MissingReceipts {
            buckets: missing_receipts,
        });
    }
    if !missing_traces.is_empty() {
        faults.push(Fault::S3MissingTraces {
            buckets: missing_traces,
        });
    }

    // pairwise comparison
    for i in 0..blocks_data.len() {
        for j in i + 1..blocks_data.len() {
            let bucket1 = &blocks_data[i].bucket;
            let bucket2 = &blocks_data[j].bucket;

            // TODO: Should we still log if one of them is "Missing"
            if blocks_data[i].block != blocks_data[j].block {
                error!(
                    "Block {} is different between bucket '{}' and bucket '{}'",
                    block_number, bucket1, bucket2
                );
                faults.push(Fault::S3InconsistentBlock {
                    bucket1: bucket1.clone(),
                    bucket2: bucket2.clone(),
                });
            } else {
                debug!(
                    "Block {} is same between bucket '{}' and bucket '{}'",
                    block_number, bucket1, bucket2
                );
            }

            if blocks_data[i].receipts != blocks_data[j].receipts {
                error!(
                    "Receipts {} is different between bucket '{}' and bucket '{}'",
                    block_number, bucket1, bucket2
                );
                faults.push(Fault::S3InconsistentReceipts {
                    bucket1: bucket1.clone(),
                    bucket2: bucket2.clone(),
                });
            } else {
                debug!(
                    "Receipts {} is same between bucket '{}' and bucket '{}'",
                    block_number, bucket1, bucket2
                );
            }

            if blocks_data[i].traces != blocks_data[j].traces {
                error!(
                    "Traces {} is different between bucket '{}' and bucket '{}'",
                    block_number, bucket1, bucket2
                );
                faults.push(Fault::S3InconsistentTraces {
                    bucket1: bucket1.clone(),
                    bucket2: bucket2.clone(),
                });
            } else {
                debug!(
                    "Traces {} is same between bucket '{}' and bucket '{}'",
                    block_number, bucket1, bucket2
                );
            }
        }
    }

    let faults_cnt = faults.len();

    if !faults.is_empty() {
        metrics.counter("faults_blocks_with_faults", 1);

        let block_check_result = BlockCheckResult::new(block_number, faults);
        for fault in &block_check_result.faults {
            match fault {
                Fault::ErrorChecking { .. } => metrics.counter("faults_error_checking", 1),
                Fault::S3MissingBlock { buckets } => {
                    metrics.counter("faults_s3_missing_block", 1);
                    metrics.counter("faults_s3_missing_block_buckets", buckets.len() as u64);
                }
                Fault::S3MissingReceipts { buckets } => {
                    metrics.counter("faults_s3_missing_receipts", 1);
                    metrics.counter("faults_s3_missing_receipts_buckets", buckets.len() as u64);
                }
                Fault::S3MissingTraces { buckets } => {
                    metrics.counter("faults_s3_missing_traces", 1);
                    metrics.counter("faults_s3_missing_traces_buckets", buckets.len() as u64);
                }
                // TODO: Should we use increment?
                Fault::S3InconsistentBlock { .. } => {
                    metrics.counter("faults_s3_inconsistent_block", 1)
                }
                Fault::S3InconsistentReceipts { .. } => {
                    metrics.counter("faults_s3_inconsistent_receipts", 1)
                }
                Fault::S3InconsistentTraces { .. } => {
                    metrics.counter("faults_s3_inconsistent_traces", 1)
                }

                // Other faults are not S3 faults
                _ => (),
            }
        }

        if let Err(e) = fault_writer.write_fault(block_check_result.clone()).await {
            error!(
                "Failed to write results for block {}: {:?}",
                block_number, e
            );
            error!(
                "BlockCheckResults should be written: {:?}",
                block_check_result
            );
        }
    }

    faults_cnt
}
