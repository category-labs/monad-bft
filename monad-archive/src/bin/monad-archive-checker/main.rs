#![allow(async_fn_in_trait, clippy::async_trait)]

use clap::Parser;
use eyre::Result;
use futures::join;
use reth_primitives::{Block, ReceiptWithBloom};
use tokio::time::{sleep, Duration};
use tracing::{error, info, Level};

use monad_archive::{
    archive_interface::{ArchiveReader, LatestKind},
    metrics::Metrics,
    s3_archive::{get_aws_config, S3Archive, S3Bucket},
};

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    let metrics = Metrics::new(args.otel_endpoint, "monad-indexer", Duration::from_secs(15))?;

    let mut s3_archive_readers = Vec::new();

    let s3_buckets = args.s3_buckets.clone();
    let regions = args.regions.clone();

    if s3_buckets.len() == 0 {
        panic!("Need to specify at least 1 bucket");
    }

    if s3_buckets.len() != regions.len() {
        panic!(
            "Size of buckets and regions should be the same. Bucket size: {}, Regions size: {}",
            s3_buckets.len(),
            regions.len()
        );
    }

    // Configure all archive checkers
    for idx in 0..s3_buckets.len() {
        let config = get_aws_config(Some(args.regions[idx].clone())).await;
        let s3_archive_reader = S3Archive::new(S3Bucket::new(
            s3_buckets[idx].clone(),
            &config,
            metrics.clone(),
        ))
        .await?;

        s3_archive_readers.push(s3_archive_reader);
    }

    let mut start_block_number = args.start_block_number;

    info!(
        "Checking buckets {:?} from regions {:?}. Starting from block: {}",
        &s3_buckets, &regions, start_block_number
    );

    loop {
        let latest_block_number =
            latest_uploaded_block(&s3_archive_readers, &args.max_lag, &s3_buckets).await;
        let end_block_number =
            latest_block_number.min(start_block_number + args.max_blocks_per_iteration);

        info!(
            "Start block: {}, end block: {}, latest block: {}",
            start_block_number, end_block_number, latest_block_number
        );

        if end_block_number <= start_block_number {
            info!("Nothing to do. Sleeping for 10s");
            sleep(Duration::from_secs(10)).await;
            continue;
        }

        for block_number in start_block_number..end_block_number {
            let blocks_data = get_block_data(&s3_archive_readers, block_number).await;
            let block_result_has_error = pairwise_check(&blocks_data, block_number);

            if !block_result_has_error {
                info!("Block {} is consistant across buckets", block_number);
            } else {
                error!("Block {} is inconsistant across buckets", block_number);
            }
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
    let mut max_block_number: u64 = 0;
    let mut buckets_latest_block = Vec::new();

    for s3_archive_reader in s3_archive_readers {
        let bucket_latest_block = s3_archive_reader
            .get_latest(LatestKind::Uploaded)
            .await
            .unwrap_or_default();
        buckets_latest_block.push(bucket_latest_block);

        max_block_number = max_block_number.max(bucket_latest_block);
    }

    let mut min_block_number: u64 = max_block_number;

    for i in 0..s3_buckets.len() {
        let bucket = &s3_buckets[i];
        let bucket_latest_block = buckets_latest_block[i];

        if bucket_latest_block + max_lag <= max_block_number {
            error!(
                "Bucket '{}' falling behind. Tip: {}, Current: {}",
                bucket, &max_block_number, bucket_latest_block
            );
        } else {
            // We only update min when it's not too behind
            min_block_number = min_block_number.min(bucket_latest_block);
        }
    }

    return min_block_number;
}

struct BlockData {
    pub bucket: String,
    pub block: Option<Block>,
    pub receipts: Option<Vec<ReceiptWithBloom>>,
    pub traces: Option<Vec<Vec<u8>>>,
}

async fn get_block_data(s3_archive_readers: &[S3Archive], block_number: u64) -> Vec<BlockData> {
    let mut blocks_data = Vec::new();

    for reader in s3_archive_readers {
        let bucket = reader.bucket.bucket.clone();

        let mut block_data = BlockData {
            bucket: bucket.clone(),
            block: None,
            receipts: None,
            traces: None,
        };

        let f_block = reader.get_block_by_number(block_number);
        let f_receipts = reader.get_block_receipts(block_number);
        let f_traces = reader.get_block_traces(block_number);

        let (block_result, receipts_result, traces_result) = join!(f_block, f_receipts, f_traces);

        match block_result {
            Ok(block) => {
                block_data.block = Some(block);
            }
            Err(e) => {
                error!(
                    "Failed to get block {} for bucket '{}'. Error: {:?}",
                    &block_number, bucket, e
                );
            }
        }

        match receipts_result {
            Ok(receipts) => {
                block_data.receipts = Some(receipts);
            }
            Err(e) => {
                error!(
                    "Failed to get receipts {} for bucket '{}'. Error: {:?}",
                    &block_number, bucket, e
                );
            }
        }

        match traces_result {
            Ok(traces) => {
                block_data.traces = Some(traces);
            }
            Err(e) => {
                error!(
                    "Failed to get traces {} for bucket '{}'. Error: {:?}",
                    &block_number, bucket, e
                );
            }
        }

        blocks_data.push(block_data);
    }

    return blocks_data;
}

fn pairwise_check(blocks_data: &[BlockData], block_number: u64) -> bool {
    let mut has_error = false;

    // pairwise comparison
    for i in 0..blocks_data.len() {
        for j in i + 1..blocks_data.len() {
            if blocks_data[i].block != blocks_data[j].block {
                error!(
                    "Block {} is different between bucket '{}' and bucket '{}'",
                    block_number, blocks_data[i].bucket, blocks_data[j].bucket
                );
                has_error = true;
            } else {
                info!(
                    "Block {} is same between bucket '{}' and bucket '{}'",
                    block_number, blocks_data[i].bucket, blocks_data[j].bucket
                );
            }

            if blocks_data[i].receipts != blocks_data[j].receipts {
                error!(
                    "Receipts {} is different between bucket '{}' and bucket '{}'",
                    block_number, blocks_data[i].bucket, blocks_data[j].bucket
                );
                has_error = true;
            } else {
                info!(
                    "Receipts {} is same between bucket '{}' and bucket '{}'",
                    block_number, blocks_data[i].bucket, blocks_data[j].bucket
                );
            }

            if blocks_data[i].traces != blocks_data[j].traces {
                error!(
                    "Traces {} is different between bucket '{}' and bucket '{}'",
                    block_number, blocks_data[i].bucket, blocks_data[j].bucket
                );
                has_error = true;
            } else {
                info!(
                    "Traces {} is same between bucket '{}' and bucket '{}'",
                    block_number, blocks_data[i].bucket, blocks_data[j].bucket
                );
            }
        }
    }

    return has_error;
}
