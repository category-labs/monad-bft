use std::{sync::Arc, time::Instant};

use clap::Parser;
use futures::future::join_all;
use reth_primitives::ReceiptWithBloom;
use s3_archive::S3ArchiveWriter;
use tokio::{
    sync::Semaphore,
    time::{sleep, Duration},
    try_join,
};
use tracing::{error, info, warn, Level};
use triedb::Triedb;

use crate::{
    archive_interface::ArchiveWriterInterface, cli::Cli, errors::ArchiveError,
    s3_archive::S3Archive, triedb::TriedbEnv,
};

mod archive_interface;
mod cli;
mod errors;
mod s3_archive;
mod triedb;

#[tokio::main]
async fn main() -> Result<(), ArchiveError> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = Cli::parse();

    let max_concurrent_blocks = args.max_concurrent_blocks;
    let concurrent_block_semaphore = Arc::new(Semaphore::new(max_concurrent_blocks));

    // This will spin off a polling thread
    let triedb = TriedbEnv::new(&args.triedb_path);

    // Construct an s3 instance
    let s3_archive = S3Archive::new(args.s3_bucket, args.db_table, args.region).await?;
    let s3_archive_writer = S3ArchiveWriter::new(s3_archive).await?;

    let mut latest_processed_block = (s3_archive_writer.get_latest().await).unwrap_or_default();

    info!("Latest processed block is : {}", latest_processed_block);

    // Check for new blocks every 100 ms
    // Issue requests to triedb, poll data and push to relevant tables
    loop {
        sleep(Duration::from_millis(100)).await;

        let block_number = match triedb.get_latest_block().await {
            Ok(number) => number,
            Err(e) => return Err(e),
        };

        if block_number <= latest_processed_block {
            info!(
                "Nothing to process. S3 archive progress: {}, triedb progress: {}",
                latest_processed_block, block_number
            );
            continue;
        }

        let start = Instant::now();
        info!(
            "Processing blocks from {} to {}, start time = {:?}",
            latest_processed_block + 1,
            block_number,
            start
        );

        let start_block_number = if latest_processed_block == 0 {
            0
        } else {
            latest_processed_block + 1
        };

        let join_handles = (start_block_number..=block_number).map(|current_block: u64| {
            let triedb = triedb.clone();
            let s3_archive_writer = s3_archive_writer.clone();

            let semaphore = concurrent_block_semaphore.clone();
            tokio::spawn(async move {
                let permit = semaphore
                    .acquire()
                    .await
                    .expect("Got permit to execute a new block");
                let _ = handle_block(&triedb, current_block, &s3_archive_writer).await;
                std::mem::drop(permit);
            })
        });

        let block_results = join_all(join_handles).await;
        let mut current_join_block = start_block_number;
        for block_result in block_results {
            match block_result {
                Ok(_) => {
                    current_join_block += 1;
                }
                Err(e) => {
                    if current_join_block != 0 {
                        s3_archive_writer
                            .update_latest(current_join_block - 1)
                            .await?;
                    }
                    return Err(ArchiveError::custom_error(format!(
                        "Failure in writing block {}, {:?}",
                        current_join_block, e
                    )));
                }
            }
        }

        s3_archive_writer.update_latest(block_number).await?;
        latest_processed_block = block_number;

        let duration = start.elapsed();
        info!("Time spent = {:?}", duration);
    }
}

async fn handle_block(
    triedb: &TriedbEnv,
    current_block: u64,
    s3_archive: &S3ArchiveWriter,
) -> Result<(), ArchiveError> {
    /*  Store Blocks */
    let block_header = match triedb.get_block_header(current_block).await? {
        Some(header) => header,
        None => {
            warn!("Can't find block {} in triedb", current_block);
            return Ok(());
        }
    };
    let transactions = triedb.get_transactions(current_block).await?;

    let f_block = s3_archive.archive_block(block_header, transactions, current_block);

    /* Store Receipts */
    let receipts: Vec<ReceiptWithBloom> = triedb.get_receipts(current_block).await?;

    let f_receipt = s3_archive.archive_receipts(receipts, current_block);

    /* Store Traces */
    let traces: Vec<Vec<u8>> = triedb.get_call_frames(current_block).await?;

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
