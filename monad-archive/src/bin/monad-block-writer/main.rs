// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::Arc;

use alloy_consensus::Block as AlloyBlock;
use alloy_rlp::Encodable;
use clap::Parser;
use futures::future::join_all;
use monad_archive::prelude::*;
use monad_compress::{brotli::BrotliCompression, CompressionAlgo};
use tokio::{fs, sync::Semaphore};
use tracing::Level;

mod cli;

async fn process_block(
    aws_reader: &ArchiveReader,
    current_block: u64,
    dest_path: &std::path::Path,
) -> Result<()> {
    let block = aws_reader
        .get_block_by_number(current_block)
        .await
        .wrap_err("Failed to get blocks from archiver")?;

    // Ethereum blocks only need transaction itself without sender, so strip sender from transactions
    let ethereum_block = AlloyBlock {
        header: block.header,
        body: alloy_consensus::BlockBody {
            transactions: block
                .body
                .transactions
                .into_iter()
                .map(|tx| tx.tx)
                .collect(),
            ommers: block.body.ommers,
            withdrawals: block.body.withdrawals,
        },
    };

    let mut block_rlp = Vec::new();
    ethereum_block.encode(&mut block_rlp);

    let mut compressed_writer =
        monad_compress::util::BoundedWriter::new((block_rlp.len().saturating_mul(2)) as u32);
    BrotliCompression::default()
        .compress(&block_rlp, &mut compressed_writer)
        .map_err(|e| eyre::eyre!("Brotli compression failed: {}", e))?;
    let compressed_block: bytes::Bytes = compressed_writer.into();

    let output_path = dest_path.join(current_block.to_string());
    fs::write(&output_path, &compressed_block)
        .await
        .wrap_err("Failed to write to file")?;

    info!("Wrote block {} to {:?}", current_block, output_path);
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    info!(?args, "Cli Arguments: ");

    let concurrent_block_semaphore = Arc::new(Semaphore::new(args.concurrency));

    let url = "https://abc.com"; // dummy proxy url
    let api_key = "";
    let aws_reader =
        ArchiveReader::init_aws_reader(args.bucket.clone(), args.region.clone(), url, api_key, 1)
            .await?;

    let aws_reader = Arc::new(aws_reader);
    let dest_path = args.dest_path.clone();
    let max_retries = args.max_retries;

    let mut failed_blocks: Vec<u64> = Vec::new();

    for attempt in 0..=max_retries {
        let blocks_to_process = if attempt == 0 {
            // First attempt: process all blocks
            (args.start_block..=args.stop_block).collect::<Vec<_>>()
        } else {
            // Retry: only process failed blocks
            if failed_blocks.is_empty() {
                break;
            }
            let to_retry = failed_blocks.clone();
            failed_blocks.clear();
            to_retry
        };

        if blocks_to_process.is_empty() {
            break;
        }

        if attempt > 0 {
            info!(
                "Retry attempt {} for {} failed blocks",
                attempt,
                blocks_to_process.len()
            );
        }

        let join_handles: Vec<_> = blocks_to_process
            .into_iter()
            .map(|current_block| {
                let aws_reader = Arc::clone(&aws_reader);
                let dest_path = dest_path.clone();
                let semaphore = concurrent_block_semaphore.clone();

                tokio::spawn(async move {
                    let _permit = semaphore
                        .acquire()
                        .await
                        .expect("Got permit to execute a new block");

                    let result = process_block(&aws_reader, current_block, &dest_path).await;
                    (current_block, result)
                })
            })
            .collect();

        let results = join_all(join_handles).await;

        // Collect failed blocks for retry
        for result in results {
            match result {
                Ok((block_num, Ok(()))) => {
                    // Success - no retry needed
                }
                Ok((block_num, Err(e))) => {
                    error!("Failed to process block {}: {:?}", block_num, e);
                    if attempt < max_retries {
                        failed_blocks.push(block_num);
                    } else {
                        error!(
                            "Block {} failed after {} retries, giving up",
                            block_num, max_retries
                        );
                    }
                }
                Err(e) => {
                    error!("Join error: {:?}", e);
                }
            }
        }
    }

    if !failed_blocks.is_empty() {
        return Err(eyre::eyre!(
            "Failed to process {} blocks after {} retries: {:?}",
            failed_blocks.len(),
            max_retries,
            failed_blocks
        ));
    }

    Ok(())
}
