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

use std::vec::IntoIter;

use alloy_consensus::Block as AlloyBlock;
use alloy_rlp::Encodable;
use clap::Parser;
use eyre::Result;
use monad_archive::{kvstore::WritePolicy, prelude::*};
use monad_compress::{brotli::BrotliCompression, CompressionAlgo};
use tracing::Level;

mod cli;

async fn process_block(
    reader: &BlockDataReaderErased,
    current_block: u64,
    fs: &FsStorage,
) -> Result<()> {
    let block = reader
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

    let compressed_block: Vec<u8> = {
        let mut block_rlp = Vec::new();
        ethereum_block.encode(&mut block_rlp);

        let mut compressed_writer =
            monad_compress::util::BoundedWriter::new((block_rlp.len().saturating_mul(2)) as u32);
        BrotliCompression::default()
            .compress(&block_rlp, &mut compressed_writer)
            .map_err(|e| eyre::eyre!("Brotli compression failed: {}", e))?;

        compressed_writer.into()
    };

    let key = current_block.to_string();
    fs.put(&key, compressed_block, WritePolicy::AllowOverwrite)
        .await
        .wrap_err_with(|| format!("Failed to write block {current_block} to file"))?;

    info!(
        "Wrote block {} to {}",
        current_block,
        fs.key_path(&key)?.to_string_lossy()
    );
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    let args = cli::Cli::parse();
    info!(?args, "Cli Arguments: ");

    // Handle SetStartBlock separately since it doesn't need shared args
    if let cli::Mode::SetStartBlock { block, dest_path } = args.mode {
        let fs = FsStorage::new(dest_path, Metrics::none())?;
        set_latest_block(&fs, block).await?;
        println!("Set latest marker: key=\"latest\", block={block}");
        return Ok(());
    }

    let shared_args = args.mode.shared();

    let dest_path = shared_args.dest_path.clone();
    let max_retries = shared_args.max_retries;

    let reader = shared_args
        .block_data_source
        .build(&Metrics::none())
        .await?;
    let fs = FsStorage::new(dest_path.clone(), Metrics::none())?;

    match args.mode {
        cli::Mode::SetStartBlock { .. } => unreachable!(),
        cli::Mode::WriteRange(ref write_range_args) => {
            tokio::spawn(write_range(
                shared_args.concurrency,
                reader,
                fs,
                max_retries,
                write_range_args.start_block,
                write_range_args.stop_block,
                shared_args.flat_dir,
            ))
            .await?;
        }
        cli::Mode::Stream(ref stream_args) => {
            loop {
                let Ok(mut start) = get_latest_block(&fs)
                    .await
                    .inspect_err(|e| error!("Failed to get latest block: {e:?}"))
                else {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                };
                if start != 0 {
                    start += 1; // We already processed this block, so start from the next one
                }

                let Ok(Some(mut stop)) = reader
                    .get_latest(LatestKind::Uploaded)
                    .await
                    .inspect_err(|e| error!("Failed to get latest block: {e:?}"))
                else {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                };

                if start >= stop {
                    info!(
                        "No new blocks to process, sleeping for {} seconds",
                        stream_args.sleep_secs
                    );
                    tokio::time::sleep(Duration::from_secs_f64(stream_args.sleep_secs)).await;
                    continue;
                }
                stop = stop.min(start + stream_args.max_blocks_per_iter);
                let last_block = match tokio::spawn(write_range(
                    shared_args.concurrency,
                    reader.clone(),
                    fs.clone(),
                    max_retries,
                    start,
                    stop,
                    shared_args.flat_dir,
                ))
                .await
                {
                    Ok(last_block) => last_block,
                    Err(e) => {
                        error!("Task panicked: {e:?}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                };

                if let Err(e) = set_latest_block(&fs, last_block).await {
                    error!("Failed to set latest block: {e:?}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            }
        }
    }

    Ok(())
}

async fn get_latest_block(fs: &FsStorage) -> Result<u64> {
    let latest = fs.get("latest").await?;
    match latest {
        Some(bytes) => {
            let latest = String::from_utf8(bytes.to_vec())?;
            latest
                .parse::<u64>()
                .wrap_err("Failed to parse latest block")
        }
        None => {
            fs.put("latest", b"0".to_vec(), WritePolicy::AllowOverwrite)
                .await?;
            Ok(0)
        }
    }
}

async fn set_latest_block(fs: &FsStorage, block: u64) -> Result<()> {
    fs.put(
        "latest",
        block.to_string().as_bytes().to_vec(),
        WritePolicy::AllowOverwrite,
    )
    .await
    .wrap_err("Failed to set latest block")?;
    Ok(())
}

async fn write_range(
    concurrency: usize,
    reader: BlockDataReaderErased,
    fs: FsStorage,
    max_retries: u32,
    start_block: u64,
    stop_block: u64,
    flat_dir: bool,
) -> u64 {
    let mut failed_blocks: Vec<u64> = Vec::new();

    for attempt in 0..=max_retries {
        let blocks_to_process: TwoIters<RangeInclusive<u64>, IntoIter<u64>> = if attempt == 0 {
            // First attempt: process all blocks
            TwoIters::A(start_block..=stop_block)
        } else {
            // Retry: only process failed blocks
            info!(
                "Retry attempt {} for {} failed blocks",
                attempt,
                failed_blocks.len()
            );
            let iter = failed_blocks.into_iter();
            failed_blocks = Vec::new();
            TwoIters::B(iter)
        };

        futures::stream::iter(blocks_to_process)
            .map(|current_block| {
                let reader = reader.clone();
                let mut fs = fs.clone();

                async move {
                    tokio::spawn(async move {
                        if !flat_dir {
                            fs = fs
                                .with_prefix(format!("{}M/", current_block / 1_000_000))
                                .await
                                .wrap_err("Failed to create prefix for block")
                                .map_err(|e| (current_block, eyre::Report::from(e)))?;
                        }

                        process_block(&reader, current_block, &fs)
                            .await
                            .map_err(|e| (current_block, e))
                    })
                    .await
                    .map_err(|e| (current_block, e))
                }
            })
            // Prefer buffered over unordered to lay down blocks in order to allow exeecution
            // to begin processing before all blocks are laid down.
            .buffered(concurrency)
            // Collect failed blocks for retry
            .for_each(|result| {
                match result {
                    Ok(Ok(())) => {
                        // Success - no retry needed
                    }
                    Ok(Err((block_num, e))) => {
                        error!("Failed to process block {}: {:?}", block_num, e);
                        failed_blocks.push(block_num);
                    }
                    Err((block_num, e)) => {
                        error!("Task panicked for block {}: {:?}", block_num, e);
                        failed_blocks.push(block_num);
                    }
                }
                futures::future::ready(())
            })
            .await;

        if failed_blocks.is_empty() {
            break;
        }
    }

    if !failed_blocks.is_empty() {
        let min_failed = *failed_blocks.iter().min().unwrap();
        error!(
            "Failed to process {} blocks after {} retries, earliest failure: {}",
            failed_blocks.len(),
            max_retries,
            min_failed
        );
        // Return the block before the first failure so we don't skip any blocks
        return min_failed.saturating_sub(1);
    }

    stop_block
}

enum TwoIters<A, B> {
    A(A),
    B(B),
}

impl<T, A: Iterator<Item = T>, B: Iterator<Item = T>> Iterator for TwoIters<A, B> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::A(a) => a.next(),
            Self::B(b) => b.next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::Block as AlloyBlock;
    use alloy_rlp::Decodable;
    use monad_archive::{
        kvstore::WritePolicy,
        test_utils::{mock_block, mock_rx, mock_tx, MemoryStorage},
    };
    use monad_compress::util::BoundedWriter;

    use super::*;

    async fn setup_source_archive(blocks: &[Block]) -> BlockDataReaderErased {
        let store = MemoryStorage::new("test-source");
        let archive = BlockDataArchive::new(store);

        for block in blocks {
            archive
                .archive_block(block.clone(), WritePolicy::NoClobber)
                .await
                .unwrap();

            let receipts: Vec<ReceiptWithLogIndex> = block
                .body
                .transactions
                .iter()
                .enumerate()
                .map(|(i, _)| mock_rx(10, (i + 1) as u128 * 21000))
                .collect();
            archive
                .archive_receipts(receipts, block.header.number, WritePolicy::NoClobber)
                .await
                .unwrap();

            let traces: Vec<Vec<u8>> = block
                .body
                .transactions
                .iter()
                .map(|_| vec![1, 2, 3])
                .collect();
            archive
                .archive_traces(traces, block.header.number, WritePolicy::NoClobber)
                .await
                .unwrap();
        }

        if let Some(last) = blocks.last() {
            archive
                .update_latest(last.header.number, LatestKind::Uploaded)
                .await
                .unwrap();
        }

        BlockDataReaderErased::BlockDataArchive(archive)
    }

    #[tokio::test]
    async fn test_write_range_flat_dir() {
        let blocks: Vec<Block> = (0..5).map(|i| mock_block(i, vec![mock_tx(i)])).collect();

        let reader = setup_source_archive(&blocks).await;

        let temp_dir = tempfile::tempdir().unwrap();
        let fs = FsStorage::new(temp_dir.path(), Metrics::none()).unwrap();

        let last_block = write_range(2, reader, fs.clone(), 3, 0, 4, true).await;

        assert_eq!(last_block, 4);

        // Verify all blocks were written
        for i in 0..5 {
            let key = i.to_string();
            let compressed_data = fs.get(&key).await.unwrap().expect("Block should exist");

            // Decompress and verify we can decode the block
            let mut decompressed = BoundedWriter::new(1024 * 1024);
            BrotliCompression::default()
                .decompress(&compressed_data, &mut decompressed)
                .unwrap();
            let decompressed: Vec<u8> = decompressed.into();

            let decoded = AlloyBlock::<alloy_consensus::TxEnvelope, Header>::decode(
                &mut decompressed.as_slice(),
            )
            .unwrap();
            assert_eq!(decoded.header.number, i);
        }
    }

    #[tokio::test]
    async fn test_write_range_with_prefix_dirs() {
        // Create blocks that span two prefix directories (0/ and 1/)
        let blocks: Vec<Block> = vec![
            mock_block(999_999, vec![mock_tx(999_999)]),
            mock_block(1_000_000, vec![mock_tx(1_000_000)]),
            mock_block(1_000_001, vec![mock_tx(1_000_001)]),
        ];

        let reader = setup_source_archive(&blocks).await;

        let temp_dir = tempfile::tempdir().unwrap();
        let fs = FsStorage::new(temp_dir.path(), Metrics::none()).unwrap();

        let last_block = write_range(2, reader, fs, 3, 999_999, 1_000_001, false).await;

        assert_eq!(last_block, 1_000_001);

        // Verify blocks are in correct prefix directories
        let path_0 = temp_dir.path().join("0M/999999");
        let path_1a = temp_dir.path().join("1M/1000000");
        let path_1b = temp_dir.path().join("1M/1000001");

        assert!(path_0.exists(), "Block 999999 should be in 0M/ directory");
        assert!(path_1a.exists(), "Block 1000000 should be in 1M/ directory");
        assert!(path_1b.exists(), "Block 1000001 should be in 1M/ directory");
    }

    #[tokio::test]
    async fn test_run_returns_stop_block_on_success() {
        let blocks: Vec<Block> = (10..15).map(|i| mock_block(i, vec![mock_tx(i)])).collect();

        let reader = setup_source_archive(&blocks).await;

        let temp_dir = tempfile::tempdir().unwrap();
        let fs = FsStorage::new(temp_dir.path(), Metrics::none()).unwrap();

        let last_block = write_range(4, reader, fs, 0, 10, 14, true).await;

        assert_eq!(last_block, 14);
    }

    #[tokio::test]
    async fn test_run_succeeds_with_valid_range() {
        // Create blocks 0-4 and process all of them
        let blocks: Vec<Block> = (0..5).map(|i| mock_block(i, vec![mock_tx(i)])).collect();

        let reader = setup_source_archive(&blocks).await;

        let temp_dir = tempfile::tempdir().unwrap();
        let fs = FsStorage::new(temp_dir.path(), Metrics::none()).unwrap();

        let last_block = write_range(2, reader, fs, 0, 0, 4, true).await;

        assert_eq!(last_block, 4);
    }
}
