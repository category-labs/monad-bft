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

use alloy_consensus::Block as AlloyBlock;
use alloy_rlp::Encodable;
use clap::Parser;
use eyre::Result;
use monad_archive::{kvstore::WritePolicy, prelude::*};
use monad_compress::{brotli::BrotliCompression, CompressionAlgo};
use tracing::Level;

mod cli;

struct BlockWriterWorker {
    reader: BlockDataReaderErased,
    fs: FsStorage,
    flat_dir: bool,
}

impl BlockRangeWorker for BlockWriterWorker {
    async fn get_checkpoint(&self) -> Result<u64> {
        get_latest_block(&self.fs).await
    }

    async fn get_source_head(&self) -> Result<u64> {
        self.reader
            .get_latest(LatestKind::Uploaded)
            .await?
            .ok_or_else(|| eyre!("No latest block available from source"))
    }

    async fn process_block(&self, block_num: u64) -> Result<()> {
        let mut fs = self.fs.clone();
        if !self.flat_dir {
            fs = fs
                .with_prefix(format!("{}M/", block_num / 1_000_000))
                .await?;
        }
        write_block(&self.reader, block_num, &fs).await
    }

    async fn checkpoint(&self, block_num: u64) {
        if let Err(e) = set_latest_block(&self.fs, block_num).await {
            error!("Failed to set latest block: {e:?}");
        }
    }
}

async fn write_block(
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
            let worker = BlockWriterWorker {
                reader,
                fs,
                flat_dir: shared_args.flat_dir,
            };
            let config = ProcessRangeConfig {
                concurrency: shared_args.concurrency,
                max_retries,
                skip_failures: false,
            };
            let range = write_range_args.start_block..=write_range_args.stop_block;
            tokio::spawn(async move { process_block_range(&worker, range, &config).await }).await?;
        }
        cli::Mode::Stream(ref stream_args) => {
            let worker = BlockWriterWorker {
                reader,
                fs,
                flat_dir: shared_args.flat_dir,
            };

            let config = WorkerLoopConfig {
                max_blocks_per_iteration: stream_args.max_blocks_per_iter,
                stop_block: None,
                poll_interval: Duration::from_secs_f64(stream_args.sleep_secs),
                process: ProcessRangeConfig {
                    concurrency: shared_args.concurrency,
                    max_retries,
                    skip_failures: false,
                },
            };

            run_worker_loop(worker, config).await;
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

        let worker = BlockWriterWorker {
            reader,
            fs: fs.clone(),
            flat_dir: true,
        };
        let config = ProcessRangeConfig {
            concurrency: 2,
            max_retries: 3,
            skip_failures: false,
        };
        let last_block = process_block_range(&worker, 0..=4, &config).await;

        assert_eq!(last_block, Some(4));

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

        let worker = BlockWriterWorker {
            reader,
            fs,
            flat_dir: false,
        };
        let config = ProcessRangeConfig {
            concurrency: 2,
            max_retries: 3,
            skip_failures: false,
        };
        let last_block = process_block_range(&worker, 999_999..=1_000_001, &config).await;

        assert_eq!(last_block, Some(1_000_001));

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

        let worker = BlockWriterWorker {
            reader,
            fs,
            flat_dir: true,
        };
        let config = ProcessRangeConfig {
            concurrency: 4,
            max_retries: 0,
            skip_failures: false,
        };
        let last_block = process_block_range(&worker, 10..=14, &config).await;

        assert_eq!(last_block, Some(14));
    }

    #[tokio::test]
    async fn test_run_succeeds_with_valid_range() {
        // Create blocks 0-4 and process all of them
        let blocks: Vec<Block> = (0..5).map(|i| mock_block(i, vec![mock_tx(i)])).collect();

        let reader = setup_source_archive(&blocks).await;

        let temp_dir = tempfile::tempdir().unwrap();
        let fs = FsStorage::new(temp_dir.path(), Metrics::none()).unwrap();

        let worker = BlockWriterWorker {
            reader,
            fs,
            flat_dir: true,
        };
        let config = ProcessRangeConfig {
            concurrency: 2,
            max_retries: 0,
            skip_failures: false,
        };
        let last_block = process_block_range(&worker, 0..=4, &config).await;

        assert_eq!(last_block, Some(4));
    }
}
