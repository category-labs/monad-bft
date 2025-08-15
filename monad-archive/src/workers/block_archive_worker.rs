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

use std::{
    ops::RangeInclusive,
    time::{Duration, Instant},
};

use eyre::Result;
use futures::{join, StreamExt, TryStreamExt};
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::prelude::*;

/// Main worker that archives block data from the execution database to durable storage.
/// Continuously polls for new blocks and archives their data.
///
/// # Arguments
/// * `block_data_source` - Source to read block data from (typically triedb)
/// * `archive_writer` - Archive to write block data to (typically S3)
/// * `max_blocks_per_iteration` - Maximum number of blocks to process in one iteration
/// * `max_concurrent_blocks` - Maximum number of blocks to process concurrently
/// * `start_block_override` - Optional block number to start archiving from
/// * `stop_block_override` - Optional block number to stop archiving at
/// * `metrics` - Metrics collection interface
pub async fn archive_worker(
    block_data_source: (impl BlockDataReader + Sync),
    fallback_source: Option<(impl BlockDataReader + Sync)>,
    archive_writer: BlockDataArchive,
    max_blocks_per_iteration: u64,
    max_concurrent_blocks: usize,
    mut start_block_override: Option<u64>,
    stop_block_override: Option<u64>,
    unsafe_skip_bad_blocks: bool,
    metrics: Metrics,
) {
    // initialize starting block using either override or stored latest
    let mut start_block = match start_block_override.take() {
        Some(start_block) => start_block,
        None => {
            let latest_uploaded = archive_writer
                .get_latest(LatestKind::Uploaded)
                .await
                .unwrap_or(Some(0))
                .unwrap_or(0);
            if latest_uploaded == 0 {
                0
            } else {
                latest_uploaded + 1
            }
        }
    };

    loop {
        // query latest
        let latest_source = match block_data_source.get_latest(LatestKind::Uploaded).await {
            Ok(number) => number.unwrap_or(0),
            Err(e) => {
                warn!("Error getting latest source block: {e:?}");
                continue;
            }
        };

        if let Some(stop_block_override) = stop_block_override {
            if start_block > stop_block_override {
                info!("Reached stop block override, stopping...");
                return;
            }
        }

        let end_block = latest_source.min(start_block + max_blocks_per_iteration - 1);
        if end_block < start_block {
            info!(start_block, end_block, "Nothing to process");
            sleep(Duration::from_millis(500)).await;
            continue;
        }

        metrics.gauge(MetricNames::SOURCE_LATEST_BLOCK_NUM, latest_source);
        metrics.gauge(MetricNames::END_BLOCK_NUMBER, end_block);
        metrics.gauge(MetricNames::START_BLOCK_NUMBER, start_block);

        info!(
            start = start_block,
            end = end_block,
            latest_source,
            "Archiving group of blocks",
        );

        let latest_uploaded = archive_blocks(
            &block_data_source,
            &fallback_source,
            start_block..=end_block,
            &archive_writer,
            &metrics,
            max_concurrent_blocks,
            unsafe_skip_bad_blocks,
        )
        .await;

        start_block = if latest_uploaded == 0 {
            0
        } else {
            latest_uploaded + 1
        };
    }
}

async fn archive_blocks(
    reader: &(impl BlockDataReader + Sync),
    fallback_reader: &Option<impl BlockDataReader + Sync>,
    range: RangeInclusive<u64>,
    archiver: &BlockDataArchive,
    metrics: &Metrics,
    concurrency: usize,
    unsafe_skip_bad_blocks: bool,
) -> u64 {
    let start = Instant::now();

    let res: Result<(), u64> = futures::stream::iter(range.clone())
        .map(|block_num: u64| async move {
            match archive_block(reader, fallback_reader, block_num, archiver, metrics).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    if unsafe_skip_bad_blocks {
                        error!("Failed to handle block {block_num}, skipping... Cause: {e:?}",);
                        Ok(())
                    } else {
                        error!("Failed to handle block {block_num}: {e:?}");
                        Err(block_num)
                    }
                }
            }
        })
        .buffered(concurrency)
        .try_collect()
        .await;

    info!(
        elapsed = start.elapsed().as_millis(),
        start = range.start(),
        end = range.end(),
        "Finished archiving range",
    );

    let new_latest_uploaded = match res {
        Ok(()) => *range.end(),
        Err(err_block) => err_block.saturating_sub(1),
    };

    if new_latest_uploaded != 0 {
        checkpoint_latest(archiver, new_latest_uploaded).await;
    }

    new_latest_uploaded
}

async fn archive_block(
    reader: &impl BlockDataReader,
    fallback: &Option<impl BlockDataReader>,
    block_num: u64,
    archiver: &BlockDataArchive,
    metrics: &Metrics,
) -> Result<()> {
    let mut num_txs = None;

    let (block, receipts, traces) = join!(
        async {
            let block = match reader.get_block_by_number(block_num).await {
                Ok(b) => b,
                Err(e) => {
                    let Some(fallback) = fallback.as_ref() else {
                        return Err(e);
                    };
                    warn!(
                        ?e,
                        block_num, "Failed to read block from primary source, trying fallback..."
                    );
                    metrics.inc_counter(MetricNames::BLOCK_ARCHIVE_WORKER_BLOCK_FALLBACK);
                    fallback.get_block_by_number(block_num).await?
                }
            };
            num_txs = Some(block.body.transactions.len());
            archiver.archive_block(block).await
        },
        async {
            let receipts = match reader.get_block_receipts(block_num).await {
                Ok(b) => b,
                Err(e) => {
                    let Some(fallback) = fallback.as_ref() else {
                        return Err(e);
                    };
                    warn!(
                        ?e,
                        block_num,
                        "Failed to read block receipts from primary source, trying fallback..."
                    );
                    metrics.inc_counter(MetricNames::BLOCK_ARCHIVE_WORKER_RECEIPTS_FALLBACK);
                    fallback.get_block_receipts(block_num).await?
                }
            };
            archiver.archive_receipts(receipts, block_num).await
        },
        async {
            let traces = match reader.get_block_traces(block_num).await {
                Ok(b) => b,
                Err(e) => {
                    let Some(fallback) = fallback.as_ref() else {
                        return Err(e);
                    };
                    warn!(
                        ?e,
                        block_num,
                        "Failed to read block traces from primary source, trying fallback..."
                    );
                    metrics.inc_counter(MetricNames::BLOCK_ARCHIVE_WORKER_TRACES_FALLBACK);
                    fallback.get_block_traces(block_num).await?
                }
            };
            archiver.archive_traces(traces, block_num).await
        },
    );

    // Failing to archive a block or its receipts is a critical error, so we return an error.
    block?;
    receipts?;

    // Failing to archive traces is not a critical error, so we log and continue.
    if let Err(e) = traces {
        metrics.inc_counter(MetricNames::BLOCK_ARCHIVE_WORKER_TRACES_FAILED);
        error!(
            block_num,
            "Failed to archive traces for block {block_num}. Continuing... Cause: {e:?}"
        );
    }

    info!(block_num, num_txs, "Successfully archived block");
    Ok(())
}

async fn checkpoint_latest(archiver: &BlockDataArchive, block_num: u64) {
    match archiver
        .update_latest(block_num, LatestKind::Uploaded)
        .await
    {
        Ok(()) => info!(block_num, "Set latest uploaded checkpoint"),
        Err(e) => error!(block_num, "Failed to set latest uploaded block: {e:?}"),
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{
        Receipt, ReceiptEnvelope, ReceiptWithBloom, SignableTransaction, TxEip1559,
    };
    use alloy_primitives::{Bloom, Log, B256, U256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};

    use super::*;
    use crate::{kvstore::memory::MemoryStorage, metrics, test_utils::mock_block};

    fn mock_tx() -> TxEnvelopeWithSender {
        let tx = TxEip1559 {
            nonce: 123,
            gas_limit: 456,
            max_fee_per_gas: 789,
            max_priority_fee_per_gas: 135,
            ..Default::default()
        };
        let signer = PrivateKeySigner::from_bytes(&B256::from(U256::from(123))).unwrap();
        let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
        let tx = tx.into_signed(sig);
        TxEnvelopeWithSender {
            tx: tx.into(),
            sender: signer.address(),
        }
    }

    fn mock_rx() -> ReceiptWithLogIndex {
        let receipt = ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
            Receipt::<Log> {
                logs: vec![],
                status: alloy_consensus::Eip658Value::Eip658(true),
                cumulative_gas_used: 55,
            },
            Bloom::repeat_byte(b'a'),
        ));
        ReceiptWithLogIndex {
            receipt,
            starting_log_index: 0,
        }
    }

    async fn mock_source(
        archive: &BlockDataArchive,
        data: impl IntoIterator<Item = (Block, BlockReceipts, BlockTraces)>,
    ) {
        let mut max_block_num = u64::MIN;
        for (block, receipts, traces) in data {
            let block_num = block.header.number;

            if block_num > max_block_num {
                max_block_num = block_num;
            }

            archive.archive_block(block.clone()).await.unwrap();
            archive
                .archive_receipts(receipts.clone(), block_num)
                .await
                .unwrap();
            archive
                .archive_traces(traces.clone(), block_num)
                .await
                .unwrap();
        }

        archive
            .update_latest(max_block_num, LatestKind::Uploaded)
            .await
            .unwrap();
    }

    fn memory_sink_source() -> (BlockDataArchive, BlockDataArchive) {
        let source: KVStoreErased = MemoryStorage::new("source").into();
        let reader = BlockDataArchive::new(source);

        let sink: KVStoreErased = MemoryStorage::new("sink").into();
        let archiver = BlockDataArchive::new(sink);

        (reader, archiver)
    }

    #[tokio::test]
    async fn archive_block_memory_fallback() {
        let (reader, _) = memory_sink_source();
        let (fallback_reader, archiver) = memory_sink_source();

        let block_num = 10;
        let block = mock_block(block_num, vec![mock_tx()]);
        let receipts = vec![mock_rx()];
        let traces = vec![vec![], vec![2]];

        mock_source(
            &fallback_reader,
            [(block.clone(), receipts.clone(), traces.clone())],
        )
        .await;

        let res = archive_block(
            &reader,
            &Some(fallback_reader),
            block_num,
            &archiver,
            &metrics::Metrics::none(),
        )
        .await;
        assert!(res.is_ok());
        assert_eq!(
            archiver.get_block_by_number(block_num).await.unwrap(),
            block
        );
        assert_eq!(archiver.get_block_traces(block_num).await.unwrap(), traces);
        assert_eq!(
            archiver.get_block_receipts(block_num).await.unwrap(),
            receipts
        );
    }

    #[tokio::test]
    async fn archive_block_memory() {
        let (reader, archiver) = memory_sink_source();

        let block_num = 10;
        let block = mock_block(block_num, vec![mock_tx()]);
        let receipts = vec![mock_rx()];
        let traces = vec![vec![], vec![2]];

        mock_source(&reader, [(block.clone(), receipts.clone(), traces.clone())]).await;

        let res = archive_block(
            &reader,
            &None::<BlockDataReaderErased>,
            block_num,
            &archiver,
            &metrics::Metrics::none(),
        )
        .await;
        assert!(res.is_ok());
        assert_eq!(
            archiver.get_block_by_number(block_num).await.unwrap(),
            block
        );
        assert_eq!(archiver.get_block_traces(block_num).await.unwrap(), traces);
        assert_eq!(
            archiver.get_block_receipts(block_num).await.unwrap(),
            receipts
        );
    }

    #[tokio::test]
    async fn archive_blocks_memory() {
        let (reader, archiver) = memory_sink_source();

        let row = |b| {
            (
                mock_block(b, vec![mock_tx()]),
                vec![mock_rx()],
                vec![vec![], vec![2]],
            )
        };
        mock_source(&reader, (0..=10).map(row)).await;

        assert_eq!(
            reader.get_latest(LatestKind::Uploaded).await.unwrap(),
            Some(10)
        );

        let end_block = archive_blocks(
            &reader,
            &None::<BlockDataReaderErased>,
            0..=10,
            &archiver,
            &metrics::Metrics::none(),
            3,
            false,
        )
        .await;

        assert_eq!(end_block, 10);
        assert_eq!(
            archiver.get_latest(LatestKind::Uploaded).await.unwrap(),
            Some(10)
        );
    }

    #[tokio::test]
    async fn archive_blocks_with_gap() {
        let (reader, archiver) = memory_sink_source();

        let row = |b| {
            (
                mock_block(b, vec![mock_tx()]),
                vec![mock_rx()],
                vec![vec![], vec![2]],
            )
        };
        let latest_source = 15;
        let end_of_first_chunk = 10;
        mock_source(
            &reader,
            (0..=end_of_first_chunk)
                .map(row)
                .chain((12..=latest_source).map(row)),
        )
        .await;

        assert_eq!(
            reader.get_latest(LatestKind::Uploaded).await.unwrap(),
            Some(latest_source)
        );

        let end_block = archive_blocks(
            &reader,
            &None::<BlockDataReaderErased>,
            0..=latest_source,
            &archiver,
            &metrics::Metrics::none(),
            3,
            false,
        )
        .await;

        assert_eq!(end_block, end_of_first_chunk);
        assert_eq!(
            archiver.get_latest(LatestKind::Uploaded).await.unwrap(),
            Some(end_of_first_chunk)
        );
    }
}
