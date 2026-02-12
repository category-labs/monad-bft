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
use monad_archive::{kvstore::WritePolicy, prelude::*};
use tracing::{error, info, warn};

pub struct ArchiveWorkerOpts {
    /// Maximum number of blocks to process in one iteration
    pub max_blocks_per_iteration: u64,
    /// Maximum number of blocks to process concurrently
    pub max_concurrent_blocks: usize,
    /// Optional block number to stop archiving at
    pub stop_block: Option<u64>,
    /// If set, archiver will skip blocks that fail to archive
    pub unsafe_skip_bad_blocks: bool,
    /// If set, archiver will require traces to be present for all blocks
    pub require_traces: bool,
    /// If set, archiver will only archive traces
    pub traces_only: bool,
    /// If set, archiver will perform an asynchronous backfill of the archive
    pub async_backfill: bool,
    /// WritePolicy for blocks archiving
    pub blocks_write_policy: WritePolicy,
    /// WritePolicy for receipts archiving
    pub receipts_write_policy: WritePolicy,
    /// WritePolicy for traces archiving
    pub traces_write_policy: WritePolicy,
}

struct ArchiveBlockRangeWorker<S, F> {
    block_data_source: S,
    fallback_source: Option<F>,
    archive_writer: BlockDataArchive,
    metrics: Metrics,
    max_concurrent_blocks: usize,
    unsafe_skip_bad_blocks: bool,
    require_traces: bool,
    traces_only: bool,
    latest_kind: LatestKind,
    blocks_write_policy: WritePolicy,
    receipts_write_policy: WritePolicy,
    traces_write_policy: WritePolicy,
}

impl<S: BlockDataReader + Sync, F: BlockDataReader + Sync> BlockRangeWorker
    for ArchiveBlockRangeWorker<S, F>
{
    async fn get_checkpoint(&self) -> Result<u64> {
        Ok(self
            .archive_writer
            .get_latest(self.latest_kind)
            .await?
            .unwrap_or(0))
    }

    async fn get_source_head(&self) -> Result<u64> {
        Ok(self
            .block_data_source
            .get_latest(LatestKind::Uploaded)
            .await?
            .unwrap_or(0))
    }

    async fn process_range(&self, range: RangeInclusive<u64>) -> u64 {
        archive_blocks(
            &self.block_data_source,
            &self.fallback_source,
            range,
            &self.archive_writer,
            &self.metrics,
            self.max_concurrent_blocks,
            self.unsafe_skip_bad_blocks,
            self.require_traces,
            self.traces_only,
            self.latest_kind,
            self.blocks_write_policy,
            self.receipts_write_policy,
            self.traces_write_policy,
        )
        .await
    }

    fn report_metrics(&self, start: u64, end: u64, source_head: u64) {
        self.metrics
            .gauge(MetricNames::SOURCE_LATEST_BLOCK_NUM, source_head);
        self.metrics.gauge(MetricNames::END_BLOCK_NUMBER, end);
        self.metrics.gauge(MetricNames::START_BLOCK_NUMBER, start);
    }
}

/// Main worker that archives block data from the execution database to durable storage.
/// Continuously polls for new blocks and archives their data.
pub async fn archive_worker(
    block_data_source: impl BlockDataReader + Sync,
    fallback_source: Option<impl BlockDataReader + Sync>,
    archive_writer: BlockDataArchive,
    opts: ArchiveWorkerOpts,
    metrics: Metrics,
) {
    let latest_kind = if opts.async_backfill {
        LatestKind::UploadedAsyncBackfill
    } else {
        LatestKind::Uploaded
    };

    let worker = ArchiveBlockRangeWorker {
        block_data_source,
        fallback_source,
        archive_writer,
        metrics,
        max_concurrent_blocks: opts.max_concurrent_blocks,
        unsafe_skip_bad_blocks: opts.unsafe_skip_bad_blocks,
        require_traces: opts.require_traces,
        traces_only: opts.traces_only,
        latest_kind,
        blocks_write_policy: opts.blocks_write_policy,
        receipts_write_policy: opts.receipts_write_policy,
        traces_write_policy: opts.traces_write_policy,
    };

    let config = WorkerLoopConfig {
        max_blocks_per_iteration: opts.max_blocks_per_iteration,
        stop_block: opts.stop_block,
        poll_interval: Duration::from_millis(500),
    };

    run_worker_loop(worker, config).await;
}

async fn archive_blocks(
    reader: &(impl BlockDataReader + Sync),
    fallback_reader: &Option<impl BlockDataReader + Sync>,
    range: RangeInclusive<u64>,
    archiver: &BlockDataArchive,
    metrics: &Metrics,
    concurrency: usize,
    unsafe_skip_bad_blocks: bool,
    require_traces: bool,
    traces_only: bool,
    latest_kind: LatestKind,
    blocks_write_policy: WritePolicy,
    receipts_write_policy: WritePolicy,
    traces_write_policy: WritePolicy,
) -> u64 {
    let start = Instant::now();

    let res: Result<(), u64> = futures::stream::iter(range.clone())
        .map(|block_num: u64| async move {
            match archive_block(
                reader,
                fallback_reader,
                block_num,
                archiver,
                require_traces,
                traces_only,
                metrics,
                blocks_write_policy,
                receipts_write_policy,
                traces_write_policy,
            )
            .await
            {
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
        checkpoint_latest(archiver, new_latest_uploaded, latest_kind).await;
    }

    new_latest_uploaded
}

async fn archive_block(
    reader: &impl BlockDataReader,
    fallback: &Option<impl BlockDataReader>,
    block_num: u64,
    archiver: &BlockDataArchive,
    require_traces: bool,
    traces_only: bool,
    metrics: &Metrics,
    blocks_write_policy: WritePolicy,
    receipts_write_policy: WritePolicy,
    traces_write_policy: WritePolicy,
) -> Result<()> {
    let mut num_txs = None;

    let (block, receipts, traces) = join!(
        async {
            if traces_only {
                return Ok(());
            }
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
            archiver.archive_block(block, blocks_write_policy).await
        },
        async {
            if traces_only {
                return Ok(());
            }
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
            archiver
                .archive_receipts(receipts, block_num, receipts_write_policy)
                .await
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
            archiver
                .archive_traces(traces, block_num, traces_write_policy)
                .await
        },
    );

    // Failing to archive a block or its receipts is a critical error, so we return an error.
    block?;
    receipts?;

    // Failing to archive traces is not a critical error, so we log and continue.
    if let Err(e) = traces {
        metrics.inc_counter(MetricNames::BLOCK_ARCHIVE_WORKER_TRACES_FAILED);
        if require_traces || traces_only {
            return Err(eyre::Report::from(e)
                .wrap_err("Archiver requires traces to be present for all blocks"));
        }
        error!(
            block_num,
            "Failed to archive traces for block {block_num}. Continuing... Cause: {e:?}"
        );
    }

    info!(block_num, num_txs, "Successfully archived block");
    Ok(())
}

async fn checkpoint_latest(archiver: &BlockDataArchive, block_num: u64, latest_kind: LatestKind) {
    match archiver.update_latest(block_num, latest_kind).await {
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
    use monad_archive::{kvstore::memory::MemoryStorage, metrics, test_utils::mock_block};
    use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};

    use super::*;

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

            archive
                .archive_block(block.clone(), WritePolicy::NoClobber)
                .await
                .unwrap();
            archive
                .archive_receipts(receipts.clone(), block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
            archive
                .archive_traces(traces.clone(), block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
        }

        archive
            .update_latest(max_block_num, LatestKind::Uploaded)
            .await
            .unwrap();
    }

    async fn mock_source_without_traces(
        archive: &BlockDataArchive,
        data: impl IntoIterator<Item = (Block, BlockReceipts)>,
    ) {
        let mut max_block_num = u64::MIN;
        for (block, receipts) in data {
            let block_num = block.header.number;

            if block_num > max_block_num {
                max_block_num = block_num;
            }

            archive
                .archive_block(block.clone(), WritePolicy::NoClobber)
                .await
                .unwrap();
            archive
                .archive_receipts(receipts.clone(), block_num, WritePolicy::NoClobber)
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
            false,
            false,
            &metrics::Metrics::none(),
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
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
            false,
            false,
            &metrics::Metrics::none(),
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
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
            false,
            false,
            LatestKind::Uploaded,
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
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
            false,
            false,
            LatestKind::Uploaded,
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
        )
        .await;

        assert_eq!(end_block, end_of_first_chunk);
        assert_eq!(
            archiver.get_latest(LatestKind::Uploaded).await.unwrap(),
            Some(end_of_first_chunk)
        );
    }

    #[tokio::test]
    async fn archive_block_without_traces_allowed() {
        let (reader, archiver) = memory_sink_source();

        let block_num = 42;
        let block = mock_block(block_num, vec![mock_tx()]);
        let receipts = vec![mock_rx()];

        mock_source_without_traces(&reader, [(block.clone(), receipts.clone())]).await;
        assert!(reader.get_block_traces(block_num).await.is_err());

        let res = archive_block(
            &reader,
            &None::<BlockDataReaderErased>,
            block_num,
            &archiver,
            false,
            false,
            &metrics::Metrics::none(),
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
        )
        .await;

        assert!(res.is_ok());
        assert_eq!(
            archiver.get_block_by_number(block_num).await.unwrap(),
            block
        );
        assert_eq!(
            archiver.get_block_receipts(block_num).await.unwrap(),
            receipts
        );
        assert!(archiver.get_block_traces(block_num).await.is_err());
    }

    #[tokio::test]
    async fn archive_block_without_traces_requires_traces() {
        let (reader, archiver) = memory_sink_source();

        let block_num = 43;
        let block = mock_block(block_num, vec![mock_tx()]);
        let receipts = vec![mock_rx()];

        mock_source_without_traces(&reader, [(block.clone(), receipts.clone())]).await;
        assert!(reader.get_block_traces(block_num).await.is_err());

        let res = archive_block(
            &reader,
            &None::<BlockDataReaderErased>,
            block_num,
            &archiver,
            true,
            false,
            &metrics::Metrics::none(),
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
        )
        .await;

        assert!(res.is_err());
        let err = res.unwrap_err();
        assert!(err
            .to_string()
            .contains("Archiver requires traces to be present for all blocks"));
        assert_eq!(
            archiver.get_block_by_number(block_num).await.unwrap(),
            block
        );
        assert_eq!(
            archiver.get_block_receipts(block_num).await.unwrap(),
            receipts
        );
        assert!(archiver.get_block_traces(block_num).await.is_err());
    }

    #[tokio::test]
    async fn archive_block_with_traces_only() {
        let (reader, archiver) = memory_sink_source();
        let block_num = 44;
        let block = mock_block(block_num, vec![mock_tx()]);
        let receipts = vec![mock_rx()];
        let traces = vec![vec![], vec![2]];

        mock_source(&reader, [(block.clone(), receipts.clone(), traces.clone())]).await;
        let res = archive_block(
            &reader,
            &None::<BlockDataReaderErased>,
            block_num,
            &archiver,
            false,
            true,
            &metrics::Metrics::none(),
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
        )
        .await;
        assert!(res.is_ok());
        assert!(archiver.get_block_by_number(block_num).await.is_err());
        assert!(archiver.get_block_receipts(block_num).await.is_err());
        assert!(archiver.get_block_traces(block_num).await.is_ok());
    }

    #[tokio::test]
    async fn archive_blocks_with_traces_only() {
        let (reader, archiver) = memory_sink_source();

        let row = |b| {
            (
                mock_block(b, vec![mock_tx()]),
                vec![mock_rx()],
                vec![vec![], vec![2]],
            )
        };
        mock_source(&reader, (0..=10).map(row)).await;

        let end_block = archive_blocks(
            &reader,
            &None::<BlockDataReaderErased>,
            0..=10,
            &archiver,
            &metrics::Metrics::none(),
            3,
            false,
            false,
            true,
            LatestKind::Uploaded,
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
            WritePolicy::NoClobber,
        )
        .await;

        assert_eq!(end_block, 10);
        assert_eq!(
            archiver.get_latest(LatestKind::Uploaded).await.unwrap(),
            Some(10)
        );
        assert!(archiver.get_block_by_number(5).await.is_err());
        assert!(archiver.get_block_receipts(5).await.is_err());
        assert!(archiver.get_block_traces(5).await.is_ok());
    }

    #[tokio::test]
    async fn archive_blocks_async_backfill_with_start_stop() {
        // This test verifies that async_backfill mode correctly reads from the
        // source's LatestKind::Uploaded marker while writing progress to its own
        // LatestKind::UploadedAsyncBackfill marker. Previously, the worker would
        // query get_latest(LatestKind::UploadedAsyncBackfill) from the source,
        // which wouldn't exist, causing it to find no blocks to process.
        let (reader, archiver) = memory_sink_source();

        let row = |b| {
            (
                mock_block(b, vec![mock_tx()]),
                vec![mock_rx()],
                vec![vec![], vec![2]],
            )
        };
        // Source has blocks 0-20 with LatestKind::Uploaded set to 20
        mock_source(&reader, (0..=20).map(row)).await;

        assert_eq!(
            reader.get_latest(LatestKind::Uploaded).await.unwrap(),
            Some(20)
        );
        // Source does NOT have UploadedAsyncBackfill marker set
        assert_eq!(
            reader
                .get_latest(LatestKind::UploadedAsyncBackfill)
                .await
                .unwrap(),
            None
        );

        // Simulate async_backfill archiving a subset (blocks 5-10)
        let end_block = archive_blocks(
            &reader,
            &None::<BlockDataReaderErased>,
            5..=10,
            &archiver,
            &metrics::Metrics::none(),
            3,
            false,
            false,
            false,
            LatestKind::UploadedAsyncBackfill,
            WritePolicy::AllowOverwrite,
            WritePolicy::AllowOverwrite,
            WritePolicy::AllowOverwrite,
        )
        .await;

        assert_eq!(end_block, 10);
        // Archiver should have UploadedAsyncBackfill marker set
        assert_eq!(
            archiver
                .get_latest(LatestKind::UploadedAsyncBackfill)
                .await
                .unwrap(),
            Some(10)
        );
        // Regular Uploaded marker should NOT be set
        assert_eq!(
            archiver.get_latest(LatestKind::Uploaded).await.unwrap(),
            None
        );
        // Verify blocks were archived
        assert!(archiver.get_block_by_number(5).await.is_ok());
        assert!(archiver.get_block_by_number(10).await.is_ok());
        // Blocks outside the range should not exist
        assert!(archiver.get_block_by_number(4).await.is_err());
        assert!(archiver.get_block_by_number(11).await.is_err());
    }
}
