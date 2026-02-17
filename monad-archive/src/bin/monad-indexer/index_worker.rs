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

use alloy_primitives::hex::ToHexExt;
use monad_archive::{model::logs_index::LogsIndexArchiver, prelude::*};

/// Main worker that indexes transaction data from blocks into a searchable format.
/// Continuously polls for new blocks and indexes their transactions.
///
/// # Arguments
/// * `block_data_reader` - Source to read block data from
/// * `indexer` - Transaction indexer to write indexed data to
/// * `log_index` - Optional logs indexer to write log indexed data
/// * `max_blocks_per_iteration` - Maximum number of blocks to process in one iteration
/// * `max_concurrent_blocks` - Maximum number of blocks to process concurrently
/// * `metrics` - Metrics collection interface
/// * `stop_block_override` - Optional block number to stop indexing at
/// * `async_backfill` - If set, indexer will perform an asynchronous backfill of the index
///    This allows a second indexer to backfill a range while the first indexer is running
/// * `poll_frequency` - How often to check for new blocks
pub async fn index_worker(
    block_data_reader: impl BlockDataReader + Sync + Send,
    fallback_block_data_source: Option<impl BlockDataReader + Sync + Send>,
    indexer: TxIndexArchiver,
    log_index: Option<LogsIndexArchiver>,
    max_blocks_per_iteration: u64,
    max_concurrent_blocks: usize,
    metrics: Metrics,
    stop_block_override: Option<u64>,
    poll_frequency: Duration,
    async_backfill: bool,
) {
    // initialize starting block from stored latest marker
    let latest = indexer
        .get_latest_indexed(async_backfill)
        .await
        .unwrap_or(Some(0))
        .unwrap_or(0);
    let mut start_block = if latest != 0 { latest + 1 } else { 0 };

    loop {
        // query latest
        let latest_source = match block_data_reader.get_latest(LatestKind::Uploaded).await {
            Ok(number) => number.unwrap(),
            Err(e) => {
                warn!("Error getting latest uploaded block: {e:?}");
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
            info!(start_block, end_block, latest_source, "Nothing to process");
            sleep(poll_frequency).await;
            continue;
        }
        info!(
            start_block,
            end_block, latest_source, "Indexing group of blocks"
        );
        metrics.gauge(MetricNames::SOURCE_LATEST_BLOCK_NUM, latest_source);
        metrics.gauge(MetricNames::END_BLOCK_NUMBER, end_block);
        metrics.gauge(MetricNames::START_BLOCK_NUMBER, start_block);

        let latest_indexed = index_blocks(
            &block_data_reader,
            &fallback_block_data_source,
            &indexer,
            log_index.as_ref(),
            start_block..=end_block,
            max_concurrent_blocks,
            &metrics,
            async_backfill,
        )
        .await;

        start_block = if latest_indexed == 0 {
            0
        } else {
            latest_indexed + 1
        };
    }
}

async fn index_blocks(
    block_data_reader: &(impl BlockDataReader + Send),
    fallback_block_data_source: &Option<impl BlockDataReader + Send>,
    indexer: &TxIndexArchiver,
    log_index: Option<&LogsIndexArchiver>,
    block_range: RangeInclusive<u64>,
    concurrency: usize,
    metrics: &Metrics,
    async_backfill: bool,
) -> u64 {
    let start = Instant::now();

    let res: Result<usize, u64> = futures::stream::iter(block_range.clone())
        .map(|block_num: u64| async move {
            match handle_block(
                block_data_reader,
                fallback_block_data_source,
                indexer,
                log_index,
                block_num,
            )
            .await
            {
                Ok(num_txs) => Ok(num_txs),
                Err(e) => {
                    error!("Failed to handle block: {e:?}");
                    Err(block_num)
                }
            }
        })
        .buffered(concurrency)
        .try_fold(0, |total_txs, block_txs| async move {
            Ok(total_txs + block_txs)
        })
        .await;

    let (num_txs_indexed, new_latest_indexed) = match res {
        Ok(num_txs) => (num_txs, *block_range.end()),
        Err(err_block) => (0, err_block - 1),
    };

    info!(
        elapsed = start.elapsed().as_millis(),
        start = block_range.start(),
        end = block_range.end(),
        num_txs_indexed,
        "Finished indexing range",
    );
    metrics.counter(MetricNames::TXS_INDEXED, num_txs_indexed as u64);

    if new_latest_indexed != 0 {
        checkpoint_latest(indexer, new_latest_indexed, async_backfill).await;
    }

    new_latest_indexed
}

async fn handle_block(
    block_data_reader: &(impl BlockDataReader + Send),
    fallback_block_data_source: &Option<impl BlockDataReader + Send>,
    tx_index_archiver: &TxIndexArchiver,
    log_index: Option<&LogsIndexArchiver>,
    block_num: u64,
) -> Result<usize> {
    let BlockDataWithOffsets {
        block,
        receipts,
        traces,
        offsets,
    } = match block_data_reader
        .get_block_data_with_offsets(block_num)
        .await
    {
        Ok(data) => data,
        Err(e) => match fallback_block_data_source {
            Some(fallback) => {
                warn!(?e, block_num, "Failed to get block data with offsets from main source for block {block_num}, trying fallback source");
                fallback
                .get_block_data_with_offsets(block_num)
                .await
                .wrap_err_with(|| format!(
                        "Failed to get block data with offsets from fallback source for block {block_num}"
                    )
                )?
            }
            None => return Err(e),
        },
    };

    let num_txs = block.body.transactions.len();
    info!(num_txs, block_num, "Indexing block...");

    let first = block.body.transactions.first().cloned();
    let first_rx = receipts.first().cloned();
    let first_trace = traces.first().cloned();

    // Note: Include tx_index_archiver in branch here to avoid clones when unnecessary
    if let Some(log_index) = log_index {
        // Index Txs
        tx_index_archiver
            .index_block(block.clone(), traces, receipts.clone(), offsets)
            .await?;

        // Index Logs only when this writer is available
        log_index.index_block(&block, &receipts).await?;
    } else {
        // Index Txs
        tx_index_archiver
            .index_block(block, traces, receipts, offsets)
            .await?;
    }

    // Check 1 key
    tokio::spawn({
        let tx_index_archiver = tx_index_archiver.clone();
        async move {
            if let Some(tx) = first {
                let tx = tx.tx;
                let key = tx.tx_hash();
                match tx_index_archiver.get_tx_indexed_data(key).await {
                    Ok(Some(resp)) => {
                        if resp.header_subset.block_number != block_num
                            || Some(&resp.receipt) != first_rx.as_ref()
                            || Some(&resp.trace) != first_trace.as_ref()
                        {
                            warn!(
                                key = key.encode_hex(),
                                block_num,
                                ?resp,
                                "Returned index not as expected"
                            );
                        } else {
                            info!(
                                key = key.encode_hex(),
                                block_num, "Index spot-check successful"
                            );
                        }
                    }
                    Ok(None) => {
                        warn!(key = key.encode_hex(), block_num, "No index found");
                    }
                    Err(e) => warn!(
                        key = key.encode_hex(),
                        block_num, "Error while checking: {e:?}"
                    ),
                };
            }
        }
    });

    Ok(num_txs)
}

async fn checkpoint_latest(archiver: &TxIndexArchiver, block_num: u64, async_backfill: bool) {
    match archiver
        .update_latest_indexed(block_num, async_backfill)
        .await
    {
        Ok(()) => info!(block_num, "Set latest indexed checkpoint"),
        Err(e) => error!(block_num, "Failed to set latest indexed block: {e:?}"),
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{
        BlockBody, Header, Receipt, ReceiptEnvelope, ReceiptWithBloom, SignableTransaction,
        TxEip1559,
    };
    use alloy_primitives::{Bloom, Log, B256, U256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_archive::kvstore::{memory::MemoryStorage, WritePolicy};
    use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};

    use super::*;

    const NO_FALLBACK: Option<BlockDataReaderErased> = Option::<BlockDataReaderErased>::None;

    fn mock_tx_with_input_len(salt: u64, input_len: usize) -> TxEnvelopeWithSender {
        let tx = TxEip1559 {
            nonce: salt,
            gas_limit: 456 + salt,
            max_fee_per_gas: 789,
            max_priority_fee_per_gas: 135,
            input: std::iter::repeat_n(42, input_len)
                .collect::<Vec<u8>>()
                .into(),
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

    fn mock_tx(salt: u64) -> TxEnvelopeWithSender {
        mock_tx_with_input_len(salt, 10)
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

    fn mock_block(number: u64, transactions: Vec<TxEnvelopeWithSender>) -> Block {
        Block {
            header: Header {
                number,
                ..Default::default()
            },
            body: BlockBody {
                transactions,
                ommers: vec![],
                withdrawals: None,
            },
        }
    }

    fn memory_sink_source() -> (BlockDataArchive, TxIndexArchiver) {
        let source: KVStoreErased = MemoryStorage::new("source").into();
        let reader = BlockDataArchive::new(source.clone());
        let index_archiver = TxIndexArchiver::new(source, reader.clone(), 1000);

        (reader, index_archiver)
    }

    #[tokio::test]
    async fn test_index_worker_resumes_after_error() {
        let (reader, index_archiver) = memory_sink_source();

        // Prepare blocks 0-2 and 4-5, leaving block 3 missing to simulate an error
        for block_num in [0, 1, 2, 4, 5] {
            let tx = mock_tx(block_num);
            let block = mock_block(block_num, vec![tx]);
            let receipts = vec![mock_rx()];
            let traces = vec![vec![]];

            reader
                .archive_block(block, WritePolicy::NoClobber)
                .await
                .unwrap();
            reader
                .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
            reader
                .archive_traces(traces, block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
        }

        reader.update_latest(5, LatestKind::Uploaded).await.unwrap();
        index_archiver
            .update_latest_indexed(0, false)
            .await
            .unwrap();

        // Start worker that should process until block 2, then stop at missing block 3
        let worker_handle = tokio::spawn(index_worker(
            reader.clone(),
            NO_FALLBACK,
            index_archiver.clone(),
            None,
            3, // max_blocks_per_iteration
            2, // max_concurrent_blocks
            Metrics::none(),
            Some(5),
            Duration::from_micros(1),
            false,
        ));

        // Small delay to let worker process initial blocks
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify we stopped at block 2
        let latest_indexed = index_archiver
            .get_latest_indexed(false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest_indexed, 2);

        // Now add the missing block 3
        let tx = mock_tx(3);
        let block = mock_block(3, vec![tx]);
        let receipts = vec![mock_rx()];
        let traces = vec![vec![]];

        reader
            .archive_block(block, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_receipts(receipts, 3, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_traces(traces, 3, WritePolicy::NoClobber)
            .await
            .unwrap();

        // Wait for worker to complete all blocks
        worker_handle.await.unwrap();

        // Verify all blocks were eventually indexed
        let final_indexed = index_archiver
            .get_latest_indexed(false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_indexed, 5);

        // Verify all transactions were indexed properly
        for block_num in 0..=5 {
            let block = reader.get_block_by_number(block_num).await.unwrap();
            let tx_hash = block.body.transactions[0].tx.tx_hash();
            let indexed = index_archiver.get_tx_indexed_data(tx_hash).await;
            assert!(indexed.is_ok());
            assert_eq!(
                indexed.unwrap().unwrap().header_subset.block_number,
                block_num
            );
        }
    }

    #[tokio::test]
    async fn test_index_worker_handles_new_blocks() {
        let (reader, index_archiver) = memory_sink_source();

        // Prepare initial blocks
        for block_num in 0..=5 {
            let tx = mock_tx(block_num);
            let block = mock_block(block_num, vec![tx]);
            let receipts = vec![mock_rx()];
            let traces = vec![vec![]];

            reader
                .archive_block(block, WritePolicy::NoClobber)
                .await
                .unwrap();
            reader
                .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
            reader
                .archive_traces(traces, block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
        }

        // Set initial latest
        reader.update_latest(5, LatestKind::Uploaded).await.unwrap();
        // Set indexer's starting point
        index_archiver
            .update_latest_indexed(0, false)
            .await
            .unwrap();

        // Start worker in separate task
        let worker_handle = tokio::spawn(index_worker(
            reader.clone(),
            NO_FALLBACK,
            index_archiver.clone(),
            None,
            3, // max_blocks_per_iteration
            2, // max_concurrent_blocks
            Metrics::none(),
            Some(10), // Stop at block 10
            Duration::from_micros(1),
            false,
        ));

        // Small delay to let worker start processing initial blocks
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Add new blocks while worker is running
        for block_num in 6..=10 {
            let tx = mock_tx_with_input_len(block_num, 1000000);
            let block = mock_block(block_num, vec![tx]);
            let receipts = vec![mock_rx()];
            let traces = vec![vec![]];

            reader
                .archive_block(block, WritePolicy::NoClobber)
                .await
                .unwrap();
            reader
                .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
            reader
                .archive_traces(traces, block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
        }

        // Update latest to include new blocks
        reader
            .update_latest(10, LatestKind::Uploaded)
            .await
            .unwrap();

        // Wait for worker to complete
        worker_handle.await.unwrap();

        // Verify all blocks were indexed
        let latest_indexed = index_archiver
            .get_latest_indexed(false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest_indexed, 10);

        // Verify each block's transaction was indexed
        for block_num in 0..=10 {
            let block = reader.get_block_by_number(block_num).await.unwrap();
            let tx_hash = block.body.transactions[0].tx.tx_hash();
            let indexed = index_archiver.get_tx_indexed_data(tx_hash).await;
            assert!(indexed.is_ok());
            assert_eq!(
                indexed.unwrap().unwrap().header_subset.block_number,
                block_num
            );
        }
    }

    #[tokio::test]
    async fn test_index_worker_basic_operation() {
        let (reader, index_archiver) = memory_sink_source();

        // Prepare initial blocks in the source
        for block_num in 0..=5 {
            let tx = mock_tx(block_num);
            let block = mock_block(block_num, vec![tx]);
            let receipts = vec![mock_rx()];
            let traces = vec![vec![]];

            reader
                .archive_block(block, WritePolicy::NoClobber)
                .await
                .unwrap();
            reader
                .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
            reader
                .archive_traces(traces, block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
        }

        // Set up latest block in source
        reader.update_latest(5, LatestKind::Uploaded).await.unwrap();
        // Set indexer's starting point
        index_archiver
            .update_latest_indexed(0, false)
            .await
            .unwrap();

        // Start the worker in a separate task since it runs indefinitely
        let worker_handle = tokio::spawn(index_worker(
            reader.clone(),
            NO_FALLBACK,
            index_archiver.clone(),
            None,
            3, // max_blocks_per_iteration
            2, // max_concurrent_blocks
            Metrics::none(),
            Some(5), // stop_block_override - make it finite for testing
            Duration::from_micros(1),
            false,
        ));

        // Wait for worker to complete
        worker_handle.await.unwrap();

        // Verify all blocks were indexed
        let latest_indexed = index_archiver
            .get_latest_indexed(false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest_indexed, 5);

        // Verify each block's transactions were indexed
        for block_num in 0..=5 {
            let block = reader.get_block_by_number(block_num).await.unwrap();
            let tx_hash = block.body.transactions[0].tx.tx_hash();
            let indexed = index_archiver.get_tx_indexed_data(tx_hash).await;
            assert!(indexed.is_ok());
            assert_eq!(
                indexed.unwrap().unwrap().header_subset.block_number,
                block_num
            );
        }
    }

    #[tokio::test]
    async fn test_index_blocks_with_checkpoint() {
        let (reader, index_archiver) = memory_sink_source();
        let block_range = 0..=15; // Range that will trigger checkpoint

        // Prepare test data
        for block_num in block_range.clone() {
            let num_txs = if block_num % 2 == 0 { 1 } else { 2 }; // Alternate between 1 and 2 txs
            let txs = (0..num_txs)
                .map(|_| mock_tx(block_num * 100 * num_txs))
                .collect();
            let block = mock_block(block_num, txs);
            let receipts = (0..num_txs).map(|_| mock_rx()).collect();
            let traces = (0..num_txs).map(|_| vec![]).collect();

            reader
                .archive_block(block, WritePolicy::NoClobber)
                .await
                .unwrap();
            reader
                .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
            reader
                .archive_traces(traces, block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
        }

        let result = index_blocks(
            &reader,
            &NO_FALLBACK,
            &index_archiver,
            None,
            block_range,
            2, // concurrency
            &Metrics::none(),
            false,
        )
        .await;

        assert_eq!(result, 15);

        // Verify checkpoint was created at block 10
        let checkpoint = index_archiver
            .get_latest_indexed(false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint, 15);

        // Verify all transactions were indexed
        for block_num in 0..=15 {
            let num_txs = if block_num % 2 == 0 { 1 } else { 2 };
            let block = reader.get_block_by_number(block_num).await.unwrap();
            assert_eq!(block.body.transactions.len(), num_txs);

            // Verify each transaction
            for tx in block.body.transactions {
                let tx = tx.tx;
                let indexed = index_archiver.get_tx_indexed_data(tx.tx_hash()).await;
                assert!(indexed.is_ok());
            }
        }
    }

    #[tokio::test]
    async fn test_index_blocks_stops_at_error() {
        let (reader, index_archiver) = memory_sink_source();
        let block_range = 0..=4;

        // Prepare data for blocks 0,1,3,4 - missing block 2 to simulate error
        for block_num in [0, 1, 3, 4] {
            let tx = mock_tx(block_num);
            let block = mock_block(block_num, vec![tx]);
            let receipts = vec![mock_rx()];
            let traces = vec![vec![]];

            reader
                .archive_block(block, WritePolicy::NoClobber)
                .await
                .unwrap();
            reader
                .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
            reader
                .archive_traces(traces, block_num, WritePolicy::NoClobber)
                .await
                .unwrap();
        }

        let result = index_blocks(
            &reader,
            &NO_FALLBACK,
            &index_archiver,
            None,
            block_range,
            2, // concurrency
            &Metrics::none(),
            false,
        )
        .await;
        // Should return the last successful block before error
        assert_eq!(result, 1);

        // Verify blocks before error were indexed
        for block_num in 0..=1 {
            let block = reader.get_block_by_number(block_num).await.unwrap();
            let tx_hash = block.body.transactions[0].tx.tx_hash();
            let indexed = index_archiver.get_tx_indexed_data(tx_hash).await;
            assert!(indexed.is_ok());
        }

        // Verify latest indexed checkpoint is correct
        let latest = index_archiver
            .get_latest_indexed(false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest, 1);
    }

    #[tokio::test]
    async fn test_handle_block_single_tx() {
        let (reader, index_archiver) = memory_sink_source();
        let block_num = 10;

        // Prepare test data
        let tx = mock_tx(12);
        let block = mock_block(block_num, vec![tx]);
        let receipts = vec![mock_rx()];
        let traces = vec![vec![]]; // Empty trace for now

        // Store test data in reader
        reader
            .archive_block(block.clone(), WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_receipts(receipts.clone(), block_num, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_traces(traces.clone(), block_num, WritePolicy::NoClobber)
            .await
            .unwrap();

        // Test handle_block
        let num_txs = handle_block(&reader, &NO_FALLBACK, &index_archiver, None, block_num)
            .await
            .unwrap();
        assert_eq!(num_txs, 1);

        // Verify indexed data
        let tx_hash = block.body.transactions[0].tx.tx_hash();
        let indexed = index_archiver
            .get_tx_indexed_data(tx_hash)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(indexed.header_subset.block_number, block_num);
        assert_eq!(indexed.receipt, receipts[0]);
        assert_eq!(indexed.trace, traces[0]);
    }

    #[tokio::test]
    async fn test_handle_block_single_tx_use_references() {
        let (reader, index_archiver) = memory_sink_source();
        let block_num = 10;

        // Prepare test data
        let tx = mock_tx_with_input_len(12, 50_000);
        let block = mock_block(block_num, vec![tx]);
        let receipts = vec![mock_rx()];
        let traces = vec![vec![]]; // Empty trace for now

        // Store test data in reader
        reader
            .archive_block(block.clone(), WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_receipts(receipts.clone(), block_num, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_traces(traces.clone(), block_num, WritePolicy::NoClobber)
            .await
            .unwrap();

        // Test handle_block
        let num_txs = handle_block(&reader, &NO_FALLBACK, &index_archiver, None, block_num)
            .await
            .unwrap();
        assert_eq!(num_txs, 1);

        // Verify indexed data
        let tx_hash = block.body.transactions[0].tx.tx_hash();
        let indexed = index_archiver
            .get_tx_indexed_data(tx_hash)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(indexed.header_subset.block_number, block_num);
        assert_eq!(indexed.receipt, receipts[0]);
        assert_eq!(indexed.trace, traces[0]);
    }

    #[tokio::test]
    async fn test_handle_block_empty() {
        let (reader, index_archiver) = memory_sink_source();
        let block_num = 10;

        // Prepare empty block test data
        let block = mock_block(block_num, vec![]);
        let receipts: BlockReceipts = vec![];
        let traces: BlockTraces = vec![];

        // Store test data in reader
        reader
            .archive_block(block.clone(), WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_traces(traces, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();

        // Test handle_block
        let num_txs = handle_block(&reader, &NO_FALLBACK, &index_archiver, None, block_num)
            .await
            .unwrap();

        assert_eq!(num_txs, 0);
    }

    #[tokio::test]
    async fn test_handle_block_multiple_txs() {
        let (reader, index_archiver) = memory_sink_source();
        let block_num = 10;

        // Prepare test data with multiple transactions
        let txs = vec![mock_tx(1), mock_tx_with_input_len(2, 20_000), mock_tx(3)];
        let block = mock_block(block_num, txs);
        let receipts = vec![mock_rx(), mock_rx(), mock_rx()];
        let traces = vec![vec![], vec![], vec![]];

        // Store test data
        reader
            .archive_block(block.clone(), WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_receipts(receipts.clone(), block_num, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_traces(traces.clone(), block_num, WritePolicy::NoClobber)
            .await
            .unwrap();

        // Test handle_block
        let num_txs = handle_block(&reader, &NO_FALLBACK, &index_archiver, None, block_num)
            .await
            .unwrap();

        assert_eq!(num_txs, 3);

        // Verify all transactions were indexed correctly
        for (i, tx) in block.body.transactions.iter().enumerate() {
            let tx_hash = tx.tx.tx_hash();
            let indexed = index_archiver.get_tx_indexed_data(tx_hash).await;
            assert!(indexed.is_ok());
            let indexed = indexed.unwrap().unwrap();
            assert_eq!(indexed.header_subset.block_number, block_num);
            assert_eq!(indexed.receipt, receipts[i]);
            assert_eq!(indexed.trace, traces[i]);
        }
    }

    #[tokio::test]
    async fn test_handle_block_missing_block() {
        let (reader, index_archiver) = memory_sink_source();
        let block_num = 10;

        // Only store receipts and traces, omit block
        let receipts = vec![mock_rx()];
        let traces = vec![vec![]];
        reader
            .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_traces(traces, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();

        // Should fail since block is missing
        let result = handle_block(&reader, &NO_FALLBACK, &index_archiver, None, block_num).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_block_missing_block_with_fallback() {
        let (reader, index_archiver) = memory_sink_source();
        let (fallback_reader, _) = memory_sink_source();
        let block_num = 10;

        // Only store receipts and traces, omit block
        let receipts = vec![mock_rx()];
        let traces = vec![vec![]];
        reader
            .archive_receipts(receipts.clone(), block_num, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_traces(traces.clone(), block_num, WritePolicy::NoClobber)
            .await
            .unwrap();

        // Fallback has everything
        let block = mock_block(block_num, vec![mock_tx(123)]);
        fallback_reader
            .archive_block(block, WritePolicy::NoClobber)
            .await
            .unwrap();
        fallback_reader
            .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();
        fallback_reader
            .archive_traces(traces, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();

        let result = handle_block(
            &reader,
            &Some(fallback_reader),
            &index_archiver,
            None,
            block_num,
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_block_missing_receipts() {
        let (reader, index_archiver) = memory_sink_source();
        let block_num = 10;

        // Store block and traces, omit receipts
        let tx = mock_tx(123);
        let block = mock_block(block_num, vec![tx]);
        let traces = vec![vec![]];
        reader
            .archive_block(block, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_traces(traces, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();

        let result = handle_block(&reader, &NO_FALLBACK, &index_archiver, None, block_num).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_block_missing_traces() {
        let (reader, index_archiver) = memory_sink_source();
        let block_num = 10;

        // Store block and receipts, omit traces
        let tx = mock_tx(123);
        let block = mock_block(block_num, vec![tx]);
        let receipts = vec![mock_rx()];
        reader
            .archive_block(block, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();

        let result = handle_block(&reader, &NO_FALLBACK, &index_archiver, None, block_num).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_block_mismatched_receipts() {
        let (reader, index_archiver) = memory_sink_source();
        let block_num = 10;

        // Create block with 2 txs but only 1 receipt
        let txs = vec![mock_tx(1), mock_tx(2)];
        let block = mock_block(block_num, txs);
        let receipts = vec![mock_rx()]; // Only one receipt
        let traces = vec![vec![], vec![]];

        reader
            .archive_block(block, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_traces(traces, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();

        let result = handle_block(&reader, &NO_FALLBACK, &index_archiver, None, block_num).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_block_mismatched_traces() {
        let (reader, index_archiver) = memory_sink_source();
        let block_num = 10;

        // Create block with 2 txs but 3 traces
        let txs = vec![mock_tx(1), mock_tx(2)];
        let block = mock_block(block_num, txs);
        let receipts = vec![mock_rx(), mock_rx()];
        let traces = vec![vec![], vec![], vec![]]; // Extra trace

        reader
            .archive_block(block, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_traces(traces, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();

        let result = handle_block(&reader, &NO_FALLBACK, &index_archiver, None, block_num).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_block_invalid_block_number() {
        let (reader, index_archiver) = memory_sink_source();
        let block_num = 10;

        // Store block with different number than requested
        let tx = mock_tx(123);
        let block = mock_block(block_num + 1, vec![tx]); // Different block number
        let receipts = vec![mock_rx()];
        let traces = vec![vec![]];

        reader
            .archive_block(block, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_receipts(receipts, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();
        reader
            .archive_traces(traces, block_num, WritePolicy::NoClobber)
            .await
            .unwrap();

        let result = handle_block(&reader, &NO_FALLBACK, &index_archiver, None, block_num).await;
        assert!(result.is_err());
    }
}
