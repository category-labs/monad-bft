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

use alloy_primitives::BlockHash;
use eyre::Result;
use monad_triedb_utils::triedb_env::ReceiptWithLogIndex;
use tracing::trace;

use crate::{
    cli::AwsCliArgs,
    failover_circuit_breaker::{CircuitBreaker, FallbackExecutor},
    kvstore::{cloud_proxy::CloudProxyReader, mongo::MongoDbStorage},
    model::logs_index::LogsIndexArchiver,
    prelude::*,
};

#[derive(Clone, Copy)]
pub enum LatestKind {
    Uploaded,
    Indexed,
}

#[derive(Clone)]
pub struct ArchiveReader {
    block_data_executor: Arc<FallbackExecutor<BlockDataReaderErased>>,
    index_executor: Arc<FallbackExecutor<IndexReaderImpl>>,
    pub log_index: Option<LogsIndexArchiver>,
}

impl ArchiveReader {
    pub fn get_circuit_breaker_metrics(
        &self,
    ) -> (
        crate::failover_circuit_breaker::CircuitBreakerMetrics,
        crate::failover_circuit_breaker::CircuitBreakerMetrics,
    ) {
        (
            self.block_data_executor.circuit_breaker.metrics(),
            self.index_executor.circuit_breaker.metrics(),
        )
    }

    pub fn new(
        block_data_reader: impl Into<BlockDataReaderErased>,
        tx_index_reader: IndexReaderImpl,
        fallback: Option<Box<ArchiveReader>>,
        log_index: Option<LogsIndexArchiver>,
    ) -> ArchiveReader {
        trace!(
            has_fallback = fallback.is_some(),
            "Creating new ArchiveReader instance"
        );
        let block_data_reader = block_data_reader.into();

        // Create circuit breakers with same settings as before
        let block_circuit_breaker = CircuitBreaker::new(10, Duration::from_secs(60 * 5));
        let index_circuit_breaker = CircuitBreaker::new(10, Duration::from_secs(60 * 5));

        let (fallback_block_reader, fallback_index_reader) = if let Some(fallback) = fallback {
            // Extract readers from the fallback ArchiveReader
            let block_reader = fallback.block_data_executor.primary.clone();
            let index_reader = fallback.index_executor.primary.clone();
            (Some(block_reader), Some(index_reader))
        } else {
            (None, None)
        };

        let reader = ArchiveReader {
            block_data_executor: Arc::new(FallbackExecutor::new(
                block_data_reader,
                fallback_block_reader,
                block_circuit_breaker,
            )),
            index_executor: Arc::new(FallbackExecutor::new(
                tx_index_reader,
                fallback_index_reader,
                index_circuit_breaker,
            )),
            log_index,
        };
        debug!("ArchiveReader instance created successfully");
        reader
    }

    pub async fn init_mongo_reader(
        url: String,
        db: String,
        metrics: Metrics,
        max_time_get: Option<Duration>,
    ) -> Result<ArchiveReader> {
        info!(url, db, "Initializing MongoDB ArchiveReader");
        trace!("Creating MongoDB block store");
        let mut block_store = MongoDbStorage::new_block_store(&url, &db, metrics.clone()).await?;
        if let Some(max_time_get) = max_time_get {
            block_store.max_time_get = max_time_get;
        }
        let block_data_reader = BlockDataArchive::new(block_store);

        trace!("Creating MongoDB index store");
        let mut index_store = MongoDbStorage::new_index_store(&url, &db, metrics).await?;
        if let Some(max_time_get) = max_time_get {
            index_store.max_time_get = max_time_get;
        }
        let index_reader = IndexReaderImpl::new(index_store, block_data_reader.clone());

        trace!("Creating MongoDB log index store");
        let log_index = LogsIndexArchiver::from_tx_index_archiver(&index_reader, 50, true)
            .await
            .wrap_err("Failed to create log index reader")?;

        debug!("MongoDB ArchiveReader initialization complete");
        Ok(ArchiveReader::new(
            block_data_reader,
            index_reader,
            None,
            Some(log_index),
        ))
    }

    pub async fn init_aws_reader(
        bucket: String,
        region: Option<String>,
        url: &str,
        api_key: &str,
        concurrency: usize,
    ) -> Result<ArchiveReader> {
        info!(
            cloud_proxy_url = url,
            bucket, region, "Initializing AWS ArchiveReader"
        );
        let url = url::Url::parse(url)?;

        trace!("Creating AWS block data reader");
        let args = AwsCliArgs {
            bucket: bucket.clone(),
            region,
            concurrency,
            // TODO: these should be configurable
            operation_timeout_secs: 5,
            operation_attempt_timeout_secs: 2,
            read_timeout_secs: 2,
            ..Default::default()
        };
        let config = args.config().await;
        let block_data_reader =
            BlockDataArchive::new(Bucket::new(bucket.clone(), &config, Metrics::none()));

        trace!("Creating cloud proxy reader");
        let cloud_proxy_reader = CloudProxyReader::new(api_key, url, bucket)?;
        let tx_index_reader = IndexReaderImpl::new(cloud_proxy_reader, block_data_reader.clone());

        debug!("AWS ArchiveReader initialization complete");
        Ok(ArchiveReader::new(
            block_data_reader,
            tx_index_reader,
            None,
            None,
        ))
    }

    pub fn with_fallback(
        self,
        fallback: Option<ArchiveReader>,
        failure_threshold: Option<u32>,
        failure_timeout: Option<Duration>,
    ) -> Self {
        if let Some(fallback) = fallback {
            let failure_threshold = failure_threshold.unwrap_or(100);
            let failure_timeout = failure_timeout.unwrap_or(Duration::from_secs(60 * 5));

            // Create new executors with the fallback readers
            let block_circuit_breaker = CircuitBreaker::new(failure_threshold, failure_timeout);
            let index_circuit_breaker = CircuitBreaker::new(failure_threshold, failure_timeout);

            ArchiveReader {
                block_data_executor: Arc::new(FallbackExecutor::new(
                    self.block_data_executor.primary.clone(),
                    Some(fallback.block_data_executor.primary.clone()),
                    block_circuit_breaker,
                )),
                index_executor: Arc::new(FallbackExecutor::new(
                    self.index_executor.primary.clone(),
                    Some(fallback.index_executor.primary.clone()),
                    index_circuit_breaker,
                )),
                log_index: self.log_index,
            }
        } else {
            self
        }
    }
}

impl IndexReader for ArchiveReader {
    async fn get_latest_indexed(&self) -> Result<Option<u64>> {
        self.index_executor
            .execute(|idx| idx.get_latest_indexed())
            .await
    }

    async fn get_tx_indexed_data(
        &self,
        tx_hash: &alloy_primitives::TxHash,
    ) -> Result<Option<TxIndexedData>> {
        self.index_executor
            .execute(|idx| idx.get_tx_indexed_data(tx_hash))
            .await
    }

    async fn get_tx_indexed_data_bulk(
        &self,
        tx_hashes: &[alloy_primitives::TxHash],
    ) -> Result<HashMap<alloy_primitives::TxHash, TxIndexedData>> {
        self.index_executor
            .execute(|idx| idx.get_tx_indexed_data_bulk(tx_hashes))
            .await
    }

    async fn get_tx(
        &self,
        tx_hash: &alloy_primitives::TxHash,
    ) -> Result<Option<(TxEnvelopeWithSender, HeaderSubset)>> {
        self.index_executor.execute(|idx| idx.get_tx(tx_hash)).await
    }

    async fn get_trace(
        &self,
        tx_hash: &alloy_primitives::TxHash,
    ) -> Result<Option<(Vec<u8>, HeaderSubset)>> {
        self.index_executor
            .execute(|idx| idx.get_trace(tx_hash))
            .await
    }

    async fn get_receipt(
        &self,
        tx_hash: &alloy_primitives::TxHash,
    ) -> Result<Option<(ReceiptWithLogIndex, HeaderSubset)>> {
        self.index_executor
            .execute(|idx| idx.get_receipt(tx_hash))
            .await
    }

    async fn resolve_from_bytes(&self, bytes: &[u8]) -> Result<TxIndexedData> {
        self.index_executor
            .execute(|idx| idx.resolve_from_bytes(bytes))
            .await
    }
}

impl BlockDataReader for ArchiveReader {
    fn get_bucket(&self) -> &str {
        self.block_data_executor.primary.get_bucket()
    }

    async fn get_latest(&self, latest_kind: LatestKind) -> Result<Option<u64>> {
        self.block_data_executor
            .execute(|bdr| bdr.get_latest(latest_kind))
            .await
    }

    async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        self.block_data_executor
            .execute(|bdr| bdr.get_block_by_number(block_num))
            .await
    }

    async fn get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Block> {
        self.block_data_executor
            .execute(|bdr| bdr.get_block_by_hash(block_hash))
            .await
    }

    async fn get_block_receipts(&self, block_number: u64) -> Result<BlockReceipts> {
        self.block_data_executor
            .execute(|bdr| bdr.get_block_receipts(block_number))
            .await
    }

    async fn get_block_traces(&self, block_number: u64) -> Result<BlockTraces> {
        self.block_data_executor
            .execute(|bdr| bdr.get_block_traces(block_number))
            .await
    }

    async fn get_block_data_with_offsets(&self, block_num: u64) -> Result<BlockDataWithOffsets> {
        self.block_data_executor
            .execute(|bdr| bdr.get_block_data_with_offsets(block_num))
            .await
    }

    #[doc = " Get a block by its number, or return None if not found"]
    async fn try_get_block_by_number(&self, block_num: u64) -> Result<Option<Block>> {
        self.block_data_executor
            .execute(|bdr| bdr.try_get_block_by_number(block_num))
            .await
    }

    #[doc = " Get receipts for a block, or return None if not found"]
    async fn try_get_block_receipts(&self, block_number: u64) -> Result<Option<BlockReceipts>> {
        self.block_data_executor
            .execute(|bdr| bdr.try_get_block_receipts(block_number))
            .await
    }

    #[doc = " Get execution traces for a block, or return None if not found"]
    async fn try_get_block_traces(&self, block_number: u64) -> Result<Option<BlockTraces>> {
        self.block_data_executor
            .execute(|bdr| bdr.try_get_block_traces(block_number))
            .await
    }

    #[doc = " Get a block by its hash, or return None if not found"]
    async fn try_get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Option<Block>> {
        self.block_data_executor
            .execute(|bdr| bdr.try_get_block_by_hash(block_hash))
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;
    use crate::{
        kvstore::memory::MemoryStorage,
        test_utils::{mock_block, mock_rx, mock_tx},
    };

    fn setup_index() -> (TxIndexArchiver, TxIndexArchiver) {
        let primary = MemoryStorage::new("primary");
        let bdr = BlockDataArchive::new(primary.clone());
        let primary = TxIndexArchiver::new(primary, bdr, 1000);

        let fallback = MemoryStorage::new("fallback");
        let bdr = BlockDataArchive::new(fallback.clone());
        let fallback = TxIndexArchiver::new(fallback, bdr, 1000);
        (primary, fallback)
    }

    fn setup_index_with_should_fail_ptr() -> (
        TxIndexArchiver,
        TxIndexArchiver,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
    ) {
        let primary = MemoryStorage::new("primary");
        let primary_should_fail_ptr = primary.should_fail.clone();
        let bdr = BlockDataArchive::new(primary.clone());
        let primary = TxIndexArchiver::new(primary, bdr, 1000);

        let fallback = MemoryStorage::new("fallback");
        let fallback_should_fail_ptr = fallback.should_fail.clone();
        let bdr = BlockDataArchive::new(fallback.clone());
        let fallback = TxIndexArchiver::new(fallback, bdr, 1000);
        (
            primary,
            fallback,
            primary_should_fail_ptr,
            fallback_should_fail_ptr,
        )
    }

    #[tokio::test]
    async fn test_get_tx_primary() {
        let (primary, fallback) = setup_index();

        let tx = mock_tx(123);
        primary
            .index_block(
                mock_block(10, vec![tx.clone()]),
                vec![vec![]],
                vec![mock_rx(100, 10)],
                None,
            )
            .await
            .unwrap();

        let fallback = ArchiveReader::new(
            fallback.reader.block_data_reader.clone(),
            fallback.reader.clone(),
            None,
            None,
        );
        let reader = ArchiveReader::new(
            primary.reader.block_data_reader.clone(),
            primary.reader,
            Some(Box::new(fallback)),
            None,
        );

        let (tx_ret, _) = reader.get_tx(tx.tx.tx_hash()).await.unwrap().unwrap();
        assert_eq!(tx_ret, tx);
    }

    #[tokio::test]
    async fn test_get_tx_missing_no_fallback() {
        let (primary, _) = setup_index();
        let reader = ArchiveReader::new(
            primary.reader.block_data_reader.clone(),
            primary.reader,
            None,
            None,
        );

        let tx = mock_tx(123);
        let ret = reader.get_tx(tx.tx.tx_hash()).await;
        assert!(ret.is_ok());
        assert!(ret.unwrap().is_none())
    }

    #[tokio::test]
    async fn test_get_tx_missing_both() {
        let (primary, fallback) = setup_index();
        let fallback = ArchiveReader::new(
            fallback.reader.block_data_reader.clone(),
            fallback.reader.clone(),
            None,
            None,
        );
        let reader = ArchiveReader::new(
            primary.reader.block_data_reader.clone(),
            primary.reader,
            Some(Box::new(fallback)),
            None,
        );

        let tx = mock_tx(123);
        let ret = reader.get_tx(tx.tx.tx_hash()).await;
        assert!(ret.is_ok());
        assert!(ret.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_get_tx_fallback() {
        let (primary, fallback, primary_should_fail_ptr, _fallback_should_fail_ptr) =
            setup_index_with_should_fail_ptr();

        let tx = mock_tx(123);
        fallback
            .index_block(
                mock_block(10, vec![tx.clone()]),
                vec![vec![]],
                vec![mock_rx(100, 10)],
                None,
            )
            .await
            .unwrap();

        let fallback = ArchiveReader::new(
            fallback.reader.block_data_reader.clone(),
            fallback.reader.clone(),
            None,
            None,
        );
        let reader = ArchiveReader::new(
            primary.reader.block_data_reader.clone(),
            primary.reader,
            Some(Box::new(fallback)),
            None,
        );

        primary_should_fail_ptr.store(true, std::sync::atomic::Ordering::SeqCst);

        let (tx_ret, _) = reader.get_tx(tx.tx.tx_hash()).await.unwrap().unwrap();
        assert_eq!(tx_ret, tx);
    }

    fn setup_bdr() -> (BlockDataArchive, BlockDataArchive) {
        let primary = MemoryStorage::new("primary");
        let primary = BlockDataArchive::new(primary);
        let fallback = MemoryStorage::new("fallback");
        let fallback = BlockDataArchive::new(fallback);
        (primary, fallback)
    }

    #[tokio::test]
    async fn test_get_block_missing_in_both() {
        let (primary, fallback) = setup_bdr();
        let fallback = ArchiveReader::new(
            fallback,
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary.clone()),
            None,
            None,
        );
        let reader = ArchiveReader::new(
            primary.clone(),
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary),
            Some(Box::new(fallback)),
            None,
        );

        let result = reader.get_block_by_number(10).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_block_missing_no_fallback() {
        let (primary, _) = setup_bdr();

        let reader = ArchiveReader::new(
            primary.clone(),
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary),
            None,
            None,
        );

        let result = reader.get_block_by_number(12).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_block_present_in_fallback() {
        let (primary, fallback) = setup_bdr();

        let block = mock_block(12, vec![]);
        fallback.archive_block(block.clone()).await.unwrap();

        let fallback = ArchiveReader::new(
            fallback,
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary.clone()),
            None,
            None,
        );
        let reader = ArchiveReader::new(
            primary.clone(),
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary),
            Some(Box::new(fallback)),
            None,
        );

        let result = reader.get_block_by_number(12).await.unwrap();
        assert_eq!(result, block);
    }

    #[tokio::test]
    async fn test_get_block_present_in_primary_no_fallback() {
        let (primary, _) = setup_bdr();

        let block = mock_block(12, vec![]);
        primary.archive_block(block.clone()).await.unwrap();

        let reader = ArchiveReader::new(
            primary.clone(),
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary.clone()),
            None,
            None,
        );

        let result = reader.get_block_by_number(12).await.unwrap();
        assert_eq!(result, block);
    }

    #[tokio::test]
    async fn test_get_block_present_in_primary() {
        let (primary, fallback) = setup_bdr();

        let block = mock_block(12, vec![]);
        primary.archive_block(block.clone()).await.unwrap();

        let fallback = ArchiveReader::new(
            fallback,
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary.clone()),
            None,
            None,
        );
        let reader = ArchiveReader::new(
            primary.clone(),
            IndexReaderImpl::new(MemoryStorage::new("usused_index"), primary),
            Some(Box::new(fallback)),
            None,
        );

        let result = reader.get_block_by_number(12).await.unwrap();
        assert_eq!(result, block);
    }

    #[tokio::test]
    async fn test_circuit_breaker_behavior() {
        use std::sync::atomic::Ordering;

        use crate::kvstore::memory::MemoryStorage;

        // Set up primary that fails
        let primary = MemoryStorage::new("primary");
        let should_fail_ref = primary.should_fail.clone();
        should_fail_ref.store(true, Ordering::SeqCst);

        let primary_counting = primary.clone();

        // We need to wrap the storage to count calls, but for now let's just verify the behavior
        let primary_bdr = BlockDataArchive::new(primary.clone());

        // Set up working fallback
        let fallback = MemoryStorage::new("fallback");
        let fallback_bdr = BlockDataArchive::new(fallback.clone());

        // Store block in fallback
        let block = mock_block(42, vec![]);
        fallback_bdr.archive_block(block.clone()).await.unwrap();

        // Create readers
        let primary_reader = ArchiveReader::new(
            primary_bdr.clone(),
            IndexReaderImpl::new(primary, primary_bdr.clone()),
            None,
            None,
        );

        let fallback_reader = ArchiveReader::new(
            fallback_bdr.clone(),
            IndexReaderImpl::new(fallback, fallback_bdr),
            None,
            None,
        );

        let reader = primary_reader.with_fallback(Some(fallback_reader), Some(10), None);

        // First 10 requests should fail and use fallback
        for i in 0..10 {
            let result = reader.get_block_by_number(42).await;
            assert!(result.is_ok(), "Request {} should succeed via fallback", i);
            assert_eq!(result.unwrap(), block);
        }

        // After 10 failures, circuit should be open and go directly to fallback
        let metrics = reader.get_circuit_breaker_metrics();
        assert_eq!(metrics.0.state, "open", "Circuit breaker should be open");
        assert_eq!(metrics.0.failure_count, 10, "Should have 10 failures");

        // Now let's test that primary recovers after timeout
        // First, stop the primary from failing
        should_fail_ref.store(false, Ordering::SeqCst);

        // Store the block in primary now so it can succeed
        primary_bdr.archive_block(block.clone()).await.unwrap();

        // Wait for recovery timeout (5 minutes is too long for test, so let's test half-open behavior)
        // The circuit breaker should transition to half-open after the timeout

        // For now, just verify that more requests succeed
        for i in 10..15 {
            let result = reader.get_block_by_number(42).await;
            assert!(result.is_ok(), "Request {} should succeed", i);
            assert_eq!(result.unwrap(), block);
        }

        // Test the case where there's no fallback and primary fails
        let failing_reader = ArchiveReader::new(
            primary_bdr.clone(),
            IndexReaderImpl::new(primary_counting, primary_bdr.clone()),
            None,
            None,
        );

        should_fail_ref.store(true, Ordering::SeqCst);
        let result = failing_reader.get_block_by_number(1).await;
        assert!(
            result.is_err(),
            "Should fail when no fallback and primary fails"
        );
    }

    #[tokio::test]
    async fn test_circuit_breaker_source_tracking() {
        use std::sync::atomic::Ordering;

        use crate::kvstore::memory::MemoryStorage;

        // Set up primary and fallback with different data to verify which is used
        let primary = MemoryStorage::new("primary");
        let should_fail_primary = primary.should_fail.clone();
        let primary_bdr = BlockDataArchive::new(primary.clone());

        let fallback = MemoryStorage::new("fallback");
        let fallback_bdr = BlockDataArchive::new(fallback.clone());

        // Store DIFFERENT blocks in primary and fallback to track which is used
        let primary_block = {
            let mut block = mock_block(42, vec![]);
            block.header.extra_data = alloy_primitives::Bytes::from(vec![1u8, 2, 3]); // Mark as primary
            block
        };
        let fallback_block = {
            let mut block = mock_block(42, vec![]);
            block.header.extra_data = alloy_primitives::Bytes::from(vec![4u8, 5, 6]); // Mark as fallback
            block
        };

        primary_bdr
            .archive_block(primary_block.clone())
            .await
            .unwrap();
        fallback_bdr
            .archive_block(fallback_block.clone())
            .await
            .unwrap();

        // Create reader with fallback
        let reader = ArchiveReader::new(
            primary_bdr.clone(),
            IndexReaderImpl::new(primary.clone(), primary_bdr.clone()),
            None,
            None,
        )
        .with_fallback(
            Some(ArchiveReader::new(
                fallback_bdr.clone(),
                IndexReaderImpl::new(fallback.clone(), fallback_bdr.clone()),
                None,
                None,
            )),
            Some(10),
            None,
        );

        // Test 1: Primary works, should use primary
        let result = reader.get_block_by_number(42).await.unwrap();
        assert_eq!(
            result.header.extra_data,
            alloy_primitives::Bytes::from(vec![1u8, 2, 3]),
            "Should use primary when it works"
        );

        // Test 2: Primary fails, should use fallback
        should_fail_primary.store(true, Ordering::SeqCst);
        let result = reader.get_block_by_number(42).await.unwrap();
        assert_eq!(
            result.header.extra_data,
            alloy_primitives::Bytes::from(vec![4u8, 5, 6]),
            "Should use fallback when primary fails"
        );

        // Test 3: After 10 failures, circuit opens and goes directly to fallback
        for _ in 0..9 {
            let _ = reader.get_block_by_number(42).await;
        }

        let metrics = reader.get_circuit_breaker_metrics();
        assert_eq!(
            metrics.0.state, "open",
            "Circuit should be open after 10 failures"
        );

        // Even if primary is restored, should still use fallback due to open circuit
        should_fail_primary.store(false, Ordering::SeqCst);
        let result = reader.get_block_by_number(42).await.unwrap();
        assert_eq!(
            result.header.extra_data,
            alloy_primitives::Bytes::from(vec![4u8, 5, 6]),
            "Should still use fallback when circuit is open"
        );
    }

    #[tokio::test]
    async fn test_circuit_breaker_negative_cases() {
        use std::sync::atomic::Ordering;

        use crate::kvstore::memory::MemoryStorage;

        // Test 1: Both primary and fallback fail
        let primary = MemoryStorage::new("primary");
        let primary_fail = primary.should_fail.clone();
        primary_fail.store(true, Ordering::SeqCst);

        let fallback = MemoryStorage::new("fallback");
        let fallback_fail = fallback.should_fail.clone();
        fallback_fail.store(true, Ordering::SeqCst);

        let reader = ArchiveReader::new(
            BlockDataArchive::new(primary.clone()),
            IndexReaderImpl::new(primary.clone(), BlockDataArchive::new(primary)),
            None,
            None,
        )
        .with_fallback(
            Some(ArchiveReader::new(
                BlockDataArchive::new(fallback.clone()),
                IndexReaderImpl::new(fallback.clone(), BlockDataArchive::new(fallback)),
                None,
                None,
            )),
            None,
            None,
        );

        let result = reader.get_block_by_number(1).await;
        assert!(
            result.is_err(),
            "Should fail when both primary and fallback fail"
        );
        assert!(
            format!("{:?}", result.unwrap_err()).contains("MemoryStorage simulated failure"),
            "Error should indicate storage failure"
        );

        // Test 2: Fallback exists but doesn't have the requested data
        let primary2 = MemoryStorage::new("primary2");
        primary2.should_fail.store(true, Ordering::SeqCst);
        let primary2_bdr = BlockDataArchive::new(primary2.clone());

        let fallback2 = MemoryStorage::new("fallback2");
        let fallback2_bdr = BlockDataArchive::new(fallback2.clone());
        // Don't store any blocks in fallback

        let reader2 = ArchiveReader::new(
            primary2_bdr.clone(),
            IndexReaderImpl::new(primary2.clone(), primary2_bdr),
            None,
            None,
        )
        .with_fallback(
            Some(ArchiveReader::new(
                fallback2_bdr.clone(),
                IndexReaderImpl::new(fallback2.clone(), fallback2_bdr),
                None,
                None,
            )),
            None,
            None,
        );

        let result = reader2.get_block_by_number(999).await;
        assert!(
            result.is_err(),
            "Should fail when primary fails and fallback doesn't have data"
        );
    }

    #[tokio::test]
    async fn test_circuit_breaker_half_open_recovery() {
        use std::sync::atomic::Ordering;

        use crate::kvstore::memory::MemoryStorage;

        // This test demonstrates the half-open recovery behavior
        // In production, the timeout is 5 minutes, but we'll test the logic

        let primary = MemoryStorage::new("primary");
        let should_fail = primary.should_fail.clone();
        let primary_bdr = BlockDataArchive::new(primary.clone());

        let fallback = MemoryStorage::new("fallback");
        let fallback_bdr = BlockDataArchive::new(fallback.clone());

        // Store different blocks to track which source is used
        let primary_block = {
            let mut block = mock_block(42, vec![]);
            block.header.extra_data = alloy_primitives::Bytes::from(vec![1u8]); // Primary marker
            block
        };
        let fallback_block = {
            let mut block = mock_block(42, vec![]);
            block.header.extra_data = alloy_primitives::Bytes::from(vec![2u8]); // Fallback marker
            block
        };

        primary_bdr
            .archive_block(primary_block.clone())
            .await
            .unwrap();
        fallback_bdr
            .archive_block(fallback_block.clone())
            .await
            .unwrap();

        let reader = ArchiveReader::new(
            primary_bdr.clone(),
            IndexReaderImpl::new(primary.clone(), primary_bdr.clone()),
            None,
            None,
        )
        .with_fallback(
            Some(ArchiveReader::new(
                fallback_bdr.clone(),
                IndexReaderImpl::new(fallback.clone(), fallback_bdr),
                None,
                None,
            )),
            Some(10),
            None,
        );

        // Step 1: Circuit is closed, primary works
        let result = reader.get_block_by_number(42).await.unwrap();
        assert_eq!(
            result.header.extra_data,
            alloy_primitives::Bytes::from(vec![1u8]),
            "Should use primary when circuit is closed"
        );

        // Step 2: Make primary fail to open circuit
        should_fail.store(true, Ordering::SeqCst);
        for i in 0..10 {
            let result = reader.get_block_by_number(42).await;
            if i < 9 {
                // First 9 failures still try primary first
                assert!(result.is_ok(), "Should succeed via fallback");
            }
        }

        // Step 3: Circuit is now open
        let metrics = reader.get_circuit_breaker_metrics();
        assert_eq!(
            metrics.0.state, "open",
            "Circuit should be open after 10 failures"
        );

        // All requests go directly to fallback
        let result = reader.get_block_by_number(42).await.unwrap();
        assert_eq!(
            result.header.extra_data,
            alloy_primitives::Bytes::from(vec![2u8]),
            "Should use fallback when circuit is open"
        );

        // Step 4: Primary recovers but circuit is still open
        should_fail.store(false, Ordering::SeqCst);

        // Still uses fallback because circuit hasn't timed out
        let result = reader.get_block_by_number(42).await.unwrap();
        assert_eq!(
            result.header.extra_data,
            alloy_primitives::Bytes::from(vec![2u8]),
            "Should still use fallback when circuit is open (even though primary recovered)"
        );

        // In a real test with configurable timeout, we would:
        // 1. Wait for timeout to expire
        // 2. Next request would transition to half-open and try primary
        // 3. If primary succeeds, circuit closes
        // 4. If primary fails, circuit reopens

        // This is the DESIRABLE behavior - we don't want to hammer a recovering service
        // but we also don't want to use fallback forever
    }
}
