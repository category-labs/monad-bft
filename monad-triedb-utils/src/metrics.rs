use std::path::PathBuf;

use opentelemetry::{
    metrics::{Histogram, Meter},
    KeyValue,
};

use crate::triedb_env::{
    Account, EthAddress, EthBlockHash, EthCodeHash, EthStorageKey, EthTxHash, Triedb, TriedbPath,
};

// Decorator for Triedb to add metrics
#[derive(Clone)]
pub struct MetricsWrapper<T: Triedb> {
    inner: T,
    metrics: std::sync::Arc<Metrics>,
}

impl<T: Triedb> MetricsWrapper<T> {
    pub fn new(inner: T, meter: Meter) -> Self {
        let metrics = std::sync::Arc::new(Metrics::new(meter));
        Self { inner, metrics }
    }
}

impl<T: Triedb + std::marker::Sync> Triedb for MetricsWrapper<T> {
    async fn get_latest_block(&self) -> Result<u64, String> {
        let time = std::time::Instant::now();
        let res = self.inner.get_latest_block().await;
        let duration = time.elapsed().as_secs_f64();
        self.metrics
            .record_read(duration, "get_latest_block".to_string());
        res
    }

    async fn get_account(&self, addr: EthAddress, block_num: u64) -> Result<Account, String> {
        let time = std::time::Instant::now();
        let res = self.inner.get_account(addr, block_num).await;
        let duration = time.elapsed().as_secs_f64();
        self.metrics
            .record_read(duration, "get_account".to_string());
        res
    }

    async fn get_storage_at(
        &self,
        addr: EthAddress,
        at: EthStorageKey,
        block_num: u64,
    ) -> Result<String, String> {
        let time = std::time::Instant::now();
        let res = self.inner.get_storage_at(addr, at, block_num).await;
        let duration = time.elapsed().as_secs_f64();
        self.metrics
            .record_read(duration, "get_storage_at".to_string());
        res
    }

    async fn get_code(&self, code_hash: EthCodeHash, block_num: u64) -> Result<String, String> {
        let time = std::time::Instant::now();
        let res = self.inner.get_code(code_hash, block_num).await;
        let duration = time.elapsed().as_secs_f64();
        self.metrics.record_read(duration, "get_code".to_string());
        res
    }

    async fn get_receipt(
        &self,
        txn_index: u64,
        block_num: u64,
    ) -> Result<Option<alloy_consensus::ReceiptEnvelope>, String> {
        let time = std::time::Instant::now();
        let res = self.inner.get_receipt(txn_index, block_num).await;
        let duration = time.elapsed().as_secs_f64();
        self.metrics
            .record_read(duration, "get_receipt".to_string());
        res
    }

    async fn get_receipts(
        &self,
        block_num: u64,
    ) -> Result<Vec<alloy_consensus::ReceiptEnvelope>, String> {
        let time = std::time::Instant::now();
        let res = self.inner.get_receipts(block_num).await;
        let duration = time.elapsed().as_secs_f64();
        self.metrics
            .record_read(duration, "get_receipts".to_string());
        res
    }

    async fn get_transaction(
        &self,
        txn_index: u64,
        block_num: u64,
    ) -> Result<Option<alloy_consensus::TxEnvelope>, String> {
        let time = std::time::Instant::now();
        let res = self.inner.get_transaction(txn_index, block_num).await;
        let duration = time.elapsed().as_secs_f64();
        self.metrics
            .record_read(duration, "get_transaction".to_string());
        res
    }

    async fn get_transactions(
        &self,
        block_num: u64,
    ) -> Result<Vec<alloy_consensus::TxEnvelope>, String> {
        let time = std::time::Instant::now();
        let res = self.inner.get_transactions(block_num).await;
        let duration = time.elapsed().as_secs_f64();
        self.metrics
            .record_read(duration, "get_transactions".to_string());
        res
    }

    async fn get_block_header(
        &self,
        block_num: u64,
    ) -> Result<Option<crate::triedb_env::BlockHeader>, String> {
        let time = std::time::Instant::now();
        let res = self.inner.get_block_header(block_num).await;
        let duration = time.elapsed().as_secs_f64();
        self.metrics
            .record_read(duration, "get_block_header".to_string());
        res
    }

    async fn get_transaction_location_by_hash(
        &self,
        tx_hash: EthTxHash,
        block_num: u64,
    ) -> Result<Option<crate::triedb_env::TransactionLocation>, String> {
        let time = std::time::Instant::now();
        let res = self
            .inner
            .get_transaction_location_by_hash(tx_hash, block_num)
            .await;
        let duration = time.elapsed().as_secs_f64();
        self.metrics
            .record_read(duration, "get_transaction_location_by_hash".to_string());
        res
    }

    async fn get_block_number_by_hash(
        &self,
        block_hash: EthBlockHash,
        block_num: u64,
    ) -> Result<Option<u64>, String> {
        let time = std::time::Instant::now();
        let res = self
            .inner
            .get_block_number_by_hash(block_hash, block_num)
            .await;
        let duration = time.elapsed().as_secs_f64();
        self.metrics
            .record_read(duration, "get_block_number_by_hash".to_string());
        res
    }

    async fn get_call_frame(
        &self,
        txn_index: u64,
        block_num: u64,
    ) -> Result<Option<Vec<u8>>, String> {
        let time = std::time::Instant::now();
        let res = self.inner.get_call_frame(txn_index, block_num).await;
        let duration = time.elapsed().as_secs_f64();
        self.metrics
            .record_read(duration, "get_call_frame".to_string());
        res
    }

    async fn get_call_frames(&self, block_num: u64) -> Result<Vec<Vec<u8>>, String> {
        let time = std::time::Instant::now();
        let res = self.inner.get_call_frames(block_num).await;
        let duration = time.elapsed().as_secs_f64();
        self.metrics
            .record_read(duration, "get_call_frames".to_string());
        res
    }
}

impl<T: Triedb + TriedbPath> TriedbPath for MetricsWrapper<T> {
    fn path(&self) -> PathBuf {
        self.inner.path()
    }
}

#[derive(Clone)]
pub struct Metrics {
    read_requests: Histogram<f64>, 
}

impl Metrics {
    pub fn new(meter: Meter) -> Self {
        let read_requests = meter
            .f64_histogram("monad.triedb.read_requests")
            .with_description("Duration of read requests to triedb")
            .with_unit("ms")
            .init();

        Self { read_requests }
    }

    pub fn record_read(&self, duration: f64, method: String) {
        let attributes = vec![KeyValue::new("method", method)];
        self.read_requests.record(duration, &attributes);
    }
}