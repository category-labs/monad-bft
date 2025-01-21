pub mod cloud_proxy;
pub mod dynamodb;
pub mod fs_storage;
pub mod memory;
pub mod rocksdb_storage;
pub mod s3;
pub mod triedb_reader;

use std::collections::HashMap;

use alloy_consensus::{ReceiptEnvelope, TxEnvelope};
use alloy_primitives::{BlockHash, TxHash};
use alloy_rlp::{RlpDecodable, RlpEncodable};
use bytes::Bytes;
pub use cloud_proxy::*;
pub use dynamodb::*;
use enum_dispatch::enum_dispatch;
use eyre::Result;
use fs_storage::FsStorage;
use memory::MemoryStorage;
pub use rocksdb_storage::*;
pub use s3::*;
use serde::{Deserialize, Serialize};
use tokio_retry::strategy::{jitter, ExponentialBackoff};

use crate::{prelude::*, storage::triedb_reader::TriedbReader};

#[enum_dispatch(BlockDataReader)]
#[derive(Clone)]
pub enum BlockDataReaderErased {
    BlockDataArchive,
    TriedbReader,
}

#[enum_dispatch(BlobReader, BlobStore)]
#[derive(Clone)]
pub enum BlobStoreErased {
    RocksDbClient,
    S3Bucket,
    MemoryStorage,
    FsStorage,
}

#[enum_dispatch(IndexStoreReader, IndexStore)]
#[derive(Clone)]
pub enum IndexStoreErased {
    RocksDbClient,
    DynamoDBArchive,
    MemoryStorage,
    FsStorage,
}

#[enum_dispatch]
pub trait BlockDataReader: Clone {
    fn get_bucket(&self) -> &str;
    async fn get_latest(&self, latest_kind: LatestKind) -> Result<u64>;
    async fn get_block_by_number(&self, block_num: u64) -> Result<Block>;
    async fn get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Block>;
    async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptEnvelope>>;
    async fn get_block_traces(&self, block_number: u64) -> Result<Vec<Vec<u8>>>;
}

#[enum_dispatch]
pub trait BlobStore: BlobReader {
    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<()>;
    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>>;
    fn bucket_name(&self) -> &str;
}
#[enum_dispatch]
pub trait BlobReader: Clone {
    async fn read(&self, key: &str) -> Result<Bytes>;
}

#[enum_dispatch]
pub trait IndexStore: IndexStoreReader {
    async fn bulk_put(&self, kvs: impl Iterator<Item = TxIndexedData>) -> Result<()>;
}

#[enum_dispatch]
pub trait IndexStoreReader: Clone {
    async fn bulk_get(&self, keys: &[TxHash]) -> Result<HashMap<TxHash, TxIndexedData>>;
    async fn get(&self, key: &TxHash) -> Result<Option<TxIndexedData>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
pub struct TxIndexedData {
    pub tx: TxEnvelope,
    pub trace: Vec<u8>,
    pub receipt: ReceiptEnvelope,
    pub header_subset: HeaderSubset,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, RlpEncodable, RlpDecodable,
)]
#[rlp(trailing)]
pub struct HeaderSubset {
    pub block_hash: BlockHash,
    pub block_number: u64,
    pub tx_index: u64,
    pub gas_used: u128,
    pub base_fee_per_gas: Option<u64>,
}

pub fn retry_strategy() -> std::iter::Map<ExponentialBackoff, fn(Duration) -> Duration> {
    ExponentialBackoff::from_millis(10)
        .max_delay(Duration::from_secs(1))
        .map(jitter)
}
