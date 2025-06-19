pub mod block_data_archive;
pub mod index_repr;
// pub mod logs_index;
pub mod logs_index;
pub mod tx_index_archive;

use alloy_primitives::BlockHash;
use alloy_rlp::{RlpDecodable, RlpEncodable};
use enum_dispatch::enum_dispatch;
use eyre::Result;
use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};
use serde::{Deserialize, Serialize};

use crate::{model_v2::ModelV2, prelude::*};

/// Core trait for reading block data from any storage backend.
/// Implementations must be able to retrieve blocks, receipts, and traces.
#[enum_dispatch]
pub trait BlockDataReader: Clone + Sync {
    /// Get the storage bucket/table name
    fn get_replica(&self) -> &str;

    /// Get the latest block number for the given type (uploaded or indexed)
    async fn get_latest(&self) -> Result<Option<u64>>;

    /// Get a block by its hash
    async fn get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Block>;

    /// Get a block by its number
    async fn get_block_by_number(&self, block_num: u64) -> Result<Block>;

    /// Get receipts for a block
    async fn get_block_receipts(&self, block_number: u64) -> Result<BlockReceipts>;

    /// Get execution traces for a block
    async fn get_block_traces(&self, block_number: u64) -> Result<BlockTraces>;
}

#[enum_dispatch]
pub trait BlockArchiver: BlockDataReader {
    fn object_store(&self) -> KVStoreErased;
    async fn update_latest(&self, block_num: u64) -> Result<()>;
    async fn archive_block_data(
        &self,
        block: Block,
        receipts: BlockReceipts,
        traces: BlockTraces,
    ) -> Result<()>;
}

#[enum_dispatch(BlockArchiver, BlockDataReader)]
#[derive(Clone)]
pub enum BlockArchiverErased {
    BlockDataArchive,
    ModelV2,
}

#[enum_dispatch(BlockDataReader)]
#[derive(Clone)]
pub enum BlockDataReaderErased {
    BlockDataArchive,
    ModelV2,
    TriedbReader,
}

/// Byte offsets for transaction data in RLP encoded block components.
/// Used for efficient partial reads without decoding entire blocks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
pub struct TxByteOffsets {
    /// Byte range for transaction in block RLP
    pub tx: RangeRlp,
    /// Byte range for receipt in receipts RLP
    pub receipt: RangeRlp,
    /// Byte range for trace in traces RLP  
    pub trace: RangeRlp,
}

/// Represents a byte range within RLP encoded data
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
pub struct RangeRlp {
    /// Start offset in bytes
    pub start: usize,
    /// End offset in bytes
    pub end: usize,
}

/// Complete indexed data for a single transaction.
/// Used for efficient transaction lookups by hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, RlpEncodable, RlpDecodable)]
#[rlp(trailing)]
pub struct TxIndexedData {
    /// Transaction with recovered sender address
    pub tx: TxEnvelopeWithSender,
    /// Execution trace for this transaction
    pub trace: Vec<u8>,
    /// Transaction receipt with log index
    pub receipt: ReceiptWithLogIndex,
    /// Relevant subset of block header fields
    pub header_subset: HeaderSubset,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, RlpEncodable, RlpDecodable,
)]
#[rlp(trailing)]
/// Subset of block header fields needed to construct rpc types
pub struct HeaderSubset {
    pub block_hash: BlockHash,
    pub block_number: u64,
    /// Timestamp of the block containing this transaction
    pub block_timestamp: u64,
    /// Index of this transaction within the block
    pub tx_index: u64,
    pub gas_used: u128,
    pub base_fee_per_gas: Option<u64>,
}
