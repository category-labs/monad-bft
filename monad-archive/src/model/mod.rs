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

pub mod block_data_archive;
pub mod index_repr;
// pub mod logs_index;
pub mod logs_index;
pub mod tx_index_archive;

use alloy_primitives::BlockHash;
use alloy_rlp::{RlpDecodable, RlpEncodable};
use enum_dispatch::enum_dispatch;
use eyre::{OptionExt, Result};
use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};
use serde::{Deserialize, Serialize};

use crate::prelude::*;

/// Core trait for reading block data from any storage backend.
/// Implementations must be able to retrieve blocks, receipts, and traces.
#[enum_dispatch]
pub trait BlockDataReader: Clone {
    /// Get the storage bucket/table name
    fn get_bucket(&self) -> &str;

    /// Get block data along with RLP encoding offsets for efficient partial reads
    async fn get_block_data_with_offsets(&self, block_num: u64) -> Result<BlockDataWithOffsets>;

    /// Get the latest block number for the given type (uploaded or indexed)
    async fn get_latest(&self, latest_kind: LatestKind) -> Result<Option<u64>>;

    /// Get a block by its hash, or return None if not found
    async fn try_get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Option<Block>>;

    /// Get a block by its hash
    async fn get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Block> {
        self.try_get_block_by_hash(block_hash)
            .await
            .and_then(|opt| opt.ok_or_eyre("Block not found"))
    }

    /// Get a block by its number, or return None if not found
    async fn try_get_block_by_number(&self, block_num: u64) -> Result<Option<Block>>;

    /// Get a block by its number
    async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        self.try_get_block_by_number(block_num)
            .await
            .and_then(|opt| opt.ok_or_eyre("Block not found"))
    }

    /// Get receipts for a block, or return None if not found
    async fn try_get_block_receipts(&self, block_number: u64) -> Result<Option<BlockReceipts>>;

    /// Get receipts for a block
    async fn get_block_receipts(&self, block_number: u64) -> Result<BlockReceipts> {
        self.try_get_block_receipts(block_number)
            .await
            .and_then(|opt| opt.ok_or_eyre("Receipt not found"))
    }

    /// Get execution traces for a block, or return None if not found
    async fn try_get_block_traces(&self, block_number: u64) -> Result<Option<BlockTraces>>;

    /// Get execution traces for a block
    async fn get_block_traces(&self, block_number: u64) -> Result<BlockTraces> {
        self.try_get_block_traces(block_number)
            .await
            .and_then(|opt| opt.ok_or_eyre("Traces not found"))
    }
}

#[enum_dispatch(BlockDataReader)]
#[derive(Clone)]
pub enum BlockDataReaderErased {
    BlockDataArchive,
    TriedbReader,
}

/// Complete block data including block, receipts, traces and their RLP encoding offsets.
/// The offsets allow for efficient partial reads of the RLP encoded data.
pub struct BlockDataWithOffsets {
    /// The full block including header and transactions
    pub block: Block,
    /// Transaction receipts with log indices
    pub receipts: BlockReceipts,
    /// Execution traces for each transaction
    pub traces: BlockTraces,
    /// RLP encoding byte offsets for efficient partial reads
    pub offsets: Option<Vec<TxByteOffsets>>,
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
    pub gas_used: u64,
    pub base_fee_per_gas: Option<u64>,
}
