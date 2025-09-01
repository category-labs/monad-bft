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

use alloy_consensus::BlockBody;
use alloy_primitives::BlockHash;
use eyre::{eyre, OptionExt, Result};
use monad_triedb_utils::triedb_env::{BlockKey, FinalizedBlockKey, Triedb, TriedbEnv};
use monad_types::SeqNum;

use super::BlockDataWithOffsets;
use crate::{cli::TrieDbCliArgs, prelude::*};

#[derive(Clone)]
pub struct TriedbReader {
    db: TriedbEnv,
}

impl TriedbReader {
    pub fn new(args: &TrieDbCliArgs) -> TriedbReader {
        Self {
            db: TriedbEnv::new(
                args.triedb_path.as_ref(),
                args.triedb_node_lru_max_mem,
                args.max_buffered_read_requests,
                args.max_triedb_async_read_concurrency,
                args.max_buffered_traverse_requests,
                args.max_triedb_async_traverse_concurrency,
                args.max_finalized_block_cache_len,
                args.max_voted_block_cache_len,
            ),
        }
    }
}

impl BlockDataReader for TriedbReader {
    async fn get_latest(&self, _latest_kind: LatestKind) -> Result<Option<u64>> {
        let seq_num = self.db.get_latest_finalized_block_key().0;
        Ok(Some(seq_num.0))
    }

    async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        let header = self
            .db
            .get_block_header(BlockKey::Finalized(FinalizedBlockKey(SeqNum(block_num))))
            .await
            .map_err(|e| eyre!("{e}"))?
            .ok_or_eyre("Can't find block in triedb")?;

        let transactions = self
            .db
            .get_transactions(BlockKey::Finalized(FinalizedBlockKey(SeqNum(block_num))))
            .await
            .map_err(|e| eyre!("Header exists but not transactions, block {block_num} might be statesynced: {e:?}"))?;

        Ok(Block {
            header: header.header,
            body: BlockBody {
                transactions,
                ommers: Vec::new(),
                withdrawals: Some(alloy_eips::eip4895::Withdrawals::default()),
            },
        })
    }

    async fn get_block_receipts(&self, block_number: u64) -> Result<BlockReceipts> {
        self.db
            .get_receipts(BlockKey::Finalized(FinalizedBlockKey(SeqNum(block_number))))
            .await
            .map_err(|e| eyre!("{e}"))
    }

    async fn get_block_traces(&self, block_number: u64) -> Result<BlockTraces> {
        self.db
            .get_call_frames(BlockKey::Finalized(FinalizedBlockKey(SeqNum(block_number))))
            .await
            .map_err(|e| eyre!("{e}"))
    }

    fn get_bucket(&self) -> &str {
        "TriedbBucket"
    }

    async fn get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Block> {
        let latest_finalized_block = self.db.get_latest_finalized_block_key();
        let block_num = self
            .db
            .get_block_number_by_hash(BlockKey::Finalized(latest_finalized_block), block_hash.0)
            .await
            .map_err(|e| eyre!("{e:?}"))?
            .ok_or_eyre("Block number for hash not found in triedb")?;
        self.get_block_by_number(block_num).await
    }

    async fn get_block_data_with_offsets(&self, block_num: u64) -> Result<BlockDataWithOffsets> {
        let (block, traces, receipts) = try_join!(
            self.get_block_by_number(block_num),
            self.get_block_traces(block_num),
            self.get_block_receipts(block_num),
        )?;

        Ok(BlockDataWithOffsets {
            block,
            traces,
            receipts,
            // Offsets don't make sense when reading from triedb since we will never
            // do sectional reads from that store
            offsets: None,
        })
    }

    #[doc = " Get a block by its number, or return None if not found"]
    async fn try_get_block_by_number(&self, block_num: u64) -> Result<Option<Block>> {
        self.get_block_by_number(block_num).await.map(Some)
    }

    #[doc = " Get receipts for a block, or return None if not found"]
    async fn try_get_block_receipts(&self, block_number: u64) -> Result<Option<BlockReceipts>> {
        self.get_block_receipts(block_number).await.map(Some)
    }

    #[doc = " Get execution traces for a block, or return None if not found"]
    async fn try_get_block_traces(&self, block_number: u64) -> Result<Option<BlockTraces>> {
        self.get_block_traces(block_number).await.map(Some)
    }
}
