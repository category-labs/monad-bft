use alloy_primitives::BlockHash;
use eyre::{eyre, OptionExt, Result};
use monad_triedb_utils::triedb_env::{BlockKey, FinalizedBlockKey, Triedb, TriedbEnv};
use monad_types::SeqNum;

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
    async fn get_latest(&self) -> Result<Option<u64>> {
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

        Ok(make_block(header.header, transactions))
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

    fn get_replica(&self) -> &str {
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
}
