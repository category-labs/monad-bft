use alloy_consensus::{BlockBody, ReceiptEnvelope, TxEnvelope};
use alloy_primitives::BlockHash;
use eyre::{eyre, OptionExt, Result};
use monad_triedb_utils::triedb_env::{BlockHeader, Triedb, TriedbEnv};

use crate::{cli::TrieDbCliArgs, prelude::*};

#[derive(Clone)]
pub struct TriedbReader {
    db: TriedbEnv,
}

impl TriedbReader {
    pub fn new(args: &TrieDbCliArgs) -> TriedbReader {
        Self {
            db: TriedbEnv::new(args.triedb_path.as_ref(), args.max_concurrent_requests),
        }
    }
}

impl BlockDataReader for TriedbReader {
    async fn get_latest(&self, _latest_kind: LatestKind) -> Result<u64> {
        self.db.get_latest_block().await.map_err(|e| eyre!("{e}"))
    }

    async fn get_block_by_number(&self, block_num: u64) -> Result<Block> {
        let header = self
            .db
            .get_block_header(block_num)
            .await
            .map_err(|e| eyre!("{e}"))?
            .ok_or_eyre("Can't find block in triedb")?;

        let txs = self
            .db
            .get_transactions(block_num)
            .await
            .map_err(|e| eyre!("{e}"))?;

        Ok(make_block(header, txs))
    }

    async fn get_block_receipts(&self, block_number: u64) -> Result<Vec<ReceiptEnvelope>> {
        self.db
            .get_receipts(block_number)
            .await
            .map_err(|e| eyre!("{e}"))
    }

    async fn get_block_traces(&self, block_number: u64) -> Result<Vec<Vec<u8>>> {
        self.db
            .get_call_frames(block_number)
            .await
            .map_err(|e| eyre!("{e}"))
    }

    fn get_bucket(&self) -> &str {
        "TriedbBucket"
    }

    async fn get_block_by_hash(&self, block_hash: &BlockHash) -> Result<Block> {
        let latest_block_num = self.db.get_latest_block().await.map_err(|e| eyre!("{e}"))?;
        let block_num = self
            .db
            .get_block_number_by_hash(block_hash.0, latest_block_num)
            .await
            .map_err(|e| eyre!("{e:?}"))?
            .ok_or_eyre("Block number for hash not found in triedb")?;
        self.get_block_by_number(block_num).await
    }
}

pub fn make_block(block_header: BlockHeader, transactions: Vec<TxEnvelope>) -> Block {
    Block {
        header: block_header.header,
        body: BlockBody {
            transactions,
            ommers: Vec::new(),
            withdrawals: None,
        },
    }
}
