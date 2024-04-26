use std::path::Path;

use heed::{types::SerdeBincode, Database, Env, EnvOpenOptions};
use reth_primitives::{Block, BlockHash, TxHash};
use serde::{Deserialize, Serialize};

pub const BLOCK_DB_MAP_SIZE: usize = 512 * 1000 * 1024 * 1024;
pub const BLOCK_DB_NUM_DBS: u32 = 8;

pub const BLOCK_TABLE_NAME: &str = "blocktable";
pub const BLOCK_NUM_TABLE_NAME: &str = "blocknumtable";
pub const TXN_HASH_TABLE_NAME: &str = "txnhashtable";
pub const BLOCK_TAG_TABLE_NAME: &str = "blocktagtable";
pub const BFT_LEDGER_TABLE_NAME: &str = "bftledgertable";

pub type BlockTableType = Database<SerdeBincode<BlockTableKey>, SerdeBincode<BlockValue>>;
pub type BlockNumTableType = Database<SerdeBincode<BlockNumTableKey>, SerdeBincode<BlockTableKey>>;
pub type TxnHashTableType = Database<SerdeBincode<EthTxKey>, SerdeBincode<EthTxValue>>;
pub type BlockTagTableType = Database<SerdeBincode<BlockTagKey>, SerdeBincode<BlockTagValue>>;
pub type BftLedgerTableType = Database<SerdeBincode<[u8; 32]>, SerdeBincode<Vec<u8>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct EthTxKey(pub TxHash);

#[derive(Debug, Serialize, Deserialize)]
pub struct EthTxValue {
    pub block_hash: BlockTableKey,
    pub transaction_index: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockNumTableKey(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockTableKey(pub BlockHash);

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockValue {
    pub block: Block,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockTagKey {
    Latest,
    Finalized,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockTagValue {
    pub block_hash: BlockTableKey,
}

pub struct BlockDbBuilder {}
impl BlockDbBuilder {
    pub fn create(blockdb_path: &Path) -> BlockDb {
        if let Err(e) = std::fs::create_dir_all(blockdb_path) {
            panic!("blockdb dir failed to open: {e}");
        };

        let blockdb_env = EnvOpenOptions::new()
            .map_size(BLOCK_DB_MAP_SIZE)
            .max_dbs(BLOCK_DB_NUM_DBS)
            .open(blockdb_path)
            .expect("db failed");

        let block_dbi: BlockTableType =
            blockdb_env.create_database(Some(BLOCK_TABLE_NAME)).unwrap();
        let block_num_dbi: BlockNumTableType = blockdb_env
            .create_database(Some(BLOCK_NUM_TABLE_NAME))
            .unwrap();
        let txn_hash_dbi: TxnHashTableType = blockdb_env
            .create_database(Some(TXN_HASH_TABLE_NAME))
            .unwrap();
        let block_tag_dbi: BlockTagTableType = blockdb_env
            .create_database(Some(BLOCK_TAG_TABLE_NAME))
            .unwrap();
        let bft_ledger_dbi: BftLedgerTableType = blockdb_env
            .create_database(Some(BFT_LEDGER_TABLE_NAME))
            .unwrap();

        BlockDb {
            env: blockdb_env,
            block_dbi,
            block_num_dbi,
            txn_hash_dbi,
            block_tag_dbi,
            bft_ledger_dbi,
        }
    }
}

#[derive(Clone)]
pub struct BlockDb {
    block_dbi: BlockTableType,
    block_num_dbi: BlockNumTableType,
    txn_hash_dbi: TxnHashTableType,
    block_tag_dbi: BlockTagTableType,
    bft_ledger_dbi: BftLedgerTableType,
    env: Env,
}

impl BlockDb {
    pub fn write_txn_hashes(&self, block: &Block, block_table_key: &BlockTableKey) {
        let mut txn_hash_table_txn = self.env.write_txn().expect("txn_hash txn create failed");

        for (i, eth_tx) in block.body.iter().enumerate() {
            let key = EthTxKey(eth_tx.recalculate_hash());
            let value = EthTxValue {
                block_hash: block_table_key.clone(),
                transaction_index: i as u64,
            };
            self.txn_hash_dbi
                .put(&mut txn_hash_table_txn, &key, &value)
                .expect("txn_hash_dbi put failed");
        }
        txn_hash_table_txn.commit().expect("txn_hash commit failed");
    }

    pub fn write_block_numbers(&self, block: &Block, block_table_key: &BlockTableKey) {
        let mut block_num_table_txn = self.env.write_txn().expect("block_num txn create failed");
        let block_num_table_key = BlockNumTableKey(block.number);
        let block_num_table_value = block_table_key.clone();
        self.block_num_dbi
            .put(
                &mut block_num_table_txn,
                &block_num_table_key,
                &block_num_table_value,
            )
            .expect("block_num_dbi put failed");
        block_num_table_txn
            .commit()
            .expect("block_num commit failed");
    }

    #[allow(clippy::ptr_arg)]
    pub fn write_full_block(
        &self,
        block: Block,
        block_table_key: &BlockTableKey,
        bft_table_key: [u8; 32],
        bft_table_value: &Vec<u8>,
    ) {
        // eth block and corresponding bft block should be atomically committed
        let mut block_txn = self.env.write_txn().expect("block txn create failed");
        let block_table_key = block_table_key.clone();
        let block_table_value = BlockValue { block };
        self.block_dbi
            .put(&mut block_txn, &block_table_key, &block_table_value)
            .expect("block_dbi put failed");
        self.bft_ledger_dbi
            .put(&mut block_txn, &bft_table_key, bft_table_value)
            .expect("bft_ledger_table put failed");

        block_txn.commit().expect("block_dbi commit failed");

        let mut block_tag_table_txn = self
            .env
            .write_txn()
            .expect("block_tag_table txn create failed");
        let block_tag_value = BlockTagValue {
            block_hash: block_table_key,
        };
        self.block_tag_dbi
            .put(
                &mut block_tag_table_txn,
                &BlockTagKey::Latest,
                &block_tag_value,
            )
            .expect("block_tag_dbi put failed");
        self.block_tag_dbi
            .put(
                &mut block_tag_table_txn,
                &BlockTagKey::Finalized,
                &block_tag_value,
            )
            .expect("block_tag_dbi put failed");
        block_tag_table_txn
            .commit()
            .expect("block_tag commit failed");
    }

    pub fn write_eth_and_bft_blocks(
        &self,
        block: Block,
        bft_block_id: [u8; 32],
        serialized_bft_block: &Vec<u8>,
    ) {
        let block_hash = BlockTableKey(block.header.hash_slow());

        self.write_txn_hashes(&block, &block_hash);
        self.write_block_numbers(&block, &block_hash);
        self.write_full_block(block, &block_hash, bft_block_id, serialized_bft_block);
    }
}
