use std::path::Path;

use heed::{flags::Flags, types::SerdeBincode, Database, Env, EnvOpenOptions};
use reth_primitives::{Block, BlockHash, TxHash};
use serde::{Deserialize, Serialize};
use tokio::task;

// FIXME: types are duped with writer to blockdb; move this crate into monad-bft workspace so the
// types can be in a common blockdb crate

const BLOCK_TABLE_NAME: &str = "blocktable";
const BLOCK_NUM_TABLE_NAME: &str = "blocknumtable";
const TXN_HASH_TABLE_NAME: &str = "txnhashtable";
// TODO const BLOCK_TAG_TABLE_NAME: &str = "blocktagtable";

type BlockTableType = Database<SerdeBincode<BlockTableKey>, SerdeBincode<BlockValue>>;
type BlockNumTableType = Database<SerdeBincode<BlockNumTableKey>, SerdeBincode<BlockTableKey>>;
type TxnHashTableType = Database<SerdeBincode<EthTxKey>, SerdeBincode<EthTxValue>>;
// TODO type BlockTagTable...

#[derive(Debug, Serialize, Deserialize)]
pub struct EthTxKey(pub TxHash);

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockNumTableKey(pub u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockTableKey(pub BlockHash);

#[derive(Debug, Serialize, Deserialize)]
pub struct EthTxValue {
    pub block_hash: BlockTableKey,
    pub transaction_index: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockValue {
    pub block: Block,
}

// FIXME: make a blockdb trait

#[derive(Clone)]
pub struct BlockDbEnv {
    blockdb_env: Env,
}

impl BlockDbEnv {
    pub fn new(blockdb_path: &Path) -> Option<Self> {
        // unsafe because of the flag function for lmdb options
        unsafe {
            let blockdb_env = EnvOpenOptions::new()
                .map_size(512 * 1000 * 1024 * 1024)
                .max_dbs(8)
                .flag(Flags::MdbNoTls) // disable TLS because tokio tasks can move threads
                .flag(Flags::MdbRdOnly) // we should never have to write to the blockdb from rpc
                .open(blockdb_path);

            match blockdb_env {
                Ok(env) => Some(Self { blockdb_env: env }),
                Err(_) => None,
            }
        }
    }

    pub async fn get_txn(&self, key: EthTxKey) -> Option<EthTxValue> {
        let env = self.blockdb_env.clone();
        task::spawn_blocking(move || {
            let db: TxnHashTableType = env
                .open_database(Some(TXN_HASH_TABLE_NAME))
                .expect("txn_hash_table should exist")
                .unwrap();
            let db_txn = env.read_txn().expect("read txn failed");
            let data: Option<EthTxValue> = db
                .get(&db_txn, &key)
                .expect("get operation should not fail");

            data
        })
        .await
        .expect("tokio spawn_blocking should not fail")
    }

    pub async fn get_block(&self, key: BlockTableKey) -> Option<BlockValue> {
        let env = self.blockdb_env.clone();
        task::spawn_blocking(move || {
            let db: BlockTableType = env
                .open_database(Some(BLOCK_TABLE_NAME))
                .expect("block_table should exist")
                .unwrap();
            let db_txn = env.read_txn().expect("read txn failed");
            let data: Option<BlockValue> = db
                .get(&db_txn, &key)
                .expect("get operation should not fail");

            data
        })
        .await
        .expect("tokio spawn_blocking should not fail")
    }
}
