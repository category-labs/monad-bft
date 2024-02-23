use std::path::Path;

use heed::{flags::Flags, Env, EnvOpenOptions};
use monad_blockdb::{
    BlockTableKey, BlockTableType, BlockValue, EthTxKey, EthTxValue, TxnHashTableType,
    BLOCK_DB_MAP_SIZE, BLOCK_DB_NUM_DBS, BLOCK_TABLE_NAME, TXN_HASH_TABLE_NAME,
};
use tokio::task;

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
                .map_size(BLOCK_DB_MAP_SIZE)
                .max_dbs(BLOCK_DB_NUM_DBS)
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
