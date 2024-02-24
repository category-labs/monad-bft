use std::path::Path;

use heed::{flags::Flags, Env, EnvOpenOptions};
use monad_blockdb::{
    BlockNumTableKey, BlockNumTableType, BlockTableKey, BlockTableType, BlockTagTableType,
    BlockTagValue, BlockValue, EthTxKey, EthTxValue, TxnHashTableType, BLOCK_DB_MAP_SIZE,
    BLOCK_DB_NUM_DBS, BLOCK_NUM_TABLE_NAME, BLOCK_TABLE_NAME, BLOCK_TAG_TABLE_NAME,
    TXN_HASH_TABLE_NAME,
};
use tokio::task;

use crate::eth_json_types::BlockTags;

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

            let _ = db_txn.commit();
            data
        })
        .await
        .expect("tokio spawn_blocking should not fail")
    }

    pub async fn get_block_by_hash(&self, key: BlockTableKey) -> Option<BlockValue> {
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

            let _ = db_txn.commit();
            data
        })
        .await
        .expect("tokio spawn_blocking should not fail")
    }

    pub async fn get_block_by_tag(&self, tag: BlockTags) -> Option<BlockValue> {
        let env = self.blockdb_env.clone();
        task::spawn_blocking(move || {
            let block_table_key = match tag {
                BlockTags::Number(q) => {
                    let db: BlockNumTableType = env
                        .open_database(Some(BLOCK_NUM_TABLE_NAME))
                        .expect("block_num_table should exist")
                        .unwrap();
                    let db_txn = env.read_txn().expect("read txn failed");
                    let result: Option<BlockTableKey> = db
                        .get(&db_txn, &BlockNumTableKey(q.0))
                        .expect("get operation should not fail");
                    result
                }
                BlockTags::Default(t) => {
                    let db: BlockTagTableType = env
                        .open_database(Some(BLOCK_TAG_TABLE_NAME))
                        .expect("block_tag_table should exist")
                        .unwrap();
                    let db_txn = env.read_txn().expect("read txn failed");
                    let result: Option<BlockTagValue> =
                        db.get(&db_txn, &t).expect("get operation should not fail");
                    result.map(|r| r.block_hash)
                }
            };

            let db: BlockTableType = env
                .open_database(Some(BLOCK_TABLE_NAME))
                .expect("block_table should exist")
                .unwrap();
            let db_txn = env.read_txn().expect("read txn failed");

            let Some(block_table_key) = block_table_key else {
                return None;
            };

            let block_data: Option<BlockValue> = db
                .get(&db_txn, &block_table_key)
                .expect("get operation should not fail");

            let _ = db_txn.commit();
            block_data
        })
        .await
        .expect("tokio spawn_blocking should not fail")
    }
}
