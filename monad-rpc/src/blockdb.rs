use std::path::Path;

use heed::{flags::Flags, Env, EnvOpenOptions};
use monad_blockdb::{
    BlockNumTableKey, BlockNumTableType, BlockTableKey, BlockTableType, BlockTagTableType,
    BlockTagValue, BlockValue, EthTxKey, EthTxValue, TxnHashTableType, BLOCK_DB_MAP_SIZE,
    BLOCK_DB_NUM_DBS, BLOCK_NUM_TABLE_NAME, BLOCK_TABLE_NAME, BLOCK_TAG_TABLE_NAME,
    TXN_HASH_TABLE_NAME,
};

use crate::eth_json_types::BlockTags;

// FIXME: make a blockdb trait

#[derive(Clone)]
pub struct BlockDbEnv {
    blockdb_env: Env,
    block_dbi: BlockTableType,
    block_num_dbi: BlockNumTableType,
    txn_hash_dbi: TxnHashTableType,
    block_tag_dbi: BlockTagTableType,
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
                Ok(env) => {
                    let block_dbi: BlockTableType = env
                        .open_database(Some(BLOCK_TABLE_NAME))
                        .expect("block_dbi should exist")
                        .unwrap();
                    let block_num_dbi: BlockNumTableType = env
                        .open_database(Some(BLOCK_NUM_TABLE_NAME))
                        .expect("block_num_dbi should exist")
                        .unwrap();
                    let txn_hash_dbi: TxnHashTableType = env
                        .open_database(Some(TXN_HASH_TABLE_NAME))
                        .expect("txn_hash_dbi should exist")
                        .unwrap();
                    let block_tag_dbi: BlockTagTableType = env
                        .open_database(Some(BLOCK_TAG_TABLE_NAME))
                        .expect("block_tag_dbi should exist")
                        .unwrap();

                    Some(Self {
                        blockdb_env: env,
                        block_dbi,
                        block_num_dbi,
                        txn_hash_dbi,
                        block_tag_dbi,
                    })
                }
                Err(_) => None,
            }
        }
    }

    pub async fn get_txn(&self, key: EthTxKey) -> Option<EthTxValue> {
        let env = self.blockdb_env.clone();
        let dbi = self.txn_hash_dbi;
        let (send, recv) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {
            let db_txn = env.read_txn().expect("read txn failed");
            let data: Option<EthTxValue> = dbi
                .get(&db_txn, &key)
                .expect("get operation should not fail");

            let _ = db_txn.commit();
            let _ = send.send(data);
        });
        recv.await.expect("rayon panic get_txn")
    }

    pub async fn get_block_by_hash(&self, key: BlockTableKey) -> Option<BlockValue> {
        let env = self.blockdb_env.clone();
        let dbi = self.block_dbi;
        let (send, recv) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {
            let db_txn = env.read_txn().expect("read txn failed");
            let data: Option<BlockValue> = dbi
                .get(&db_txn, &key)
                .expect("get operation should not fail");

            let _ = db_txn.commit();
            let _ = send.send(data);
        });
        recv.await.expect("rayon panic get_block_by_hash")
    }

    pub async fn get_block_by_tag(&self, tag: BlockTags) -> Option<BlockValue> {
        let env = self.blockdb_env.clone();
        let block_num_dbi = self.block_num_dbi;
        let block_tag_dbi = self.block_tag_dbi;
        let block_dbi = self.block_dbi;
        let (send, recv) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {
            let block_table_key = match tag {
                BlockTags::Number(q) => {
                    let db_txn = env.read_txn().expect("read txn failed");
                    let result: Option<BlockTableKey> = block_num_dbi
                        .get(&db_txn, &BlockNumTableKey(q.0))
                        .expect("get operation should not fail");
                    result
                }
                BlockTags::Default(t) => {
                    let db_txn = env.read_txn().expect("read txn failed");
                    let result: Option<BlockTagValue> = block_tag_dbi
                        .get(&db_txn, &t)
                        .expect("get operation should not fail");
                    result.map(|r| r.block_hash)
                }
            };

            let db_txn = env.read_txn().expect("read txn failed");

            let Some(block_table_key) = block_table_key else {
                let _ = send.send(None);
                return;
            };

            let block_data: Option<BlockValue> = block_dbi
                .get(&db_txn, &block_table_key)
                .expect("get operation should not fail");

            let _ = db_txn.commit();
            let _ = send.send(block_data);
        });
        recv.await.expect("rayon panic get_block_by_tag")
    }
}
