use heed::{types::SerdeBincode, Database};
use reth_primitives::{Block, BlockHash, TxHash};
use serde::{Deserialize, Serialize};

pub const BLOCK_DB_MAP_SIZE: usize = 512 * 1000 * 1024 * 1024;
pub const BLOCK_DB_NUM_DBS: u32 = 8;

pub const BLOCK_TABLE_NAME: &str = "blocktable";
pub const BLOCK_NUM_TABLE_NAME: &str = "blocknumtable";
pub const TXN_HASH_TABLE_NAME: &str = "txnhashtable";
pub const BLOCK_TAG_TABLE_NAME: &str = "blocktagtable";

pub type BlockTableType = Database<SerdeBincode<BlockTableKey>, SerdeBincode<BlockValue>>;
pub type BlockNumTableType = Database<SerdeBincode<BlockNumTableKey>, SerdeBincode<BlockTableKey>>;
pub type TxnHashTableType = Database<SerdeBincode<EthTxKey>, SerdeBincode<EthTxValue>>;
pub type BlockTagTableType = Database<SerdeBincode<BlockTagKey>, SerdeBincode<BlockTagValue>>;

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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockTagKey {
    Latest,
    Finalized,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockTagValue {
    pub block_hash: BlockTableKey,
}
