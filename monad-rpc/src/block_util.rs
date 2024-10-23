use std::path::PathBuf;

use alloy_rlp::{Decodable, Error as AlloyError};
use monad_types::{BlockId, Hash};
use reth_primitives::Block as EthBlock;
use tokio::{fs::File as TokioFile, io::AsyncReadExt};

use crate::{
    eth_json_types::BlockTags,
    jsonrpc::{JsonRpcError, JsonRpcResult},
    triedb::{Triedb, TriedbResult},
};

pub async fn get_block_num_from_tag<T: Triedb>(
    triedb_env: &T,
    tag: BlockTags,
) -> JsonRpcResult<u64> {
    match tag {
        BlockTags::Number(n) => Ok(n.0),
        BlockTags::Latest => {
            let result = triedb_env.get_latest_block().await;
            let TriedbResult::BlockNum(n) = result else {
                return Err(JsonRpcError::internal_error(
                    "could not get latest block from triedb".to_string(),
                ));
            };
            Ok(n)
        }
    }
}

#[derive(Clone)]
pub struct FileBlockReader {
    eth_block_dir_path: PathBuf,
}

// TODO temp placeholder type, should be replaced when things are in triedb
pub struct TxnValue {
    pub block_hash: BlockId,
    pub transaction_index: u64,
}

impl FileBlockReader {
    pub fn new(eth_block_dir_path: PathBuf) -> Self {
        Self { eth_block_dir_path }
    }

    pub async fn async_read_encoded_eth_block(&self, block_num: u64) -> std::io::Result<Vec<u8>> {
        let filename = block_num.to_string();
        let mut file_path = PathBuf::from(&self.eth_block_dir_path);
        file_path.push(format!("{}", filename));
        let mut file = TokioFile::open(file_path).await?;

        let size = file.metadata().await?.len();
        let mut buf = vec![0; size as usize];
        file.read_exact(&mut buf).await?;

        Ok(buf)
    }

    pub fn decode_eth_block(&self, encoded_block: Vec<u8>) -> Result<EthBlock, AlloyError> {
        EthBlock::decode(&mut &encoded_block[..])
    }

    pub fn get_block_by_hash(&self, id: BlockId) -> Option<EthBlock> {
        None
    }

    pub fn get_txn_by_hash(&self, txn_hash: Hash) -> Option<TxnValue> {
        None
    }
}
