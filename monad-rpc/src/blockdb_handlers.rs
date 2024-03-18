use log::trace;
use monad_blockdb::BlockTagKey;
use serde::Serialize;
use serde_json::Value;

use crate::{
    blockdb::BlockDbEnv,
    eth_json_types::{serialize_result, BlockTags},
    jsonrpc::JsonRpcError,
};

#[allow(non_snake_case)]
pub async fn monad_eth_blockNumber(blockdb_env: &BlockDbEnv) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_blockNumber");

    let Some(block) = blockdb_env
        .get_block_by_tag(BlockTags::Default(BlockTagKey::Latest))
        .await
    else {
        return serialize_result(format!("0x{:x}", 0));
    };

    serialize_result(format!("0x{:x}", block.block.number))
}

// TODO: does chainId come from a config file?
#[allow(non_snake_case)]
pub async fn monad_eth_chainId(blockdb_env: &BlockDbEnv) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_chainId");

    serialize_result(format!("0x{:x}", 1337))
}
