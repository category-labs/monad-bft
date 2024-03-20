use alloy_primitives::aliases::B256;
use log::{debug, trace};
use monad_blockdb::{BlockTableKey, BlockTagKey, BlockValue};
use reth_primitives::BlockHash;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    blockdb::BlockDbEnv,
    eth_json_types::{
        deserialize_block_tags, deserialize_fixed_data, serialize_result, BlockTags, EthHash,
    },
    jsonrpc::JsonRpcError,
};

#[derive(Serialize, Debug)]
struct BlockObject {
    block_hash: B256,
    block_number: u64,
    size: u64,
    gas_limit: u64,
    gas_used: u64,
    transactions: Vec<B256>, //FIXME: need an enum for either full txns or just hashes
}

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

#[derive(Serialize, Debug)]
struct MonadEthGetBlockReturn {
    block_object: Option<BlockObject>,
}

#[derive(Deserialize, Debug)]
struct MonadEthGetBlockByHashParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    block_hash: EthHash,
    return_full_txns: bool,
}

#[allow(non_snake_case)]
fn parse_block_content(value: BlockValue) -> MonadEthGetBlockReturn {
    //TODO: check value.return_full_txns...
    let block_object = BlockObject {
        block_hash: value.block.hash_slow(),
        block_number: value.block.number,
        size: value.block.size() as u64,
        gas_limit: value.block.gas_limit,
        gas_used: value.block.gas_used,
        transactions: value.block.body.iter().map(|t| t.hash()).collect(),
    };

    MonadEthGetBlockReturn {
        block_object: Some(block_object),
    }
}

#[allow(non_snake_case)]
pub async fn monad_eth_getBlockByHash(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getBlockByHash: {params:?}");

    let p: MonadEthGetBlockByHashParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let key = BlockTableKey(BlockHash::new(p.block_hash.0));
    let Some(value) = blockdb_env.get_block_by_hash(key).await else {
        return serialize_result(MonadEthGetBlockReturn { block_object: None });
    };

    let retval = parse_block_content(value);
    serialize_result(retval)
}

#[derive(Deserialize, Debug)]
struct MonadEthGetBlockByNumberParams {
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_number: BlockTags,
    return_full_txns: bool,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getBlockByNumber(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getBlockByNumber: {params:?}");

    let p: MonadEthGetBlockByNumberParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let Some(value) = blockdb_env.get_block_by_tag(p.block_number).await else {
        return serialize_result(MonadEthGetBlockReturn { block_object: None });
    };

    let retval = parse_block_content(value);
    serialize_result(retval)
}
