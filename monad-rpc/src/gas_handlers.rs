use log::{debug, trace};
use monad_blockdb::BlockTagKey;
use reth_primitives::Transaction;
use reth_rpc_types::FeeHistory;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    blockdb::BlockDbEnv,
    eth_json_types::{
        deserialize_block_tags, deserialize_quantity, serialize_result, BlockTags, Quantity,
    },
    jsonrpc::JsonRpcError,
};

#[derive(Deserialize, Debug)]
struct MonadEthEstimateGasParams {
    tx: Transaction,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block: BlockTags, // TODO: this should be made optional
}

#[allow(non_snake_case)]
pub async fn monad_eth_estimateGas(params: Value) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_estimateGas: {params:?}");

    let p: MonadEthEstimateGasParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    // TODO: send transaction to execution client for required gas calculation
    // Hardcoded as 200,000 gas for now
    serialize_result(format!("0x{:x}", 2000000))
}

pub async fn suggested_priority_fee(blockdb_env: &BlockDbEnv) -> Result<u64, JsonRpcError> {
    // TODO: hardcoded as 2 gwei for now, need to implement gas oracle
    // Refer to <https://github.com/ethereum/pm/issues/328#issuecomment-853234014>
    Ok(2000000000)
}

#[allow(non_snake_case)]
pub async fn monad_eth_gasPrice(blockdb_env: &BlockDbEnv) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_gasPrice");

    let block = match blockdb_env
        .get_block_by_tag(BlockTags::Default(BlockTagKey::Latest))
        .await
    {
        Some(block) => block,
        None => {
            debug!("unable to retrieve latest block");
            return Err(JsonRpcError::internal_error());
        }
    };

    // Obtain base fee from latest block header
    let base_fee_per_gas = match block.block.base_fee_per_gas {
        Some(base_fee) => base_fee,
        None => 0,
    };
    // Obtain suggested priority fee
    let priority_fee = suggested_priority_fee(blockdb_env).await.unwrap();

    serialize_result(format!("0x{:x}", base_fee_per_gas + priority_fee))
}

#[allow(non_snake_case)]
pub async fn monad_eth_maxPriorityFeePerGas(
    blockdb_env: &BlockDbEnv,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_maxPriorityFeePerGas");

    let priority_fee = suggested_priority_fee(blockdb_env).await.unwrap();
    serialize_result(format!("0x{:x}", priority_fee))
}

#[derive(Deserialize, Debug)]
struct MonadEthHistoryParams {
    #[serde(deserialize_with = "deserialize_quantity")]
    block_count: Quantity,
    #[serde(deserialize_with = "deserialize_block_tags")]
    newest_block: BlockTags,
    reward_percentiles: Vec<f64>,
}

#[allow(non_snake_case)]
pub async fn monad_eth_feeHistory(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_feeHistory");

    let p: MonadEthHistoryParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let block_count: u64 = p.block_count.0;
    if block_count == 0 {
        return serialize_result(FeeHistory::default());
    }

    // TODO: retrieve fee parameters from historical blocks
    serialize_result(FeeHistory::default())
}
