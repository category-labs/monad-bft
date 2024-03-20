use log::{debug, trace};
use reth_primitives::Transaction;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    blockdb::BlockDbEnv,
    eth_json_types::{deserialize_block_tags, serialize_result, BlockTags},
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

#[allow(non_snake_case)]
pub async fn monad_eth_gasPrice(blockdb_env: &BlockDbEnv) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_gasPrice");

    // TODO: read block data and calculate EIP-1559 base fee + suggested priority fee
    // Refer to <https://github.com/ethereum/pm/issues/328#issuecomment-853234014>
    // Hardcoded as 1 gwei for now
    serialize_result(format!("0x{:x}", 1000000000))
}
