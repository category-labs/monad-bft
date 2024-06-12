use std::path::Path;

use log::{debug, trace};
use monad_blockdb::BlockTagKey;
use monad_blockdb_utils::BlockDbEnv;
use monad_triedb_utils::{TriedbEnv, TriedbResult};
use reth_rpc_types::FeeHistory;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    call::{sender_gas_allowance, CallRequest},
    eth_json_types::{
        deserialize_block_tags, deserialize_quantity, serialize_result, BlockTags, Quantity,
    },
    jsonrpc::JsonRpcError,
};

#[derive(Deserialize, Debug)]
struct MonadEthEstimateGasParams {
    tx: CallRequest,
    #[serde(default, deserialize_with = "deserialize_block_tags")]
    block: BlockTags,
}

#[allow(non_snake_case)]
pub async fn monad_eth_estimateGas(
    blockdb_env: &BlockDbEnv,
    triedb_path: &Path,
    execution_ledger_path: &Path,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_estimateGas: {params:?}");

    let mut params: MonadEthEstimateGasParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let triedb_env = TriedbEnv::new(triedb_path);

    let block_number = match params.block {
        BlockTags::Default(_) => {
            let TriedbResult::BlockNum(block_number) = triedb_env.get_latest_block().await else {
                debug!("triedb did not have latest block header");
                return Err(JsonRpcError::internal_error());
            };
            block_number
        }
        BlockTags::Number(block_number) => block_number.0,
    };

    let Some(block_header) = blockdb_env
        .get_block_by_tag(monad_blockdb_utils::BlockTags::Number(block_number))
        .await
    else {
        debug!("blockdb did not have latest block header");
        return Err(JsonRpcError::internal_error());
    };

    let allowance = if params.tx.gas.is_none() {
        sender_gas_allowance(&triedb_env, block_number, &params.tx).await?
    } else {
        None
    };

    if allowance.is_some() {
        params.tx.gas = allowance;
    };

    let sender = params.tx.from.unwrap_or_default();
    let txn: reth_primitives::transaction::Transaction = params.tx.try_into()?;

    // TODO: return an optimized gas estimate instead of a single call result.
    // Also, skip the call step if it's a typed transaction we already know.
    match monad_cxx::eth_call(
        txn,
        block_header.block.header,
        sender,
        block_number,
        triedb_path,
        execution_ledger_path,
    ) {
        monad_cxx::CallResult::Success(monad_cxx::SuccessCallResult { gas_used, .. }) => {
            serialize_result(format!("0x{:x}", gas_used))
        }
        monad_cxx::CallResult::Failure(error_message) => {
            Err(JsonRpcError::eth_call_error(error_message))
        }
    }
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
        .get_block_by_tag(monad_blockdb_utils::BlockTags::Default(BlockTagKey::Latest))
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
    let priority_fee = suggested_priority_fee(blockdb_env)
        .await
        .unwrap_or_default();

    serialize_result(format!("0x{:x}", base_fee_per_gas + priority_fee))
}

#[allow(non_snake_case)]
pub async fn monad_eth_maxPriorityFeePerGas(
    blockdb_env: &BlockDbEnv,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_maxPriorityFeePerGas");

    let priority_fee = suggested_priority_fee(blockdb_env)
        .await
        .unwrap_or_default();
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
