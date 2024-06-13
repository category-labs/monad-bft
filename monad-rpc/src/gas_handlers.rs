use std::{ops::Sub, path::Path};

use log::{debug, trace};
use monad_blockdb::BlockTagKey;
use monad_blockdb_utils::BlockDbEnv;
use monad_triedb_utils::{TriedbEnv, TriedbResult};
use reth_primitives::{Transaction, TransactionKind, U256};
use reth_rpc_types::FeeHistory;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    call::{sender_gas_allowance, CallRequest},
    eth_json_types::{
        deserialize_block_tags, deserialize_quantity, serialize_result, BlockTags, Quantity,
    },
    gas_oracle::GasOracle,
    jsonrpc::JsonRpcError,
};

#[derive(Deserialize, Debug)]
struct MonadEthEstimateGasParams {
    tx: CallRequest,
    #[serde(default, deserialize_with = "deserialize_block_tags")]
    block: BlockTags,
}

// TODO: bump reth-primitives to use the setter method from library
fn set_gas_limit(tx: &mut Transaction, gas_limit: u64) {
    match tx {
        Transaction::Legacy(tx) => {
            tx.gas_limit = gas_limit;
        }
        Transaction::Eip2930(tx) => {
            tx.gas_limit = gas_limit;
        }
        Transaction::Eip1559(tx) => {
            tx.gas_limit = gas_limit;
        }
        Transaction::Eip4844(tx) => {
            tx.gas_limit = gas_limit;
        }
    }
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

    params.tx.fill_gas_prices(U256::from(
        block_header.block.base_fee_per_gas.unwrap_or_default(),
    ))?;

    let allowance: Option<u64> = if params.tx.gas.is_none() {
        Some(sender_gas_allowance(&triedb_env, &block_header.block, &params.tx).await?)
    } else {
        None
    };

    if allowance.is_some() {
        params.tx.gas = allowance.map(U256::from);
    };

    let sender = params.tx.from.unwrap_or_default();
    let mut txn: reth_primitives::transaction::Transaction = params.tx.try_into()?;

    if matches!(txn.kind(), TransactionKind::Call(_)) && txn.input().is_empty() {
        return serialize_result(format!("0x{:x}", 21_000));
    }

    let (gas_used, gas_refund) = match monad_cxx::eth_call(
        txn.clone(),
        block_header.block.header.clone(),
        sender,
        block_number,
        triedb_path,
        execution_ledger_path,
    ) {
        monad_cxx::CallResult::Success(monad_cxx::SuccessCallResult {
            gas_used,
            gas_refund,
            ..
        }) => (gas_used, gas_refund),
        monad_cxx::CallResult::Failure(error_message) => {
            return Err(JsonRpcError::eth_call_error(error_message))
        }
    };

    let upper_bound_gas_limit: u64 = txn.gas_limit();
    set_gas_limit(&mut txn, (gas_used + gas_refund) * 64 / 63);

    let (mut lower_bound_gas_limit, mut upper_bound_gas_limit) =
        if txn.gas_limit() < upper_bound_gas_limit {
            match monad_cxx::eth_call(
                txn.clone(),
                block_header.block.header.clone(),
                sender,
                block_number,
                triedb_path,
                execution_ledger_path,
            ) {
                monad_cxx::CallResult::Success(monad_cxx::SuccessCallResult {
                    gas_used, ..
                }) => (gas_used.sub(1), txn.gas_limit()),
                monad_cxx::CallResult::Failure(_error_message) => {
                    (txn.gas_limit(), upper_bound_gas_limit)
                }
            }
        } else {
            (gas_used.sub(1), txn.gas_limit())
        };

    // Binary search for the lowest gas limit.
    while (upper_bound_gas_limit - lower_bound_gas_limit) > 1 {
        // Error ratio from geth https://github.com/ethereum/go-ethereum/blob/c736b04d9b3bec8d9281146490b05075a91e7eea/internal/ethapi/api.go#L57
        if (upper_bound_gas_limit - lower_bound_gas_limit) as f64 / (upper_bound_gas_limit as f64)
            < 0.015
        {
            break;
        }

        let mid = (upper_bound_gas_limit + lower_bound_gas_limit) / 2;

        set_gas_limit(&mut txn, mid);

        match monad_cxx::eth_call(
            txn.clone(),
            block_header.block.header.clone(),
            sender,
            block_number,
            triedb_path,
            execution_ledger_path,
        ) {
            monad_cxx::CallResult::Success(monad_cxx::SuccessCallResult { .. }) => {
                upper_bound_gas_limit = mid;
            }
            monad_cxx::CallResult::Failure(_error_message) => {
                lower_bound_gas_limit = mid;
            }
        };
    }

    serialize_result(format!("0x{:x}", upper_bound_gas_limit))
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
    let base_fee_per_gas = block.block.base_fee_per_gas.unwrap_or_default();

    // Obtain suggested priority fee
    let priority_fee = suggested_priority_fee(blockdb_env)
        .await
        .unwrap_or_default();

    serialize_result(format!("0x{:x}", base_fee_per_gas + priority_fee))
}

#[allow(non_snake_case)]
pub async fn monad_eth_maxPriorityFeePerGas(gas_oracle: &GasOracle) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_maxPriorityFeePerGas");
    let priority_fee = match gas_oracle.tip().await {
        Some(price) => price,
        None => return Err(JsonRpcError::internal_error()),
    };
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
