use std::{
    ops::{Div, Sub},
    path::Path,
};

use alloy_primitives::{U256, U64};
use monad_blockdb::BlockTagKey;
use monad_blockdb_utils::BlockDbEnv;
use monad_cxx::StateOverrideSet;
use monad_rpc_docs::rpc;
use reth_primitives::{Transaction, TransactionKind};
use reth_rpc_types::FeeHistory;
use serde::Deserialize;
use tracing::{debug, trace};

use crate::{
    call::{sender_gas_allowance, CallRequest},
    eth_json_types::{
        deserialize_block_tags, deserialize_quantity, BlockTags, MonadFeeHistory, Quantity,
    },
    jsonrpc::{JsonRpcError, JsonRpcResult},
    triedb::{TriedbEnv, TriedbResult},
};

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthEstimateGasParams {
    tx: CallRequest,
    #[serde(default, deserialize_with = "deserialize_block_tags")]
    block: BlockTags,
    #[schemars(skip)] // TODO: move StateOverrideSet from monad-cxx
    #[serde(default)]
    state_override_set: StateOverrideSet,
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

#[rpc(method = "eth_estimateGas", ignore = "chain_id")]
#[allow(non_snake_case)]
/// Generates and returns an estimate of how much gas is necessary to allow the transaction to complete.
pub async fn monad_eth_estimateGas(
    blockdb_env: &BlockDbEnv,
    triedb_env: &TriedbEnv,
    execution_ledger_path: &Path,
    chain_id: u64,
    params: MonadEthEstimateGasParams,
) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_estimateGas: {params:?}");
    let mut params = params;

    params.tx.input.input = match (params.tx.input.input.take(), params.tx.input.data.take()) {
        (Some(input), Some(data)) => {
            if input != data {
                return Err(JsonRpcError::invalid_params());
            }
            Some(input)
        }
        (None, data) | (data, None) => data,
    };

    let state_override_set = &params.state_override_set;

    let block_number = match params.block {
        BlockTags::Default(_) => {
            let TriedbResult::BlockNum(triedb_block_number) = triedb_env.get_latest_block().await
            else {
                debug!("triedb did not have latest block number");
                return Err(JsonRpcError::internal_error());
            };
            let Some(blockdb_block_number) = blockdb_env.get_latest_block().await else {
                debug!("blockdb did not have latest block number");
                return Err(JsonRpcError::internal_error());
            };
            std::cmp::min(triedb_block_number, blockdb_block_number.0)
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
        Some(sender_gas_allowance(triedb_env, &block_header.block, &params.tx).await?)
    } else {
        None
    };

    if allowance.is_some() {
        params.tx.gas = allowance.map(U256::from);
    };

    if params.tx.chain_id.is_none() {
        params.tx.chain_id = Some(U64::from(chain_id));
    }

    let sender = params.tx.from.unwrap_or_default();
    let mut txn: reth_primitives::transaction::Transaction = params.tx.try_into()?;

    if matches!(txn.kind(), TransactionKind::Call(_)) && txn.input().is_empty() {
        return Ok(Quantity(21_000));
    }

    let (gas_used, gas_refund) = match monad_cxx::eth_call(
        txn.clone(),
        block_header.block.header.clone(),
        sender,
        block_number,
        &triedb_env.path(),
        execution_ledger_path,
        state_override_set,
    ) {
        monad_cxx::CallResult::Success(monad_cxx::SuccessCallResult {
            gas_used,
            gas_refund,
            ..
        }) => (gas_used, gas_refund),
        monad_cxx::CallResult::Failure(error) => {
            return Err(JsonRpcError::eth_call_error(error.message, error.data))
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
                &triedb_env.path(),
                execution_ledger_path,
                state_override_set,
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
            &triedb_env.path(),
            execution_ledger_path,
            state_override_set,
        ) {
            monad_cxx::CallResult::Success(monad_cxx::SuccessCallResult { .. }) => {
                upper_bound_gas_limit = mid;
            }
            monad_cxx::CallResult::Failure(_error_message) => {
                lower_bound_gas_limit = mid;
            }
        };
    }

    Ok(Quantity(upper_bound_gas_limit))
}

pub async fn suggested_priority_fee(blockdb_env: &BlockDbEnv) -> Result<u64, JsonRpcError> {
    // TODO: hardcoded as 2 gwei for now, need to implement gas oracle
    // Refer to <https://github.com/ethereum/pm/issues/328#issuecomment-853234014>
    Ok(2000000000)
}

#[rpc(method = "eth_gasPrice")]
#[allow(non_snake_case)]
/// Returns the current price per gas in wei.
pub async fn monad_eth_gasPrice(blockdb_env: &BlockDbEnv) -> JsonRpcResult<Quantity> {
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

    Ok(Quantity(base_fee_per_gas + priority_fee))
}

#[rpc(method = "eth_maxPriorityFeePerGas")]
#[allow(non_snake_case)]
/// Returns the current maxPriorityFeePerGas per gas in wei.
pub async fn monad_eth_maxPriorityFeePerGas(blockdb_env: &BlockDbEnv) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_maxPriorityFeePerGas");

    let priority_fee = suggested_priority_fee(blockdb_env)
        .await
        .unwrap_or_default();
    Ok(Quantity(priority_fee))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthHistoryParams {
    #[serde(deserialize_with = "deserialize_quantity")]
    block_count: Quantity,
    #[serde(deserialize_with = "deserialize_block_tags")]
    newest_block: BlockTags,
    reward_percentiles: Vec<f64>,
}

#[rpc(method = "eth_feeHistory")]
#[allow(non_snake_case)]
/// Transaction fee history
/// Returns transaction base fee per gas and effective priority fee per gas for the requested/supported block range.
pub async fn monad_eth_feeHistory(
    blockdb_env: &BlockDbEnv,
    params: MonadEthHistoryParams,
) -> JsonRpcResult<MonadFeeHistory> {
    trace!("monad_eth_feeHistory");

    // Between 1 and 1024 blocks are supported
    let block_count = params.block_count.0;
    if !(1..=1024).contains(&block_count) {
        return Err(JsonRpcError::custom(
            "block count must be between 1 and 1024".to_string(),
        ));
    }

    let block = match blockdb_env
        .get_block_by_tag(params.newest_block.into())
        .await
    {
        Some(block) => block,
        None => {
            debug!("unable to retrieve latest block");
            return Err(JsonRpcError::internal_error());
        }
    };

    let base_fee_per_gas = block.block.base_fee_per_gas.unwrap_or_default();
    let gas_used_ratio = (block.block.gas_used as f64).div(block.block.gas_limit as f64);

    let reward = if params.reward_percentiles.is_empty() {
        None
    } else {
        Some(vec![
            vec![U256::ZERO; params.reward_percentiles.len()];
            block_count as usize
        ])
    };

    // TODO: retrieve fee parameters from historical blocks. For now, return a hacky default
    Ok(MonadFeeHistory(FeeHistory {
        base_fee_per_gas: vec![U256::from(base_fee_per_gas); (block_count + 1) as usize],
        gas_used_ratio: vec![gas_used_ratio; block_count as usize],
        reward,
        oldest_block: U256::from(block.block.number.saturating_sub(block_count)),
    }))
}
