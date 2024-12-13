use std::{
    ops::{Div, Sub},
    path::Path,
};

use alloy_consensus::Transaction as _;
use alloy_primitives::{TxKind, U256, U64};
use alloy_rpc_types::FeeHistory;
use monad_cxx::StateOverrideSet;
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{Triedb, TriedbPath};
use serde::Deserialize;
use tracing::trace;

use crate::{
    block_handlers::get_block_num_from_tag,
    call::{sender_gas_allowance, CallRequest},
    eth_json_types::{BlockTags, MonadFeeHistory, Quantity},
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthEstimateGasParams {
    tx: CallRequest,
    #[serde(default)]
    block: BlockTags,
    #[schemars(skip)] // TODO: move StateOverrideSet from monad-cxx
    #[serde(default)]
    state_override_set: StateOverrideSet,
}

#[rpc(method = "eth_estimateGas", ignore = "chain_id")]
#[allow(non_snake_case)]
/// Generates and returns an estimate of how much gas is necessary to allow the transaction to complete.
pub async fn monad_eth_estimateGas<T: Triedb + TriedbPath>(
    triedb_env: &T,
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

    let block_number = get_block_num_from_tag(triedb_env, params.block).await?;
    let header = match triedb_env
        .get_block_header(block_number)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => {
            return Err(JsonRpcError::internal_error(
                "error getting block header".into(),
            ))
        }
    };

    params.tx.fill_gas_prices(U256::from(
        header.header.base_fee_per_gas.unwrap_or_default(),
    ))?;

    let allowance: Option<u64> = if params.tx.gas.is_none() {
        Some(sender_gas_allowance(triedb_env, &header.header, &params.tx).await?)
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

    if matches!(txn.kind(), TxKind::Call(_)) && txn.input().is_empty() {
        return Ok(Quantity(21_000));
    }

    let (gas_used, gas_refund) = match monad_cxx::eth_call(
        txn.clone(),
        header.header.clone(),
        sender,
        block_number,
        &triedb_env.path(),
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
    txn.set_gas_limit((gas_used + gas_refund) * 64 / 63);

    let (mut lower_bound_gas_limit, mut upper_bound_gas_limit) =
        if txn.gas_limit() < upper_bound_gas_limit {
            match monad_cxx::eth_call(
                txn.clone(),
                header.header.clone(),
                sender,
                block_number,
                &triedb_env.path(),
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

        txn.set_gas_limit(mid);

        match monad_cxx::eth_call(
            txn.clone(),
            header.header.clone(),
            sender,
            block_number,
            &triedb_env.path(),
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

pub async fn suggested_priority_fee() -> Result<u64, JsonRpcError> {
    // TODO: hardcoded as 2 gwei for now, need to implement gas oracle
    // Refer to <https://github.com/ethereum/pm/issues/328#issuecomment-853234014>
    Ok(2000000000)
}

#[rpc(method = "eth_gasPrice")]
#[allow(non_snake_case)]
/// Returns the current price per gas in wei.
pub async fn monad_eth_gasPrice<T: Triedb>(triedb_env: &T) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_gasPrice");

    let block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
    let header = match triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => {
            return Err(JsonRpcError::internal_error(
                "error getting latest block header".into(),
            ))
        }
    };

    // Obtain base fee from latest block header
    let base_fee_per_gas = header.header.base_fee_per_gas.unwrap_or_default();

    // Obtain suggested priority fee
    let priority_fee = suggested_priority_fee().await.unwrap_or_default();

    Ok(Quantity(base_fee_per_gas + priority_fee))
}

#[rpc(method = "eth_maxPriorityFeePerGas")]
#[allow(non_snake_case)]
/// Returns the current maxPriorityFeePerGas per gas in wei.
pub async fn monad_eth_maxPriorityFeePerGas<T: Triedb>(triedb_env: &T) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_maxPriorityFeePerGas");

    let priority_fee = suggested_priority_fee().await.unwrap_or_default();
    Ok(Quantity(priority_fee))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthHistoryParams {
    block_count: Quantity,
    newest_block: BlockTags,
    #[serde(default)]
    reward_percentiles: Option<Vec<f64>>,
}

#[rpc(method = "eth_feeHistory")]
#[allow(non_snake_case)]
/// Transaction fee history
/// Returns transaction base fee per gas and effective priority fee per gas for the requested/supported block range.
pub async fn monad_eth_feeHistory<T: Triedb>(
    triedb_env: &T,
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

    let block_num = get_block_num_from_tag(triedb_env, params.newest_block).await?;
    let header = match triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => {
            return Err(JsonRpcError::internal_error(
                "Unable to retrieve specified block".into(),
            ))
        }
    };

    let base_fee_per_gas = header.header.base_fee_per_gas.unwrap_or_default();
    let gas_used_ratio = (header.header.gas_used as f64).div(header.header.gas_limit as f64);
    let blob_gas_used = header.header.blob_gas_used.unwrap_or_default();
    let blob_gas_used_ratio = (blob_gas_used as f64).div(header.header.gas_limit as f64);

    let reward = match params.reward_percentiles {
        Some(percentiles) => {
            // Check percentiles are between 0-100
            if percentiles.iter().any(|p| *p < 0.0 || *p > 100.0) {
                return Err(JsonRpcError::internal_error(
                    "reward percentiles must be between 0-100".into(),
                ));
            }

            // Check percentiles are sorted
            if !percentiles.windows(2).all(|w| w[0] <= w[1]) {
                return Err(JsonRpcError::internal_error(
                    "reward percentiles must be sorted".into(),
                ));
            }

            if percentiles.is_empty() {
                None
            } else {
                Some(vec![vec![0; percentiles.len()]; block_count as usize])
            }
        }
        None => None,
    };

    // TODO: retrieve fee parameters from historical blocks. For now, return a hacky default
    Ok(MonadFeeHistory(FeeHistory {
        base_fee_per_gas: vec![base_fee_per_gas.into(); (block_count + 1) as usize],
        gas_used_ratio: vec![gas_used_ratio; block_count as usize],
        // TODO: proper calculation of blob fee
        base_fee_per_blob_gas: vec![base_fee_per_gas.into(); (block_count + 1) as usize],
        blob_gas_used_ratio: vec![blob_gas_used_ratio; block_count as usize],
        oldest_block: header.header.number.saturating_sub(block_count),
        reward,
    }))
}
