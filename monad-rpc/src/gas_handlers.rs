use std::{ops::Sub, path::Path};

use alloy_primitives::{U256, U64};
use monad_cxx::StateOverrideSet;
use monad_rpc_docs::rpc;
use reth_primitives::{Transaction, TransactionKind};
use reth_rpc_types::FeeHistory;
use serde::Deserialize;
use tracing::{debug, trace};

use crate::{
    block_util::{get_block_from_num, get_block_num_from_tag, BlockResult, FileBlockReader},
    call::{sender_gas_allowance, CallRequest},
    eth_json_types::{BlockTags, MonadFeeHistory, Quantity},
    jsonrpc::{JsonRpcError, JsonRpcResult},
    triedb::{TriedbEnv, TriedbResult},
};

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthEstimateGasParams {
    tx: CallRequest,
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

#[rpc(
    method = "eth_estimateGas",
    ignore = "chain_id",
    ignore = "file_ledger_reader"
)]
#[allow(non_snake_case)]
/// Generates and returns an estimate of how much gas is necessary to allow the transaction to complete.
pub async fn monad_eth_estimateGas(
    triedb_env: &TriedbEnv,
    file_ledger_reader: &FileBlockReader,
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
        BlockTags::Latest => {
            let TriedbResult::BlockNum(triedb_block_number) = triedb_env.get_latest_block().await
            else {
                debug!("triedb did not have latest block number");
                return Err(JsonRpcError::internal_error());
            };
            triedb_block_number
        }
        BlockTags::Number(block_number) => block_number.0,
    };

    let block = match get_block_from_num(file_ledger_reader, block_number).await {
        BlockResult::Block(b) => b,
        BlockResult::NotFound => {
            debug!("latest block header not found");
            return Err(JsonRpcError::internal_error());
        }
        BlockResult::DecodeFailed(_) => {
            debug!("block decode failed");
            return Err(JsonRpcError::internal_error());
        }
    };

    params
        .tx
        .fill_gas_prices(U256::from(block.base_fee_per_gas.unwrap_or_default()))?;

    let allowance: Option<u64> = if params.tx.gas.is_none() {
        Some(sender_gas_allowance(triedb_env, &block, &params.tx).await?)
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
        block.header.clone(),
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
                block.header.clone(),
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
            block.header.clone(),
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

pub async fn suggested_priority_fee() -> Result<u64, JsonRpcError> {
    // TODO: hardcoded as 2 gwei for now, need to implement gas oracle
    // Refer to <https://github.com/ethereum/pm/issues/328#issuecomment-853234014>
    Ok(2000000000)
}

#[rpc(method = "eth_gasPrice", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns the current price per gas in wei.
pub async fn monad_eth_gasPrice(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &TriedbEnv,
) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_gasPrice");

    let block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
    let block = match get_block_from_num(file_ledger_reader, block_num).await {
        BlockResult::Block(b) => b,
        BlockResult::NotFound => {
            debug!("unable to retrieve latest block");
            return Err(JsonRpcError::internal_error());
        }
        BlockResult::DecodeFailed(e) => {
            return Err(JsonRpcError::custom(format!("decode block failed: {}", e)))
        }
    };

    // Obtain base fee from latest block header
    let base_fee_per_gas = block.base_fee_per_gas.unwrap_or_default();

    // Obtain suggested priority fee
    let priority_fee = suggested_priority_fee().await.unwrap_or_default();

    Ok(Quantity(base_fee_per_gas + priority_fee))
}

#[rpc(method = "eth_maxPriorityFeePerGas")]
#[allow(non_snake_case)]
/// Returns the current maxPriorityFeePerGas per gas in wei.
pub async fn monad_eth_maxPriorityFeePerGas() -> JsonRpcResult<Quantity> {
    trace!("monad_eth_maxPriorityFeePerGas");

    let priority_fee = suggested_priority_fee().await.unwrap_or_default();
    Ok(Quantity(priority_fee))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthHistoryParams {
    block_count: Quantity,
    newest_block: BlockTags,
    reward_percentiles: Vec<f64>,
}

#[rpc(method = "eth_feeHistory")]
#[allow(non_snake_case)]
/// Transaction fee history
/// Returns transaction base fee per gas and effective priority fee per gas for the requested/supported block range.
pub async fn monad_eth_feeHistory(params: MonadEthHistoryParams) -> JsonRpcResult<MonadFeeHistory> {
    trace!("monad_eth_feeHistory");

    let block_count: u64 = params.block_count.0;
    if block_count == 0 {
        return Ok(MonadFeeHistory(FeeHistory::default()));
    }

    // TODO: retrieve fee parameters from historical blocks
    Ok(MonadFeeHistory(FeeHistory::default()))
}
