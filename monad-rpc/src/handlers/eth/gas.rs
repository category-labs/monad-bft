// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::ops::{Div, Sub};

use alloy_consensus::{
    Header, SignableTransaction, Transaction, TxEip1559, TxEip2930, TxEip7702, TxEnvelope, TxLegacy,
};
use alloy_eips::eip2718::{
    EIP1559_TX_TYPE_ID, EIP2930_TX_TYPE_ID, EIP4844_TX_TYPE_ID, EIP7702_TX_TYPE_ID,
    LEGACY_TX_TYPE_ID,
};
use alloy_primitives::{Address, Signature, TxKind, U256, U64, U8};
use alloy_rpc_types::{FeeHistory, TransactionReceipt};
use futures::stream::StreamExt;
use itertools::Itertools;
use monad_ethcall::{
    CallResult, EthCallExecutor, EthCallRequest, MonadTracer, StateOverrideObject, StateOverrideSet,
};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{BlockKey, FinalizedBlockKey, ProposedBlockKey, Triedb};
use monad_types::{BlockId, Hash, SeqNum};
use serde::Deserialize;
use tracing::trace;

use crate::{
    data::{
        eth_call_handler::EthCallHandlerConfig, get_block_key_from_tag,
        get_block_key_from_tag_or_hash, DataProvider,
    },
    handlers::{
        eth::call::{check_contract_creation_size, fill_gas_params, CallRequest, GasPriceDetails},
        parse_ethcall_chain_id,
    },
    types::{
        eth_json::{
            BlockTagOrHash, BlockTags, FillTransactionResult, MonadFeeHistory, Quantity,
            UnformattedData,
        },
        jsonrpc::{JsonRpcError, JsonRpcResult},
    },
};

/// Additional gas added during a CALL.
const CALL_STIPEND: u64 = 2_300;

fn block_key_to_parts(block_key: BlockKey) -> (u64, Option<[u8; 32]>) {
    match block_key {
        BlockKey::Finalized(FinalizedBlockKey(SeqNum(n))) => (n, None),
        BlockKey::Proposed(ProposedBlockKey(SeqNum(n), BlockId(Hash(id)))) => (n, Some(id)),
    }
}

#[derive(Clone, Copy)]
enum EstimateGasMode {
    // Used by eth_estimateGas, which should keep searching until the simulated
    // transaction executes successfully.
    RequireSuccess,
    // Used by eth_fillTransaction, which only needs a gas limit large enough to
    // execute without running out of gas, even if execution still fails.
    AllowExecutionFailure,
}

fn is_terminal_estimate_result(
    mode: EstimateGasMode,
    error: &monad_ethcall::FailureCallResult,
) -> bool {
    matches!(mode, EstimateGasMode::AllowExecutionFailure)
        && matches!(
            error.error_code,
            monad_ethcall::EthCallResult::ExecutionError
                | monad_ethcall::EthCallResult::ReserveBalanceViolation
        )
}

async fn estimate_gas(
    eth_call_fn: impl AsyncFn(&TxEnvelope) -> CallResult,
    call_request: &mut CallRequest,
    original_tx_gas: U256,
    provider_gas_limit: u64,
    protocol_gas_limit: u64,
    mode: EstimateGasMode,
) -> Result<Quantity, JsonRpcError> {
    estimate_gas_with_builder(
        eth_call_fn,
        call_request,
        original_tx_gas,
        provider_gas_limit,
        protocol_gas_limit,
        mode,
        |request| request.clone().try_into(),
    )
    .await
}

async fn estimate_gas_with_builder(
    eth_call_fn: impl AsyncFn(&TxEnvelope) -> CallResult,
    call_request: &mut CallRequest,
    original_tx_gas: U256,
    provider_gas_limit: u64,
    protocol_gas_limit: u64,
    mode: EstimateGasMode,
    build_tx: impl Fn(&CallRequest) -> Result<TxEnvelope, JsonRpcError> + Copy,
) -> Result<Quantity, JsonRpcError> {
    let mut txn = build_tx(call_request)?;

    let (gas_used, gas_refund) = match eth_call_fn(&txn).await {
        monad_ethcall::CallResult::Success(monad_ethcall::SuccessCallResult {
            gas_used,
            gas_refund,
            ..
        }) => (gas_used, gas_refund),
        monad_ethcall::CallResult::Failure(error) if is_terminal_estimate_result(mode, &error) => {
            (error.gas_used, error.gas_refund)
        }
        monad_ethcall::CallResult::Failure(error) => match error.error_code {
            monad_ethcall::EthCallResult::OutOfGas => {
                if provider_gas_limit < protocol_gas_limit
                    && U256::from(provider_gas_limit) < original_tx_gas
                {
                    return Err(JsonRpcError::eth_call_error(
                        "provider-specified eth_estimateGas gas limit exceeded".to_string(),
                        error.data,
                    ));
                }
                return Err(JsonRpcError::eth_call_error(
                    "out of gas".to_string(),
                    error.data,
                ));
            }
            _ => return Err(JsonRpcError::eth_call_error(error.message, error.data)),
        },
        _ => {
            return Err(JsonRpcError::internal_error(
                "Unexpected CallResult type".into(),
            ));
        }
    };

    let upper_bound_gas_limit = txn.gas_limit();
    // Set gas to used + refund + call stipend and apply the 63/64 rule
    call_request.gas = Some(U256::from((gas_used + gas_refund + CALL_STIPEND) * 64 / 63));
    txn = build_tx(call_request)?;

    let (mut lower_bound_gas_limit, mut upper_bound_gas_limit) =
        if txn.gas_limit() < upper_bound_gas_limit {
            match eth_call_fn(&txn).await {
                monad_ethcall::CallResult::Success(monad_ethcall::SuccessCallResult {
                    gas_used,
                    ..
                }) => (gas_used.sub(1), txn.gas_limit()),
                monad_ethcall::CallResult::Failure(error)
                    if is_terminal_estimate_result(mode, &error) =>
                {
                    (error.gas_used.sub(1), txn.gas_limit())
                }
                monad_ethcall::CallResult::Failure(_error_message) => {
                    (txn.gas_limit(), upper_bound_gas_limit)
                }
                _ => {
                    return Err(JsonRpcError::internal_error(
                        "Unexpected CallResult type".into(),
                    ));
                }
            }
        } else {
            (gas_used.sub(1), upper_bound_gas_limit)
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

        call_request.gas = Some(U256::from(mid));
        txn = build_tx(call_request)?;

        match eth_call_fn(&txn).await {
            monad_ethcall::CallResult::Success(monad_ethcall::SuccessCallResult { .. }) => {
                upper_bound_gas_limit = mid;
            }
            monad_ethcall::CallResult::Failure(error)
                if is_terminal_estimate_result(mode, &error) =>
            {
                upper_bound_gas_limit = mid;
            }
            monad_ethcall::CallResult::Failure(_error_message) => {
                lower_bound_gas_limit = mid;
            }
            _ => {
                return Err(JsonRpcError::internal_error(
                    "Unexpected CallResult type".into(),
                ));
            }
        };
    }

    Ok(Quantity(upper_bound_gas_limit))
}

fn requires_eip2930_encoding(tx: &CallRequest) -> bool {
    matches!(tx.gas_price_details, GasPriceDetails::Legacy { .. })
        && match tx.transaction_type {
            Some(transaction_type) => transaction_type == U8::from(EIP2930_TX_TYPE_ID),
            None => tx.access_list.is_some(),
        }
}

fn validate_fill_transaction_type(tx: &CallRequest) -> Result<(), JsonRpcError> {
    let Some(transaction_type) = tx.transaction_type else {
        return Ok(());
    };

    let expected_type = match (
        &tx.gas_price_details,
        &tx.authorization_list,
        &tx.access_list,
    ) {
        (GasPriceDetails::Legacy { .. }, _, access_list) if access_list.is_some() => {
            Some(U8::from(EIP2930_TX_TYPE_ID))
        }
        (GasPriceDetails::Legacy { .. }, _, _) => Some(U8::from(LEGACY_TX_TYPE_ID)),
        (GasPriceDetails::Eip1559 { .. }, Some(_), _) => Some(U8::from(EIP7702_TX_TYPE_ID)),
        (GasPriceDetails::Eip1559 { .. }, None, _) => Some(U8::from(EIP1559_TX_TYPE_ID)),
    };

    if expected_type == Some(transaction_type) {
        return Ok(());
    }

    Err(JsonRpcError::invalid_params())
}

fn build_fill_transaction_eip2930(
    tx: &CallRequest,
    chain_id: u64,
) -> Result<TxEip2930, JsonRpcError> {
    if tx.to.is_none() {
        check_contract_creation_size(tx.input.input.as_ref())?;
    }

    let GasPriceDetails::Legacy { gas_price } = tx.gas_price_details else {
        unreachable!("EIP-2930 fill transaction builder requires legacy gas price details");
    };

    Ok(TxEip2930 {
        chain_id,
        nonce: tx
            .nonce
            .unwrap_or_default()
            .try_into()
            .map_err(|_| JsonRpcError::invalid_params())?,
        gas_price: gas_price
            .try_into()
            .map_err(|_| JsonRpcError::invalid_params())?,
        gas_limit: tx
            .gas
            .unwrap_or_default()
            .try_into()
            .map_err(|_| JsonRpcError::invalid_params())?,
        to: if let Some(to) = tx.to {
            TxKind::Call(to)
        } else {
            TxKind::Create
        },
        value: tx.value.unwrap_or_default(),
        access_list: tx.access_list.clone().unwrap_or_default(),
        input: tx.input.input.clone().unwrap_or_default(),
    })
}

fn build_fill_transaction_envelope(
    tx: &CallRequest,
    chain_id: u64,
) -> Result<TxEnvelope, JsonRpcError> {
    if !requires_eip2930_encoding(tx) {
        return tx.clone().try_into();
    }

    let signature = Signature::new(U256::from(0), U256::from(0), false);
    let transaction = build_fill_transaction_eip2930(tx, chain_id)?;

    Ok(transaction.into_signed(signature).into())
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthEstimateGasParams {
    tx: CallRequest,
    #[serde(default)]
    block: BlockTagOrHash,
    #[schemars(skip)] // TODO: move StateOverrideSet from monad-cxx
    #[serde(default)]
    state_override_set: StateOverrideSet,
}

#[rpc(
    method = "eth_estimateGas",
    ignore = "eth_call_handler_config",
    ignore = "eth_call_executor",
    ignore = "chain_id"
)]
#[allow(non_snake_case)]
/// Generates and returns an estimate of how much gas is necessary to allow the transaction to complete.
pub async fn monad_eth_estimateGas<T: Triedb>(
    data_provider: &DataProvider<T>,
    eth_call_handler_config: &EthCallHandlerConfig,
    eth_call_executor: &EthCallExecutor,
    chain_id: u64,
    params: MonadEthEstimateGasParams,
) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_estimateGas: {params:?}");

    let MonadEthEstimateGasParams {
        mut tx,
        block,
        state_override_set,
    } = params;

    tx.input.input = match (tx.input.input.take(), tx.input.data.take()) {
        (Some(input), Some(data)) => {
            if input != data {
                return Err(JsonRpcError::invalid_params());
            }
            Some(input)
        }
        (None, data) | (data, None) => data,
    };

    if tx.gas
        > Some(U256::from(
            eth_call_handler_config.provider_gas_limit_eth_estimate_gas,
        ))
    {
        return Err(JsonRpcError::eth_call_error(
            "user-specified gas exceeds provider limit".to_string(),
            None,
        ));
    }

    let block_key = get_block_key_from_tag_or_hash(&data_provider.triedb_env, block)
        .await
        .ok_or_else(JsonRpcError::block_not_found)?;

    let mut header = match data_provider
        .triedb_env
        .get_block_header(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => return Err(JsonRpcError::block_not_found()),
    };

    let gas_specified = tx.gas.is_some();
    let provider_gas_limit = eth_call_handler_config
        .provider_gas_limit_eth_estimate_gas
        .min(header.header.gas_limit);
    let original_tx_gas = tx.gas.unwrap_or(U256::from(header.header.gas_limit));
    fill_gas_params(
        &data_provider.triedb_env,
        block_key,
        &mut tx,
        &mut header.header,
        &state_override_set,
        U256::from(provider_gas_limit),
    )
    .await?;

    if let Some(tx_chain_id) = tx.chain_id {
        if tx_chain_id != U64::from(chain_id) {
            return Err(JsonRpcError::invalid_chain_id(
                chain_id,
                tx_chain_id.to::<u64>(),
            ));
        }
    } else {
        tx.chain_id = Some(U64::from(chain_id));
    }

    let sender = tx.from.unwrap_or_default();
    let tx_chain_id = tx.chain_id.expect("chain id must be populated").to::<u64>();
    let ethcall_chain_id = parse_ethcall_chain_id(tx_chain_id)?;
    let protocol_gas_limit = header.header.gas_limit;
    let (block_number, block_id) = block_key_to_parts(block_key);

    let eth_call_fn = async |transaction: &TxEnvelope| {
        monad_ethcall::eth_call(
            EthCallRequest {
                chain_id: ethcall_chain_id,
                transaction,
                block_header: &header.header,
                sender,
                block_number,
                block_id,
                state_override_set: &state_override_set,
                tracer: MonadTracer::NoopTracer,
                gas_specified,
            },
            eth_call_executor,
        )
        .await
    };

    // If the transaction is a regular value transfer, execute the transaction with a 21000 gas limit and return that gas limit if executes successfully.
    // Returning 21000 without execution is risky since some transaction field combinations can increase the price even for regular transfers.
    if let Some(to) = tx.to {
        if tx.input.input.as_ref().is_none_or(|b| b.is_empty())
            && data_provider
                .triedb_env
                .get_account(block_key, to.into())
                .await
                .is_ok_and(|acct| acct.code_hash.is_none())
        {
            let saved_gas = tx.gas;
            tx.gas = Some(U256::from(21_000));
            let txn: TxEnvelope = tx.clone().try_into()?;
            tx.gas = saved_gas;

            if matches!(
                eth_call_fn(&txn).await,
                monad_ethcall::CallResult::Success(_)
            ) {
                return Ok(Quantity(21_000));
            }
        }
    }

    estimate_gas(
        eth_call_fn,
        &mut tx,
        original_tx_gas,
        provider_gas_limit,
        protocol_gas_limit,
        EstimateGasMode::RequireSuccess,
    )
    .await
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MonadEthFillTransactionParams {
    pub tx: CallRequest,
}

#[rpc(
    method = "eth_fillTransaction",
    ignore = "eth_call_handler_config",
    ignore = "eth_call_executor",
    ignore = "chain_id"
)]
#[allow(non_snake_case)]
pub async fn monad_eth_fillTransaction<T: Triedb>(
    data_provider: &DataProvider<T>,
    eth_call_handler_config: &EthCallHandlerConfig,
    eth_call_executor: &EthCallExecutor,
    chain_id: u64,
    params: MonadEthFillTransactionParams,
) -> JsonRpcResult<FillTransactionResult> {
    let ethcall_chain_id = parse_ethcall_chain_id(chain_id)?;
    let from = params.tx.from.unwrap_or_default();
    let state_override = fill_transaction_state_overrides(from);

    let eth_call_fn =
        async |header: &Header, from: Address, block_key: BlockKey, transaction: &TxEnvelope| {
            let (block_number, block_id) = block_key_to_parts(block_key);
            monad_ethcall::eth_call(
                EthCallRequest {
                    chain_id: ethcall_chain_id,
                    transaction,
                    block_header: header,
                    sender: from,
                    block_number,
                    block_id,
                    state_override_set: &state_override,
                    tracer: MonadTracer::NoopTracer,
                    gas_specified: true,
                },
                eth_call_executor,
            )
            .await
        };

    fill_transaction_with_provider(
        data_provider,
        eth_call_handler_config,
        chain_id,
        params,
        eth_call_fn,
    )
    .await
}

fn fill_transaction_state_overrides(from: Address) -> StateOverrideSet {
    let mut state_overrides = StateOverrideSet::default();
    state_overrides.insert(
        from,
        StateOverrideObject {
            balance: Some(U256::MAX),
            ..Default::default()
        },
    );
    state_overrides
}

async fn fill_transaction_with_provider<T: Triedb>(
    data_provider: &DataProvider<T>,
    eth_call_handler_config: &EthCallHandlerConfig,
    chain_id: u64,
    params: MonadEthFillTransactionParams,
    eth_call_fn: impl AsyncFn(&Header, Address, BlockKey, &TxEnvelope) -> CallResult,
) -> JsonRpcResult<FillTransactionResult> {
    trace!("monad_eth_fillTransaction: {params:?}");

    let mut tx = params.tx;
    normalize_fill_transaction_request(&mut tx)?;

    let from = tx.from.ok_or_else(JsonRpcError::invalid_params)?;

    if tx.value.is_none() {
        tx.value = Some(U256::ZERO);
    }

    tx.chain_id = Some(U64::from(chain_id));

    let header = data_provider
        .get_block_header(BlockTagOrHash::BlockTags(BlockTags::Latest))
        .await
        .map_err(|_| JsonRpcError::block_not_found())?;

    let block_key = get_block_key_from_tag(&data_provider.triedb_env, BlockTags::Latest)
        .ok_or(JsonRpcError::block_not_found())?;

    if tx.nonce.is_none() {
        let account = data_provider
            .triedb_env
            .get_account(block_key, from.into())
            .await
            .map_err(JsonRpcError::internal_error)?;
        tx.nonce = Some(U64::from(account.nonce));
    }

    let base_fee = U256::from(header.base_fee_per_gas.unwrap_or_default());
    let (fill_base_fee, suggested_priority_fee) = match &tx.gas_price_details {
        GasPriceDetails::Legacy { .. } => (base_fee, None),
        GasPriceDetails::Eip1559 { .. } => (
            base_fee.saturating_mul(U256::from(3)) / U256::from(2),
            Some(U256::from(
                suggested_priority_fee().await.unwrap_or_default(),
            )),
        ),
    };
    fill_transaction_gas_price_details(&mut tx, base_fee, fill_base_fee, suggested_priority_fee)?;

    if tx.gas.is_none() {
        let protocol_gas_limit = header.gas_limit;
        let eth_call_provider_gas_limit = eth_call_handler_config
            .provider_gas_limit_eth_estimate_gas
            .min(protocol_gas_limit);
        let original_tx_gas = U256::from(protocol_gas_limit);

        tx.gas = Some(U256::from(eth_call_provider_gas_limit));

        let estimate_eth_call_fn = async |transaction: &TxEnvelope| {
            eth_call_fn(&header, from, block_key, transaction).await
        };

        let Quantity(estimated_gas) = estimate_gas_with_builder(
            estimate_eth_call_fn,
            &mut tx,
            original_tx_gas,
            eth_call_provider_gas_limit,
            protocol_gas_limit,
            EstimateGasMode::AllowExecutionFailure,
            |request| build_fill_transaction_envelope(request, chain_id),
        )
        .await?;

        tx.gas = Some(U256::from(estimated_gas));
    }

    let (raw, filled_tx) = build_unsigned_transaction(&tx, chain_id)?;

    Ok(FillTransactionResult { raw, tx: filled_tx })
}

fn normalize_fill_transaction_request(tx: &mut CallRequest) -> Result<(), JsonRpcError> {
    tx.input.input = match (tx.input.input.take(), tx.input.data.take()) {
        (Some(input), Some(data)) => {
            if input != data {
                return Err(JsonRpcError::invalid_params());
            }
            Some(input)
        }
        (None, data) | (data, None) => data,
    };

    if tx.transaction_type == Some(U8::from(EIP4844_TX_TYPE_ID))
        || tx.max_fee_per_blob_gas.is_some()
        || tx.blob_versioned_hashes.is_some()
    {
        return Err(JsonRpcError::invalid_params());
    }

    if matches!(tx.gas, Some(gas) if gas.is_zero()) {
        tx.gas = None;
    }

    if matches!(tx.gas_price_details, GasPriceDetails::Legacy { .. })
        && tx.authorization_list.is_some()
    {
        return Err(JsonRpcError::invalid_params());
    }

    validate_fill_transaction_type(tx)?;

    Ok(())
}

/// Fill fee fields for `eth_fillTransaction` without clamping a user cap down.
///
/// For EIP-1559 transactions:
/// - preserve a nonzero user `maxFeePerGas` when it is already high enough
/// - fill a missing `maxPriorityFeePerGas`
/// - raise `maxFeePerGas` to at least the current base fee and priority fee so the tx is signable now
///
/// Missing or zero `maxFeePerGas` follows the existing default-fill path.
fn fill_transaction_gas_price_details(
    tx: &mut CallRequest,
    current_base_fee: U256,
    default_fill_base_fee: U256,
    suggested_priority_fee: Option<U256>,
) -> Result<(), JsonRpcError> {
    match &tx.gas_price_details {
        GasPriceDetails::Legacy { .. } => tx.fill_gas_prices(default_fill_base_fee),
        GasPriceDetails::Eip1559 {
            max_fee_per_gas: Some(max_fee_per_gas),
            max_priority_fee_per_gas,
        } if *max_fee_per_gas != U256::ZERO => {
            let max_priority_fee_per_gas =
                (*max_priority_fee_per_gas).unwrap_or(suggested_priority_fee.unwrap_or_default());
            let max_fee_per_gas = max_fee_per_gas
                .to_owned()
                .max(current_base_fee)
                .max(max_priority_fee_per_gas);

            tx.gas_price_details = GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(max_fee_per_gas),
                max_priority_fee_per_gas: Some(max_priority_fee_per_gas),
            };
            Ok(())
        }
        GasPriceDetails::Eip1559 {
            max_fee_per_gas,
            max_priority_fee_per_gas,
        } => {
            let max_fee_per_gas = match max_fee_per_gas {
                Some(max_fee_per_gas) if max_fee_per_gas.is_zero() => None,
                other => *other,
            };
            tx.gas_price_details = GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas: Some(
                    (*max_priority_fee_per_gas)
                        .unwrap_or(suggested_priority_fee.unwrap_or_default()),
                ),
            };
            tx.fill_gas_prices(default_fill_base_fee)
        }
    }
}

fn build_unsigned_transaction(
    tx: &CallRequest,
    chain_id: u64,
) -> Result<(UnformattedData, CallRequest), JsonRpcError> {
    let from = tx.from.unwrap_or_default();
    let nonce: u64 = tx
        .nonce
        .unwrap_or_default()
        .try_into()
        .map_err(|_| JsonRpcError::invalid_params())?;
    let gas_limit: u64 = tx
        .gas
        .unwrap_or_default()
        .try_into()
        .map_err(|_| JsonRpcError::invalid_params())?;
    let value = tx.value.unwrap_or_default();
    let input = tx.input.input.clone().unwrap_or_default();
    let to = tx.to;
    let is_eip2930 = requires_eip2930_encoding(tx);

    let raw_bytes = match &tx.gas_price_details {
        GasPriceDetails::Legacy { .. } if is_eip2930 => {
            let unsigned = build_fill_transaction_eip2930(tx, chain_id)?;
            let mut buf = Vec::new();
            unsigned.encode_for_signing(&mut buf);
            buf
        }
        GasPriceDetails::Legacy { gas_price } => {
            let unsigned = TxLegacy {
                chain_id: Some(chain_id),
                nonce,
                gas_price: (*gas_price)
                    .try_into()
                    .map_err(|_| JsonRpcError::invalid_params())?,
                gas_limit,
                to: to.map(TxKind::Call).unwrap_or(TxKind::Create),
                value,
                input,
            };
            let mut buf = Vec::new();
            unsigned.encode_for_signing(&mut buf);
            buf
        }
        GasPriceDetails::Eip1559 {
            max_fee_per_gas,
            max_priority_fee_per_gas,
        } => {
            if let Some(auth_list) = &tx.authorization_list {
                let unsigned = TxEip7702 {
                    chain_id,
                    nonce,
                    max_fee_per_gas: max_fee_per_gas
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    max_priority_fee_per_gas: max_priority_fee_per_gas
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    gas_limit,
                    to: to.ok_or(JsonRpcError::invalid_params())?,
                    value,
                    input,
                    access_list: tx.access_list.clone().unwrap_or_default(),
                    authorization_list: auth_list.clone(),
                };
                let mut buf = Vec::new();
                unsigned.encode_for_signing(&mut buf);
                buf
            } else {
                let unsigned = TxEip1559 {
                    chain_id,
                    nonce,
                    max_fee_per_gas: max_fee_per_gas
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    max_priority_fee_per_gas: max_priority_fee_per_gas
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    gas_limit,
                    to: to.map(TxKind::Call).unwrap_or(TxKind::Create),
                    value,
                    input,
                    access_list: tx.access_list.clone().unwrap_or_default(),
                };
                let mut buf = Vec::new();
                unsigned.encode_for_signing(&mut buf);
                buf
            }
        }
    };

    let filled = CallRequest {
        from: Some(from),
        to,
        gas: Some(tx.gas.unwrap_or_default()),
        gas_price_details: tx.gas_price_details.clone(),
        value: Some(value),
        input: tx.input.clone(),
        nonce: Some(tx.nonce.unwrap_or_default()),
        chain_id: Some(tx.chain_id.unwrap_or(U64::from(chain_id))),
        transaction_type: if is_eip2930 {
            Some(U8::from(EIP2930_TX_TYPE_ID))
        } else {
            tx.transaction_type
        },
        ..tx.clone()
    };

    Ok((UnformattedData(raw_bytes), filled))
}

async fn suggested_priority_fee() -> Result<u64, JsonRpcError> {
    // TODO: hardcoded as 2 gwei for now, need to implement gas oracle
    // Refer to <https://github.com/ethereum/pm/issues/328#issuecomment-853234014>
    Ok(2000000000)
}

#[rpc(method = "eth_gasPrice")]
#[allow(non_snake_case)]
/// Returns the current price per gas in wei.
pub async fn monad_eth_gasPrice<T: Triedb>(
    data_provider: &DataProvider<T>,
) -> JsonRpcResult<Quantity> {
    trace!("monad_eth_gasPrice");

    let header = data_provider
        .get_block_header(BlockTagOrHash::BlockTags(BlockTags::Latest))
        .await
        .map_err(|_| JsonRpcError::internal_error("could not get block data".into()))?;

    // Obtain base fee from latest block header
    let base_fee_per_gas = header.base_fee_per_gas.unwrap_or_default();

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
    #[serde(default)]
    reward_percentiles: Option<Vec<f64>>,
}

#[rpc(method = "eth_feeHistory")]
#[allow(non_snake_case)]
/// Transaction fee history
/// Returns transaction base fee per gas and effective priority fee per gas for the requested/supported block range.
pub async fn monad_eth_feeHistory<T: Triedb>(
    data_provider: &DataProvider<T>,
    params: MonadEthHistoryParams,
) -> JsonRpcResult<MonadFeeHistory> {
    trace!("monad_eth_feeHistory");

    let block_count = params.block_count.0;
    match block_count {
        0 => return Ok(MonadFeeHistory(FeeHistory::default())),
        1..=1024 => (),
        _ => {
            return Err(JsonRpcError::custom(
                "block count must be between 1 and 1024".to_string(),
            ));
        }
    }

    let header = data_provider
        .get_block_header(BlockTagOrHash::BlockTags(params.newest_block))
        .await
        .map_err(|_| JsonRpcError::internal_error("could not get block data".into()))?;

    let percentiles = match params.reward_percentiles {
        Some(percentiles) => {
            if percentiles.len() > 100 {
                return Err(JsonRpcError::internal_error(
                    "number of reward percentiles must be less than or equal to 100".into(),
                ));
            }

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
                Some(percentiles)
            }
        }
        None => None,
    };

    let oldest_block = header.number.saturating_sub(block_count - 1);
    let mut base_fee_per_gas_history: Vec<u128> = Vec::with_capacity(block_count as usize + 1);
    let mut gas_used_ratio_history = Vec::with_capacity(block_count as usize);
    let mut rewards = Vec::with_capacity(block_count as usize + 1);

    let block_range = oldest_block..=header.number;

    let block_data_futures = block_range.map(|blk_num| async move {
        let block = data_provider
            .get_block(
                BlockTagOrHash::BlockTags(BlockTags::Number(Quantity(blk_num))),
                true,
            )
            .await
            .map_err(|_| JsonRpcError::internal_error("could not get block data".into()))?;

        let receipts = data_provider
            .get_block_receipts(BlockTagOrHash::BlockTags(BlockTags::Number(Quantity(
                blk_num,
            ))))
            .await
            .map_err(|_| JsonRpcError::internal_error("could not get block receipts".into()))?;

        Ok::<_, JsonRpcError>((blk_num, block, receipts))
    });

    let block_data: Vec<_> = futures::stream::iter(block_data_futures)
        .buffered(20)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, JsonRpcError>>()?;

    for (_blk_num, block, receipts) in block_data.into_iter() {
        let header = block.header;
        let base_fee = header.base_fee_per_gas.unwrap_or_default();
        base_fee_per_gas_history.push(header.base_fee_per_gas.unwrap_or_default().into());

        gas_used_ratio_history.push((header.gas_used as f64).div(header.gas_limit as f64));

        let txns: Vec<alloy_rpc_types::Transaction> =
            block.transactions.into_transactions().collect::<Vec<_>>();

        let receipts = receipts.into_iter().map(|r| r.0).collect::<Vec<_>>();

        // Rewards are the requested percentiles of the effective priority fees per gas. Sorted in ascending order and weighted by gas used.
        let percentile_rewards = calculate_fee_history_rewards(
            txns,
            receipts,
            base_fee,
            header.gas_used,
            percentiles.as_ref(),
        );

        rewards.push(percentile_rewards);
    }

    let last_base_fee = base_fee_per_gas_history
        .last()
        .map(|&fee| fee as u64)
        .unwrap_or_default();

    // Get the newest block after the last block in the range
    let next_block_base_fee =
        get_next_block_base_fee(data_provider, params.newest_block, last_base_fee).await?;
    base_fee_per_gas_history.push(next_block_base_fee.into());

    let rewards = if percentiles.is_some() {
        Some(rewards)
    } else {
        Some(vec![])
    };

    Ok(MonadFeeHistory(FeeHistory {
        base_fee_per_gas: base_fee_per_gas_history,
        gas_used_ratio: gas_used_ratio_history,
        base_fee_per_blob_gas: vec![0; (block_count + 1) as usize],
        blob_gas_used_ratio: vec![0.0; (block_count) as usize],
        oldest_block,
        reward: rewards,
    }))
}

fn calculate_fee_history_rewards(
    transactions: Vec<alloy_rpc_types::Transaction>,
    receipts: Vec<TransactionReceipt>,
    base_fee: u64,
    block_gas_used: u64,
    percentiles: Option<&Vec<f64>>,
) -> Vec<u128> {
    let Some(percentiles) = percentiles else {
        return vec![];
    };

    if transactions.is_empty() {
        return vec![0; percentiles.len()];
    }

    let transactions_len = transactions.len();

    // Get the reward and gas used for each transaction using receipt.
    let gas_and_rewards = transactions
        .into_iter()
        .zip(receipts)
        .map(|(tx, receipt)| {
            let gas_used = receipt.gas_used;
            let reward = tx.effective_tip_per_gas(base_fee).unwrap_or_default();
            (gas_used, reward)
        })
        .sorted_by_key(|(_, reward)| *reward)
        .collect::<Vec<_>>();

    let mut idx = 0;
    let mut cumulative_gas_used: u64 = 0;
    let mut rewards = Vec::new();

    for pct in percentiles {
        let gas_threshold = (block_gas_used as f64 * pct / 100.0).round() as u64;
        while cumulative_gas_used < gas_threshold && idx < transactions_len {
            cumulative_gas_used += gas_and_rewards[idx].0;
            idx += 1;
        }
        // Clamp idx to valid range
        let reward_idx = idx.min(transactions_len - 1);
        rewards.push(gas_and_rewards[reward_idx].1);
    }

    rewards
}

pub async fn get_next_block_base_fee<T>(
    data_provider: &DataProvider<T>,
    latest: BlockTags,
    previous_base_fee: u64,
) -> JsonRpcResult<u64>
where
    T: Triedb,
{
    let latest_plus_one = match latest {
        BlockTags::Latest | BlockTags::Safe => {
            // TODO: rpc does not have access to consensus headers to calculate the next block base fee.
            // Return base fee of the previous block.
            return Ok(previous_base_fee);
        }
        BlockTags::Number(num) => {
            let latest_block_num = data_provider.get_latest_block_number();
            if num.0 + 1 > latest_block_num {
                return Ok(previous_base_fee);
            }
            BlockTags::Number(Quantity(num.0 + 1))
        }
        BlockTags::Finalized => BlockTags::Latest,
    };

    let header = data_provider
        .get_block_header(BlockTagOrHash::BlockTags(latest_plus_one))
        .await
        .map_err(|_| JsonRpcError::internal_error("could not get block data".into()))?;

    Ok(header.base_fee_per_gas.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{
        transaction::Recovered, Block, Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom,
        SignableTransaction, TxEip1559, TxEip2930,
    };
    use alloy_primitives::{Bloom, Bytes, FixedBytes, Log, LogData, B256};
    use alloy_rpc_types::{AccessList, AccessListItem};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use monad_chain_config::execution_revision::MonadExecutionRevision;
    use monad_eth_types::{EthAccount, ReceiptWithLogIndex};
    use monad_ethcall::{EthCallResult, FailureCallResult, SuccessCallResult};
    use monad_triedb_utils::mock_triedb::MockTriedb;

    use super::*;
    use crate::{
        data::eth_call_handler::EthCallHandlerConfig,
        handlers::eth::call::{CallInput, CallRequest, GasPriceDetails},
    };

    #[derive(Clone, Copy)]
    enum MockTerminalResult {
        Success,
        ExecutionError,
        ReserveBalanceViolation,
    }

    fn mock_eth_call(
        gas_used: u64,
        gas_refund: u64,
        terminal_result: MockTerminalResult,
    ) -> impl AsyncFn(&TxEnvelope) -> CallResult {
        async move |txn: &TxEnvelope| {
            if txn.gas_limit() >= gas_used + gas_refund {
                match terminal_result {
                    MockTerminalResult::Success => CallResult::Success(SuccessCallResult {
                        gas_used,
                        gas_refund,
                        ..Default::default()
                    }),
                    MockTerminalResult::ExecutionError => CallResult::Failure(FailureCallResult {
                        error_code: EthCallResult::ExecutionError,
                        gas_used,
                        gas_refund,
                        message: "execution reverted".to_string(),
                        data: Some("0x".to_string()),
                    }),
                    MockTerminalResult::ReserveBalanceViolation => {
                        CallResult::Failure(FailureCallResult {
                            error_code: EthCallResult::ReserveBalanceViolation,
                            gas_used,
                            gas_refund,
                            message: "reserve balance violation".to_string(),
                            data: None,
                        })
                    }
                }
            } else {
                CallResult::Failure(FailureCallResult {
                    error_code: EthCallResult::OutOfGas,
                    message: "out of gas".to_string(),
                    data: Some("0x".to_string()),
                    ..Default::default()
                })
            }
        }
    }

    fn mock_eth_call_handler_config() -> EthCallHandlerConfig {
        EthCallHandlerConfig {
            enable_stats: false,
            pool_low: monad_ethcall::PoolConfig {
                num_threads: 0,
                num_fibers: 0,
                timeout_sec: 0,
                queue_limit: 0,
            },
            pool_high: monad_ethcall::PoolConfig {
                num_threads: 0,
                num_fibers: 0,
                timeout_sec: 0,
                queue_limit: 0,
            },
            pool_block: monad_ethcall::PoolConfig {
                num_threads: 0,
                num_fibers: 0,
                timeout_sec: 0,
                queue_limit: 0,
            },
            tx_exec_num_fibers: 0,
            node_cache_max_mem: 0,
            max_concurrent_permits: 0,
            provider_gas_limit_eth_call: 30_000_000,
            provider_gas_limit_eth_estimate_gas: 30_000_000,
        }
    }

    fn make_fill_data_provider(
        from: Address,
        nonce: u64,
        balance: u128,
        latest_block: u64,
        base_fee: u64,
    ) -> DataProvider<MockTriedb> {
        let mut mock_triedb = MockTriedb::default();
        mock_triedb.set_latest_block(latest_block);
        mock_triedb.set_account(
            from.into(),
            EthAccount {
                nonce,
                balance: U256::from(balance),
                ..Default::default()
            },
        );
        mock_triedb.set_finalized_block(
            SeqNum(latest_block),
            make_block(latest_block, base_fee, vec![]),
        );

        DataProvider::new(None, mock_triedb, None)
    }

    #[tokio::test]
    async fn test_gas_limit_too_low() {
        // user specified gas limit lower than actual gas used
        let mut call_request = CallRequest {
            gas: Some(U256::from(30_000)),
            ..Default::default()
        };
        let eth_call_fn = mock_eth_call(50_000, 10_000, MockTerminalResult::Success);

        // should return gas estimation failure
        let result = estimate_gas(
            eth_call_fn,
            &mut call_request,
            U256::from(30_000),
            u64::MAX,
            u64::MAX,
            EstimateGasMode::RequireSuccess,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_gas_limit_unspecified() {
        // user did not specify gas limit
        let mut call_request = CallRequest::default();
        let eth_call_fn = mock_eth_call(50_000, 10_000, MockTerminalResult::Success);

        // should return correct gas estimation
        let result = estimate_gas(
            eth_call_fn,
            &mut call_request,
            U256::MAX,
            u64::MAX,
            u64::MAX,
            EstimateGasMode::RequireSuccess,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Quantity(60795));
    }

    #[tokio::test]
    async fn test_gas_limit_sufficient() {
        // user specify gas limit that is sufficient
        let mut call_request = CallRequest {
            gas: Some(U256::from(70_000)),
            ..Default::default()
        };
        let eth_call_fn = mock_eth_call(50_000, 10_000, MockTerminalResult::Success);

        // should return correct gas estimation
        let result = estimate_gas(
            eth_call_fn,
            &mut call_request,
            U256::from(70_000),
            u64::MAX,
            u64::MAX,
            EstimateGasMode::RequireSuccess,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Quantity(60795));
    }

    #[tokio::test]
    async fn test_gas_limit_just_sufficient() {
        // user specify gas limit that is just sufficient
        let mut call_request = CallRequest {
            gas: Some(U256::from(60_000)),
            ..Default::default()
        };
        let eth_call_fn = mock_eth_call(50_000, 10_000, MockTerminalResult::Success);

        // should return correct gas estimation
        let result = estimate_gas(
            eth_call_fn,
            &mut call_request,
            U256::from(60_000),
            u64::MAX,
            u64::MAX,
            EstimateGasMode::RequireSuccess,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Quantity(60_000));
    }

    #[tokio::test]
    async fn test_gas_limit_unspecified_allows_execution_failure_for_fill_transaction() {
        let mut call_request = CallRequest::default();
        let eth_call_fn = mock_eth_call(50_000, 10_000, MockTerminalResult::ExecutionError);

        let result = estimate_gas(
            eth_call_fn,
            &mut call_request,
            U256::MAX,
            u64::MAX,
            u64::MAX,
            EstimateGasMode::AllowExecutionFailure,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Quantity(60795));
    }

    #[tokio::test]
    async fn test_gas_limit_unspecified_allows_reserve_balance_failure_for_fill_transaction() {
        let mut call_request = CallRequest::default();
        let eth_call_fn =
            mock_eth_call(50_000, 10_000, MockTerminalResult::ReserveBalanceViolation);

        let result = estimate_gas(
            eth_call_fn,
            &mut call_request,
            U256::MAX,
            u64::MAX,
            u64::MAX,
            EstimateGasMode::AllowExecutionFailure,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Quantity(60795));
    }

    #[tokio::test]
    async fn test_gas_limit_unspecified_rejects_execution_failure_for_estimate_gas() {
        let mut call_request = CallRequest::default();
        let eth_call_fn = mock_eth_call(50_000, 10_000, MockTerminalResult::ExecutionError);

        let result = estimate_gas(
            eth_call_fn,
            &mut call_request,
            U256::MAX,
            u64::MAX,
            u64::MAX,
            EstimateGasMode::RequireSuccess,
        )
        .await;
        assert!(result.is_err());
    }

    fn make_block(num: u64, base_fee: u64, txns: Vec<TxEnvelope>) -> Block<TxEnvelope> {
        let mut blk = Block::<TxEnvelope>::default();
        blk.header.gas_limit = 30_000_000;
        blk.header.gas_used = txns.iter().map(|t| t.gas_limit()).sum();
        blk.header.base_fee_per_gas = Some(base_fee);
        blk.header.number = num;
        blk.body.transactions = txns;
        blk
    }

    fn make_tx(
        sender: FixedBytes<32>,
        max_fee_per_gas: u128,
        max_priority_fee_per_gas: u128,
        gas_limit: u64,
        nonce: u64,
        chain_id: u64,
    ) -> TxEnvelope {
        let transaction = TxEip1559 {
            chain_id,
            nonce,
            gas_limit,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            to: TxKind::Call(Address::repeat_byte(0u8)),
            value: Default::default(),
            access_list: Default::default(),
            input: vec![].into(),
        };

        let signer = PrivateKeySigner::from_bytes(&sender).unwrap();
        let signature = signer
            .sign_hash_sync(&transaction.signature_hash())
            .unwrap();
        transaction.into_signed(signature).into()
    }

    fn make_receipt(gas_used: u64) -> TransactionReceipt {
        use alloy_rpc_types::Log as RpcLog;

        TransactionReceipt {
            inner: alloy_consensus::ReceiptEnvelope::Eip1559(alloy_consensus::ReceiptWithBloom::<
                alloy_consensus::Receipt<RpcLog>,
            >::new(
                alloy_consensus::Receipt::<RpcLog> {
                    logs: vec![],
                    status: Eip658Value::Eip658(true),
                    cumulative_gas_used: gas_used,
                },
                Bloom::default(),
            )),
            transaction_hash: Default::default(),
            transaction_index: None,
            block_hash: None,
            block_number: None,
            gas_used,
            effective_gas_price: 0,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Default::default(),
            to: None,
            contract_address: None,
        }
    }

    #[tokio::test]
    async fn test_eth_fee_history() {
        let mut mock_triedb = MockTriedb::default();
        mock_triedb.set_latest_block(1000);
        let sender = FixedBytes::<32>::from([1u8; 32]);

        // Fetch fee history for an empty block.
        mock_triedb.set_finalized_block(SeqNum(1000), make_block(1000, 1_000, vec![]));

        let data_provider = DataProvider::new(None, mock_triedb, None);
        let res = monad_eth_feeHistory(
            &data_provider,
            MonadEthHistoryParams {
                block_count: Quantity(1),
                newest_block: BlockTags::Latest,
                reward_percentiles: Some(vec![0.0, 25.0, 50.0, 75.0, 100.0]),
            },
        )
        .await
        .expect("should get fee history");
        assert_eq!(res.0.oldest_block, 1000);
        assert_eq!(res.0.base_fee_per_blob_gas, vec![0, 0]);
        assert_eq!(res.0.blob_gas_used_ratio, vec![0.0]);
        assert_eq!(res.0.gas_used_ratio, vec![0.0]);
        assert_eq!(res.0.base_fee_per_gas, vec![1_000, 1_000]);
        assert_eq!(res.0.reward, Some(vec![vec![0, 0, 0, 0, 0]]));

        // Fetch fee history for blocks that have 4 transactions.
        let mut txs = Vec::new();
        let mut receipts = Vec::new();
        for i in 1..=4 {
            let tx = make_tx(sender, 1000 * i, 1000 * i, 21_000, 1, 1);
            txs.push(tx);
            let receipt = ReceiptWithBloom::new(
                Receipt::<Log> {
                    logs: vec![Log {
                        address: Default::default(),
                        data: LogData::new(vec![], Bytes::default()).unwrap(),
                    }],
                    status: Eip658Value::Eip658(true),
                    cumulative_gas_used: 21000 * i as u64,
                },
                Bloom::repeat_byte(b'a'),
            );
            receipts.push(ReceiptWithLogIndex {
                receipt: ReceiptEnvelope::Eip1559(receipt),
                starting_log_index: 0,
            });
        }
        let mut mock_triedb = MockTriedb::default();
        mock_triedb.set_latest_block(1000);
        mock_triedb.set_finalized_block(SeqNum(1000), make_block(1000, 2_000, txs.clone()));
        mock_triedb.set_finalized_block(SeqNum(999), make_block(999, 1_000, txs));
        mock_triedb.set_finalized_block(SeqNum(998), make_block(999, 5_000, vec![]));
        mock_triedb.set_receipts(SeqNum(1000), receipts.clone());
        mock_triedb.set_receipts(SeqNum(999), receipts);

        let data_provider = DataProvider::new(None, mock_triedb, None);
        let res = monad_eth_feeHistory(
            &data_provider,
            MonadEthHistoryParams {
                block_count: Quantity(1),
                newest_block: BlockTags::Latest,
                reward_percentiles: Some(vec![50.0, 75.0]),
            },
        )
        .await
        .expect("should get fee history");

        let gas_used = 21_000.0 * 4.0 / 30_000_000.0;

        assert_eq!(res.0.oldest_block, 1000);
        assert_eq!(res.0.base_fee_per_gas, vec![2_000, 2_000]);
        assert_eq!(res.0.gas_used_ratio, vec![gas_used]);
        assert_eq!(res.0.reward, Some(vec![vec![1000, 2000]]));

        // Fetch block history with explicit block heights
        let res = monad_eth_feeHistory(
            &data_provider,
            MonadEthHistoryParams {
                block_count: Quantity(1),
                newest_block: BlockTags::Number(Quantity(999)),
                reward_percentiles: None,
            },
        )
        .await
        .expect("should get fee history");
        assert_eq!(res.0.oldest_block, 999);
        assert_eq!(res.0.base_fee_per_gas, vec![1_000, 2_000]);
        assert_eq!(res.0.gas_used_ratio, vec![gas_used]);
        assert_eq!(res.0.reward, Some(vec![]));

        let res = monad_eth_feeHistory(
            &data_provider,
            MonadEthHistoryParams {
                block_count: Quantity(2),
                newest_block: BlockTags::Number(Quantity(999)),
                reward_percentiles: None,
            },
        )
        .await
        .expect("should get fee history");
        assert_eq!(res.0.oldest_block, 998);
        assert_eq!(res.0.base_fee_per_gas, vec![5_000, 1_000, 2_000]);
        assert_eq!(res.0.gas_used_ratio, vec![0.0, gas_used]);
        assert_eq!(res.0.reward, Some(vec![]));
    }

    /// When transactions have different gas_used values that don't correlate with rewards,
    /// the returned percentile rewards should still be sorted in ascending order.
    #[test]
    fn test_fee_history_rewards_should_be_sorted_ascending() {
        let sender = FixedBytes::<32>::from([1u8; 32]);
        let base_fee = 1000u64;

        // Create transactions where gas_used order doesn't match reward order
        let txs = vec![
            make_tx(sender, 4000, 4000, 10_000, 1, 1), // reward = 3000
            make_tx(sender, 2000, 2000, 20_000, 2, 1), // reward = 1000
            make_tx(sender, 5000, 5000, 30_000, 3, 1), // reward = 4000
            make_tx(sender, 3000, 3000, 40_000, 4, 1), // reward = 2000
        ];

        let receipts: Vec<TransactionReceipt> = vec![
            make_receipt(10_000),
            make_receipt(20_000),
            make_receipt(30_000),
            make_receipt(40_000),
        ];

        let block_gas_used = 100_000u64;
        let percentiles = vec![10.0, 30.0, 60.0, 100.0];

        let signer = PrivateKeySigner::from_bytes(&sender).unwrap();
        let from_addr = signer.address();

        let transactions: Vec<alloy_rpc_types::Transaction> = txs
            .into_iter()
            .map(|tx| alloy_rpc_types::Transaction {
                inner: Recovered::new_unchecked(tx, from_addr),
                block_hash: None,
                block_number: None,
                transaction_index: None,
                effective_gas_price: None,
            })
            .collect();

        let rewards = calculate_fee_history_rewards(
            transactions,
            receipts,
            base_fee,
            block_gas_used,
            Some(&percentiles),
        );

        for i in 1..rewards.len() {
            assert!(
                rewards[i - 1] <= rewards[i],
                "Rewards should be sorted in ascending order. Got {:?}",
                rewards
            );
        }
    }

    #[test]
    fn test_build_unsigned_transaction_eip1559() {
        let chain_id = 12345u64;
        let from_addr = Address::repeat_byte(0x11);
        let to_addr = Address::repeat_byte(0x22);

        let call_request = CallRequest {
            from: Some(from_addr),
            to: Some(to_addr),
            gas: Some(U256::from(21_000)),
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::from(100_000_000_000u64)),
                max_priority_fee_per_gas: Some(U256::from(2_000_000_000u64)),
            },
            value: Some(U256::from(1_000_000_000_000_000_000u64)),
            input: CallInput {
                input: Some(Bytes::from(vec![0xde, 0xad, 0xbe, 0xef])),
                data: None,
            },
            nonce: Some(U64::from(42)),
            chain_id: Some(U64::from(chain_id)),
            access_list: None,
            authorization_list: None,
            max_fee_per_blob_gas: None,
            blob_versioned_hashes: None,
            transaction_type: None,
        };

        let (raw, filled_tx) = build_unsigned_transaction(&call_request, chain_id).unwrap();

        assert!(!raw.0.is_empty(), "raw bytes should not be empty");
        assert_eq!(filled_tx.from, Some(from_addr));
        assert_eq!(filled_tx.to, Some(to_addr));
        assert_eq!(filled_tx.gas, Some(U256::from(21_000)));
        assert_eq!(
            filled_tx.value,
            Some(U256::from(1_000_000_000_000_000_000u64))
        );
        assert_eq!(filled_tx.nonce, Some(U64::from(42)));
        assert_eq!(filled_tx.chain_id, Some(U64::from(chain_id)));

        match filled_tx.gas_price_details {
            GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
            } => {
                assert_eq!(max_fee_per_gas, Some(U256::from(100_000_000_000u64)));
                assert_eq!(max_priority_fee_per_gas, Some(U256::from(2_000_000_000u64)));
            }
            _ => panic!("Expected EIP-1559 gas price details"),
        }
    }

    #[tokio::test]
    async fn test_fill_transaction_happy_path_end_to_end() {
        let chain_id = 12345u64;
        let from = Address::repeat_byte(0x11);
        let to = Address::repeat_byte(0x22);
        let data_provider = make_fill_data_provider(from, 7, 0, 1000, 100);
        let config = mock_eth_call_handler_config();

        let eth_call_fn = mock_eth_call(50_000, 10_000, MockTerminalResult::Success);
        let result = fill_transaction_with_provider(
            &data_provider,
            &config,
            chain_id,
            MonadEthFillTransactionParams {
                tx: CallRequest {
                    from: Some(from),
                    to: Some(to),
                    ..Default::default()
                },
            },
            async |_, _, _, txn| eth_call_fn(txn).await,
        )
        .await
        .unwrap();

        assert!(!result.raw.0.is_empty());
        assert_eq!(result.tx.from, Some(from));
        assert_eq!(result.tx.to, Some(to));
        assert_eq!(result.tx.nonce, Some(U64::from(7)));
        assert_eq!(result.tx.chain_id, Some(U64::from(chain_id)));
        assert_eq!(result.tx.value, Some(U256::ZERO));
        assert_eq!(result.tx.gas, Some(U256::from(60795)));
        match result.tx.gas_price_details {
            GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
            } => {
                assert_eq!(max_fee_per_gas, Some(U256::from(2_000_000_150u64)));
                assert_eq!(max_priority_fee_per_gas, Some(U256::from(2_000_000_000u64)));
            }
            _ => panic!("Expected EIP-1559 gas price details"),
        }
    }

    #[tokio::test]
    async fn test_fill_transaction_revert_without_gas_succeeds() {
        let chain_id = 12345u64;
        let from = Address::repeat_byte(0x11);
        let to = Address::repeat_byte(0x22);
        let data_provider = make_fill_data_provider(from, 3, 0, 1000, 100);
        let config = mock_eth_call_handler_config();

        let eth_call_fn = mock_eth_call(50_000, 10_000, MockTerminalResult::ExecutionError);
        let result = fill_transaction_with_provider(
            &data_provider,
            &config,
            chain_id,
            MonadEthFillTransactionParams {
                tx: CallRequest {
                    from: Some(from),
                    to: Some(to),
                    input: CallInput {
                        input: Some(Bytes::from(vec![0xde, 0xad, 0xbe, 0xef])),
                        data: None,
                    },
                    ..Default::default()
                },
            },
            async |_, _, _, txn| eth_call_fn(txn).await,
        )
        .await
        .unwrap();

        assert_eq!(result.tx.gas, Some(U256::from(60795)));
        assert_eq!(
            result.tx.input.input,
            Some(Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]))
        );
    }

    #[tokio::test]
    async fn test_fill_transaction_ignores_sender_affordability_and_reserve_balance() {
        let chain_id = 12345u64;
        let from = Address::repeat_byte(0x11);
        let to = Address::repeat_byte(0x22);
        let data_provider = make_fill_data_provider(from, 0, 0, 1000, 100);
        let config = mock_eth_call_handler_config();

        let eth_call_fn =
            mock_eth_call(50_000, 10_000, MockTerminalResult::ReserveBalanceViolation);
        let result = fill_transaction_with_provider(
            &data_provider,
            &config,
            chain_id,
            MonadEthFillTransactionParams {
                tx: CallRequest {
                    from: Some(from),
                    to: Some(to),
                    ..Default::default()
                },
            },
            async |_, _, _, txn| eth_call_fn(txn).await,
        )
        .await
        .unwrap();

        assert_eq!(result.tx.gas, Some(U256::from(60795)));
        assert_eq!(result.tx.nonce, Some(U64::from(0)));
    }

    #[tokio::test]
    async fn test_fill_transaction_rejects_eip7702_without_to() {
        let chain_id = 12345u64;
        let from = Address::repeat_byte(0x11);
        let data_provider = make_fill_data_provider(from, 1, 0, 1000, 100);
        let config = mock_eth_call_handler_config();

        let eth_call_fn = mock_eth_call(50_000, 10_000, MockTerminalResult::Success);
        let err = fill_transaction_with_provider(
            &data_provider,
            &config,
            chain_id,
            MonadEthFillTransactionParams {
                tx: CallRequest {
                    from: Some(from),
                    gas: Some(U256::from(21_000)),
                    authorization_list: Some(vec![]),
                    ..Default::default()
                },
            },
            async |_, _, _, txn| eth_call_fn(txn).await,
        )
        .await
        .unwrap_err();

        assert_eq!(err, JsonRpcError::invalid_params());
    }

    #[tokio::test]
    async fn test_fill_transaction_canonicalizes_input_and_fills_returned_fields() {
        let chain_id = 12345u64;
        let from = Address::repeat_byte(0x11);
        let to = Address::repeat_byte(0x22);
        let data_provider = make_fill_data_provider(from, 42, 0, 1000, 100);
        let config = mock_eth_call_handler_config();

        let eth_call_fn = mock_eth_call(50_000, 10_000, MockTerminalResult::Success);
        let result = fill_transaction_with_provider(
            &data_provider,
            &config,
            chain_id,
            MonadEthFillTransactionParams {
                tx: CallRequest {
                    from: Some(from),
                    to: Some(to),
                    gas: Some(U256::from(21_000)),
                    chain_id: Some(U64::from(chain_id + 1)),
                    input: CallInput {
                        input: None,
                        data: Some(Bytes::from(vec![0xca, 0xfe])),
                    },
                    ..Default::default()
                },
            },
            async |_, _, _, txn| eth_call_fn(txn).await,
        )
        .await
        .unwrap();

        assert_eq!(result.tx.value, Some(U256::ZERO));
        assert_eq!(result.tx.nonce, Some(U64::from(42)));
        assert_eq!(result.tx.chain_id, Some(U64::from(chain_id)));
        assert_eq!(result.tx.input.input, Some(Bytes::from(vec![0xca, 0xfe])));
        assert_eq!(result.tx.input.data, None);
        assert_eq!(result.tx.gas, Some(U256::from(21_000)));
    }

    #[test]
    fn test_fill_transaction_gas_price_details_preserves_user_max_fee_per_gas() {
        let mut call_request = CallRequest {
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::from(200)),
                max_priority_fee_per_gas: None,
            },
            ..Default::default()
        };

        fill_transaction_gas_price_details(
            &mut call_request,
            U256::from(100),
            U256::from(150),
            Some(U256::from(2)),
        )
        .unwrap();

        match call_request.gas_price_details {
            GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
            } => {
                assert_eq!(max_fee_per_gas, Some(U256::from(200)));
                assert_eq!(max_priority_fee_per_gas, Some(U256::from(2)));
            }
            _ => panic!("Expected EIP-1559 gas price details"),
        }
    }

    #[test]
    fn test_fill_transaction_gas_price_details_uses_existing_defaults_when_cap_missing() {
        let mut call_request = CallRequest {
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
            },
            ..Default::default()
        };

        fill_transaction_gas_price_details(
            &mut call_request,
            U256::from(100),
            U256::from(150),
            Some(U256::from(2)),
        )
        .unwrap();

        match call_request.gas_price_details {
            GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
            } => {
                assert_eq!(max_fee_per_gas, Some(U256::from(152)));
                assert_eq!(max_priority_fee_per_gas, Some(U256::from(2)));
            }
            _ => panic!("Expected EIP-1559 gas price details"),
        }
    }

    #[test]
    fn test_fill_transaction_gas_price_details_raises_user_cap_to_current_base_fee() {
        let mut call_request = CallRequest {
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::from(50)),
                max_priority_fee_per_gas: None,
            },
            ..Default::default()
        };

        fill_transaction_gas_price_details(
            &mut call_request,
            U256::from(100),
            U256::from(150),
            Some(U256::from(2)),
        )
        .unwrap();

        match call_request.gas_price_details {
            GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
            } => {
                assert_eq!(max_fee_per_gas, Some(U256::from(100)));
                assert_eq!(max_priority_fee_per_gas, Some(U256::from(2)));
            }
            _ => panic!("Expected EIP-1559 gas price details"),
        }
    }

    #[test]
    fn test_fill_transaction_gas_price_details_raises_user_cap_to_priority_fee() {
        let mut call_request = CallRequest {
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::from(50)),
                max_priority_fee_per_gas: Some(U256::from(75)),
            },
            ..Default::default()
        };

        fill_transaction_gas_price_details(
            &mut call_request,
            U256::from(10),
            U256::from(15),
            Some(U256::from(2)),
        )
        .unwrap();

        match call_request.gas_price_details {
            GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
            } => {
                assert_eq!(max_fee_per_gas, Some(U256::from(75)));
                assert_eq!(max_priority_fee_per_gas, Some(U256::from(75)));
            }
            _ => panic!("Expected EIP-1559 gas price details"),
        }
    }

    #[test]
    fn test_fill_transaction_gas_price_details_preserves_user_cap_above_base_fee_below_default() {
        let mut call_request = CallRequest {
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::from(120)),
                max_priority_fee_per_gas: None,
            },
            ..Default::default()
        };

        fill_transaction_gas_price_details(
            &mut call_request,
            U256::from(100),
            U256::from(150),
            Some(U256::from(2)),
        )
        .unwrap();

        match call_request.gas_price_details {
            GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
            } => {
                assert_eq!(max_fee_per_gas, Some(U256::from(120)));
                assert_eq!(max_priority_fee_per_gas, Some(U256::from(2)));
            }
            _ => panic!("Expected EIP-1559 gas price details"),
        }
    }

    #[test]
    fn test_fill_transaction_state_overrides_ignore_sender_affordability() {
        let sender = Address::repeat_byte(0x11);
        let state_overrides = fill_transaction_state_overrides(sender);

        assert_eq!(state_overrides.len(), 1);
        assert_eq!(
            state_overrides.get(&sender).and_then(|state| state.balance),
            Some(U256::MAX)
        );
    }

    #[test]
    fn test_fill_transaction_gas_price_details_treats_zero_cap_as_missing() {
        let mut without_cap = CallRequest {
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
            },
            ..Default::default()
        };
        let mut zero_cap = CallRequest {
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::ZERO),
                max_priority_fee_per_gas: None,
            },
            ..Default::default()
        };

        fill_transaction_gas_price_details(
            &mut without_cap,
            U256::from(100),
            U256::from(150),
            Some(U256::from(2)),
        )
        .unwrap();
        fill_transaction_gas_price_details(
            &mut zero_cap,
            U256::from(100),
            U256::from(150),
            Some(U256::from(2)),
        )
        .unwrap();

        match (without_cap.gas_price_details, zero_cap.gas_price_details) {
            (
                GasPriceDetails::Eip1559 {
                    max_fee_per_gas: without_cap_max_fee,
                    max_priority_fee_per_gas: without_cap_priority_fee,
                },
                GasPriceDetails::Eip1559 {
                    max_fee_per_gas: zero_cap_max_fee,
                    max_priority_fee_per_gas: zero_cap_priority_fee,
                },
            ) => {
                assert_eq!(without_cap_max_fee, zero_cap_max_fee);
                assert_eq!(without_cap_priority_fee, zero_cap_priority_fee);
            }
            _ => panic!("Expected EIP-1559 gas price details"),
        }
    }

    #[test]
    fn test_normalize_fill_transaction_request_treats_zero_gas_as_missing() {
        let mut without_gas = CallRequest::default();
        let mut zero_gas = CallRequest {
            gas: Some(U256::ZERO),
            ..Default::default()
        };

        normalize_fill_transaction_request(&mut without_gas).unwrap();
        normalize_fill_transaction_request(&mut zero_gas).unwrap();

        assert_eq!(without_gas.gas, None);
        assert_eq!(zero_gas.gas, None);
    }

    #[test]
    fn test_normalize_fill_transaction_request_rejects_mismatched_input_and_data() {
        let mut request = CallRequest {
            input: CallInput {
                input: Some(Bytes::from(vec![0xde, 0xad])),
                data: Some(Bytes::from(vec![0xbe, 0xef])),
            },
            ..Default::default()
        };

        assert!(normalize_fill_transaction_request(&mut request).is_err());
    }

    #[test]
    fn test_normalize_fill_transaction_request_rejects_blob_transaction_type() {
        let mut request = CallRequest {
            transaction_type: Some(U8::from(EIP4844_TX_TYPE_ID)),
            ..Default::default()
        };

        let err = normalize_fill_transaction_request(&mut request).unwrap_err();
        assert_eq!(err, JsonRpcError::invalid_params());
    }

    #[test]
    fn test_normalize_fill_transaction_request_rejects_blob_fields() {
        let mut request = CallRequest {
            max_fee_per_blob_gas: Some(U256::from(1)),
            blob_versioned_hashes: Some(vec![U256::from(1)]),
            ..Default::default()
        };

        let err = normalize_fill_transaction_request(&mut request).unwrap_err();
        assert_eq!(err, JsonRpcError::invalid_params());
    }

    #[test]
    fn test_normalize_fill_transaction_request_rejects_legacy_type_mismatch() {
        let mut request = CallRequest {
            gas_price_details: GasPriceDetails::Legacy {
                gas_price: U256::from(1),
            },
            transaction_type: Some(U8::from(EIP1559_TX_TYPE_ID)),
            ..Default::default()
        };

        let err = normalize_fill_transaction_request(&mut request).unwrap_err();
        assert_eq!(err, JsonRpcError::invalid_params());
    }

    #[test]
    fn test_normalize_fill_transaction_request_rejects_legacy_zero_type_with_access_list() {
        let mut request = CallRequest {
            gas_price_details: GasPriceDetails::Legacy {
                gas_price: U256::from(1),
            },
            access_list: Some(AccessList::default()),
            transaction_type: Some(U8::from(LEGACY_TX_TYPE_ID)),
            ..Default::default()
        };

        let err = normalize_fill_transaction_request(&mut request).unwrap_err();
        assert_eq!(err, JsonRpcError::invalid_params());
    }

    #[test]
    fn test_normalize_fill_transaction_request_rejects_eip1559_type_mismatch() {
        let mut request = CallRequest {
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::from(1)),
                max_priority_fee_per_gas: Some(U256::from(1)),
            },
            transaction_type: Some(U8::from(EIP2930_TX_TYPE_ID)),
            ..Default::default()
        };

        let err = normalize_fill_transaction_request(&mut request).unwrap_err();
        assert_eq!(err, JsonRpcError::invalid_params());
    }

    #[test]
    fn test_normalize_fill_transaction_request_rejects_eip7702_type_mismatch() {
        let mut request = CallRequest {
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::from(1)),
                max_priority_fee_per_gas: Some(U256::from(1)),
            },
            authorization_list: Some(vec![]),
            transaction_type: Some(U8::from(EIP1559_TX_TYPE_ID)),
            ..Default::default()
        };

        let err = normalize_fill_transaction_request(&mut request).unwrap_err();
        assert_eq!(err, JsonRpcError::invalid_params());
    }

    #[test]
    fn test_normalize_fill_transaction_request_rejects_legacy_authorization_list() {
        let mut request = CallRequest {
            gas_price_details: GasPriceDetails::Legacy {
                gas_price: U256::from(1),
            },
            authorization_list: Some(vec![]),
            ..Default::default()
        };

        let err = normalize_fill_transaction_request(&mut request).unwrap_err();
        assert_eq!(err, JsonRpcError::invalid_params());
    }

    #[test]
    fn test_build_fill_transaction_envelope_uses_eip2930_for_legacy_access_list() {
        let chain_id = 12345u64;
        let request = CallRequest {
            to: Some(Address::repeat_byte(0x22)),
            gas: Some(U256::from(21_000)),
            gas_price_details: GasPriceDetails::Legacy {
                gas_price: U256::from(2_000_000_000u64),
            },
            nonce: Some(U64::from(0)),
            access_list: Some(AccessList(vec![AccessListItem {
                address: Address::repeat_byte(0x33),
                storage_keys: vec![B256::ZERO],
            }])),
            ..Default::default()
        };

        let envelope = build_fill_transaction_envelope(&request, chain_id).unwrap();

        match envelope {
            TxEnvelope::Eip2930(tx) => assert_eq!(tx.tx().chain_id, chain_id),
            _ => panic!("expected EIP-2930 envelope"),
        }
    }

    #[test]
    fn test_build_fill_transaction_envelope_rejects_oversized_eip2930_initcode() {
        let max_code_size = MonadExecutionRevision::LATEST
            .execution_chain_params()
            .max_code_size;
        let chain_id = 12345u64;
        let request = CallRequest {
            to: None,
            gas: Some(U256::from(100_000)),
            gas_price_details: GasPriceDetails::Legacy {
                gas_price: U256::from(2_000_000_000u64),
            },
            nonce: Some(U64::from(0)),
            transaction_type: Some(U8::from(EIP2930_TX_TYPE_ID)),
            input: CallInput {
                input: Some(Bytes::from(vec![0x60; 2 * max_code_size + 1])),
                data: None,
            },
            ..Default::default()
        };

        let err = build_fill_transaction_envelope(&request, chain_id).unwrap_err();
        assert_eq!(
            err,
            JsonRpcError::code_size_too_large(2 * max_code_size + 1)
        );
    }

    #[test]
    fn test_build_unsigned_transaction_legacy_access_list_uses_eip2930() {
        let chain_id = 12345u64;
        let from_addr = Address::repeat_byte(0x11);
        let to_addr = Address::repeat_byte(0x22);
        let access_list = AccessList(vec![AccessListItem {
            address: Address::repeat_byte(0x33),
            storage_keys: vec![B256::ZERO],
        }]);
        let gas_price = U256::from(2_000_000_000u64);

        let call_request = CallRequest {
            from: Some(from_addr),
            to: Some(to_addr),
            gas: Some(U256::from(21_000)),
            gas_price_details: GasPriceDetails::Legacy { gas_price },
            value: Some(U256::ZERO),
            input: CallInput {
                input: Some(Bytes::new()),
                data: None,
            },
            nonce: Some(U64::from(0)),
            chain_id: Some(U64::from(chain_id)),
            access_list: Some(access_list.clone()),
            authorization_list: None,
            max_fee_per_blob_gas: None,
            blob_versioned_hashes: None,
            transaction_type: None,
        };

        let unsigned = TxEip2930 {
            chain_id,
            nonce: 0,
            gas_price: gas_price.try_into().unwrap(),
            gas_limit: 21_000,
            to: TxKind::Call(to_addr),
            value: U256::ZERO,
            access_list,
            input: Bytes::new(),
        };
        let mut expected_raw = Vec::new();
        unsigned.encode_for_signing(&mut expected_raw);

        let (raw, filled_tx) = build_unsigned_transaction(&call_request, chain_id).unwrap();

        assert_eq!(raw.0, expected_raw);
        assert_eq!(
            filled_tx.transaction_type,
            Some(U8::from(EIP2930_TX_TYPE_ID))
        );
    }

    #[test]
    fn test_build_unsigned_transaction_rejects_oversized_eip2930_initcode() {
        let chain_id = 12345u64;
        let max_code_size = MonadExecutionRevision::LATEST
            .execution_chain_params()
            .max_code_size;

        let call_request = CallRequest {
            from: Some(Address::repeat_byte(0x11)),
            to: None,
            gas: Some(U256::from(100_000)),
            gas_price_details: GasPriceDetails::Legacy {
                gas_price: U256::from(2_000_000_000u64),
            },
            value: Some(U256::ZERO),
            input: CallInput {
                input: Some(Bytes::from(vec![0x60; 2 * max_code_size + 1])),
                data: None,
            },
            nonce: Some(U64::from(0)),
            chain_id: Some(U64::from(chain_id)),
            access_list: Some(AccessList::default()),
            authorization_list: None,
            max_fee_per_blob_gas: None,
            blob_versioned_hashes: None,
            transaction_type: None,
        };

        let err = build_unsigned_transaction(&call_request, chain_id).unwrap_err();
        assert_eq!(
            err,
            JsonRpcError::code_size_too_large(2 * max_code_size + 1)
        );
    }

    #[test]
    fn test_build_unsigned_transaction_legacy() {
        let chain_id = 12345u64;
        let from_addr = Address::repeat_byte(0x11);
        let to_addr = Address::repeat_byte(0x22);

        let call_request = CallRequest {
            from: Some(from_addr),
            to: Some(to_addr),
            gas: Some(U256::from(21_000)),
            gas_price_details: GasPriceDetails::Legacy {
                gas_price: U256::from(50_000_000_000u64),
            },
            value: Some(U256::from(500_000_000_000_000_000u64)),
            input: CallInput {
                input: None,
                data: None,
            },
            nonce: Some(U64::from(0)),
            chain_id: Some(U64::from(chain_id)),
            access_list: None,
            authorization_list: None,
            max_fee_per_blob_gas: None,
            blob_versioned_hashes: None,
            transaction_type: None,
        };

        let (raw, filled_tx) = build_unsigned_transaction(&call_request, chain_id).unwrap();

        assert!(!raw.0.is_empty(), "raw bytes should not be empty");

        match filled_tx.gas_price_details {
            GasPriceDetails::Legacy { gas_price } => {
                assert_eq!(gas_price, U256::from(50_000_000_000u64));
            }
            _ => panic!("Expected Legacy gas price details"),
        }
    }

    #[test]
    fn test_build_unsigned_transaction_contract_creation() {
        let chain_id = 12345u64;
        let from_addr = Address::repeat_byte(0x11);

        let call_request = CallRequest {
            from: Some(from_addr),
            to: None,
            gas: Some(U256::from(100_000)),
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::from(100_000_000_000u64)),
                max_priority_fee_per_gas: Some(U256::from(2_000_000_000u64)),
            },
            value: Some(U256::ZERO),
            input: CallInput {
                input: Some(Bytes::from(vec![0x60, 0x80, 0x60, 0x40])),
                data: None,
            },
            nonce: Some(U64::from(0)),
            chain_id: Some(U64::from(chain_id)),
            access_list: None,
            authorization_list: None,
            max_fee_per_blob_gas: None,
            blob_versioned_hashes: None,
            transaction_type: None,
        };

        let (raw, filled_tx) = build_unsigned_transaction(&call_request, chain_id).unwrap();

        assert!(!raw.0.is_empty(), "raw bytes should not be empty");
        assert_eq!(filled_tx.to, None);
    }
}
