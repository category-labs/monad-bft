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

use std::{
    cmp::min,
    collections::{BTreeMap, BTreeSet},
};

use alloy_consensus::{Header, SignableTransaction, TxEip1559, TxEip7702, TxEnvelope, TxLegacy};
use alloy_eips::eip7702::SignedAuthorization;
use alloy_primitives::{Address, Bytes, Signature, TxKind, Uint, B256, U256, U64, U8};
use alloy_rpc_types::{AccessList, AccessListItem, AccessListResult};
use monad_chain_config::execution_revision::MonadExecutionRevision;
use monad_ethcall::{
    eth_call, CallResult, EthCallExecutor, EthCallRequest, EthCallResult, MonadTracer,
    StateOverrideSet,
};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{
    BlockKey, FinalizedBlockKey, ProposedBlockKey, Triedb, TriedbPath,
};
use monad_types::{BlockId, Hash, SeqNum};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tracing::{debug, trace};

use crate::{
    data::{
        eth_call_handler::{EthCallHandlerConfig, EthCallStatsTracker},
        get_block_key_from_tag_or_hash, DataProvider,
    },
    handlers::{
        debug::{decode_call_frame, TracerObject},
        parse_ethcall_chain_id,
    },
    types::{
        eth_json::BlockTagOrHash,
        ethhex,
        jsonrpc::{JsonRpcError, JsonRpcResult},
    },
};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallRequest {
    pub from: Option<Address>,
    pub to: Option<Address>,
    pub gas: Option<U256>,
    #[serde(flatten)]
    pub gas_price_details: GasPriceDetails,
    pub value: Option<U256>,
    #[serde(flatten)]
    pub input: CallInput,
    pub nonce: Option<U64>,
    pub chain_id: Option<U64>,
    pub access_list: Option<AccessList>,
    pub authorization_list: Option<Vec<SignedAuthorization>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_fee_per_blob_gas: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_versioned_hashes: Option<Vec<U256>>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub transaction_type: Option<U8>,
}

impl schemars::JsonSchema for CallRequest {
    fn schema_name() -> String {
        "CallRequest".to_string()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::CallRequest"))
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let schema = schemars::schema_for_value!(CallRequest {
            from: None,
            to: None,
            gas: None,
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::default()),
                max_priority_fee_per_gas: Some(U256::default())
            },
            value: None,
            input: CallInput::default(),
            nonce: None,
            chain_id: None,
            access_list: None,
            authorization_list: None,
            max_fee_per_blob_gas: None,
            blob_versioned_hashes: None,
            transaction_type: None,
        });
        schema.schema.into()
    }
}

impl CallRequest {
    pub fn max_fee_per_gas(&self) -> Option<U256> {
        match self.gas_price_details {
            GasPriceDetails::Legacy { gas_price } => Some(gas_price),
            GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(max_fee_per_gas),
                ..
            } => Some(max_fee_per_gas),
            _ => None,
        }
    }

    pub fn fill_gas_prices(&mut self, base_fee: U256) -> Result<(), JsonRpcError> {
        match self.gas_price_details {
            GasPriceDetails::Legacy { gas_price } => {
                if gas_price < base_fee {
                    self.gas_price_details = GasPriceDetails::Legacy {
                        gas_price: base_fee,
                    };
                }
            }
            GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
            } => {
                let effective_fee_cap = base_fee
                    .checked_add(max_priority_fee_per_gas.unwrap_or_default())
                    .ok_or_else(|| {
                        JsonRpcError::eth_call_error("tip too high".to_string(), None)
                    })?;

                let max_fee_per_gas = match max_fee_per_gas {
                    Some(mut max_fee_per_gas) => {
                        if max_fee_per_gas == U256::ZERO {
                            max_fee_per_gas = base_fee;
                        } else if max_fee_per_gas < base_fee {
                            return Err(JsonRpcError::eth_call_error(
                                "max fee per gas less than block base fee".to_string(),
                                None,
                            ));
                        }

                        if let Some(max_priority_fee_per_gas) = max_priority_fee_per_gas {
                            if max_fee_per_gas < max_priority_fee_per_gas {
                                return Err(JsonRpcError::eth_call_error(
                                    "priority fee greater than max".to_string(),
                                    None,
                                ));
                            }
                        }

                        min(max_fee_per_gas, effective_fee_cap)
                    }
                    None => effective_fee_cap,
                };

                self.gas_price_details = GasPriceDetails::Eip1559 {
                    max_fee_per_gas: Some(max_fee_per_gas),
                    max_priority_fee_per_gas,
                };
            }
        };

        Ok(())
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CallInput {
    /// Transaction data
    pub input: Option<alloy_primitives::Bytes>,

    /// This is the same as `input` but is used for backwards compatibility:
    /// <https://github.com/ethereum/go-ethereum/issues/15628>
    pub data: Option<alloy_primitives::Bytes>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged, rename_all_fields = "camelCase")]
pub enum GasPriceDetails {
    Legacy {
        gas_price: U256,
    },
    Eip1559 {
        max_fee_per_gas: Option<U256>,
        max_priority_fee_per_gas: Option<U256>,
    },
}

impl Default for GasPriceDetails {
    fn default() -> Self {
        GasPriceDetails::Eip1559 {
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
        }
    }
}

/// Optimistically create a typed Ethereum transaction from a CallRequest based on provided fields.
/// TODO: add support for other transaction types.
impl TryFrom<CallRequest> for TxEnvelope {
    type Error = JsonRpcError;
    fn try_from(call_request: CallRequest) -> Result<Self, JsonRpcError> {
        let CallRequest {
            from,
            to,
            gas,
            gas_price_details,
            value,
            input: CallInput { input, data },
            nonce,
            chain_id,
            access_list,
            authorization_list,
            max_fee_per_blob_gas,
            blob_versioned_hashes,
            transaction_type,
        } = call_request;

        let nonce = nonce
            .unwrap_or_default()
            .try_into()
            .map_err(|_| JsonRpcError::invalid_params())?;

        let gas_limit = gas
            .unwrap_or(Uint::from(u64::MAX))
            .try_into()
            .map_err(|_| JsonRpcError::invalid_params())?;

        let value = value.unwrap_or_default();
        let input = input.unwrap_or_default();

        // default signature as eth_call doesn't require it
        let signature = Signature::new(U256::ZERO, U256::ZERO, false);

        match gas_price_details {
            GasPriceDetails::Legacy { gas_price } => {
                let transaction = TxLegacy {
                    chain_id: chain_id
                        .map(|id| id.try_into())
                        .transpose()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    nonce,
                    gas_price: gas_price
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    gas_limit,
                    to: if let Some(to) = to {
                        TxKind::Call(to)
                    } else {
                        // EIP-3860
                        check_contract_creation_size(Some(&input))?;
                        TxKind::Create
                    },
                    value,
                    input,
                };

                Ok(transaction.into_signed(signature).into())
            }
            GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
            } => {
                let chain_id = chain_id
                    .unwrap_or_default()
                    .try_into()
                    .map_err(|_| JsonRpcError::invalid_params())?;

                let max_fee_per_gas = max_fee_per_gas
                    .unwrap_or_default()
                    .try_into()
                    .map_err(|_| JsonRpcError::invalid_params())?;

                let max_priority_fee_per_gas = max_priority_fee_per_gas
                    .unwrap_or_default()
                    .try_into()
                    .map_err(|_| JsonRpcError::invalid_params())?;

                let access_list = access_list.unwrap_or_default();

                if let Some(authorization_list) = authorization_list {
                    let transaction = TxEip7702 {
                        chain_id,
                        nonce,
                        gas_limit,
                        max_fee_per_gas,
                        max_priority_fee_per_gas,
                        to: to.ok_or(JsonRpcError::invalid_params())?,
                        value,
                        access_list,
                        authorization_list,
                        input,
                    };

                    Ok(transaction.into_signed(signature).into())
                } else {
                    let transaction = TxEip1559 {
                        chain_id,
                        nonce,
                        gas_limit,
                        max_fee_per_gas,
                        max_priority_fee_per_gas,
                        value,
                        to: if let Some(to) = to {
                            TxKind::Call(to)
                        } else {
                            // EIP-3860
                            check_contract_creation_size(Some(&input))?;
                            TxKind::Create
                        },
                        access_list,
                        input,
                    };

                    Ok(transaction.into_signed(signature).into())
                }
            }
        }
    }
}

// EIP-3860
pub fn check_contract_creation_size(input: Option<&Bytes>) -> Result<(), JsonRpcError> {
    let Some(code) = input else {
        return Ok(());
    };

    let max_code_size = MonadExecutionRevision::LATEST
        .execution_chain_params()
        .max_code_size;

    if code.len() > 2 * max_code_size {
        return Err(JsonRpcError::code_size_too_large(code.len()));
    }

    Ok(())
}

/// Merge the generated access list with caller-supplied entries for the
/// follow-up execution used to compute `gasUsed` and the optional result
/// `error`. The public response still uses the generated access list; ordered
/// collections keep this internal follow-up transaction deterministic while
/// preserving the existing address/storage-key deduplication.
pub fn merge_access_lists(generated: AccessList, original: Option<AccessList>) -> AccessList {
    let Some(original) = original else {
        return generated;
    };

    let mut access_map: BTreeMap<Address, BTreeSet<B256>> = BTreeMap::new();

    for item in generated.0.into_iter().chain(original.0) {
        access_map
            .entry(item.address)
            .or_default()
            .extend(item.storage_keys);
    }

    let merged_items: Vec<AccessListItem> = access_map
        .into_iter()
        .map(|(address, storage_keys)| AccessListItem {
            address,
            storage_keys: storage_keys.into_iter().collect(),
        })
        .collect();

    AccessList(merged_items)
}

/// Populate gas limit and gas prices
pub async fn fill_gas_params<T: Triedb>(
    triedb_env: &T,
    block_key: BlockKey,
    tx: &mut CallRequest,
    header: &mut Header,
    state_overrides: &StateOverrideSet,
    eth_call_provider_gas_limit: U256,
) -> Result<(), JsonRpcError> {
    // Geth checks that the sender can pay for gas if gas price is populated.
    // Set the base fee to zero if gas price is not populated.
    // https://github.com/ethereum/go-ethereum/pull/20783
    match tx.gas_price_details {
        GasPriceDetails::Legacy { gas_price: _ }
        | GasPriceDetails::Eip1559 {
            max_fee_per_gas: Some(_),
            ..
        } => {
            tx.fill_gas_prices(U256::from(header.base_fee_per_gas.unwrap_or_default()))?;

            if tx.gas.is_none() {
                let allowance =
                    sender_gas_allowance(triedb_env, block_key, header, tx, state_overrides)
                        .await?;
                tx.gas = Some(U256::from(allowance).min(eth_call_provider_gas_limit));
            }
        }
        _ => {
            header.base_fee_per_gas = Some(0);
            tx.fill_gas_prices(U256::ZERO)?;
            if tx.gas.is_none() {
                tx.gas = Some(U256::from(header.gas_limit).min(eth_call_provider_gas_limit));
            }
        }
    }
    Ok(())
}

/// Subtract the effective gas price from the balance to get an accurate gas limit.
pub async fn sender_gas_allowance<T: Triedb>(
    triedb_env: &T,
    block_key: BlockKey,
    header: &Header,
    request: &CallRequest,
    state_overrides: &StateOverrideSet,
) -> Result<u64, JsonRpcError> {
    let (Some(sender), Some(gas_price)) = (request.from, request.max_fee_per_gas()) else {
        return Ok(header.gas_limit);
    };

    if gas_price.is_zero() {
        return Ok(header.gas_limit);
    }

    let balance = match state_overrides
        .get(&sender)
        .and_then(|override_state| override_state.balance)
    {
        Some(balance) => balance,
        None => {
            let account = triedb_env
                .get_account(block_key, sender.into())
                .await
                .map_err(JsonRpcError::internal_error)?;
            U256::from(account.balance)
        }
    };

    if balance == U256::ZERO {
        return Err(JsonRpcError::insufficient_funds());
    }

    let gas_limit = balance
        .checked_sub(request.value.unwrap_or_default())
        .ok_or_else(JsonRpcError::insufficient_funds)?
        .checked_div(gas_price)
        .ok_or_else(|| JsonRpcError::internal_error("zero gas price".into()))?;

    Ok(min(
        gas_limit.try_into().unwrap_or(header.gas_limit),
        header.gas_limit,
    ))
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MonadEthCallParams {
    transaction: CallRequest,
    #[serde(default)]
    block: BlockTagOrHash,
    #[schemars(skip)] // TODO: move StateOverrideSet from monad-cxx
    #[serde(default)]
    state_overrides: StateOverrideSet, // empty = no state overrides
}

#[derive(Deserialize, Debug, Default, schemars::JsonSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EnrichedTracerObject {
    #[serde(flatten)]
    tracer_params: TracerObject,
    #[schemars(skip)]
    #[serde(default)]
    state_overrides: StateOverrideSet,
}

#[derive(Debug, Deserialize, schemars::JsonSchema, Clone)]
pub struct MonadDebugTraceCallParams {
    transaction: CallRequest,
    #[serde(default)]
    block: BlockTagOrHash,
    tracer: EnrichedTracerObject,
}

#[derive(Debug, Deserialize, schemars::JsonSchema, Clone)]
pub struct MonadCreateAccessListParams {
    pub transaction: CallRequest,
    #[serde(default)]
    pub block: BlockTagOrHash,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub enum CallParams {
    Call(MonadEthCallParams),
    Trace(MonadDebugTraceCallParams),
    AccessList(MonadCreateAccessListParams),
}

impl CallParams {
    /// Destructure into the component parts needed by `prepare_eth_call`.
    fn into_parts(self) -> (CallRequest, BlockTagOrHash, StateOverrideSet, MonadTracer) {
        match self {
            CallParams::Call(p) => (
                p.transaction,
                p.block,
                p.state_overrides,
                MonadTracer::NoopTracer,
            ),
            CallParams::Trace(p) => (
                p.transaction,
                p.block,
                p.tracer.state_overrides,
                p.tracer.tracer_params.into(),
            ),
            CallParams::AccessList(p) => (
                p.transaction,
                p.block,
                StateOverrideSet::default(),
                MonadTracer::AccessListTracer,
            ),
        }
    }
}

/// Controls response shaping for provider-cap and execution out-of-gas cases.
/// Other call failures pass through and are handled by the RPC method.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutOfGasHandling {
    RpcError,
    ReturnAsCallFailure,
}

pub async fn prepare_eth_call<T: Triedb + TriedbPath>(
    triedb_env: &T,
    eth_call_handler_config: &EthCallHandlerConfig,
    eth_call_executor: &EthCallExecutor,
    chain_id: u64,
    params: CallParams,
) -> Result<(BlockKey, CallResult), JsonRpcError> {
    prepare_eth_call_with_out_of_gas_handling(
        triedb_env,
        eth_call_handler_config,
        eth_call_executor,
        chain_id,
        params,
        OutOfGasHandling::RpcError,
    )
    .await
}

#[tracing::instrument(level = "debug", name = "prepare_eth_call")]
async fn prepare_eth_call_with_out_of_gas_handling<T: Triedb + TriedbPath>(
    triedb_env: &T,
    eth_call_handler_config: &EthCallHandlerConfig,
    eth_call_executor: &EthCallExecutor,
    chain_id: u64,
    params: CallParams,
    out_of_gas_handling: OutOfGasHandling,
) -> Result<(BlockKey, CallResult), JsonRpcError> {
    prepare_eth_call_with_resolved_block(
        triedb_env,
        eth_call_handler_config,
        eth_call_executor,
        chain_id,
        params,
        out_of_gas_handling,
        None,
    )
    .await
}

async fn prepare_eth_call_with_resolved_block<T: Triedb + TriedbPath>(
    triedb_env: &T,
    eth_call_handler_config: &EthCallHandlerConfig,
    eth_call_executor: &EthCallExecutor,
    chain_id: u64,
    params: CallParams,
    out_of_gas_handling: OutOfGasHandling,
    resolved_block_key: Option<BlockKey>,
) -> Result<(BlockKey, CallResult), JsonRpcError> {
    // When a block key is already resolved, the block tag carried inside
    // `params` is ignored. This is used by `eth_createAccessList` to pin its
    // traced pass and follow-up execution to the same block.
    let (mut tx, block_tag, state_overrides, tracer) = params.into_parts();

    tx.input.input = match (tx.input.input.take(), tx.input.data.take()) {
        (Some(input), Some(data)) => {
            if input != data {
                return Err(JsonRpcError::invalid_params());
            }
            Some(input)
        }
        (None, data) | (data, None) => data,
    };

    let provider_gas_limit = U256::from(eth_call_handler_config.provider_gas_limit_eth_call);
    if tx.gas > Some(provider_gas_limit) {
        match out_of_gas_handling {
            OutOfGasHandling::RpcError => {
                return Err(JsonRpcError::eth_call_error(
                    "user-specified gas exceeds provider limit".to_string(),
                    None,
                ));
            }
            OutOfGasHandling::ReturnAsCallFailure => {
                // Geth caps eth_createAccessList gas above RPCGasCap instead
                // of rejecting it before execution.
                tx.gas = Some(provider_gas_limit);
            }
        }
    }

    // TODO: check duplicate address, duplicate storage key, etc.

    let block_key = match resolved_block_key {
        Some(block_key) => block_key,
        None => get_block_key_from_tag_or_hash(triedb_env, block_tag)
            .await
            .ok_or_else(JsonRpcError::block_not_found)?,
    };

    let mut header = match triedb_env
        .get_block_header(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => return Err(JsonRpcError::block_not_found()),
    };

    let gas_specified = tx.gas.is_some();
    let original_tx_gas = tx.gas.unwrap_or(U256::from(header.header.gas_limit));
    let eth_call_provider_gas_limit = eth_call_handler_config
        .provider_gas_limit_eth_call
        .min(header.header.gas_limit);
    fill_gas_params(
        triedb_env,
        block_key,
        &mut tx,
        &mut header.header,
        &state_overrides,
        U256::from(eth_call_provider_gas_limit),
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
    let tx_chain_id = tx.chain_id.expect("chain_id was set above").to::<u64>();
    let ethcall_chain_id = parse_ethcall_chain_id(tx_chain_id)?;
    let txn: TxEnvelope = tx.try_into()?;
    let (block_number, block_id) = match block_key {
        BlockKey::Finalized(FinalizedBlockKey(SeqNum(n))) => (n, None),
        BlockKey::Proposed(ProposedBlockKey(SeqNum(n), BlockId(Hash(id)))) => (n, Some(id)),
    };

    let header_gas_limit = header.header.gas_limit;

    match eth_call(
        EthCallRequest {
            chain_id: ethcall_chain_id,
            transaction: &txn,
            block_header: &header.header,
            sender,
            block_number,
            block_id,
            state_override_set: &state_overrides,
            tracer,
            gas_specified,
        },
        eth_call_executor,
    )
    .await
    {
        CallResult::Failure(error) => {
            if matches!(error.error_code, EthCallResult::OutOfGas) {
                handle_out_of_gas_failure(
                    error,
                    eth_call_provider_gas_limit,
                    header_gas_limit,
                    original_tx_gas,
                    out_of_gas_handling,
                )
                .map(|result| (block_key, result))
            } else {
                Ok((block_key, CallResult::Failure(error)))
            }
        }
        result => Ok((block_key, result)),
    }
}

/// Applies RPC-level out-of-gas handling after execution has been attempted.
///
/// Most `eth_call` users expect out-of-gas to be a top-level JSON-RPC error,
/// but `eth_createAccessList` must return execution failures in the result
/// object's `error` field after the access list has been generated.
fn handle_out_of_gas_failure(
    mut error: monad_ethcall::FailureCallResult,
    eth_call_provider_gas_limit: u64,
    header_gas_limit: u64,
    original_tx_gas: U256,
    out_of_gas_handling: OutOfGasHandling,
) -> Result<CallResult, JsonRpcError> {
    if out_of_gas_handling == OutOfGasHandling::RpcError
        && eth_call_provider_gas_limit < header_gas_limit
        && U256::from(eth_call_provider_gas_limit) < original_tx_gas
    {
        // Normal eth_call distinguishes provider-cap exhaustion from EVM
        // out-of-gas. eth_createAccessList follows geth by returning capped
        // execution failures in the result object.
        return Err(JsonRpcError::eth_call_error(
            "provider-specified max eth_call gas limit exceeded".to_string(),
            None,
        ));
    }

    match out_of_gas_handling {
        OutOfGasHandling::RpcError => {
            Err(JsonRpcError::eth_call_error("out of gas".to_string(), None))
        }
        OutOfGasHandling::ReturnAsCallFailure => {
            error.message = "out of gas".to_string();
            error.data = None;
            Ok(CallResult::Failure(error))
        }
    }
}

/// Executes a new message call immediately without creating a transaction on the block chain.
#[tracing::instrument(level = "debug", skip(data_provider))]
#[rpc(
    method = "eth_call",
    ignore = "eth_call_handler_config",
    ignore = "eth_call_executor",
    ignore = "chain_id"
)]
pub async fn monad_eth_call<T: Triedb + TriedbPath>(
    data_provider: &DataProvider<T>,
    eth_call_handler_config: &EthCallHandlerConfig,
    eth_call_executor: &EthCallExecutor,
    chain_id: u64,
    params: MonadEthCallParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_call: {params:?}");

    let (_, result) = prepare_eth_call(
        &data_provider.triedb_env,
        eth_call_handler_config,
        eth_call_executor,
        chain_id,
        CallParams::Call(params),
    )
    .await?;
    match result {
        CallResult::Success(monad_ethcall::SuccessCallResult { output_data, .. }) => {
            Ok(ethhex::encode_bytes(&output_data))
        }
        CallResult::Failure(error) => Err(JsonRpcError::eth_call_error(error.message, error.data)),
        _ => Err(JsonRpcError::internal_error(
            "Unexpected CallResult type".into(),
        )),
    }
}

/// Returns the tracing result by executing an eth call.
#[rpc(
    method = "debug_traceCall",
    ignore = "eth_call_handler_config",
    ignore = "eth_call_executor",
    ignore = "chain_id"
)]
#[allow(non_snake_case)]
pub async fn monad_debug_traceCall<T: Triedb + TriedbPath>(
    data_provider: &DataProvider<T>,
    eth_call_handler_config: &EthCallHandlerConfig,
    eth_call_executor: &EthCallExecutor,
    chain_id: u64,
    params: MonadDebugTraceCallParams,
) -> JsonRpcResult<Box<RawValue>> {
    debug!(?params, "monad_debug_traceCall");

    let tracer_params = params.tracer.tracer_params;
    let tracer: MonadTracer = tracer_params.into();

    let (block_key, call_result) = prepare_eth_call(
        &data_provider.triedb_env,
        eth_call_handler_config,
        eth_call_executor,
        chain_id,
        CallParams::Trace(params),
    )
    .await?;
    let raw_payload: Vec<u8> = match call_result {
        CallResult::Success(monad_ethcall::SuccessCallResult { output_data, .. }) => output_data,
        CallResult::Failure(error) => {
            return Err(JsonRpcError::eth_call_error(error.message, error.data))
        }
        CallResult::Revert(result) => result.trace,
    };

    match tracer {
        MonadTracer::CallTracer => {
            let mut slice: &[u8] = raw_payload.as_slice();
            let frame = decode_call_frame(
                &data_provider.triedb_env,
                &mut slice,
                block_key,
                &tracer_params,
            )
            .await?;
            serde_json::value::to_raw_value(&frame).map_err(|e| {
                JsonRpcError::internal_error(format!("json serialization error: {}", e))
            })
        }

        MonadTracer::PreStateTracer | MonadTracer::StateDiffTracer => {
            let v: serde_cbor::Value = serde_cbor::from_slice(&raw_payload)
                .map_err(|e| JsonRpcError::internal_error(format!("cbor decode error: {}", e)))?;
            serde_json::value::to_raw_value(&v).map_err(|e| {
                JsonRpcError::internal_error(format!("json serialization error: {}", e))
            })
        }

        MonadTracer::AccessListTracer => Err(JsonRpcError::invalid_params()),

        MonadTracer::NoopTracer => Ok(Box::<RawValue>::default()),
    }
}

/// Returns an access list containing all addresses and storage slots accessed during a simulated transaction.
#[rpc(
    method = "eth_createAccessList",
    ignore = "eth_call_handler_config",
    ignore = "eth_call_executor",
    ignore = "chain_id"
)]
#[allow(non_snake_case)]
pub async fn monad_createAccessList<T: Triedb + TriedbPath>(
    data_provider: &DataProvider<T>,
    eth_call_handler_config: &EthCallHandlerConfig,
    eth_call_executor: &EthCallExecutor,
    chain_id: u64,
    params: MonadCreateAccessListParams,
) -> JsonRpcResult<Box<RawValue>> {
    trace!("monad_createAccessList: {params:?}");

    let mut follow_up_tx = params.transaction.clone();
    let original_access_list = params.transaction.access_list.clone();
    let block = params.block.clone();

    let (block_key, call_result) = prepare_eth_call_with_out_of_gas_handling(
        &data_provider.triedb_env,
        eth_call_handler_config,
        eth_call_executor,
        chain_id,
        CallParams::AccessList(params),
        OutOfGasHandling::ReturnAsCallFailure,
    )
    .await?;
    let access_list = access_list_from_trace_call_result(call_result)?;

    // This intentionally uses one traced pass plus one follow-up execution.
    // Geth repeats tracing until the access list stabilizes, which can discover
    // extra slots when warming the first pass lets gas-sensitive code run
    // further. Keeping this single-pass behavior avoids expanding the scope of
    // this compatibility fix.
    //
    // The public result uses the traced access list. Monad execution warms
    // tx-initial access-list entries before tracing, so the trace already
    // includes caller-supplied entries while preserving execution-side
    // filtering. The merge here is only for the follow-up execution call.
    follow_up_tx.access_list = Some(merge_access_lists(
        access_list.clone(),
        original_access_list,
    ));

    let call_params = MonadEthCallParams {
        transaction: follow_up_tx,
        block,
        state_overrides: StateOverrideSet::default(),
    };

    let (_, call_result) = prepare_eth_call_with_resolved_block(
        &data_provider.triedb_env,
        eth_call_handler_config,
        eth_call_executor,
        chain_id,
        CallParams::Call(call_params),
        OutOfGasHandling::ReturnAsCallFailure,
        Some(block_key),
    )
    .await?;
    let result = access_list_result_from_call_result(access_list, call_result)?;

    serde_json::value::to_raw_value(&result)
        .map_err(|e| JsonRpcError::internal_error(format!("json serialization error: {}", e)))
}

fn decode_access_list_trace(raw_payload: &[u8]) -> Result<AccessList, JsonRpcError> {
    if raw_payload.is_empty() {
        return Ok(AccessList::default());
    }

    serde_cbor::from_slice(raw_payload)
        .map_err(|e| JsonRpcError::internal_error(format!("failed to decode access list: {}", e)))
}

/// Extracts access-list tracer output from a simulated execution.
///
/// A tracer `Revert` still contains the state trace produced before execution
/// failed, so it is decoded the same way as a successful trace. A `Failure`
/// has no trace payload to decode, including out-of-gas failures shaped as call
/// failures, so it remains a top-level RPC error.
fn access_list_from_trace_call_result(call_result: CallResult) -> Result<AccessList, JsonRpcError> {
    match call_result {
        CallResult::Success(monad_ethcall::SuccessCallResult { output_data, .. }) => {
            decode_access_list_trace(&output_data)
        }
        CallResult::Failure(error) => Err(JsonRpcError::eth_call_error(error.message, error.data)),
        CallResult::Revert(result) => decode_access_list_trace(&result.trace),
    }
}

fn create_access_list_error_message(error: &monad_ethcall::FailureCallResult) -> String {
    // `eth_call` reports ABI-decoded revert reasons as a top-level error, but
    // geth's `eth_createAccessList` returns the raw VM error string instead.
    if error.error_code == EthCallResult::ExecutionError
        && error.message.starts_with("execution reverted:")
    {
        "execution reverted".to_string()
    } else {
        error.message.clone()
    }
}

/// Builds the public `eth_createAccessList` result from the follow-up call.
///
/// Unlike `eth_call`, execution failures are reported in the result object's
/// `error` field so callers can still use the generated access list.
fn access_list_result_from_call_result(
    access_list: AccessList,
    call_result: CallResult,
) -> Result<AccessListResult, JsonRpcError> {
    match call_result {
        CallResult::Success(monad_ethcall::SuccessCallResult { gas_used, .. }) => {
            Ok(AccessListResult {
                access_list,
                gas_used: U256::from(gas_used),
                error: None,
            })
        }
        CallResult::Failure(error) => match error.error_code {
            EthCallResult::OutOfGas
            | EthCallResult::ExecutionError
            | EthCallResult::ReserveBalanceViolation => Ok(AccessListResult {
                access_list,
                gas_used: U256::from(error.gas_used),
                error: Some(create_access_list_error_message(&error)),
            }),
            EthCallResult::OtherError => {
                Err(JsonRpcError::eth_call_error(error.message, error.data))
            }
            EthCallResult::Success => Err(JsonRpcError::internal_error(
                "unexpected successful eth_call failure".into(),
            )),
        },
        CallResult::Revert(_) => {
            debug_assert!(
                false,
                "NoopTracer follow-up call unexpectedly returned traced revert data"
            );
            Err(JsonRpcError::internal_error(
                "unexpected traced revert from NoopTracer eth_createAccessList follow-up call"
                    .into(),
            ))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EthCallCapacityStats {
    pub inactive_executors: usize,
    pub queued_requests: usize,
    pub oldest_request_age_ms: u64,
    pub average_request_age_ms: u64,
    pub total_requests: u64,
    pub total_errors: u64,
    pub queue_rejections: u64,
}

/// Returns statistics about eth_call capacity including inactive executors and queued requests
#[allow(non_snake_case)]
#[tracing::instrument(level = "debug")]
#[monad_rpc_docs::rpc(
    method = "admin_ethCallStatistics",
    ignore = "config,available_permits"
)]
pub async fn monad_admin_ethCallStatistics(
    config: &EthCallHandlerConfig,
    available_permits: usize,
    stats_tracker: &EthCallStatsTracker,
) -> JsonRpcResult<EthCallCapacityStats> {
    let active_requests = config.max_concurrent_permits - available_permits;
    let executor_fibers = config.pool_low.num_fibers as usize;

    let inactive_executors = executor_fibers.saturating_sub(active_requests);

    let queued_requests = active_requests.saturating_sub(executor_fibers);

    let (max_age, avg_age, cumulative_stats) = stats_tracker.get_stats();

    Ok(EthCallCapacityStats {
        inactive_executors,
        queued_requests,
        oldest_request_age_ms: max_age.map(|d| d.as_millis() as u64).unwrap_or(0),
        average_request_age_ms: avg_age.map(|d| d.as_millis() as u64).unwrap_or(0),
        total_requests: cumulative_stats.total_requests,
        total_errors: cumulative_stats.total_errors,
        queue_rejections: cumulative_stats.queue_rejections,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use alloy_consensus::{Header, TxEnvelope};
    use alloy_primitives::{Address, Bytes, B256, U256};
    use alloy_rpc_types::{AccessList, AccessListItem};
    use monad_chain_config::execution_revision::MonadExecutionRevision;
    use monad_ethcall::{
        CallResult, EthCallResult, FailureCallResult, RevertCallResult, StateOverrideObject,
        StateOverrideSet, SuccessCallResult,
    };
    use monad_triedb_utils::{
        mock_triedb::MockTriedb,
        triedb_env::{BlockKey, FinalizedBlockKey},
    };
    use monad_types::SeqNum;
    use serde_json::{from_str, json};

    use super::{fill_gas_params, CallRequest, GasPriceDetails};
    use crate::{
        handlers::{
            debug::Tracer,
            eth::call::{
                access_list_from_trace_call_result, access_list_result_from_call_result,
                decode_access_list_trace, handle_out_of_gas_failure, merge_access_lists,
                sender_gas_allowance, CallInput, MonadDebugTraceCallParams, OutOfGasHandling,
            },
        },
        types::jsonrpc::JsonRpcError,
    };

    fn sample_access_list() -> AccessList {
        AccessList(vec![AccessListItem {
            address: Address::from([0x11; 20]),
            storage_keys: vec![B256::from([0x22; 32])],
        }])
    }

    fn access_list_contains(access_list: &AccessList, address: Address, storage_key: B256) -> bool {
        access_list
            .0
            .iter()
            .any(|item| item.address == address && item.storage_keys.contains(&storage_key))
    }

    fn cbor_access_list(access_list: &AccessList) -> Vec<u8> {
        serde_cbor::to_vec(access_list).expect("access list encodes as CBOR")
    }

    fn out_of_gas_failure() -> FailureCallResult {
        FailureCallResult {
            error_code: EthCallResult::OutOfGas,
            gas_used: 12_345,
            gas_refund: 0,
            message: "execution reverted".into(),
            data: Some("0x".into()),
        }
    }

    fn failed_call_result(error_code: EthCallResult, gas_used: u64, message: &str) -> CallResult {
        CallResult::Failure(FailureCallResult {
            error_code,
            gas_used,
            gas_refund: 0,
            message: message.into(),
            data: None,
        })
    }

    #[test]
    fn decode_access_list_trace_empty_payload() {
        let access_list = decode_access_list_trace(&[]).expect("empty trace decodes");
        assert_eq!(access_list, AccessList::default());
    }

    #[test]
    fn decode_access_list_trace_cbor_payload() {
        let expected = sample_access_list();
        let payload = cbor_access_list(&expected);

        let access_list = decode_access_list_trace(&payload).expect("access list trace decodes");

        assert_eq!(access_list, expected);
    }

    #[test]
    fn access_list_from_successful_tracer_result() {
        let expected = sample_access_list();
        let payload = cbor_access_list(&expected);

        let access_list =
            access_list_from_trace_call_result(CallResult::Success(SuccessCallResult {
                output_data: payload,
                ..Default::default()
            }))
            .expect("successful tracer result decodes");

        assert_eq!(access_list, expected);
    }

    #[test]
    fn access_list_from_reverted_tracer_result() {
        let expected = sample_access_list();
        let payload = cbor_access_list(&expected);

        let access_list =
            access_list_from_trace_call_result(CallResult::Revert(RevertCallResult {
                trace: payload,
            }))
            .expect("reverted tracer result decodes");

        assert_eq!(access_list, expected);
    }

    #[test]
    fn access_list_from_failed_tracer_result_stays_rpc_error() {
        let err = access_list_from_trace_call_result(CallResult::Failure(FailureCallResult {
            error_code: EthCallResult::OtherError,
            gas_used: 0,
            gas_refund: 0,
            message: "failed to apply transaction".into(),
            data: Some("0xdead".into()),
        }))
        .expect_err("access-list setup failures stay top-level RPC errors");

        assert_eq!(
            err,
            JsonRpcError::eth_call_error(
                "failed to apply transaction".into(),
                Some("0xdead".into())
            )
        );
    }

    #[test]
    fn access_list_result_records_follow_up_success() {
        let access_list = sample_access_list();
        let call_result = CallResult::Success(SuccessCallResult {
            gas_used: 21_000,
            ..Default::default()
        });

        let result = access_list_result_from_call_result(access_list.clone(), call_result)
            .expect("successful follow-up call produces access-list result");

        assert_eq!(result.access_list, access_list);
        assert_eq!(result.gas_used, U256::from(21_000));
        assert_eq!(result.error, None);
    }

    #[test]
    fn merge_access_lists_keeps_user_supplied_entries_for_follow_up_call() {
        let generated_address = Address::from([0x11; 20]);
        let generated_key = B256::from([0x22; 32]);
        let user_supplied_address = Address::from([0x33; 20]);
        let user_supplied_key = B256::from([0x44; 32]);

        let generated = AccessList(vec![AccessListItem {
            address: generated_address,
            storage_keys: vec![generated_key],
        }]);
        let user_supplied = AccessList(vec![AccessListItem {
            address: user_supplied_address,
            storage_keys: vec![user_supplied_key],
        }]);

        let merged = merge_access_lists(generated, Some(user_supplied));

        assert!(access_list_contains(
            &merged,
            generated_address,
            generated_key
        ));
        assert!(access_list_contains(
            &merged,
            user_supplied_address,
            user_supplied_key
        ));
    }

    #[test]
    fn access_list_result_records_follow_up_execution_failures() {
        for (error_code, gas_used, message, expected_message) in [
            (
                EthCallResult::ExecutionError,
                54_321,
                "execution reverted: abi error",
                "execution reverted",
            ),
            (EthCallResult::OutOfGas, 12_345, "out of gas", "out of gas"),
            (
                EthCallResult::ReserveBalanceViolation,
                23_456,
                "reserve balance violation",
                "reserve balance violation",
            ),
        ] {
            let access_list = sample_access_list();
            let call_result = failed_call_result(error_code, gas_used, message);

            let result = access_list_result_from_call_result(access_list.clone(), call_result)
                .expect("execution failure is represented in access list result");

            assert_eq!(result.access_list, access_list);
            assert_eq!(result.gas_used, U256::from(gas_used));
            assert_eq!(result.error.as_deref(), Some(expected_message));
        }
    }

    #[test]
    fn access_list_result_preserves_other_error_as_rpc_error() {
        let access_list = sample_access_list();
        let call_result = CallResult::Failure(FailureCallResult {
            error_code: EthCallResult::OtherError,
            gas_used: 1,
            gas_refund: 0,
            message: "internal eth_call error".into(),
            data: Some("0xdead".into()),
        });

        let err = access_list_result_from_call_result(access_list, call_result)
            .expect_err("non-execution failures stay top-level RPC errors");

        assert_eq!(
            err,
            JsonRpcError::eth_call_error("internal eth_call error".into(), Some("0xdead".into()))
        );
    }

    #[test]
    fn create_access_list_revert_result_matches_execution_apis_shape() {
        let expected_access_list = sample_access_list();
        let trace_payload = cbor_access_list(&expected_access_list);

        let access_list =
            access_list_from_trace_call_result(CallResult::Revert(RevertCallResult {
                trace: trace_payload,
            }))
            .expect("reverted access-list trace still decodes");

        let result = access_list_result_from_call_result(
            access_list.clone(),
            CallResult::Failure(FailureCallResult {
                error_code: EthCallResult::ExecutionError,
                gas_used: 54_321,
                gas_refund: 0,
                message: "execution reverted: user error".into(),
                data: Some("0x08c379a0".into()),
            }),
        )
        .expect("reverting follow-up call is returned in the result object");

        assert_eq!(access_list, expected_access_list);
        assert_eq!(result.access_list, expected_access_list);
        assert_eq!(result.gas_used, U256::from(54_321));
        assert_eq!(result.error.as_deref(), Some("execution reverted"));

        let value = serde_json::to_value(&result).expect("access-list result serializes");
        let serialized_access_list = value["accessList"]
            .as_array()
            .expect("accessList is an array");
        assert!(
            !serialized_access_list.is_empty(),
            "accessList must be present even when execution reverts"
        );
        assert_eq!(value["error"], json!("execution reverted"));
        assert!(value.get("gasUsed").is_some(), "gasUsed must be present");
    }

    #[test]
    fn out_of_gas_default_handling_is_rpc_error() {
        let err = handle_out_of_gas_failure(
            out_of_gas_failure(),
            30_000_000,
            30_000_000,
            U256::from(30_000_000),
            OutOfGasHandling::RpcError,
        )
        .expect_err("eth_call reports out of gas as a JSON-RPC error");

        assert_eq!(err, JsonRpcError::eth_call_error("out of gas".into(), None));
    }

    #[test]
    fn out_of_gas_return_failure_handling_preserves_result() {
        let result = handle_out_of_gas_failure(
            out_of_gas_failure(),
            30_000_000,
            30_000_000,
            U256::from(30_000_000),
            OutOfGasHandling::ReturnAsCallFailure,
        )
        .expect("eth_createAccessList receives execution failures");

        let CallResult::Failure(error) = result else {
            panic!("expected failure result");
        };
        assert_eq!(error.error_code, EthCallResult::OutOfGas);
        assert_eq!(error.gas_used, 12_345);
        assert_eq!(error.message, "out of gas");
        assert_eq!(error.data, None);
    }

    #[test]
    fn out_of_gas_provider_limit_stays_rpc_error_for_eth_call() {
        let err = handle_out_of_gas_failure(
            out_of_gas_failure(),
            10_000_000,
            30_000_000,
            U256::from(30_000_001),
            OutOfGasHandling::RpcError,
        )
        .expect_err("eth_call provider gas cap failures stay top-level errors");

        assert_eq!(
            err,
            JsonRpcError::eth_call_error(
                "provider-specified max eth_call gas limit exceeded".into(),
                None
            )
        );
    }

    #[test]
    fn out_of_gas_provider_limit_is_result_failure_for_create_access_list() {
        let result = handle_out_of_gas_failure(
            out_of_gas_failure(),
            10_000_000,
            30_000_000,
            U256::from(30_000_001),
            OutOfGasHandling::ReturnAsCallFailure,
        )
        .expect("eth_createAccessList reports provider-cap OOG in the result");

        let CallResult::Failure(error) = result else {
            panic!("expected failure result");
        };
        assert_eq!(error.error_code, EthCallResult::OutOfGas);
        assert_eq!(error.gas_used, 12_345);
        assert_eq!(error.message, "out of gas");
        assert_eq!(error.data, None);
    }

    #[test]
    fn parse_call_request_with_tracer() {
        let raw = r#"
        [
          {
            "from": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
            "to": "0x0000000000a39bb272e79075ade125fd351887ac",
            "gas": "0x1E9EF",
            "gasPrice": "0xBD32B2ABC",
            "data": "0xd0e30db0"
          },
          "latest",
          { "tracer": "prestateTracer" }
        ]
        "#;

        let params: MonadDebugTraceCallParams = from_str(raw).unwrap();

        assert_eq!(params.tracer.tracer_params.tracer, Tracer::PreStateTracer);
    }

    #[test]
    fn parse_call_request() {
        let payload = json!(
            {
                "from": "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
                "to": "0xd46e8dd67c5d32be8058bb8eb970870f07244567",
                "gas": "0x76c0",
                "gasPrice": "0x9184e72a000",
                "value": "0x9184e72a",
                "data": "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
            }
        );
        let result = serde_json::from_value::<super::CallRequest>(payload).expect("parse failed");
        assert!(result.input.data.is_some());
        assert!(matches!(
            result.gas_price_details,
            super::GasPriceDetails::Legacy { gas_price: _ }
        ));
        assert_eq!(
            result.max_fee_per_gas(),
            Some(U256::from_str_radix("9184e72a000", 16).unwrap())
        );

        let payload = json!(
            {
                "from": "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
                "to": "0xd46e8dd67c5d32be8058bb8eb970870f07244567",
                "gas": "0x76c0",
                "maxFeePerGas": "0x9184e72a000",
                "value": "0x9184e72a",
                "data": "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
            }
        );
        let result = serde_json::from_value::<super::CallRequest>(payload).expect("parse failed");
        assert!(matches!(
            result.gas_price_details,
            super::GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(_),
                ..
            }
        ));
        assert_eq!(
            result.max_fee_per_gas(),
            Some(U256::from_str_radix("9184e72a000", 16).unwrap())
        );
    }

    #[tokio::test]
    async fn test_fill_gas_params() {
        let mock_triedb = MockTriedb::default();

        // when gas price is not populated, then
        // (1) header base fee is set to zero and (2) tx gas limit is set to block gas limit
        let mut call_request = CallRequest::default();
        let mut header = Header {
            base_fee_per_gas: Some(10_000_000_000),
            gas_limit: 300_000_000,
            ..Default::default()
        };
        let block_key = BlockKey::Finalized(FinalizedBlockKey(SeqNum(header.number)));
        let state_overrides = StateOverrideSet::default();

        let result = fill_gas_params(
            &mock_triedb,
            block_key,
            &mut call_request,
            &mut header,
            &state_overrides,
            U256::MAX,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(call_request.gas, Some(U256::from(300_000_000)));
        assert_eq!(header.base_fee_per_gas, Some(0));

        // when gas price is populated but sender address is not populated, then
        // (1) tx gas limit is set to block gas limit
        let mut call_request = CallRequest {
            gas_price_details: GasPriceDetails::Legacy {
                gas_price: U256::from(1_000_000),
            },
            ..Default::default()
        };
        let mut header = Header {
            base_fee_per_gas: Some(100_000),
            gas_limit: 300_000_000,
            ..Default::default()
        };
        let result = fill_gas_params(
            &mock_triedb,
            block_key,
            &mut call_request,
            &mut header,
            &state_overrides,
            U256::MAX,
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(call_request.gas, Some(U256::from(300_000_000)));

        // when gas price is populated and sender address is populated, then
        // (1) check whether user has sufficient balance
        let mut call_request = CallRequest {
            from: Some(Address::default()),
            gas_price_details: GasPriceDetails::Legacy {
                gas_price: U256::from(1_000_000),
            },
            ..Default::default()
        };
        let mut header = Header {
            base_fee_per_gas: Some(10_000_000_000),
            gas_limit: 300_000_000,
            ..Default::default()
        };
        let result = fill_gas_params(
            &mock_triedb,
            block_key,
            &mut call_request,
            &mut header,
            &state_overrides,
            U256::MAX,
        )
        .await;
        assert!(result.is_err());

        // when gas price is populated and higher than artificial gas limit,
        // (1) tx gas limit is set to artificial gas limit
        let mut call_request = CallRequest::default();
        let mut header = Header {
            base_fee_per_gas: Some(10_000_000_000),
            gas_limit: 300_000_000,
            ..Default::default()
        };
        let block_key = BlockKey::Finalized(FinalizedBlockKey(SeqNum(header.number)));
        let state_overrides = StateOverrideSet::default();

        let result = fill_gas_params(
            &mock_triedb,
            block_key,
            &mut call_request,
            &mut header,
            &state_overrides,
            U256::from(100),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(call_request.gas, Some(U256::from(100)));
    }

    #[test]
    fn test_fill_gas_prices() {
        // when gas price is specified, returns error if gas price is less than block base fee
        let mut call_request = CallRequest {
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::from(50)),
                max_priority_fee_per_gas: None,
            },
            ..Default::default()
        };
        assert!(call_request.fill_gas_prices(U256::from(100)).is_err());

        // when gas price is not specified, do not return error, set maxFeePerGas to block base fee
        let mut call_request = CallRequest {
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
            },
            ..Default::default()
        };
        assert!(call_request.fill_gas_prices(U256::from(100)).is_ok());
        assert_eq!(call_request.max_fee_per_gas(), Some(U256::from(100)));

        // legacy transaction gas prices is not checked
        let mut call_request = CallRequest {
            gas_price_details: GasPriceDetails::Legacy {
                gas_price: U256::from(50),
            },
            ..Default::default()
        };
        assert!(call_request.fill_gas_prices(U256::from(100)).is_ok());
    }

    #[tokio::test]
    async fn test_sender_gas_allowance() {
        let mock_triedb = MockTriedb::default();

        // when both sender and gas price is populated, and balance override is specified
        // (1) use overriden balance to check gas allowance
        let gas_price = U256::from(2000);
        let call_request = CallRequest {
            from: Some(Address::ZERO),
            gas_price_details: GasPriceDetails::Legacy { gas_price },
            ..Default::default()
        };
        let header = Header {
            base_fee_per_gas: Some(1000),
            gas_limit: 300_000_000,
            ..Default::default()
        };
        let balance_override = U256::from(1_000_000);
        let mut overrides: StateOverrideSet = HashMap::new();
        overrides.insert(
            Address::ZERO,
            StateOverrideObject {
                balance: Some(balance_override),
                ..Default::default()
            },
        );

        let block_key = BlockKey::Finalized(FinalizedBlockKey(SeqNum(header.number)));
        let result =
            sender_gas_allowance(&mock_triedb, block_key, &header, &call_request, &overrides).await;
        let gas_limit = result.unwrap();
        assert_eq!(U256::from(gas_limit), balance_override / gas_price);
    }

    #[test]
    fn test_tx_envelope_try_from() {
        let max_code_size = MonadExecutionRevision::LATEST
            .execution_chain_params()
            .max_code_size;

        let call_request = CallRequest {
            input: CallInput {
                input: Some(Bytes::from(vec![3; 2 * max_code_size + 1])),
                data: None,
            },
            ..Default::default()
        };

        let result: Result<TxEnvelope, _> = call_request.try_into();
        assert_eq!(
            result,
            Err(JsonRpcError::code_size_too_large(2 * max_code_size + 1))
        );

        let call_request = CallRequest {
            input: CallInput {
                input: Some(Bytes::from(vec![3; 2 * max_code_size])),
                data: None,
            },
            ..Default::default()
        };

        let result: Result<TxEnvelope, _> = call_request.try_into();
        assert!(result.is_ok());
    }
}
