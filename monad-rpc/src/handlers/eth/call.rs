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
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use alloy_consensus::{Header, SignableTransaction, TxEip1559, TxEip7702, TxEnvelope, TxLegacy};
use alloy_eips::eip7702::SignedAuthorization;
use alloy_primitives::{Address, PrimitiveSignature, TxKind, Uint, U256, U64, U8};
use alloy_rpc_types::AccessList;
use monad_chain_config::execution_revision::MonadExecutionRevision;
use monad_ethcall::{eth_call, CallResult, EthCallExecutor, MonadTracer, StateOverrideSet};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{
    BlockKey, FinalizedBlockKey, ProposedBlockKey, Triedb, TriedbPath,
};
use monad_types::{BlockId, Hash, SeqNum};
use serde::{Deserialize, Serialize};
use serde_cbor;
use serde_json::value::RawValue;
use tokio::sync::Mutex;
use tracing::{info, trace};

use super::block::get_block_key_from_tag_or_hash;
use crate::{
    eth_json_types::BlockTagOrHash,
    handlers::debug::{decode_call_frame, Tracer, TracerObject},
    hex,
    jsonrpc::{JsonRpcError, JsonRpcResult},
    timing::RequestId,
};

#[derive(Debug)]
struct EthCallRequestStats {
    entry_time: Instant,
}

#[derive(Debug)]
struct CumulativeStats {
    total_requests: AtomicU64,
    total_errors: AtomicU64,
    queue_rejections: AtomicU64,
}

impl Default for CumulativeStats {
    fn default() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            queue_rejections: AtomicU64::new(0),
        }
    }
}

impl Clone for CumulativeStats {
    fn clone(&self) -> Self {
        Self {
            total_requests: AtomicU64::new(self.total_requests.load(Ordering::Relaxed)),
            total_errors: AtomicU64::new(self.total_errors.load(Ordering::Relaxed)),
            queue_rejections: AtomicU64::new(self.queue_rejections.load(Ordering::Relaxed)),
        }
    }
}

#[derive(Debug, Default)]
pub struct EthCallStatsTracker {
    active_requests: Arc<Mutex<HashMap<RequestId, EthCallRequestStats>>>,
    stats: CumulativeStats,
}

impl EthCallStatsTracker {
    pub async fn record_request_start(&self, request_id: RequestId) {
        let mut requests = self.active_requests.lock().await;
        requests.insert(
            request_id,
            EthCallRequestStats {
                entry_time: Instant::now(),
            },
        );

        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn record_request_complete(&self, request_id: &RequestId, is_error: bool) {
        let mut requests = self.active_requests.lock().await;
        requests.remove(request_id);

        if is_error {
            self.stats.total_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub async fn record_queue_rejection(&self) {
        self.stats.queue_rejections.fetch_add(1, Ordering::Relaxed);
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);
        self.stats.total_errors.fetch_add(1, Ordering::Relaxed);
    }

    async fn get_stats(&self) -> (Option<Duration>, Option<Duration>, CumulativeStats) {
        let requests = self.active_requests.lock().await;

        if requests.is_empty() {
            return (None, None, self.stats.clone());
        }

        let now = Instant::now();
        let mut max_age = Duration::ZERO;
        let mut total_age = Duration::ZERO;

        for stats in requests.values() {
            let age = now - stats.entry_time;
            max_age = max_age.max(age);
            total_age += age;
        }

        let avg_age = total_age / requests.len() as u32;

        (Some(max_age), Some(avg_age), self.stats.clone())
    }
}

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
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::NonGenericType"))
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
            GasPriceDetails::Legacy { mut gas_price } => {
                if gas_price < base_fee {
                    gas_price = base_fee;
                    self.gas_price_details = GasPriceDetails::Legacy { gas_price };
                }
            }
            GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
            } => {
                let max_fee_per_gas = match max_fee_per_gas {
                    Some(mut max_fee_per_gas) => {
                        if max_fee_per_gas != U256::ZERO && max_fee_per_gas < base_fee {
                            return Err(JsonRpcError::eth_call_error(
                                "max fee per gas less than block base fee".to_string(),
                                None,
                            ));
                        } else if max_fee_per_gas == U256::ZERO {
                            max_fee_per_gas = base_fee;
                        }

                        if max_priority_fee_per_gas.is_some()
                            && max_fee_per_gas < max_priority_fee_per_gas.unwrap_or_default()
                        {
                            return Err(JsonRpcError::eth_call_error(
                                "priority fee greater than max".to_string(),
                                None,
                            ));
                        }

                        min(
                            max_fee_per_gas,
                            base_fee
                                .checked_add(max_priority_fee_per_gas.unwrap_or_default())
                                .ok_or_else(|| {
                                    JsonRpcError::eth_call_error("tip too high".to_string(), None)
                                })?,
                        )
                    }
                    None => base_fee
                        .checked_add(max_priority_fee_per_gas.unwrap_or_default())
                        .ok_or_else(|| {
                            JsonRpcError::eth_call_error("tip too high".to_string(), None)
                        })?,
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
        match call_request {
            CallRequest {
                gas_price_details: GasPriceDetails::Legacy { gas_price },
                ..
            } => {
                // Legacy

                // default signature as eth_call doesn't require it
                let signature = PrimitiveSignature::new(U256::from(0), U256::from(0), false);
                let transaction = TxLegacy {
                    chain_id: call_request
                        .chain_id
                        .map(|id| id.try_into())
                        .transpose()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    nonce: call_request
                        .nonce
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    gas_price: gas_price
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    gas_limit: call_request
                        .gas
                        .unwrap_or(Uint::from(u64::MAX))
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    to: if let Some(to) = call_request.to {
                        TxKind::Call(to)
                    } else {
                        // EIP-3860
                        check_contract_creation_size(&call_request)?;
                        TxKind::Create
                    },
                    value: call_request.value.unwrap_or_default(),
                    input: call_request.input.input.unwrap_or_default(),
                };

                Ok(transaction.into_signed(signature).into())
            }
            CallRequest {
                authorization_list: Some(auth_list),
                gas_price_details:
                    GasPriceDetails::Eip1559 {
                        max_fee_per_gas,
                        max_priority_fee_per_gas,
                    },
                ..
            } => {
                // EIP-7702

                // default signature as eth_call doesn't require it
                let signature = PrimitiveSignature::new(U256::from(0), U256::from(0), false);
                let transaction = TxEip7702 {
                    chain_id: call_request
                        .chain_id
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    nonce: call_request
                        .nonce
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    max_fee_per_gas: max_fee_per_gas
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    max_priority_fee_per_gas: max_priority_fee_per_gas
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    gas_limit: call_request
                        .gas
                        .unwrap_or(Uint::from(u64::MAX))
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    access_list: call_request.access_list.unwrap_or_default(),
                    authorization_list: auth_list,
                    to: call_request.to.ok_or(JsonRpcError::invalid_params())?,
                    value: call_request.value.unwrap_or_default(),
                    input: call_request.input.input.unwrap_or_default(),
                };

                Ok(transaction.into_signed(signature).into())
            }
            CallRequest {
                gas_price_details:
                    GasPriceDetails::Eip1559 {
                        max_fee_per_gas,
                        max_priority_fee_per_gas,
                    },
                ..
            } => {
                // EIP-1559

                // default signature as eth_call doesn't require it
                let signature = PrimitiveSignature::new(U256::from(0), U256::from(0), false);
                let transaction = TxEip1559 {
                    chain_id: call_request
                        .chain_id
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    nonce: call_request
                        .nonce
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    max_fee_per_gas: max_fee_per_gas
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    max_priority_fee_per_gas: max_priority_fee_per_gas
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    gas_limit: call_request
                        .gas
                        .unwrap_or(Uint::from(u64::MAX))
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    to: if let Some(to) = call_request.to {
                        TxKind::Call(to)
                    } else {
                        // EIP-3860
                        check_contract_creation_size(&call_request)?;
                        TxKind::Create
                    },
                    value: call_request.value.unwrap_or_default(),
                    input: call_request.input.input.unwrap_or_default(),
                    access_list: call_request.access_list.unwrap_or_default(),
                };

                Ok(transaction.into_signed(signature).into())
            }
        }
    }
}

pub fn check_contract_creation_size(call_request: &CallRequest) -> Result<(), JsonRpcError> {
    // EIP-3860
    let max_code_size = MonadExecutionRevision::LATEST
        .execution_chain_params()
        .max_code_size;
    if let Some(code) = call_request.input.input.as_ref() {
        if code.len() > 2 * max_code_size {
            return Err(JsonRpcError::code_size_too_large(code.len()));
        }
    }
    Ok(())
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
    block: &Header,
    request: &CallRequest,
    state_overrides: &StateOverrideSet,
) -> Result<u64, JsonRpcError> {
    if let (Some(sender), Some(gas_price)) = (request.from, request.max_fee_per_gas()) {
        if gas_price.is_zero() {
            return Ok(block.gas_limit);
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
            gas_limit.try_into().unwrap_or(block.gas_limit),
            block.gas_limit,
        ))
    } else {
        Ok(block.gas_limit)
    }
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

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub enum CallParams {
    Call(MonadEthCallParams),
    Trace(MonadDebugTraceCallParams),
}

impl CallParams {
    /// Mutable handle to the embedded `CallRequest`.
    fn tx(&mut self) -> &mut CallRequest {
        match self {
            CallParams::Call(p) => &mut p.transaction,
            CallParams::Trace(p) => &mut p.transaction,
        }
    }

    fn block(&self) -> BlockTagOrHash {
        match self {
            CallParams::Call(p) => p.block.clone(),
            CallParams::Trace(p) => p.block.clone(),
        }
    }

    fn state_overrides(&self) -> &StateOverrideSet {
        match self {
            CallParams::Call(p) => &p.state_overrides,
            CallParams::Trace(p) => &p.tracer.state_overrides,
        }
    }

    fn monad_tracer(&self) -> MonadTracer {
        match self {
            CallParams::Call(_) => MonadTracer::NoopTracer,
            CallParams::Trace(p) => {
                if p.tracer.tracer_params.tracer == Tracer::CallTracer {
                    MonadTracer::CallTracer
                } else if p.tracer.tracer_params.config.diff_mode {
                    MonadTracer::StateDiffTracer
                } else {
                    MonadTracer::PreStateTracer
                }
            }
        }
    }

    /// `true` only for the trace variant.
    fn trace(&self) -> bool {
        matches!(self, CallParams::Trace(_))
    }
}

#[tracing::instrument(level = "debug")]
pub async fn prepare_eth_call<T: Triedb + TriedbPath>(
    triedb_env: &T,
    eth_call_executor: Arc<EthCallExecutor>,
    chain_id: u64,
    eth_call_provider_gas_limit: u64,
    mut params: CallParams,
) -> Result<CallResult, JsonRpcError> {
    params.tx().input.input = match (
        params.tx().input.input.take(),
        params.tx().input.data.take(),
    ) {
        (Some(input), Some(data)) => {
            if input != data {
                return Err(JsonRpcError::invalid_params());
            }
            Some(input)
        }
        (None, data) | (data, None) => data,
    };

    if params.tx().gas > Some(U256::from(eth_call_provider_gas_limit)) {
        return Err(JsonRpcError::eth_call_error(
            "user-specified gas exceeds provider limit".to_string(),
            None,
        ));
    }

    // TODO: check duplicate address, duplicate storage key, etc.

    let block_key = get_block_key_from_tag_or_hash(triedb_env, params.block()).await?;

    let mut header = match triedb_env
        .get_block_header(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => return Err(JsonRpcError::block_not_found()),
    };

    let state_overrides = params.state_overrides().clone();
    let gas_specified = params.tx().gas.is_some();
    let original_tx_gas = params
        .tx()
        .gas
        .unwrap_or(U256::from(header.header.gas_limit));
    let eth_call_provider_gas_limit = eth_call_provider_gas_limit.min(header.header.gas_limit);
    fill_gas_params(
        triedb_env,
        block_key,
        params.tx(),
        &mut header.header,
        &state_overrides,
        U256::from(eth_call_provider_gas_limit),
    )
    .await?;

    if let Some(tx_chain_id) = params.tx().chain_id {
        if tx_chain_id != U64::from(chain_id) {
            return Err(JsonRpcError::invalid_chain_id(
                chain_id,
                tx_chain_id.to::<u64>(),
            ));
        }
    } else {
        params.tx().chain_id = Some(U64::from(chain_id));
    }

    let sender = params.tx().from.unwrap_or_default();
    let tx_chain_id = params
        .tx()
        .chain_id
        .expect("chain id must be populated")
        .to::<u64>();
    let txn: TxEnvelope = params.tx().clone().try_into()?;
    let (block_number, block_id) = match block_key {
        BlockKey::Finalized(FinalizedBlockKey(SeqNum(n))) => (n, None),
        BlockKey::Proposed(ProposedBlockKey(SeqNum(n), BlockId(Hash(id)))) => (n, Some(id)),
    };

    let state_overrides = params.state_overrides().clone();
    let tracer = params.monad_tracer();
    let header_gas_limit = header.header.gas_limit;
    match eth_call(
        tx_chain_id,
        txn,
        header.header,
        sender,
        block_number,
        block_id,
        eth_call_executor,
        &state_overrides,
        tracer,
        gas_specified,
    )
    .await
    {
        monad_ethcall::CallResult::Failure(error)
            if matches!(error.error_code, monad_ethcall::EthCallResult::OutOfGas) =>
        {
            if eth_call_provider_gas_limit < header_gas_limit
                && U256::from(eth_call_provider_gas_limit) < original_tx_gas
            {
                return Err(JsonRpcError::eth_call_error(
                    "provider-specified max eth_call gas limit exceeded".to_string(),
                    None,
                ));
            }
            return Err(JsonRpcError::eth_call_error("out of gas".to_string(), None));
        }
        result => Ok(result),
    }
}

/// Executes a new message call immediately without creating a transaction on the block chain.
#[tracing::instrument(level = "debug")]
#[rpc(
    method = "eth_call",
    ignore = "eth_call_executor,chain_id,eth_call_provider_gas_limit"
)]
pub async fn monad_eth_call<T: Triedb + TriedbPath>(
    triedb_env: &T,
    eth_call_executor: Arc<EthCallExecutor>,
    chain_id: u64,
    eth_call_provider_gas_limit: u64,
    params: MonadEthCallParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_call: {params:?}");

    match prepare_eth_call(
        triedb_env,
        eth_call_executor,
        chain_id,
        eth_call_provider_gas_limit,
        CallParams::Call(params),
    )
    .await?
    {
        CallResult::Success(monad_ethcall::SuccessCallResult { output_data, .. }) => {
            Ok(hex::encode(&output_data))
        }
        CallResult::Failure(error) => Err(JsonRpcError::eth_call_error(error.message, error.data)),
        _ => Err(JsonRpcError::internal_error(
            "Unexpected CallResult type".into(),
        )),
    }
}

/// Returns the tracing result result by executing an eth call.
#[rpc(
    method = "debug_traceCall",
    ignore = "chain_id",
    ignore = "eth_call_executor",
    ignore = "eth_call_gas_limit"
)]
#[allow(non_snake_case)]
pub async fn monad_debug_traceCall<T: Triedb + TriedbPath>(
    triedb_env: &T,
    eth_call_executor: Arc<EthCallExecutor>,
    chain_id: u64,
    eth_call_gas_limit: u64,
    params: MonadDebugTraceCallParams,
) -> JsonRpcResult<Box<RawValue>> {
    info!("monad_debug_traceCall: {params:?}");

    let block_key = get_block_key_from_tag_or_hash(triedb_env, params.block.clone()).await?;

    let tracer = CallParams::Trace(params.clone()).monad_tracer();

    let raw_payload: Vec<u8> = match prepare_eth_call(
        triedb_env,
        eth_call_executor,
        chain_id,
        eth_call_gas_limit,
        CallParams::Trace(params.clone()),
    )
    .await?
    {
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
                triedb_env,
                &mut slice,
                block_key,
                &params.tracer.tracer_params,
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

        MonadTracer::NoopTracer => Ok(Box::<RawValue>::default()),
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
    ignore = "eth_call_executor_fibers,total_permits,available_permits"
)]
pub async fn monad_admin_ethCallStatistics(
    eth_call_executor_fibers: usize,
    total_permits: usize,
    available_permits: usize,
    stats_tracker: &EthCallStatsTracker,
) -> JsonRpcResult<EthCallCapacityStats> {
    let active_requests = total_permits - available_permits;

    let inactive_executors = eth_call_executor_fibers.saturating_sub(active_requests);

    let queued_requests = active_requests.saturating_sub(eth_call_executor_fibers);
    let (max_age, avg_age, cumulative_stats) = stats_tracker.get_stats().await;

    Ok(EthCallCapacityStats {
        inactive_executors,
        queued_requests,
        oldest_request_age_ms: max_age.map(|d| d.as_millis() as u64).unwrap_or(0),
        average_request_age_ms: avg_age.map(|d| d.as_millis() as u64).unwrap_or(0),
        total_requests: cumulative_stats.total_requests.load(Ordering::Relaxed),
        total_errors: cumulative_stats.total_errors.load(Ordering::Relaxed),
        queue_rejections: cumulative_stats.queue_rejections.load(Ordering::Relaxed),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use alloy_consensus::{Header, TxEnvelope};
    use alloy_primitives::{Address, Bytes, U256};
    use monad_chain_config::execution_revision::MonadExecutionRevision;
    use monad_ethcall::{StateOverrideObject, StateOverrideSet};
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
            eth::call::{sender_gas_allowance, CallInput, MonadDebugTraceCallParams},
        },
        jsonrpc::JsonRpcError,
    };

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
