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

use std::sync::Arc;

use monad_ethcall::{eth_call, CallResult, EthCallExecutor, MonadTracer, StateOverrideSet};
use monad_triedb_utils::triedb_env::Triedb;
use monad_types::SeqNum;
use serde_json::value::RawValue;
use tokio::sync::Mutex;

use crate::{
    chainstate::get_block_key_from_tag,
    eth_json_types::{BlockTags, EthHash},
    handlers::debug::{
        MonadDebugTraceBlockByHashParams, MonadDebugTraceBlockByNumberParams,
        MonadDebugTraceTransactionParams, Tracer, TracerObject,
    },
    jsonrpc::JsonRpcError,
};

impl From<TracerObject> for MonadTracer {
    fn from(tracer_obj: TracerObject) -> Self {
        match tracer_obj.tracer {
            Tracer::PreStateTracer if tracer_obj.config.diff_mode => MonadTracer::StateDiffTracer,
            Tracer::PreStateTracer => MonadTracer::PreStateTracer,
            Tracer::CallTracer => MonadTracer::CallTracer,
        }
    }
}

/// A trait for debug trace parameters as well as determining if a request requires transaction replay.
pub trait DebugTraceParams {
    /// Returns true if the tracer requires transaction replay (e.g., PreStateTracer).
    fn requires_replay(&self) -> bool;
    /// Returns the hash or tag parameter payload associated with the trace request.
    fn hash_or_tag(&self) -> HashOrTag;
    /// Returns whether the trace request is a single operation or bulk operation (e.g. traceTransaction vs traceBlockByNumber).
    fn execution_mode(&self) -> ExecutionMode;
    /// Returns the tracer configuration associated with the trace request.
    fn tracer(&self) -> TracerObject;
}

impl DebugTraceParams for MonadDebugTraceTransactionParams {
    fn requires_replay(&self) -> bool {
        matches!(self.tracer.tracer, Tracer::PreStateTracer)
    }
    fn hash_or_tag(&self) -> HashOrTag {
        HashOrTag::Hash(self.tx_hash)
    }
    fn execution_mode(&self) -> ExecutionMode {
        ExecutionMode::Single
    }
    fn tracer(&self) -> TracerObject {
        self.tracer
    }
}

impl DebugTraceParams for MonadDebugTraceBlockByNumberParams {
    fn requires_replay(&self) -> bool {
        matches!(self.tracer.tracer, Tracer::PreStateTracer)
    }
    fn hash_or_tag(&self) -> HashOrTag {
        HashOrTag::Tag(self.block_number)
    }
    fn execution_mode(&self) -> ExecutionMode {
        ExecutionMode::Bulk
    }
    fn tracer(&self) -> TracerObject {
        self.tracer
    }
}

impl DebugTraceParams for MonadDebugTraceBlockByHashParams {
    fn requires_replay(&self) -> bool {
        matches!(self.tracer.tracer, Tracer::PreStateTracer)
    }
    fn hash_or_tag(&self) -> HashOrTag {
        HashOrTag::Hash(self.block_hash)
    }
    fn execution_mode(&self) -> ExecutionMode {
        ExecutionMode::Bulk
    }
    fn tracer(&self) -> TracerObject {
        self.tracer
    }
}

/// Indicates whether the trace request is for a single transaction or for all transactions in a block.
pub enum ExecutionMode {
    Single,
    Bulk,
}

/// Represents either a block or transaction hash, or a block tag.
pub enum HashOrTag {
    Hash(EthHash),
    Tag(BlockTags),
}

/// Converts a HashOrTag into BlockTags, treating any hash as 'latest'. Useful for block key retrieval.
impl From<HashOrTag> for BlockTags {
    fn from(value: HashOrTag) -> Self {
        match value {
            HashOrTag::Hash(_) => BlockTags::Latest,
            HashOrTag::Tag(tag) => tag,
        }
    }
}

impl<T: DebugTraceParams> From<&T> for BlockTags {
    fn from(params: &T) -> Self {
        params.hash_or_tag().into()
    }
}

impl TryFrom<HashOrTag> for EthHash {
    type Error = JsonRpcError;
    fn try_from(value: HashOrTag) -> Result<Self, Self::Error> {
        match value {
            HashOrTag::Hash(hash) => Ok(hash),
            HashOrTag::Tag(_) => Err(JsonRpcError::internal_error(
                "expected block hash, found tag".into(),
            )),
        }
    }
}

/// A generic handler for debug trace requests that requires transaction replay (e.g., PreStateTracer).
pub async fn monad_debug_trace_replay<T: Triedb>(
    triedb_env: &T,
    eth_call_executor: Arc<Mutex<EthCallExecutor>>,
    chain_id: u64,
    params: &impl DebugTraceParams,
) -> Result<Box<RawValue>, JsonRpcError> {
    let state_overrides = StateOverrideSet::default();
    let block_key = get_block_key_from_tag(triedb_env, params.into());
    let header = triedb_env
        .get_block_header(block_key)
        .await
        .map_err(|e| JsonRpcError::internal_error(format!("error getting block header: {}", e)))?
        .ok_or_else(|| {
            JsonRpcError::internal_error("error getting block header: found none".into())
        })?;
    let tracer: MonadTracer = params.tracer().into();
    match params.execution_mode() {
        ExecutionMode::Single => {
            let tx_hash: EthHash = params.hash_or_tag().try_into()?;
            let tx_loc = triedb_env
                .get_transaction_location_by_hash(block_key, tx_hash.0)
                .await
                .map_err(JsonRpcError::internal_error)?
                .ok_or_else(|| {
                    JsonRpcError::internal_error(format!("transaction not found: {:?}", tx_hash))
                })?;
            let block_key = triedb_env.get_block_key(SeqNum(tx_loc.block_num));
            let txn = triedb_env
                .get_transaction(block_key, tx_loc.tx_index)
                .await
                .map_err(|e| {
                    JsonRpcError::internal_error(format!("error getting transaction: {}", e))
                })?
                .ok_or_else(|| JsonRpcError::internal_error("no transaction data".into()))?;
            let (seq_number, block_id) = block_key.seq_num_block_id();
            let raw_payload = match eth_call(
                chain_id,
                txn.tx,
                header.header,
                txn.sender,
                seq_number.0,
                block_id.map(|id| id.0 .0),
                eth_call_executor,
                &state_overrides,
                tracer,
                false,
            )
            .await
            {
                CallResult::Success(monad_ethcall::SuccessCallResult { output_data, .. }) => {
                    output_data
                }
                CallResult::Failure(error) => {
                    return Err(JsonRpcError::eth_call_error(error.message, error.data))
                }
                CallResult::Revert(result) => result.trace,
            };
            let v: serde_cbor::Value = serde_cbor::from_slice(&raw_payload)
                .map_err(|e| JsonRpcError::internal_error(format!("cbor decode error: {}", e)))?;
            serde_json::value::to_raw_value(&v).map_err(|e| {
                JsonRpcError::internal_error(format!("json serialization error: {}", e))
            })
        }
        ExecutionMode::Bulk => {
            let block_key = match params.hash_or_tag() {
                HashOrTag::Hash(block_hash) => {
                    if let Some(block_num) = triedb_env
                        .get_block_number_by_hash(block_key, block_hash.0)
                        .await
                        .map_err(JsonRpcError::internal_error)?
                    {
                        triedb_env.get_block_key(SeqNum(block_num))
                    } else {
                        return Err(JsonRpcError::internal_error(format!(
                            "block not found: {:?}",
                            block_hash
                        )));
                    }
                }
                HashOrTag::Tag(_) => block_key,
            };
            let txns = triedb_env.get_transactions(block_key).await.map_err(|e| {
                JsonRpcError::internal_error(format!("error getting transactions: {}", e))
            })?;
            let (seq_number, block_id) = block_key.seq_num_block_id();
            let mut results: Vec<serde_cbor::Value> = Vec::with_capacity(txns.len());
            for txn in txns {
                let tx_hash = txn.tx.signature_hash();
                // TODO(dhil): May skew the eth_call statistics tracker.
                let raw_payload = match eth_call(
                    chain_id,
                    txn.tx,
                    header.header.clone(),
                    txn.sender,
                    seq_number.0,
                    block_id.map(|id| id.0 .0),
                    eth_call_executor.clone(),
                    &state_overrides,
                    tracer,
                    false,
                )
                .await
                {
                    CallResult::Success(monad_ethcall::SuccessCallResult {
                        output_data, ..
                    }) => output_data,
                    CallResult::Failure(error) => {
                        return Err(JsonRpcError::eth_call_error(error.message, error.data))
                    }
                    CallResult::Revert(result) => result.trace,
                };

                let trace: serde_cbor::Value =
                    serde_cbor::from_slice(&raw_payload).map_err(|e| {
                        JsonRpcError::internal_error(format!("cbor decode error: {}", e))
                    })?;

                // Create BTreeMap for CBOR Map
                let mut result_obj = std::collections::BTreeMap::new();
                result_obj.insert(
                    serde_cbor::Value::Text("txHash".to_string()),
                    serde_cbor::Value::Text(format!("{}", tx_hash)),
                );
                result_obj.insert(serde_cbor::Value::Text("result".to_string()), trace);
                let result = serde_cbor::Value::Map(result_obj);

                results.push(result);
            }
            results
                .iter()
                .map(serde_json::value::to_raw_value)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    JsonRpcError::internal_error(format!("json serialization error: {}", e))
                })
                .and_then(|v| {
                    serde_json::value::to_raw_value(&v).map_err(|e| {
                        JsonRpcError::internal_error(format!("json serialization error: {}", e))
                    })
                })
        }
    }
}
