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
    eth_json_types::{BlockTags, FixedData},
    handlers::debug::{
        MonadDebugTraceBlockByNumberParams, MonadDebugTraceTransactionParams, Tracer, TracerObject,
    },
    jsonrpc::JsonRpcError,
};

fn to_monad_tracer(tracer_obj: &TracerObject) -> MonadTracer {
    match tracer_obj.tracer {
        Tracer::PreStateTracer if tracer_obj.config.diff_mode => MonadTracer::StateDiffTracer,
        Tracer::PreStateTracer => MonadTracer::PreStateTracer,
        _ => panic!("only PreStateTracer is supported in debug replay"),
    }
}

pub trait DebugTraceExecutor {
    async fn eth_call(
        &self,
        eth_call_executor: Arc<Mutex<EthCallExecutor>>,
        chain_id: u64,
    ) -> Result<Box<RawValue>, JsonRpcError>;
}

pub struct MonadDebugTraceTransactionExecutor<'a, T: Triedb> {
    triedb_env: &'a T,
    tx_hash: FixedData<32>,
    tracer: TracerObject,
}

impl<'a, T: Triedb> MonadDebugTraceTransactionExecutor<'a, T> {
    pub fn new(triedb_env: &'a T, params: MonadDebugTraceTransactionParams) -> Self {
        assert!(matches!(params.tracer.tracer, Tracer::PreStateTracer));
        Self {
            triedb_env,
            tx_hash: params.tx_hash,
            tracer: params.tracer,
        }
    }
}

impl<'a, T: Triedb> DebugTraceExecutor for MonadDebugTraceTransactionExecutor<'a, T> {
    async fn eth_call(
        &self,
        eth_call_executor: Arc<Mutex<EthCallExecutor>>,
        chain_id: u64,
    ) -> Result<Box<RawValue>, JsonRpcError> {
        let latest_block_key = get_block_key_from_tag(self.triedb_env, BlockTags::Latest);
        let tx_loc = self
            .triedb_env
            .get_transaction_location_by_hash(latest_block_key, self.tx_hash.0)
            .await
            .map_err(JsonRpcError::internal_error)?
            .ok_or_else(|| {
                JsonRpcError::internal_error(format!("transaction not found: {:?}", self.tx_hash))
            })?;
        let block_key = self.triedb_env.get_block_key(SeqNum(tx_loc.block_num));
        let txn = self
            .triedb_env
            .get_transaction(block_key, tx_loc.tx_index)
            .await
            .map_err(|e| JsonRpcError::internal_error(format!("error getting transaction: {}", e)))?
            .ok_or_else(|| JsonRpcError::internal_error("no transaction data".into()))?;
        let header = self
            .triedb_env
            .get_block_header(latest_block_key)
            .await
            .map_err(|e| {
                JsonRpcError::internal_error(format!("error getting block header: {}", e))
            })?
            .ok_or_else(|| JsonRpcError::internal_error("error getting block".into()))?;
        let state_overrides = StateOverrideSet::default();
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
            to_monad_tracer(&self.tracer),
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
        return serde_json::value::to_raw_value(&v)
            .map_err(|e| JsonRpcError::internal_error(format!("json serialization error: {}", e)));
    }
}

pub struct MonadDebugTraceBlockByNumberExecutor<'a, T: Triedb> {
    triedb_env: &'a T,
    block_tag: BlockTags,
    tracer: TracerObject,
}

impl<'a, T: Triedb> MonadDebugTraceBlockByNumberExecutor<'a, T> {
    pub fn new(triedb_env: &'a T, params: MonadDebugTraceBlockByNumberParams) -> Self {
        assert!(matches!(params.tracer.tracer, Tracer::PreStateTracer));
        Self {
            triedb_env,
            block_tag: params.block_number,
            tracer: params.tracer,
        }
    }
}

impl<'a, T: Triedb> DebugTraceExecutor for MonadDebugTraceBlockByNumberExecutor<'a, T> {
    async fn eth_call(
        &self,
        eth_call_executor: Arc<Mutex<EthCallExecutor>>,
        chain_id: u64,
    ) -> Result<Box<RawValue>, JsonRpcError> {
        let block_key = get_block_key_from_tag(self.triedb_env, self.block_tag);
        let header = self
            .triedb_env
            .get_block_header(block_key)
            .await
            .map_err(|e| {
                JsonRpcError::internal_error(format!("error getting block header: {}", e))
            })?
            .ok_or_else(|| JsonRpcError::internal_error("error getting block".into()))?;
        let txns = self
            .triedb_env
            .get_transactions(block_key)
            .await
            .map_err(|e| {
                JsonRpcError::internal_error(format!("error getting transactions: {}", e))
            })?;
        let state_overrides = StateOverrideSet::default();
        let (seq_number, block_id) = block_key.seq_num_block_id();
        let mut results: Vec<serde_cbor::Value> = Vec::with_capacity(txns.len());
        for txn in txns {
            let tx_hash = txn.tx.signature_hash();
            let raw_payload = match eth_call(
                chain_id,
                txn.tx,
                header.header.clone(),
                txn.sender,
                seq_number.0,
                block_id.map(|id| id.0 .0),
                eth_call_executor.clone(),
                &state_overrides,
                to_monad_tracer(&self.tracer),
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

            let trace: serde_cbor::Value = serde_cbor::from_slice(&raw_payload)
                .map_err(|e| JsonRpcError::internal_error(format!("cbor decode error: {}", e)))?;

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
            .map_err(|e| JsonRpcError::internal_error(format!("json serialization error: {}", e)))
            .and_then(|v| {
                serde_json::value::to_raw_value(&v).map_err(|e| {
                    JsonRpcError::internal_error(format!("json serialization error: {}", e))
                })
            })
    }
}
