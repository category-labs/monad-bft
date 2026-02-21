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

use std::{num::TryFromIntError, sync::Arc};

use monad_ethcall::{eth_trace_block_or_transaction, CallResult, EthCallExecutor, MonadTracer};
use monad_triedb_utils::triedb_env::{BlockKey, Triedb};
use monad_types::SeqNum;
use serde_json::value::RawValue;

use crate::{
    chainstate::get_block_key_from_tag,
    handlers::{
        debug::{
            MonadDebugTraceBlockByHashParams, MonadDebugTraceBlockByNumberParams,
            MonadDebugTraceTransactionParams, Tracer, TracerObject,
        },
        MonadRpcResources,
    },
    middleware::TimingRequestId,
    types::{
        eth_json::{BlockTagOrHash, BlockTags, EthHash},
        jsonrpc::{JsonRpcError, JsonRpcResult},
    },
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
    fn block_tag_or_hash(&self) -> BlockTagOrHash;
    /// Returns whether the trace request is for a single transaction or an entire block (e.g. traceTransaction vs traceBlockByNumber).
    fn trace_target(&self) -> TraceTarget;
    /// Returns the tracer configuration associated with the trace request.
    fn tracer(&self) -> TracerObject;
}

impl DebugTraceParams for MonadDebugTraceTransactionParams {
    fn requires_replay(&self) -> bool {
        matches!(self.tracer.tracer, Tracer::PreStateTracer)
    }
    fn block_tag_or_hash(&self) -> BlockTagOrHash {
        BlockTagOrHash::Hash(self.tx_hash)
    }
    fn trace_target(&self) -> TraceTarget {
        TraceTarget::Transaction
    }
    fn tracer(&self) -> TracerObject {
        self.tracer
    }
}

impl DebugTraceParams for MonadDebugTraceBlockByNumberParams {
    fn requires_replay(&self) -> bool {
        matches!(self.tracer.tracer, Tracer::PreStateTracer)
    }
    fn block_tag_or_hash(&self) -> BlockTagOrHash {
        BlockTagOrHash::BlockTags(self.block_number)
    }
    fn trace_target(&self) -> TraceTarget {
        TraceTarget::Block
    }
    fn tracer(&self) -> TracerObject {
        self.tracer
    }
}

impl DebugTraceParams for MonadDebugTraceBlockByHashParams {
    fn requires_replay(&self) -> bool {
        matches!(self.tracer.tracer, Tracer::PreStateTracer)
    }
    fn block_tag_or_hash(&self) -> BlockTagOrHash {
        BlockTagOrHash::Hash(self.block_hash)
    }
    fn trace_target(&self) -> TraceTarget {
        TraceTarget::Block
    }
    fn tracer(&self) -> TracerObject {
        self.tracer
    }
}

/// Indicates whether the trace request is for a single transaction or for all transactions in a block.
pub enum TraceTarget {
    Block,
    Transaction,
}

/// Projects the block tag, treating any hash as 'latest'. Useful for block key retrieval.
impl<T: DebugTraceParams> From<&T> for BlockTags {
    fn from(params: &T) -> Self {
        match params.block_tag_or_hash() {
            BlockTagOrHash::Hash(_) => BlockTags::Latest,
            BlockTagOrHash::BlockTags(tag) => tag,
        }
    }
}

impl TryFrom<BlockTagOrHash> for EthHash {
    type Error = JsonRpcError;
    fn try_from(value: BlockTagOrHash) -> Result<Self, Self::Error> {
        match value {
            BlockTagOrHash::Hash(hash) => Ok(hash),
            BlockTagOrHash::BlockTags(_) => Err(JsonRpcError::internal_error(
                "expected block hash, found tag".into(),
            )),
        }
    }
}

/// A generic handler for debug trace requests that requires transaction replay (e.g., PreStateTracer).
pub async fn monad_debug_trace_replay<T: Triedb>(
    triedb_env: &T,
    eth_call_executor: Arc<EthCallExecutor>,
    chain_id: u64,
    params: &impl DebugTraceParams,
) -> JsonRpcResult<Box<RawValue>> {
    let block_key = get_block_key_from_tag(triedb_env, params.into()).ok_or_else(|| {
        JsonRpcError::internal_error("error getting block key from tag: found none".into())
    })?;
    let tracer: MonadTracer = params.tracer().into();
    let (block_key, block_number, transaction_index) = match params.trace_target() {
        TraceTarget::Transaction => {
            let tx_hash: EthHash = params.block_tag_or_hash().try_into()?;
            let tx_loc = triedb_env
                .get_transaction_location_by_hash(block_key, tx_hash.0)
                .await
                .map_err(JsonRpcError::internal_error)?
                .ok_or_else(|| {
                    JsonRpcError::internal_error(format!("transaction not found: {:?}", tx_hash))
                })?;
            let block_key = triedb_env
                .get_block_key(SeqNum(tx_loc.block_num))
                .ok_or_else(|| {
                    JsonRpcError::internal_error(
                        "error getting block key from block number: found none".into(),
                    )
                })?;
            (
                block_key,
                tx_loc.block_num,
                tx_loc
                    .tx_index
                    .try_into()
                    .map_err(|e: TryFromIntError| JsonRpcError::internal_error(e.to_string()))?,
            )
        }
        TraceTarget::Block => {
            let block_key = match params.block_tag_or_hash() {
                BlockTagOrHash::Hash(block_hash) => {
                    if let Some(block_num) = triedb_env
                        .get_block_number_by_hash(block_key, block_hash.0)
                        .await
                        .map_err(JsonRpcError::internal_error)?
                    {
                        triedb_env.get_block_key(SeqNum(block_num)).ok_or_else(|| {
                            JsonRpcError::internal_error(
                                "error getting block key from block number: found none".into(),
                            )
                        })?
                    } else {
                        return Err(JsonRpcError::internal_error(format!(
                            "block not found: {:?}",
                            block_hash
                        )));
                    }
                }
                BlockTagOrHash::BlockTags(_) => block_key,
            };
            (block_key, block_key.seq_num().0, -1)
        }
    };
    if block_number == 0 {
        return Err(JsonRpcError::internal_error(
            "cannot trace the genesis block".into(),
        ));
    }
    let header = triedb_env
        .get_block_header(block_key)
        .await
        .map_err(|e| JsonRpcError::internal_error(format!("error getting block header: {}", e)))?
        .ok_or_else(|| {
            JsonRpcError::internal_error("error getting block header: found none".into())
        })?;
    let parent_key = triedb_env
        .get_block_key(SeqNum(block_number - 1))
        .ok_or_else(|| {
            JsonRpcError::internal_error(
                "error getting parent block key from block number: found none".into(),
            )
        })?;
    let grandparent_key = if block_number > 1 {
        Some(
            triedb_env
                .get_block_key(SeqNum(block_number - 2))
                .ok_or_else(|| {
                    JsonRpcError::internal_error(
                        "error getting grandparent block key from block number: found none".into(),
                    )
                })?,
        )
    } else {
        None
    };
    let call_result = eth_trace_block_or_transaction(
        chain_id,
        header.header,
        block_number,
        block_key.into(),
        parent_key.into(),
        grandparent_key.and_then(|key: BlockKey| key.into()),
        transaction_index,
        eth_call_executor,
        tracer,
    )
    .await;
    let raw_payload = match call_result {
        CallResult::Success(monad_ethcall::SuccessCallResult { output_data, .. }) => output_data,
        CallResult::Failure(error) => {
            return Err(JsonRpcError::eth_call_error(error.message, error.data))
        }
        CallResult::Revert(result) => result.trace,
    };
    let v: serde_cbor::Value = serde_cbor::from_slice(&raw_payload)
        .map_err(|e| JsonRpcError::internal_error(format!("cbor decode error: {}", e)))?;
    serde_json::value::to_raw_value(&v)
        .map_err(|e| JsonRpcError::internal_error(format!("json serialization error: {}", e)))
}

pub async fn collect_debug_trace_via_replay(
    request_id: TimingRequestId,
    triedb_env: &impl Triedb,
    app_state: &MonadRpcResources,
    params: &impl DebugTraceParams,
) -> Result<Box<RawValue>, JsonRpcError> {
    let Some(ref eth_call_executor) = app_state.eth_call_executor else {
        return Err(JsonRpcError::method_not_supported());
    };
    // acquire the concurrent requests permit
    let _permit = match app_state.rate_limiter.try_acquire() {
        Ok(permit) => permit,
        Err(_) => {
            if let Some(tracker) = &app_state.eth_call_stats_tracker {
                tracker.record_queue_rejection().await;
            }
            return Err(JsonRpcError::internal_error(
                "eth_call concurrent requests limit".into(),
            ));
        }
    };

    if let Some(tracker) = &app_state.eth_call_stats_tracker {
        tracker.record_request_start(request_id).await;
    }

    let result = monad_debug_trace_replay(
        triedb_env,
        eth_call_executor.clone(),
        app_state.chain_id,
        params,
    )
    .await;

    if let Some(tracker) = &app_state.eth_call_stats_tracker {
        let is_error = result.is_err();
        tracker.record_request_complete(&request_id, is_error).await;
    }

    result
}
