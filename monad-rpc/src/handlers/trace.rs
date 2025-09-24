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

use std::rc::Rc;

use alloy_primitives::Address;
use alloy_rlp::Decodable;
use alloy_rpc_types::trace::parity::{
    Action, CallAction, CallOutput, CallType, CreateAction, CreateOutput, CreationMethod,
    LocalizedTransactionTrace, SelfdestructAction, TraceOutput, TransactionTrace,
};
use monad_archive::prelude::{ArchiveReader, BlockDataReader, IndexReader};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{BlockKey, FinalizedBlockKey, Triedb};
use monad_types::SeqNum;
use serde::{Deserialize, Serialize};
use tracing::{error, trace};
use crate::{
    chainstate::get_block_key_from_tag,
    eth_json_types::{
        BlockTags, EthAddress, EthHash, FixedData, MonadU256, Quantity, UnformattedData,
    }, handlers::debug::{decode_call_frame, get_call_frames_from_triedb, CallKind, MonadCallFrame, MonadDebugTraceBlockResult, TracerObject}, hex, jsonrpc::{JsonRpcError, JsonRpcResult}
};
use alloy_primitives::{U256, Bytes};

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadTraceBlockParams {
    block_tag: BlockTags,
}

#[derive(Serialize, schemars::JsonSchema)]
pub struct MonadLocalizedTransactionTraces(
    #[schemars(schema_with = "schema_for_parity_trace")] pub Vec<LocalizedTransactionTrace>,
);

fn schema_for_parity_trace(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    let obj = vec![LocalizedTransactionTrace {
        trace: TransactionTrace {
            action: Action::Call(CallAction {
                call_type: CallType::Call,
                from: Address::default(),
                to: Address::default(),
                value: U256::ZERO,
                gas: 0,
                input: Bytes::default(),
            }),
            error: None,
            result: Some(TraceOutput::Call(CallOutput {
                gas_used: 0,
                output: Bytes::default(),
            })),
            subtraces: 0,
            trace_address: vec![],
        },
        block_hash: None,
        block_number: None,
        transaction_hash: None,
        transaction_position: None,
    }];

    schemars::schema_for_value!(obj).schema.into()
}

#[rpc(method = "trace_block")]
#[allow(non_snake_case)]
pub async fn monad_trace_block<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadTraceBlockParams,
) -> JsonRpcResult<MonadLocalizedTransactionTraces> {
    trace!("monad_trace_block: {params:?}");

    let block_key = get_block_key_from_tag(triedb_env, params.block_tag);

    if let (Ok(traces), Some(header)) = (
        get_call_frames_from_triedb(triedb_env, block_key, &TracerObject::default()).await,
        triedb_env
            .get_block_header(block_key)
            .await
            .map_err(JsonRpcError::internal_error)?,
    ) {
        return create_parity_traces(header.header, traces).map(MonadLocalizedTransactionTraces);
    } else if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
        (archive_reader, block_key)
    {
        if let Ok(block) = archive_reader.get_block_by_number(block_num.0).await {
            if let Ok(call_frames) = archive_reader.get_block_traces(block_num.0).await {
                let tx_ids = block
                    .body
                    .transactions
                    .iter()
                    .map(|tx| *tx.tx.tx_hash())
                    .collect::<Vec<_>>();

                let mut traces = Vec::new();
                for (call_frame, tx_id) in call_frames.iter().zip(tx_ids.into_iter()) {
                    let rlp_call_frame = &mut call_frame.as_slice();
                    let Some(trace) = decode_call_frame(
                        triedb_env,
                        rlp_call_frame,
                        block_key,
                        &TracerObject::default(),
                    )
                    .await?
                    else {
                        return Err(JsonRpcError::internal_error("traces not found".to_string()));
                    };
                    traces.push(MonadDebugTraceBlockResult {
                        tx_hash: FixedData::<32>::from(tx_id),
                        result: trace,
                    });
                }

                if let Ok(block) = archive_reader
                    .get_block_by_number(block_key.seq_num().0)
                    .await
                {
                    return create_parity_traces(block.header, traces)
                        .map(MonadLocalizedTransactionTraces);
                }
            }
        }
    };

    Ok(MonadLocalizedTransactionTraces(vec![]))
}

impl From<CallKind> for CallType {
    fn from(call_kind: CallKind) -> Self {
        match call_kind {
            CallKind::Call => CallType::Call,
            CallKind::DelegateCall => CallType::DelegateCall,
            CallKind::CallCode => CallType::CallCode,
            CallKind::StaticCall => CallType::StaticCall,
            CallKind::SelfDestruct => CallType::None,
            CallKind::Create => CallType::None,
            CallKind::Create2 => CallType::None,
        }
    }
}

impl TryFrom<CallKind> for CreationMethod {
    type Error = JsonRpcError;
    fn try_from(call_kind: CallKind) -> Result<Self, Self::Error> {
        Ok(match call_kind {
            CallKind::Create => CreationMethod::Create,
            CallKind::Create2 => CreationMethod::Create2,
            _ => {
                return Err(JsonRpcError::internal_error(
                    "invalid creation method".to_string(),
                ))
            }
        })
    }
}

fn create_parity_traces(
    header: alloy_consensus::Header,
    traces: Vec<MonadDebugTraceBlockResult>,
) -> JsonRpcResult<Vec<LocalizedTransactionTrace>> {
    let mut res = Vec::new();
    for (pos, top_level_frame) in traces.into_iter().enumerate() {
        let frames = flatten_call_tree_with_paths(top_level_frame.result);

        for (trace_address, frame) in frames {
            let to = frame.to.clone().map(|v| v.0.into()).unwrap_or_default();
            let subtraces_count = count_sub_call_frames(&frame);

            let action = match frame.typ {
                CallKind::Create | CallKind::Create2 => Action::Create(CreateAction {
                    creation_method: frame.typ.clone().try_into()?,
                    from: frame.from.0.into(),
                    value: frame.value.clone().map(|v| v.0).unwrap_or_default(),
                    gas: frame.gas.0,
                    init: Bytes::copy_from_slice(&frame.input.0),
                }),
                CallKind::Call
                | CallKind::CallCode
                | CallKind::StaticCall
                | CallKind::DelegateCall => Action::Call(CallAction {
                    call_type: frame.typ.clone().into(),
                    from: frame.from.0.into(),
                    to,
                    value: frame.value.clone().map(|v| v.0).unwrap_or_default(),
                    gas: frame.gas.0,
                    input: frame.input.0.clone().into(),
                }),
                CallKind::SelfDestruct => {
                    Action::Selfdestruct(SelfdestructAction {
                        address: frame.from.0.into(),
                        balance: frame.value.clone().map(|v| v.0).unwrap_or_default(),
                        refund_address: frame.to.clone().map(|v| v.0.into()).unwrap_or_default(),
                    })
                }
            };

            let result = if matches!(frame.typ, CallKind::Create | CallKind::Create2) {
                TraceOutput::Create(CreateOutput {
                    address: to,
                    code: frame.output.0.clone().into(),
                    gas_used: frame.gas_used.0,
                })
            } else {
                TraceOutput::Call(CallOutput {
                    gas_used: frame.gas_used.0,
                    output: frame.output.0.clone().into(),
                })
            };

            res.push(LocalizedTransactionTrace {
                transaction_hash: Some(alloy_primitives::FixedBytes(
                    top_level_frame.tx_hash.clone().0,
                )),
                transaction_position: Some(pos as u64),
                block_number: Some(header.number),
                block_hash: header.hash_slow().into(),
                trace: TransactionTrace {
                    action,
                    error: frame.revert_reason.clone(),
                    result: Some(result),
                    subtraces: subtraces_count,
                    trace_address,
                },
            })
        }
    }
    Ok(res)
}

fn count_sub_call_frames(root: &MonadCallFrame) -> usize {
    let mut count = 0;
    let mut stack = vec![];

    for child in &root.calls {
        stack.push(Rc::clone(child))
    }

    while let Some(current) = stack.pop() {
        count += 1;

        let current_borrow = current.borrow();
        for child in &current_borrow.calls {
            stack.push(Rc::clone(child));
        }
    }

    count
}

fn flatten_call_tree_with_paths(root: MonadCallFrame) -> Vec<(Vec<usize>, MonadCallFrame)> {
    let mut result = Vec::new();
    let mut stack = vec![(vec![], root)];

    while let Some((path, mut current)) = stack.pop() {
        result.push((path.clone(), current.clone()));
        let children = std::mem::take(&mut current.calls);

        for (i, child) in children.iter().rev().enumerate() {
            let index = children.len() - 1 - i;
            let mut new_path = path.clone();
            new_path.push(index);
            let owned_child = match Rc::try_unwrap(child.clone()) {
                Ok(inner) => inner.into_inner(),
                Err(rc) => rc.borrow().clone(),
            };
            stack.push((new_path, owned_child));
        }
    }

    result
}
