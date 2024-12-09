use std::{cell::RefCell, rc::Rc, str::FromStr};

use alloy_primitives::{
    aliases::{U256, U64, U8},
    Address, Bytes,
};
use alloy_rlp::Decodable;
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::Triedb;
use reth_rpc_types::trace::parity::{
    Action, CallAction, CallOutput, CallType, CreateAction, CreateOutput,
    LocalizedTransactionTrace, TraceOutput,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, trace};

use crate::{
    block_handlers::get_block_num_from_tag,
    eth_json_types::{
        BlockTags, EthAddress, EthHash, FixedData, MonadU256, Quantity, UnformattedData,
    },
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

#[derive(Clone, Debug)]
struct CallFrame {
    typ: CallKind,
    flags: U64,
    from: Address,
    to: Option<Address>,
    value: U256,
    gas: U64,
    gas_used: U64,
    input: Bytes,
    output: Bytes,
    status: U8,
    depth: U64,
}

impl Decodable for CallFrame {
    fn decode(rlp_buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let typ: U8 = U8::decode(rlp_buf)?;
        let flags: U64 = U64::decode(rlp_buf)?;
        let from: Address = Address::decode(rlp_buf)?;

        // Decode the `to` field, handling the case where it's `None`.
        let to: Option<Address> = {
            let first_byte = rlp_buf.first().ok_or(alloy_rlp::Error::InputTooShort)?;
            if *first_byte == 0x80 {
                // If the first byte is 0x80, it represents an empty value (None for the Address).
                *rlp_buf = &rlp_buf[1..]; // Advance the buffer
                None
            } else {
                // Otherwise, decode it as a normal Address.
                Some(Address::decode(rlp_buf)?)
            }
        };

        let value: U256 = U256::decode(rlp_buf)?;
        let gas: U64 = U64::decode(rlp_buf)?;
        let gas_used: U64 = U64::decode(rlp_buf)?;
        let input = Bytes::decode(rlp_buf)?;
        let output = Bytes::decode(rlp_buf)?;
        let status: U8 = U8::decode(rlp_buf)?;
        let depth: U64 = U64::decode(rlp_buf)?;

        let typ = match typ.to::<u8>() {
            0 if flags == U64::from(1) => CallKind::StaticCall,
            0 => CallKind::Call,
            1 => CallKind::DelegateCall,
            2 => CallKind::CallCode,
            3 => CallKind::Create,
            4 => CallKind::Create2,
            5 => CallKind::SelfDestruct,
            _ => return Err(alloy_rlp::Error::Custom("Invalid call kind")),
        };

        Ok(Self {
            typ,
            flags,
            from,
            to,
            value,
            gas,
            gas_used,
            input,
            output,
            status,
            depth,
        })
    }
}

#[derive(Deserialize, Debug, Default, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TracerObject {
    #[serde(default)]
    tracer: Tracer,
    only_top_call: Option<bool>,
}

#[derive(Deserialize, Debug, Default, schemars::JsonSchema)]
pub enum Tracer {
    #[default]
    #[serde(rename = "callTracer")]
    CallTracer,
    #[serde(rename = "prestateTracer")]
    PreStateTracer, // TODO: implement prestate tracer
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadDebugTraceTransactionParams {
    tx_hash: EthHash,
    #[serde(default)]
    tracer: TracerObject,
}

#[derive(Clone, Serialize, Debug, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MonadCallFrame {
    #[serde(rename = "type")]
    typ: CallKind,
    from: EthAddress,
    to: Option<EthAddress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<MonadU256>,
    gas: Quantity,
    gas_used: Quantity,
    input: UnformattedData,
    #[serde(skip_serializing_if = "UnformattedData::is_empty")]
    output: UnformattedData,
    #[serde(skip)]
    depth: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revert_reason: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    calls: Vec<std::rc::Rc<std::cell::RefCell<MonadCallFrame>>>,
}

impl From<CallFrame> for MonadCallFrame {
    fn from(value: CallFrame) -> Self {
        // the “value” argument is not included for STATICALL
        let frame_value = if matches!(value.typ, CallKind::StaticCall) {
            None
        } else {
            Some(MonadU256(value.value))
        };

        let error = if !value.status.is_zero() {
            alloy_sol_types::decode_revert_reason(&value.output.to_vec())
        } else {
            None
        };

        Self {
            typ: value.typ,
            from: value.from.into(),
            to: value.to.map(Into::into),
            value: frame_value,
            gas: Quantity(u64::from_le_bytes(value.gas.to_le_bytes())),
            gas_used: Quantity(u64::from_le_bytes(value.gas_used.to_le_bytes())),
            input: value.input.into(),
            output: value.output.into(),
            depth: value.depth.to::<usize>(),
            error: error.clone(),
            revert_reason: error,
            calls: Vec::new(),
        }
    }
}

#[derive(Serialize, Debug, Clone, schemars::JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
enum CallKind {
    Call,
    DelegateCall,
    CallCode,
    Create,
    Create2,
    SelfDestruct,
    StaticCall,
}

impl TryFrom<CallKind> for CallType {
    type Error = JsonRpcError;
    fn try_from(call_kind: CallKind) -> Result<Self, Self::Error> {
        Ok(match call_kind {
            CallKind::Call => CallType::Call,
            CallKind::DelegateCall => CallType::DelegateCall,
            CallKind::CallCode => CallType::CallCode,
            CallKind::StaticCall => CallType::StaticCall,
            _ => {
                return Err(JsonRpcError::internal_error(
                    "invalid call kind".to_string(),
                ))
            }
        })
    }
}

async fn call_traces_from_block_num<T: Triedb>(
    triedb_env: &T,
    block_num: u64,
    tracer: &TracerObject,
) -> Result<Vec<MonadDebugTraceBlockResult>, JsonRpcError> {
    let mut resp = Vec::new();

    let tx_ids = triedb_env
        .get_transactions(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
        .iter()
        .map(|tx| tx.hash())
        .collect::<Vec<_>>();
    let call_frames = triedb_env
        .get_call_frames(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?;

    for (call_frame, tx_id) in call_frames.iter().zip(tx_ids.into_iter()) {
        let rlp_call_frame = &mut call_frame.as_slice();
        let Some(traces) = decode_call_frame(triedb_env, rlp_call_frame, block_num, tracer).await?
        else {
            return Err(JsonRpcError::internal_error("traces not found".to_string()));
        };
        resp.push(MonadDebugTraceBlockResult {
            tx_hash: FixedData::<32>::from(tx_id),
            result: traces,
        });
    }

    Ok(resp)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadTraceBlockParams {
    block_tag: BlockTags,
}

#[allow(non_snake_case)]
pub async fn monad_trace_block<T: Triedb>(
    triedb_env: &T,
    params: MonadTraceBlockParams,
) -> JsonRpcResult<Vec<LocalizedTransactionTrace>> {
    trace!("monad_trace_block: {params:?}");

    let block_num = get_block_num_from_tag(triedb_env, params.block_tag).await?;

    let header = match triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => return Ok(Vec::new()),
    };

    let traces =
        call_traces_from_block_num(triedb_env, block_num, &TracerObject::default()).await?;

    let mut res = Vec::new();
    for (pos, top_level_frame) in traces.into_iter().enumerate() {
        let subtraces = count_sub_call_frames(&top_level_frame.result);
        let frame = top_level_frame.result.clone();
        let to = frame.to.map(|v| v.0.into()).unwrap_or_default();
        let action = match top_level_frame.result.typ {
            CallKind::Create | CallKind::Create2 => Action::Create(CreateAction {
                from: frame.from.0.into(),
                value: frame.value.map(|v| v.0).unwrap_or_default(),
                gas: U64::from(frame.gas.0),
                init: Bytes::copy_from_slice(&frame.input.0),
            }),
            _ => Action::Call(CallAction {
                call_type: frame.typ.try_into()?,
                from: frame.from.0.into(),
                to,
                value: frame.value.map(|v| v.0).unwrap_or_default(),
                gas: U64::from(frame.gas.0),
                input: frame.input.0.into(),
            }),
        };

        let result = match top_level_frame.result.typ {
            CallKind::Create | CallKind::Create2 => TraceOutput::Create(CreateOutput {
                address: to,
                code: frame.output.0.into(),
                gas_used: U64::from(frame.gas_used.0),
            }),
            _ => TraceOutput::Call(CallOutput {
                gas_used: U64::from(frame.gas_used.0),
                output: frame.output.0.into(),
            }),
        };

        res.push(LocalizedTransactionTrace {
            transaction_hash: Some(alloy_primitives::FixedBytes(
                top_level_frame.tx_hash.clone().0,
            )),
            transaction_position: Some(pos as u64),
            block_number: Some(header.header.number),
            block_hash: header.hash.into(),
            trace: reth_rpc_types::trace::parity::TransactionTrace {
                action,
                error: frame.revert_reason,
                result: Some(result),
                subtraces,
                trace_address: Vec::new(),
            },
        })
    }

    Ok(res)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadDebugTraceBlockByHashParams {
    block_hash: EthHash,
    #[serde(default)]
    tracer: TracerObject,
}

#[rpc(method = "debug_traceBlockByHash")]
#[allow(non_snake_case)]
/// Returns the tracing result by executing all transactions in the block specified by the block hash with a tracer.
pub async fn monad_debug_traceBlockByHash<T: Triedb>(
    triedb_env: &T,
    params: MonadDebugTraceBlockByHashParams,
) -> JsonRpcResult<Vec<MonadDebugTraceBlockResult>> {
    trace!("monad_debugTraceBlockByHash: {params:?}");

    let latest_block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
    let Some(block_num) = triedb_env
        .get_block_number_by_hash(params.block_hash.0, latest_block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        debug!("block not found");
        return Err(JsonRpcError::internal_error("block not found".to_string()));
    };

    call_traces_from_block_num(triedb_env, block_num, &params.tracer).await
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadDebugTraceBlockByNumberParams {
    block_number: Quantity,
    #[serde(default)]
    tracer: TracerObject,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MonadDebugTraceBlockResult {
    tx_hash: EthHash,
    result: MonadCallFrame,
}

#[rpc(method = "debug_traceBlockByNumber")]
#[allow(non_snake_case)]
/// Returns the tracing result by executing all transactions in the block specified by the block number with a tracer.
pub async fn monad_debug_traceBlockByNumber<T: Triedb>(
    triedb_env: &T,
    params: MonadDebugTraceBlockByNumberParams,
) -> JsonRpcResult<Vec<MonadDebugTraceBlockResult>> {
    trace!("monad_debugTraceBlockByNumber: {params:?}");

    let block_num = params.block_number.0;

    call_traces_from_block_num(triedb_env, block_num, &params.tracer).await
}

#[rpc(method = "debug_traceTransaction")]
#[allow(non_snake_case)]
/// Returns all traces of a given transaction.
pub async fn monad_debug_traceTransaction<T: Triedb>(
    triedb_env: &T,
    params: MonadDebugTraceTransactionParams,
) -> JsonRpcResult<Option<MonadCallFrame>> {
    trace!("monad_eth_debugTraceTransaction: {params:?}");

    let latest_block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
    let Some(tx_loc) = triedb_env
        .get_transaction_location_by_hash(params.tx_hash.0, latest_block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        debug!("transaction not found");
        return Err(JsonRpcError::internal_error(
            "transaction not found".to_string(),
        ));
    };

    let Some(rlp_call_frame) = triedb_env
        .get_call_frame(tx_loc.tx_index, tx_loc.block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Ok(None);
    };

    let rlp_call_frame = &mut rlp_call_frame.as_slice();

    decode_call_frame(triedb_env, rlp_call_frame, tx_loc.block_num, &params.tracer).await
}

async fn decode_call_frame<T: Triedb>(
    triedb_env: &T,
    rlp_call_frame: &mut &[u8],
    block_num: u64,
    tracer: &TracerObject,
) -> JsonRpcResult<Option<MonadCallFrame>> {
    let mut call_frames = Vec::<Vec<CallFrame>>::decode(rlp_call_frame)
        .map_err(|e| JsonRpcError::custom(format!("Rlp Decode error: {e}")))?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    match tracer.tracer {
        Tracer::CallTracer => {
            if let Some(true) = tracer.only_top_call {
                if call_frames.is_empty() {
                    Ok(None)
                } else {
                    let mut call_frame = call_frames.remove(0);
                    include_code_output(&mut call_frame, triedb_env, block_num).await?;
                    Ok(Some(MonadCallFrame::from(call_frame)))
                }
            } else {
                let call_frames = futures::future::join_all(
                    call_frames
                        .into_iter()
                        .map(|mut frame| async move {
                            include_code_output(&mut frame, triedb_env, block_num).await?;
                            Ok::<_, JsonRpcError>(frame)
                        })
                        .collect::<Vec<_>>(),
                )
                .await
                .into_iter()
                .collect::<Result<Vec<_>, JsonRpcError>>()?;

                Ok(build_call_tree(call_frames)
                    .await?
                    .and_then(|rc| Rc::try_unwrap(rc).ok().map(|refcell| refcell.into_inner())))
            }
        }
        _ => Err(JsonRpcError::method_not_supported()),
    }
}

async fn include_code_output<T: Triedb>(
    frame: &mut CallFrame,
    triedb_env: &T,
    block_num: u64,
) -> JsonRpcResult<()> {
    if matches!(frame.typ, CallKind::Create) || matches!(frame.typ, CallKind::Create2) {
        let Some(contract_addr) = &frame.to else {
            error!("expected contract address in call frame");
            return Err(JsonRpcError::internal_error(
                "contract address not found in call frame".to_string(),
            ));
        };

        let account = triedb_env
            .get_account(contract_addr.0.into(), block_num)
            .await
            .map_err(JsonRpcError::internal_error)?;
        let code = triedb_env
            .get_code(account.code_hash, block_num)
            .await
            .map_err(JsonRpcError::internal_error)?;

        frame.output = Bytes::from_str(&code)
            .map_err(|_| JsonRpcError::internal_error("could not decode code".to_string()))?;
    }

    Ok(())
}

async fn build_call_tree(
    nodes: Vec<CallFrame>,
) -> JsonRpcResult<Option<std::rc::Rc<std::cell::RefCell<MonadCallFrame>>>> {
    if nodes.is_empty() {
        return Ok(None);
    }

    let root = Rc::new(RefCell::new(MonadCallFrame::from(nodes[0].clone())));
    let mut stack = vec![Rc::clone(&root)];

    for value in nodes.into_iter().skip(1) {
        let depth = value.depth.to::<usize>();
        let new_node = Rc::new(RefCell::new(MonadCallFrame::from(value)));
        while let Some(last) = stack.last() {
            if last.borrow().depth < depth {
                last.borrow_mut().calls.push(Rc::clone(&new_node));
                break;
            }
            stack.pop();
        }

        stack.push(new_node);
    }

    Ok(Some(root))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hex;

    #[tokio::test]
    async fn test_build_call_tree() {
        // depth of each call is the following [1, 2, 3, 3]
        let frames = hex::decode("0xf90aa0f9019f808094f39fd6e51aad88f6f4ce6ab8827279cfffb92266949fe46736679d2d9a65f0992f2272de9f3c7fa6e080831e84808307a930b90144f4a6659c000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb922660000000000000000000000005fbdb2315678afecb367f032d93f642f64180aa3000000000000000000000000e7f1725e7734ce288f8367e1bb143e90bb3f0512000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005f5e100000000000000000000000000000000000000000000000000000000000000271000000000000000000000000000000000000000000000000000005af3107a40000000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000e451980132e65465d0a498c53f0b5227326dd73f8080f906bf0380949fe46736679d2d9a65f0992f2272de9f3c7fa6e094e451980132e65465d0a498c53f0b5227326dd73f80831d2263830608c3b9068460806040526040516104c43803806104c4833981016040819052610022916102d2565b61002d82825f610034565b50506103e7565b61003d8361005f565b5f825111806100495750805b1561005a57610058838361009e565b505b505050565b610068816100ca565b6040516001600160a01b038216907fbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b905f90a250565b60606100c3838360405180606001604052806027815260200161049d6027913961017d565b9392505050565b6001600160a01b0381163b61013c5760405162461bcd60e51b815260206004820152602d60248201527f455243313936373a206e657720696d706c656d656e746174696f6e206973206e60448201526c1bdd08184818dbdb9d1c9858dd609a1b60648201526084015b60405180910390fd5b7f360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc80546001600160a01b0319166001600160a01b0392909216919091179055565b60605f80856001600160a01b031685604051610199919061039a565b5f60405180830381855af49150503d805f81146101d1576040519150601f19603f3d011682016040523d82523d5f602084013e6101d6565b606091505b5090925090506101e8868383876101f2565b9695505050505050565b606083156102605782515f03610259576001600160a01b0385163b6102595760405162461bcd60e51b815260206004820152601d60248201527f416464726573733a2063616c6c20746f206e6f6e2d636f6e74726163740000006044820152606401610133565b508161026a565b61026a8383610272565b949350505050565b8151156102825781518083602001fd5b8060405162461bcd60e51b815260040161013391906103b5565b634e487b7160e01b5f52604160045260245ffd5b5f5b838110156102ca5781810151838201526020016102b2565b50505f910152565b5f80604083850312156102e3575f80fd5b82516001600160a01b03811681146102f9575f80fd5b60208401519092506001600160401b0380821115610315575f80fd5b818501915085601f830112610328575f80fd5b81518181111561033a5761033a61029c565b604051601f8201601f19908116603f011681019083821181831017156103625761036261029c565b8160405282815288602084870101111561037a575f80fd5b61038b8360208301602088016102b0565b80955050505050509250929050565b5f82516103ab8184602087016102b0565b9190910192915050565b602081525f82518060208401526103d38160408501602087016102b0565b601f01601f19169190910160400192915050565b60aa806103f35f395ff3fe608060405236601057600e6013565b005b600e5b601f601b6021565b6057565b565b5f60527f360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc546001600160a01b031690565b905090565b365f80375f80365f845af43d5f803e8080156070573d5ff35b3d5ffdfea2646970667358221220dc385d1a646905a2bf7c2558648b32507745ba71a9f460aa1dc57cc1bf40e8ce64736f6c63430008140033416464726573733a206c6f772d6c6576656c2064656c65676174652063616c6c206661696c656400000000000000000000000075537828f2ce51be7289709686a69cbfdbb714f10000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000014415fcc826000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb922660000000000000000000000005fbdb2315678afecb367f032d93f642f64180aa3000000000000000000000000e7f1725e7734ce288f8367e1bb143e90bb3f0512000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005f5e100000000000000000000000000000000000000000000000000000000000000271000000000000000000000000000000000000000000000000000005af3107a4000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000808001f9017f018094e451980132e65465d0a498c53f0b5227326dd73f9475537828f2ce51be7289709686a69cbfdbb714f180831c3f6e83051220b9014415fcc826000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb922660000000000000000000000005fbdb2315678afecb367f032d93f642f64180aa3000000000000000000000000e7f1725e7734ce288f8367e1bb143e90bb3f0512000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005f5e100000000000000000000000000000000000000000000000000000000000000271000000000000000000000000000000000000000000000000000005af3107a40000000000000000000000000000000000000000000000000000000000000000000808002f85b800194e451980132e65465d0a498c53f0b5227326dd73f94e7f1725e7734ce288f8367e1bb143e90bb3f0512808318fc7881f884313ce567a000000000000000000000000000000000000000000000000000000000000000128003f85b800194e451980132e65465d0a498c53f0b5227326dd73f945fbdb2315678afecb367f032d93f642f64180aa3808318998f81f884313ce567a000000000000000000000000000000000000000000000000000000000000000068003").expect("decode call frame");
        let frames = Vec::<Vec<CallFrame>>::decode(&mut frames.as_slice())
            .expect("decode call frame")
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let result = build_call_tree(frames)
            .await
            .unwrap()
            .and_then(|rc| Rc::try_unwrap(rc).ok().map(|refcell| refcell.into_inner()));

        assert!(result.is_some());
        let result: MonadCallFrame = result.unwrap();
        assert_eq!(result.calls.len(), 1);
        assert_eq!(count_sub_call_frames(&result), 4);

        result
            .calls
            .iter()
            .enumerate()
            .for_each(|(idx, frame)| match idx {
                0 => assert_eq!(frame.borrow().calls.len(), 1),

                1 => assert_eq!(frame.borrow().calls.len(), 1),

                2 => assert_eq!(frame.borrow().calls.len(), 2),

                _ => panic!("unexpected index"),
            });
    }
}
