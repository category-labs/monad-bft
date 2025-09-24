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

use std::{cell::RefCell, rc::Rc};

use alloy_consensus::{Block, BlockBody, Header, ReceiptEnvelope, TxEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{
    aliases::{U256, U64, U8},
    Address, Bytes,
};
use alloy_rlp::{Decodable, Encodable};
use monad_archive::prelude::{ArchiveReader, BlockDataReader, IndexReader};
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{BlockKey, FinalizedBlockKey, Triedb};
use monad_types::SeqNum;
use serde::{Deserialize, Serialize};
use tracing::{error, trace};

use crate::{
    chainstate::{get_block_key_from_tag, ChainState},
    eth_json_types::{
        BlockTagOrHash, BlockTags, EthAddress, EthHash, FixedData, MonadU256, Quantity,
        UnformattedData,
    },
    hex,
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct DebugBlockParams {
    block: BlockTags,
}

#[rpc(method = "debug_getRawBlock")]
#[allow(non_snake_case)]
/// Returns an RLP-encoded block.
pub async fn monad_debug_getRawBlock<T: Triedb>(
    chain_state: &ChainState<T>,
    params: DebugBlockParams,
) -> JsonRpcResult<String> {
    trace!("monad_debug_getRawBlock: {params:?}");

    let encode_block = |block: Block<TxEnvelope>| {
        let mut res = Vec::new();
        block.encode(&mut res);
        Ok(hex::encode(&res))
    };

    match chain_state
        .get_block(BlockTagOrHash::BlockTags(params.block), true)
        .await
    {
        Ok(block) => encode_block(Block {
            header: block.header.inner,
            body: BlockBody {
                transactions: block
                    .transactions
                    .into_transactions()
                    .map(|tx| tx.into())
                    .collect(),
                ommers: vec![],
                withdrawals: None,
            },
        }),
        Err(_) => Err(JsonRpcError::internal_error("block data not found".into())),
    }
}

#[rpc(method = "debug_getRawHeader")]
#[allow(non_snake_case)]
/// Returns an RLP-encoded header.
pub async fn monad_debug_getRawHeader<T: Triedb>(
    chain_state: &ChainState<T>,
    params: DebugBlockParams,
) -> JsonRpcResult<String> {
    trace!("monad_debug_getRawHeader: {params:?}");

    let encode_header = |header: Header| {
        let mut res = Vec::new();
        header.encode(&mut res);
        Ok(hex::encode(&res))
    };

    match chain_state
        .get_block_header(BlockTagOrHash::BlockTags(params.block))
        .await
    {
        Ok(header) => encode_header(header),
        Err(_) => Err(JsonRpcError::internal_error("block data not found".into())),
    }
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
#[serde(transparent)]
pub struct MonadDebugGetRawReceiptsResult {
    receipts: Vec<String>,
}

#[rpc(method = "debug_getRawReceipts")]
#[allow(non_snake_case)]
/// Returns an array of EIP-2718 binary-encoded receipts.
pub async fn monad_debug_getRawReceipts<T: Triedb>(
    chain_state: &ChainState<T>,
    params: DebugBlockParams,
) -> JsonRpcResult<MonadDebugGetRawReceiptsResult> {
    trace!("monad_debug_getRawReceipts: {params:?}");

    let encode_receipts = |receipts: Vec<ReceiptEnvelope<alloy_primitives::Log>>| {
        let receipts = receipts
            .into_iter()
            .map(|r| {
                let mut res = Vec::new();
                r.encode(&mut res);
                hex::encode(&res)
            })
            .collect();
        Ok(MonadDebugGetRawReceiptsResult { receipts })
    };

    match chain_state.get_raw_receipts(params.block).await {
        Ok(receipts) => encode_receipts(receipts),
        Err(_) => Err(JsonRpcError::internal_error("block data not found".into())),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadDebugGetRawTransactionParams {
    tx_hash: EthHash,
}

#[rpc(method = "debug_getRawTransaction")]
#[allow(non_snake_case)]
/// Returns an array of EIP-2718 binary-encoded transactions.
pub async fn monad_debug_getRawTransaction<T: Triedb>(
    chain_state: &ChainState<T>,
    params: MonadDebugGetRawTransactionParams,
) -> JsonRpcResult<String> {
    trace!("monad_debug_getRawTransaction: {params:?}");

    match chain_state.get_transaction(params.tx_hash.0).await {
        Ok(tx) => {
            let mut res = Vec::new();
            let tx: TxEnvelope = tx.into();
            tx.encode_2718(&mut res);
            Ok(hex::encode(&res))
        }
        Err(_) => Err(JsonRpcError::internal_error("block data not found".into())),
    }
}

#[derive(Clone, Debug)]
struct CallFrame {
    typ: CallKind,
    #[allow(dead_code)]
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

#[derive(Deserialize, Debug, Default, schemars::JsonSchema, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TracerObject {
    #[serde(default)]
    pub tracer: Tracer,
    #[serde(default, rename = "tracerConfig")]
    pub config: TracerConfig,
}

#[derive(Deserialize, Debug, Default, schemars::JsonSchema, Clone, PartialEq, Eq)]
pub enum Tracer {
    #[default]
    #[serde(rename = "callTracer")]
    CallTracer,
    #[serde(rename = "prestateTracer")]
    PreStateTracer,
}

#[derive(Clone, Debug, Deserialize, Default, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TracerConfig {
    /// onlyTopCall for callTracer, ignored for prestateTracer
    #[serde(default)]
    pub only_top_call: bool,

    /// diff mode for prestateTracer, ignored for callTracer
    #[serde(default)]
    pub diff_mode: bool,
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
    pub typ: CallKind,
    pub from: EthAddress,
    pub to: Option<EthAddress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<MonadU256>,
    pub gas: Quantity,
    pub gas_used: Quantity, 
    pub input: UnformattedData,
    #[serde(skip_serializing_if = "UnformattedData::is_empty")]
    pub output: UnformattedData,
    #[serde(skip)]
    pub depth: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revert_reason: Option<String>,
    // FIXME why Rc<RefCell<_>> ?
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[schemars(skip)] // TODO: handle recursive generation in jsonrpc schema
    pub calls: Vec<std::rc::Rc<std::cell::RefCell<MonadCallFrame>>>,
}

impl From<CallFrame> for MonadCallFrame {
    fn from(value: CallFrame) -> Self {
        // the “value” argument is not included for STATICALL
        let frame_value = if matches!(value.typ, CallKind::StaticCall) {
            None
        } else {
            Some(MonadU256(value.value))
        };

        // Status maps to the evmc_status_code enum
        let error = match value.status.to::<usize>() {
            0 => None,
            2 => Some("execution reverted".to_string()),
            3 => Some("out of gas".to_string()),
            _ => Some("error".to_string()),
        };

        let revert_reason = error
            .as_ref()
            .and_then(|_| monad_ethcall::decode_revert_message(&value.output));

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
            error,
            revert_reason,
            calls: Vec::new(),
        }
    }
}

#[derive(Serialize, Debug, Clone, schemars::JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum CallKind {
    Call,
    DelegateCall,
    CallCode,
    Create,
    Create2,
    SelfDestruct,
    StaticCall,
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
    archive_reader: &Option<ArchiveReader>,
    params: MonadDebugTraceBlockByHashParams,
) -> JsonRpcResult<Vec<MonadDebugTraceBlockResult>> {
    trace!("monad_debugTraceBlockByHash: {params:?}");

    let latest_block_key = get_block_key_from_tag(triedb_env, BlockTags::Latest);
    if let Some(block_num) = triedb_env
        .get_block_number_by_hash(latest_block_key, params.block_hash.0)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        let block_key = triedb_env.get_block_key(SeqNum(block_num));
        if let Ok(result) = get_call_frames_from_triedb(triedb_env, block_key, &params.tracer).await
        {
            return Ok(result);
        }
    }

    // try archive if block hash not found and archive reader specified
    let mut resp = Vec::new();
    if let Some(archive_reader) = archive_reader {
        if let Some(block) = archive_reader
            .try_get_block_by_hash(&params.block_hash.0.into())
            .await?
        {
            if let Some(call_frames) = archive_reader
                .try_get_block_traces(block.header.number)
                .await?
            {
                let tx_ids = block.body.transactions().map(|tx| *tx.tx.tx_hash());

                for (call_frame, tx_id) in call_frames.iter().zip(tx_ids) {
                    let rlp_call_frame = &mut call_frame.as_slice();
                    let Some(traces) = decode_call_frame(
                        triedb_env,
                        rlp_call_frame,
                        BlockKey::Finalized(FinalizedBlockKey(SeqNum(block.header.number))),
                        &params.tracer,
                    )
                    .await?
                    else {
                        return Err(JsonRpcError::internal_error("traces not found".to_string()));
                    };
                    resp.push(MonadDebugTraceBlockResult {
                        tx_hash: FixedData::<32>::from(tx_id),
                        result: traces,
                    });
                }

                return Ok(resp);
            }
        }
    }

    Err(JsonRpcError::internal_error("block not found".into()))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadDebugTraceBlockByNumberParams {
    block_number: BlockTags,
    #[serde(default)]
    tracer: TracerObject,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MonadDebugTraceBlockResult {
    pub tx_hash: EthHash,
    pub result: MonadCallFrame,
}

#[rpc(method = "debug_traceBlockByNumber")]
#[allow(non_snake_case)]
/// Returns the tracing result by executing all transactions in the block specified by the block number with a tracer.
pub async fn monad_debug_traceBlockByNumber<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadDebugTraceBlockByNumberParams,
) -> JsonRpcResult<Vec<MonadDebugTraceBlockResult>> {
    trace!("monad_debugTraceBlockByNumber: {params:?}");

    let block_key = get_block_key_from_tag(triedb_env, params.block_number);
    if let Ok(result) = get_call_frames_from_triedb(triedb_env, block_key, &params.tracer).await {
        return Ok(result);
    }

    // try archive if block number or transactions not found and archive reader specified
    let mut resp = Vec::new();
    if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
        (archive_reader, block_key)
    {
        if let Some(block) = archive_reader.try_get_block_by_number(block_num.0).await? {
            if let Some(call_frames) = archive_reader.try_get_block_traces(block_num.0).await? {
                let tx_ids = block.body.transactions().map(|tx| *tx.tx.tx_hash());

                // TODO: parallelize this with stream + buffered + try_collect
                for (call_frame, tx_id) in call_frames.iter().zip(tx_ids) {
                    let rlp_call_frame = &mut call_frame.as_slice();
                    let Some(traces) =
                        decode_call_frame(triedb_env, rlp_call_frame, block_key, &params.tracer)
                            .await?
                    else {
                        return Err(JsonRpcError::internal_error("traces not found".to_string()));
                    };
                    resp.push(MonadDebugTraceBlockResult {
                        tx_hash: FixedData::<32>::from(tx_id),
                        result: traces,
                    });
                }

                return Ok(resp);
            }
        }
    }

    Err(JsonRpcError::internal_error("block not found".into()))
}

#[rpc(method = "debug_traceTransaction")]
#[allow(non_snake_case)]
/// Returns all traces of a given transaction.
pub async fn monad_debug_traceTransaction<T: Triedb>(
    triedb_env: &T,
    archive_reader: &Option<ArchiveReader>,
    params: MonadDebugTraceTransactionParams,
) -> JsonRpcResult<Option<MonadCallFrame>> {
    trace!("monad_eth_debugTraceTransaction: {params:?}");

    let latest_block_key = get_block_key_from_tag(triedb_env, BlockTags::Latest);
    if let Some(tx_loc) = triedb_env
        .get_transaction_location_by_hash(latest_block_key, params.tx_hash.0)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        let block_key = triedb_env.get_block_key(SeqNum(tx_loc.block_num));
        if let Some(rlp_call_frame) = triedb_env
            .get_call_frame(block_key, tx_loc.tx_index)
            .await
            .map_err(JsonRpcError::internal_error)?
        {
            let rlp_call_frame = &mut rlp_call_frame.as_slice();
            return decode_call_frame(triedb_env, rlp_call_frame, block_key, &params.tracer).await;
        }
    }

    // try archive if transaction hash not found and archive reader specified
    if let Some(archive_reader) = archive_reader {
        if let Some((trace, header_subset)) =
            archive_reader.get_trace(&params.tx_hash.0.into()).await?
        {
            let rlp_call_frame = &mut trace.as_slice();
            return decode_call_frame(
                triedb_env,
                rlp_call_frame,
                BlockKey::Finalized(FinalizedBlockKey(SeqNum(header_subset.block_number))),
                &params.tracer,
            )
            .await;
        }
    }

    Ok(None)
}

pub async fn get_call_frames_from_triedb<T: Triedb>(
    triedb_env: &T,
    block_key: BlockKey,
    tracer: &TracerObject,
) -> JsonRpcResult<Vec<MonadDebugTraceBlockResult>> {
    let mut resp = Vec::new();
    let transactions = triedb_env
        .get_transactions(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?;

    let tx_ids = transactions
        .iter()
        .map(|tx| *tx.tx.tx_hash())
        .collect::<Vec<_>>();

    let call_frames = triedb_env
        .get_call_frames(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?;

    for (call_frame, tx_id) in call_frames.iter().zip(tx_ids.into_iter()) {
        let rlp_call_frame = &mut call_frame.as_slice();
        let Some(traces) = decode_call_frame(triedb_env, rlp_call_frame, block_key, tracer).await?
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

pub async fn decode_call_frame<T: Triedb>(
    triedb_env: &T,
    rlp_call_frame: &mut &[u8],
    block_key: BlockKey,
    tracer: &TracerObject,
) -> JsonRpcResult<Option<MonadCallFrame>> {
    let mut call_frames = Vec::<Vec<CallFrame>>::decode(rlp_call_frame)
        .map_err(|e| JsonRpcError::custom(format!("Rlp Decode error: {e}")))?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    match tracer.tracer {
        Tracer::CallTracer => {
            // Diff mode is supported only by the prestate tracer
            if tracer.config.diff_mode {
                Err(JsonRpcError::method_not_supported())
            } else if tracer.config.only_top_call {
                if call_frames.is_empty() {
                    Ok(None)
                } else {
                    let mut call_frame = call_frames.remove(0);
                    include_code_output(&mut call_frame, triedb_env, block_key).await?;
                    Ok(Some(MonadCallFrame::from(call_frame)))
                }
            } else {
                let call_frames = futures::future::join_all(
                    call_frames
                        .into_iter()
                        .map(|mut frame| async move {
                            include_code_output(&mut frame, triedb_env, block_key).await?;
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
    block_key: BlockKey,
) -> JsonRpcResult<()> {
    if matches!(frame.typ, CallKind::Create) || matches!(frame.typ, CallKind::Create2) {
        let Some(contract_addr) = &frame.to else {
            error!("expected contract address in call frame");
            return Err(JsonRpcError::internal_error(
                "contract address not found in call frame".to_string(),
            ));
        };

        let account = triedb_env
            .get_account(block_key, contract_addr.0.into())
            .await
            .map_err(JsonRpcError::internal_error)?;
        let code = triedb_env
            .get_code(block_key, account.code_hash)
            .await
            .map_err(JsonRpcError::internal_error)?;

        let decoded_code = hex::decode(&code)
            .map_err(|_| JsonRpcError::internal_error("could not decode code".to_string()))?;
        frame.output = decoded_code.into();
    }

    Ok(())
}

async fn build_call_tree(
    nodes: Vec<CallFrame>,
) -> JsonRpcResult<
    Option<
        // FIXME why Rc<RefCell<_>> ?
        std::rc::Rc<std::cell::RefCell<MonadCallFrame>>,
    >,
> {
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

#[cfg(test)]
mod tests {
    use monad_triedb_utils::{
        mock_triedb,
        triedb_env::{EthTxHash, TransactionLocation},
    };

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
        let result = build_call_tree(frames).await.unwrap();

        assert!(result.is_some());
        let result: Rc<RefCell<MonadCallFrame>> = result.unwrap();
        assert_eq!(result.borrow().calls.len(), 1);

        result
            .borrow()
            .calls
            .iter()
            .enumerate()
            .for_each(|(idx, frame)| match idx {
                0 => assert_eq!(frame.borrow().calls.len(), 1),

                1 => assert_eq!(frame.borrow().calls.len(), 1),

                2 => assert_eq!(frame.borrow().calls.len(), 2),

                _ => panic!("unexpected index"),
            });

        assert_eq!(result.borrow().error, None);
    }

    #[tokio::test]
    async fn debug_trace_revert() {
        // Reverted contract call
        let frame = hex::decode("0xf83ef83c808094f39fd6e51aad88f6f4ce6ab8827279cfffb9226694e7f1725e7734ce288f8367e1bb143e90bb3f0512808307a12082529884b0bea725800280").expect("decode call frame");
        let mut mock_triedb = mock_triedb::MockTriedb::default();
        mock_triedb.set_transaction_location_by_hash(
            EthTxHash::default(),
            TransactionLocation {
                block_num: 1,
                tx_index: 0,
            },
        );

        mock_triedb.set_call_frame(
            TransactionLocation {
                block_num: 1,
                tx_index: 0,
            },
            frame,
        );

        let resp = monad_debug_traceTransaction(
            &mock_triedb,
            &None,
            MonadDebugTraceTransactionParams {
                tx_hash: FixedData::<32>([0u8; 32]),
                tracer: TracerObject::default(),
            },
        )
        .await
        .unwrap();

        assert!(resp.is_some());

        let resp = resp.unwrap();
        assert_eq!(resp.error, Some("execution reverted".to_string()));
        assert!(resp.revert_reason.is_none());
        assert_eq!(resp.calls.len(), 0);
        assert_eq!(
            resp.from.0,
            *hex::decode("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266").unwrap()
        );
        assert_eq!(
            resp.to.unwrap().0,
            *hex::decode("0xe7f1725e7734ce288f8367e1bb143e90bb3f0512").unwrap()
        );
        assert_eq!(resp.gas.0, 500000);
        assert_eq!(resp.gas_used.0, 21144);
        assert_eq!(resp.input.0, *hex::decode("0xb0bea725").unwrap());
        assert_eq!(resp.output.0, [0u8; 0]);
        assert!(matches!(resp.typ, CallKind::Call));
        assert_eq!(resp.value.unwrap().0, U256::ZERO);
    }

    #[tokio::test]
    async fn debug_trace_create() {
        // contract creation
        let frame = hex::decode("0xf901b9f901b6038094f39fd6e51aad88f6f4ce6ab8827279cfffb9226694dc64a140aa3e981100a9beca4e685f962f0cf6c98083018d9583018a75b8976080604052348015600f57600080fd5b5060e48061001e6000396000f3fe608060405260043610603f5760003560e01c80635c60da1b146044575b600080fd5b605060048036036020811015605857600080fd5b5035606e565b005b6000548156fea2646970667358221220a0f2af6f9a7d2b0c8c3c32bd2d8a4f3d856c7f8a8888a1e0dc8b9a8a2a47e2ea64736f6c63430008000033b8e4608060405260043610603f5760003560e01c80635c60da1b146044575b600080fd5b605060048036036020811015605857600080fd5b5035606e565b005b6000548156fea2646970667358221220a0f2af6f9a7d2b0c8c3c32bd2d8a4f3d856c7f8a8888a1e0dc8b9a8a2a47e2ea64736f6c6343000800003300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008080").expect("decode call frame");
        let mut mock_triedb = mock_triedb::MockTriedb::default();
        mock_triedb.set_transaction_location_by_hash(
            EthTxHash::default(),
            TransactionLocation {
                block_num: 1,
                tx_index: 0,
            },
        );

        mock_triedb.set_call_frame(
            TransactionLocation {
                block_num: 1,
                tx_index: 0,
            },
            frame,
        );

        mock_triedb.set_code("0x608060405260043610603f5760003560e01c80635c60da1b146044575b600080fd5b605060048036036020811015605857600080fd5b5035606e565b005b6000548156fea2646970667358221220a0f2af6f9a7d2b0c8c3c32bd2d8a4f3d856c7f8a8888a1e0dc8b9a8a2a47e2ea64736f6c634300080000330000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000".to_string());

        let resp = monad_debug_traceTransaction(
            &mock_triedb,
            &None,
            MonadDebugTraceTransactionParams {
                tx_hash: FixedData::<32>([0u8; 32]),
                tracer: TracerObject::default(),
            },
        )
        .await
        .unwrap();

        assert!(resp.is_some());
        let resp = resp.unwrap();
        assert!(resp.calls.is_empty());
        assert!(resp.error.is_none());
        assert!(resp.revert_reason.is_none());
        assert!(matches!(resp.typ, CallKind::Create));
        assert_eq!(
            resp.from.0,
            *hex::decode("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266").unwrap()
        );
        assert_eq!(
            resp.to.unwrap().0,
            *hex::decode("0xdc64a140aa3e981100a9beca4e685f962f0cf6c9").unwrap()
        );
        assert_eq!(resp.gas.0, 101781);
        assert_eq!(resp.gas_used.0, 100981);
        assert_eq!(resp.input.0, *hex::decode("0x6080604052348015600f57600080fd5b5060e48061001e6000396000f3fe608060405260043610603f5760003560e01c80635c60da1b146044575b600080fd5b605060048036036020811015605857600080fd5b5035606e565b005b6000548156fea2646970667358221220a0f2af6f9a7d2b0c8c3c32bd2d8a4f3d856c7f8a8888a1e0dc8b9a8a2a47e2ea64736f6c63430008000033").unwrap());
        assert_eq!(resp.output.0, *hex::decode("0x608060405260043610603f5760003560e01c80635c60da1b146044575b600080fd5b605060048036036020811015605857600080fd5b5035606e565b005b6000548156fea2646970667358221220a0f2af6f9a7d2b0c8c3c32bd2d8a4f3d856c7f8a8888a1e0dc8b9a8a2a47e2ea64736f6c634300080000330000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000").unwrap());
        assert_eq!(resp.depth, 0);
    }
}
