use alloy_rlp::Encodable;
use monad_rpc_docs::rpc;
use serde::{Deserialize, Serialize};

use crate::{
    block_util::{get_block_from_num, get_block_num_from_tag, BlockResult, FileBlockReader},
    eth_json_types::{deserialize_fixed_data, BlockTags, EthHash, MonadU256},
    hex,
    jsonrpc::{JsonRpcError, JsonRpcResult},
    trace::{TraceCallObject, TracerObject},
    triedb::{TriedbEnv, TriedbResult},
};

#[rpc(method = "debug_getRawBlock", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns an RLP-encoded block.
pub async fn monad_debug_getRawBlock(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &TriedbEnv,
    params: BlockTags,
) -> JsonRpcResult<String> {
    let block_num = get_block_num_from_tag(triedb_env, params).await?;
    let Ok(raw_block) = file_ledger_reader
        .async_read_encoded_eth_block(block_num)
        .await
    else {
        return Err(JsonRpcError::internal_error());
    };
    Ok(hex::encode(&raw_block))
}

#[rpc(method = "debug_getRawHeader", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns an RLP-encoded header.
pub async fn monad_debug_getRawHeader(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &TriedbEnv,
    params: BlockTags,
) -> JsonRpcResult<String> {
    let block_num = get_block_num_from_tag(triedb_env, params).await?;
    let block = match get_block_from_num(file_ledger_reader, block_num).await {
        BlockResult::Block(b) => b,
        BlockResult::NotFound => return Err(JsonRpcError::custom(format!("block not found"))),
        BlockResult::DecodeFailed(e) => {
            return Err(JsonRpcError::custom(format!("decode block failed: {}", e)))
        }
    };

    let mut buf = Vec::default();
    block.header.encode(&mut buf);
    Ok(hex::encode(&buf))
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
#[serde(transparent)]
pub struct MonadDebugGetRawReceiptsResult {
    receipts: Vec<String>,
}

#[rpc(method = "debug_getRawReceipts", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns an array of EIP-2718 binary-encoded receipts.
pub async fn monad_debug_getRawReceipts(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &TriedbEnv,
    params: BlockTags,
) -> JsonRpcResult<MonadDebugGetRawReceiptsResult> {
    let block_num = get_block_num_from_tag(triedb_env, params).await?;
    let block = match get_block_from_num(file_ledger_reader, block_num).await {
        BlockResult::Block(b) => b,
        BlockResult::NotFound => return Err(JsonRpcError::custom(format!("block not found"))),
        BlockResult::DecodeFailed(e) => {
            return Err(JsonRpcError::custom(format!("decode block failed: {}", e)))
        }
    };

    let mut receipts = Vec::new();
    for txn_index in 0..block.body.len() {
        match triedb_env.get_receipt(txn_index as u64, block.number).await {
            TriedbResult::Null => continue,
            TriedbResult::Receipt(rlp_receipt) => {
                let receipt = hex::encode(&rlp_receipt);
                receipts.push(receipt);
            }
            _ => return Err(JsonRpcError::internal_error()),
        }
    }

    Ok(MonadDebugGetRawReceiptsResult { receipts })
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadDebugGetRawTransactionParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    tx_hash: EthHash,
}

#[rpc(method = "debug_getRawTransaction", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns an array of EIP-2718 binary-encoded transactions.
pub async fn monad_debug_getRawTransaction(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &TriedbEnv,
    params: MonadDebugGetRawTransactionParams,
) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[rpc(method = "debug_traceBlockByHash", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns the tracing result by executing all transactions in the block specified by the block hash with a tracer.
pub async fn monad_debug_traceBlockByHash(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &TriedbEnv,
    params: EthHash,
) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[rpc(method = "debug_traceBlockByNumber", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns the tracing result by executing all transactions in the block specified by the block number with a tracer.
pub async fn monad_debug_traceBlockByNumber(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &TriedbEnv,
    params: MonadU256,
) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct DebugTraceCallParams {
    pub call: Vec<TraceCallObject>,
    pub block: BlockTags,
    pub tracer: TracerObject,
}

#[rpc(method = "debug_traceCall")]
#[allow(non_snake_case)]
/// Returns the tracing result by executing an eth call within the context of the given block execution.
pub async fn monad_debug_traceCall(
    triedb_env: &TriedbEnv,
    params: DebugTraceCallParams,
) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct DebugTraceTransactionParams {
    pub tx: EthHash,
    pub tracer: TracerObject,
}

#[rpc(method = "debug_traceTransaction")]
#[allow(non_snake_case)]
/// Returns all traces of a given transaction.
pub async fn monad_debug_traceTransaction(
    triedb_env: &TriedbEnv,
    params: DebugTraceTransactionParams,
) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}
