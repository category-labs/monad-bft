use monad_rpc_docs::rpc;
use serde::{Deserialize, Serialize};

use crate::{
    block_util::{get_block_num_from_tag, FileBlockReader},
    eth_json_types::{BlockTags, EthHash, MonadU256},
    hex,
    jsonrpc::{JsonRpcError, JsonRpcResult},
    trace::{TraceCallObject, TracerObject},
    triedb::{Triedb, TriedbResult},
};

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct DebugBlockParams {
    block: BlockTags,
}

#[rpc(method = "debug_getRawBlock", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns an RLP-encoded block.
pub async fn monad_debug_getRawBlock<T: Triedb>(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &T,
    params: DebugBlockParams,
) -> JsonRpcResult<String> {
    let block_num = get_block_num_from_tag(triedb_env, params.block).await?;
    let Ok(raw_block) = file_ledger_reader
        .async_read_encoded_eth_block(block_num)
        .await
    else {
        return Err(JsonRpcError::internal_error(
            "error reading block data".into(),
        ));
    };
    Ok(hex::encode(&raw_block))
}

#[rpc(method = "debug_getRawHeader", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns an RLP-encoded header.
pub async fn monad_debug_getRawHeader<T: Triedb>(
    triedb_env: &T,
    params: DebugBlockParams,
) -> JsonRpcResult<String> {
    let block_num = get_block_num_from_tag(triedb_env, params.block).await?;
    match triedb_env.get_block_header(block_num).await {
        TriedbResult::BlockHeader(block_header_rlp) => Ok(hex::encode(&block_header_rlp)),
        _ => Err(JsonRpcError::internal_error(
            "error reading block header from db".into(),
        )),
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
    triedb_env: &T,
    params: DebugBlockParams,
) -> JsonRpcResult<MonadDebugGetRawReceiptsResult> {
    let block_num = get_block_num_from_tag(triedb_env, params.block).await?;
    let transactions = match triedb_env.get_transactions(block_num).await {
        TriedbResult::BlockTransactions(transactions) => transactions,
        TriedbResult::Null => vec![],
        _ => {
            return Err(JsonRpcError::internal_error(
                "error reading transactions from db".into(),
            ))
        }
    };

    let mut receipts = Vec::new();
    for txn_index in 0..transactions.len() {
        match triedb_env.get_receipt(txn_index as u64, block_num).await {
            TriedbResult::Null => continue,
            TriedbResult::Receipt(rlp_receipt) => {
                let receipt = hex::encode(&rlp_receipt);
                receipts.push(receipt);
            }
            _ => return Err(JsonRpcError::internal_error("error reading from db".into())),
        }
    }

    Ok(MonadDebugGetRawReceiptsResult { receipts })
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadDebugGetRawTransactionParams {
    tx_hash: EthHash,
}

#[rpc(method = "debug_getRawTransaction", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns an array of EIP-2718 binary-encoded transactions.
pub async fn monad_debug_getRawTransaction<T: Triedb>(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &T,
    params: MonadDebugGetRawTransactionParams,
) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[rpc(method = "debug_traceBlockByHash", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns the tracing result by executing all transactions in the block specified by the block hash with a tracer.
pub async fn monad_debug_traceBlockByHash<T: Triedb>(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &T,
    params: EthHash,
) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[rpc(method = "debug_traceBlockByNumber", ignore = "file_ledger_reader")]
#[allow(non_snake_case)]
/// Returns the tracing result by executing all transactions in the block specified by the block number with a tracer.
pub async fn monad_debug_traceBlockByNumber<T: Triedb>(
    file_ledger_reader: &FileBlockReader,
    triedb_env: &T,
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
pub async fn monad_debug_traceCall<T: Triedb>(
    triedb_env: &T,
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
pub async fn monad_debug_traceTransaction<T: Triedb>(
    triedb_env: &T,
    params: DebugTraceTransactionParams,
) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}
