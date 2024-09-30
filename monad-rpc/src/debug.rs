use alloy_rlp::Encodable;
use monad_blockdb::EthTxKey;
use monad_blockdb_utils::{BlockDB, BlockDbEnv};
use monad_rpc_docs::rpc;
use reth_primitives::B256;
use serde::{Deserialize, Serialize};

use crate::{
    eth_json_types::{deserialize_fixed_data, BlockTags, EthHash, MonadU256},
    hex,
    jsonrpc::{JsonRpcError, JsonRpcResult, JsonRpcResultExt},
    trace::{TraceCallObject, TracerObject},
    triedb::{Triedb, TriedbEnv, TriedbResult},
};

#[rpc(method = "debug_getRawBlock")]
#[allow(non_snake_case)]
/// Returns an RLP-encoded block.
pub async fn monad_debug_getRawBlock(
    blockdb_env: &BlockDbEnv,
    params: BlockTags,
) -> JsonRpcResult<String> {
    let block = blockdb_env
        .get_block_by_tag(params.into())
        .await
        .block_not_found()?;

    let mut buf = Vec::default();
    block.block.encode(&mut buf);
    Ok(hex::encode(&buf))
}

#[rpc(method = "debug_getRawHeader")]
#[allow(non_snake_case)]
/// Returns an RLP-encoded header.
pub async fn monad_debug_getRawHeader(
    blockdb_env: &BlockDbEnv,
    params: BlockTags,
) -> JsonRpcResult<String> {
    let value = blockdb_env
        .get_block_by_tag(params.into())
        .await
        .block_not_found()?;

    let mut buf = Vec::default();
    value.block.header.encode(&mut buf);
    Ok(hex::encode(&buf))
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
#[serde(transparent)]
pub struct MonadDebugGetRawReceiptsResult {
    receipts: Vec<String>,
}

#[rpc(method = "debug_getRawReceipts")]
#[allow(non_snake_case)]
/// Returns an array of EIP-2718 binary-encoded receipts.
pub async fn monad_debug_getRawReceipts(
    blockdb_env: &BlockDbEnv,
    triedb_env: &TriedbEnv,
    params: BlockTags,
) -> JsonRpcResult<MonadDebugGetRawReceiptsResult> {
    let block = blockdb_env
        .get_block_by_tag(params.into())
        .await
        .block_not_found()?;

    let mut receipts = Vec::new();
    for txn_index in 0..block.block.body.len() {
        match triedb_env
            .get_receipt(txn_index as u64, block.block.number)
            .await
        {
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

#[rpc(method = "debug_getRawTransaction")]
#[allow(non_snake_case)]
/// Returns an array of EIP-2718 binary-encoded transactions.
pub async fn monad_debug_getRawTransaction(
    blockdb_env: &BlockDbEnv,
    params: MonadDebugGetRawTransactionParams,
) -> JsonRpcResult<String> {
    let key = EthTxKey(B256::new(params.tx_hash.0));
    let result = blockdb_env.get_txn(key).await.block_not_found()?;

    let block_key = result.block_hash;
    let block = blockdb_env
        .get_block_by_hash(block_key)
        .await
        .expect("txn was found so its block should exist");

    let transaction = block
        .block
        .body
        .get(result.transaction_index as usize)
        .expect("txn and block found so its index should be correct");

    let mut buf = Vec::default();
    transaction.encode_enveloped(&mut buf);
    Ok(hex::encode(&buf))
}

#[rpc(method = "debug_traceBlockByHash")]
#[allow(non_snake_case)]
/// Returns the tracing result by executing all transactions in the block specified by the block hash with a tracer.
pub async fn monad_debug_traceBlockByHash(
    blockdb_env: &BlockDbEnv,
    params: EthHash,
) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[rpc(method = "debug_traceBlockByNumber")]
#[allow(non_snake_case)]
/// Returns the tracing result by executing all transactions in the block specified by the block number with a tracer.
pub async fn monad_debug_traceBlockByNumber(
    blockdb_env: &BlockDbEnv,
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
