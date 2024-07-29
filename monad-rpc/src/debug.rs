use alloy_rlp::Encodable;
use monad_rpc_docs::rpc;
use reth_primitives::{Header, ReceiptWithBloom};
use serde::{Deserialize, Serialize};

use crate::{
    eth_json_types::{BlockTags, EthHash},
    hex,
    jsonrpc::{JsonRpcError, JsonRpcResult},
    trace::{TraceCallObject, TracerObject},
    triedb::{get_block_num_from_tag, Triedb},
};

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct DebugBlockParams {
    block: BlockTags,
}

#[rpc(method = "debug_getRawBlock")]
#[allow(non_snake_case)]
/// Returns an RLP-encoded block.
pub async fn monad_debug_getRawBlock<T: Triedb>(
    triedb_env: &T,
    params: DebugBlockParams,
) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[rpc(method = "debug_getRawHeader")]
#[allow(non_snake_case)]
/// Returns an RLP-encoded header.
pub async fn monad_debug_getRawHeader<T: Triedb>(
    triedb_env: &T,
    params: DebugBlockParams,
) -> JsonRpcResult<String> {
    let block_num = get_block_num_from_tag(triedb_env, params.block).await?;
    let header = match triedb_env.get_block_header(block_num).await? {
        Some(header) => header,
        None => return Ok("0x0".to_string()),
    };
    let mut rlp_header: &mut [u8] = &mut [];
    Header::encode(&header.header, &mut rlp_header);
    Ok(hex::encode(rlp_header))
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
    let transactions = triedb_env.get_transactions(block_num).await?;

    let mut receipts = Vec::new();
    for txn_index in 0..transactions.len() {
        let Some(receipt) = triedb_env.get_receipt(txn_index as u64, block_num).await? else {
            return Ok(MonadDebugGetRawReceiptsResult { receipts: vec![] });
        };
        let mut rlp_receipt: &mut [u8] = &mut [];
        ReceiptWithBloom::encode(&receipt, &mut rlp_receipt);
        receipts.push(hex::encode(rlp_receipt));
    }

    Ok(MonadDebugGetRawReceiptsResult { receipts })
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadDebugGetRawTransactionParams {
    tx_hash: EthHash,
}

#[rpc(method = "debug_getRawTransaction")]
#[allow(non_snake_case)]
/// Returns an array of EIP-2718 binary-encoded transactions.
pub async fn monad_debug_getRawTransaction<T: Triedb>(
    triedb_env: &T,
    params: MonadDebugGetRawTransactionParams,
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
