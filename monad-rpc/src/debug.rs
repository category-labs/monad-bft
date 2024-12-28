use alloy_rlp::Encodable;
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{TransactionLocation, Triedb};
use reth_primitives::{Block, BlockBody, TransactionSigned};
use serde::{Deserialize, Serialize};

use crate::{
    block_handlers::get_block_num_from_tag,
    eth_json_types::{BlockTags, EthHash},
    hex,
    jsonrpc::{JsonRpcError, JsonRpcResult},
    trace::{TraceCallObject, TracerObject},
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
    let block_num = get_block_num_from_tag(triedb_env, params.block).await?;

    let header = match triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => {
            return Err(JsonRpcError::internal_error(
                "block header not found".into(),
            ))
        }
    };
    let transactions = triedb_env
        .get_transactions(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?;

    let block = Block {
        header: header.header,
        body: BlockBody {
            transactions: transactions
                .into_iter()
                .map(TransactionSigned::from)
                .collect(),
            ommers: Vec::new(),
            withdrawals: None,
        },
    };

    let mut res = Vec::new();
    block.encode(&mut res);
    Ok(hex::encode(&res))
}

#[rpc(method = "debug_getRawHeader")]
#[allow(non_snake_case)]
/// Returns an RLP-encoded header.
pub async fn monad_debug_getRawHeader<T: Triedb>(
    triedb_env: &T,
    params: DebugBlockParams,
) -> JsonRpcResult<String> {
    let block_num = get_block_num_from_tag(triedb_env, params.block).await?;
    let header = match triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => {
            return Err(JsonRpcError::internal_error(
                "block header not found".into(),
            ))
        }
    };

    let mut res = Vec::new();
    header.header.encode(&mut res);
    Ok(hex::encode(&res))
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
    let receipts = triedb_env
        .get_receipts(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?;

    let mut rlp_receipts = Vec::new();
    for receipt in receipts {
        let mut res = Vec::new();
        receipt.encode(&mut res);
        rlp_receipts.push(hex::encode(&res));
    }

    Ok(MonadDebugGetRawReceiptsResult {
        receipts: rlp_receipts,
    })
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
    let latest_block_num = get_block_num_from_tag(triedb_env, BlockTags::Latest).await?;
    let Some(TransactionLocation {
        tx_index,
        block_num,
    }) = triedb_env
        .get_transaction_location_by_hash(params.tx_hash.0, latest_block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Err(JsonRpcError::internal_error("transaction not found".into()));
    };

    let Some(tx) = triedb_env
        .get_transaction(tx_index, block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Err(JsonRpcError::internal_error("transaction not found".into()));
    };

    let mut res = Vec::new();
    tx.encode(&mut res);
    Ok(hex::encode(&res))
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
