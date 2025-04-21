use alloy_consensus::{Block, BlockBody, ReceiptEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_rlp::Encodable;
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{TransactionLocation, Triedb};
use monad_types::SeqNum;
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    block_handlers::get_block_key_from_tag,
    eth_json_types::{BlockTags, EthHash},
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
    triedb_env: &T,
    params: DebugBlockParams,
) -> JsonRpcResult<String> {
    trace!("monad_debug_getRawBlock: {params:?}");
    let block_key = get_block_key_from_tag(triedb_env, params.block);

    let header = match triedb_env
        .get_block_header(block_key)
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
        .get_transactions(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
        .into_iter()
        .map(|recovered_tx| recovered_tx.tx)
        .collect();

    let block = Block {
        header: header.header,
        body: BlockBody {
            transactions,
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
    trace!("monad_debug_getRawHeader: {params:?}");
    let block_key = get_block_key_from_tag(triedb_env, params.block);
    let header = match triedb_env
        .get_block_header(block_key)
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
    trace!("monad_debug_getRawReceipts: {params:?}");
    let block_key = get_block_key_from_tag(triedb_env, params.block);
    let receipts: Vec<ReceiptEnvelope> = triedb_env
        .get_receipts(block_key)
        .await
        .map_err(JsonRpcError::internal_error)?
        .into_iter()
        .map(|receipt_with_log_index| receipt_with_log_index.receipt)
        .collect();

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
    trace!("monad_debug_getRawTransaction: {params:?}");
    let latest_block_key = get_block_key_from_tag(triedb_env, BlockTags::Latest);
    let Some(TransactionLocation {
        tx_index,
        block_num,
    }) = triedb_env
        .get_transaction_location_by_hash(latest_block_key, params.tx_hash.0)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Err(JsonRpcError::internal_error("transaction not found".into()));
    };

    let Some(tx) = triedb_env
        .get_transaction(triedb_env.get_block_key(SeqNum(block_num)), tx_index)
        .await
        .map_err(JsonRpcError::internal_error)?
    else {
        return Err(JsonRpcError::internal_error("transaction not found".into()));
    };

    let mut res = Vec::new();
    tx.tx.encode_2718(&mut res);
    Ok(hex::encode(&res))
}
