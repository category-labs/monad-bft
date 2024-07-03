use alloy_rlp::Encodable;
use monad_blockdb::EthTxKey;
use monad_blockdb_utils::BlockDbEnv;
use monad_triedb_utils::{TriedbEnv, TriedbResult};
use reth_primitives::B256;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    eth_json_types::{
        deserialize_block_tags, deserialize_fixed_data, serialize_result, BlockTags, EthHash,
    },
    hex,
    jsonrpc::{JsonRpcError, JsonRpcResult, JsonRpcResultExt},
};

#[derive(Deserialize, Debug)]
pub struct MonadDebugGetRawBlockParams {
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_tag: BlockTags,
}

#[allow(non_snake_case)]
pub async fn monad_debug_getRawBlock(
    blockdb_env: &BlockDbEnv,
    params: MonadDebugGetRawBlockParams,
) -> JsonRpcResult<Value> {
    let block = blockdb_env
        .get_block_by_tag(params.block_tag.into())
        .await
        .block_not_found()?;

    let mut buf = Vec::default();
    block.block.encode(&mut buf);
    serialize_result(hex::encode(&buf))
}

#[derive(Deserialize, Debug)]
pub struct MonadDebugGetRawHeaderParams {
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_tag: BlockTags,
}

#[allow(non_snake_case)]
pub async fn monad_debug_getRawHeader(
    blockdb_env: &BlockDbEnv,
    params: MonadDebugGetRawHeaderParams,
) -> JsonRpcResult<Value> {
    let value = blockdb_env
        .get_block_by_tag(params.block_tag.into())
        .await
        .block_not_found()?;

    let mut buf = Vec::default();
    value.block.header.encode(&mut buf);
    serialize_result(hex::encode(&buf))
}

#[derive(Deserialize, Debug)]
pub struct MonadDebugGetRawReceiptsParams {
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_tag: BlockTags,
}

#[allow(non_snake_case)]
pub async fn monad_debug_getRawReceipts(
    blockdb_env: &BlockDbEnv,
    triedb_env: &TriedbEnv,
    params: MonadDebugGetRawReceiptsParams,
) -> JsonRpcResult<Value> {
    let block = blockdb_env
        .get_block_by_tag(params.block_tag.into())
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

    serialize_result(receipts)
}

#[derive(Deserialize, Debug)]
pub struct MonadDebugGetRawTransactionParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    tx_hash: EthHash,
}

#[allow(non_snake_case)]
pub async fn monad_debug_getRawTransaction(
    blockdb_env: &BlockDbEnv,
    params: MonadDebugGetRawTransactionParams,
) -> JsonRpcResult<Value> {
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
    serialize_result(hex::encode(&buf))
}
