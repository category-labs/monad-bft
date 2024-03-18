use alloy_primitives::aliases::{B160, B256};
use log::{debug, trace};
use monad_blockdb::{BlockTableKey, BlockValue, EthTxKey};
use reth_primitives::{BlockHash, TransactionSigned};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    blockdb::BlockDbEnv,
    eth_json_types::{
        deserialize_block_tags, deserialize_fixed_data, deserialize_quantity,
        deserialize_unformatted_data, serialize_result, BlockTags, EthHash, Quantity,
        UnformattedData,
    },
    jsonrpc::JsonRpcError,
};

#[derive(Serialize, Debug)]
struct BlockObject {
    block_hash: B256,
    block_number: u64,
    size: u64,
    gas_limit: u64,
    gas_used: u64,
    transactions: Vec<B256>, //FIXME: need an enum for either full txns or just hashes
}

#[derive(Serialize, Debug)]
struct TransactionObject {
    block_hash: B256,
    block_number: u64,
    to: B160,
    from: B160,
    transaction_index: u64,
}

#[derive(Deserialize, Debug)]
struct MonadEthSendRawTransactionParams {
    #[serde(deserialize_with = "deserialize_unformatted_data")]
    hex_tx: UnformattedData,
}

// TODO: need to support EIP-4844 transactions
#[allow(non_snake_case)]
pub async fn monad_eth_sendRawTransaction(
    ipc: flume::Sender<TransactionSigned>,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_sendRawTransaction: {params:?}");

    let p: MonadEthSendRawTransactionParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    match TransactionSigned::decode_enveloped(&mut &p.hex_tx.0[..]) {
        Ok(txn) => {
            let hash = txn.hash();

            match flume::Sender::send_async(&ipc, txn).await {
                Ok(_) => Ok(Value::String(hash.to_string())),
                Err(e) => {
                    debug!("mempool ipc send error {:?}", e);
                    Err(JsonRpcError::internal_error())
                }
            }
        }
        Err(e) => {
            debug!("eth txn decode failed {:?}", e);
            Err(JsonRpcError::txn_decode_error())
        }
    }
}

#[derive(Deserialize, Debug)]
struct MonadEthGetTransactionByHashParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    tx_hash: EthHash,
}

#[derive(Serialize, Debug)]
struct MonadEthGetTransactionByHashReturn {
    tx_object: Option<TransactionObject>,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getTransactionByHash(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getTransactionByHash: {params:?}");

    let p: MonadEthGetTransactionByHashParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let key = EthTxKey(B256::new(p.tx_hash.0));
    let Some(result) = blockdb_env.get_txn(key).await else {
        return serialize_result(MonadEthGetTransactionByHashReturn { tx_object: None });
    };

    let block_key = result.block_hash;
    let block_hash = block_key.0;
    let block = blockdb_env
        .get_block_by_hash(block_key)
        .await
        .expect("txn was found so its block should exist");

    let transaction = block
        .block
        .body
        .get(result.transaction_index as usize)
        .expect("txn and block found so its index should be correct");

    let to = transaction.transaction.to().unwrap();
    let from = transaction.recover_signer().unwrap();

    let retval = MonadEthGetTransactionByHashReturn {
        tx_object: Some(TransactionObject {
            block_hash: B256::new(block_hash.0),
            block_number: block.block.number,
            transaction_index: result.transaction_index,
            to: to.into(),
            from: from.into(),
        }),
    };

    serialize_result(retval)
}

#[derive(Serialize, Debug)]
struct MonadEthGetBlockReturn {
    block_object: Option<BlockObject>,
}

#[derive(Deserialize, Debug)]
struct MonadEthGetBlockByHashParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    block_hash: EthHash,
    return_full_txns: bool,
}

#[allow(non_snake_case)]
pub fn parse_block_content(value: BlockValue) -> MonadEthGetBlockReturn {
    //TODO: check value.return_full_txns...
    let block_object = BlockObject {
        block_hash: value.block.hash_slow(),
        block_number: value.block.number,
        size: value.block.size() as u64,
        gas_limit: value.block.gas_limit,
        gas_used: value.block.gas_used,
        transactions: value.block.body.iter().map(|t| t.hash()).collect(),
    };

    MonadEthGetBlockReturn {
        block_object: Some(block_object),
    }
}

#[allow(non_snake_case)]
pub async fn monad_eth_getBlockByHash(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getBlockByHash: {params:?}");

    let p: MonadEthGetBlockByHashParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let key = BlockTableKey(BlockHash::new(p.block_hash.0));
    let Some(value) = blockdb_env.get_block_by_hash(key).await else {
        return serialize_result(MonadEthGetBlockReturn { block_object: None });
    };

    let retval = parse_block_content(value);
    serialize_result(retval)
}

#[derive(Deserialize, Debug)]
struct MonadEthGetBlockByNumberParams {
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_number: BlockTags,
    return_full_txns: bool,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getBlockByNumber(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getBlockByNumber: {params:?}");

    let p: MonadEthGetBlockByNumberParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let Some(value) = blockdb_env.get_block_by_tag(p.block_number).await else {
        return serialize_result(MonadEthGetBlockReturn { block_object: None });
    };

    let retval = parse_block_content(value);
    serialize_result(retval)
}

#[derive(Deserialize, Debug)]
struct MonadEthGetTransactionByBlockHashAndIndexParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    block_hash: EthHash,
    #[serde(deserialize_with = "deserialize_quantity")]
    index: Quantity,
}

#[derive(Serialize, Debug)]
struct MonadEthGetTransactionByBlockHashAndIndexReturn {
    tx_object: Option<TransactionObject>,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getTransactionByBlockHashAndIndex(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getTransactionByBlockHashAndIndex: {params:?}");

    let p: MonadEthGetTransactionByBlockHashAndIndexParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let key = BlockTableKey(BlockHash::new(p.block_hash.0));
    let Some(value) = blockdb_env.get_block_by_hash(key).await else {
        return serialize_result(MonadEthGetTransactionByBlockHashAndIndexReturn {
            tx_object: None,
        });
    };

    let Some(transaction) = value.block.body.get(p.index.0 as usize) else {
        return serialize_result(MonadEthGetTransactionByBlockHashAndIndexReturn {
            tx_object: None,
        });
    };

    let to = transaction.transaction.to().unwrap();
    let from = transaction.recover_signer().unwrap();
    let retval = MonadEthGetTransactionByBlockHashAndIndexReturn {
        tx_object: Some(TransactionObject {
            block_hash: B256::new(p.block_hash.0),
            block_number: value.block.number,
            transaction_index: p.index.0,
            to: to.into(),
            from: from.into(),
        }),
    };

    serialize_result(retval)
}

#[derive(Deserialize, Debug)]
struct MonadEthGetTransactionByBlockNumberAndIndexParams {
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_tag: BlockTags,
    #[serde(deserialize_with = "deserialize_quantity")]
    index: Quantity,
}

#[derive(Serialize, Debug)]
struct MonadEthGetTransactionByBlockNumberAndIndexReturn {
    tx_object: Option<TransactionObject>,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getTransactionByBlockNumberAndIndex(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getTransactionByBlockNumberAndIndex: {params:?}");

    let p: MonadEthGetTransactionByBlockNumberAndIndexParams = match serde_json::from_value(params)
    {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let Some(value) = blockdb_env.get_block_by_tag(p.block_tag).await else {
        return serialize_result(MonadEthGetTransactionByBlockNumberAndIndexReturn {
            tx_object: None,
        });
    };

    let Some(transaction) = value.block.body.get(p.index.0 as usize) else {
        return serialize_result(MonadEthGetTransactionByBlockNumberAndIndexReturn {
            tx_object: None,
        });
    };

    let to = transaction.transaction.to().unwrap();
    let from = transaction.recover_signer().unwrap();
    let retval = MonadEthGetTransactionByBlockNumberAndIndexReturn {
        tx_object: Some(TransactionObject {
            block_hash: B256::new(*value.block.header.hash_slow()),
            block_number: value.block.number,
            transaction_index: p.index.0,
            to: to.into(),
            from: from.into(),
        }),
    };

    serialize_result(retval)
}

#[derive(Deserialize, Debug)]
struct MonadEthGetBlockTransactionCountByHashParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    block_hash: EthHash,
}

#[derive(Serialize, Debug)]
struct MonadEthGetBlockTransactionCountByHashReturn {
    count: u64,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getBlockTransactionCountByHash(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getBlockTransactionCountByHash: {params:?}");

    let p: MonadEthGetBlockTransactionCountByHashParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let key = BlockTableKey(BlockHash::new(p.block_hash.0));
    let Some(value) = blockdb_env.get_block_by_hash(key).await else {
        return serialize_result(MonadEthGetBlockTransactionCountByHashReturn { count: 0 });
    };

    let count = value.block.body.len() as u64;
    serialize_result(MonadEthGetBlockTransactionCountByHashReturn { count })
}

#[derive(Deserialize, Debug)]
struct MonadEthGetBlockTransactionCountByNumberParams {
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_tag: BlockTags,
}

#[derive(Serialize, Debug)]
struct MonadEthGetBlockTransactionCountByNumberReturn {
    count: u64,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getBlockTransactionCountByNumber(
    blockdb_env: &BlockDbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getBlockTransactionCountByNumber: {params:?}");

    let p: MonadEthGetBlockTransactionCountByNumberParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let Some(value) = blockdb_env.get_block_by_tag(p.block_tag).await else {
        return serialize_result(MonadEthGetBlockTransactionCountByNumberReturn { count: 0 });
    };

    let count = value.block.body.len() as u64;
    serialize_result(MonadEthGetBlockTransactionCountByNumberReturn { count })
}
