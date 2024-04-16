use alloy_primitives::aliases::{B160, B256};
use alloy_rlp::Decodable;
use log::{debug, trace};
use monad_blockdb::EthTxKey;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    blockdb::BlockDbEnv,
    eth_json_types::{
        deserialize_block_tags, deserialize_fixed_data, serialize_result, BlockTags, EthAddress,
        EthHash, EthStorageKey,
    },
    hex,
    jsonrpc::JsonRpcError,
    triedb::{TriedbEnv, TriedbResult, YpTransactionReceipt},
};

#[derive(Deserialize, Debug)]
struct MonadEthGetBalanceParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    account: EthAddress,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block: BlockTags,
}

// FIXME: triedb related gets are supposed to use blocknumber in the key as well but its not
// supported yet and can only return for latest block -- add block num to key when there is support
#[allow(non_snake_case)]
pub async fn monad_eth_getBalance(
    triedb_env: &TriedbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getBalance: {params:?}");

    let p: MonadEthGetBalanceParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    match triedb_env.get_account(p.account).await {
        TriedbResult::Null => serialize_result(format!("0x{:x}", 0)),
        TriedbResult::Account(_, balance, _) => serialize_result(format!("0x{:x}", balance)),
        _ => Err(JsonRpcError::internal_error()),
    }
}

#[derive(Deserialize, Debug)]
struct MonadEthGetCodeParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    account: EthAddress,

    #[serde(deserialize_with = "deserialize_block_tags")]
    block: BlockTags,
}

// FIXME: triedb related gets are supposed to use blocknumber in the key as well but its not
// supported yet and can only return for latest block -- add block num to key when there is support
#[allow(non_snake_case)]
pub async fn monad_eth_getCode(
    triedb_env: &TriedbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getCode: {params:?}");

    let p: MonadEthGetCodeParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let code_hash = match triedb_env.get_account(p.account).await {
        TriedbResult::Null => return serialize_result(format!("0x{:x}", 0)),
        TriedbResult::Account(_, _, code_hash) => code_hash,
        _ => return Err(JsonRpcError::internal_error()),
    };

    match triedb_env.get_code(code_hash).await {
        TriedbResult::Null => serialize_result(format!("0x{:x}", 0)),
        TriedbResult::Code(code) => serialize_result(hex::encode(&code)),
        _ => Err(JsonRpcError::internal_error()),
    }
}

#[derive(Deserialize, Debug)]
struct MonadEthGetStorageAtParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    account: EthAddress,
    #[serde(deserialize_with = "deserialize_fixed_data")]
    position: EthStorageKey,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block: BlockTags,
}

// FIXME: triedb related gets are supposed to use blocknumber in the key as well but its not
// supported yet and can only return for latest block -- add block num to key when there is support
#[allow(non_snake_case)]
pub async fn monad_eth_getStorageAt(
    triedb_env: &TriedbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getStorageAt: {params:?}");

    let p: MonadEthGetStorageAtParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    match triedb_env.get_storage_at(p.account, p.position).await {
        TriedbResult::Null => serialize_result(format!("0x{:x}", 0)),
        TriedbResult::Storage(storage) => serialize_result(hex::encode(&storage)),
        _ => Err(JsonRpcError::internal_error()),
    }
}

#[derive(Deserialize, Debug)]
struct MonadEthGetTransactionCountParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    account: EthAddress,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block: BlockTags,
}

#[derive(Serialize, Debug)]
struct MonadEthGetTransactionCountReturn {
    count: String,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getTransactionCount(
    triedb_env: &TriedbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getTransactionCount: {params:?}");

    let p: MonadEthGetTransactionCountParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    match triedb_env.get_account(p.account).await {
        TriedbResult::Null => serialize_result(format!("0x{:x}", 0)),
        TriedbResult::Account(nonce, _, _) => serialize_result(format!("0x{:x}", nonce)),
        _ => Err(JsonRpcError::internal_error()),
    }
}

#[derive(Deserialize, Debug)]
struct MonadEthGetTransactionReceiptParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    tx_hash: EthHash,
}

#[derive(Serialize, Debug)]
struct MonadEthGetTransactionReceiptReturn {
    // TODO, the transaction object in the eth json spec has more fields than those
    // specified for the transaction receipt in the Yellow Paper
    tx_object: Option<YpTransactionReceipt>,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getTransactionReceipt(
    blockdb_env: &BlockDbEnv,
    triedb_env: &TriedbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_getTransactionReceipt: {params:?}");

    let p: MonadEthGetTransactionReceiptParams = match serde_json::from_value(params) {
        Ok(s) => s,
        Err(e) => {
            debug!("invalid params {e}");
            return Err(JsonRpcError::invalid_params());
        }
    };

    let key = EthTxKey(B256::new(p.tx_hash.0));
    let Some(txn_value) = blockdb_env.get_txn(key).await else {
        return serialize_result(MonadEthGetTransactionReceiptReturn { tx_object: None });
    };
    let txn_index = txn_value.transaction_index;

    let Some(block) = blockdb_env.get_block_by_hash(txn_value.block_hash).await else {
        return serialize_result(MonadEthGetTransactionReceiptReturn { tx_object: None });
    };
    let block_num = block.block.number;

    match triedb_env.get_receipt(txn_index, block_num).await {
        TriedbResult::Null => {
            serialize_result(MonadEthGetTransactionReceiptReturn { tx_object: None })
        }
        TriedbResult::Receipt(rlp_receipt) => {
            let mut rlp_buf = rlp_receipt.as_slice();
            match YpTransactionReceipt::decode(&mut rlp_buf) {
                Ok(r) => {
                    serialize_result(MonadEthGetTransactionReceiptReturn { tx_object: Some(r) })
                }
                Err(e) => {
                    debug!("rlp decode error: {e}");
                    Err(JsonRpcError::internal_error())
                }
            }
        }
        _ => Err(JsonRpcError::internal_error()),
    }
}

#[allow(non_snake_case)]
pub async fn monad_eth_syncing(triedb_env: &TriedbEnv) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_syncing");

    // TODO. TBD where this data actually comes from

    serialize_result(serde_json::Value::Bool(false))
}

#[derive(Serialize, Debug)]
struct MonadEthCoinbaseReturn {
    address: B160,
}

#[allow(non_snake_case)]
pub async fn monad_eth_coinbase(triedb_env: &TriedbEnv) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_coinbase");

    // TODO. TBD where this data actually comes from

    let retval = MonadEthCoinbaseReturn {
        address: B160::default(),
    };

    serialize_result(retval)
}

#[allow(non_snake_case)]
pub async fn monad_eth_accounts(triedb_env: &TriedbEnv) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_accounts");

    serialize_result(serde_json::Value::Array(vec![]))
}
