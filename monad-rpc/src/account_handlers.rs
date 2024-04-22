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
    block_number: BlockTags,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getBalance(
    blockdb_env: &BlockDbEnv,
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

    let Some(value) = blockdb_env.get_block_by_tag(p.block_number).await else {
        return serialize_result(format!("0x{:x}", 0));
    };

    match triedb_env.get_account(p.account, value.block.number).await {
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
    block_number: BlockTags,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getCode(
    blockdb_env: &BlockDbEnv,
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

    let Some(value) = blockdb_env.get_block_by_tag(p.block_number).await else {
        return serialize_result(format!("0x{:x}", 0));
    };

    let code_hash = match triedb_env.get_account(p.account, value.block.number).await {
        TriedbResult::Null => return serialize_result(format!("0x{:x}", 0)),
        TriedbResult::Account(_, _, code_hash) => code_hash,
        _ => return Err(JsonRpcError::internal_error()),
    };

    match triedb_env.get_code(code_hash, value.block.number).await {
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
    block_number: BlockTags,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getStorageAt(
    blockdb_env: &BlockDbEnv,
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

    let Some(value) = blockdb_env.get_block_by_tag(p.block_number).await else {
        return serialize_result(format!("0x{:x}", 0));
    };

    match triedb_env
        .get_storage_at(p.account, p.position, value.block.number)
        .await
    {
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
    block_number: BlockTags,
}

#[allow(non_snake_case)]
pub async fn monad_eth_getTransactionCount(
    blockdb_env: &BlockDbEnv,
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

    let Some(value) = blockdb_env.get_block_by_tag(p.block_number).await else {
        return serialize_result(format!("0x{:x}", 0));
    };

    match triedb_env.get_account(p.account, value.block.number).await {
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

// TODO, the transaction object in the eth json spec has more fields than those
// specified for the transaction receipt in the Yellow Paper
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
        return serialize_result(None::<YpTransactionReceipt>);
    };
    let txn_index = txn_value.transaction_index;

    let Some(block) = blockdb_env.get_block_by_hash(txn_value.block_hash).await else {
        return serialize_result(None::<YpTransactionReceipt>);
    };
    let block_num = block.block.number;

    match triedb_env.get_receipt(txn_index, block_num).await {
        TriedbResult::Null => {
            serialize_result(None::<YpTransactionReceipt>)
        }
        TriedbResult::Receipt(rlp_receipt) => {
            let mut rlp_buf = rlp_receipt.as_slice();
            match YpTransactionReceipt::decode(&mut rlp_buf) {
                Ok(r) => {
                    serialize_result(Some(r))
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
