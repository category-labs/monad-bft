use alloy_primitives::aliases::{B160, U256};
use monad_triedb_utils::{TriedbEnv, TriedbResult};
use reth_primitives::B256;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, trace};

use crate::{
    eth_json_types::{
        deserialize_block_tags, deserialize_fixed_data, serialize_result, BlockTags, EthAddress,
    },
    hex,
    jsonrpc::JsonRpcError,
};

#[derive(Deserialize, Debug)]
struct MonadEthGetBalanceParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    account: EthAddress,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_number: BlockTags,
}

#[allow(non_snake_case)]
/// Returns the balance of the account of given address.
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

    match triedb_env
        .get_account(p.account.0, p.block_number.into())
        .await
    {
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
/// Returns code at a given address.
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

    let code_hash = match triedb_env
        .get_account(p.account.0, p.block_number.clone().into())
        .await
    {
        TriedbResult::Null => return serialize_result(format!("0x")),
        TriedbResult::Account(_, _, code_hash) => code_hash,
        _ => return Err(JsonRpcError::internal_error()),
    };

    match triedb_env.get_code(code_hash, p.block_number.into()).await {
        TriedbResult::Null => serialize_result(format!("0x")),
        TriedbResult::Code(code) => serialize_result(hex::encode(&code)),
        _ => Err(JsonRpcError::internal_error()),
    }
}

#[derive(Deserialize, Debug)]
struct MonadEthGetStorageAtParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    account: EthAddress,
    position: U256,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_number: BlockTags,
}

#[allow(non_snake_case)]
/// Returns the value from a storage position at a given address.
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

    match triedb_env
        .get_storage_at(p.account.0, B256::from(p.position).0, p.block_number.into())
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
/// Returns the number of transactions sent from an address.
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

    match triedb_env
        .get_account(p.account.0, p.block_number.into())
        .await
    {
        TriedbResult::Null => serialize_result(format!("0x{:x}", 0)),
        TriedbResult::Account(nonce, _, _) => serialize_result(format!("0x{:x}", nonce)),
        _ => Err(JsonRpcError::internal_error()),
    }
}

#[allow(non_snake_case)]
/// Returns an object with data about the sync status or false.
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
/// Returns the client coinbase address.
pub async fn monad_eth_coinbase(triedb_env: &TriedbEnv) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_coinbase");

    // TODO. TBD where this data actually comes from

    let retval = MonadEthCoinbaseReturn {
        address: B160::default(),
    };

    serialize_result(retval)
}

#[allow(non_snake_case)]
/// Returns a list of addresses owned by client.
pub async fn monad_eth_accounts(triedb_env: &TriedbEnv) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_accounts");

    serialize_result(serde_json::Value::Array(vec![]))
}
