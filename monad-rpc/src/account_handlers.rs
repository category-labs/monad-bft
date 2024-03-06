use alloy_primitives::aliases::B160;
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    eth_json_types::{
        deserialize_block_tags, deserialize_fixed_data, serialize_result, BlockTags, EthAddress,
    },
    jsonrpc::JsonRpcError,
    triedb::{TriedbEnv, TriedbResult},
};

#[derive(Deserialize, Debug)]
struct MonadEthGetBalanceParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    account: EthAddress,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block: BlockTags,
}

#[derive(Serialize, Debug)]
struct MonadEthGetBalanceReturn {
    balance: String,
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
        TriedbResult::Null => Ok(serde_json::Value::Null),
        TriedbResult::RlpParseError => Err(JsonRpcError::internal_error()),
        TriedbResult::Account(_, balance, _) => serialize_result(MonadEthGetBalanceReturn {
            balance: format!("0x{:x}", balance),
        }),
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
        TriedbResult::Null => Ok(serde_json::Value::Null),
        TriedbResult::RlpParseError => Err(JsonRpcError::internal_error()),
        TriedbResult::Account(nonce, _, _) => serialize_result(MonadEthGetBalanceReturn {
            balance: format!("0x{:x}", nonce),
        }),
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
