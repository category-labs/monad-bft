use alloy_primitives::aliases::U256;
use monad_rpc_docs::rpc;
use reth_primitives::B256;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::trace;

use crate::{
    eth_json_types::{
        deserialize_block_tags, deserialize_fixed_data, deserialize_u256, serialize_result,
        BlockTags, EthAddress, EthHash, MonadU256,
    },
    hex,
    jsonrpc::{JsonRpcError, JsonRpcResult},
    triedb::{TriedbEnv, TriedbResult},
};

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBalanceParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    account: EthAddress,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_number: BlockTags,
}

#[rpc(method = "eth_getBalance")]
#[allow(non_snake_case)]
/// Returns the balance of the account of given address.
pub async fn monad_eth_getBalance(
    triedb_env: &TriedbEnv,
    params: MonadEthGetBalanceParams,
) -> JsonRpcResult<MonadU256> {
    trace!("monad_eth_getBalance: {params:?}");

    match triedb_env
        .get_account(params.account.0, params.block_number.into())
        .await
    {
        TriedbResult::Null => Ok(MonadU256(U256::ZERO)),
        TriedbResult::Account(_, balance, _) => Ok(MonadU256(U256::from(balance))),
        _ => Err(JsonRpcError::internal_error()),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetCodeParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    account: EthAddress,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_number: BlockTags,
}

#[rpc(method = "eth_getCode")]
#[allow(non_snake_case)]
/// Returns code at a given address.
pub async fn monad_eth_getCode(
    triedb_env: &TriedbEnv,
    params: MonadEthGetCodeParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getCode: {params:?}");

    let code_hash = match triedb_env
        .get_account(params.account.0, params.block_number.into())
        .await
    {
        TriedbResult::Null => return Ok(format!("0x")),
        TriedbResult::Account(_, _, code_hash) => code_hash,
        _ => return Err(JsonRpcError::internal_error()),
    };

    match triedb_env
        .get_code(code_hash, params.block_number.into())
        .await
    {
        TriedbResult::Null => Ok(format!("0x")),
        TriedbResult::Code(code) => Ok(hex::encode(&code)),
        _ => Err(JsonRpcError::internal_error()),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetStorageAtParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    account: EthAddress,
    #[serde(deserialize_with = "deserialize_u256")]
    position: MonadU256,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_number: BlockTags,
}

#[rpc(method = "eth_getStorageAt")]
#[allow(non_snake_case)]
/// Returns the value from a storage position at a given address.
pub async fn monad_eth_getStorageAt(
    triedb_env: &TriedbEnv,
    params: MonadEthGetStorageAtParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getStorageAt: {params:?}");

    match triedb_env
        .get_storage_at(
            params.account.0,
            B256::from(params.position.0).0,
            params.block_number.into(),
        )
        .await
    {
        TriedbResult::Null => Ok(format!("0x{:x}", 0)),
        TriedbResult::Storage(storage) => Ok(hex::encode(&storage)),
        _ => Err(JsonRpcError::internal_error()),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionCountParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    account: EthAddress,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_number: BlockTags,
}

#[rpc(method = "eth_getTransactionCount")]
#[allow(non_snake_case)]
/// Returns the number of transactions sent from an address.
pub async fn monad_eth_getTransactionCount(
    triedb_env: &TriedbEnv,
    params: MonadEthGetTransactionCountParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getTransactionCount: {params:?}");

    match triedb_env
        .get_account(params.account.0, params.block_number.into())
        .await
    {
        TriedbResult::Null => Ok(format!("0x{:x}", 0)),
        TriedbResult::Account(nonce, _, _) => Ok(format!("0x{:x}", nonce)),
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

#[allow(non_snake_case)]
/// Returns a list of addresses owned by client.
pub async fn monad_eth_accounts(triedb_env: &TriedbEnv) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_accounts");

    serialize_result(serde_json::Value::Array(vec![]))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetProofParams {
    #[serde(deserialize_with = "deserialize_fixed_data")]
    account: EthAddress,
    keys: Vec<EthHash>,
    #[serde(deserialize_with = "deserialize_block_tags")]
    block_number: BlockTags,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct StorageProof {
    key: EthHash,
    value: EthHash,
    proof: Vec<EthHash>,
}

#[derive(Serialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetProofResult {
    balance: MonadU256,
    code_hash: EthHash,
    nonce: u128,
    storage_hash: EthHash,
    account_proof: Vec<EthHash>,
    storage_proof: Vec<StorageProof>,
}

#[rpc(method = "eth_getProof")]
#[allow(non_snake_case)]
/// Returns the account and storage values of the specified account including the Merkle-proof.
// TODO: this is a stub to support rpc docs, need to implement
pub async fn monad_eth_getProof(
    triedb_env: &TriedbEnv,
    params: MonadEthGetProofParams,
) -> JsonRpcResult<MonadEthGetProofResult> {
    trace!("monad_eth_getProof");
    Err(JsonRpcError::method_not_supported())
}
