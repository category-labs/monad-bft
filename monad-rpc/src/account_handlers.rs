use alloy_primitives::aliases::U256;
use monad_rpc_docs::rpc;
use reth_primitives::B256;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::trace;

use crate::{
    eth_json_types::{serialize_result, BlockTags, EthAddress, EthHash, MonadU256},
    hex,
    jsonrpc::{JsonRpcError, JsonRpcResult},
    triedb::{Triedb, TriedbResult},
};

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBalanceParams {
    account: EthAddress,
    block_number: BlockTags,
}

#[rpc(method = "eth_getBalance")]
#[allow(non_snake_case)]
/// Returns the balance of the account of given address.
pub async fn monad_eth_getBalance<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetBalanceParams,
) -> JsonRpcResult<MonadU256> {
    trace!("monad_eth_getBalance: {params:?}");

    match triedb_env
        .get_account(params.account.0, params.block_number)
        .await
    {
        TriedbResult::Null => Ok(MonadU256(U256::ZERO)),
        TriedbResult::Account(_, balance, _) => Ok(MonadU256(U256::from(balance))),
        _ => Err(JsonRpcError::internal_error("error reading from db".into())),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetCodeParams {
    account: EthAddress,
    block_number: BlockTags,
}

#[rpc(method = "eth_getCode")]
#[allow(non_snake_case)]
/// Returns code at a given address.
pub async fn monad_eth_getCode<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetCodeParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getCode: {params:?}");

    let code_hash = match triedb_env
        .get_account(params.account.0, params.block_number.clone())
        .await
    {
        TriedbResult::Null => return Ok("0x".to_string()),
        TriedbResult::Account(_, _, code_hash) => code_hash,
        _ => return Err(JsonRpcError::internal_error("error reading from db".into())),
    };

    match triedb_env.get_code(code_hash, params.block_number).await {
        TriedbResult::Null => Ok("0x".to_string()),
        TriedbResult::Code(code) => Ok(hex::encode(&code)),
        _ => Err(JsonRpcError::internal_error("error reading from db".into())),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetStorageAtParams {
    account: EthAddress,
    position: MonadU256,
    block_number: BlockTags,
}

#[rpc(method = "eth_getStorageAt")]
#[allow(non_snake_case)]
/// Returns the value from a storage position at a given address.
pub async fn monad_eth_getStorageAt<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetStorageAtParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getStorageAt: {params:?}");

    match triedb_env
        .get_storage_at(
            params.account.0,
            B256::from(params.position.0).0,
            params.block_number,
        )
        .await
    {
        TriedbResult::Null => {
            Ok("0x0000000000000000000000000000000000000000000000000000000000000000".to_string())
        }
        TriedbResult::Storage(storage) => Ok(hex::encode(&storage)),
        _ => Err(JsonRpcError::internal_error("error reading from db".into())),
    }
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionCountParams {
    account: EthAddress,
    block_number: BlockTags,
}

#[rpc(method = "eth_getTransactionCount")]
#[allow(non_snake_case)]
/// Returns the number of transactions sent from an address.
pub async fn monad_eth_getTransactionCount<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetTransactionCountParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getTransactionCount: {params:?}");

    match triedb_env
        .get_account(params.account.0, params.block_number)
        .await
    {
        TriedbResult::Null => Ok(format!("0x{:x}", 0)),
        TriedbResult::Account(nonce, _, _) => Ok(format!("0x{:x}", nonce)),
        _ => Err(JsonRpcError::internal_error("error reading from db".into())),
    }
}

#[allow(non_snake_case)]
/// Returns an object with data about the sync status or false.
pub async fn monad_eth_syncing() -> Result<Value, JsonRpcError> {
    trace!("monad_eth_syncing");

    // TODO. TBD where this data actually comes from

    serialize_result(serde_json::Value::Bool(false))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetProofParams {
    account: EthAddress,
    keys: Vec<EthHash>,
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
pub async fn monad_eth_getProof<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetProofParams,
) -> JsonRpcResult<MonadEthGetProofResult> {
    trace!("monad_eth_getProof");
    Err(JsonRpcError::method_not_supported())
}
