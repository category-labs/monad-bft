use alloy_primitives::B256;
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::Triedb;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::trace;

use crate::{
    block_handlers::get_block_key_from_tag,
    eth_json_types::{serialize_result, BlockTags, EthAddress, EthHash, MonadU256},
    jsonrpc::{JsonRpcError, JsonRpcResult},
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
) -> JsonRpcResult<String> {
    trace!("monad_eth_getBalance: {params:?}");

    let block_key = get_block_key_from_tag(triedb_env, params.block_number);
    let account = triedb_env
        .get_account(block_key, params.account.0)
        .await
        .map_err(JsonRpcError::internal_error)?;

    Ok(format!("0x{:x}", account.balance))
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

    let block_key = get_block_key_from_tag(triedb_env, params.block_number.clone());
    let account = triedb_env
        .get_account(block_key, params.account.0)
        .await
        .map_err(JsonRpcError::internal_error)?;

    triedb_env
        .get_code(block_key, account.code_hash)
        .await
        .map_err(JsonRpcError::internal_error)
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

    let block_key = get_block_key_from_tag(triedb_env, params.block_number);
    triedb_env
        .get_storage_at(block_key, params.account.0, B256::from(params.position.0).0)
        .await
        .map_err(JsonRpcError::internal_error)
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

    let block_key = get_block_key_from_tag(triedb_env, params.block_number);
    let account = triedb_env
        .get_account(block_key, params.account.0)
        .await
        .map_err(JsonRpcError::internal_error)?;

    Ok(format!("0x{:x}", account.nonce))
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
