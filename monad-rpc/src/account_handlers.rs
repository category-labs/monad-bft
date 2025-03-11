use std::sync::Arc;

use alloy_primitives::B256;
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::Triedb;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::trace;

use crate::{
    block_handlers::get_block_key_from_tag_or_hash,
    eth_json_types::{serialize_result, BlockTagOrHash, BlockTags, EthAddress, EthHash, MonadU256},
    jsonrpc::{JsonRpcError, JsonRpcResult},
    txpool::EthTxPoolBridgeState,
};

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetBalanceParams {
    account: EthAddress,
    block_number: BlockTagOrHash,
}

#[rpc(method = "eth_getBalance")]
#[allow(non_snake_case)]
/// Returns the balance of the account of given address.
pub async fn monad_eth_getBalance<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetBalanceParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getBalance: {params:?}");

    let block_key = get_block_key_from_tag_or_hash(triedb_env, params.block_number).await?;
    let account = triedb_env
        .get_account(block_key, params.account.0)
        .await
        .map_err(JsonRpcError::internal_error)?;

    Ok(format!("0x{:x}", account.balance))
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetCodeParams {
    account: EthAddress,
    block: BlockTagOrHash,
}

#[rpc(method = "eth_getCode")]
#[allow(non_snake_case)]
/// Returns code at a given address.
pub async fn monad_eth_getCode<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetCodeParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getCode: {params:?}");

    let block_key = get_block_key_from_tag_or_hash(triedb_env, params.block).await?;
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
    block: BlockTagOrHash,
}

#[rpc(method = "eth_getStorageAt")]
#[allow(non_snake_case)]
/// Returns the value from a storage position at a given address.
pub async fn monad_eth_getStorageAt<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetStorageAtParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getStorageAt: {params:?}");

    let block_key = get_block_key_from_tag_or_hash(triedb_env, params.block).await?;
    triedb_env
        .get_storage_at(block_key, params.account.0, B256::from(params.position.0).0)
        .await
        .map_err(JsonRpcError::internal_error)
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct MonadEthGetTransactionCountParams {
    account: EthAddress,
    block: BlockTagOrHash,
}

#[rpc(method = "eth_getTransactionCount")]
#[allow(non_snake_case)]
/// Returns the number of transactions sent from an address.
pub async fn monad_eth_getTransactionCount<T: Triedb>(
    triedb_env: &T,
    params: MonadEthGetTransactionCountParams,
) -> JsonRpcResult<String> {
    trace!("monad_eth_getTransactionCount: {params:?}");

    let block_key = get_block_key_from_tag_or_hash(triedb_env, params.block).await?;
    let account = triedb_env
        .get_account(block_key, params.account.0)
        .await
        .map_err(JsonRpcError::internal_error)?;

    Ok(format!("0x{:x}", account.nonce))
}

#[allow(non_snake_case)]
/// Returns an object with data about the sync status or false.
pub async fn monad_eth_syncing(
    mempool_state: &Arc<EthTxPoolBridgeState>,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_syncing");

    let is_connected = mempool_state.is_connected().await;

    serialize_result(serde_json::Value::Bool(!is_connected))
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

#[cfg(test)]
mod tests {
    use super::MonadEthGetStorageAtParams;
    use crate::eth_json_types::{BlockTags, Quantity};

    #[test]
    fn params_without_eip_1898() {
        let res: MonadEthGetStorageAtParams = serde_json::from_str(
            r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", "latest"]"#,
        )
        .unwrap();
        assert!(matches!(
            res.block,
            super::BlockTagOrHash::BlockTags(BlockTags::Latest)
        ));
        let res: MonadEthGetStorageAtParams =
            serde_json::from_str(r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", "0x1"]"#)
                .unwrap();
        assert!(matches!(
            res.block,
            super::BlockTagOrHash::BlockTags(BlockTags::Number(Quantity(1)))
        ));
    }

    #[test]
    fn eip_1898_blockhash() {
        let res: MonadEthGetStorageAtParams = serde_json::from_str(r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", {"blockHash": "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"}]"#).unwrap();
        assert!(matches!(res.block, super::BlockTagOrHash::Hash(_)));
        let res: MonadEthGetStorageAtParams = serde_json::from_str(r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", {"blockHash": "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3", "requireCanonical": false}]"#).unwrap();
        assert!(matches!(res.block, super::BlockTagOrHash::Hash(_)));
        let res: MonadEthGetStorageAtParams = serde_json::from_str(r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", {"blockHash": "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3", "requireCanonical": true}]"#).unwrap();
        assert!(matches!(res.block, super::BlockTagOrHash::Hash(_)));
    }

    #[test]
    fn eip_1898_blocknumber() {
        let res: MonadEthGetStorageAtParams = serde_json::from_str(
            r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", {"blockNumber": "0x0"}]"#,
        )
        .unwrap();
        assert!(matches!(
            res.block,
            super::BlockTagOrHash::BlockTags(BlockTags::Number(Quantity(0)))
        ));
        let res: MonadEthGetStorageAtParams = serde_json::from_str(
            r#"["0xDeaDbeefdEAdbeefdEadbEEFdeadbeEFdEaDbeeF", "0x0", {"blockNumber": "latest"}]"#,
        )
        .unwrap();
        assert!(matches!(
            res.block,
            super::BlockTagOrHash::BlockTags(BlockTags::Latest)
        ));
    }
}
