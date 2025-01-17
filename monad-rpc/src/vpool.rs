use monad_rpc_docs::rpc;
use serde::{Deserialize, Serialize};

use crate::{
    eth_json_types::EthAddress,
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

#[rpc(method = "txpool_content")]
#[allow(non_snake_case)]
pub async fn monad_txpool_content() -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct TxPoolContentFromParams {
    pub address: EthAddress,
}

#[rpc(method = "txpool_contentFrom")]
#[allow(non_snake_case)]
pub async fn monad_txpool_contentFrom(params: TxPoolContentFromParams) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[rpc(method = "txpool_inspect")]
#[allow(non_snake_case)]
pub async fn monad_txpool_inspect() -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub struct TxPoolStatus {
    pub pending: usize,
    pub queued: usize,
}

#[rpc(method = "txpool_status")]
#[allow(non_snake_case)]
pub async fn monad_txpool_status() -> JsonRpcResult<TxPoolStatus> {
    Err(JsonRpcError::method_not_supported())
}
