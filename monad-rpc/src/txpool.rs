use monad_rpc_docs::rpc;

use crate::jsonrpc::{JsonRpcError, JsonRpcResult};

#[rpc(method = "txpool_content")]
#[allow(non_snake_case)]
pub async fn monad_txpool_content() -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[rpc(method = "txpool_contentFrom")]
#[allow(non_snake_case)]
pub async fn monad_txpool_contentFrom() -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[rpc(method = "txpool_inspect")]
#[allow(non_snake_case)]
pub async fn monad_txpool_inspect() -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[rpc(method = "txpool_status")]
#[allow(non_snake_case)]
pub async fn monad_txpool_status() -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}
