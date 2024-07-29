use monad_blockdb_utils::BlockDbEnv;
use monad_triedb_utils::TriedbEnv;
use serde_json::Value;
use tracing::trace;

use crate::jsonrpc::JsonRpcError;

#[allow(non_snake_case)]
pub async fn monad_eth_debugTraceTransaction(
    blockdb_env: &BlockDbEnv,
    triedb_env: &TriedbEnv,
    params: Value,
) -> Result<Value, JsonRpcError> {
    trace!("monad_eth_debugTraceTransaction: {params:?}");
    return Err(JsonRpcError::internal_error());
}
