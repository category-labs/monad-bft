use monad_rpc_docs::rpc;

use crate::jsonrpc::JsonRpcResult;

const WEB3_RPC_CLIENT_VERSION: &str = concat!("Monad/", env!("VERGEN_GIT_DESCRIBE"));

#[rpc(method = "net_version", ignore = "chain_id")]
pub fn monad_net_version(chain_id: u64) -> JsonRpcResult<String> {
    Ok(chain_id.to_string())
}

#[rpc(method = "web3_clientVersion")]
pub fn monad_web3_client_version() -> JsonRpcResult<String> {
    Ok(WEB3_RPC_CLIENT_VERSION.to_string())
}
