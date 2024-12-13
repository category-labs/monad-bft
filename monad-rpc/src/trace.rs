use monad_rpc_docs::rpc;
use serde::Deserialize;

use crate::{
    eth_json_types::{BlockTags, EthAddress, EthHash, MonadU256, Quantity},
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct TracerObject {
    tracer: Tracer,
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub enum Tracer {
    #[serde(rename = "callTracer")]
    CallTracer,
    #[serde(rename = "prestateTracer")]
    PreStateTracer,
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct TraceCallObject {
    pub from: Option<EthAddress>,
    pub to: EthAddress,
    pub gas: Option<MonadU256>,
    pub gas_price: Option<MonadU256>,
    pub value: Option<MonadU256>,
    pub data: Option<String>,
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct TraceCallParams {
    pub calls: Vec<TraceCallObject>,
    pub block: BlockTags,
}

#[rpc(method = "trace_call")]
#[allow(non_snake_case)]
/// Executes a new message call and returns a number of possible traces.
pub async fn monad_trace_call(params: TraceCallParams) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[rpc(method = "trace_callMany")]
#[allow(non_snake_case)]
/// Executes multiple message calls within the same block and returns a number of possible traces.
pub async fn monad_trace_callMany() -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}

#[derive(Deserialize, Debug, schemars::JsonSchema)]
pub struct TraceGetParams {
    transaction: EthHash,
    indexes: Vec<Quantity>,
}

#[rpc(method = "trace_get")]
#[allow(non_snake_case)]
/// Returns trace at given position.
pub async fn monad_trace_get(params: TraceGetParams) -> JsonRpcResult<String> {
    Err(JsonRpcError::method_not_supported())
}
