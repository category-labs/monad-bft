use std::collections::HashMap;

pub use inventory;
pub use monad_rpc_docs_derive::rpc;
pub use once_cell::sync::Lazy;
use serde::Serialize;

inventory::collect!(RpcMethodInfo);

pub static FUNCTION_MAP: Lazy<HashMap<String, &RpcMethodInfo>> = Lazy::new(|| {
    let mut map = HashMap::new();
    for info in inventory::iter::<RpcMethodInfo> {
        map.insert(info.name.to_string(), info);
    }
    map
});

pub trait RpcMethod {
    type Input;
    type Output;

    const NAME: &'static str;

    fn input_schema() -> Option<schemars::schema::RootSchema>;
    fn output_schema() -> Option<schemars::schema::RootSchema>;
}

#[derive(Clone, Copy)]
pub struct RpcMethodInfo {
    pub name: &'static str,
    pub docs: &'static str,
    pub input_schema: fn() -> Option<schemars::schema::RootSchema>,
    pub output_schema: fn() -> Option<schemars::schema::RootSchema>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenRpc {
    pub openrpc: String,
    pub info: Info,
    pub components: Components,
    pub methods: Vec<Method>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Info {
    pub version: String,
    pub title: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Method {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    pub params: Vec<Params>,
    pub result: MethodResult,
}

#[derive(Debug, Clone, Serialize)]
pub struct MethodResult {
    pub name: String,
    pub description: String,
    pub schema: schemars::schema::Schema,
}

#[derive(Debug, Clone, Serialize)]
pub struct Components {
    pub schemas: HashMap<String, schemars::schema::Schema>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Params {
    pub name: String,
    pub description: String,
    pub required: bool,
    pub schema: schemars::schema::Schema,
}
