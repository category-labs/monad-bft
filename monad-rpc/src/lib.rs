pub mod chainstate;
pub mod docs;
pub mod eth_json_types;
pub mod fee;
pub mod gas_oracle;
pub mod handlers;
pub mod hex;
pub mod jsonrpc;
pub mod metrics;
pub mod timing;
pub mod trace;
pub mod txpool;
pub mod vpool;
pub mod websocket;

pub const WEB3_RPC_CLIENT_VERSION: &str = concat!("Monad/", env!("VERGEN_GIT_DESCRIBE"));
