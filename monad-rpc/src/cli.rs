use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None)]
pub struct Cli {
    /// Set the mempool ipc path
    #[arg(long)]
    pub ipc_path: PathBuf,

    /// Set the execution ledger path
    #[arg(long)]
    pub execution_ledger_path: PathBuf,

    /// Set the monad triedb path
    #[arg(long)]
    pub triedb_path: Option<PathBuf>,

    /// Set the address for RPC to bind to
    #[arg(long, default_value_t = String::from("0.0.0.0"))]
    pub rpc_addr: String,

    /// Set the port number for RPC to listen
    #[arg(long, default_value_t = 8080)]
    pub rpc_port: u16,

    /// Set the chain ID
    #[arg(long, default_value_t = 41454)]
    pub chain_id: u64,

    /// Set the max number of requests in a batch request
    #[arg(long, default_value_t = 5000)]
    pub batch_request_limit: u16,

    /// Set the max response size in bytes
    #[arg(long, default_value_t = 25_000_000)]
    pub max_response_size: u32,

    /// Otel endpoint to collect metrics data
    #[arg(long)]
    pub otel_endpoint: Option<String>,

    /// Allow pre EIP-155 transactions
    #[arg(long, default_value_t = false)]
    pub allow_unprotected_txs: bool,

    /// Set the max concurrent requests for eth_call and eth_estimateGas
    #[arg(long, default_value_t = 1000)]
    pub eth_call_max_concurrent_requests: u32,
}
