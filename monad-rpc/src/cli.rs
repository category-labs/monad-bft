use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None)]
pub struct Cli {
    /// Set the mempool ipc path
    #[arg(long)]
    pub ipc_path: PathBuf,

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
    #[arg(long)]
    pub chain_id: u64,

    /// Set the max number of requests in a batch request
    #[arg(long, default_value_t = 5000)]
    pub batch_request_limit: u16,

    /// Set the max request size in bytes
    #[arg(long, default_value_t = 10_000)]
    pub max_request_size: usize,

    /// Set the max response size in bytes
    #[arg(long, default_value_t = 25_000_000)]
    pub max_response_size: u32,

    /// Otel endpoint to collect metrics data
    #[arg(long)]
    pub otel_endpoint: Option<String>,

    /// Service name to set for metrics
    #[arg(long)]
    pub metrics_service_name: Option<String>,

    /// Allow pre EIP-155 transactions
    #[arg(long, default_value_t = false)]
    pub allow_unprotected_txs: bool,

    /// Set the max concurrent requests for eth_call and eth_estimateGas
    #[arg(long, default_value_t = 1000)]
    pub eth_call_max_concurrent_requests: u32,

    /// Set the max concurrent requests for triedb reads
    #[arg(long, default_value_t = 20_000)]
    pub triedb_max_buffered_read_requests: u32,

    /// Set the max number of concurrently executing async triedb read requests before we
    /// start exerting backpressure
    #[arg(long, default_value_t = 10_000)]
    pub triedb_max_async_read_concurrency: u32,

    /// Set the max concurrent requests for triedb traversals
    #[arg(long, default_value_t = 40)]
    pub triedb_max_buffered_traverse_requests: u32,

    /// Set the max number of concurrently executing async triedb traverse requests before we
    /// start exerting backpressure
    #[arg(long, default_value_t = 20)]
    pub triedb_max_async_traverse_concurrency: u32,

    #[arg(long, default_value_t = 1)]
    pub compute_threadpool_size: usize,

    /* Archive Options */
    /// Set the s3 bucket name to read archive data from
    #[arg(long)]
    pub s3_bucket: Option<String>,

    /// Set the s3 region to read archive data from
    #[arg(long)]
    pub region: Option<String>,

    /// Set the archive URL to read archive data from
    #[arg(long)]
    pub archive_url: Option<String>,

    /// Set the API key to read archive data from
    #[arg(long)]
    pub archive_api_key: Option<String>,

    #[arg(long)]
    pub mongo_url: Option<String>,

    #[arg(long)]
    pub mongo_db_name: Option<String>,
}
