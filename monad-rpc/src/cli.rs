// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::path::PathBuf;

use clap::Parser;
use monad_archive::cli::BlockDataReaderArgs;

#[derive(Debug, Parser)]
#[command(name = "monad-rpc", about, long_about = None, version = monad_version::version!())]
pub struct Cli {
    /// Set the mempool ipc path
    /// If not set, the tx pool will be disabled.
    #[arg(long)]
    pub ipc_path: Option<PathBuf>,

    /// Set the monad triedb path
    #[arg(long)]
    pub triedb_path: Option<PathBuf>,

    /// Run only queryX RPC methods backed by chain-data
    #[arg(long, default_value_t = false)]
    pub queryx_only: bool,

    /// Set one monad chain-data fjall path for both meta and blob stores
    #[arg(long, conflicts_with_all = ["chain_data_meta_path", "chain_data_blob_path"])]
    pub chain_data_path: Option<PathBuf>,

    /// Set the monad chain-data fjall metadata path for queryX methods
    #[arg(long, requires = "chain_data_blob_path")]
    pub chain_data_meta_path: Option<PathBuf>,

    /// Set the monad chain-data fjall blob path for queryX methods
    #[arg(long, requires = "chain_data_meta_path")]
    pub chain_data_blob_path: Option<PathBuf>,

    /// Set the maximum primary-object limit accepted by queryX methods
    #[arg(long, default_value_t = 10_000)]
    pub queryx_max_limit: usize,

    /// Set the maximum resolved block range accepted by queryX methods
    #[arg(long, default_value_t = 100_000)]
    pub queryx_max_block_range: u64,

    /// Archive source to continuously ingest into chain-data inside the RPC
    /// process. Requires chain-data storage flags. Examples:
    /// `"fs /var/lib/monad-archive"`, `"aws my-bucket"`.
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    pub chain_data_ingest_block_data_source: Option<BlockDataReaderArgs>,

    /// Skip execution trace ingestion for embedded chain-data ingest.
    #[arg(long)]
    pub chain_data_ingest_no_traces: bool,

    /// Poll interval while embedded chain-data ingest waits for the next
    /// archive block.
    #[arg(long, default_value_t = 1000)]
    pub chain_data_ingest_live_poll_ms: u64,

    /// Maximum number of ready fetched blocks coalesced into one embedded
    /// chain-data ingest batch.
    #[arg(long, default_value_t = 1)]
    pub chain_data_ingest_max_batch: usize,

    /// Maximum number of block fetches in flight for embedded chain-data ingest.
    #[arg(long, default_value_t = 512)]
    pub chain_data_ingest_concurrency: usize,

    /// Number of ordered fetch futures to keep buffered ahead of embedded ingest.
    #[arg(long)]
    pub chain_data_ingest_fetch_buffer: Option<usize>,

    /// Enable adaptive fetch concurrency for embedded chain-data ingest.
    #[arg(long)]
    pub chain_data_ingest_autotune: bool,

    /// Lower bound on adaptive embedded ingest fetch concurrency.
    #[arg(long, default_value_t = 1)]
    pub chain_data_ingest_min_concurrency: usize,

    /// Upper bound on adaptive embedded ingest fetch concurrency.
    #[arg(long, default_value_t = 5000)]
    pub chain_data_ingest_max_concurrency: usize,

    /// fjall total-journal cap in MiB for chain-data stores.
    #[arg(long, default_value_t = 512)]
    pub chain_data_fjall_journal_mib: u64,

    /// Per-keyspace fjall memtable cap in MiB for chain-data stores.
    #[arg(long, default_value_t = 64)]
    pub chain_data_fjall_memtable_mib: u64,

    /// fjall flush/compaction worker thread count for chain-data stores.
    #[arg(long)]
    pub chain_data_fjall_workers: Option<usize>,

    /// Per-table chain-data read-cache budget in MiB.
    #[arg(long)]
    pub chain_data_cache_mib: Option<usize>,

    /// Set the address for RPC to bind to
    #[arg(long, default_value_t = String::from("0.0.0.0"))]
    pub rpc_addr: String,

    /// Set the port number for RPC to listen
    #[arg(long, default_value_t = 8080)]
    pub rpc_port: u16,

    /// Set the node config path
    #[arg(long)]
    pub node_config: Option<PathBuf>,

    /// Set the number of worker threads for the RPC server
    #[arg(long, default_value_t = 2)]
    pub worker_threads: usize,

    /// Enable the WebSocket server
    #[arg(long, default_value_t = false)]
    pub ws_enabled: bool,

    /// Set the port number for the WebSocket server
    #[arg(long, default_value_t = 8081)]
    pub ws_port: u16,

    /// Set the number of worker threads for the WebSocket server
    #[arg(long, default_value_t = 2)]
    pub ws_worker_threads: usize,

    /// Set the connection limit for WebSocket server
    #[arg(long, default_value_t = 500)]
    pub ws_conn_limit: usize,

    /// Set the subscription limit for each connection
    #[arg(long, default_value_t = 100)]
    pub ws_sub_per_conn_limit: u16,

    /// Set the max number of requests in a batch request
    #[arg(long, default_value_t = 5000)]
    pub batch_request_limit: u16,

    /// Set the max request size in bytes (default 2MB)
    #[arg(long, default_value_t = 2_000_000)]
    pub max_request_size: usize,

    /// Set the max response size in bytes
    #[arg(long, default_value_t = 25_000_000)]
    pub max_response_size: u32,

    /// Otel endpoint to collect metrics data
    #[arg(long)]
    pub otel_endpoint: Option<String>,

    /// HTTP endpoint to collect RPC comparison data
    #[arg(long)]
    pub rpc_comparison_endpoint: Option<String>,

    /// Allow pre EIP-155 transactions
    #[arg(long, default_value_t = false)]
    pub allow_unprotected_txs: bool,

    /// Set the max block range for eth_getLogs
    #[arg(long, default_value_t = 1000)]
    pub eth_get_logs_max_block_range: u64,

    /// Set the max concurrent requests for eth_call and eth_estimateGas
    #[arg(long, default_value_t = 1000)]
    pub eth_call_max_concurrent_requests: u32,

    /// Set the number of threads used for executing eth_call and eth_estimateGas
    #[arg(long, default_value_t = 2)]
    pub eth_call_executor_threads: u32,

    /// Set the number of fibers used for executing eth_call and eth_estimateGas
    #[arg(long, default_value_t = 64)]
    pub eth_call_executor_fibers: u32,

    /// Set the max concurrent requests for eth_call and eth_estimateGas with high gas cost
    #[arg(long, default_value_t = 20)]
    pub eth_call_high_max_concurrent_requests: u32,

    /// Set the number of threads used for executing eth_call and eth_estimateGas with high gas cost
    #[arg(long, default_value_t = 1)]
    pub eth_call_high_executor_threads: u32,

    /// Set the number of fibers used for executing eth_call and eth_estimateGas with high gas cost
    #[arg(long, default_value_t = 2)]
    pub eth_call_high_executor_fibers: u32,

    /// Set the max concurrent requests for block tracing methods (e.g. `debug_traceTransaction`, `debug_traceBlockByNumber`, etc.)
    #[arg(long, default_value_t = 20)]
    pub eth_trace_block_max_concurrent_requests: u32,

    /// Set the number of threads used for trace operations (shared by block and transaction execution)
    #[arg(long, default_value_t = 1)]
    pub eth_trace_block_executor_threads: u32,

    /// Set the number of fibers used for executing block tracing methods (e.g. `debug_traceTransaction`, `debug_traceBlockByNumber`, etc.)
    #[arg(long, default_value_t = 2)]
    pub eth_trace_block_executor_fibers: u32,

    /// Set the number of fibers used for executing transactions within trace blocks
    #[arg(long, default_value_t = 100)]
    pub eth_trace_tx_executor_fibers: u32,

    /// Set the memory limit of the node cache when executing eth_call and eth_estimateGas
    #[arg(long, default_value_t = 100 << 20)] // 100 MB
    pub eth_call_executor_node_lru_max_mem: u64,

    /// Set the gas limit for eth_call
    #[arg(long, default_value_t = 30_000_000)]
    pub eth_call_provider_gas_limit: u64,

    /// Set the gas limit for eth_estimateGas
    #[arg(long, default_value_t = 30_000_000)]
    pub eth_estimate_gas_provider_gas_limit: u64,

    /// Set the default timeout (in milliseconds) for eth_sendRawTransactionSync
    #[arg(long, default_value_t = 2_000)]
    pub eth_send_raw_transaction_sync_default_timeout_ms: u64,

    /// Set the maximum timeout (in milliseconds) for eth_sendRawTransactionSync
    #[arg(long, default_value_t = 10_000)]
    pub eth_send_raw_transaction_sync_max_timeout_ms: u64,

    /// Enable admin_ethCallStatistics method
    #[arg(long, default_value_t = false)]
    pub enable_admin_eth_call_statistics: bool,

    /// Set the maximum timeout (in seconds) for queuing when executing block tracing methods (e.g. `debug_traceTransaction`, `debug_traceBlockByNumber`, etc.)
    #[arg(long, default_value_t = 30)]
    pub eth_trace_block_executor_queuing_timeout: u32,

    /// Set the maximum timeout (in seconds) for queuing when executing eth_call with high gas cost
    #[arg(long, default_value_t = 30)]
    pub eth_call_high_executor_queuing_timeout: u32,

    /// Set the maximum timeout (in seconds) for queuing when executing eth_call and eth_estimateGas
    #[arg(long, default_value_t = 2)]
    pub eth_call_executor_queuing_timeout: u32,

    /// Set the memory limit of the node cache for RPC requests other than eth_call and eth_estimateGas
    #[arg(long, default_value_t = 100 << 20)] // 100MB
    pub triedb_node_lru_max_mem: u64,

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

    /// Set the RPC rayon threadpool size
    #[arg(long, default_value_t = 1)]
    pub compute_threadpool_size: usize,

    /// Set the maximum number of finalized blocks in cache
    #[arg(long, default_value_t = 200)]
    pub max_finalized_block_cache_len: u64,

    /// Set the maximum number of voted blocks in cache
    #[arg(long, default_value_t = 3)]
    pub max_voted_block_cache_len: u64,

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

    /// Set the mongo url to read archive data from
    #[arg(long)]
    pub mongo_url: Option<String>,

    /// Set the mongo db name to read archive data from
    #[arg(long)]
    pub mongo_db_name: Option<String>,

    /// Set the max time to get from mongo
    #[arg(long)]
    pub mongo_max_time_get_millis: Option<u64>,

    /// Set the mongo failure threshold
    #[arg(long)]
    pub mongo_failure_threshold: Option<u32>,

    /// Set the mongo failure timeout
    #[arg(long)]
    pub mongo_failure_timeout_millis: Option<u64>,

    /// Use mongo index to serve eth_getLogs
    #[arg(long)]
    pub use_eth_get_logs_index: bool,

    /// Dry run using mongo index for eth_getLogs
    #[arg(long)]
    pub dry_run_get_logs_index: bool,

    #[arg(
        long,
        help = "listen address for pprof server. pprof server won't be enabled if address is empty",
        default_value = ""
    )]
    pub pprof: String,

    /// Sets the socket path for the monad execution event server
    #[arg(long)]
    pub exec_event_path: Option<PathBuf>,

    #[arg(long)]
    pub manytrace_socket: Option<String>,
}
