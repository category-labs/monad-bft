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
use monad_rpc::config::MonadRpcConfig;

#[derive(Debug, Parser)]
#[command(name = "monad-rpc", about, long_about = None, version = monad_version::version!())]
pub struct Cli {
    /// Set the rpc config path
    pub rpc_config: PathBuf,

    /// Set the mempool ipc path
    /// If not set, the tx pool will be disabled.
    #[arg(long)]
    pub ipc_path: Option<PathBuf>,

    /// Set the monad triedb path
    #[arg(long)]
    pub triedb_path: Option<PathBuf>,

    /// Set the address for RPC to bind to
    #[arg(long)]
    pub rpc_addr: Option<String>,

    /// Set the port number for RPC to listen
    #[arg(long)]
    pub rpc_port: Option<u16>,

    /// Set the number of worker threads for the RPC server
    #[arg(long)]
    pub worker_threads: Option<usize>,

    /// Enable the WebSocket server
    #[arg(long)]
    pub ws_enabled: Option<bool>,

    /// Set the port number for the WebSocket server
    #[arg(long)]
    pub ws_port: Option<u16>,

    /// Set the number of worker threads for the WebSocket server
    #[arg(long)]
    pub ws_worker_threads: Option<usize>,

    /// Set the connection limit for WebSocket server
    #[arg(long)]
    pub ws_conn_limit: Option<usize>,

    /// Set the subscription limit for each connection
    #[arg(long)]
    pub ws_sub_per_conn_limit: Option<u16>,

    /// Set the max number of requests in a batch request
    #[arg(long)]
    pub batch_request_limit: Option<u16>,

    /// Set the max request size in bytes (default 2MB)
    #[arg(long)]
    pub max_request_size: Option<usize>,

    /// Set the max response size in bytes
    #[arg(long)]
    pub max_response_size: Option<u32>,

    /// Otel endpoint to collect metrics data
    #[arg(long)]
    pub otel_endpoint: Option<String>,

    /// HTTP endpoint to collect RPC comparison data
    #[arg(long)]
    pub rpc_comparison_endpoint: Option<String>,

    /// Allow pre EIP-155 transactions
    #[arg(long)]
    pub allow_unprotected_txs: Option<bool>,

    /// Set the max block range for eth_getLogs
    #[arg(long)]
    pub eth_get_logs_max_block_range: Option<u64>,

    /// Set the max concurrent requests for eth_call and eth_estimateGas
    #[arg(long)]
    pub eth_call_max_concurrent_requests: Option<u32>,

    /// Set the number of threads used for executing eth_call and eth_estimateGas
    #[arg(long)]
    pub eth_call_executor_threads: Option<u32>,

    /// Set the number of fibers used for executing eth_call and eth_estimateGas
    #[arg(long)]
    pub eth_call_executor_fibers: Option<u32>,

    /// Set the max concurrent requests for eth_call and eth_estimateGas with high gas cost
    #[arg(long)]
    pub eth_call_high_max_concurrent_requests: Option<u32>,

    /// Set the number of threads used for executing eth_call and eth_estimateGas with high gas cost
    #[arg(long)]
    pub eth_call_high_executor_threads: Option<u32>,

    /// Set the number of fibers used for executing eth_call and eth_estimateGas with high gas cost
    #[arg(long)]
    pub eth_call_high_executor_fibers: Option<u32>,

    /// Set the max concurrent requests for block tracing methods (e.g. `debug_traceTransaction`, `debug_traceBlockByNumber`, etc.)
    #[arg(long)]
    pub eth_trace_block_max_concurrent_requests: Option<u32>,

    /// Set the number of threads used for trace operations (shared by block and transaction execution)
    #[arg(long)]
    pub eth_trace_block_executor_threads: Option<u32>,

    /// Set the number of fibers used for executing block tracing methods (e.g. `debug_traceTransaction`, `debug_traceBlockByNumber`, etc.)
    #[arg(long)]
    pub eth_trace_block_executor_fibers: Option<u32>,

    /// Set the number of fibers used for executing transactions within trace blocks
    #[arg(long)]
    pub eth_trace_tx_executor_fibers: Option<u32>,

    /// Set the memory limit of the node cache when executing eth_call and eth_estimateGas
    #[arg(long)] // 100 MB
    pub eth_call_executor_node_lru_max_mem: Option<u64>,

    /// Set the gas limit for eth_call
    #[arg(long)]
    pub eth_call_provider_gas_limit: Option<u64>,

    /// Set the gas limit for eth_estimateGas
    #[arg(long)]
    pub eth_estimate_gas_provider_gas_limit: Option<u64>,

    /// Set the default timeout (in milliseconds) for eth_sendRawTransactionSync
    #[arg(long)]
    pub eth_send_raw_transaction_sync_default_timeout_ms: Option<u64>,

    /// Set the maximum timeout (in milliseconds) for eth_sendRawTransactionSync
    #[arg(long)]
    pub eth_send_raw_transaction_sync_max_timeout_ms: Option<u64>,

    /// Enable admin_ethCallStatistics method
    #[arg(long)]
    pub enable_admin_eth_call_statistics: Option<bool>,

    /// Set the maximum timeout (in seconds) for queuing when executing block tracing methods (e.g. `debug_traceTransaction`, `debug_traceBlockByNumber`, etc.)
    #[arg(long)]
    pub eth_trace_block_executor_queuing_timeout: Option<u32>,

    /// Set the maximum timeout (in seconds) for queuing when executing eth_call with high gas cost
    #[arg(long)]
    pub eth_call_high_executor_queuing_timeout: Option<u32>,

    /// Set the maximum timeout (in seconds) for queuing when executing eth_call and eth_estimateGas
    #[arg(long)]
    pub eth_call_executor_queuing_timeout: Option<u32>,

    /// Set the memory limit of the node cache for RPC requests other than eth_call and eth_estimateGas
    #[arg(long)] // 100MB
    pub triedb_node_lru_max_mem: Option<u64>,

    /// Set the max concurrent requests for triedb reads
    #[arg(long)]
    pub triedb_max_buffered_read_requests: Option<u32>,

    /// Set the max number of concurrently executing async triedb read requests before we
    /// start exerting backpressure
    #[arg(long)]
    pub triedb_max_async_read_concurrency: Option<u32>,

    /// Set the max concurrent requests for triedb traversals
    #[arg(long)]
    pub triedb_max_buffered_traverse_requests: Option<u32>,

    /// Set the max number of concurrently executing async triedb traverse requests before we
    /// start exerting backpressure
    #[arg(long)]
    pub triedb_max_async_traverse_concurrency: Option<u32>,

    /// Set the RPC rayon threadpool size
    #[arg(long)]
    pub compute_threadpool_size: Option<usize>,

    /// Set the maximum number of finalized blocks in cache
    #[arg(long)]
    pub max_finalized_block_cache_len: Option<u64>,

    /// Set the maximum number of voted blocks in cache
    #[arg(long)]
    pub max_voted_block_cache_len: Option<u64>,

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
    pub use_eth_get_logs_index: Option<bool>,

    /// Dry run using mongo index for eth_getLogs
    #[arg(long)]
    pub dry_run_get_logs_index: Option<bool>,

    #[arg(
        long,
        help = "listen address for pprof server. pprof server won't be enabled if address is empty"
    )]
    pub pprof: Option<String>,

    /// Sets the socket path for the monad execution event server
    #[arg(long)]
    pub exec_event_path: Option<PathBuf>,

    #[arg(long)]
    pub manytrace_socket: Option<String>,
}

impl Cli {
    pub fn apply_to_config(self, config: &mut MonadRpcConfig) {
        let Cli {
            rpc_config: _,
            ipc_path,
            triedb_path,
            rpc_addr,
            rpc_port,
            worker_threads,
            ws_enabled,
            ws_port,
            ws_worker_threads,
            ws_conn_limit,
            ws_sub_per_conn_limit,
            batch_request_limit,
            max_request_size,
            max_response_size,
            otel_endpoint,
            rpc_comparison_endpoint,
            allow_unprotected_txs,
            eth_get_logs_max_block_range,
            eth_call_max_concurrent_requests,
            eth_call_executor_threads,
            eth_call_executor_fibers,
            eth_call_high_max_concurrent_requests,
            eth_call_high_executor_threads,
            eth_call_high_executor_fibers,
            eth_trace_block_max_concurrent_requests,
            eth_trace_block_executor_threads,
            eth_trace_block_executor_fibers,
            eth_trace_tx_executor_fibers,
            eth_call_executor_node_lru_max_mem,
            eth_call_provider_gas_limit,
            eth_estimate_gas_provider_gas_limit,
            eth_send_raw_transaction_sync_default_timeout_ms,
            eth_send_raw_transaction_sync_max_timeout_ms,
            enable_admin_eth_call_statistics,
            eth_trace_block_executor_queuing_timeout,
            eth_call_high_executor_queuing_timeout,
            eth_call_executor_queuing_timeout,
            triedb_node_lru_max_mem,
            triedb_max_buffered_read_requests,
            triedb_max_async_read_concurrency,
            triedb_max_buffered_traverse_requests,
            triedb_max_async_traverse_concurrency,
            compute_threadpool_size,
            max_finalized_block_cache_len,
            max_voted_block_cache_len,
            s3_bucket,
            region,
            archive_url,
            archive_api_key,
            mongo_url,
            mongo_db_name,
            mongo_max_time_get_millis,
            mongo_failure_threshold,
            mongo_failure_timeout_millis,
            use_eth_get_logs_index,
            dry_run_get_logs_index,
            pprof,
            exec_event_path,
            manytrace_socket,
        } = self;

        config.ipc_path = ipc_path.or(config.ipc_path.take());
        config.triedb_path = triedb_path.or(config.triedb_path.take());
        if let Some(val) = rpc_addr {
            config.rpc_addr = val;
        }
        if let Some(val) = rpc_port {
            config.rpc_port = val;
        }
        if let Some(val) = worker_threads {
            config.worker_threads = val;
        }
        if let Some(val) = ws_enabled {
            config.ws_enabled = val;
        }
        if let Some(val) = ws_port {
            config.ws_port = val;
        }
        if let Some(val) = ws_worker_threads {
            config.ws_worker_threads = val;
        }
        if let Some(val) = ws_conn_limit {
            config.ws_conn_limit = val;
        }
        if let Some(val) = ws_sub_per_conn_limit {
            config.ws_sub_per_conn_limit = val;
        }
        if let Some(val) = batch_request_limit {
            config.batch_request_limit = val;
        }
        if let Some(val) = max_request_size {
            config.max_request_size = val;
        }
        if let Some(val) = max_response_size {
            config.max_response_size = val;
        }
        config.otel_endpoint = otel_endpoint.or(config.otel_endpoint.take());
        config.rpc_comparison_endpoint =
            rpc_comparison_endpoint.or(config.rpc_comparison_endpoint.take());
        if let Some(val) = allow_unprotected_txs {
            config.allow_unprotected_txs = val;
        }
        if let Some(val) = eth_get_logs_max_block_range {
            config.eth_get_logs_max_block_range = val;
        }
        if let Some(val) = eth_call_max_concurrent_requests {
            config.eth_call_max_concurrent_requests = val;
        }
        if let Some(val) = eth_call_executor_threads {
            config.eth_call_executor_threads = val;
        }
        if let Some(val) = eth_call_executor_fibers {
            config.eth_call_executor_fibers = val;
        }
        if let Some(val) = eth_call_high_max_concurrent_requests {
            config.eth_call_high_max_concurrent_requests = val;
        }
        if let Some(val) = eth_call_high_executor_threads {
            config.eth_call_high_executor_threads = val;
        }
        if let Some(val) = eth_call_high_executor_fibers {
            config.eth_call_high_executor_fibers = val;
        }
        if let Some(val) = eth_trace_block_max_concurrent_requests {
            config.eth_trace_block_max_concurrent_requests = val;
        }
        if let Some(val) = eth_trace_block_executor_threads {
            config.eth_trace_block_executor_threads = val;
        }
        if let Some(val) = eth_trace_block_executor_fibers {
            config.eth_trace_block_executor_fibers = val;
        }
        if let Some(val) = eth_trace_tx_executor_fibers {
            config.eth_trace_tx_executor_fibers = val;
        }
        if let Some(val) = eth_call_executor_node_lru_max_mem {
            config.eth_call_executor_node_lru_max_mem = val;
        }
        if let Some(val) = eth_call_provider_gas_limit {
            config.eth_call_provider_gas_limit = val;
        }
        if let Some(val) = eth_estimate_gas_provider_gas_limit {
            config.eth_estimate_gas_provider_gas_limit = val;
        }
        if let Some(val) = eth_send_raw_transaction_sync_default_timeout_ms {
            config.eth_send_raw_transaction_sync_default_timeout_ms = val;
        }
        if let Some(val) = eth_send_raw_transaction_sync_max_timeout_ms {
            config.eth_send_raw_transaction_sync_max_timeout_ms = val;
        }
        if let Some(val) = enable_admin_eth_call_statistics {
            config.enable_admin_eth_call_statistics = val;
        }
        if let Some(val) = eth_trace_block_executor_queuing_timeout {
            config.eth_trace_block_executor_queuing_timeout = val;
        }
        if let Some(val) = eth_call_high_executor_queuing_timeout {
            config.eth_call_high_executor_queuing_timeout = val;
        }
        if let Some(val) = eth_call_executor_queuing_timeout {
            config.eth_call_executor_queuing_timeout = val;
        }
        if let Some(val) = triedb_node_lru_max_mem {
            config.triedb_node_lru_max_mem = val;
        }
        if let Some(val) = triedb_max_buffered_read_requests {
            config.triedb_max_buffered_read_requests = val;
        }
        if let Some(val) = triedb_max_async_read_concurrency {
            config.triedb_max_async_read_concurrency = val;
        }
        if let Some(val) = triedb_max_buffered_traverse_requests {
            config.triedb_max_buffered_traverse_requests = val;
        }
        if let Some(val) = triedb_max_async_traverse_concurrency {
            config.triedb_max_async_traverse_concurrency = val;
        }
        if let Some(val) = compute_threadpool_size {
            config.compute_threadpool_size = val;
        }
        if let Some(val) = max_finalized_block_cache_len {
            config.max_finalized_block_cache_len = val;
        }
        if let Some(val) = max_voted_block_cache_len {
            config.max_voted_block_cache_len = val;
        }
        config.s3_bucket = s3_bucket.or(config.s3_bucket.take());
        config.region = region.or(config.region.take());
        config.archive_url = archive_url.or(config.archive_url.take());
        config.archive_api_key = archive_api_key.or(config.archive_api_key.take());
        config.mongo_url = mongo_url.or(config.mongo_url.take());
        config.mongo_db_name = mongo_db_name.or(config.mongo_db_name.take());
        config.mongo_max_time_get_millis =
            mongo_max_time_get_millis.or(config.mongo_max_time_get_millis.take());
        config.mongo_failure_threshold =
            mongo_failure_threshold.or(config.mongo_failure_threshold.take());
        config.mongo_failure_timeout_millis =
            mongo_failure_timeout_millis.or(config.mongo_failure_timeout_millis.take());
        if let Some(val) = use_eth_get_logs_index {
            config.use_eth_get_logs_index = val;
        }
        if let Some(val) = dry_run_get_logs_index {
            config.dry_run_get_logs_index = val;
        }
        config.pprof = pprof.or(config.pprof.take());
        config.exec_event_path = exec_event_path.or(config.exec_event_path.take());
        config.manytrace_socket = manytrace_socket.or(config.manytrace_socket.take());
    }
}
