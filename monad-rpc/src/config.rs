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

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct MonadRpcConfig {
    pub node_name: String,
    pub chain_id: u64,

    pub ipc_path: Option<PathBuf>,
    pub triedb_path: Option<PathBuf>,
    pub rpc_addr: String,
    pub rpc_port: u16,
    pub worker_threads: usize,
    pub ws_enabled: bool,
    pub ws_port: u16,
    pub ws_worker_threads: usize,
    pub ws_conn_limit: usize,
    pub ws_sub_per_conn_limit: u16,
    pub batch_request_limit: u16,
    pub max_request_size: usize,
    pub max_response_size: u32,
    pub otel_endpoint: Option<String>,
    pub rpc_comparison_endpoint: Option<String>,
    pub allow_unprotected_txs: bool,
    pub eth_get_logs_max_block_range: u64,
    pub eth_call_max_concurrent_requests: u32,
    pub eth_call_executor_threads: u32,
    pub eth_call_executor_fibers: u32,
    pub eth_call_high_max_concurrent_requests: u32,
    pub eth_call_high_executor_threads: u32,
    pub eth_call_high_executor_fibers: u32,
    pub eth_trace_block_max_concurrent_requests: u32,
    pub eth_trace_block_executor_threads: u32,
    pub eth_trace_block_executor_fibers: u32,
    pub eth_trace_tx_executor_fibers: u32,
    pub eth_call_executor_node_lru_max_mem: u64,
    pub eth_call_provider_gas_limit: u64,
    pub eth_estimate_gas_provider_gas_limit: u64,
    pub eth_send_raw_transaction_sync_default_timeout_ms: u64,
    pub eth_send_raw_transaction_sync_max_timeout_ms: u64,
    pub enable_admin_eth_call_statistics: bool,
    pub eth_trace_block_executor_queuing_timeout: u32,
    pub eth_call_high_executor_queuing_timeout: u32,
    pub eth_call_executor_queuing_timeout: u32,
    pub triedb_node_lru_max_mem: u64,
    pub triedb_max_buffered_read_requests: u32,
    pub triedb_max_async_read_concurrency: u32,
    pub triedb_max_buffered_traverse_requests: u32,
    pub triedb_max_async_traverse_concurrency: u32,
    pub compute_threadpool_size: usize,
    pub max_finalized_block_cache_len: u64,
    pub max_voted_block_cache_len: u64,
    pub s3_bucket: Option<String>,
    pub region: Option<String>,
    pub archive_url: Option<String>,
    pub archive_api_key: Option<String>,
    pub mongo_url: Option<String>,
    pub mongo_db_name: Option<String>,
    pub mongo_max_time_get_millis: Option<u64>,
    pub mongo_failure_threshold: Option<u32>,
    pub mongo_failure_timeout_millis: Option<u64>,
    pub use_eth_get_logs_index: bool,
    pub dry_run_get_logs_index: bool,
    pub pprof: Option<String>,
    pub exec_event_path: Option<PathBuf>,
    pub manytrace_socket: Option<String>,
}
