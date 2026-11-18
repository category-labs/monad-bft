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

#![recursion_limit = "256"]

use std::sync::Arc;

use actix_web::{web, App, HttpServer};
use clap::{error::ErrorKind, CommandFactory, Parser};
use monad_ethcall::ffi::PoolConfig;
use monad_rpc::{
    data::{
        eth_call_handler::{EthCallHandler, EthCallHandlerConfig},
        DataProvider,
    },
    handlers::resources::{MonadJsonRootSpanBuilder, MonadRpcResources},
    middleware::DecompressionGuard,
};
use monad_triedb_utils::triedb_env::TriedbEnv;
use tokio::sync::Semaphore;
use tracing::info;
use tracing_actix_web::TracingLogger;
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer as FmtLayer},
    layer::SubscriberExt,
    EnvFilter, Layer, Registry,
};

use self::{
    cli::{is_domain_of, Cli},
    dispatch::{rpc_handler, DomainRpc},
};

mod cli;
mod dispatch;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> std::io::Result<()> {
    let args = Cli::parse();

    let l1_chain_id = args.chain.chain_id();
    if !is_domain_of(args.domain_chain_id, l1_chain_id) {
        Cli::command()
            .error(
                ErrorKind::ValueValidation,
                format!(
                    "the low 16 bits of --domain-chain-id {:#x} must be the L1 chain id {:#x}, below a nonzero domain number",
                    args.domain_chain_id, l1_chain_id
                ),
            )
            .exit();
    }

    let subscriber = Registry::default().with(
        FmtLayer::default()
            .json()
            .with_span_events(FmtSpan::NONE)
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(std::io::stdout)
            .with_ansi(false)
            .with_filter(EnvFilter::from_default_env()),
    );
    tracing::subscriber::set_global_default(subscriber).map_err(std::io::Error::other)?;

    info!(
        domain_chain_id = args.domain_chain_id,
        l1_chain_id, "serving the triedb of a private domain"
    );

    let triedb_env = TriedbEnv::new(
        &args.triedb_path,
        args.triedb_node_lru_max_mem,
        args.triedb_max_buffered_read_requests,
        args.triedb_max_async_read_concurrency,
        args.triedb_max_buffered_traverse_requests,
        args.triedb_max_async_traverse_concurrency,
        args.max_finalized_block_cache_len,
        args.max_voted_block_cache_len,
    );

    // TODO: the executor runs calls under the L1's rules; it has no private domain support yet.
    let eth_call_handler = EthCallHandler::new(
        EthCallHandlerConfig {
            enable_stats: false,
            pool_low: PoolConfig {
                num_threads: args.eth_call_executor_threads,
                num_fibers: args.eth_call_executor_fibers,
                timeout_sec: args.eth_call_executor_queuing_timeout,
                queue_limit: args.eth_call_max_concurrent_requests,
            },
            pool_high: PoolConfig {
                num_threads: args.eth_call_high_executor_threads,
                num_fibers: args.eth_call_high_executor_fibers,
                timeout_sec: args.eth_call_high_executor_queuing_timeout,
                queue_limit: args.eth_call_high_max_concurrent_requests,
            },
            pool_block: PoolConfig {
                num_threads: args.eth_trace_block_executor_threads,
                num_fibers: args.eth_trace_block_executor_fibers,
                timeout_sec: args.eth_trace_block_executor_queuing_timeout,
                queue_limit: args.eth_trace_block_max_concurrent_requests,
            },
            tx_exec_num_fibers: args.eth_trace_tx_executor_fibers,
            node_cache_max_mem: args.eth_call_executor_node_lru_max_mem,
            max_concurrent_permits: args.eth_call_max_concurrent_requests as usize,
            provider_gas_limit_eth_call: args.eth_call_provider_gas_limit,
            provider_gas_limit_eth_estimate_gas: args.eth_estimate_gas_provider_gas_limit,
            provider_gas_limit_eth_simulate: 0,
            provider_max_calls_eth_simulate: 0,
            provider_max_blocks_eth_simulate: 0,
            execution_chain_id: l1_chain_id,
        },
        &args.triedb_path,
    );

    let rpc = web::Data::new(DomainRpc::new(MonadRpcResources {
        txpool_bridge_client: None,
        eth_call_handler: Some(eth_call_handler),
        chain_id: args.domain_chain_id,
        data_provider: Some(DataProvider::new(None, Arc::new(triedb_env), None)),
        event_server_client: None,
        batch_request_limit: args.batch_request_limit,
        batch_concurrent_limit: args.batch_concurrent_limit,
        max_response_size: args.max_response_size,
        allow_unprotected_txs: false,
        logs_max_block_range: args.eth_get_logs_max_block_range,
        eth_send_raw_transaction_sync_default_timeout_ms: 0,
        eth_send_raw_transaction_sync_max_timeout_ms: 0,
        dry_run_get_logs_index: false,
        use_eth_get_logs_index: false,
        max_finalized_block_cache_len: args.max_finalized_block_cache_len as u64,
        enable_eth_simulate_v1: false,
        metrics: None,
        rpc_comparator: None,
        feehistory_limiter: Arc::new(Semaphore::new(args.feehistory_max_concurrent_requests)),
    }));

    HttpServer::new(move || {
        App::new()
            .wrap(DecompressionGuard)
            .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
            .app_data(web::PayloadConfig::default().limit(args.max_request_size))
            .app_data(rpc.clone())
            .service(web::resource("/").route(web::post().to(rpc_handler)))
    })
    .bind((args.rpc_addr, args.rpc_port))?
    .shutdown_timeout(1)
    .workers(args.worker_threads)
    .run()
    .await
}
