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

use std::{sync::Arc, time::Duration};

use actix_web::{web, App, HttpServer};
use agent::AgentBuilder;
use clap::Parser;
use monad_archive::archive_reader::{redact_mongo_url, ArchiveReader};
use monad_ethcall::EthCallExecutor;
use monad_event_ring::EventRing;
use monad_node_config::MonadNodeConfig;
use monad_pprof::start_pprof_server;
use monad_rpc::{
    chainstate::{buffer::ChainStateBuffer, ChainState},
    comparator::RpcComparator,
    event::EventServer,
    handlers::{
        resources::{MonadJsonRootSpanBuilder, MonadRpcResources},
        rpc_handler,
    },
    metrics,
    timing::TimingMiddleware,
    txpool::EthTxPoolBridge,
    websocket, MONAD_RPC_VERSION,
};
use monad_tracing_timing::TimingsLayer;
use monad_triedb_utils::triedb_env::TriedbEnv;
use opentelemetry::metrics::MeterProvider;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};
use tracing_actix_web::TracingLogger;
use tracing_manytrace::{ManytraceLayer, TracingExtension};
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer as FmtLayer},
    layer::SubscriberExt,
    EnvFilter, Layer, Registry,
};

use self::cli::Cli;

mod cli;

#[cfg(all(not(target_env = "msvc"), feature = "jemallocator"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "jemallocator")]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> std::io::Result<()> {
    let args = Cli::parse();

    let node_config: MonadNodeConfig = toml::from_str(&std::fs::read_to_string(&args.node_config)?)
        .expect("node toml parse error");

    let _agent = if let Some(socket_path) = &args.manytrace_socket {
        let extension = Arc::new(TracingExtension::new());
        let agent = AgentBuilder::new(socket_path.clone())
            .register_tracing(Box::new((*extension).clone()))
            .build()
            .expect("failed to build manytrace agent");

        let s = Registry::default()
            .with(ManytraceLayer::new(extension))
            .with(
                FmtLayer::default()
                    .json()
                    .with_span_events(FmtSpan::NONE)
                    .with_current_span(false)
                    .with_span_list(false)
                    .with_writer(std::io::stdout)
                    .with_ansi(false)
                    .with_filter(EnvFilter::from_default_env()),
            )
            .with(TimingsLayer::new());
        tracing::subscriber::set_global_default(s).expect("failed to set logger");
        Some(agent)
    } else {
        let s = Registry::default()
            .with(
                FmtLayer::default()
                    .json()
                    .with_span_events(FmtSpan::NONE)
                    .with_current_span(false)
                    .with_span_list(false)
                    .with_writer(std::io::stdout)
                    .with_ansi(false)
                    .with_filter(EnvFilter::from_default_env()),
            )
            .with(TimingsLayer::new());
        tracing::subscriber::set_global_default(s).expect("failed to set logger");
        None
    };

    if !args.pprof.is_empty() {
        tokio::spawn(async {
            let server = match start_pprof_server(args.pprof) {
                Ok(server) => server,
                Err(err) => {
                    error!("failed to start pprof server: {}", err);
                    return;
                }
            };
            if let Err(err) = server.await {
                error!("pprof server faiiled: {}", err);
            }
        });
    }

    // initialize concurrent requests limiter
    let concurrent_requests_limiter = Arc::new(Semaphore::new(
        args.eth_call_max_concurrent_requests as usize,
    ));

    MONAD_RPC_VERSION.map(|v| info!("starting monad-rpc with version {}", v));

    let (txpool_bridge_client, _txpool_bridge_handle) = if let Some(ipc_path) = args.ipc_path {
        // Wait for bft to be in a ready state before starting the RPC server.
        // Bft will bind to the ipc socket after state syncing.
        let mut print_message_timer = tokio::time::interval(Duration::from_secs(60));
        let mut retry_timer = tokio::time::interval(Duration::from_secs(1));
        let (txpool_bridge_client, _txpool_bridge_handle) = loop {
            tokio::select! {
                _ = print_message_timer.tick() => {
                    info!("Waiting for statesync to complete");
                }
                _= retry_timer.tick() => {
                    match EthTxPoolBridge::start(&ipc_path).await  {
                        Ok((client, handle)) => {
                            info!("Statesync complete, starting RPC server");
                            break (client, handle)
                        },
                        Err(e) => {
                            debug!("caught error: {e}, retrying");
                        },
                    }
                },
            }
        };
        (Some(txpool_bridge_client), Some(_txpool_bridge_handle))
    } else {
        warn!("--ipc-path is not set, tx pool will be disabled. This means that the node will not be able to send transactions.");
        (None, None)
    };

    let triedb_env = args.triedb_path.clone().as_deref().map(|path| {
        TriedbEnv::new(
            path,
            args.triedb_node_lru_max_mem,
            args.triedb_max_buffered_read_requests as usize,
            args.triedb_max_async_read_concurrency as usize,
            args.triedb_max_buffered_traverse_requests as usize,
            args.triedb_max_async_traverse_concurrency as usize,
            args.max_finalized_block_cache_len as usize,
            args.max_voted_block_cache_len as usize,
        )
    });

    // Used for compute heavy tasks
    rayon::ThreadPoolBuilder::new()
        .thread_name(|i| format!("monad-rpc-rn-{i}"))
        .num_threads(args.compute_threadpool_size)
        .build_global()
        .unwrap();

    // Initialize archive reader if specified. If not specified, RPC can only read the latest <history_length> blocks from chain tip
    info!("Initializing archive readers for historical data access");

    let aws_archive_reader = match (
        &args.s3_bucket,
        &args.region,
        &args.archive_url,
        &args.archive_api_key,
    ) {
        (Some(s3_bucket), Some(region), Some(archive_url), Some(archive_api_key)) => {
            info!(
                s3_bucket,
                region, archive_url, "Initializing AWS archive reader"
            );
            match ArchiveReader::init_aws_reader(
                s3_bucket.clone(),
                Some(region.clone()),
                archive_url,
                archive_api_key,
                5,
            )
            .await
            {
                Ok(reader) => {
                    info!("AWS archive reader initialized successfully");
                    Some(reader)
                }
                Err(e) => {
                    warn!(error = %e, "Unable to initialize AWS archive reader");
                    None
                }
            }
        }
        _ => {
            debug!("AWS archive reader configuration not provided, skipping initialization");
            None
        }
    };

    let archive_reader = match (&args.mongo_db_name, &args.mongo_url) {
        (Some(db_name), Some(url)) => {
            info!(
                "Initializing MongoDB archive reader  with connection: {}, database: {}",
                redact_mongo_url(url),
                db_name
            );
            match ArchiveReader::init_mongo_reader(
                url.clone(),
                db_name.clone(),
                monad_archive::prelude::Metrics::none(),
                args.mongo_max_time_get_millis.map(Duration::from_millis),
            )
            .await
            {
                Ok(mongo_reader) => {
                    let has_aws_fallback = aws_archive_reader.is_some();
                    info!(
                        has_aws_fallback,
                        "MongoDB archive reader initialized successfully"
                    );
                    Some(mongo_reader.with_fallback(
                        aws_archive_reader,
                        args.mongo_failure_threshold,
                        args.mongo_failure_timeout_millis.map(Duration::from_millis),
                    ))
                }
                Err(e) => {
                    warn!(error = %e, "Unable to initialize MongoDB archive reader");
                    if aws_archive_reader.is_some() {
                        info!("Falling back to AWS archive reader");
                    }
                    aws_archive_reader
                }
            }
        }
        _ => {
            if aws_archive_reader.is_some() {
                info!("MongoDB configuration not provided, using AWS archive reader only");
            } else {
                info!("No archive readers configured, historical data access will be limited");
            }
            aws_archive_reader
        }
    };

    let low_pool_config = monad_ethcall::PoolConfig {
        num_threads: args.eth_call_executor_threads,
        num_fibers: args.eth_call_executor_fibers,
        timeout_sec: args.eth_call_executor_queuing_timeout,
        queue_limit: args.eth_call_max_concurrent_requests,
    };
    let high_pool_config = monad_ethcall::PoolConfig {
        num_threads: args.eth_call_high_executor_threads,
        num_fibers: args.eth_call_high_executor_fibers,
        timeout_sec: args.eth_call_high_executor_queuing_timeout,
        queue_limit: args.eth_call_high_max_concurrent_requests,
    };
    let block_pool_config = monad_ethcall::PoolConfig {
        num_threads: args.eth_trace_block_executor_threads,
        num_fibers: args.eth_trace_block_executor_fibers,
        timeout_sec: args.eth_trace_block_executor_queuing_timeout,
        queue_limit: args.eth_trace_block_max_concurrent_requests,
    };
    let tx_exec_num_fibers = args.eth_trace_tx_executor_fibers;

    let eth_call_executor = args.triedb_path.clone().as_deref().map(|path| {
        Arc::new(EthCallExecutor::new(
            low_pool_config,
            high_pool_config,
            block_pool_config,
            tx_exec_num_fibers,
            args.eth_call_executor_node_lru_max_mem,
            path,
        ))
    });

    let meter_provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider> =
        args.otel_endpoint.as_ref().map(|endpoint| {
            let provider = metrics::build_otel_meter_provider(
                endpoint,
                node_config.node_name.clone(),
                std::time::Duration::from_secs(5),
            )
            .expect("failed to build otel meter");
            opentelemetry::global::set_meter_provider(provider.clone());
            provider
        });

    let with_metrics = meter_provider
        .as_ref()
        .map(|provider| metrics::Metrics::new(provider.clone().meter("opentelemetry")));

    // Configure event ring, websocket server and event cache.
    let (events_client, events_for_cache) = if let Some(exec_event_path) = args.exec_event_path {
        let event_ring =
            EventRing::new_from_path(exec_event_path).expect("Execution event ring is ready");

        let events_client = EventServer::start(event_ring);

        // Subscribe to the event server to populate the event cache.
        let events_for_cache = events_client
            .subscribe()
            .expect("Failed to subscribe to event server");

        (Some(events_client), Some(events_for_cache))
    } else {
        if args.ws_enabled {
            panic!("exec-event-path is not set but is required for websockets");
        }

        (None, None)
    };

    let event_buffer = if let Some(mut events_for_cache) = events_for_cache {
        let event_buffer = Arc::new(ChainStateBuffer::new(1024));

        let event_buffer2 = event_buffer.clone();
        tokio::spawn(async move {
            while let Ok(event) = events_for_cache.recv().await {
                event_buffer2.insert(event).await;
            }
        });

        Some(event_buffer)
    } else {
        None
    };

    let chain_state = triedb_env
        .clone()
        .map(|t| ChainState::new(event_buffer, t, archive_reader.clone()));

    let rpc_comparator: Option<RpcComparator> = args
        .rpc_comparison_endpoint
        .as_ref()
        .map(|endpoint| RpcComparator::new(endpoint.to_string(), node_config.node_name));

    let app_state = MonadRpcResources::new(
        txpool_bridge_client,
        triedb_env,
        eth_call_executor,
        args.eth_call_executor_fibers as usize,
        archive_reader,
        node_config.chain_id,
        chain_state,
        args.batch_request_limit,
        args.max_response_size,
        args.allow_unprotected_txs,
        concurrent_requests_limiter,
        args.eth_call_max_concurrent_requests as usize,
        args.eth_get_logs_max_block_range,
        args.eth_call_provider_gas_limit,
        args.eth_estimate_gas_provider_gas_limit,
        args.eth_send_raw_transaction_sync_default_timeout_ms,
        args.eth_send_raw_transaction_sync_max_timeout_ms,
        args.dry_run_get_logs_index,
        args.use_eth_get_logs_index,
        args.max_finalized_block_cache_len,
        args.enable_admin_eth_call_statistics,
        with_metrics.clone(),
        rpc_comparator.clone(),
    );

    // Configure the websocket server if enabled
    let ws_server_handle = if let Some(events_client) = events_client {
        let ws_app_data = app_state.clone();
        let conn_limit = websocket::handler::ConnectionLimit::new(args.ws_conn_limit);
        let sub_limit = websocket::handler::SubscriptionLimit(args.ws_sub_per_conn_limit);

        args.ws_enabled.then(|| {
            HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::new(conn_limit.clone()))
                    .app_data(web::Data::new(events_client.clone()))
                    .app_data(web::Data::new(ws_app_data.clone()))
                    .app_data(web::Data::new(sub_limit.clone()))
                    .service(
                        web::resource("/").route(web::get().to(websocket::handler::ws_handler)),
                    )
            })
            .bind((args.rpc_addr.clone(), args.ws_port))
            .expect("Failed to bind WebSocket server")
            .shutdown_timeout(1)
            .workers(args.ws_worker_threads)
        })
    } else {
        None
    };

    // Configure the rpc server with or without metrics
    let app = match with_metrics {
        Some(metrics) => HttpServer::new(move || {
            App::new()
                .wrap(metrics.clone())
                .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                .wrap(TimingMiddleware)
                .app_data(web::PayloadConfig::default().limit(args.max_request_size))
                .app_data(web::Data::new(app_state.clone()))
                .service(web::resource("/").route(web::post().to(rpc_handler)))
        })
        .bind((args.rpc_addr, args.rpc_port))?
        .shutdown_timeout(1)
        .workers(args.worker_threads)
        .run(),
        None => HttpServer::new(move || {
            App::new()
                .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                .wrap(TimingMiddleware)
                .app_data(web::PayloadConfig::default().limit(args.max_request_size))
                .app_data(web::Data::new(app_state.clone()))
                .service(web::resource("/").route(web::post().to(rpc_handler)))
        })
        .bind((args.rpc_addr, args.rpc_port))?
        .shutdown_timeout(1)
        .workers(args.worker_threads)
        .run(),
    };

    let ws_fut = ws_server_handle.map(|ws| ws.run());

    tokio::select! {
        result = app => {
            let () = result?;
        }

        result = async {
            if let Some(fut) = ws_fut {
                fut.await
            } else {
                futures::future::pending().await
            }
        } => {
            let () = result?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use actix_http::{Request, StatusCode};
    use actix_web::{
        body::{to_bytes, MessageBody},
        dev::{Service, ServiceResponse},
        test, Error,
    };
    use jsonrpc::Response;
    use monad_rpc::{
        handlers::eth::call::EthCallStatsTracker,
        jsonrpc::{self, JsonRpcError, RequestId, ResponseWrapper},
        txpool::EthTxPoolBridgeClient,
    };
    use serde_json::{json, Value};
    use test_case::test_case;

    use super::*;

    async fn init_server(
    ) -> impl Service<Request, Response = ServiceResponse<impl MessageBody>, Error = Error> {
        let app_state = MonadRpcResources {
            txpool_bridge_client: Some(EthTxPoolBridgeClient::for_testing()),
            triedb_reader: None,
            eth_call_executor: None,
            eth_call_executor_fibers: 64,
            eth_call_stats_tracker: Some(Arc::new(EthCallStatsTracker::default())),
            archive_reader: None,
            chain_id: 1337,
            chain_state: None,
            batch_request_limit: 5,
            max_response_size: 25_000_000,
            allow_unprotected_txs: false,
            rate_limiter: Arc::new(Semaphore::new(1000)),
            total_permits: 1000,
            logs_max_block_range: 1000,
            eth_call_provider_gas_limit: u64::MAX,
            eth_estimate_gas_provider_gas_limit: u64::MAX,
            eth_send_raw_transaction_sync_default_timeout_ms: 2_000,
            eth_send_raw_transaction_sync_max_timeout_ms: 10_000,
            dry_run_get_logs_index: false,
            use_eth_get_logs_index: false,
            max_finalized_block_cache_len: 200,
            enable_eth_call_statistics: true,
            metrics: None,
            rpc_comparator: None,
        };

        test::init_service(
            App::new()
                .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                .app_data(web::PayloadConfig::default().limit(2_000_000))
                .app_data(web::Data::new(app_state.clone()))
                .service(web::resource("/").route(web::post().to(rpc_handler))),
        )
        .await
    }

    async fn recover_response_body(resp: ServiceResponse<impl MessageBody>) -> serde_json::Value {
        let b = to_bytes(resp.into_body())
            .await
            .unwrap_or_else(|_| panic!("body to_bytes failed"));
        serde_json::from_slice(&b)
            .inspect_err(|_| {
                println!("failed to serialize {:?}", &b);
            })
            .unwrap()
    }

    #[actix_web::test]
    async fn test_rpc_request_size() {
        let app = init_server().await;

        // payload within limit
        let payload = json!(
            {
                "jsonrpc": "2.0",
                "method": "subtract",
                "params": vec![1; 950_000],
                "id": 1
            }
        );
        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();
        let resp = app.call(req).await.unwrap();
        let resp: jsonrpc::Response =
            serde_json::from_value(recover_response_body(resp).await).unwrap();
        match resp.error {
            Some(e) => assert_eq!(e.code, -32601),
            None => panic!("expected error in response"),
        }

        // payload too large
        let payload = json!(
            {
                "jsonrpc": "2.0",
                "method": "subtract",
                "params": vec![1; 1_000_000],
                "id": 1
            }
        );
        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();
        let resp = app.call(req).await.unwrap();
        assert_eq!(resp.response().status(), StatusCode::from_u16(413).unwrap());
    }

    #[actix_web::test]
    async fn test_rpc_method_not_found() {
        let app = init_server().await;

        let payload = json!(
            {
                "jsonrpc": "2.0",
                "method": "subtract",
                "params": [42, 43],
                "id": 1
            }
        );
        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp = app.call(req).await.unwrap();
        let resp: jsonrpc::Response =
            serde_json::from_value(recover_response_body(resp).await).unwrap();

        match resp.error {
            Some(e) => assert_eq!(e.code, -32601),
            None => panic!("expected error in response"),
        }
    }

    #[allow(non_snake_case)]
    #[test_case(json!([]), ResponseWrapper::Single(Response::new(None, Some(JsonRpcError::custom("empty batch request".to_string())), RequestId::Null)); "empty batch")]
    #[test_case(json!([1]), ResponseWrapper::Batch(vec![Response::new(None, Some(JsonRpcError::invalid_request()), RequestId::Null)]); "invalid batch but not empty")]
    #[test_case(json!([1, 2, 3, 4]),
    ResponseWrapper::Batch(vec![
        Response::new(None, Some(JsonRpcError::invalid_request()), RequestId::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), RequestId::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), RequestId::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), RequestId::Null),
    ]); "multiple invalid batch")]
    #[test_case(json!([
        {"jsonrpc": "2.0", "method": "subtract", "params": [42, 43], "id": 1},
        1,
        {"jsonrpc": "2.0", "method": "subtract", "params": [42, 43], "id": 1}
    ]),
    ResponseWrapper::Batch(
        vec![
            Response::new(None, Some(JsonRpcError::method_not_found()), RequestId::Number(1)),
            Response::new(None, Some(JsonRpcError::invalid_request()), RequestId::Null),
            Response::new(None, Some(JsonRpcError::method_not_found()), RequestId::Number(1)),
        ],
    ); "partial success")]
    #[test_case(json!([
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1},
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1}
    ]),
    ResponseWrapper::Single(
        Response::new(None, Some(JsonRpcError::custom("number of requests in batch request exceeds limit of 5".to_string())), RequestId::Null)
    ); "exceed batch request limit")]
    #[actix_web::test]
    async fn json_rpc_specification_batch_compliance(
        payload: Value,
        expected: ResponseWrapper<Response>,
    ) {
        let app = init_server().await;

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp = app.call(req).await.unwrap();
        let resp: jsonrpc::ResponseWrapper<Response> =
            serde_json::from_value(recover_response_body(resp).await).unwrap();
        assert_eq!(resp, expected);
    }

    #[allow(non_snake_case)]
    #[actix_web::test]
    async fn test_monad_eth_call_sha256_precompile() {
        let app = init_server().await;
        let payload = json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                {
                    "to": "0x0000000000000000000000000000000000000002",
                    "data": "0x68656c6c6f" // hex for "hello"
                },
                "latest"
            ],
            "id": 1
        });

        let req = actix_web::test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp: jsonrpc::Response = actix_test::call_and_read_body_json(&app, req).await;
        assert!(resp.result.is_none());
    }

    #[allow(non_snake_case)]
    #[actix_web::test]
    async fn test_monad_eth_call() {
        let app = init_server().await;
        let payload = json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
            {
                "from": "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
                "to": "0xd46e8dd67c5d32be8058bb8eb970870f07244567",
                "gas": "0x76c0",
                "gasPrice": "0x9184e72a000",
                "value": "0x9184e72a",
                "data": "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
            },
            "latest"
            ],
            "id": 1
        });

        let req = actix_web::test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp: jsonrpc::Response = actix_test::call_and_read_body_json(&app, req).await;
        assert!(resp.result.is_none());
    }
}
