use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use actix::prelude::*;
use actix_web::{
    dev::{ServiceRequest, ServiceResponse},
    web, App, Error, HttpResponse, HttpServer,
};
use alloy_consensus::TxEnvelope;
use clap::Parser;
use eth_json_types::serialize_result;
use futures::{SinkExt, StreamExt};
use monad_archive::archive_reader::ArchiveReader;
use monad_eth_types::BASE_FEE_PER_GAS;
use monad_triedb_utils::triedb_env::TriedbEnv;
use opentelemetry::{metrics::MeterProvider, trace::TracerProvider, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use serde_json::Value;
use tracing::{debug, warn};
use tracing_actix_web::{RootSpan, RootSpanBuilder, TracingLogger};
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer as FmtLayer},
    layer::SubscriberExt,
    EnvFilter, Registry,
};

use crate::{
    account_handlers::{
        monad_eth_getBalance, monad_eth_getCode, monad_eth_getProof, monad_eth_getStorageAt,
        monad_eth_getTransactionCount, monad_eth_syncing,
    },
    block_handlers::{
        monad_eth_blockNumber, monad_eth_chainId, monad_eth_getBlockByHash,
        monad_eth_getBlockByNumber, monad_eth_getBlockReceipts,
        monad_eth_getBlockTransactionCountByHash, monad_eth_getBlockTransactionCountByNumber,
    },
    call::monad_eth_call,
    cli::Cli,
    debug::{
        monad_debug_getRawBlock, monad_debug_getRawHeader, monad_debug_getRawReceipts,
        monad_debug_getRawTransaction, monad_debug_traceCall,
    },
    eth_txn_handlers::{
        monad_eth_getLogs, monad_eth_getTransactionByBlockHashAndIndex,
        monad_eth_getTransactionByBlockNumberAndIndex, monad_eth_getTransactionByHash,
        monad_eth_getTransactionReceipt, monad_eth_sendRawTransaction,
    },
    fee::FixedFee,
    gas_handlers::{
        monad_eth_estimateGas, monad_eth_feeHistory, monad_eth_gasPrice,
        monad_eth_maxPriorityFeePerGas,
    },
    jsonrpc::{JsonRpcError, JsonRpcResultExt, Request, RequestWrapper, Response},
    middleware::{
        build_middleware_chain, LoggingMiddleware, Middleware,
        RateLimitMiddleware, RpcHandler,
    },
    trace::{
        monad_trace_block, monad_trace_call, monad_trace_callMany, monad_trace_get,
        monad_trace_transaction,
    },
    trace_handlers::{
        monad_debug_traceBlockByHash, monad_debug_traceBlockByNumber, monad_debug_traceTransaction,
    },
    txpool::{EthTxPoolBridge, EthTxPoolBridgeState},
    vpool::{monad_txpool_statusByAddress, monad_txpool_statusByHash},
    websocket::Disconnect,
};

mod account_handlers;
mod block_handlers;
mod call;
mod cli;
mod debug;
pub mod docs;
mod eth_json_types;
mod eth_txn_handlers;
mod fee;
mod gas_handlers;
mod gas_oracle;
mod hex;
mod jsonrpc;
mod metrics;
mod middleware;
mod trace;
mod trace_handlers;
mod txpool;
mod vpool;
mod websocket;

const WEB3_RPC_CLIENT_VERSION: &str = concat!("Monad/", env!("VERGEN_GIT_DESCRIBE"));

#[derive(Clone)]
pub enum RpcLimiterId {
    EthCall = 0,
}

impl From<RpcLimiterId> for usize {
    fn from(id: RpcLimiterId) -> Self {
        id as usize
    }
}

pub struct MonadRpcServer {
    resources: MonadRpcResources,
    meter_provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
    middlewares: Vec<Box<dyn Middleware<MonadRpcResources>>>,
    rpc_handler: Box<dyn RpcHandler<MonadRpcResources>>,
    args: Cli,
}

pub struct JsonRpcMethodHandler;

impl RpcHandler<MonadRpcResources> for JsonRpcMethodHandler {
    fn handle<'a>(
        &'a self,
        request: &'a Value,
        app_state: &'a MonadRpcResources,
    ) -> Pin<Box<dyn Future<Output = Result<Value, JsonRpcError>> + 'a>> {
        Box::pin(async move {
            let request: RequestWrapper<Value> = serde_json::from_value(request.clone())?;

            match request {
                RequestWrapper::Single(json_request) => {
                    let request = serde_json::from_value::<Request>(json_request.clone())
                        .map_err(|_| JsonRpcError::parse_error())?;

                    serde_json::to_value(Response::from_result(
                        request.id.clone(),
                        MonadRpcServer::rpc_select(app_state, &request.method, request.params)
                            .await,
                    ))
                    .map_err(|_| JsonRpcError::internal_error("Serialization error".into()))
                }
                RequestWrapper::Batch(batch) => {
                    if batch.is_empty() || batch.len() > app_state.batch_request_limit as usize {
                        return Err(JsonRpcError::invalid_request());
                    }

                    let responses =
                        futures::future::join_all(batch.iter().map(|json_request| async move {
                            let request =
                                match serde_json::from_value::<Request>(json_request.clone()) {
                                    Ok(req) => req,
                                    Err(_) => {
                                        return Response::from_error(JsonRpcError::invalid_request())
                                    }
                                };

                            Response::from_result(
                                request.id,
                                MonadRpcServer::rpc_select(
                                    app_state,
                                    &request.method,
                                    request.params,
                                )
                                .await,
                            )
                        }))
                        .await;

                    serde_json::to_value(responses)
                        .map_err(|_| JsonRpcError::internal_error("Serialization error".into()))
                }
            }
        })
    }
}

impl MonadRpcServer {
    pub async fn new(args: Cli) -> Result<Self, Box<dyn std::error::Error>> {
        Self::setup_tracing(&args)?;

        let mut rate_limiter = RateLimitMiddleware::new();
        rate_limiter.add_limiter(RpcLimiterId::EthCall, args.eth_call_max_concurrent_requests);
        rate_limiter.map_method_to_limiter("eth_call", RpcLimiterId::EthCall)?;
        rate_limiter.map_method_to_limiter("eth_estimateGas", RpcLimiterId::EthCall)?;

        let (ipc_sender, ipc_receiver) = flume::bounded::<TxEnvelope>(10_000);
        let txpool_state = EthTxPoolBridgeState::new();

        Self::spawn_mempool_bridge(
            ipc_receiver,
            txpool_state.clone(),
            args.ipc_path
                .clone()
                .into_os_string()
                .into_string()
                .unwrap(),
        );

        let triedb_env = Self::setup_triedb_env(&args);
        let archive_reader = Self::setup_archive_reader(&args).await;

        rayon::ThreadPoolBuilder::new()
            .num_threads(args.compute_threadpool_size)
            .build_global()?;

        let resources = MonadRpcResources::new(
            ipc_sender,
            txpool_state,
            triedb_env,
            archive_reader,
            BASE_FEE_PER_GAS.into(),
            args.chain_id,
            args.batch_request_limit,
            args.max_response_size,
            args.allow_unprotected_txs,
            args.eth_get_logs_max_block_range,
        );

        let meter_provider = Self::setup_metrics(&args)?;

        let middlewares: Vec<Box<dyn Middleware<MonadRpcResources>>> = vec![
            Box::new(LoggingMiddleware::new()),
            Box::new(rate_limiter),
        ];
        let rpc_handler = Box::new(JsonRpcMethodHandler);

        Ok(Self {
            resources,
            meter_provider,
            middlewares,
            rpc_handler,
            args,
        })
    }

    pub async fn run(self) -> std::io::Result<()> {
        let metrics = self
            .meter_provider
            .as_ref()
            .map(|provider| metrics::Metrics::new(provider.clone().meter("opentelemetry")));

        let resources = self.resources;
        let max_request_size = self.args.max_request_size;
        let middlewares = web::Data::new(self.middlewares);
        let handler = web::Data::new(self.rpc_handler);

        match metrics {
            Some(metrics) => {
                HttpServer::new(move || {
                    App::new()
                        .wrap(metrics.clone())
                        .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                        .app_data(web::PayloadConfig::default().limit(max_request_size))
                        .app_data(web::Data::new(resources.clone()))
                        .app_data(middlewares.clone())
                        .app_data(handler.clone())
                        .service(web::resource("/").route(web::post().to(Self::rpc_handler)))
                        .service(web::resource("/ws/").route(web::get().to(websocket::handler)))
                })
                .bind((self.args.rpc_addr, self.args.rpc_port))?
                .shutdown_timeout(1)
                .workers(2)
                .run()
                .await
            }
            None => {
                HttpServer::new(move || {
                    App::new()
                        .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                        .app_data(web::PayloadConfig::default().limit(max_request_size))
                        .app_data(web::Data::new(resources.clone()))
                        .app_data(middlewares.clone())
                        .app_data(handler.clone())
                        .service(web::resource("/").route(web::post().to(Self::rpc_handler)))
                        .service(web::resource("/ws/").route(web::get().to(websocket::handler)))
                })
                .bind((self.args.rpc_addr, self.args.rpc_port))?
                .shutdown_timeout(1)
                .workers(2)
                .run()
                .await
            }
        }
    }

    pub async fn rpc_handler(
        root_span: RootSpan,
        body: bytes::Bytes,
        app_state: web::Data<MonadRpcResources>,
        middlewares: web::Data<Vec<Box<dyn Middleware<MonadRpcResources>>>>,
        handler: web::Data<Box<dyn RpcHandler<MonadRpcResources>>>,
    ) -> HttpResponse {
        let request_value = match serde_json::from_slice::<Value>(&body) {
            Ok(req) => req,
            Err(e) => {
                debug!("parse error: {e} {body:?}");
                return HttpResponse::Ok().json(Response::from_error(JsonRpcError::parse_error()));
            }
        };

        // Record method for tracing
        if let Ok(request) = serde_json::from_value::<Request>(request_value.clone()) {
            root_span.record("json_method", &request.method);
        } else if let Ok(RequestWrapper::<Value>::Batch(_)) =
            serde_json::from_value(request_value.clone())
        {
            root_span.record("json_method", "batch");
        }

        match build_middleware_chain(
            &request_value,
            app_state.as_ref(),
            middlewares.as_ref(),
            handler.as_ref().as_ref(),
        )
        .await
        {
            Ok(response) => HttpResponse::Ok().json(response),
            Err(e) => HttpResponse::Ok().json(Response::from_error(e)),
        }
    }

    async fn rpc_select(
        app_state: &MonadRpcResources,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        match method {
            "debug_getRawBlock" => {
                let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
                let params = serde_json::from_value(params).invalid_params()?;
                monad_debug_getRawBlock(triedb_env, params)
                    .await
                    .map(serialize_result)?
            }
            "debug_getRawHeader" => {
                let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
                let params = serde_json::from_value(params).invalid_params()?;
                monad_debug_getRawHeader(triedb_env, params)
                    .await
                    .map(serialize_result)?
            }
            "debug_getRawReceipts" => {
                let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
                let params = serde_json::from_value(params).invalid_params()?;
                monad_debug_getRawReceipts(triedb_env, params)
                    .await
                    .map(serialize_result)?
            }
            "debug_getRawTransaction" => {
                let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
                let params = serde_json::from_value(params).invalid_params()?;
                monad_debug_getRawTransaction(triedb_env, params)
                    .await
                    .map(serialize_result)?
            }
            "debug_traceBlockByHash" => {
                let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
                let params = serde_json::from_value(params).invalid_params()?;
                monad_debug_traceBlockByHash(triedb_env, &app_state.archive_reader, params)
                    .await
                    .map(serialize_result)?
            }
            "debug_traceBlockByNumber" => {
                let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
                let params = serde_json::from_value(params).invalid_params()?;
                monad_debug_traceBlockByNumber(triedb_env, &app_state.archive_reader, params)
                    .await
                    .map(serialize_result)?
            }
            "debug_traceCall" => {
                let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
                let params = serde_json::from_value(params).invalid_params()?;
                monad_debug_traceCall(triedb_env, params)
                    .await
                    .map(serialize_result)?
            }
            "debug_traceTransaction" => {
                let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
                let params = serde_json::from_value(params).invalid_params()?;
                monad_debug_traceTransaction(triedb_env, &app_state.archive_reader, params)
                    .await
                    .map(serialize_result)?
            }
            "eth_call" => {
                let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_call(triedb_env, app_state.chain_id, params)
                    .await
                    .map(serialize_result)?
            }
            "eth_sendRawTransaction" => {
                let params = serde_json::from_value(params).invalid_params()?;
                let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
                monad_eth_sendRawTransaction(
                    triedb_env,
                    app_state.mempool_sender.clone(),
                    &app_state.mempool_state,
                    app_state.base_fee_per_gas.clone(),
                    params,
                    app_state.chain_id,
                    app_state.allow_unprotected_txs,
                )
                .await
                .map(serialize_result)?
            }
            "eth_getLogs" => {
                let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;

                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getLogs(
                    triedb_env,
                    &app_state.archive_reader,
                    app_state.logs_max_block_range,
                    params,
                )
                .await
                .map(serialize_result)?
            }
            "eth_getTransactionByHash" => {
                if let Some(triedb_env) = app_state.triedb_reader.as_ref() {
                    let params = serde_json::from_value(params).invalid_params()?;
                    monad_eth_getTransactionByHash(triedb_env, &app_state.archive_reader, params)
                        .await
                        .map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_getBlockByHash" => {
                if let Some(triedb_env) = &app_state.triedb_reader {
                    let params = serde_json::from_value(params).invalid_params()?;
                    monad_eth_getBlockByHash(triedb_env, &app_state.archive_reader, params)
                        .await
                        .map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_getBlockByNumber" => {
                if let Some(reader) = &app_state.triedb_reader {
                    let params = serde_json::from_value(params).invalid_params()?;
                    monad_eth_getBlockByNumber(reader, &app_state.archive_reader, params)
                        .await
                        .map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_getTransactionByBlockHashAndIndex" => {
                if let Some(triedb_env) = &app_state.triedb_reader {
                    let params = serde_json::from_value(params).invalid_params()?;
                    monad_eth_getTransactionByBlockHashAndIndex(
                        triedb_env,
                        &app_state.archive_reader,
                        params,
                    )
                    .await
                    .map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_getTransactionByBlockNumberAndIndex" => {
                if let Some(triedb_env) = &app_state.triedb_reader {
                    let params = serde_json::from_value(params).invalid_params()?;
                    monad_eth_getTransactionByBlockNumberAndIndex(
                        triedb_env,
                        &app_state.archive_reader,
                        params,
                    )
                    .await
                    .map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_getBlockTransactionCountByHash" => {
                if let Some(triedb_env) = app_state.triedb_reader.as_ref() {
                    let params = serde_json::from_value(params).invalid_params()?;
                    monad_eth_getBlockTransactionCountByHash(
                        triedb_env,
                        &app_state.archive_reader,
                        params,
                    )
                    .await
                    .map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_getBlockTransactionCountByNumber" => {
                if let Some(triedb_env) = app_state.triedb_reader.as_ref() {
                    let params = serde_json::from_value(params).invalid_params()?;
                    monad_eth_getBlockTransactionCountByNumber(
                        triedb_env,
                        &app_state.archive_reader,
                        params,
                    )
                    .await
                    .map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_getBalance" => {
                if let Some(reader) = &app_state.triedb_reader {
                    let params = serde_json::from_value(params).invalid_params()?;
                    monad_eth_getBalance(reader, params)
                        .await
                        .map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_getCode" => {
                if let Some(reader) = &app_state.triedb_reader {
                    let params = serde_json::from_value(params).invalid_params()?;
                    monad_eth_getCode(reader, params)
                        .await
                        .map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_getStorageAt" => {
                if let Some(reader) = &app_state.triedb_reader {
                    let params = serde_json::from_value(params).invalid_params()?;
                    monad_eth_getStorageAt(reader, params)
                        .await
                        .map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_getTransactionCount" => {
                if let Some(reader) = &app_state.triedb_reader {
                    let params = serde_json::from_value(params).invalid_params()?;
                    monad_eth_getTransactionCount(reader, params)
                        .await
                        .map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_blockNumber" => {
                if let Some(reader) = &app_state.triedb_reader {
                    monad_eth_blockNumber(reader).await.map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_chainId" => monad_eth_chainId(app_state.chain_id)
                .await
                .map(serialize_result)?,
            "eth_syncing" => monad_eth_syncing().await,
            "eth_estimateGas" => {
                let Some(triedb_env) = &app_state.triedb_reader else {
                    return Err(JsonRpcError::method_not_supported());
                };

                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_estimateGas(triedb_env, app_state.chain_id, params)
                    .await
                    .map(serialize_result)?
            }
            "eth_gasPrice" => {
                if let Some(triedb_env) = &app_state.triedb_reader {
                    monad_eth_gasPrice(triedb_env).await.map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_maxPriorityFeePerGas" => {
                if let Some(triedb_env) = &app_state.triedb_reader {
                    monad_eth_maxPriorityFeePerGas(triedb_env)
                        .await
                        .map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_feeHistory" => {
                if let Some(triedb_env) = &app_state.triedb_reader {
                    let params = serde_json::from_value(params).invalid_params()?;
                    monad_eth_feeHistory(triedb_env, params)
                        .await
                        .map(serialize_result)?
                } else {
                    Err(JsonRpcError::method_not_supported())
                }
            }
            "eth_getTransactionReceipt" => {
                let Some(triedb_reader) = &app_state.triedb_reader else {
                    return Err(JsonRpcError::method_not_supported());
                };

                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getTransactionReceipt(triedb_reader, &app_state.archive_reader, params)
                    .await
                    .map(serialize_result)?
            }
            "eth_getBlockReceipts" => {
                let triedb_reader = app_state.triedb_reader.as_ref().method_not_supported()?;
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getBlockReceipts(triedb_reader, &app_state.archive_reader, params)
                    .await
                    .map(serialize_result)?
            }
            "eth_getProof" => {
                let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
                let params = serde_json::from_value(params).invalid_params()?;
                monad_eth_getProof(triedb_env, params)
                    .await
                    .map(serialize_result)?
            }
            "eth_sendTransaction" => Err(JsonRpcError::method_not_supported()),
            "eth_signTransaction" => Err(JsonRpcError::method_not_supported()),
            "eth_sign" => Err(JsonRpcError::method_not_supported()),
            "eth_hashrate" => Err(JsonRpcError::method_not_supported()),
            "net_version" => serialize_result(app_state.chain_id.to_string()),
            "trace_block" => {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_trace_block(params).await.map(serialize_result)?
            }
            "trace_call" => {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_trace_call(params).await.map(serialize_result)?
            }
            "trace_callMany" => monad_trace_callMany().await.map(serialize_result)?,
            "trace_get" => {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_trace_get(params).await.map(serialize_result)?
            }
            "trace_transaction" => {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_trace_transaction(params)
                    .await
                    .map(serialize_result)?
            }
            "txpool_statusByHash" => {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_txpool_statusByHash(&app_state.mempool_state, params)
                    .await
                    .map(serialize_result)?
            }
            "txpool_statusByAddress" => {
                let params = serde_json::from_value(params).invalid_params()?;
                monad_txpool_statusByAddress(&app_state.mempool_state, params)
                    .await
                    .map(serialize_result)?
            }
            "web3_clientVersion" => serialize_result(WEB3_RPC_CLIENT_VERSION),
            _ => Err(JsonRpcError::method_not_found()),
        }
    }

    fn setup_tracing(args: &Cli) -> Result<(), Box<dyn std::error::Error>> {
        let otlp_exporter: Option<opentelemetry_otlp::SpanExporter> =
            args.otel_endpoint.as_ref().map(|endpoint| {
                opentelemetry_otlp::SpanExporterBuilder::Tonic(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint(endpoint),
                )
                .build_span_exporter()
                .expect("cannot build span exporter for otel_endpoint")
            });

        let rt = opentelemetry_sdk::runtime::Tokio;
        let service_name = args
            .metrics_service_name
            .clone()
            .unwrap_or_else(|| "monad-rpc".to_string());

        let otel_span_telemetry = match otlp_exporter {
            Some(exporter) => {
                let otel_config = opentelemetry_sdk::trace::Config::default().with_resource(
                    opentelemetry_sdk::Resource::new(vec![KeyValue::new(
                        "service.name".to_string(),
                        service_name,
                    )]),
                );

                let trace_provider = opentelemetry_sdk::trace::TracerProvider::builder()
                    .with_config(otel_config)
                    .with_batch_exporter(exporter, rt)
                    .build();
                let tracer = trace_provider.tracer("monad-rpc");
                Some(tracing_opentelemetry::layer().with_tracer(tracer))
            }
            None => None,
        };

        let fmt_layer = FmtLayer::default()
            .json()
            .with_span_events(FmtSpan::NONE)
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(std::io::stdout)
            .with_ansi(false);

        match otel_span_telemetry {
            Some(telemetry) => {
                let s = Registry::default()
                    .with(EnvFilter::from_default_env())
                    .with(telemetry)
                    .with(fmt_layer);
                tracing::subscriber::set_global_default(s)?;
            }
            None => {
                let s = Registry::default()
                    .with(EnvFilter::from_default_env())
                    .with(
                        FmtLayer::default()
                            .json()
                            .with_span_events(FmtSpan::NONE)
                            .with_current_span(false)
                            .with_span_list(false)
                            .with_writer(std::io::stdout)
                            .with_ansi(false),
                    );
                tracing::subscriber::set_global_default(s)?;
            }
        };

        Ok(())
    }

    fn setup_metrics(
        args: &Cli,
    ) -> Result<Option<opentelemetry_sdk::metrics::SdkMeterProvider>, Box<dyn std::error::Error>>
    {
        let meter_provider = args.otel_endpoint.as_ref().map(|endpoint| {
            let service_name = args
                .metrics_service_name
                .clone()
                .unwrap_or_else(|| "monad-rpc".to_string());

            let provider = metrics::build_otel_meter_provider(
                endpoint,
                service_name,
                std::time::Duration::from_secs(5),
            )
            .expect("failed to build otel meter");

            opentelemetry::global::set_meter_provider(provider.clone());
            provider
        });

        Ok(meter_provider)
    }

    fn setup_triedb_env(args: &Cli) -> Option<TriedbEnv> {
        args.triedb_path.clone().as_deref().map(|path| {
            TriedbEnv::new(
                path,
                args.triedb_max_buffered_read_requests as usize,
                args.triedb_max_async_read_concurrency as usize,
                args.triedb_max_buffered_traverse_requests as usize,
                args.triedb_max_async_traverse_concurrency as usize,
                args.max_finalized_block_cache_len as usize,
                args.max_voted_block_cache_len as usize,
            )
        })
    }

    async fn setup_archive_reader(args: &Cli) -> Option<ArchiveReader> {
        let aws_archive_reader = match (
            &args.s3_bucket,
            &args.region,
            &args.archive_url,
            &args.archive_api_key,
        ) {
            (Some(s3_bucket), Some(region), Some(archive_url), Some(archive_api_key)) => {
                match ArchiveReader::init_aws_reader(
                    s3_bucket.clone(),
                    Some(region.clone()),
                    archive_url,
                    archive_api_key,
                    5,
                )
                .await
                {
                    Ok(reader) => Some(reader),
                    Err(e) => {
                        warn!("Unable to initialize archive reader {e}");
                        None
                    }
                }
            }
            _ => None,
        };

        match (&args.mongo_db_name, &args.mongo_url) {
            (Some(db_name), Some(url)) => {
                match ArchiveReader::init_mongo_reader(url.clone(), db_name.clone()).await {
                    Ok(mongo_reader) => Some(mongo_reader.with_fallback(aws_archive_reader)),
                    Err(e) => {
                        warn!("Unable to initialize mongo-backed ArchiveReader: {e:?}");
                        aws_archive_reader
                    }
                }
            }
            _ => aws_archive_reader,
        }
    }

    fn spawn_mempool_bridge(
        ipc_receiver: flume::Receiver<TxEnvelope>,
        txpool_state: Arc<EthTxPoolBridgeState>,
        ipc_path: String,
    ) {
        tokio::spawn(async move {
            let mut bridge =
                retry(|| async { EthTxPoolBridge::new(&ipc_path, txpool_state.clone()).await })
                    .await
                    .expect("failed to create ipc sender");

            let mut cleanup_timer = tokio::time::interval(Duration::from_secs(5));

            loop {
                tokio::select! {
                    result = ipc_receiver.recv_async() => {
                        let tx = result.unwrap();

                        txpool_state.add_tx(&tx);
                        if let Err(e) = bridge.send(&tx).await {
                            warn!("IPC send failed, monad-bft likely crashed: {}", e);
                        }
                    }

                    result = bridge.next() => {
                        let Some(events) = result else {
                            continue;
                        };

                        txpool_state.handle_events(events);
                    }

                    now = cleanup_timer.tick() => {
                        txpool_state.cleanup(now);
                    }
                }
            }
        });
    }
}

#[derive(Clone)]
struct MonadRpcResources {
    mempool_sender: flume::Sender<TxEnvelope>,
    mempool_state: Arc<EthTxPoolBridgeState>,
    triedb_reader: Option<TriedbEnv>,
    archive_reader: Option<ArchiveReader>,
    base_fee_per_gas: FixedFee,
    chain_id: u64,
    batch_request_limit: u16,
    max_response_size: u32,
    allow_unprotected_txs: bool,
    logs_max_block_range: u64,
}

impl Handler<Disconnect> for MonadRpcResources {
    type Result = ();

    fn handle(&mut self, _msg: Disconnect, ctx: &mut Self::Context) -> Self::Result {
        debug!("received disconnect {:?}", ctx);
    }
}

impl MonadRpcResources {
    pub fn new(
        mempool_sender: flume::Sender<TxEnvelope>,
        mempool_state: Arc<EthTxPoolBridgeState>,
        triedb_reader: Option<TriedbEnv>,
        archive_reader: Option<ArchiveReader>,
        fixed_base_fee: u128,
        chain_id: u64,
        batch_request_limit: u16,
        max_response_size: u32,
        allow_unprotected_txs: bool,
        logs_max_block_range: u64,
    ) -> Self {
        Self {
            mempool_sender,
            mempool_state,
            triedb_reader,
            archive_reader,
            base_fee_per_gas: FixedFee::new(fixed_base_fee),
            chain_id,
            batch_request_limit,
            max_response_size,
            allow_unprotected_txs,
            logs_max_block_range,
        }
    }
}

impl Actor for MonadRpcResources {
    type Context = Context<Self>;
}

pub struct MonadJsonRootSpanBuilder;

impl RootSpanBuilder for MonadJsonRootSpanBuilder {
    fn on_request_start(request: &ServiceRequest) -> tracing::Span {
        tracing_actix_web::root_span!(request, json_method = tracing::field::Empty)
    }

    fn on_request_end<B: actix_web::body::MessageBody>(
        span: tracing::Span,
        outcome: &Result<ServiceResponse<B>, Error>,
    ) {
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> std::io::Result<()> {
    let args = Cli::parse();

    let server = MonadRpcServer::new(args)
        .await
        .expect("Failed to initialize server");

    server.run().await
}

async fn retry<T, E, F>(attempt: impl Fn() -> F) -> Result<T, E>
where
    F: futures::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let duration = std::time::Duration::from_secs(2);
    let mut retries = 1;

    loop {
        match attempt().await {
            Ok(t) => return Ok(t),
            Err(e) if retries <= 3 => {
                let timeout = duration * retries;
                debug!("caught error: {e}, retrying in {timeout:#?}");
                tokio::time::sleep(timeout).await;
                retries += 1;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use actix_http::{Request, StatusCode};
    use actix_web::{
        body::{to_bytes, MessageBody},
        dev::{Service, ServiceResponse},
        test, Error,
    };
    use alloy_consensus::TxEnvelope;
    use jsonrpc::ResponseWrapper;
    use serde_json::{json, Number};
    use test_case::test_case;

    use super::*;

    pub struct MonadRpcResourcesState {
        pub ipc_receiver: flume::Receiver<TxEnvelope>,
    }

    pub async fn init_server() -> (
        impl Service<Request, Response = ServiceResponse<impl MessageBody>, Error = Error>,
        MonadRpcResourcesState,
    ) {
        let (ipc_sender, ipc_receiver) = flume::bounded(1_000);
        let m = MonadRpcResourcesState { ipc_receiver };
        let resources = MonadRpcResources {
            mempool_sender: ipc_sender.clone(),
            mempool_state: EthTxPoolBridgeState::new(),
            triedb_reader: None,
            archive_reader: None,
            base_fee_per_gas: FixedFee::new(2000),
            chain_id: 1337,
            batch_request_limit: 5,
            max_response_size: 25_000_000,
            allow_unprotected_txs: false,
            logs_max_block_range: 1000,
        };
        let app = test::init_service(
            App::new()
                .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                .app_data(web::PayloadConfig::default().limit(2_000_000))
                .app_data(web::Data::new(resources.clone()))
                .app_data(web::Data::new(Vec::<Box<dyn Middleware<MonadRpcResources>>>::new()))
                .app_data(web::Data::new(Box::new(JsonRpcMethodHandler) as Box<dyn RpcHandler<MonadRpcResources>>))
                .service(web::resource("/").route(web::post().to(MonadRpcServer::rpc_handler)))
                .service(web::resource("/ws/").route(web::get().to(websocket::handler))),
        )
        .await;
        (app, m)
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
        let (app, _) = init_server().await;

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
        let (app, _) = init_server().await;

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
    #[test_case(json!([]), ResponseWrapper::Single(Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null)); "empty batch")]
    #[test_case(json!([1]), ResponseWrapper::Batch(vec![Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null)]); "invalid batch but not empty")]
    #[test_case(json!([1, 2, 3, 4]),
    ResponseWrapper::Batch(vec![
        Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
    ]); "multiple invalid batch")]
    #[test_case(json!([
        {"jsonrpc": "2.0", "method": "subtract", "params": [42, 43], "id": 1},
        1,
        {"jsonrpc": "2.0", "method": "subtract", "params": [42, 43], "id": 1}
    ]),
    ResponseWrapper::Batch(
        vec![
            Response::new(None, Some(JsonRpcError::method_not_found()), Value::Number(Number::from(1))),
            Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null),
            Response::new(None, Some(JsonRpcError::method_not_found()), Value::Number(Number::from(1))),
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
    ResponseWrapper::Single(Response::new(None, Some(JsonRpcError::invalid_request()), Value::Null)); "exceed batch request limit")]
    #[actix_web::test]
    async fn json_rpc_specification_batch_compliance(
        payload: Value,
        expected: ResponseWrapper<Response>,
    ) {
        let (app, _) = init_server().await;

        let req = test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp = app.call(req).await.unwrap();
        let resp: jsonrpc::ResponseWrapper<Response> =
            serde_json::from_value(recover_response_body(resp).await).unwrap();
        assert_eq!(resp, expected);
    }
}
