use std::{sync::Arc, time::Duration};

use actix::prelude::*;
use actix_web::{
    dev::{ServiceRequest, ServiceResponse},
    web, App, Error, HttpResponse, HttpServer,
};
use clap::Parser;
use eth_json_types::serialize_result;
use fee::FixedFee;
use meta::{monad_net_version, monad_web3_client_version};
use monad_archive::archive_reader::ArchiveReader;
use monad_eth_types::BASE_FEE_PER_GAS;
use monad_ethcall::EthCallExecutor;
use monad_node_config::MonadNodeConfig;
use monad_triedb_utils::triedb_env::TriedbEnv;
use opentelemetry::{metrics::MeterProvider, trace::TracerProvider as _, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use serde_json::Value;
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, error, info, warn};
use tracing_actix_web::{RootSpan, RootSpanBuilder, TracingLogger};
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer as FmtLayer},
    layer::SubscriberExt,
    EnvFilter, Registry,
};

use crate::{
    account_handlers::{
        monad_eth_getBalance, monad_eth_getCode, monad_eth_getStorageAt,
        monad_eth_getTransactionCount, monad_eth_syncing,
    },
    block_handlers::{
        monad_eth_blockNumber, monad_eth_chainId, monad_eth_getBlockByHash,
        monad_eth_getBlockByNumber, monad_eth_getBlockReceipts,
        monad_eth_getBlockTransactionCountByHash, monad_eth_getBlockTransactionCountByNumber,
    },
    call::{
        monad_admin_ethCallStatistics, monad_debug_traceCall, monad_eth_call, EthCallStatsTracker,
    },
    cli::Cli,
    debug::{
        monad_debug_getRawBlock, monad_debug_getRawHeader, monad_debug_getRawReceipts,
        monad_debug_getRawTransaction,
    },
    eth_txn_handlers::{
        monad_eth_getLogs, monad_eth_getTransactionByBlockHashAndIndex,
        monad_eth_getTransactionByBlockNumberAndIndex, monad_eth_getTransactionByHash,
        monad_eth_getTransactionReceipt, monad_eth_sendRawTransaction,
    },
    gas_handlers::{
        monad_eth_estimateGas, monad_eth_feeHistory, monad_eth_gasPrice,
        monad_eth_maxPriorityFeePerGas,
    },
    jsonrpc::{JsonRpcError, JsonRpcResultExt, Request, RequestWrapper, Response, ResponseWrapper},
    timing::{RequestId, TimingMiddleware},
    trace::{
        monad_trace_block, monad_trace_call, monad_trace_callMany, monad_trace_get,
        monad_trace_transaction,
    },
    trace_handlers::{
        monad_debug_traceBlockByHash, monad_debug_traceBlockByNumber, monad_debug_traceTransaction,
    },
    txpool::{EthTxPoolBridge, EthTxPoolBridgeClient},
    vpool::{monad_txpool_statusByAddress, monad_txpool_statusByHash},
    websocket::Disconnect,
};

mod account_handlers;
mod block_handlers;
mod call;
mod cli;
mod debug;
mod eth_json_types;
mod eth_txn_handlers;
mod fee;
mod gas_handlers;
mod gas_oracle;
mod hex;
mod jsonrpc;
mod meta;
mod metrics;
mod timing;
mod trace;
mod trace_handlers;
mod txpool;
mod vpool;
mod websocket;

pub(crate) async fn rpc_handler(
    root_span: RootSpan,
    body: bytes::Bytes,
    app_state: web::Data<MonadRpcResources>,
    request_id: RequestId,
) -> HttpResponse {
    let request: RequestWrapper<Value> = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(e) => {
            debug!("parse error: {e} {body:?}");
            return HttpResponse::Ok().json(Response::from_error(JsonRpcError::parse_error()));
        }
    };

    let response = match request {
        RequestWrapper::Single(json_request) => {
            let Ok(request) = serde_json::from_value::<Request>(json_request) else {
                return HttpResponse::Ok().json(Response::from_error(JsonRpcError::parse_error()));
            };
            root_span.record("json_method", &request.method);
            ResponseWrapper::Single(Response::from_result(
                request.id,
                rpc_select(
                    &app_state,
                    &request.method,
                    request.params,
                    request_id.clone(),
                )
                .await,
            ))
        }
        RequestWrapper::Batch(json_batch_request) => {
            root_span.record("json_method", "batch");
            if json_batch_request.is_empty() {
                return HttpResponse::Ok().json(Response::from_error(JsonRpcError::custom(
                    "empty batch request".to_string(),
                )));
            }
            if json_batch_request.len() > app_state.batch_request_limit as usize {
                return HttpResponse::Ok().json(Response::from_error(JsonRpcError::custom(
                    format!(
                        "number of requests in batch request exceeds limit of {}",
                        app_state.batch_request_limit
                    ),
                )));
            }
            let batch_response =
                futures::future::join_all(json_batch_request.into_iter().map(|json_request| {
                    let app_state = app_state.clone(); // cheap copy
                    let request_id = request_id.clone();

                    async move {
                        let Ok(request) = serde_json::from_value::<Request>(json_request) else {
                            return (Value::Null, Err(JsonRpcError::invalid_request()));
                        };
                        let (state, id, method, params) =
                            (app_state, request.id, request.method, request.params);
                        (id, rpc_select(&state, &method, params, request_id).await)
                    }
                }))
                .await
                .into_iter()
                .map(|(request_id, response)| Response::from_result(request_id, response))
                .collect::<Vec<_>>();
            ResponseWrapper::Batch(batch_response)
        }
    };

    // check if the response size exceeds the limit
    // return invalid request error if it does
    match serde_json::to_vec(&response) {
        Ok(bytes) => {
            if bytes.len() > app_state.max_response_size as usize {
                info!("response exceed size limit: {body:?}");
                return HttpResponse::Ok().json(Response::from_error(JsonRpcError::custom(
                    "response exceed size limit".to_string(),
                )));
            }
        }
        Err(e) => {
            debug!("response serialization error: {e}");
            return HttpResponse::Ok().json(Response::from_error(JsonRpcError::internal_error(
                format!("serialization error: {}", e),
            )));
        }
    };

    // log the request and response based on the response content
    match &response {
        ResponseWrapper::Single(resp) => match resp.error {
            Some(_) => info!(?body, ?response, "rpc_request/response error"),
            None => debug!(
                ?body,
                ?response,
                ?request_id,
                "rpc_request/response successful"
            ),
        },
        _ => debug!(?body, ?response, ?request_id, "rpc_batch_request/response"),
    }

    HttpResponse::Ok().json(&response)
}

#[tracing::instrument(level = "debug", skip(app_state))]
async fn rpc_select(
    app_state: &MonadRpcResources,
    method: &str,
    params: Value,
    request_id: RequestId,
) -> Result<Value, JsonRpcError> {
    match method {
        "admin_ethCallStatistics" => {
            if app_state.enable_eth_call_statistics {
                let available_permits = app_state.rate_limiter.available_permits();
                if let Some(tracker) = &app_state.eth_call_stats_tracker {
                    monad_admin_ethCallStatistics(
                        app_state.eth_call_executor_fibers,
                        app_state.total_permits,
                        available_permits,
                        tracker,
                    )
                    .await
                    .map(serialize_result)?
                } else {
                    Err(JsonRpcError::internal_error(
                        "stats tracking not initialized".into(),
                    ))
                }
            } else {
                Err(JsonRpcError::method_not_supported())
            }
        }
        "debug_getRawBlock" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_debug_getRawBlock(triedb_env, &app_state.archive_reader, params)
                .await
                .map(serialize_result)?
        }
        "debug_getRawHeader" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_debug_getRawHeader(triedb_env, &app_state.archive_reader, params)
                .await
                .map(serialize_result)?
        }
        "debug_getRawReceipts" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_debug_getRawReceipts(triedb_env, &app_state.archive_reader, params)
                .await
                .map(serialize_result)?
        }
        "debug_getRawTransaction" => {
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            let params = serde_json::from_value(params).invalid_params()?;
            monad_debug_getRawTransaction(triedb_env, &app_state.archive_reader, params)
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
            let Some(ref eth_call_executor) = app_state.eth_call_executor else {
                return Err(JsonRpcError::method_not_supported());
            };
            // acquire the concurrent requests permit
            let _permit = &app_state.rate_limiter.try_acquire().map_err(|_| {
                JsonRpcError::internal_error("eth_call concurrent requests limit".into())
            })?;

            let params = serde_json::from_value(params).invalid_params()?;
            monad_debug_traceCall(
                triedb_env,
                eth_call_executor.clone(),
                app_state.chain_id,
                app_state.eth_call_provider_gas_limit,
                params,
            )
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
            let Some(ref eth_call_executor) = app_state.eth_call_executor else {
                return Err(JsonRpcError::method_not_supported());
            };

            // acquire the concurrent requests permit
            let _permit = match app_state.rate_limiter.try_acquire() {
                Ok(permit) => permit,
                Err(_) => {
                    if let Some(tracker) = &app_state.eth_call_stats_tracker {
                        tracker.record_queue_rejection().await;
                    }
                    return Err(JsonRpcError::internal_error(
                        "eth_call concurrent requests limit".into(),
                    ));
                }
            };

            let params = serde_json::from_value(params).invalid_params()?;

            if let Some(tracker) = &app_state.eth_call_stats_tracker {
                tracker.record_request_start(&request_id).await;
            }

            let result = monad_eth_call(
                triedb_env,
                eth_call_executor.clone(),
                app_state.chain_id,
                app_state.eth_call_provider_gas_limit,
                params,
            )
            .await;

            if let Some(tracker) = &app_state.eth_call_stats_tracker {
                let is_error = result.is_err();
                tracker.record_request_complete(&request_id, is_error).await;
            }

            result.map(serialize_result)?
        }
        "eth_sendRawTransaction" => {
            let params = serde_json::from_value(params).invalid_params()?;
            let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
            monad_eth_sendRawTransaction(
                triedb_env,
                &app_state.txpool_bridge_client,
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
                app_state.use_eth_get_logs_index,
                app_state.dry_run_get_logs_index,
                app_state.max_finalized_block_cache_len,
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
        "eth_syncing" => serialize_result(monad_eth_syncing().await),
        "eth_estimateGas" => {
            let Some(triedb_env) = &app_state.triedb_reader else {
                return Err(JsonRpcError::method_not_supported());
            };
            let Some(ref eth_call_executor) = app_state.eth_call_executor else {
                return Err(JsonRpcError::method_not_supported());
            };

            // acquire the concurrent requests permit
            let _permit = match app_state.rate_limiter.try_acquire() {
                Ok(permit) => permit,
                Err(_) => {
                    if let Some(tracker) = &app_state.eth_call_stats_tracker {
                        tracker.record_queue_rejection().await;
                    }
                    return Err(JsonRpcError::internal_error(
                        "eth_estimateGas concurrent requests limit".into(),
                    ));
                }
            };

            if let Some(tracker) = &app_state.eth_call_stats_tracker {
                tracker.record_request_start(&request_id).await;
            }

            let params = serde_json::from_value(params).invalid_params()?;
            let result = monad_eth_estimateGas(
                triedb_env,
                eth_call_executor.clone(),
                app_state.chain_id,
                app_state.eth_estimate_gas_provider_gas_limit,
                params,
            )
            .await;

            if let Some(tracker) = &app_state.eth_call_stats_tracker {
                let is_error = result.is_err();
                tracker.record_request_complete(&request_id, is_error).await;
            }

            result.map(serialize_result)?
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
        "net_version" => monad_net_version(app_state.chain_id).map(serialize_result)?,
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
            monad_txpool_statusByHash(&app_state.txpool_bridge_client, params)
                .await
                .map(serialize_result)?
        }
        "txpool_statusByAddress" => {
            let params = serde_json::from_value(params).invalid_params()?;
            monad_txpool_statusByAddress(&app_state.txpool_bridge_client, params)
                .await
                .map(serialize_result)?
        }
        "web3_clientVersion" => monad_web3_client_version().map(serialize_result)?,
        _ => Err(JsonRpcError::method_not_found()),
    }
}

#[derive(Clone)]
struct MonadRpcResources {
    txpool_bridge_client: EthTxPoolBridgeClient,
    triedb_reader: Option<TriedbEnv>,
    eth_call_executor: Option<Arc<Mutex<EthCallExecutor>>>,
    eth_call_executor_fibers: usize,
    eth_call_stats_tracker: Option<Arc<EthCallStatsTracker>>,
    archive_reader: Option<ArchiveReader>,
    base_fee_per_gas: FixedFee,
    chain_id: u64,
    batch_request_limit: u16,
    max_response_size: u32,
    allow_unprotected_txs: bool,
    rate_limiter: Arc<Semaphore>,
    total_permits: usize,
    logs_max_block_range: u64,
    eth_call_provider_gas_limit: u64,
    eth_estimate_gas_provider_gas_limit: u64,
    dry_run_get_logs_index: bool,
    use_eth_get_logs_index: bool,
    max_finalized_block_cache_len: u64,
    enable_eth_call_statistics: bool,
}

impl Handler<Disconnect> for MonadRpcResources {
    type Result = ();

    fn handle(&mut self, _msg: Disconnect, ctx: &mut Self::Context) -> Self::Result {
        debug!("received disconnect {:?}", ctx);
    }
}

impl MonadRpcResources {
    pub fn new(
        txpool_bridge_client: EthTxPoolBridgeClient,
        triedb_reader: Option<TriedbEnv>,
        eth_call_executor: Option<Arc<Mutex<EthCallExecutor>>>,
        eth_call_executor_fibers: usize,
        archive_reader: Option<ArchiveReader>,
        fixed_base_fee: u128,
        chain_id: u64,
        batch_request_limit: u16,
        max_response_size: u32,
        allow_unprotected_txs: bool,
        rate_limiter: Arc<Semaphore>,
        total_permits: usize,
        logs_max_block_range: u64,
        eth_call_provider_gas_limit: u64,
        eth_estimate_gas_provider_gas_limit: u64,
        dry_run_get_logs_index: bool,
        use_eth_get_logs_index: bool,
        max_finalized_block_cache_len: u64,
        enable_eth_call_statistics: bool,
    ) -> Self {
        Self {
            txpool_bridge_client,
            triedb_reader,
            eth_call_executor,
            eth_call_executor_fibers,
            eth_call_stats_tracker: if enable_eth_call_statistics {
                Some(Arc::new(EthCallStatsTracker::new()))
            } else {
                None
            },
            archive_reader,
            base_fee_per_gas: FixedFee::new(fixed_base_fee),
            chain_id,
            batch_request_limit,
            max_response_size,
            allow_unprotected_txs,
            rate_limiter,
            total_permits,
            logs_max_block_range,
            eth_call_provider_gas_limit,
            eth_estimate_gas_provider_gas_limit,
            dry_run_get_logs_index,
            use_eth_get_logs_index,
            max_finalized_block_cache_len,
            enable_eth_call_statistics,
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
    let node_config: MonadNodeConfig = toml::from_str(&std::fs::read_to_string(&args.node_config)?)
        .expect("node toml parse error");

    let otlp_exporter: Option<opentelemetry_otlp::SpanExporter> =
        args.otel_endpoint.as_ref().map(|endpoint| {
            opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .expect("cannot build span exporter for otel_endpoint")
        });

    let otel_span_telemetry = match otlp_exporter {
        Some(exporter) => {
            let resource = opentelemetry_sdk::Resource::builder_empty()
                .with_attribute(KeyValue::new(
                    "service.name".to_string(),
                    node_config.node_name.clone(),
                ))
                .build();
            let trace_provider = SdkTracerProvider::builder()
                .with_resource(resource)
                .with_batch_exporter(exporter)
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
            tracing::subscriber::set_global_default(s).expect("failed to set logger");
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
            tracing::subscriber::set_global_default(s).expect("failed to set logger");
        }
    };

    // initialize concurrent requests limiter
    let concurrent_requests_limiter = Arc::new(Semaphore::new(
        args.eth_call_max_concurrent_requests as usize,
    ));

    // Wait for bft to be in a ready state before starting the RPC server.
    // Bft will bind to the ipc socket after state syncing.
    let ipc_path = args.ipc_path;

    let mut print_message_timer = tokio::time::interval(Duration::from_secs(60));
    let mut retry_timer = tokio::time::interval(Duration::from_secs(1));
    let (txpool_bridge_client, txpool_bridge_handle) = loop {
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

    let triedb_env = args.triedb_path.clone().as_deref().map(|path| {
        TriedbEnv::new(
            path,
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
            info!(url, db_name, "Initializing MongoDB archive reader");
            match ArchiveReader::init_mongo_reader(url.clone(), db_name.clone()).await {
                Ok(mongo_reader) => {
                    let has_aws_fallback = aws_archive_reader.is_some();
                    info!(
                        has_aws_fallback,
                        "MongoDB archive reader initialized successfully"
                    );
                    Some(mongo_reader.with_fallback(aws_archive_reader))
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

    let eth_call_executor = args.triedb_path.clone().as_deref().map(|path| {
        Arc::new(tokio::sync::Mutex::new(EthCallExecutor::new(
            args.eth_call_executor_threads,
            args.eth_call_executor_fibers,
            args.eth_call_executor_node_lru_size,
            path,
        )))
    });

    let resources = MonadRpcResources::new(
        txpool_bridge_client,
        triedb_env,
        eth_call_executor,
        args.eth_call_executor_fibers as usize,
        archive_reader,
        BASE_FEE_PER_GAS.into(),
        node_config.chain_id,
        args.batch_request_limit,
        args.max_response_size,
        args.allow_unprotected_txs,
        concurrent_requests_limiter,
        args.eth_call_max_concurrent_requests as usize,
        args.eth_get_logs_max_block_range,
        args.eth_call_provider_gas_limit,
        args.eth_estimate_gas_provider_gas_limit,
        args.dry_run_get_logs_index,
        args.use_eth_get_logs_index,
        args.max_finalized_block_cache_len,
        args.enable_admin_eth_call_statistics,
    );

    let meter_provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider> =
        args.otel_endpoint.as_ref().map(|endpoint| {
            let provider = metrics::build_otel_meter_provider(
                endpoint,
                node_config.node_name,
                std::time::Duration::from_secs(5),
            )
            .expect("failed to build otel meter");
            opentelemetry::global::set_meter_provider(provider.clone());
            provider
        });

    let with_metrics = meter_provider
        .as_ref()
        .map(|provider| metrics::Metrics::new(provider.clone().meter("opentelemetry")));

    // main server app
    let app = match with_metrics {
        Some(metrics) => HttpServer::new(move || {
            App::new()
                .wrap(metrics.clone())
                .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                .wrap(TimingMiddleware)
                .app_data(web::PayloadConfig::default().limit(args.max_request_size))
                .app_data(web::Data::new(resources.clone()))
                .service(web::resource("/").route(web::post().to(rpc_handler)))
                .service(web::resource("/ws/").route(web::get().to(websocket::handler)))
        })
        .bind((args.rpc_addr, args.rpc_port))?
        .shutdown_timeout(1)
        .workers(2)
        .run(),
        None => HttpServer::new(move || {
            App::new()
                .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                .wrap(TimingMiddleware)
                .app_data(web::PayloadConfig::default().limit(args.max_request_size))
                .app_data(web::Data::new(resources.clone()))
                .service(web::resource("/").route(web::post().to(rpc_handler)))
                .service(web::resource("/ws/").route(web::get().to(websocket::handler)))
        })
        .bind((args.rpc_addr, args.rpc_port))?
        .shutdown_timeout(1)
        .workers(2)
        .run(),
    };

    tokio::select! {
        result = app => {
            let () = result?;
        }

        () = txpool_bridge_handle => {
            error!("txpool bridge crashed");
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
    use serde_json::{json, Number};
    use test_case::test_case;

    use super::*;

    pub async fn init_server(
    ) -> impl Service<Request, Response = ServiceResponse<impl MessageBody>, Error = Error> {
        let resources = MonadRpcResources {
            txpool_bridge_client: EthTxPoolBridgeClient::for_testing(),
            triedb_reader: None,
            eth_call_executor: None,
            eth_call_executor_fibers: 64,
            eth_call_stats_tracker: Some(Arc::new(EthCallStatsTracker::new())),
            archive_reader: None,
            base_fee_per_gas: FixedFee::new(2000),
            chain_id: 1337,
            batch_request_limit: 5,
            max_response_size: 25_000_000,
            allow_unprotected_txs: false,
            rate_limiter: Arc::new(Semaphore::new(1000)),
            total_permits: 1000,
            logs_max_block_range: 1000,
            eth_call_provider_gas_limit: u64::MAX,
            eth_estimate_gas_provider_gas_limit: u64::MAX,
            dry_run_get_logs_index: false,
            use_eth_get_logs_index: false,
            max_finalized_block_cache_len: 200,
            enable_eth_call_statistics: true,
        };

        test::init_service(
            App::new()
                .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                .app_data(web::PayloadConfig::default().limit(2_000_000))
                .app_data(web::Data::new(resources.clone()))
                .service(web::resource("/").route(web::post().to(rpc_handler)))
                .service(web::resource("/ws/").route(web::get().to(websocket::handler))),
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
    #[test_case(json!([]), ResponseWrapper::Single(Response::new(None, Some(JsonRpcError::custom("empty batch request".to_string())), Value::Null)); "empty batch")]
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
    ResponseWrapper::Single(
        Response::new(None, Some(JsonRpcError::custom("number of requests in batch request exceeds limit of 5".to_string())), Value::Null)
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
