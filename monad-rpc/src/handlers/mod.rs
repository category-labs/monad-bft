use actix_web::{web, HttpResponse};
use serde_json::Value;
use tracing::{debug, info};
use tracing_actix_web::RootSpan;

use self::{
    debug::{
        monad_debug_getRawBlock, monad_debug_getRawHeader, monad_debug_getRawReceipts,
        monad_debug_getRawTransaction,
    },
    debug_trace::{
        monad_debug_traceBlockByHash, monad_debug_traceBlockByNumber, monad_debug_traceTransaction,
    },
    eth::{
        account::{
            monad_eth_getBalance, monad_eth_getCode, monad_eth_getStorageAt,
            monad_eth_getTransactionCount, monad_eth_syncing,
        },
        block::{
            monad_eth_blockNumber, monad_eth_chainId, monad_eth_getBlockByHash,
            monad_eth_getBlockByNumber, monad_eth_getBlockReceipts,
            monad_eth_getBlockTransactionCountByHash, monad_eth_getBlockTransactionCountByNumber,
        },
        call::{monad_admin_ethCallStatistics, monad_debug_traceCall, monad_eth_call},
        gas::{
            monad_eth_estimateGas, monad_eth_feeHistory, monad_eth_gasPrice,
            monad_eth_maxPriorityFeePerGas,
        },
        txn::{
            monad_eth_getLogs, monad_eth_getTransactionByBlockHashAndIndex,
            monad_eth_getTransactionByBlockNumberAndIndex, monad_eth_getTransactionByHash,
            monad_eth_getTransactionReceipt, monad_eth_sendRawTransaction,
        },
    },
    meta::{monad_net_version, monad_web3_client_version},
    resources::MonadRpcResources,
};
use crate::{
    eth_json_types::serialize_result,
    jsonrpc::{JsonRpcError, JsonRpcResultExt, Request, RequestWrapper, Response, ResponseWrapper},
    timing::RequestId,
    trace::{
        monad_trace_block, monad_trace_call, monad_trace_callMany, monad_trace_get,
        monad_trace_transaction,
    },
    vpool::{monad_txpool_statusByAddress, monad_txpool_statusByHash},
};

mod debug;
mod debug_trace;
pub mod eth;
mod meta;
pub mod resources;

pub async fn rpc_handler(
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
