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

use actix_web::{web, HttpResponse};
use monad_tracing_timing::TimingSpanExtension;
use monad_triedb_utils::triedb_env::Triedb;
use serde_json::{value::RawValue, Value};
use tracing::{debug, info, trace_span, Instrument, Span};
use tracing_actix_web::RootSpan;

use self::{
    debug::{
        monad_debug_getRawBlock, monad_debug_getRawHeader, monad_debug_getRawReceipts,
        monad_debug_getRawTransaction, monad_debug_traceBlockByHash,
        monad_debug_traceBlockByNumber, monad_debug_traceTransaction,
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
    vpool::{monad_txpool_statusByAddress, monad_txpool_statusByHash},
};

mod debug;
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
            let Ok(request) = serde_json::from_value::<Request>(json_request.clone()) else {
                return HttpResponse::Ok().json(Response::from_error(JsonRpcError::parse_error()));
            };
            root_span.record("json_method", &request.method);
            let result = rpc_select(
                &app_state,
                &request.method,
                request.params.clone(),
                request_id.clone(),
            )
            .await;
            let response = Response::from_result(request.id, result);

            if let Some(comparator) = &app_state.rpc_comparator {
                let block_number = if let Some(triedb_env) = &app_state.triedb_reader {
                    triedb_env.get_latest_voted_block_key().seq_num().0
                } else {
                    0
                };

                let comparator = comparator.clone();
                let json_request = json_request.clone();
                let response_value = serde_json::to_value(&response).unwrap_or_default();

                tokio::spawn(async move {
                    comparator
                        .submit_comparison(block_number, json_request, response_value)
                        .await;
                });
            }

            ResponseWrapper::Single(response)
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
                            return (
                                crate::jsonrpc::RequestId::Null,
                                Err(JsonRpcError::invalid_request()),
                            );
                        };
                        let (state, id, method, params) =
                            (app_state, request.id, request.method, request.params);
                        (id, rpc_select(&state, &method, params, request_id).await)
                    }
                }))
                .await
                .into_iter()
                .map(|(id, response)| Response::from_result(id, response))
                .collect::<Vec<_>>();
            ResponseWrapper::Batch(batch_response)
        }
    };

    let response_raw_value = match serde_json::value::to_raw_value(&response) {
        Err(e) => {
            debug!("response serialization error: {e}");
            return HttpResponse::Ok().json(Response::from_error(JsonRpcError::internal_error(
                format!("serialization error: {}", e),
            )));
        }
        Ok(response) => response,
    };

    if response_raw_value.get().as_bytes().len() > app_state.max_response_size as usize {
        info!("response exceed size limit: {body:?}");
        return HttpResponse::Ok().json(Response::from_error(JsonRpcError::custom(
            "response exceed size limit".to_string(),
        )));
    }

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

    HttpResponse::Ok().json(response_raw_value)
}

#[allow(non_snake_case)]
async fn admin_ethCallStatistics(
    _: RequestId,
    app_state: &MonadRpcResources,
    _params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
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

#[allow(non_snake_case)]
async fn debug_getRawBlock(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    let chain_state = app_state.chain_state.as_ref().method_not_supported()?;
    let params = serde_json::from_value(params).invalid_params()?;
    monad_debug_getRawBlock(chain_state, params)
        .await
        .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn debug_getRawHeader(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    let chain_state = app_state.chain_state.as_ref().method_not_supported()?;
    let params = serde_json::from_value(params).invalid_params()?;
    monad_debug_getRawHeader(chain_state, params)
        .await
        .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn debug_getRawReceipts(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    let chain_state = app_state.chain_state.as_ref().method_not_supported()?;
    let params = serde_json::from_value(params).invalid_params()?;
    monad_debug_getRawReceipts(chain_state, params)
        .await
        .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn debug_getRawTransaction(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    let chain_state = app_state.chain_state.as_ref().method_not_supported()?;
    let params = serde_json::from_value(params).invalid_params()?;
    monad_debug_getRawTransaction(chain_state, params)
        .await
        .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn debug_traceBlockByHash(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
    let params = serde_json::from_value(params).invalid_params()?;
    monad_debug_traceBlockByHash(triedb_env, &app_state.archive_reader, params)
        .await
        .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn debug_traceBlockByNumber(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
    let params = serde_json::from_value(params).invalid_params()?;
    monad_debug_traceBlockByNumber(triedb_env, &app_state.archive_reader, params)
        .await
        .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn debug_traceCall(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
    let Some(ref eth_call_executor) = app_state.eth_call_executor else {
        return Err(JsonRpcError::method_not_supported());
    };
    // acquire the concurrent requests permit
    let _permit = &app_state
        .rate_limiter
        .try_acquire()
        .map_err(|_| JsonRpcError::internal_error("eth_call concurrent requests limit".into()))?;

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

#[allow(non_snake_case)]
async fn debug_traceTransaction(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    let triedb_env = app_state.triedb_reader.as_ref().method_not_supported()?;
    let params = serde_json::from_value(params).invalid_params()?;
    monad_debug_traceTransaction(triedb_env, &app_state.archive_reader, params)
        .await
        .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn eth_call(
    request_id: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
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

#[allow(non_snake_case)]
async fn eth_sendRawTransaction(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    let params = serde_json::from_value(params).invalid_params()?;
    monad_eth_sendRawTransaction(
        &app_state.txpool_bridge_client,
        app_state.base_fee_per_gas.clone(),
        params,
        app_state.chain_id,
        app_state.allow_unprotected_txs,
    )
    .await
    .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn eth_getLogs(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(chain_state) = app_state.chain_state.as_ref() {
        let params = serde_json::from_value(params).invalid_params()?;
        monad_eth_getLogs(
            chain_state,
            app_state.logs_max_block_range,
            params,
            app_state.use_eth_get_logs_index,
            app_state.dry_run_get_logs_index,
            app_state.max_finalized_block_cache_len,
        )
        .await
        .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_getTransactionByHash(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(chain_state) = app_state.chain_state.as_ref() {
        let params = serde_json::from_value(params).invalid_params()?;
        monad_eth_getTransactionByHash(chain_state, params)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_getBlockByHash(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(chain_state) = &app_state.chain_state {
        let params = serde_json::from_value(params).invalid_params()?;
        monad_eth_getBlockByHash(chain_state, params)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_getBlockByNumber(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(chain_state) = &app_state.chain_state {
        let params = serde_json::from_value(params).invalid_params()?;
        monad_eth_getBlockByNumber(chain_state, params)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_getTransactionByBlockHashAndIndex(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(chain_state) = &app_state.chain_state {
        let params = serde_json::from_value(params).invalid_params()?;
        monad_eth_getTransactionByBlockHashAndIndex(chain_state, params)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_getTransactionByBlockNumberAndIndex(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(chain_state) = &app_state.chain_state {
        let params = serde_json::from_value(params).invalid_params()?;
        monad_eth_getTransactionByBlockNumberAndIndex(chain_state, params)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_getBlockTransactionCountByHash(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(chain_state) = app_state.chain_state.as_ref() {
        let params = serde_json::from_value(params).invalid_params()?;
        monad_eth_getBlockTransactionCountByHash(chain_state, params)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_getBlockTransactionCountByNumber(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(chain_state) = app_state.chain_state.as_ref() {
        let params = serde_json::from_value(params).invalid_params()?;
        monad_eth_getBlockTransactionCountByNumber(chain_state, params)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_getBalance(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(reader) = &app_state.triedb_reader {
        let params = serde_json::from_value(params).invalid_params()?;
        monad_eth_getBalance(reader, params)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_getCode(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(reader) = &app_state.triedb_reader {
        let params = serde_json::from_value(params).invalid_params()?;
        monad_eth_getCode(reader, params)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_getStorageAt(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(reader) = &app_state.triedb_reader {
        let params = serde_json::from_value(params).invalid_params()?;
        monad_eth_getStorageAt(reader, params)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_getTransactionCount(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(reader) = &app_state.triedb_reader {
        let params = serde_json::from_value(params).invalid_params()?;
        monad_eth_getTransactionCount(reader, params)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_blockNumber(
    _: RequestId,
    app_state: &MonadRpcResources,
    _params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(chain_state) = &app_state.chain_state {
        monad_eth_blockNumber(chain_state)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_chainId(
    _: RequestId,
    app_state: &MonadRpcResources,
    _params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    monad_eth_chainId(app_state.chain_id)
        .await
        .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn eth_syncing(
    _: RequestId,
    _app_state: &MonadRpcResources,
    _params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    serialize_result(monad_eth_syncing().await)
}

#[allow(non_snake_case)]
async fn eth_estimateGas(
    request_id: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
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

#[allow(non_snake_case)]
async fn eth_gasPrice(
    _: RequestId,
    app_state: &MonadRpcResources,
    _params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(chain_state) = &app_state.chain_state {
        monad_eth_gasPrice(chain_state)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_maxPriorityFeePerGas(
    _: RequestId,
    _app_state: &MonadRpcResources,
    _params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    monad_eth_maxPriorityFeePerGas()
        .await
        .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn eth_feeHistory(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    if let Some(chain_state) = &app_state.chain_state {
        let params = serde_json::from_value(params).invalid_params()?;
        monad_eth_feeHistory(chain_state, params)
            .await
            .map(serialize_result)?
    } else {
        Err(JsonRpcError::method_not_supported())
    }
}

#[allow(non_snake_case)]
async fn eth_getTransactionReceipt(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    let Some(chain_state) = &app_state.chain_state else {
        return Err(JsonRpcError::method_not_supported());
    };

    let params = serde_json::from_value(params).invalid_params()?;
    monad_eth_getTransactionReceipt(chain_state, params)
        .await
        .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn eth_getBlockReceipts(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    let chain_state = app_state.chain_state.as_ref().method_not_supported()?;
    let params = serde_json::from_value(params).invalid_params()?;
    monad_eth_getBlockReceipts(chain_state, params)
        .await
        .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn net_version(
    _: RequestId,
    app_state: &MonadRpcResources,
    _params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    monad_net_version(app_state.chain_id).map(serialize_result)?
}

#[allow(non_snake_case)]
async fn txpool_statusByHash(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    let params = serde_json::from_value(params).invalid_params()?;
    monad_txpool_statusByHash(&app_state.txpool_bridge_client, params)
        .await
        .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn txpool_statusByAddress(
    _: RequestId,
    app_state: &MonadRpcResources,
    params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    let params = serde_json::from_value(params).invalid_params()?;
    monad_txpool_statusByAddress(&app_state.txpool_bridge_client, params)
        .await
        .map(serialize_result)?
}

#[allow(non_snake_case)]
async fn web3_clientVersion(
    _: RequestId,
    _app_state: &MonadRpcResources,
    _params: Value,
) -> Result<Box<RawValue>, JsonRpcError> {
    monad_web3_client_version().map(serialize_result)?
}

macro_rules! enabled_methods {
    ($($(#[$attr:meta])* $method:ident),* $(,)?) => {

        #[derive(Debug, Clone, Copy)]
        #[allow(non_camel_case_types)]
        enum EnabledMethod {
            $(
                $(#[$attr])*
                $method,
            )*
        }

        impl TryFrom<&str> for EnabledMethod {
            type Error = JsonRpcError;

            fn try_from(method: &str) -> Result<Self, Self::Error> {
                match method {
                    $(
                        stringify!($method) => Ok(EnabledMethod::$method),
                    )*
                    _ => Err(JsonRpcError::method_not_found()),
                }
            }
        }

        impl EnabledMethod {
            fn span(&self) -> Span {
                match self {
                    $(
                        EnabledMethod::$method => trace_span!(stringify!($method)),
                    )*
                }
            }

            async fn call(
                &self,
                request_id: RequestId,
                app_state: &MonadRpcResources,
                params: Value,
            ) -> Result<Box<RawValue>, JsonRpcError> {
                match self {
                    $(
                        EnabledMethod::$method => $method(request_id, app_state, params).await,
                    )*
                }
            }
        }
    };
}

enabled_methods!(
    admin_ethCallStatistics,
    debug_getRawBlock,
    debug_getRawHeader,
    debug_getRawReceipts,
    debug_getRawTransaction,
    debug_traceBlockByHash,
    debug_traceBlockByNumber,
    debug_traceCall,
    debug_traceTransaction,
    eth_call,
    eth_sendRawTransaction,
    eth_getLogs,
    eth_getTransactionByHash,
    eth_getBlockByHash,
    eth_getBlockByNumber,
    eth_getTransactionByBlockHashAndIndex,
    eth_getTransactionByBlockNumberAndIndex,
    eth_getBlockTransactionCountByHash,
    eth_getBlockTransactionCountByNumber,
    eth_getBalance,
    eth_getCode,
    eth_getStorageAt,
    eth_getTransactionCount,
    eth_blockNumber,
    eth_chainId,
    eth_syncing,
    eth_estimateGas,
    eth_gasPrice,
    eth_maxPriorityFeePerGas,
    eth_feeHistory,
    eth_getTransactionReceipt,
    eth_getBlockReceipts,
    net_version,
    txpool_statusByHash,
    txpool_statusByAddress,
    web3_clientVersion
);

#[tracing::instrument(level = "debug", skip_all)]
pub async fn rpc_select(
    app_state: &MonadRpcResources,
    method: &str,
    params: Value,
    request_id: RequestId,
) -> Result<Box<RawValue>, JsonRpcError> {
    let method: EnabledMethod = method.try_into()?;
    let mut span = method.span();
    if let Some(metrics) = &app_state.metrics {
        span = span.with_main_timings(metrics.execution_histogram.clone());
    }
    method
        .call(request_id, app_state, params)
        .instrument(span)
        .await
}
