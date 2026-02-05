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

use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use alloy_json_rpc::RpcError;
use alloy_network::TransactionResponse;
use alloy_primitives::{Address, BlockNumber, LogData, U256, U64};
use alloy_rpc_client::ReqwestClient;
use alloy_rpc_types::{BlockTransactionHashes, Filter, TransactionRequest};
use alloy_rpc_types_trace::geth::{
    GethDebugBuiltInTracerType, GethDebugTracerType, GethDebugTracingOptions, GethTrace,
};
use futures::{
    future::Future,
    stream::{self, FuturesUnordered, SplitStream},
    SinkExt, StreamExt, TryStreamExt,
};
use itertools::Itertools;
use serde::de::DeserializeOwned;
use tokio::time::{Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, warn};
use url::Url;

use crate::workers::Metrics;

const MAX_CONCURRENT_REQUESTS: usize = 10;
const MAX_CONCURRENT_INDEX_TASKS: usize = 16;
const RPC_RETRY_DELAY: Duration = Duration::from_millis(10);
const WS_RETRY_DELAY: Duration = Duration::from_millis(200);
const RPC_BATCH_SIZE: usize = 1000;

async fn ws_call(
    write: &mut futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    read: &mut futures::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
    id: u64,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let req = serde_json::json!({
        "id": id,
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
    });
    write
        .send(Message::Text(req.to_string()))
        .await
        .map_err(|e| format!("ws send error: {:?}", e))?;
    loop {
        let msg = read
            .next()
            .await
            .ok_or_else(|| "ws closed".to_string())
            .and_then(|r| r.map_err(|e| format!("ws recv error: {:?}", e)))?;
        match msg {
            Message::Text(txt) => {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&txt) {
                    if json.get("id") == Some(&serde_json::Value::from(id)) {
                        if let Some(err) = json.get("error") {
                            return Err(format!("ws error response: {}", err));
                        }
                        if let Some(result) = json.get("result") {
                            return Ok(result.clone());
                        }
                    }
                }
            }
            Message::Ping(data) => {
                // respond to ping
                let _ = write.send(Message::Pong(data)).await;
            }
            Message::Binary(_) | Message::Pong(_) | Message::Close(_) | Message::Frame(_) => {}
        }
    }
}

// Create a function that accepts two futures (e.g. websocket call and rpc request) and compares the results.
// The two futures should be run in parallel.
async fn compare_results<F1, F2, R, E>(ws_future: F1, rpc_future: F2) -> Result<(R, R), ()>
where
    R: DeserializeOwned + PartialEq + std::fmt::Debug,
    F1: Future<Output = Result<serde_json::Value, String>>,
    F2: Future<Output = Result<R, RpcError<E>>>,
{
    let (ws_result, rpc_result) = tokio::join!(ws_future, rpc_future);
    match (ws_result, rpc_result) {
        (Ok(ws_val), Ok(rpc_val)) => {
            let ws_val: R = serde_json::from_value(ws_val).expect("invalid ws value");
            Ok((ws_val, rpc_val))
        }
        _ => {
            warn!("compare_results failed; continuing");
            Err(())
        }
    }
}

#[derive(Debug)]
struct BlockIndexError {
    block_number: BlockNumber,
    error: RpcError<alloy_transport::TransportErrorKind>,
}

// RpcRequestGenerator will send common wallet workflow requests to an rpc and websocket endpoints
pub struct RpcRequestGenerator {
    rpc_client: ReqwestClient,
    ws_url: Url,
    requests_per_block: usize,
    // number of concurrent websocket connections
    num_connections: usize,
    metrics: Option<Arc<Metrics>>,
}

impl RpcRequestGenerator {
    pub fn new(
        rpc_client: ReqwestClient,
        ws_url: Url,
        requests_per_block: usize,
        num_connections: usize,
        metrics: Option<Arc<Metrics>>,
    ) -> Self {
        Self {
            rpc_client,
            ws_url,
            requests_per_block,
            num_connections,
            metrics,
        }
    }

    // FIXME: fix rpc error handling
    fn handle_rpc_error(
        rpc_error: RpcError<alloy_transport::TransportErrorKind>,
    ) -> Result<(), RpcError<alloy_transport::TransportErrorKind>> {
        match rpc_error {
            // retry on transport error
            RpcError::Transport(_) => Ok(()),
            _ => Err(rpc_error),
        }
    }

    async fn eth_block_number(
        client: &ReqwestClient,
        metrics: Option<Arc<Metrics>>,
    ) -> BlockNumber {
        loop {
            if let Some(ref m) = metrics {
                m.total_rpc_calls.fetch_add(1, Ordering::SeqCst);
            }
            match client
                .request_noparams::<U64>("eth_blockNumber")
                .map_resp(|res| res.to())
                .await
            {
                Ok(block_number) => return block_number,
                Err(err) => {
                    warn!(?err, "failed to get block number");
                    tokio::time::sleep(RPC_RETRY_DELAY).await;
                    continue;
                }
            }
        }
    }

    async fn get_block_by_number(
        client: &ReqwestClient,
        block_number: BlockNumber,
        metrics: Option<Arc<Metrics>>,
    ) -> Result<alloy_rpc_types_eth::Block, BlockIndexError> {
        loop {
            if let Some(ref m) = metrics {
                m.total_rpc_calls.fetch_add(1, Ordering::SeqCst);
            }
            match client
                .request::<_, alloy_rpc_types_eth::Block>(
                    "eth_getBlockByNumber",
                    (U64::from(block_number), true),
                )
                .await
            {
                Ok(block) => return Ok(block),
                Err(err) => {
                    Self::handle_rpc_error(err).map_err(|e| BlockIndexError {
                        block_number,
                        error: e,
                    })?;
                    tokio::time::sleep(RPC_RETRY_DELAY).await;
                    continue;
                }
            }
        }
    }

    async fn get_block_receipts(
        client: &ReqwestClient,
        block_number: BlockNumber,
        metrics: Option<Arc<Metrics>>,
    ) -> Result<
        Vec<alloy_rpc_types_eth::TransactionReceipt>,
        RpcError<alloy_transport::TransportErrorKind>,
    > {
        loop {
            if let Some(ref m) = metrics {
                m.total_rpc_calls.fetch_add(1, Ordering::SeqCst);
            }
            let resp = client
                .request::<_, Vec<alloy_rpc_types_eth::TransactionReceipt>>(
                    "eth_getBlockReceipts",
                    (U64::from(block_number),),
                )
                .await;
            match resp {
                Ok(_) => return resp,
                Err(err) => {
                    Self::handle_rpc_error(err)?;
                    tokio::time::sleep(RPC_RETRY_DELAY).await;
                    continue;
                }
            }
        }
    }

    async fn debug_trace_block_by_number(
        client: &ReqwestClient,
        block_number: BlockNumber,
        tracer: GethDebugBuiltInTracerType,
        metrics: Option<Arc<Metrics>>,
    ) -> Result<
        Vec<alloy_rpc_types_trace::geth::GethTrace>,
        RpcError<alloy_transport::TransportErrorKind>,
    > {
        loop {
            if let Some(ref m) = metrics {
                m.total_rpc_calls.fetch_add(1, Ordering::SeqCst);
            }
            let resp = client
                .request::<_, Vec<alloy_rpc_types_trace::geth::GethTrace>>(
                    "debug_traceBlockByNumber",
                    (
                        U64::from(block_number),
                        GethDebugTracingOptions {
                            tracer: Some(GethDebugTracerType::BuiltInTracer(tracer)),
                            ..Default::default()
                        },
                    ),
                )
                .await;
            match resp {
                Ok(_) => return resp,
                Err(err) => {
                    Self::handle_rpc_error(err)?;
                    tokio::time::sleep(RPC_RETRY_DELAY).await;
                    continue;
                }
            }
        }
    }

    async fn get_logs(
        client: &ReqwestClient,
        block_number: BlockNumber,
        metrics: Option<Arc<Metrics>>,
    ) -> Result<Vec<alloy_rpc_types_eth::Log<LogData>>, RpcError<alloy_transport::TransportErrorKind>>
    {
        loop {
            if let Some(ref m) = metrics {
                m.total_rpc_calls.fetch_add(1, Ordering::SeqCst);
                m.logs_rpc_calls.fetch_add(1, Ordering::SeqCst);
            }
            let filter = Filter::new();
            let filter = filter.from_block(block_number);
            let filter = filter.to_block(block_number);
            let resp = client
                .request::<_, Vec<alloy_rpc_types_eth::Log<LogData>>>("eth_getLogs", (filter,))
                .await;
            match resp {
                Ok(_) => return resp,
                Err(err) => {
                    if let Some(ref m) = metrics {
                        m.logs_rpc_calls_error.fetch_add(1, Ordering::SeqCst);
                    }
                    Self::handle_rpc_error(err)?;
                    tokio::time::sleep(RPC_RETRY_DELAY).await;
                    continue;
                }
            }
        }
    }

    async fn get_balances(
        client: &ReqwestClient,
        block_number: BlockNumber,
        addrs: Vec<Address>,
        metrics: Option<Arc<Metrics>>,
    ) -> Result<(), RpcError<alloy_transport::TransportErrorKind>> {
        for chunk in addrs.chunks(RPC_BATCH_SIZE) {
            let futs = loop {
                if let Some(ref m) = metrics {
                    m.total_rpc_calls.fetch_add(1, Ordering::SeqCst);
                }
                let mut batch = client.new_batch();
                let futs = chunk
                    .iter()
                    .map(|addr| {
                        let params = (addr, U64::from(block_number));
                        batch
                            .add_call::<_, U256>("eth_getBalance", &params)
                            .unwrap()
                    })
                    .collect::<Vec<_>>();

                let resp = batch.send().await;
                match resp {
                    Ok(_) => break futs,
                    Err(err) => {
                        debug!(?err, "eth_getBalance error");
                        tokio::time::sleep(RPC_RETRY_DELAY).await;
                        continue;
                    }
                }
            };

            for fut in futs {
                fut.await?;
            }
        }
        Ok(())
    }

    async fn debug_trace_transaction<T: TransactionResponse>(
        client: &ReqwestClient,
        txn_hashes: BlockTransactionHashes<'_, T>,
        metrics: Option<Arc<Metrics>>,
    ) -> Result<(), RpcError<alloy_transport::TransportErrorKind>> {
        let txn_hashes = txn_hashes.into_iter().collect::<Vec<_>>();
        for chunk in txn_hashes.chunks(RPC_BATCH_SIZE) {
            let futs = loop {
                if let Some(ref m) = metrics {
                    m.total_rpc_calls.fetch_add(1, Ordering::SeqCst);
                }
                let mut batch = client.new_batch();

                let futs = chunk
                    .iter()
                    .map(|txn| {
                        let config = GethDebugTracingOptions {
                            tracer: Some(GethDebugTracerType::BuiltInTracer(
                                GethDebugBuiltInTracerType::CallTracer,
                            )),
                            ..Default::default()
                        };
                        let params = (txn, config);
                        batch
                            .add_call::<_, GethTrace>("debug_traceTransaction", &params)
                            .unwrap()
                    })
                    .collect::<Vec<_>>();

                let resp = batch.send().await;
                match resp {
                    Ok(_) => break futs,
                    Err(err) => {
                        Self::handle_rpc_error(err)?;
                        tokio::time::sleep(RPC_RETRY_DELAY).await;
                        continue;
                    }
                }
            };

            for fut in futs {
                fut.await?;
            }
        }
        Ok(())
    }

    // Call rpc using common indexer workflow requests.
    async fn index_block(
        client: ReqwestClient,
        block_number: BlockNumber,
        requests_per_block: usize,
        result_sender: tokio::sync::mpsc::Sender<
            Result<(BlockNumber, Vec<Address>), BlockIndexError>,
        >,
        metrics: Option<Arc<Metrics>>,
    ) {
        let start = Instant::now();

        let block = match Self::get_block_by_number(&client, block_number, metrics.clone()).await {
            Ok(header) => {
                header
            },
            Err(err) => {
                warn!(?err, "Failed to get block by number");
                return;
            }
        };

        let uniq_addrs = if !block.transactions.is_empty() {
            // generate requests for block receipts and traces
            let start = Instant::now();
            let (receipts_results, _, _, _) = tokio::join!(
                async {
                    let receipts: Result<Vec<_>, _> = stream::iter(0..requests_per_block)
                        .map(|_| {
                            Self::get_block_receipts(&client, block_number, metrics.clone())
                        })
                        .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                        .try_collect()
                        .await;

                    match receipts {
                        Ok(mut r) => r.pop().ok_or(()),
                        Err(err) => {
                            let _ = result_sender
                                .send(Err(BlockIndexError {
                                    block_number,
                                    error: err,
                                }))
                                .await;
                            Err(())
                        }
                    }
                },
                async {
                    let logs: Result<Vec<_>, _> = stream::iter(0..requests_per_block)
                        .map(|_| {
                            Self::get_logs(&client, block_number, metrics.clone())
                        })
                        .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                        .try_collect()
                        .await;

                    match logs {
                        Ok(mut l) => l.pop().ok_or(()),
                        Err(err) => {
                            let _ = result_sender
                                .send(Err(BlockIndexError {
                                    block_number,
                                    error: err,
                                }))
                                .await;
                            Err(())
                        }
                    }
                },
                async {
                    let traces: Result<Vec<_>, _> = stream::iter(0..requests_per_block)
                        .map(|_| {
                            Self::debug_trace_block_by_number(
                                &client,
                                block_number,
                                GethDebugBuiltInTracerType::CallTracer,
                                metrics.clone(),
                            )
                        })
                        .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                        .try_collect()
                        .await;

                    match traces {
                        Ok(mut t) => t.pop().ok_or(()),
                        Err(err) => {
                            let _ = result_sender
                                .send(Err(BlockIndexError {
                                    block_number,
                                    error: err,
                                }))
                                .await;
                            Err(())
                        }
                    }
                },
                async {
                    let traces: Result<Vec<_>, _> = stream::iter(0..requests_per_block)
                        .map(|_| {
                            Self::debug_trace_block_by_number(
                                &client,
                                block_number,
                                GethDebugBuiltInTracerType::PreStateTracer,
                                metrics.clone(),
                            )
                        })
                        .buffer_unordered(MAX_CONCURRENT_REQUESTS)
                        .try_collect()
                        .await;

                    match traces {
                        Ok(mut t) => t.pop().ok_or(()),
                        Err(err) => {
                            let _ = result_sender
                                .send(Err(BlockIndexError {
                                    block_number,
                                    error: err,
                                }))
                                .await;
                            Err(())
                        }
                    }
                },
            );
            let receipts = match receipts_results {
                Ok(r) => r,
                Err(_) => {
                    warn!(?block_number, "Unable to retrieve block receipts");
                    return;
                }
            };
            let txn_hashes = block.transactions.hashes();
            let duration = start.elapsed();
            debug!(
                ?block_number,
                ?duration,
                "eth_getBlockReceipts, eth_getLogs and debug_traceBlockByNumber duration"
            );
            // account balances and eth call
            let start = Instant::now();
            let addrs = receipts
                .into_iter()
                .map(|receipt| receipt.from)
                .collect::<Vec<_>>();
            debug!(n = addrs.len(), "reading account balances");
            let (balances_res, trace_res) = tokio::join!(
                Self::get_balances(&client, block_number, addrs.clone(), metrics.clone()),
                Self::debug_trace_transaction(&client, txn_hashes, metrics.clone()),
            );
            if let Err(ref err) = balances_res {
                warn!(?block_number, ?err, "Error fetching balances");
            }
            if let Err(ref err) = trace_res {
                warn!(?block_number, ?err, "Error tracing transaction");
            }
            let duration = start.elapsed();
            let num_addr = addrs.len();
            debug!(
                ?block_number,
                ?duration,
                ?num_addr,
                "eth_getBalance and debug_traceTransaction"
            );
            addrs.into_iter().unique().collect()
        } else {
            Vec::new()
        };

        let duration = start.elapsed();

        if let Some(ref m) = metrics {
            m.indexer_blocks_indexed.fetch_add(1, Ordering::SeqCst);
            let mut total_duration = m.indexer_total_duration_ms.write().unwrap();
            *total_duration += duration.as_millis() as u64;
        }

        if result_sender
            .send(Ok((block_number, uniq_addrs)))
            .await
            .is_err()
        {
            warn!(?block_number, "Failed to send block index result");
        }
    }

    // Call rpc and rpc on a websocket connection using common wallet workflow requests.
    pub async fn subscribe_and_compare(
        rpc_client: ReqwestClient,
        ws_url: Url,
        shutdown: Arc<AtomicBool>,
        metrics: Option<Arc<Metrics>>,
    ) {
        // Each connection has its own request id counter
        let mut next_id: u64 = 1;
        let mut prev_id: u64 = 1;

        while !shutdown.load(Ordering::Relaxed) {
            // Add metrics from previous iteration
            let messages_in_prev_iteration = next_id - prev_id;
            if let Some(ref m) = metrics {
                m.ws_messages_sent
                    .fetch_add(messages_in_prev_iteration as usize, Ordering::SeqCst);
                m.ws_messages_received
                    .fetch_add(messages_in_prev_iteration as usize, Ordering::SeqCst);
                m.wallet_comparison_attempts
                    .fetch_add(messages_in_prev_iteration as usize, Ordering::SeqCst);
            }
            prev_id = next_id;

            // Open a websocket connection
            let (ws_stream, _) = match connect_async(&ws_url.to_string()).await {
                Ok(ok) => {
                    ok
                },
                Err(err) => {
                    warn!(?err, "Failed to connect websocket; retrying");
                    tokio::time::sleep(WS_RETRY_DELAY).await;
                    continue;
                }
            };
            let (mut write, mut read) = ws_stream.split();

            // 1) eth_chainId
            next_id += 1;
            match compare_results(
                ws_call(
                    &mut write,
                    &mut read,
                    next_id,
                    "eth_chainId",
                    serde_json::json!([]),
                ),
                async { rpc_client.request_noparams::<U64>("eth_chainId").await },
            )
            .await
            {
                Ok((ws_val, rpc_val)) => {
                    if ws_val != rpc_val {
                        warn!("eth_chainId mismatch; ws: {:?}, rpc: {:?}", ws_val, rpc_val);
                        if let Some(ref m) = metrics {
                            m.wallet_comparison_mismatches.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
                Err(_) => {
                    warn!("eth_chainId failed; restarting connection");
                    continue;
                }
            }

            // 2) eth_blockNumber
            next_id += 1;
            let block_number = match compare_results(
                ws_call(
                    &mut write,
                    &mut read,
                    next_id,
                    "eth_blockNumber",
                    serde_json::json!([]),
                ),
                async { rpc_client.request_noparams::<U64>("eth_blockNumber").await },
            )
            .await
            {
                Ok((ws_val, rpc_val)) => {
                    // Websocket can return a block number that is 1 higher than the rpc.
                    // Compare the results and assert that the difference is at most 1.
                    if ws_val != rpc_val && ws_val != rpc_val + U64::ONE {
                        warn!(
                            "eth_blockNumber mismatch; ws: {:?}, rpc: {:?}",
                            ws_val, rpc_val
                        );
                        if let Some(ref m) = metrics {
                            m.wallet_comparison_mismatches.fetch_add(1, Ordering::SeqCst);
                        }
                        continue;
                    }
                    rpc_val
                }
                Err(_) => {
                    warn!("eth_blockNumber failed; restarting connection");
                    continue;
                }
            };

            // 3) eth_getBlockByNumber for the exact same block
            next_id += 1;
            match compare_results(
                ws_call(
                    &mut write,
                    &mut read,
                    next_id,
                    "eth_getBlockByNumber",
                    serde_json::json!([block_number, true]),
                ),
                async {
                    rpc_client
                        .request::<_, alloy_rpc_types_eth::Block>(
                            "eth_getBlockByNumber",
                            (block_number, true),
                        )
                        .await
                },
            )
            .await
            {
                Ok((ws_val, rpc_val)) => {
                    if ws_val != rpc_val {
                        warn!(
                            "eth_getBlockByNumber mismatch; ws: {:?}, rpc: {:?}",
                            ws_val, rpc_val
                        );
                        if let Some(ref m) = metrics {
                            m.wallet_comparison_mismatches.fetch_add(1, Ordering::SeqCst);
                        }
                        continue;
                    }
                }
                Err(_) => {
                    warn!("eth_getBlockByNumber failed; restarting connection");
                    continue;
                }
            }

            // 4) eth_getBalance at the same block
            next_id += 1;
            let random_addr = Address::random();
            match compare_results(
                ws_call(
                    &mut write,
                    &mut read,
                    next_id,
                    "eth_getBalance",
                    serde_json::json!([random_addr, block_number]),
                ),
                async {
                    rpc_client
                        .request::<_, U256>("eth_getBalance", (&random_addr, block_number))
                        .await
                },
            )
            .await
            {
                Ok((ws_val, rpc_val)) => {
                    if ws_val != rpc_val {
                        warn!(
                            "eth_getBalance mismatch; ws: {:?}, rpc: {:?}",
                            ws_val, rpc_val
                        );
                        if let Some(ref m) = metrics {
                            m.wallet_comparison_mismatches.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
                Err(_) => {
                    warn!("eth_getBalance failed; restarting connection");
                    continue;
                }
            }

            // 5) eth_getTransactionCount at the same block
            next_id += 1;
            match compare_results(
                ws_call(
                    &mut write,
                    &mut read,
                    next_id,
                    "eth_getTransactionCount",
                    serde_json::json!([random_addr, block_number]),
                ),
                async {
                    rpc_client
                        .request::<_, U256>("eth_getTransactionCount", (&random_addr, block_number))
                        .await
                },
            )
            .await
            {
                Ok((ws_val, rpc_val)) => {
                    if ws_val != rpc_val {
                        warn!(
                            "eth_getTransactionCount mismatch; ws: {:?}, rpc: {:?}",
                            ws_val, rpc_val
                        );
                        if let Some(ref m) = metrics {
                            m.wallet_comparison_mismatches.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
                Err(_) => {
                    warn!("eth_getTransactionCount failed; restarting connection");
                    continue;
                }
            }

            // 6) eth_estimateGas
            let estimate_req = TransactionRequest {
                from: Some(random_addr),
                to: Some(random_addr.into()),
                value: Some(U256::from(0)),
                ..Default::default()
            };
            next_id += 1;
            match compare_results(
                ws_call(
                    &mut write,
                    &mut read,
                    next_id,
                    "eth_estimateGas",
                    serde_json::json!([estimate_req, block_number]),
                ),
                async {
                    rpc_client
                        .request::<_, U256>("eth_estimateGas", (estimate_req, block_number))
                        .await
                },
            )
            .await
            {
                Ok((ws_val, rpc_val)) => {
                    if ws_val != rpc_val {
                        warn!(
                            "eth_estimateGas mismatch; ws: {:?}, rpc: {:?}",
                            ws_val, rpc_val
                        );
                        if let Some(ref m) = metrics {
                            m.wallet_comparison_mismatches.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
                Err(_) => {
                    warn!("eth_estimateGas failed; restarting connection");
                    continue;
                }
            }

            if let Err(err) = write.send(Message::Close(None)).await {
                warn!(?err, "failed to send close message");
            }
        }
    }

    pub async fn run_indexer_workflow(&self, shutdown: Arc<AtomicBool>) {
        // start block tip refresher task
        let (tip_sender, mut tip_receiver) = tokio::sync::mpsc::channel(8);
        let (index_done_sender, mut index_done_receiver) = tokio::sync::mpsc::channel(32);
        let refresher = TipRefresher::new(self.rpc_client.clone(), tip_sender, self.metrics.clone());
        tokio::spawn(async move { refresher.run().await });

        // Semaphore to limit concurrent indexing tasks
        let index_semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_INDEX_TASKS));

        let mut status_interval = tokio::time::interval(Duration::from_secs(1));
        status_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut total_indexed = 0;
        let mut total_failed = 0;
        let mut uniq_addrs = BTreeSet::new();

        // select on tip refresher channel and completion channel
        while !shutdown.load(Ordering::Relaxed) {
            tokio::select! {
                Some((index_from, index_to)) = tip_receiver.recv() => {
                    debug!(from=index_from, to=index_to, "index range");

                    for block_number in index_from..=index_to {
                        // Wait for permit before spawning - limits concurrent tasks
                        let permit = index_semaphore.clone().acquire_owned().await.unwrap();
                        let client = self.rpc_client.clone();
                        let requests_per_block = self.requests_per_block;
                        let sender = index_done_sender.clone();
                        let metrics = self.metrics.clone();
                        tokio::spawn(async move {
                            RpcRequestGenerator::index_block(client, block_number, requests_per_block, sender, metrics).await;
                            drop(permit);
                        });
                    }
                }

                Some(index_result) = index_done_receiver.recv() => {
                    let _block_number = match index_result {
                        Ok((num, addrs)) => {
                            total_indexed += 1;
                            uniq_addrs.extend(addrs.into_iter());
                            num
                        },
                        Err(err) => {
                            total_failed += 1;
                            if let Some(ref m) = self.metrics {
                                m.indexer_blocks_failed.fetch_add(1, Ordering::SeqCst);
                            }
                            warn!(?err.block_number, ?err.error,"failed to index block");
                            err.block_number
                        }
                    };
                }

                _ = status_interval.tick() => {
                    debug!(indexed=total_indexed, failed=total_failed, addrs=uniq_addrs.len(), "indexer status");
                }
            }
        }
    }

    pub async fn run_wallet_workflow(&self, shutdown: Arc<AtomicBool>) {
        let mut tasks = FuturesUnordered::new();
        for _ in 0..self.num_connections {
            let ws_url = self.ws_url.clone();
            let http_client = self.rpc_client.clone();
            let metrics = self.metrics.clone();

            let shutdown2 = shutdown.clone();

            tasks.push(tokio::spawn(async move {
                Self::subscribe_and_compare(http_client, ws_url, shutdown2, metrics).await;
            }));
        }
        while let Some(res) = tasks.next().await {
            if let Err(err) = res {
                warn!(?err, "connection task failed");
            }
        }
    }
}

// RpcWsCompare compares results between an rpc and websocket endpoint
pub struct RpcWsCompare {
    rpc_client: ReqwestClient,
    ws_url: Url,
    metrics: Option<Arc<Metrics>>,
}

impl RpcWsCompare {
    pub fn new(rpc_client: ReqwestClient, ws_url: Url, metrics: Option<Arc<Metrics>>) -> Self {
        Self { rpc_client, ws_url, metrics }
    }

    pub async fn run(&self, shutdown: Arc<AtomicBool>) {
        let client = self.rpc_client.clone();

        // Get the current tip from the rpc
        let mut tip = RpcRequestGenerator::eth_block_number(&client, self.metrics.clone()).await;

        let (block_sender, mut block_receiver) = tokio::sync::mpsc::channel(100);
        let rpc_client_clone = self.rpc_client.clone();

        // shutdown this tokio task
        let shutdown2 = shutdown.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let client = rpc_client_clone;
            while !shutdown2.load(Ordering::Relaxed) {
                let block = RpcRequestGenerator::get_block_by_number(&client, tip, metrics.clone()).await.unwrap();
                block_sender.send(block).await.unwrap();
                tip += 1;
                tokio::time::sleep(Duration::from_millis(500)).await; // Add a small delay
            }
        });

        // Create a websocket stream to listen to new blocks
        let (ws_stream, _) = connect_async(&self.ws_url.to_string())
            .await
            .expect("Failed to connect");
        let (mut write, mut read) = ws_stream.split();
        write.send(Message::Text("{ \"id\": 1, \"jsonrpc\": \"2.0\", \"method\": \"eth_subscribe\", \"params\": [\"newHeads\"] }".into())).await.expect("failed to send message");
        Self::wait_for_subscription_id(&mut read, 1)
            .await
            .expect("failed to get newHeads subscription ID");

        let (ws_tx, mut ws_rx) = tokio::sync::mpsc::channel(100);
        let shutdown3 = shutdown.clone();
        let metrics_clone = self.metrics.clone();
        tokio::spawn(async move {
            while !shutdown3.load(Ordering::Relaxed) {
                tokio::select! {
                    Some(message) = read.next() => {
                        let message = message.expect("failed to parse message");
                        match message {
                            Message::Text(text) => {
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if let Some(result) = json.get("params").unwrap().get("result") {
                                        if let Some(ref m) = metrics_clone {
                                            m.ws_messages_received.fetch_add(1, Ordering::SeqCst);
                                        }
                                        // Convert result to alloy_rpc_types_eth::Header
                                        let header = serde_json::from_value::<alloy_rpc_types_eth::Header>(result.clone()).expect("failed to convert result to alloy_rpc_types_eth::Header");
                                        ws_tx.send(header).await.unwrap();
                                    }
                                }
                            }
                            Message::Binary(_) => {}
                            Message::Ping(data) => {
                                write.send(Message::Pong(data)).await.expect("failed to send message");
                            }
                            Message::Pong(_) => {}
                            Message::Close(frame) => {
                                panic!("Received close message: {:?}", frame);
                            }
                            Message::Frame(_) => {}
                        }
                    }
                }
            }
        });

        while !shutdown.load(Ordering::Relaxed) {
            let ws_header = match ws_rx.recv().await {
                Some(header) => header,
                None => break,
            };
            let rpc_header = match block_receiver.recv().await {
                Some(block) => block.header,
                None => break,
            };

            if let Some(ref m) = self.metrics {
                m.rpcws_blocks_compared.fetch_add(1, Ordering::SeqCst);
            }

            if ws_header.number != rpc_header.number {
                error!(
                    "block number mismatch websocket {:?} rpc {:?}. stopping comparison",
                    ws_header.number, rpc_header.number
                );
                if let Some(ref m) = self.metrics {
                    m.rpcws_block_number_mismatches.fetch_add(1, Ordering::SeqCst);
                }
                break;
            }
            if ws_header != rpc_header {
                error!(
                    "block header mismatch websocket {:?} rpc {:?}",
                    ws_header, rpc_header
                );
                if let Some(ref m) = self.metrics {
                    m.rpcws_header_mismatches.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }

    async fn wait_for_subscription_id(
        read: &mut SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
        json_rpc_id: u32,
    ) -> Option<String> {
        while let Some(message) = read.next().await {
            let message = message.expect("failed to parse message");
            match message {
                Message::Text(text) => {
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                        if let Some(id) = json.get("id") {
                            if id.as_u64() == Some(json_rpc_id as u64) {
                                if let Some(result) = json.get("result") {
                                    if let Some(sub_id) = result.as_str() {
                                        return Some(sub_id.to_string());
                                    }
                                }
                                if let Some(error) = json.get("error") {
                                    error!("Error in subscription response: {}", error);
                                    return None;
                                }
                            }
                        }
                    }
                }
                Message::Ping(_) => {
                    // We don't have access to the write half here, so we can't respond to pings
                    warn!("Received ping while waiting for subscription ID");
                }
                _ => {}
            }
        }
        None
    }
}

struct TipRefresher {
    tip: BlockNumber,
    tip_sender: tokio::sync::mpsc::Sender<(BlockNumber, BlockNumber)>,
    client: ReqwestClient,
    metrics: Option<Arc<Metrics>>,
}

impl TipRefresher {
    fn new(
        client: ReqwestClient,
        sender: tokio::sync::mpsc::Sender<(BlockNumber, BlockNumber)>,
        metrics: Option<Arc<Metrics>>,
    ) -> Self {
        Self {
            tip: 0,
            tip_sender: sender,
            client,
            metrics,
        }
    }

    async fn run(mut self) {
        loop {
            let tip = RpcRequestGenerator::eth_block_number(&self.client, self.metrics.clone()).await;

            if self.tip == 0 {
                self.tip = tip;
            } else if tip > self.tip {
                let Ok(()) = self.tip_sender.send((self.tip + 1, tip)).await else {
                    warn!("tip sender channel full");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                };
                self.tip = tip;
            } else {
                tokio::time::sleep(RPC_RETRY_DELAY).await;
            }
        }
    }
}
