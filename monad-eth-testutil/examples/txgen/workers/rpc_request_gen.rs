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

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use alloy_json_rpc::RpcError;
use alloy_primitives::{Address, BlockNumber, U256, U64};
use alloy_rpc_client::ReqwestClient;
use alloy_rpc_types::TransactionRequest;
use futures::{
    future::Future,
    stream::{FuturesUnordered, SplitStream},
    SinkExt, StreamExt,
};
use serde::de::DeserializeOwned;
use tokio::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, warn};
use url::Url;

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

// RpcWalletSpam will send common wallet workflow requests to an rpc and websocket endpoints
pub struct RpcWalletSpam {
    rpc_client: ReqwestClient,
    ws_url: Url,
    // number of concurrent websocket connections
    num_connections: usize,
}

impl RpcWalletSpam {
    pub fn new(rpc_client: ReqwestClient, ws_url: Url, num_connections: usize) -> Self {
        Self {
            rpc_client,
            ws_url,
            num_connections,
        }
    }

    pub async fn subscribe_and_compare(
        rpc_client: ReqwestClient,
        ws_url: Url,
        shutdown: Arc<AtomicBool>,
    ) {
        // Each connection has its own request id counter
        let mut next_id: u64 = 1;
        while !shutdown.load(Ordering::Relaxed) {
            // Open a websocket connection
            let (ws_stream, _) = match connect_async(&ws_url.to_string()).await {
                Ok(ok) => ok,
                Err(err) => {
                    warn!(?err, "Failed to connect websocket; retrying");
                    tokio::time::sleep(Duration::from_millis(200)).await;
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

    pub async fn run(&self, shutdown: Arc<AtomicBool>) {
        let mut tasks = FuturesUnordered::new();
        for _ in 0..self.num_connections {
            let ws_url = self.ws_url.clone();
            let http_client = self.rpc_client.clone();

            let shutdown2 = shutdown.clone();

            tasks.push(tokio::spawn(async move {
                Self::subscribe_and_compare(http_client, ws_url, shutdown2).await;
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
}

impl RpcWsCompare {
    pub fn new(rpc_client: ReqwestClient, ws_url: Url) -> Self {
        Self { rpc_client, ws_url }
    }

    pub async fn run(&self, shutdown: Arc<AtomicBool>) {
        let client = self.rpc_client.clone();

        // Get the current tip from the rpc
        let mut tip = client
            .request_noparams::<U64>("eth_blockNumber")
            .map_resp(|res| res.to())
            .await
            .unwrap();

        let (block_sender, mut block_receiver) = tokio::sync::mpsc::channel(100);
        let rpc_client_clone = self.rpc_client.clone();

        // shutdown this tokio task
        let shutdown2 = shutdown.clone();
        tokio::spawn(async move {
            let client = rpc_client_clone;
            while !shutdown2.load(Ordering::Relaxed) {
                let block = Self::get_block_by_number(&client, tip).await.unwrap();
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
        tokio::spawn(async move {
            while !shutdown3.load(Ordering::Relaxed) {
                tokio::select! {
                    Some(message) = read.next() => {
                        let message = message.expect("failed to parse message");
                        match message {
                            Message::Text(text) => {
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if let Some(result) = json.get("params").unwrap().get("result") {
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
            let ws_header = ws_rx.recv().await.expect("ws block not found");
            let rpc_header = block_receiver
                .recv()
                .await
                .take()
                .expect("rpc block not found")
                .header;

            if ws_header.number != rpc_header.number {
                error!(
                    "block number mismatch websocket {:?} rpc {:?}. stopping comparison",
                    ws_header.number, rpc_header.number
                );
                break;
            }
            if ws_header != rpc_header {
                error!(
                    "block header mismatch websocket {:?} rpc {:?}",
                    ws_header, rpc_header
                );
            }
        }
    }

    async fn get_block_by_number(
        client: &ReqwestClient,
        block_number: BlockNumber,
    ) -> Option<alloy_rpc_types_eth::Block> {
        loop {
            match client
                .request::<_, alloy_rpc_types_eth::Block>(
                    "eth_getBlockByNumber",
                    (U64::from(block_number), true),
                )
                .await
            {
                Ok(block) => return Some(block),
                Err(err) => {
                    warn!(?err, "failed to get block by number");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
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
