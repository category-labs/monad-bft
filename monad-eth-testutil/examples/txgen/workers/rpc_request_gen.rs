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

use alloy_primitives::{Address, BlockNumber, U256, U64};
use alloy_rpc_client::ReqwestClient;
use alloy_rpc_types::TransactionRequest;
use futures::{
    stream::{FuturesUnordered, SplitStream},
    SinkExt, StreamExt,
};
use tokio::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, warn};
use url::Url;

// RpcWalletSpam will send common wallet workflow requests to an rpc and websocket endpoints
pub struct RpcWalletSpam {
    rpc_client: ReqwestClient,
    ws_url: Url,
}

impl RpcWalletSpam {
    pub fn new(rpc_client: ReqwestClient, ws_url: Url) -> Self {
        Self { rpc_client, ws_url }
    }

    pub async fn run(&self) {
        // number of concurrent websocket connections
        let num_connections: usize = 4;

        let mut tasks = FuturesUnordered::new();
        for _ in 0..num_connections {
            let ws_url = self.ws_url.clone();
            let http_client = self.rpc_client.clone();
            tasks.push(tokio::spawn(async move {
                // Each connection has its own request id counter
                let mut next_id: u64 = 1;
                loop {
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
                                    if let Ok(json) =
                                        serde_json::from_str::<serde_json::Value>(&txt)
                                    {
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
                                Message::Binary(_)
                                | Message::Pong(_)
                                | Message::Close(_)
                                | Message::Frame(_) => {}
                            }
                        }
                    }

                    // 1) eth_chainId
                    let id = {
                        let v = next_id;
                        next_id += 1;
                        v
                    };
                    let (ws_chain, http_chain) = tokio::join!(
                        ws_call(
                            &mut write,
                            &mut read,
                            id,
                            "eth_chainId",
                            serde_json::json!([])
                        ),
                        async {
                            http_client
                                .request_noparams::<U64>("eth_chainId")
                                .map_resp(|res| res)
                                .await
                        }
                    );
                    match (ws_chain, http_chain) {
                        (Ok(ws_val), Ok(http_u64)) => {
                            let ws_u64: U64 =
                                serde_json::from_value(ws_val).expect("invalid ws chainId");
                            assert_eq!(ws_u64, http_u64, "eth_chainId mismatch");
                        }
                        _ => {
                            warn!("eth_chainId failed; restarting connection");
                            continue;
                        }
                    }

                    // 2) eth_blockNumber
                    let id = {
                        let v = next_id;
                        next_id += 1;
                        v
                    };
                    let (ws_bn, http_bn) = tokio::join!(
                        ws_call(
                            &mut write,
                            &mut read,
                            id,
                            "eth_blockNumber",
                            serde_json::json!([])
                        ),
                        async {
                            http_client
                                .request_noparams::<U64>("eth_blockNumber")
                                .map_resp(|res| res)
                                .await
                        }
                    );
                    let block_number: U64 = match (ws_bn, http_bn) {
                        (Ok(ws_val), Ok(http_u64)) => {
                            let _ws_u64: U64 =
                                serde_json::from_value(ws_val).expect("invalid ws blockNumber");
                            // Do not assert since blockchain might have progressed since the last request
                            http_u64
                        }
                        _ => {
                            warn!("eth_blockNumber failed; restarting connection");
                            continue;
                        }
                    };

                    // 3) eth_getBlockByNumber for the exact same block
                    let id = {
                        let v = next_id;
                        next_id += 1;
                        v
                    };
                    let params = serde_json::json!([block_number, true]);
                    let (ws_block, http_block) = tokio::join!(
                        ws_call(&mut write, &mut read, id, "eth_getBlockByNumber", params),
                        async {
                            http_client
                                .request::<_, alloy_rpc_types_eth::Block>(
                                    "eth_getBlockByNumber",
                                    (block_number, true),
                                )
                                .await
                        }
                    );
                    match (ws_block, http_block) {
                        (Ok(ws_val), Ok(http_blk)) => {
                            let mut ws_blk: alloy_rpc_types_eth::Block =
                                serde_json::from_value(ws_val).expect("invalid ws block");
                            ws_blk.header.parent_beacon_block_root = None;
                            let mut http_blk = http_blk;
                            http_blk.header.parent_beacon_block_root = None;
                            assert_eq!(ws_blk, http_blk, "eth_getBlockByNumber mismatch");
                        }
                        _ => {
                            warn!("eth_getBlockByNumber failed; restarting connection");
                            continue;
                        }
                    }

                    // 4) eth_getBalance at the same block
                    let random_addr = Address::random();
                    let id = {
                        let v = next_id;
                        next_id += 1;
                        v
                    };
                    let (ws_bal, http_bal) = tokio::join!(
                        ws_call(
                            &mut write,
                            &mut read,
                            id,
                            "eth_getBalance",
                            serde_json::json!([random_addr, block_number])
                        ),
                        async {
                            http_client
                                .request::<_, U256>("eth_getBalance", (&random_addr, block_number))
                                .await
                        }
                    );
                    match (ws_bal, http_bal) {
                        (Ok(ws_val), Ok(http_u256)) => {
                            let ws_u256: U256 =
                                serde_json::from_value(ws_val).expect("invalid ws balance");
                            assert_eq!(ws_u256, http_u256, "eth_getBalance mismatch");
                        }
                        _ => warn!("eth_getBalance failed; continuing"),
                    }

                    // 5) eth_getTransactionCount at the same block
                    let id = {
                        let v = next_id;
                        next_id += 1;
                        v
                    };
                    let (ws_nonce, http_nonce) = tokio::join!(
                        ws_call(
                            &mut write,
                            &mut read,
                            id,
                            "eth_getTransactionCount",
                            serde_json::json!([random_addr, block_number])
                        ),
                        async {
                            http_client
                                .request::<_, U256>(
                                    "eth_getTransactionCount",
                                    (&random_addr, block_number),
                                )
                                .await
                        }
                    );
                    match (ws_nonce, http_nonce) {
                        (Ok(ws_val), Ok(http_u256)) => {
                            let ws_u256: U256 =
                                serde_json::from_value(ws_val).expect("invalid ws nonce");
                            assert_eq!(ws_u256, http_u256, "eth_getTransactionCount mismatch");
                        }
                        _ => warn!("eth_getTransactionCount failed; continuing"),
                    }

                    // 6) eth_estimateGas
                    let estimate_req = TransactionRequest {
                        from: Some(random_addr),
                        to: Some(random_addr.into()),
                        value: Some(U256::from(0)).into(),
                        ..Default::default()
                    };
                    let id = {
                        let v = next_id;
                        next_id += 1;
                        v
                    };
                    let (ws_gas, http_gas) = tokio::join!(
                        ws_call(
                            &mut write,
                            &mut read,
                            id,
                            "eth_estimateGas",
                            serde_json::json!([estimate_req, block_number])
                        ),
                        async {
                            http_client
                                .request::<_, U256>("eth_estimateGas", (estimate_req, block_number))
                                .await
                        }
                    );
                    match (ws_gas, http_gas) {
                        (Ok(ws_val), Ok(http_u256)) => {
                            let ws_u256: U256 =
                                serde_json::from_value(ws_val).expect("invalid ws gas");
                            assert_eq!(ws_u256, http_u256, "eth_estimateGas mismatch");
                        }
                        (Err(_), Err(_)) => {
                            // both failed; acceptable as parity
                        }
                        _ => warn!("eth_estimateGas parity failed; continuing"),
                    }

                    // Close and immediately loop to create a fresh connection and continue spamming
                }
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

    pub async fn run(&self) {
        let client = self.rpc_client.clone();

        // Get the current tip from the rpc
        let mut tip = client
            .request_noparams::<U64>("eth_blockNumber")
            .map_resp(|res| res.to())
            .await
            .unwrap();

        let (block_sender, mut block_receiver) = tokio::sync::mpsc::channel(100);
        let rpc_client_clone = self.rpc_client.clone();
        tokio::spawn(async move {
            let client = rpc_client_clone;
            loop {
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
        tokio::spawn(async move {
            loop {
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

        loop {
            let ws_header = ws_rx.recv().await.expect("ws block not found");
            let rpc_header = block_receiver
                .recv()
                .await
                .take()
                .expect("rpc block not found")
                .header;

            if ws_header.number != rpc_header.number {
                panic!(
                    "block number mismatch websocket {} rpc {}",
                    ws_header.number, rpc_header.number
                );
            }
            debug!(
                "comparing websocket header with rpc header at height {}",
                &ws_header.number
            );
            assert_eq!(ws_header, rpc_header);
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
                            if id == json_rpc_id {
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
