mod rpc;

use std::net::SocketAddr;

use crate::rpc::{RpcRequest, RpcResponse};
use axum::{
    extract::{rejection::JsonRejection, State},
    routing::post,
    Json, Router,
};
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{event, Level};

#[derive(Clone)]
struct ServerState {
    sender: mpsc::Sender<String>,
}

pub async fn start(sender: mpsc::Sender<String>, port: u16) {
    let state = ServerState { sender };

    let app = Router::new()
        .route("/", post(root_handler))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    event!(Level::INFO, "Listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap()
}

async fn root_handler(
    State(state): State<ServerState>,
    request: Result<Json<RpcRequest>, JsonRejection>,
) -> Json<RpcResponse> {
    let request = match request {
        Ok(request) => request.0,
        Err(_) => {
            return Json(RpcResponse::error(
                -32700,
                "Parse error".to_string(),
                None,
                Value::Null,
            ));
        }
    };

    event!(Level::INFO, ?request, "Received request");

    if request.jsonrpc != "2.0" {
        return Json(RpcResponse::error(
            -32600,
            "jsonrpc must be 2.0".to_string(),
            None,
            request.id,
        ));
    }

    match request.method.as_str() {
        "add_tx" => Json(handle_add_tx(state, request).await),
        m => Json(RpcResponse::error(
            -32601,
            "Method not found".to_string(),
            Some(Value::String(m.to_string())),
            request.id,
        )),
    }
}

async fn handle_add_tx(state: ServerState, request: RpcRequest) -> RpcResponse {
    let tx_hex = match request.params {
        Value::String(s) => s,
        _ => {
            return RpcResponse::error(
                -32602,
                "Invalid params".to_string(),
                Some(Value::String("params must be a string".to_string())),
                request.id,
            );
        }
    };

    if let Err(e) = state.sender.send(tx_hex).await {
        return RpcResponse::error(
            -32603,
            "Internal error".to_string(),
            Some(Value::String(e.to_string())),
            request.id,
        );
    };

    RpcResponse::success("Submitted tx for processing".into(), request.id)
}
