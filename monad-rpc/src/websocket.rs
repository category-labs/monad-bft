use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use actix::prelude::*;
use actix_http::ws::{Message as WebsocketMessage, ProtocolError};
use actix_web::{web, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use rand::Rng;
use reth_rpc_types::{
    other::OtherFields, pubsub::Params, BlockTransactions, Filter, FilteredParams, Log,
    Transaction, TransactionReceipt,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, error};

use crate::{
    eth_json_types::{serialize_result, FixedData},
    exec_update_builder::BlockUpdate,
    jsonrpc::{JsonRpcError, Request, RequestWrapper},
    MonadRpcResources,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect {}

pub async fn handler(
    req: HttpRequest,
    stream: web::Payload,
    app_state: web::Data<MonadRpcResources>,
    tx: web::Data<tokio::sync::broadcast::Sender<Box<BlockUpdate>>>,
) -> Result<HttpResponse, actix_web::Error> {
    debug!("ws_handler {:?}", &req);

    ws::start(
        WebsocketSession {
            heartbeat: Instant::now(),
            server: app_state.get_ref().clone().start(),
            subscriptions: HashMap::new(),
            tx: tx.get_ref().clone(),
        },
        &req,
        stream,
    )
}

#[derive(Deserialize)]
pub struct EthSubscribeRequest {
    kind: SubscriptionKind,
    params: Params,
}

#[derive(Serialize, Deserialize)]
pub struct EthSubscribeResponse {
    id: FixedData<32>,
}

#[derive(Deserialize)]
pub struct EthUnsubscribeRequest {
    id: FixedData<32>,
}

#[derive(Serialize, Deserialize)]
pub struct EthUnsubscribeResponse {
    result: bool,
}

// TODO: better subscription id
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct SubscriptionID(FixedData<32>);

impl SubscriptionID {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        let random_bytes: [u8; 32] = rng.gen();
        let id = FixedData::<32>(random_bytes);
        SubscriptionID(id)
    }
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
struct SubscriptionMessage {
    id: SubscriptionID,
    kind: SubscriptionKind,
    message: Box<BlockUpdate>,
    filter: Option<Filter>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SubscriptionUpdate {
    subscription: FixedData<32>,
    result: SubscriptionResult,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum SubscriptionResult {
    NewHeads(reth_rpc_types::Block),
    Logs(Vec<Log>),
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
enum SubscriptionKind {
    NewHeads,
    Logs,
}

pub struct WebsocketSession {
    pub heartbeat: Instant,
    pub server: Addr<MonadRpcResources>,
    // Maps a subscription to a handle
    subscriptions: HashMap<SubscriptionID, (SpawnHandle, usize)>,
    tx: tokio::sync::broadcast::Sender<Box<BlockUpdate>>,
}

impl WebsocketSession {
    fn heartbeat(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |actor, ctx| {
            debug!("heartbeat");
            if Instant::now().duration_since(actor.heartbeat) > CLIENT_TIMEOUT {
                debug!("client failed heartbeat, disconnecting");

                actor.server.do_send(Disconnect {});

                ctx.stop();
                return;
            }

            ctx.ping(b"");
        });
    }
}

impl Actor for WebsocketSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.heartbeat(ctx);
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        self.server.do_send(Disconnect {});
        Running::Stop
    }
}

impl Handler<SubscriptionMessage> for WebsocketSession {
    type Result = ();

    fn handle(&mut self, msg: SubscriptionMessage, ctx: &mut Self::Context) {
        debug!("Handling subscription message: {:?}", msg);

        match msg.kind {
            SubscriptionKind::NewHeads => {
                let block_header = msg.message.as_ref().clone().header;
                let txns: Vec<Transaction> = msg
                    .message
                    .as_ref()
                    .clone()
                    .txns
                    .into_iter()
                    .filter_map(|txn| txn.map(|txn| txn.header.clone()))
                    .collect();

                let body = SubscriptionUpdate {
                    subscription: msg.id.0,
                    result: SubscriptionResult::NewHeads(reth_rpc_types::Block {
                        header: block_header,
                        withdrawals: None,
                        total_difficulty: None,
                        uncles: vec![],
                        transactions: BlockTransactions::Full(txns),
                        size: None,                    // TODO
                        other: OtherFields::default(), // TODO
                    }),
                };

                if let Ok(body) = serde_json::to_value(&body) {
                    let update = crate::jsonrpc::Request::new(
                        "eth_subscription".to_string(),
                        body,
                        serde_json::Value::Null,
                    );

                    ctx.text(prepare_ws_response(&update));
                };
            }
            SubscriptionKind::Logs => {
                let block_receipts: Vec<TransactionReceipt> = msg
                    .message
                    .as_ref()
                    .clone()
                    .txns
                    .into_iter()
                    .filter_map(|txn| txn.map(|txn| txn.receipt.clone()))
                    .collect();

                let logs = if let Some(filter) = msg.filter {
                    let filtered_params = FilteredParams::new(Some(filter));
                    let receipt_logs: Vec<Log> = block_receipts
                        .into_iter()
                        .flat_map(|receipt| {
                            let logs: Vec<Log> = receipt
                                .logs
                                .into_iter()
                                .filter(|log: &Log| {
                                    !(filtered_params.filter.is_some()
                                        && (!filtered_params.filter_address(log)
                                            || !filtered_params.filter_topics(log)))
                                })
                                .collect();
                            logs
                        })
                        .collect();
                    receipt_logs
                } else {
                    let receipt_logs: Vec<Log> = block_receipts
                        .into_iter()
                        .flat_map(|receipt| receipt.logs)
                        .collect();
                    receipt_logs
                };

                let body = SubscriptionUpdate {
                    subscription: msg.id.0,
                    result: SubscriptionResult::Logs(logs),
                };

                if let Ok(body) = serde_json::to_value(&body) {
                    let update = crate::jsonrpc::Request::new(
                        "eth_subscription".to_string(),
                        body,
                        serde_json::Value::Null,
                    );

                    ctx.text(prepare_ws_response(&update));
                };
            }
        };
    }
}

fn prepare_ws_response<S: Serialize + std::fmt::Debug>(resp: &S) -> String {
    match serde_json::to_string(resp) {
        Ok(resp) => resp,
        Err(e) => {
            error!("error serializing response: {:?} for {:?}", e, resp);
            serde_json::to_string(&crate::jsonrpc::Response::from_error(
                JsonRpcError::internal_error("serializing response".to_string()),
            ))
            .expect("failed to serialize error response")
        }
    }
}

impl StreamHandler<Result<WebsocketMessage, ProtocolError>> for WebsocketSession {
    fn handle(&mut self, item: Result<WebsocketMessage, ProtocolError>, ctx: &mut Self::Context) {
        match item {
            Ok(message) => {
                debug!("StreamHandler::handle {:?}", message);
                match message {
                    WebsocketMessage::Text(body) => {
                        let request: RequestWrapper<Value> = match serde_json::from_str(&body) {
                            Ok(req) => req,
                            Err(e) => {
                                debug!("parse error: {e} {body:?}");
                                ctx.text(prepare_ws_response(
                                    &crate::jsonrpc::Response::from_error(
                                        JsonRpcError::invalid_params(),
                                    ),
                                ));
                                return;
                            }
                        };

                        let request = match request {
                            RequestWrapper::Single(req) => {
                                match serde_json::from_value::<Request>(req) {
                                    Ok(req) => req,
                                    Err(e) => {
                                        debug!("parse error: {e} {body:?}");
                                        ctx.text(prepare_ws_response(
                                            &crate::jsonrpc::Response::from_error(
                                                JsonRpcError::invalid_request(),
                                            ),
                                        ));
                                        return;
                                    }
                                }
                            }
                            _ => {
                                ctx.text(prepare_ws_response(
                                    &crate::jsonrpc::Response::from_error(
                                        JsonRpcError::invalid_params(),
                                    ),
                                ));
                                return;
                            }
                        };

                        match request.method.as_str() {
                            "eth_subscribe" => {
                                let req: EthSubscribeRequest =
                                    match serde_json::from_value(request.params) {
                                        Ok(params) => params,
                                        Err(_) => {
                                            ctx.text(prepare_ws_response(
                                                &crate::jsonrpc::Response::from_error(
                                                    JsonRpcError::invalid_params(),
                                                ),
                                            ));
                                            return;
                                        }
                                    };

                                let id = SubscriptionID::new();

                                ctx.text(prepare_ws_response(
                                    &crate::jsonrpc::Response::from_result(
                                        request.id,
                                        serialize_result(EthSubscribeResponse { id: id.0 }),
                                    ),
                                ));

                                match req.kind {
                                    SubscriptionKind::NewHeads => {
                                        let addr = ctx.address();
                                        let mut rx = self.tx.subscribe();
                                        let id = id.clone();

                                        let handle =
                                            ctx.spawn(actix::fut::wrap_future(async move {
                                                while let Ok(msg) = rx.recv().await {
                                                    let _ = addr.do_send(SubscriptionMessage {
                                                        id,
                                                        kind: SubscriptionKind::NewHeads,
                                                        message: msg,
                                                        filter: None,
                                                    });
                                                }
                                            }));

                                        self.subscriptions.insert(id, (handle, 0));
                                    }
                                    SubscriptionKind::Logs => {
                                        let filter = match req.params {
                                            Params::None => None,
                                            Params::Logs(filter) => Some(filter),
                                            _ => {
                                                ctx.text(prepare_ws_response(
                                                    &crate::jsonrpc::Response::from_error(
                                                        JsonRpcError::invalid_params(),
                                                    ),
                                                ));
                                                return;
                                            }
                                        };

                                        let addr = ctx.address();
                                        let mut rx = self.tx.subscribe();
                                        let id = id.clone();

                                        ctx.spawn(actix::fut::wrap_future(async move {
                                            while let Ok(msg) = rx.recv().await {
                                                let _ = addr.do_send(SubscriptionMessage {
                                                    id,
                                                    kind: SubscriptionKind::Logs,
                                                    message: msg,
                                                    filter: filter.clone().map(|f| *f.clone()),
                                                });
                                            }
                                        }));
                                    }
                                }
                            }
                            "eth_unsubscribe" => {
                                let params: EthUnsubscribeRequest =
                                    match serde_json::from_value(request.params) {
                                        Ok(params) => params,
                                        Err(_) => {
                                            ctx.text(prepare_ws_response(
                                                &crate::jsonrpc::Response::from_error(
                                                    JsonRpcError::invalid_params(),
                                                ),
                                            ));
                                            return;
                                        }
                                    };

                                if let Some((handle, _)) =
                                    self.subscriptions.remove(&SubscriptionID(params.id))
                                {
                                    ctx.cancel_future(handle);

                                    ctx.text(prepare_ws_response(
                                        &crate::jsonrpc::Response::from_result(
                                            request.id,
                                            serialize_result(EthUnsubscribeResponse {
                                                result: true,
                                            }),
                                        ),
                                    ));
                                } else {
                                    ctx.text(prepare_ws_response(
                                        &crate::jsonrpc::Response::from_result(
                                            request.id,
                                            serialize_result(EthUnsubscribeResponse {
                                                result: false,
                                            }),
                                        ),
                                    ));
                                }
                            }
                            _ => {
                                ctx.text(prepare_ws_response(
                                    &crate::jsonrpc::Response::from_error(
                                        JsonRpcError::method_not_supported(),
                                    ),
                                ));
                                return;
                            }
                        }
                    }
                    WebsocketMessage::Continuation(_continuation) => {}
                    WebsocketMessage::Ping(_) => {
                        debug!("received ping frame from client {:?}", ctx.address());
                    }
                    WebsocketMessage::Pong(_) => {
                        debug!("received pong frame from client {:?}", ctx.address());
                    }
                    WebsocketMessage::Close(_close) => {}
                    _ => {}
                }
            }
            Err(e) => {
                debug!("StreamHandler::handle error {:?}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actix_http::{ws, ws::Frame};
    use bytes::Bytes;
    use futures_util::{SinkExt as _, StreamExt as _};
    use monad_exec_event::{
        event::EventRingType,
        event_client::{ConnectOptions, EventProc, ImportedEventRing},
        event_reader::EventReader,
    };
    use reth_primitives::TransactionSigned;
    use serde_json::json;
    use tokio::sync::Semaphore;

    use crate::{
        create_app,
        exec_update_builder::{BlockUpdate, ExecUpdateBuilder, PollResult},
        tests::MonadRpcResourcesState,
        vpool,
        websocket::{EthSubscribeResponse, EthUnsubscribeResponse, SubscriptionUpdate},
        ExecutionLedgerPath, MonadRpcResources,
    };

    fn create_event_stream() -> ExecUpdateBuilder<'static, 'static> {
        let socket_address = "/tmp/monad_rpc_exec_event.sock";
        let basic_test_shmem_contents = include_bytes!("test_data/exec_events.shm");

        let mut test_server = crate::exec_update_builder::TestServer::create(
            socket_address,
            basic_test_shmem_contents,
        );
        test_server.prepare_accept_one();

        let options = ConnectOptions {
            socket_path: std::path::PathBuf::from(socket_address),
            timeout: libc::timeval {
                tv_sec: 1,
                tv_usec: 0,
            },
        };
        let mut event_proc = EventProc::connect(&options).unwrap();
        let imported_ring = ImportedEventRing::import(event_proc, EventRingType::Exec).unwrap();
        let mut event_reader = EventReader::new(&imported_ring);
        event_reader.last_seqno = 0;
        let mut update_builder =
            ExecUpdateBuilder::new(event_reader, imported_ring.parent.block_header_table);

        update_builder
    }

    fn create_test_server() -> (MonadRpcResourcesState, actix_test::TestServer) {
        let (ws_tx, _rx) = tokio::sync::broadcast::channel::<Box<BlockUpdate>>(10000000);

        tokio::spawn({
            let tx = ws_tx.clone();
            let mut update_builder = create_event_stream();

            let mut update_count: u32 = 0;

            let mut res = Vec::new();

            loop {
                if let PollResult::Update(update) = update_builder.poll() {
                    res.push(update);
                }
                update_count += 1;
                if update_count == 100 {
                    break;
                }
            }

            async move {
                // sleep for 1 sec
                for update in res.iter() {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    tx.send(update.clone());
                }
            }
        });

        let (ipc_sender, ipc_receiver) = flume::unbounded::<TransactionSigned>();
        let resources = MonadRpcResources {
            mempool_sender: ipc_sender.clone(),
            triedb_reader: None,
            execution_ledger_path: ExecutionLedgerPath(None),
            chain_id: 41454,
            batch_request_limit: 1000,
            max_response_size: 25_000_000,
            allow_unprotected_txs: false,
            rate_limiter: Arc::new(Semaphore::new(1000)),
            tx_pool: Arc::new(vpool::VirtualPool::new(ipc_sender, 20_000)),
        };
        (
            MonadRpcResourcesState { ipc_receiver },
            actix_test::start(move || create_app(resources.clone(), ws_tx.clone())),
        )
    }

    #[actix::test]
    async fn websocket_wait_for_ping() {
        env_logger::try_init().expect("failed to initialize logger");

        let (_, mut server) = create_test_server();

        let mut framed = server.ws_at("/ws/").await.unwrap();
        let frame = framed.next().await.unwrap().unwrap();
        assert_eq!(frame, Frame::Ping(Bytes::from_static(b"")));
        framed
            .send(ws::Message::Pong(Bytes::from_static(b"")))
            .await
            .unwrap();
    }

    #[actix::test]
    async fn websocket_eth_subscribe() {
        env_logger::try_init().expect("failed to initialize logger");

        let (_, mut server) = create_test_server();

        let mut framed = server.ws_at("/ws/").await.unwrap();

        let _frame = framed.next().await.unwrap().unwrap();
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["newHeads"],
            "id": 1
        });

        framed
            .send(ws::Message::Text(body.to_string().into()))
            .await
            .unwrap();
        let frame = framed.next().await.unwrap().unwrap();

        assert!(matches!(frame, Frame::Text(_)));
        let subscription_id = if let Frame::Text(resp) = frame {
            let resp: serde_json::Value = serde_json::from_slice(&resp).unwrap();
            let resp: crate::jsonrpc::Response = serde_json::from_value(resp).unwrap();
            let resp: EthSubscribeResponse = serde_json::from_value(resp.result.unwrap()).unwrap();
            resp.id
        } else {
            panic!("Expected a text frame");
        };

        // Receive some messages, then unsubscribe
        let mut count: usize = 0;
        loop {
            if let Some(frame) = framed.next().await {
                if let Ok(frame) = frame {
                    assert!(matches!(frame, Frame::Text(_)));
                    if let Frame::Text(update) = frame {
                        let update: serde_json::Value = serde_json::from_slice(&update).unwrap();
                        let update: crate::jsonrpc::Request =
                            serde_json::from_value(update).unwrap();
                        let update: SubscriptionUpdate =
                            serde_json::from_value(update.params).unwrap();
                        assert_eq!(update.subscription, subscription_id);
                    } else {
                        panic!("Expected a text frame");
                    };
                }
                count += 1;
            }

            if count > 2 {
                let body = json!({
                    "jsonrpc": "2.0",
                    "method": "eth_unsubscribe",
                    "params": [subscription_id],
                    "id": 1
                });

                framed
                    .send(ws::Message::Text(body.to_string().into()))
                    .await
                    .unwrap();

                let frame = framed.next().await.unwrap().unwrap();
                assert!(matches!(frame, Frame::Text(_)));
                if let Frame::Text(resp) = frame {
                    let resp: serde_json::Value = serde_json::from_slice(&resp).unwrap();
                    let resp: crate::jsonrpc::Response = serde_json::from_value(resp).unwrap();
                    let resp: EthUnsubscribeResponse =
                        serde_json::from_value(resp.result.unwrap()).unwrap();
                    assert_eq!(resp.result, true);
                } else {
                    panic!("Expected a text frame");
                };
                break;
            }
        }
    }
}
