use std::{
    collections::HashMap,
    pin::pin,
    time::{Duration, Instant},
};

use actix_http::ws;
use actix_web::{web, HttpRequest, HttpResponse};
use actix_ws::{AggregatedMessage, CloseReason};
use alloy_rpc_types::eth::{pubsub::Params, Filter, FilteredParams};
use futures::StreamExt;
use itertools::Itertools;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, error, warn};

use crate::{
    eth_json_types::{
        serialize_result, BlockCommitState, EthSubscribeRequest, EthSubscribeResult,
        EthUnsubscribeRequest, FixedData, SpeculativeLog, SpeculativeNewHead, SubscriptionKind,
        SubscriptionResult,
    },
    jsonrpc::{JsonRpcError, Request, RequestWrapper},
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Deserialize, Serialize)]
pub struct SubscriptionId(pub FixedData<16>);

#[derive(Clone)]
pub struct WebSocketServerHandle {
    pub tx: tokio::sync::broadcast::Sender<crate::websocket_server::Event>,
}

pub async fn ws_handler(
    req: HttpRequest,
    stream: web::Payload,
    server: web::Data<WebSocketServerHandle>,
) -> Result<HttpResponse, actix_web::Error> {
    let (res, session, msg_stream) = actix_ws::handle(&req, stream)?;
    actix_rt::spawn(handler(session, msg_stream, server.tx.subscribe()));
    Ok(res)
}

async fn handler(
    mut session: actix_ws::Session,
    msg_stream: actix_ws::MessageStream,
    rx: tokio::sync::broadcast::Receiver<crate::websocket_server::Event>,
) {
    debug!("new websocket connection started");
    let mut last_heartbeat = Instant::now();
    let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);

    let msg_stream = msg_stream
        .max_frame_size(128 * 1024)
        .aggregate_continuations()
        .max_continuation_size(2 * 1024 * 1024);

    let mut msg_stream = pin!(msg_stream);
    let mut broadcast_rx = pin!(rx);

    let mut subscriptions: HashMap<SubscriptionKind, Vec<(SubscriptionId, Option<Filter>)>> =
        HashMap::new();

    let close_reason = loop {
        tokio::select! {
            _ = interval.tick() => {
                if Instant::now().duration_since(last_heartbeat) > CLIENT_TIMEOUT {
                    break None;
                }

                if let Err(err) = session.ping(b"").await {
                    warn!("ws handler ping error: {err:?}");
                }
            }
            msg = msg_stream.next() => {
                match msg {
                    Some(Ok(AggregatedMessage::Ping(bytes))) => {
                        last_heartbeat = Instant::now();
                        if let Err(err) = session.pong(&bytes).await {
                            warn!("ws handler pong error: {err:?}");
                        }
                    }
                    Some(Ok(AggregatedMessage::Pong(_))) => {
                        last_heartbeat = Instant::now();
                    }
                    Some(Ok(AggregatedMessage::Text(body))) => {
                        last_heartbeat = Instant::now();
                        let request = to_request::<Request>(&body);
                        match request {
                            Ok(req) => {
                                handle_request(&mut session, &mut subscriptions, req).await;
                            }
                            Err(e) => {
                                if let Err(err) = session
                                    .text(to_response(&crate::jsonrpc::Response::from_error(e)))
                                    .await {
                                    warn!("ws handler AggregatedMessage text error: {err:?}");
                                }
                            }
                        };
                    }
                    Some(Ok(AggregatedMessage::Binary(body))) => {
                        last_heartbeat = Instant::now();
                        let request = to_request::<Request>(&body);
                        match request {
                            Ok(req) => {
                                handle_request(&mut session, &mut subscriptions, req).await;
                            }
                            Err(e) => {
                                if let Err(err) = session
                                    .binary(to_response(&crate::jsonrpc::Response::from_error(e)))
                                    .await {
                                    warn!("ws handler AggregatedMessage binary error: {err:?}");
                                }
                            }
                        };
                    }
                    Some(Ok(AggregatedMessage::Close(reason))) => {
                        break reason;
                    }
                    Some(Err(err)) => {
                        error!("error in websocket stream: {:?}", err);
                        break None;
                    }
                    None => {
                        break None;
                    }
                }
            }
            cmd = broadcast_rx.recv() => {
                match cmd {
                    Ok(msg) => {
                        handle_notification(&mut session, &subscriptions, msg).await;
                    }
                    Err(err) => {
                        warn!("received error from websocket server: {err:?}");
                        break Some(CloseReason { code: ws::CloseCode::Error, description: Some("websocket server is not running".to_string()) })
                    }
                }
            }
        };
    };

    if let Err(err) = session.close(close_reason).await {
        warn!("ws handler close error: {err:?}");
    }
}

async fn handle_notification(
    mut session: &mut actix_ws::Session,
    subscriptions: &HashMap<SubscriptionKind, Vec<(SubscriptionId, Option<Filter>)>>,
    msg: crate::websocket_server::Event,
) {
    match msg {
        crate::websocket_server::Event::ProcessedBlock {
            header,
            commit_state: BlockCommitState::Finalized,
            ..
        } if subscriptions.contains_key(&SubscriptionKind::NewHeads) => {
            match serde_json::to_value(SubscriptionResult::NewHeads(header)) {
                Ok(result) => {
                    if let Some(subs) = subscriptions.get(&SubscriptionKind::NewHeads) {
                        for (id, _) in subs.iter() {
                            send_notification(&mut session, id.clone(), result.clone()).await;
                        }
                    }
                }
                Err(err) => {
                    error!("error serializing NewHeads to JSON: {err:?}");
                }
            }
        }
        crate::websocket_server::Event::ProcessedBlock {
            header,
            block_id,
            commit_state,
        } if subscriptions.contains_key(&SubscriptionKind::MonadNewHeads) => {
            match serde_json::to_value(SubscriptionResult::SpeculativeNewHeads(
                SpeculativeNewHead {
                    header,
                    block_id,
                    commit_state,
                },
            )) {
                Ok(result) => {
                    if let Some(subs) = subscriptions.get(&SubscriptionKind::MonadNewHeads) {
                        for (id, _) in subs.iter() {
                            send_notification(&mut session, id.clone(), result.clone()).await;
                        }
                    }
                }
                Err(err) => {
                    error!("error serializing SpeculativeNewHeads to JSON: {err:?}");
                }
            }
        }
        crate::websocket_server::Event::ProcessedLogs {
            logs,
            header,
            commit_state: BlockCommitState::Finalized,
            ..
        } if subscriptions.contains_key(&SubscriptionKind::Logs) => {
            if let Some(subs) = subscriptions.get(&SubscriptionKind::Logs) {
                for (id, filter) in subs.iter() {
                    let logs = maybe_filter_logs(filter, &header, &logs);

                    match logs {
                        Some(logs) => {
                            for log in logs {
                                match serde_json::to_value(SubscriptionResult::Logs(log.clone())) {
                                    Ok(result) => {
                                        send_notification(&mut session, id.clone(), result.clone())
                                            .await;
                                    }
                                    Err(err) => {
                                        error!("error serializing Logs to JSON: {err:?}");
                                    }
                                }
                            }
                        }
                        None => {}
                    }
                }
            }
        }
        crate::websocket_server::Event::ProcessedLogs {
            logs,
            header,
            block_id,
            commit_state,
        } if subscriptions.contains_key(&SubscriptionKind::MonadLogs) => {
            if let Some(subs) = subscriptions.get(&SubscriptionKind::MonadLogs) {
                for (id, filter) in subs.iter() {
                    let logs = maybe_filter_logs(filter, &header, &logs);

                    match logs {
                        Some(logs) => {
                            for log in logs {
                                match serde_json::to_value(SubscriptionResult::SpeculativeLogs(
                                    SpeculativeLog {
                                        log: log.clone(),
                                        block_id,
                                        commit_state,
                                    },
                                )) {
                                    Ok(result) => {
                                        send_notification(&mut session, id.clone(), result.clone())
                                            .await;
                                    }
                                    Err(err) => {
                                        error!("error serializing MonadLogs to JSON: {err:?}");
                                    }
                                }
                            }
                        }
                        None => {}
                    }
                }
            }
        }
        crate::websocket_server::Event::Event(event)
            if subscriptions.contains_key(&SubscriptionKind::MonadEventStream) =>
        {
            match serde_json::to_value(SubscriptionResult::MonadEventStream(event)) {
                Ok(result) => {
                    if let Some(subs) = subscriptions.get(&SubscriptionKind::MonadEventStream) {
                        for (id, _) in subs.iter() {
                            send_notification(&mut session, id.clone(), result.clone()).await;
                        }
                    }
                }
                Err(err) => {
                    error!("error serializing MonadEventStream to JSON: {err:?}");
                }
            }
        }
        _ => {}
    }
}

fn maybe_filter_logs<'a>(
    filter: &Option<Filter>,
    header: &alloy_rpc_types::eth::Header,
    logs: &'a [alloy_rpc_types::Log],
) -> Option<Vec<&'a alloy_rpc_types::Log>> {
    match filter {
        Some(filter) => {
            // Before doing any work, check if the block's bloom filter matches the filter.
            if FilteredParams::matches_address(
                header.logs_bloom,
                &FilteredParams::address_filter(&filter.address),
            ) && FilteredParams::matches_topics(
                header.logs_bloom,
                &FilteredParams::topics_filter(&filter.topics),
            ) {
                let filtered_params: FilteredParams = FilteredParams::new(Some(filter.clone()));

                // Filter all logs
                let logs: Vec<&alloy_rpc_types::Log> = logs
                    .into_iter()
                    .filter_map(|log| {
                        if !(filtered_params.filter.is_some()
                            && (!filtered_params.filter_address(&log.address())
                                || !filtered_params.filter_topics(log.topics())))
                        {
                            Some(log)
                        } else {
                            None
                        }
                    })
                    .collect();
                Some(logs)
            } else {
                // The block's bloom filter doesn't match the filter, so we can skip this block.
                None
            }
        }
        None => {
            // Return all logs in the block
            Some(logs.iter().collect_vec())
        }
    }
}

async fn handle_request(
    ctx: &mut actix_ws::Session,
    subscriptions: &mut HashMap<SubscriptionKind, Vec<(SubscriptionId, Option<Filter>)>>,
    request: Request,
) {
    match request.method.as_str() {
        "eth_subscribe" => {
            let req: EthSubscribeRequest = match serde_json::from_value(request.params) {
                Ok(params) => params,
                Err(_) => {
                    if let Err(err) = ctx
                        .text(to_response(&crate::jsonrpc::Response::new(
                            None,
                            Some(JsonRpcError::invalid_params()),
                            request.id,
                        )))
                        .await
                    {
                        warn!("ws handle_request subscribe invalid_params request text error: {err:?}");
                    }
                    return;
                }
            };

            let filter = match req.params {
                Params::Logs(filter) => Some(*filter),
                Params::None => None,
                _ => {
                    if let Err(err) = ctx
                        .text(to_response(&crate::jsonrpc::Response::new(
                            None,
                            Some(JsonRpcError::invalid_params()),
                            request.id,
                        )))
                        .await
                    {
                        warn!(
                            "ws handle_request subscribe invalid_params filter text error: {err:?}"
                        );
                    }
                    return;
                }
            };

            let mut rng = rand::thread_rng();
            let random_bytes: [u8; 16] = rng.gen();
            let id = SubscriptionId(FixedData(random_bytes));

            debug!("received subscribe request for {:?}", &req.kind);

            if let Err(err) = ctx
                .text(to_response(&crate::jsonrpc::Response::from_result(
                    request.id,
                    serialize_result(id),
                )))
                .await
            {
                warn!("ws handle_request subscribe text error: {err:?}");
            }

            subscriptions
                .entry(req.kind)
                .or_insert_with(Vec::new)
                .push((id, filter));
        }
        "eth_unsubscribe" => {
            let params: EthUnsubscribeRequest = match serde_json::from_value(request.params) {
                Ok(params) => params,
                Err(_) => {
                    if let Err(err) = ctx
                        .text(to_response(&crate::jsonrpc::Response::new(
                            None,
                            Some(JsonRpcError::invalid_params()),
                            request.id,
                        )))
                        .await
                    {
                        warn!("ws handle_request unsubscribe invalid_params text error: {err:?}");
                    }
                    return;
                }
            };

            debug!("received unsubscribe request");

            let mut exists: bool = false;
            for vec in subscriptions.values_mut() {
                let original_len = vec.len();
                vec.retain(|x| x.0 != SubscriptionId(params.id));
                if vec.len() < original_len {
                    exists = true;
                    break;
                }
            }

            if let Err(err) = ctx
                .text(to_response(&crate::jsonrpc::Response::from_result(
                    request.id,
                    serialize_result(exists),
                )))
                .await
            {
                warn!("ws handle_request unsubscribe text error: {err:?}");
            }
        }
        _ => {
            if let Err(err) = ctx
                .text(to_response(&crate::jsonrpc::Response::new(
                    None,
                    Some(JsonRpcError::method_not_found()),
                    request.id,
                )))
                .await
            {
                warn!("ws handle_request unsubscribe method_not_found text error: {err:?}");
            }
        }
    }
}

#[inline]
async fn send_notification(session: &mut actix_ws::Session, id: SubscriptionId, result: Value) {
    match serde_json::to_value(EthSubscribeResult::new(id.0, result)) {
        Ok(body) => {
            let update = crate::jsonrpc::Notification::new("eth_subscription".to_string(), body);
            if let Err(err) = session.text(to_response(&update)).await {
                warn!("ws handler PublishMessage text error: {err:?}");
            };
        }
        Err(err) => {
            error!("error serializing EthSubscribeResult to JSON: {err:?}");
        }
    };
}

#[inline]
fn to_response<S: Serialize + std::fmt::Debug>(resp: &S) -> String {
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

fn to_request<T: serde::de::DeserializeOwned>(
    body: impl AsRef<[u8]>,
) -> Result<Request, JsonRpcError> {
    let request: RequestWrapper<Value> =
        serde_json::from_slice(body.as_ref()).map_err(|_| JsonRpcError::invalid_params())?;

    let request = match request {
        RequestWrapper::Single(req) => {
            serde_json::from_value::<Request>(req).map_err(|_| JsonRpcError::invalid_params())
        }
        _ => Err(JsonRpcError::invalid_params()), // TODO: handle batch requests
    }?;

    Ok(request)
}

#[cfg(test)]
mod tests {
    use actix_http::{ws, ws::Frame};
    use actix_web::{web, App};
    use alloy_consensus::TxEip1559;
    use alloy_primitives::{address, hex::FromHex, Bloom, BloomInput, FixedBytes, Log, B256, U256};
    use alloy_rpc_types::AccessList;
    use bytes::Bytes;
    use flume::Receiver;
    use futures_util::{SinkExt as _, StreamExt as _};
    use monad_event_ring::{
        event_reader::EventReader,
        event_ring::{EventRing, EventRingType},
        event_ring_util::EventRingSnapshot,
    };
    use monad_exec_events::{
        block_builder::TransactionInfo,
        exec_event_ctypes::EXEC_EVENT_DOMAIN_METADATA,
        exec_event_stream::{ExecEventStream, ExecEventStreamConfig, PollResult},
        exec_event_test_util::{ExecEventTestScenario, ETHEREUM_MAINNET_30B_15M},
        exec_events::*,
    };
    use monad_types::{BlockId, Hash};
    use serde_json::json;
    use tracing_actix_web::TracingLogger;

    use super::{ws_handler, WebSocketServerHandle};
    use crate::{
        eth_json_types::{
            BlockCommitState, EthSubscribeResult, FixedData, SpeculativeNewHead, StreamEvent,
            SubscriptionResult,
        },
        hex,
        websocket_server::{Event, WebSocketServer},
        MonadJsonRootSpanBuilder,
    };

    const DUMMY_PROPOSAL_META: ProposalMetadata = ProposalMetadata {
        round: 1,
        epoch: 0,
        block_number: 1,
        id: MonadBlockId(B256::ZERO),
        parent_round: 0,
        parent_id: MonadBlockId(B256::ZERO),
    };

    const DUMMY_ETH_EXEC_INPUT: EthBlockExecInput = EthBlockExecInput {
        parent_hash: B256::ZERO,
        ommers_hash: B256::ZERO,
        beneficiary: alloy_primitives::Address::ZERO,
        transactions_root: B256::ZERO,
        difficulty: 0,
        number: 0,
        gas_limit: 0,
        timestamp: 0,
        extra_data: alloy_primitives::Bytes::new(),
        prev_randao: B256::ZERO,
        nonce: alloy_primitives::B64::ZERO,
        base_fee_per_gas: None,
        withdrawals_root: None,
        transaction_count: 0,
    };

    const ERC20_ADDR: alloy_primitives::Address =
        address!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");

    const ERC20_TRANSFER_TOPIC: [u8; 32] = alloy_primitives::hex!(
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    );

    const ERC20_LOG_DATA: [u8; 32] = alloy_primitives::hex!(
        "0x0000000000000000000000000000000000000000000000000000000250ad9d3a"
    );

    fn make_erc20_tx() -> TransactionInfo {
        let log = Log::new_unchecked(
            ERC20_ADDR,
            vec![ERC20_TRANSFER_TOPIC.into()],
            ERC20_LOG_DATA.into(),
        );

        let receipt = alloy_consensus::Receipt {
            cumulative_gas_used: 0,
            status: alloy_consensus::Eip658Value::Eip658(true),
            logs: vec![log.clone()],
        };

        let mut logs_bloom = Bloom::default();
        logs_bloom.accrue(BloomInput::Raw(&log.address.0 .0));
        for topic in log.topics() {
            logs_bloom.accrue(BloomInput::Raw(&topic.0));
        }

        TransactionInfo {
            txn_index: 0,
            txn_envelope: alloy_consensus::TxEnvelope::Eip1559(
                alloy_consensus::Signed::new_unchecked(
                    TxEip1559 {
                        chain_id: 0,
                        access_list: AccessList::default(),
                        max_fee_per_gas: 0,
                        max_priority_fee_per_gas: 0,
                        nonce: 0,
                        gas_limit: 0,
                        to: alloy_primitives::TxKind::Call(
                            FixedBytes::from_hex("0xdEADBEeF00000000000000000000000000000000")
                                .unwrap()
                                .into(),
                        ),
                        value: U256::ZERO,
                        input: alloy_primitives::Bytes::new(),
                    },
                    alloy_signer::Signature::new(U256::ZERO, U256::ZERO, false),
                    FixedBytes::from_slice(&[0u8; 32]),
                ),
            ),
            sender: FixedBytes::from_hex("0xdEADBEeF00000000000000000000000000000000")
                .unwrap()
                .into(),
            receipt,
            txn_gas_used: 0,
            call_frames: Vec::new(),
            account_accesses: std::collections::HashMap::new(),
        }
    }

    fn make_block_events(
        proposal_meta: ProposalMetadata,
        mut exec_input: EthBlockExecInput,
        txn_info: &[TransactionInfo],
        eth_block_hash: B256,
    ) -> Vec<ExecEvent> {
        let mut v = Vec::new();

        exec_input.transaction_count = txn_info.len() as u64;
        v.push(ExecEvent::BlockStart {
            consensus_state: ConsensusState::Proposed,
            proposal_meta,
            chain_id: 1,
            exec_input,
        });

        let mut block_gas_used: u64 = 0;
        let mut logs_bloom = Box::new(Bloom::default());
        for txn in txn_info {
            v.push(ExecEvent::TransactionStart {
                txn_index: txn.txn_index,
                sender: txn.sender,
                txn_envelope: txn.txn_envelope.clone(),
            });
            let log_count = txn.receipt.logs.len();
            let call_frame_count = txn.call_frames.len();
            for (log_index, log) in txn.receipt.logs.iter().enumerate() {
                logs_bloom.accrue_log(log);
                v.push(ExecEvent::TransactionLog {
                    txn_index: txn.txn_index,
                    log_index: log_index as u32,
                    log: log.clone(),
                });
            }
            v.push(ExecEvent::TransactionReceipt {
                txn_index: txn.txn_index,
                status: txn.receipt.status,
                log_count,
                call_frame_count,
                txn_gas_used: txn.txn_gas_used,
            });
            block_gas_used += txn.txn_gas_used as u64;
        }

        v.push(ExecEvent::BlockEnd {
            eth_block_hash,
            state_root: B256::default(),
            receipts_root: B256::default(),
            logs_bloom,
            gas_used: block_gas_used,
        });

        v
    }

    async fn exec_event_loop(tx: flume::Sender<PollResult>) {
        const TEST_SCENARIO: &ExecEventTestScenario = &ETHEREUM_MAINNET_30B_15M;
        let snapshot = EventRingSnapshot::load_from_bytes(
            TEST_SCENARIO.event_ring_snapshot_zst,
            TEST_SCENARIO.name,
        );
        let event_ring = EventRing::mmap_from_fd(
            libc::PROT_READ,
            0,
            snapshot.snapshot_fd,
            snapshot.snapshot_off,
            TEST_SCENARIO.name,
        )
        .unwrap();
        let event_reader = EventReader::new(
            &event_ring,
            EventRingType::Exec,
            &EXEC_EVENT_DOMAIN_METADATA.metadata_hash,
        );
        if let Err(e) = event_reader {
            panic!("unable to open scenario {}: {}", TEST_SCENARIO.name, e);
        }
        let mut event_reader = event_reader.unwrap();
        event_reader.read_last_seqno = 0;

        let mut event_stream = ExecEventStream::new(
            event_reader,
            ExecEventStreamConfig {
                parse_txn_input: true,
                opt_process_exit_monitor: None,
            },
        );

        let mut update_count: u32 = 0;

        // let mut res = Vec::new();

        loop {
            match event_stream.poll() {
                pr @ PollResult::Ready { .. } => {
                    tx.send(pr.clone()).expect("send failed");
                    tokio::task::yield_now().await;
                    // update_count += 1;
                    // if update_count == 100 {
                    //     break;
                    // }
                }
                _ => {}
            }
        }
    }

    fn create_test_server(rx_exec_events: Receiver<PollResult>) -> actix_test::TestServer {
        let (websocket_broadcast_tx, websocket_broadcast_rx) =
            tokio::sync::broadcast::channel::<Event>(10_000);

        let ws_server = WebSocketServer::new(rx_exec_events, websocket_broadcast_tx.clone());
        tokio::spawn(async move {
            ws_server.run().await;
        });

        let ws_server_handle = WebSocketServerHandle {
            tx: websocket_broadcast_tx,
        };
        actix_test::start(move || {
            App::new()
                .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
                .app_data(web::JsonConfig::default().limit(8192))
                .app_data(web::Data::new(ws_server_handle.clone()))
                .service(web::resource("/ws/").route(web::get().to(ws_handler)))
        })
    }

    #[actix_rt::test]
    async fn websocket_wait_for_ping() {
        let (tx, rx) = flume::bounded::<PollResult>(100000000);
        tokio::spawn(exec_event_loop(tx.clone()));
        let mut server = create_test_server(rx);
        let mut framed = server.ws_at("/ws/").await.unwrap();
        let frame = framed.next().await.unwrap().unwrap();
        assert_eq!(frame, Frame::Ping(Bytes::from_static(b"")));
        framed
            .send(ws::Message::Pong(Bytes::from_static(b"")))
            .await
            .unwrap();
    }

    #[actix_rt::test]
    async fn websocket_eth_subscribe() {
        let (tx, rx) = flume::bounded::<PollResult>(100000000);
        tokio::spawn(exec_event_loop(tx.clone()));
        let mut server: actix_test::TestServer = create_test_server(rx);
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
            let resp: FixedData<16> = serde_json::from_value(resp.result.unwrap()).unwrap();
            resp
        } else {
            panic!("Expected a text frame");
        };

        // Receive some messages, then unsubscribe
        let mut count: usize = 0;
        loop {
            if let Some(frame) = framed.next().await {
                if let Ok(frame) = frame {
                    match frame {
                        Frame::Ping(_) => {
                            framed
                                .send(ws::Message::Pong(Bytes::from_static(b"")))
                                .await
                                .unwrap();
                        }
                        Frame::Text(update) => {
                            let update: serde_json::Value =
                                serde_json::from_slice(&update).unwrap();
                            let update: crate::jsonrpc::Notification =
                                serde_json::from_value(update).unwrap();
                            let update: EthSubscribeResult =
                                serde_json::from_value(update.params).unwrap();
                            assert_eq!(update.subscription.0, subscription_id.0);
                        }
                        _ => panic!("unexpected frame"),
                    }
                }
                count += 1;
            }

            if count > 2 {
                let body = json!({
                    "jsonrpc": "2.0",
                    "method": "eth_unsubscribe",
                    "params": [hex::encode(&subscription_id.0)],
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
                    let resp: bool = serde_json::from_value(resp.result.unwrap()).unwrap();
                    assert!(resp);
                } else {
                    panic!("Expected a text frame");
                };
                break;
            }
        }
    }

    #[actix_rt::test]
    async fn websocket_multiple_connections() {
        // Create a test server with two connections
        let (tx, rx) = flume::bounded::<PollResult>(100000000);
        let mut server = create_test_server(rx);
        let mut conn0 = server.ws_at("/ws/").await.unwrap();
        let mut conn1 = server.ws_at("/ws/").await.unwrap();

        // Subscribe both connections to newHeads
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["newHeads"],
            "id": 1
        });

        // Send subscription requests
        conn0
            .send(ws::Message::Text(body.to_string().into()))
            .await
            .unwrap();
        conn1
            .send(ws::Message::Text(body.to_string().into()))
            .await
            .unwrap();

        // Handle initial ping and subscription responses
        let mut frames0 = Vec::new();
        let mut frames1 = Vec::new();

        // Collect initial frames (ping + subscription response)
        for _ in 0..2 {
            frames0.push(conn0.next().await.unwrap().unwrap());
            frames1.push(conn1.next().await.unwrap().unwrap());
        }

        // Send a block update
        let eth_block_hash = B256::from([1; 32]);
        for (seqno, event) in make_block_events(
            DUMMY_PROPOSAL_META,
            DUMMY_ETH_EXEC_INPUT,
            &[],
            eth_block_hash,
        )
        .into_iter()
        .enumerate()
        {
            tx.send(PollResult::Ready {
                seqno: seqno as u64,
                event,
            })
            .unwrap();
        }

        // We're testing newHeads, so we need an explicit Finalization
        tx.send(PollResult::Ready {
            seqno: 3,
            event: ExecEvent::Referendum {
                proposal_meta: DUMMY_PROPOSAL_META,
                outcome: ConsensusState::Finalized,
            },
        })
        .unwrap();

        // Both connections should receive the block update
        let mut update0 = None;
        let mut update1 = None;

        // Keep reading until we get updates or timeout
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(5);

        while (update0.is_none() || update1.is_none()) && start.elapsed() < timeout {
            tokio::select! {
                frame = conn0.next(), if update0.is_none() => {
                    if let Some(Ok(frame)) = frame {
                        update0 = Some(frame);
                    }
                }
                frame = conn1.next(), if update1.is_none() => {
                    if let Some(Ok(frame)) = frame {
                        update1 = Some(frame);
                    }
                }
            }
        }

        assert!(
            update0.is_some(),
            "Connection 0 did not receive block update"
        );
        assert!(
            update1.is_some(),
            "Connection 1 did not receive block update"
        );

        // Verify the updates contain the expected block hash
        for update in [update0, update1] {
            if let Some(Frame::Text(body)) = update {
                let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let result = &value["params"]["result"];
                assert_eq!(
                    result["hash"].as_str().unwrap(),
                    hex::encode(&eth_block_hash.0),
                    "Unexpected block hash in update"
                );
            } else {
                if matches!(update, Some(Frame::Ping(_))) {
                    continue;
                };
                panic!(
                    "Expected text frame containing block update, got {:?}",
                    update
                );
            }
        }
    }

    #[actix_rt::test]
    async fn websocket_disconnect() {
        // Create a test server with two connections
        let (tx, rx) = flume::bounded::<PollResult>(100000000);
        let mut server = create_test_server(rx);
        let mut conn0 = server.ws_at("/ws/").await.unwrap();
        let mut conn1 = server.ws_at("/ws/").await.unwrap();

        // Subscribe both connections to newHeads
        let subscribe_msg = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["newHeads"],
            "id": 1
        });

        // Send subscription requests
        conn0
            .send(ws::Message::Text(subscribe_msg.to_string().into()))
            .await
            .unwrap();
        conn1
            .send(ws::Message::Text(subscribe_msg.to_string().into()))
            .await
            .unwrap();

        // Handle initial ping and subscription responses
        let mut frames0 = Vec::new();
        let mut frames1 = Vec::new();

        // Collect initial frames (ping + subscription response)
        for _ in 0..2 {
            frames0.push(conn0.next().await.unwrap().unwrap());
            frames1.push(conn1.next().await.unwrap().unwrap());
        }

        // Trigger disconnect
        tx.send(PollResult::Disconnected).unwrap();

        // Both connections should receive a close frame
        let mut close0 = None;
        let mut close1 = None;

        // Keep reading until we get close frames or timeout
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(5);

        while (close0.is_none() || close1.is_none()) && start.elapsed() < timeout {
            tokio::select! {
                frame = conn0.next(), if close0.is_none() => {
                    if let Some(Ok(frame)) = frame {
                        if matches!(frame, Frame::Close(_)) {
                            close0 = Some(frame);
                        }
                    }
                }
                frame = conn1.next(), if close1.is_none() => {
                    if let Some(Ok(frame)) = frame {
                        if matches!(frame, Frame::Close(_)) {
                            close1 = Some(frame);
                        }
                    }
                }
            }
        }

        assert!(close0.is_some(), "Connection 0 did not receive close frame");
        assert!(close1.is_some(), "Connection 1 did not receive close frame");

        // Further reads should return None as the connection is closed
        assert!(conn0.next().await.is_none());
        assert!(conn1.next().await.is_none());

        // Connect to the server again, expect an error
        let mut conn2 = server.ws_at("/ws/").await.unwrap();

        let frame = conn2.next().await.unwrap().unwrap();
        assert!(matches!(frame, Frame::Close(_)));
    }

    #[actix_rt::test]
    async fn websocket_speculative_blocks() {
        let (tx, rx) = flume::bounded::<PollResult>(100000000);
        let mut server = create_test_server(rx);

        let mut framed = server.ws_at("/ws/").await.unwrap();

        let _frame = framed.next().await.unwrap().unwrap();
        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["monadNewHeads"],
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
            let resp: FixedData<16> = serde_json::from_value(resp.result.unwrap()).unwrap();
            resp
        } else {
            panic!("Expected a text frame");
        };

        // Create the following block updates:
        // execution (1)
        // voted (1)
        // execution (2)
        // finalized (1)
        // voted (2)
        // finalized (2)
        // verified (1)
        // verified (2)
        let block_one_id = BlockId(Hash([1; 32]));
        let block_two_id = BlockId(Hash([2; 32]));

        let proposal_one = ProposalMetadata {
            round: 1,
            epoch: 0,
            block_number: 1,
            id: MonadBlockId(B256::from(block_one_id.0 .0)),
            parent_round: 0,
            parent_id: MonadBlockId(B256::ZERO),
        };

        let proposal_two = ProposalMetadata {
            round: proposal_one.round + 1,
            epoch: proposal_one.epoch,
            block_number: proposal_one.block_number + 1,
            id: MonadBlockId(B256::from(block_two_id.0 .0)),
            parent_round: proposal_one.round,
            parent_id: proposal_one.id,
        };

        // Block 1 execution events
        let mut event_counter: u64 = 1;
        for event in make_block_events(proposal_one, DUMMY_ETH_EXEC_INPUT, &[], FixedBytes([1; 32]))
        {
            tx.send(PollResult::Ready {
                seqno: event_counter,
                event,
            })
            .unwrap();
            event_counter += 1;
        }

        tx.send(PollResult::Ready {
            seqno: event_counter,
            event: ExecEvent::Referendum {
                proposal_meta: proposal_one,
                outcome: ConsensusState::QC,
            },
        })
        .unwrap();
        event_counter += 1;

        // Block 2 execution events
        for event in make_block_events(proposal_two, DUMMY_ETH_EXEC_INPUT, &[], FixedBytes([1; 32]))
        {
            tx.send(PollResult::Ready {
                seqno: event_counter,
                event,
            })
            .unwrap();
            event_counter += 1;
        }

        tx.send(PollResult::Ready {
            seqno: event_counter,
            event: ExecEvent::Referendum {
                proposal_meta: proposal_one,
                outcome: ConsensusState::Finalized,
            },
        })
        .unwrap();
        event_counter += 1;

        tx.send(PollResult::Ready {
            seqno: event_counter,
            event: ExecEvent::Referendum {
                proposal_meta: proposal_two,
                outcome: ConsensusState::QC,
            },
        })
        .unwrap();
        event_counter += 1;

        tx.send(PollResult::Ready {
            seqno: event_counter,
            event: ExecEvent::Referendum {
                proposal_meta: proposal_two,
                outcome: ConsensusState::Finalized,
            },
        })
        .unwrap();
        event_counter += 1;

        tx.send(PollResult::Ready {
            seqno: event_counter,
            event: ExecEvent::Referendum {
                proposal_meta: proposal_one,
                outcome: ConsensusState::Verified,
            },
        })
        .unwrap();
        event_counter += 1;

        tx.send(PollResult::Ready {
            seqno: event_counter,
            event: ExecEvent::Referendum {
                proposal_meta: proposal_two,
                outcome: ConsensusState::Verified,
            },
        })
        .unwrap();

        // Assert the order of notifications received by client.
        for idx in 0..8 {
            let frame = framed.next().await.unwrap().unwrap();
            if let Frame::Text(update) = frame {
                let update: serde_json::Value = serde_json::from_slice(&update).unwrap();
                let update: crate::jsonrpc::Notification = serde_json::from_value(update).unwrap();
                let update: EthSubscribeResult = serde_json::from_value(update.params).unwrap();
                assert_eq!(update.subscription.0, subscription_id.0);
                let result: SubscriptionResult = serde_json::from_value(update.result).unwrap();
                match result {
                    SubscriptionResult::SpeculativeNewHeads(SpeculativeNewHead {
                        header: _,
                        block_id,
                        commit_state,
                    }) => match idx {
                        0 => {
                            assert_eq!(block_id, block_one_id);
                            assert!(matches!(commit_state, BlockCommitState::Proposed));
                        }
                        1 => {
                            assert_eq!(block_id, block_one_id);
                            assert!(matches!(commit_state, BlockCommitState::Voted));
                        }
                        2 => {
                            assert_eq!(block_id, block_two_id);
                            assert!(matches!(commit_state, BlockCommitState::Proposed));
                        }
                        3 => {
                            assert_eq!(block_id, block_one_id);
                            assert!(matches!(commit_state, BlockCommitState::Finalized));
                        }
                        4 => {
                            assert_eq!(block_id, block_two_id);
                            assert!(matches!(commit_state, BlockCommitState::Voted));
                        }
                        5 => {
                            assert_eq!(block_id, block_two_id);
                            assert!(matches!(commit_state, BlockCommitState::Finalized));
                        }
                        6 => {
                            assert_eq!(block_id, block_one_id);
                            assert!(matches!(commit_state, BlockCommitState::Verified));
                        }
                        7 => {
                            assert_eq!(block_id, block_two_id);
                            assert!(matches!(commit_state, BlockCommitState::Verified));
                        }
                        _ => {
                            panic!("Unexpected speculative new head");
                        }
                    },
                    _ => {
                        panic!("Expected speculative new head");
                    }
                }
            } else {
                panic!("Expected a text frame, got {frame:?}");
            }
        }
    }

    #[actix_rt::test]
    async fn websocket_logs() {
        let (tx, rx) = flume::bounded::<PollResult>(100000000);
        let mut server = create_test_server(rx);

        let mut conn = server.ws_at("/ws/").await.unwrap();

        let _frame = conn.next().await.unwrap().unwrap();
        let params = vec![
            json!(["logs"]),
            json!(["logs", {"address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"}]),
            json!(["logs", {"topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]}]),
            json!(["logs", {"address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]}]),
            json!(["logs", {"address": "0x00000000000000000000000000000000DeaDBeef"}]), // should not match any logs
            json!(["logs", {"address": "0x00000000000000000000000000000000DeaDBeef", "topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]}]), // should not match any logs
        ];

        let mut subscription_ids = Vec::new();
        for params in params.iter() {
            let body = json!({
                "jsonrpc": "2.0",
                "method": "eth_subscribe",
                "params": params,
                "id": 1
            });

            conn.send(ws::Message::Text(body.to_string().into()))
                .await
                .unwrap();
            let frame = conn.next().await.unwrap().unwrap();

            assert!(matches!(frame, Frame::Text(_)));
            let subscription_id = if let Frame::Text(resp) = frame {
                let resp: serde_json::Value = serde_json::from_slice(&resp).unwrap();
                let resp: crate::jsonrpc::Response = serde_json::from_value(resp).unwrap();
                let resp: FixedData<16> = serde_json::from_value(resp.result.unwrap()).unwrap();
                resp
            } else {
                panic!("Expected a text frame");
            };
            subscription_ids.push(subscription_id);
        }

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Create a block with an erc20 transfer log
        let txs = vec![make_erc20_tx()];

        let mut last_seqno: usize = 0;
        for (seqno, event) in make_block_events(
            DUMMY_PROPOSAL_META,
            DUMMY_ETH_EXEC_INPUT,
            txs.as_slice(),
            FixedBytes([1; 32]),
        )
        .into_iter()
        .enumerate()
        {
            tx.send(PollResult::Ready {
                seqno: seqno as u64,
                event,
            })
            .unwrap();
            last_seqno = seqno
        }

        tx.send(PollResult::Ready {
            seqno: (last_seqno + 1) as u64,
            event: ExecEvent::Referendum {
                proposal_meta: DUMMY_PROPOSAL_META,
                outcome: ConsensusState::Finalized,
            },
        })
        .unwrap();

        // Assert the order of notifications received by client.
        for idx in 0..params.len() {
            let frame = conn.next().await.unwrap().unwrap();
            match frame {
                Frame::Text(update) => {
                    let update: serde_json::Value = serde_json::from_slice(&update).unwrap();
                    let update: crate::jsonrpc::Notification =
                        serde_json::from_value(update).unwrap();
                    let update: EthSubscribeResult = serde_json::from_value(update.params).unwrap();

                    subscription_ids
                        .iter()
                        .find(|id| id.0 == update.subscription.0)
                        .expect("subscription not found");
                    let result: SubscriptionResult = serde_json::from_value(update.result).unwrap();
                    let log = match result {
                        SubscriptionResult::Logs(logs) => logs,
                        r => panic!("Expected logs; got {r:#?}"),
                    };

                    assert_eq!(log.address(), ERC20_ADDR);
                    assert_eq!(*log.topics().first().unwrap(), ERC20_TRANSFER_TOPIC);
                    assert_eq!(log.data().data, Bytes::from_static(&ERC20_LOG_DATA));
                }
                Frame::Ping(_) | Frame::Pong(_) => {
                    conn.send(ws::Message::Pong(Bytes::from_static(b"")))
                        .await
                        .unwrap();
                }
                Frame::Close(_) if idx > 4 => {
                    break;
                }
                x => {
                    panic!("Unexpected frame: {x:#?}");
                }
            };
        }
    }

    #[actix_rt::test]
    async fn websocket_event_stream() {
        let (tx, rx) = flume::bounded::<PollResult>(100000000);
        let mut server = create_test_server(rx);

        let body = json!({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["monadEventStream"],
            "id": 1
        });

        let mut framed = server.ws_at("/ws/").await.unwrap();
        framed
            .send(ws::Message::Text(body.to_string().into()))
            .await
            .unwrap();
        let frame = framed.next().await.unwrap().unwrap();
        assert!(matches!(frame, Frame::Text(_)));

        // Send a block update
        let txs = vec![make_erc20_tx()];
        let mut expected_updates = Vec::new();
        let eth_block_hash = B256::from([1; 32]);
        for (seqno, event) in make_block_events(
            DUMMY_PROPOSAL_META,
            DUMMY_ETH_EXEC_INPUT,
            &txs,
            eth_block_hash,
        )
        .into_iter()
        .enumerate()
        {
            expected_updates.push(StreamEvent::ExecutionEvent {
                seqno: seqno as u64,
                event: event.clone(),
            });
            tx.send(PollResult::Ready {
                seqno: seqno as u64,
                event,
            })
            .unwrap();
        }

        let mut update_count = 0;
        while update_count < expected_updates.len() {
            let frame = framed.next().await.unwrap().unwrap();
            match frame {
                Frame::Text(frame_text) => {
                    let update: serde_json::Value = serde_json::from_slice(&frame_text).unwrap();
                    let update: crate::jsonrpc::Notification =
                        serde_json::from_value(update).unwrap();
                    let update: EthSubscribeResult = serde_json::from_value(update.params).unwrap();
                    let result: SubscriptionResult = serde_json::from_value(update.result).unwrap();
                    match result {
                        SubscriptionResult::MonadEventStream(actual) => {
                            let expected = expected_updates.get(update_count).unwrap();
                            assert_eq!(actual.event, *expected);
                            update_count += 1;
                        }
                        r => panic!("unexpected SubscriptionResult: {r:#?}"),
                    }
                }
                Frame::Ping(_) => {
                    framed
                        .send(ws::Message::Pong(Bytes::from_static(b"")))
                        .await
                        .unwrap();
                }
                x => {
                    panic!("Unexpected frame: {x:#?}");
                }
            };
        }
    }
}
