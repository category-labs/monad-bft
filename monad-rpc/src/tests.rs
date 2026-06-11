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

use actix_http::{Request, StatusCode};
use actix_web::{
    body::{to_bytes, MessageBody},
    dev::{Service, ServiceResponse},
    test, web, App, Error,
};
use serde_json::{json, Value};
use test_case::test_case;
use tracing_actix_web::TracingLogger;

use crate::{
    handlers::{
        resources::{MonadJsonRootSpanBuilder, MonadRpcResources},
        rpc_handler,
    },
    middleware::DecompressionGuard,
    txpool::EthTxPoolBridgeClient,
    types::jsonrpc::{JsonRpcError, RequestId, Response, ResponseWrapper},
};

fn test_resources() -> MonadRpcResources {
    MonadRpcResources {
        txpool_bridge_client: Some(EthTxPoolBridgeClient::for_testing()),
        eth_call_handler: None,
        chain_id: 1337,
        data_provider: None,
        chain_data_query: None,
        event_server_client: None,
        batch_request_limit: 5,
        max_response_size: 25_000_000,
        allow_unprotected_txs: false,
        logs_max_block_range: 1000,
        eth_send_raw_transaction_sync_default_timeout_ms: 2_000,
        eth_send_raw_transaction_sync_max_timeout_ms: 10_000,
        dry_run_get_logs_index: false,
        use_eth_get_logs_index: false,
        max_finalized_block_cache_len: 200,
        metrics: None,
        rpc_comparator: None,
    }
}

pub async fn init_server(
) -> impl Service<Request, Response = ServiceResponse<impl MessageBody>, Error = Error> {
    init_server_with_resources(test_resources()).await
}

async fn init_server_with_resources(
    app_state: MonadRpcResources,
) -> impl Service<Request, Response = ServiceResponse<impl MessageBody>, Error = Error> {
    test::init_service(
        App::new()
            .wrap(DecompressionGuard::default())
            .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
            .app_data(web::PayloadConfig::default().limit(2_000_000))
            .app_data(web::Data::new(app_state.clone()))
            .service(web::resource("/").route(web::post().to(rpc_handler))),
    )
    .await
}

pub async fn recover_response_body(
    resp: ServiceResponse<impl MessageBody>,
) -> ResponseWrapper<Response> {
    let b = to_bytes(resp.into_body())
        .await
        .unwrap_or_else(|_| panic!("body to_bytes failed"));

    ResponseWrapper::from_body_bytes(b).unwrap()
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
    let resp = recover_response_body(resp).await;

    match resp {
        ResponseWrapper::Batch(_) => panic!("expected single response"),
        ResponseWrapper::Single(resp) => match resp.error {
            Some(e) => assert_eq!(e.code, -32601),
            None => panic!("expected error in response"),
        },
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

    let resp = recover_response_body(resp).await;

    match resp {
        ResponseWrapper::Batch(_) => panic!("expected single response"),
        ResponseWrapper::Single(resp) => match resp.error {
            Some(e) => assert_eq!(e.code, -32601),
            None => panic!("expected error in response"),
        },
    }
}

#[allow(non_snake_case)]
#[test_case(json!([]), ResponseWrapper::Single(Response::new(None, Some(JsonRpcError::custom("empty batch request".to_string())), RequestId::Null)); "empty batch")]
#[test_case(json!([1]), ResponseWrapper::Batch(vec![Response::new(None, Some(JsonRpcError::invalid_request()), RequestId::Null)]); "invalid batch but not empty")]
#[test_case(json!([
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1}
    ]),
    ResponseWrapper::Batch(
        vec![Response::new(Some(serde_json::from_str("\"0x539\"").unwrap()), None, RequestId::Number(1))]
    ); "valid batch request")]
#[test_case(json!([1, 2, 3, 4]),
    ResponseWrapper::Batch(vec![
        Response::new(None, Some(JsonRpcError::invalid_request()), RequestId::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), RequestId::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), RequestId::Null),
        Response::new(None, Some(JsonRpcError::invalid_request()), RequestId::Null),
    ]); "multiple invalid batch")]
#[test_case(json!([
        {"jsonrpc": "2.0", "method": "subtract", "params": [42, 43], "id": 1},
        1,
        {"jsonrpc": "2.0", "method": "subtract", "params": [42, 43], "id": 1}
    ]),
    ResponseWrapper::Batch(
        vec![
            Response::new(None, Some(JsonRpcError::method_not_found()), RequestId::Number(1)),
            Response::new(None, Some(JsonRpcError::invalid_request()), RequestId::Null),
            Response::new(None, Some(JsonRpcError::method_not_found()), RequestId::Number(1)),
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
        Response::new(None, Some(JsonRpcError::custom("number of requests in batch request exceeds limit of 5".to_string())), RequestId::Null)
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

    let resp = recover_response_body(resp).await;

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

    let resp: Response = actix_test::call_and_read_body_json(&app, req).await;
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

    let resp: Response = actix_test::call_and_read_body_json(&app, req).await;
    assert!(resp.result.is_none());
}

/// Two parent-linked blocks of logs (block 1: addr 0xaa, block 2: addr 0xbb),
/// ingested through the real chain-data engine into in-memory stores.
async fn chain_data_test_reader() -> monad_chain_data::ConfiguredChainDataReader {
    use monad_chain_data::{
        Address, ConfiguredChainDataReader, EvmBlockHeader, FinalizedBlock, Log, LogData, B256,
    };

    let mut blocks = Vec::new();
    let mut parent = monad_chain_data::Hash32::ZERO;
    for (number, address_byte) in [(1u64, 0xaa_u8), (2, 0xbb)] {
        let block = FinalizedBlock {
            header: EvmBlockHeader {
                number,
                parent_hash: parent,
                ..EvmBlockHeader::default()
            },
            logs_by_tx: vec![vec![Log {
                address: Address::repeat_byte(address_byte),
                data: LogData::new_unchecked(
                    vec![B256::repeat_byte(0x10)],
                    monad_chain_data::Bytes::new(),
                ),
            }]],
            txs: Vec::new(),
            traces: Vec::new(),
        };
        parent = block.block_hash();
        blocks.push(block);
    }
    let store = monad_chain_data::testkit::populate_via_engine(blocks).await;
    ConfiguredChainDataReader::in_memory(store.reader())
}

#[actix_web::test]
async fn test_eth_query_logs_round_trip() {
    let mut resources = test_resources();
    resources.chain_data_query = Some(crate::handlers::chaindata::ChainDataQueryRuntime {
        reader: std::sync::Arc::new(chain_data_test_reader().await),
        handle: tokio::runtime::Handle::current(),
    });
    let app = init_server_with_resources(resources).await;

    // Spec wire shape (monad-chain-data/queryX): flat camelCase request with
    // QUANTITY bounds; `fields` both projects and opts into relations.
    let payload = json!({
        "jsonrpc": "2.0",
        "method": "eth_queryLogs",
        "params": [{
            "fromBlock": "0x1",
            "toBlock": "0x2",
            "filter": {"address": format!("0x{}", "aa".repeat(20))},
            "fields": {"logs": ["address", "blockNumber"], "blocks": true}
        }],
        "id": 1
    });
    let req = test::TestRequest::post()
        .uri("/")
        .set_payload(payload.to_string())
        .to_request();
    let resp: Response = actix_test::call_and_read_body_json(&app, req).await;

    assert!(resp.error.is_none(), "unexpected error: {:?}", resp.error);
    let result: Value = serde_json::from_str(resp.result.unwrap().get()).unwrap();
    let logs = result["data"]["logs"].as_array().unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0]["blockNumber"], "0x1");
    assert!(logs[0]["address"]
        .as_str()
        .unwrap()
        .eq_ignore_ascii_case(&format!("0x{}", "aa".repeat(20))));
    assert!(
        logs[0].get("topics").is_none(),
        "fields projection drops unselected fields: {logs:?}"
    );
    // `fields.blocks` joins the blocks that contributed logs (block 1 only).
    let blocks = result["data"]["blocks"].as_array().unwrap();
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0]["number"], "0x1");
    assert!(blocks[0]["hash"].is_string());
    assert!(blocks[0]["timestamp"].is_string());
    // The resolved block references: camelCase QUANTITY/DATA at top level.
    assert_eq!(result["fromBlock"]["number"], "0x1");
    assert_eq!(result["toBlock"]["number"], "0x2");
    assert_eq!(result["cursorBlock"]["number"], "0x2");
    assert!(result["cursorBlock"]["hash"].is_string());
    assert!(result["cursorBlock"]["parentHash"].is_string());
}

#[actix_web::test]
async fn test_eth_query_methods_unsupported_without_reader() {
    let app = init_server().await;

    let payload = json!({
        "jsonrpc": "2.0",
        "method": "eth_queryLogs",
        "params": [{}],
        "id": 1
    });
    let req = test::TestRequest::post()
        .uri("/")
        .set_payload(payload.to_string())
        .to_request();
    let resp: Response = actix_test::call_and_read_body_json(&app, req).await;

    assert_eq!(resp.error.unwrap(), JsonRpcError::method_not_supported());
}
