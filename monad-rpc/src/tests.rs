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
use alloy_signer_local::PrivateKeySigner;
use serde_json::{json, Value};
use test_case::test_case;
use tracing_actix_web::TracingLogger;

use crate::{
    auth::{DomainAuthError, SignedDomainAccessToken, DOMAIN_AUTH_HEADER},
    handlers::{
        access::DomainOperators,
        domain_rpc_handler,
        resources::{MonadJsonRootSpanBuilder, MonadRpcResources},
        rpc_handler,
    },
    middleware::DecompressionGuard,
    txpool::EthTxPoolBridgeClient,
    types::jsonrpc::{ErrorCode, JsonRpcError, RequestId, Response, ResponseWrapper},
};

pub async fn init_server(
) -> impl Service<Request, Response = ServiceResponse<impl MessageBody>, Error = Error> {
    let app_state = MonadRpcResources {
        txpool_bridge_client: Some(EthTxPoolBridgeClient::for_testing()),
        eth_call_handler: None,
        base_chain_id: 1337,
        chain_id: 1337,
        domain: None,
        caller: None,
        domain_operators: std::sync::Arc::new(DomainOperators::from([(
            DOMAIN_CHAIN_ID,
            std::collections::HashSet::from([operator_signer().address()]),
        )])),
        data_provider: None,
        event_server_client: None,
        batch_request_limit: 5,
        batch_concurrent_limit: 5,
        max_response_size: 25_000_000,
        allow_unprotected_txs: false,
        logs_max_block_range: 1000,
        eth_send_raw_transaction_sync_default_timeout_ms: 2_000,
        eth_send_raw_transaction_sync_max_timeout_ms: 10_000,
        dry_run_get_logs_index: false,
        use_eth_get_logs_index: false,
        max_finalized_block_cache_len: 200,
        enable_eth_simulate_v1: false,
        metrics: None,
        rpc_comparator: None,
        feehistory_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(100)),
    };

    test::init_service(
        App::new()
            .wrap(DecompressionGuard::default())
            .wrap(TracingLogger::<MonadJsonRootSpanBuilder>::new())
            .app_data(web::PayloadConfig::default().limit(2_000_000))
            .app_data(web::Data::new(app_state.clone()))
            .service(web::resource("/").route(web::post().to(rpc_handler)))
            .service(
                web::resource("/domain/{chain_id}").route(web::post().to(domain_rpc_handler)),
            ),
    )
    .await
}

const DOMAIN_CHAIN_ID: u64 = (1 << 16) | 1337;

/// An ordinary domain user.
fn user_signer() -> PrivateKeySigner {
    PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0x22)).unwrap()
}

/// The configured operator of `DOMAIN_CHAIN_ID`.
fn operator_signer() -> PrivateKeySigner {
    PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0x33)).unwrap()
}

fn access_token(signer: &PrivateKeySigner, chain_id: u64) -> String {
    SignedDomainAccessToken::sign(signer, chain_id)
        .unwrap()
        .encode()
}

fn user_token() -> String {
    access_token(&user_signer(), DOMAIN_CHAIN_ID)
}

fn operator_token() -> String {
    access_token(&operator_signer(), DOMAIN_CHAIN_ID)
}

async fn call_domain(
    app: &impl Service<Request, Response = ServiceResponse<impl MessageBody>, Error = Error>,
    token: Option<String>,
    method: &str,
    params: Value,
) -> ResponseWrapper<Response> {
    let payload = json!({"jsonrpc": "2.0", "method": method, "params": params, "id": 1});
    let mut req = test::TestRequest::post()
        .uri(&format!("/domain/{DOMAIN_CHAIN_ID}"))
        .set_payload(payload.to_string());
    if let Some(token) = token {
        req = req.insert_header((DOMAIN_AUTH_HEADER, token));
    }
    let resp = app.call(req.to_request()).await.unwrap();
    recover_response_body(resp).await
}

async fn call_domain_eth_chain_id(
    app: &impl Service<Request, Response = ServiceResponse<impl MessageBody>, Error = Error>,
    token: Option<String>,
) -> ResponseWrapper<Response> {
    call_domain(app, token, "eth_chainId", json!([])).await
}

fn error_code(resp: ResponseWrapper<Response>) -> i32 {
    match resp {
        ResponseWrapper::Single(resp) => resp.error.expect("expected an error response").code,
        other => panic!("expected a single response, got {other:?}"),
    }
}

/// The error a method reports once access control has let it through: the test server has
/// no data provider or eth_call handler, so every data-backed method stops here.
fn passed_access_control() -> i32 {
    JsonRpcError::new(ErrorCode::MethodNotFound).code
}

fn forbidden() -> i32 {
    crate::types::jsonrpc::FORBIDDEN_ERROR_CODE
}

fn hex_address(signer: &PrivateKeySigner) -> String {
    format!("{:#x}", signer.address())
}

fn topic_of(signer: &PrivateKeySigner) -> String {
    format!("{:#x}", signer.address().into_word())
}

#[actix_web::test]
async fn domain_route_requires_access_token() {
    let app = init_server().await;

    let resp = call_domain_eth_chain_id(&app, None).await;

    assert_eq!(
        resp,
        ResponseWrapper::Single(Response::from_error(
            DomainAuthError::MissingToken.into()
        ))
    );
}

#[actix_web::test]
async fn domain_route_accepts_valid_access_token() {
    let app = init_server().await;
    let resp = call_domain_eth_chain_id(&app, Some(user_token())).await;

    let chain_id = serde_json::from_str(&format!("\"{DOMAIN_CHAIN_ID:#x}\"")).unwrap();
    assert_eq!(
        resp,
        ResponseWrapper::Single(Response::new(Some(chain_id), None, RequestId::Number(1)))
    );
}

#[actix_web::test]
async fn domain_route_rejects_token_for_other_domain() {
    let app = init_server().await;
    let other_chain_id = (2 << 16) | 1337;
    let token = access_token(&user_signer(), other_chain_id);

    let resp = call_domain_eth_chain_id(&app, Some(token)).await;

    assert_eq!(
        resp,
        ResponseWrapper::Single(Response::from_error(
            DomainAuthError::ChainIdMismatch {
                token: other_chain_id,
                route: DOMAIN_CHAIN_ID,
            }
            .into()
        ))
    );
}

#[actix_web::test]
async fn domain_route_rejects_malformed_token() {
    let app = init_server().await;

    let resp = call_domain_eth_chain_id(&app, Some("not-a-token".to_string())).await;

    assert_eq!(
        resp,
        ResponseWrapper::Single(Response::from_error(
            DomainAuthError::MalformedToken.into()
        ))
    );
}

#[actix_web::test]
async fn domain_route_serves_block_methods_to_operators_only() {
    let app = init_server().await;
    let params = json!(["latest", false]);

    let user = call_domain(
        &app,
        Some(user_token()),
        "eth_getBlockByNumber",
        params.clone(),
    )
    .await;
    assert_eq!(error_code(user), forbidden());

    let operator =
        call_domain(&app, Some(operator_token()), "eth_getBlockByNumber", params).await;
    assert_eq!(error_code(operator), passed_access_control());

    let user = call_domain(
        &app,
        Some(user_token()),
        "eth_getStorageAt",
        json!([hex_address(&user_signer()), "0x0", "latest"]),
    )
    .await;
    assert_eq!(error_code(user), forbidden());
}

#[actix_web::test]
async fn domain_route_scopes_account_queries_to_signer() {
    let app = init_server().await;
    let own = json!([hex_address(&user_signer()), "latest"]);
    let other = json!([hex_address(&operator_signer()), "latest"]);

    for method in ["eth_getBalance", "eth_getTransactionCount"] {
        let resp = call_domain(&app, Some(user_token()), method, own.clone()).await;
        assert_eq!(
            error_code(resp),
            passed_access_control(),
            "{method} own account"
        );

        let resp = call_domain(&app, Some(user_token()), method, other.clone()).await;
        assert_eq!(error_code(resp), forbidden(), "{method} other account");

        let resp = call_domain(&app, Some(operator_token()), method, own.clone()).await;
        assert_eq!(
            error_code(resp),
            passed_access_control(),
            "{method} operator"
        );
    }
}

#[actix_web::test]
async fn domain_route_requires_log_filter_pinned_to_signer() {
    let app = init_server().await;
    let signer_topic = topic_of(&user_signer());

    let pinned = json!([{"topics": [null, signer_topic]}]);
    let resp = call_domain(&app, Some(user_token()), "eth_getLogs", pinned).await;
    assert_eq!(error_code(resp), passed_access_control());

    let unpinned = json!([{"address": hex_address(&user_signer())}]);
    let resp = call_domain(&app, Some(user_token()), "eth_getLogs", unpinned.clone()).await;
    assert_eq!(error_code(resp), forbidden());

    let resp = call_domain(&app, Some(operator_token()), "eth_getLogs", unpinned).await;
    assert_eq!(error_code(resp), passed_access_control());
}

#[actix_web::test]
async fn domain_route_rejects_state_overrides() {
    let app = init_server().await;
    let call = json!({"to": "0x0000000000000000000000000000000000000002", "data": "0x00"});
    let overrides = json!({ hex_address(&operator_signer()): {"balance": "0x1"} });

    for method in ["eth_call", "eth_estimateGas"] {
        let with_overrides = json!([call, "latest", overrides]);
        let resp = call_domain(&app, Some(user_token()), method, with_overrides.clone()).await;
        assert_eq!(
            error_code(resp),
            forbidden(),
            "{method} user with overrides"
        );

        let resp = call_domain(&app, Some(operator_token()), method, with_overrides).await;
        assert_eq!(
            error_code(resp),
            passed_access_control(),
            "{method} operator with overrides"
        );

        let resp = call_domain(&app, Some(user_token()), method, json!([call, "latest"])).await;
        assert_eq!(error_code(resp), passed_access_control(), "{method} user");
    }
}

#[actix_web::test]
async fn root_route_is_not_subject_to_domain_rules() {
    let app = init_server().await;
    let payload = json!({
        "jsonrpc": "2.0",
        "method": "eth_getBalance",
        "params": [hex_address(&operator_signer()), "latest"],
        "id": 1
    });
    let req = test::TestRequest::post()
        .uri("/")
        .set_payload(payload.to_string())
        .to_request();

    let resp = recover_response_body(app.call(req).await.unwrap()).await;

    assert_eq!(error_code(resp), passed_access_control());
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
#[test_case(json!([]), ResponseWrapper::Single(Response::new(None, Some(JsonRpcError::new(ErrorCode::InvalidRequest)), RequestId::Null)); "empty batch")]
#[test_case(json!([1]), ResponseWrapper::Batch(vec![Response::new(None, Some(JsonRpcError::new(ErrorCode::InvalidRequest)), RequestId::Null)]); "invalid batch but not empty")]
#[test_case(json!([
        {"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1}
    ]),
    ResponseWrapper::Batch(
        vec![Response::new(Some(serde_json::from_str("\"0x539\"").unwrap()), None, RequestId::Number(1))]
    ); "valid batch request")]
#[test_case(json!([1, 2, 3, 4]),
    ResponseWrapper::Batch(vec![
        Response::new(None, Some(JsonRpcError::new(ErrorCode::InvalidRequest)), RequestId::Null),
        Response::new(None, Some(JsonRpcError::new(ErrorCode::InvalidRequest)), RequestId::Null),
        Response::new(None, Some(JsonRpcError::new(ErrorCode::InvalidRequest)), RequestId::Null),
        Response::new(None, Some(JsonRpcError::new(ErrorCode::InvalidRequest)), RequestId::Null),
    ]); "multiple invalid batch")]
#[test_case(json!([
        {"jsonrpc": "2.0", "method": "subtract", "params": [42, 43], "id": 1},
        1,
        {"jsonrpc": "2.0", "method": "subtract", "params": [42, 43], "id": 1}
    ]),
    ResponseWrapper::Batch(
        vec![
            Response::new(None, Some(JsonRpcError::new(ErrorCode::MethodNotFound)), RequestId::Number(1)),
            Response::new(None, Some(JsonRpcError::new(ErrorCode::InvalidRequest)), RequestId::Null),
            Response::new(None, Some(JsonRpcError::new(ErrorCode::MethodNotFound)), RequestId::Number(1)),
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
        Response::new(None, Some(JsonRpcError::with_message(ErrorCode::ServerError, "number of requests in batch request exceeds limit of 5".to_string())), RequestId::Null)
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
