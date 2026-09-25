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

//! Request validation, status codes and cors, against the app in process.

mod common;

use actix_web::{
    body::MessageBody,
    dev::{Service, ServiceResponse},
    http::{Method, StatusCode, header},
    test, web,
};
use common::{FakeSwarm, body, hash_hex, node_config, tx, unix_now};
use monad_mcp_chorus::ledger::MAX_TX_PAYLOAD;
use monad_mcp_rpc::{RpcConfig, RpcState, api::app};
use serde_json::{Value, json};

// an rpc colocated with validator 0 of `swarm`
fn state(swarm: &FakeSwarm, dir: &std::path::Path, max_pending: usize) -> web::Data<RpcState> {
    web::Data::new(
        RpcState::new(RpcConfig {
            max_pending,
            ..RpcConfig::colocated(&swarm.config(), dir.join("ledger")).unwrap()
        })
        .unwrap(),
    )
}

async fn json_of(
    response: ServiceResponse<impl MessageBody>,
) -> (StatusCode, Option<String>, Value) {
    let status = response.status();
    let cors = response
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .map(|v| v.to_str().unwrap().to_owned());
    let bytes = test::read_body(response).await;
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, cors, value)
}

async fn call(
    service: &impl Service<
        actix_http::Request,
        Response = ServiceResponse<impl MessageBody>,
        Error = actix_web::Error,
    >,
    request: test::TestRequest,
) -> (StatusCode, Option<String>, Value) {
    json_of(test::call_service(service, request.to_request()).await).await
}

#[actix_web::test]
async fn malformed_requests_are_400_and_oversized_payloads_413() {
    let dir = tempfile::tempdir().unwrap();
    let swarm = FakeSwarm::spawn(1).await;
    let service = test::init_service(app(state(&swarm, dir.path(), 10))).await;
    let post = |value: Value| test::TestRequest::post().uri("/tx").set_json(value);

    for (value, needle) in [
        (json!({}), "exactly one"),
        (
            json!({"payload_utf8": "a", "payload_hex": "00"}),
            "exactly one",
        ),
        (json!({"payload_hex": "0xnothex"}), "hex"),
        (json!({"payload_utf8": "a", "sender": "0x1234"}), "sender"),
        (json!({"payload_utf8": "a", "nonce": -1}), ""),
        (json!({"payload_utf8": "a", "extra": 1}), "extra"),
    ] {
        let (status, cors, reply) = call(&service, post(value.clone())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}: {reply}");
        assert_eq!(cors.as_deref(), Some("*"), "errors carry cors too");
        assert!(reply["error"].as_str().unwrap().contains(needle), "{reply}");
    }

    let raw = test::TestRequest::post()
        .uri("/tx")
        .insert_header((header::CONTENT_TYPE, "text/plain"))
        .set_payload("{not json");
    assert_eq!(call(&service, raw).await.0, StatusCode::BAD_REQUEST);

    let over = json!({"payload_hex": "ab".repeat(MAX_TX_PAYLOAD + 1)});
    let (status, _, reply) = call(&service, post(over)).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(
        reply["error"].as_str().unwrap().contains("1025 bytes"),
        "{reply}"
    );

    let huge = json!({"payload_utf8": "x".repeat(64 * 1024)});
    assert_eq!(
        call(&service, post(huge)).await.0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(swarm.frames(), 0, "a rejected request sent a frame");
}

#[actix_web::test]
async fn a_payload_at_the_cap_is_taken_even_without_a_content_type() {
    let dir = tempfile::tempdir().unwrap();
    let swarm = FakeSwarm::spawn(1).await;
    let service = test::init_service(app(state(&swarm, dir.path(), 10))).await;
    let request = test::TestRequest::post()
        .uri("/tx")
        .set_payload(json!({"payload_utf8": "a".repeat(MAX_TX_PAYLOAD)}).to_string());
    let (status, _, reply) = call(&service, request).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["payload_len"], MAX_TX_PAYLOAD);
    assert_eq!(reply["status"], "sent");
    assert_eq!(reply["leader"], 0);
    assert_eq!(reply["target_lane"], 0);
    assert!(reply["target_slot"].is_u64(), "{reply}");
    assert_eq!(reply["sender"].as_str().unwrap().len(), 42);
    common::eventually(std::time::Duration::from_secs(2), "the frame", || async {
        (swarm.frames() == 1).then_some(())
    })
    .await;
}

#[actix_web::test]
async fn lookups_validate_the_hash() {
    let dir = tempfile::tempdir().unwrap();
    let swarm = FakeSwarm::spawn(1).await;
    let service = test::init_service(app(state(&swarm, dir.path(), 10))).await;
    let get = |uri: &str| test::TestRequest::get().uri(uri);
    assert_eq!(
        call(&service, get("/tx/0x1234")).await.0,
        StatusCode::BAD_REQUEST
    );
    let unknown = format!("/tx/0x{}", "ab".repeat(32));
    assert_eq!(call(&service, get(&unknown)).await.0, StatusCode::NOT_FOUND);
    assert_eq!(call(&service, get("/nope")).await.0, StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn submissions_are_owned_idempotent_and_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let swarm = FakeSwarm::spawn(5).await;
    let service = test::init_service(app(state(&swarm, dir.path(), 1))).await;
    let post = |value: Value| test::TestRequest::post().uri("/tx").set_json(value);

    // udp has no answer: sent is all the rpc can say until the ledger shows it
    let (status, cors, reply) = call(&service, post(body(&tx(1)))).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(cors.as_deref(), Some("*"));
    assert_eq!(reply["state"], "pending");
    assert_eq!(reply["status"], "sent");
    assert_eq!(reply["known"], false);
    assert_eq!(reply["attempts"], 1);
    assert!(reply["leader"].as_u64().unwrap() < 5, "{reply}");
    assert!(reply["target_lane"].as_u64().unwrap() < 5, "{reply}");
    assert_eq!(reply["slot"], Value::Null, "not committed yet");
    assert_eq!(reply["lane"], Value::Null, "the committed lane stays empty");

    let (status, _, again) = call(&service, post(body(&tx(1)))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again["known"], true);
    assert_eq!(again["attempts"], 1);

    let (status, _, full) = call(&service, post(body(&tx(2)))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{full}");
    assert!(full["error"].as_str().unwrap().contains("in flight"));

    let get = test::TestRequest::get().uri(&format!("/tx/{}", hash_hex(&tx(1))));
    let (status, _, view) = call(&service, get).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(view["tx_hash"], hash_hex(&tx(1)));
    assert_eq!(view["nonce"], 1);
    let history = view["history"].as_array().unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0]["status"], "sent");
    assert_eq!(history[0]["leader"], reply["leader"]);
    assert_eq!(history[0]["target_lane"], reply["target_lane"]);
    assert_eq!(history[0]["target_slot"], reply["target_slot"]);

    let (status, _, health) = call(&service, test::TestRequest::get().uri("/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(health["ok"], true, "{health}");
    assert_eq!(health["in_flight"], 1);
    assert_eq!(health["node"]["id"], 0);
    assert_eq!(health["node"]["last_status"], "sent");
}

#[actix_web::test]
async fn a_send_the_socket_refuses_is_202_resent_later_and_unhealthy() {
    let dir = tempfile::tempdir().unwrap();
    // broadcast without SO_BROADCAST: the kernel refuses the datagram
    let node = node_config(0, &["255.255.255.255:9".parse().unwrap()], unix_now());
    let config = RpcConfig::colocated(&node, dir.path().join("ledger")).unwrap();
    let state = web::Data::new(RpcState::new(config).unwrap());
    let service = test::init_service(app(state)).await;

    let post = test::TestRequest::post().uri("/tx").set_json(body(&tx(1)));
    let (status, _, reply) = call(&service, post).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{reply}");
    assert_eq!(reply["state"], "pending");
    assert_eq!(reply["status"], "error");
    assert_eq!(reply["known"], false);

    let (status, _, health) = call(&service, test::TestRequest::get().uri("/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(health["ok"], false, "{health}");
    assert_eq!(health["node"]["last_status"], "error");
    assert!(health["node"]["last_error"].is_string(), "{health}");
}

#[actix_web::test]
async fn health_names_the_colocated_node_before_any_send() {
    let dir = tempfile::tempdir().unwrap();
    let swarm = FakeSwarm::spawn(3).await;
    let service = test::init_service(app(state(&swarm, dir.path(), 10))).await;
    let (status, _, health) = call(&service, test::TestRequest::get().uri("/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(health["ok"], true, "{health}");
    assert_eq!(health["node"]["id"], 0);
    assert_eq!(health["node"]["last_status"], Value::Null);
    assert!(health["maintenance_age_ms"].is_u64());
    // what the rpc routes with, for tooling now that the node has no --schedule
    assert_eq!(
        health["schedule"],
        json!({
            "concurrent_proposers": 5,
            "observation_cutoff": 5,
            "rotation_slack": 3,
            "slots_per_epoch": 400,
            "horizon": 40,
            "slot_interval_ms": 100,
            "lead_ms": 350,
            "genesis_ms": (swarm.genesis.as_nanos() / 1_000_000) as u64,
        }),
        "{health}"
    );
}

#[actix_web::test]
async fn preflights_are_answered_on_every_path() {
    let dir = tempfile::tempdir().unwrap();
    let swarm = FakeSwarm::spawn(1).await;
    let service = test::init_service(app(state(&swarm, dir.path(), 10))).await;
    for uri in ["/tx", "/tx/0xab", "/health"] {
        let request = test::TestRequest::default()
            .method(Method::OPTIONS)
            .uri(uri)
            .insert_header((header::ORIGIN, "http://127.0.0.1:8090"))
            .insert_header((header::ACCESS_CONTROL_REQUEST_METHOD, "POST"))
            .insert_header((header::ACCESS_CONTROL_REQUEST_HEADERS, "content-type"))
            .to_request();
        let response = test::call_service(&service, request).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT, "{uri}");
        let headers = response.headers();
        assert_eq!(
            headers.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "*"
        );
        assert!(
            headers
                .get(header::ACCESS_CONTROL_ALLOW_METHODS)
                .unwrap()
                .to_str()
                .unwrap()
                .contains("POST")
        );
        assert_eq!(
            headers.get(header::ACCESS_CONTROL_ALLOW_HEADERS).unwrap(),
            "content-type"
        );
    }
}
