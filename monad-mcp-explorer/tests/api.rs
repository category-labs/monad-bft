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

mod common;

use std::{
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};

use actix_web::{
    App, HttpResponse, HttpServer,
    http::{StatusCode, header},
    test as atest, web,
};
use bytes::Bytes;
use common::*;
use monad_mcp_chorus::ledger::encode_batch; // demo(tx-timeline)
use monad_mcp_chorus::ledger::{
    BLOCKS_DIR, LedgerWriter, MAX_TX_PAYLOAD, block_dir_name, lane_file_name,
};
use monad_mcp_explorer::{api::MAX_SEND_BODY, index::IndexConfig, loader::LoaderConfig};
use serde_json::{Value, json};
use tempfile::TempDir;

fn hex0x(b: &[u8]) -> String {
    format!("0x{}", hex::encode(b))
}

// demo(tx-timeline)
fn received_at(ms: u64) -> u64 {
    u64::try_from(GENESIS_NS).unwrap() + ms * 1_000_000
}

// demo(tx-timeline): a tx's phases in a `write_block` slot (sealed 20 ms before the deadline);
// only fast slots have a fast voting phase.
fn phases(slot: u64, received_ms: Option<f64>) -> Value {
    let genesis_ms = (GENESIS_NS / 1_000_000) as f64;
    let deadline = genesis_ms + 100.0 * slot as f64;
    assert_eq!(sealed_at(slot) / 1_000_000, (deadline - 20.0) as u64);
    let sealed = deadline - 20.0;
    let fast = fast_block_at(slot).map(|_| deadline + 7.0);
    let finalized = deadline + 12.0;
    let all = [
        (
            "mempool",
            received_ms.map(|ms| genesis_ms + ms),
            Some(sealed),
        ),
        ("proposing", Some(sealed), Some(deadline)),
        ("fast_voting", Some(deadline), fast),
        ("finalizing", fast.or(Some(deadline)), Some(finalized)),
    ];
    let out = all.into_iter().filter_map(|(name, start, end)| {
        let (start, end) = (start?, end?);
        Some(json!({"name": name, "start_ms": start, "end_ms": end, "duration_ms": end - start}))
    });
    Value::Array(out.collect())
}

struct Fixture {
    _dir: TempDir,
    explorer: Explorer,
    hello: monad_mcp_chorus::ledger::Tx,
    binary: monad_mcp_chorus::ledger::Tx,
    resent: monad_mcp_chorus::ledger::Tx,
}

// slots 1, 2, 4, 5 (3 is missing); slot 4 carries txs, a negative lane and a garbage lane.
fn fixture() -> Fixture {
    let dir = TempDir::new().unwrap();
    let writer = LedgerWriter::open(dir.path()).unwrap();
    let hello = tx(0xaa, 1, "hello chorus");
    let binary = tx(0xbb, 2, Bytes::from(vec![0xff; 100]));
    let mut resent = tx(0xaa, 3, "resent");
    resent.received_at_ns = received_at(150); // demo(tx-timeline)
    write_empty(&writer, 1);
    write_block(
        &writer,
        2,
        vec![Lane::Txs(vec![resent.clone()]), Lane::Negative],
    );
    write_block(
        &writer,
        4,
        vec![
            Lane::Txs(vec![hello.clone(), binary.clone(), resent.clone()]),
            Lane::Negative,
            Lane::Raw(Bytes::from_static(b"\xde\xad")),
            Lane::Txs(vec![]),
        ],
    );
    write_empty(&writer, 5);
    let explorer = start(dir.path(), IndexConfig::default(), fast_loader());
    explorer.wait_booted();
    Fixture {
        _dir: dir,
        explorer,
        hello,
        binary,
        resent,
    }
}

#[actix_web::test]
async fn stats_and_blocks() {
    let f = fixture();
    let app = f.explorer.app().await;

    let stats = ok(&app, "/api/stats").await;
    assert_eq!(
        stats["indexing"],
        json!({"done": 4, "total": 4, "complete": true})
    );
    assert_eq!(
        (stats["head"].as_u64(), stats["oldest"].as_u64()),
        (Some(5), Some(1))
    );
    assert_eq!(stats["missing_slots"], 1);
    assert_eq!(stats["totals"]["blocks"], 4);
    assert_eq!(stats["totals"]["txs"], 4);
    assert_eq!(stats["totals"]["decode_errors"], 1);
    assert_eq!(stats["totals"]["fallback"], 1);
    assert_eq!(stats["block_times_ms"], json!([100.0, 200.0, 100.0]));
    let w = &stats["windows"][0];
    assert_eq!(
        (w["size"].as_u64(), w["blocks"].as_u64()),
        (Some(100), Some(4))
    );
    assert_eq!(w["latency_ms"]["p50"], 12.0);
    assert_eq!(w["fast_ratio"], 0.75);

    let blocks = ok(&app, "/api/blocks").await;
    let slots: Vec<u64> = blocks["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["slot"].as_u64().unwrap())
        .collect();
    assert_eq!(slots, vec![5, 4, 2, 1]);
    assert_eq!(blocks["has_more"], false);
    let b4 = &blocks["blocks"][1];
    assert_eq!(b4["num_lanes"], 4);
    assert_eq!(b4["positive_lanes"], 3);
    assert_eq!(b4["lanes_with_txs"], 1);
    assert_eq!(b4["tx_count"], 3);
    assert_eq!(b4["latency_ms"], 12.0);
    assert_eq!(b4["path"], "fast");
    assert_eq!(
        b4["finalized_at_ms"].as_u64(),
        Some((finalized_at(4) / 1_000_000) as u64)
    );

    let page = ok(&app, "/api/blocks?before=5&limit=2").await;
    assert_eq!(page["blocks"][0]["slot"], 4);
    assert_eq!(page["blocks"][1]["slot"], 2);
    assert_eq!(page["has_more"], true);
    let page = ok(&app, "/api/blocks?after=1&limit=2").await;
    assert_eq!(page["blocks"][0]["slot"], 4);
    assert_eq!(page["has_more"], true);
    let page = ok(&app, "/api/blocks?after=5").await;
    assert_eq!(page["blocks"], json!([]));
    assert_eq!(page["head"], 5);

    for bad in [
        "/api/blocks?before=1&after=2",
        "/api/blocks?before=x",
        "/api/blocks?limit=-1",
        "/api/txs?after=1.2",
        "/api/block/abc",
        "/api/tx/0x1234",
        "/api/sender/0x12",
    ] {
        let (status, body, _) = get(&app, bad).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}: {body}");
        assert!(body["error"].is_string());
    }
    let (status, body, _) = get(&app, "/api/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "no such endpoint");
}

#[actix_web::test]
async fn block_detail_lanes_and_proof() {
    let f = fixture();
    let app = f.explorer.app().await;

    let b = ok(&app, "/api/block/4").await;
    assert_eq!(b["slot"], 4);
    assert_eq!(b["on_disk"], true);
    assert_eq!((b["prev"].as_u64(), b["next"].as_u64()), (Some(2), Some(5)));
    assert_eq!(b["gap_before"], 1);
    assert_eq!(b["proof_size"], proof_of(4).len());
    assert_eq!(b["finalized_at_ns"], finalized_at(4).to_string());
    let lanes = b["lanes"].as_array().unwrap();
    assert_eq!(lanes.len(), 4);
    assert_eq!(lanes[0]["proposer"], 100);
    assert_eq!(lanes[0]["root"], hex0x(&[1; 20]));
    assert_eq!(lanes[0]["tx_count"], 3);
    assert_eq!(lanes[0]["txs"][0]["hash"], hex0x(&f.hello.hash()));
    assert_eq!(lanes[0]["more_txs"], false);
    assert_eq!(lanes[1]["positive"], false);
    assert_eq!(lanes[1]["root"], Value::Null);
    assert_eq!(lanes[2]["decode_error"], true);
    assert_eq!(lanes[2]["payload_len"], 2);
    assert_eq!(lanes[3]["positive"], true);
    assert_eq!(lanes[3]["tx_count"], 0);

    let (status, body, _) = get(&app, "/api/block/3").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(body["error"].is_string(), "{body}");

    let lane = ok(&app, "/api/block/4/lane/0?limit=2").await;
    assert_eq!(lane["tx_count"], 3);
    assert_eq!(lane["txs"].as_array().unwrap().len(), 2);
    assert_eq!(lane["next_cursor"], 2);
    let rest = ok(&app, "/api/block/4/lane/0?cursor=2").await;
    assert_eq!(rest["txs"][0]["hash"], hex0x(&f.resent.hash()));
    assert_eq!(rest["next_cursor"], Value::Null);
    let neg = ok(&app, "/api/block/4/lane/1").await;
    assert_eq!(neg["txs"], json!([]));
    let (status, _, _) = get(&app, "/api/block/4/lane/4").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let proof = ok(&app, "/api/block/4/proof").await;
    assert_eq!(proof["proof"], hex0x(&proof_of(4)));
    assert_eq!(proof["size"], proof_of(4).len());
    let (status, _, _) = get(&app, "/api/block/3/proof").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn txs_payloads_senders_and_search() {
    let f = fixture();
    let app = f.explorer.app().await;

    let txs = ok(&app, "/api/txs").await;
    let list = txs["txs"].as_array().unwrap();
    let cursors: Vec<&str> = list.iter().map(|t| t["cursor"].as_str().unwrap()).collect();
    assert_eq!(cursors, vec!["4.0.2", "4.0.1", "4.0.0", "2.0.0"]);
    let hello = &list[2];
    assert_eq!(hello["sender"], hex0x(&[0xaa; 20]));
    assert_eq!(hello["size"], 12);
    assert_eq!(hello["preview_text"], "hello chorus");
    assert_eq!(hello.get("preview"), None);
    let binary = &list[1];
    assert_eq!(binary["preview"], hex0x(&[0xff; 32]));
    assert_eq!(binary.get("preview_text"), None);
    let page = ok(&app, "/api/txs?before=4.0.0").await;
    assert_eq!(page["txs"][0]["cursor"], "2.0.0");
    let page = ok(&app, "/api/txs?after=4.0.0&limit=1").await;
    assert_eq!(page["txs"][0]["cursor"], "4.0.1");
    assert_eq!(page["has_more"], true);

    let t = ok(&app, &format!("/api/tx/{}", hex0x(&f.hello.hash()))).await;
    assert_eq!(t["payload"], hex0x(b"hello chorus"));
    assert_eq!(t["payload_utf8"], "hello chorus");
    assert_eq!(t["payload_hash"], hex0x(&f.hello.payload_hash()));
    assert_eq!(
        (t["slot"].as_u64(), t["lane"].as_u64(), t["pos"].as_u64()),
        (Some(4), Some(0), Some(0))
    );
    assert_eq!(t["nonce"], 1);
    let t = ok(
        &app,
        &format!("/api/tx/{}", hex::encode(f.binary.hash()).to_uppercase()),
    )
    .await;
    assert_eq!(t["payload"], hex0x(&[0xff; 100]));
    assert_eq!(t["payload_utf8"], Value::Null);
    let t = ok(&app, &format!("/api/tx/{}", hex0x(&f.resent.hash()))).await;
    assert_eq!(t["slot"], 2);
    // demo(tx-timeline)
    assert_eq!(
        t["inclusions"],
        json!([
            {"slot": 2, "lane": 0, "pos": 0, "phases": phases(2, Some(150.0))},
            {"slot": 4, "lane": 0, "pos": 2, "phases": phases(4, Some(150.0))},
        ])
    );
    // demo(tx-timeline): a fast slot has all four phases, an unstamped tx no mempool phase
    let names = |p: &Value| {
        p.as_array()
            .unwrap()
            .iter()
            .map(|p| p["name"].clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        names(&t["inclusions"][0]["phases"]),
        ["mempool", "proposing", "fast_voting", "finalizing"]
    );
    assert_eq!(t["inclusions"][0]["phases"][0]["duration_ms"], 30.0);
    let t = ok(&app, &format!("/api/tx/{}", hex0x(&f.hello.hash()))).await;
    assert_eq!(t["inclusions"][0]["phases"], phases(4, None));
    assert_eq!(
        names(&t["inclusions"][0]["phases"]),
        ["proposing", "fast_voting", "finalizing"]
    );
    // end demo(tx-timeline)
    let (status, _, _) = get(&app, &format!("/api/tx/{}", hex0x(&[9; 32]))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let p = ok(
        &app,
        &format!("/api/payload/{}", hex0x(&f.resent.payload_hash())),
    )
    .await;
    assert_eq!(p["tx_count"], 2);
    assert_eq!(p["txs"][0]["slot"], 4);
    let p = ok(
        &app,
        &format!("/api/payload/{}?limit=1", hex0x(&f.resent.payload_hash())),
    )
    .await;
    assert_eq!(p["next_cursor"], "4.0.2");
    let p = ok(
        &app,
        &format!(
            "/api/payload/{}?cursor=4.0.2",
            hex0x(&f.resent.payload_hash())
        ),
    )
    .await;
    assert_eq!(p["txs"][0]["slot"], 2);
    assert_eq!(p["next_cursor"], Value::Null);

    let s = ok(&app, &format!("/api/sender/{}", hex0x(&[0xaa; 20]))).await;
    assert_eq!(s["tx_count"], 3);
    assert_eq!(s["id"], hex0x(&[0xaa; 20]));
    let s = ok(&app, &format!("/api/sender/{}", hex0x(&[0x01; 20]))).await;
    assert_eq!(s["tx_count"], 0);

    let search = |q: String| {
        let app = &app;
        async move { ok(app, &format!("/api/search?q={q}")).await }
    };
    assert_eq!(search("4".into()).await, json!({"kind": "block", "id": 4}));
    assert_eq!(search("3".into()).await, json!({"kind": "none"}));
    assert_eq!(
        search(hex::encode(f.hello.hash())).await,
        json!({"kind": "tx", "id": hex0x(&f.hello.hash())})
    );
    assert_eq!(
        search(hex0x(&f.hello.payload_hash())).await,
        json!({"kind": "payload", "id": hex0x(&f.hello.payload_hash())})
    );
    assert_eq!(
        search(format!("%20{}%20", hex0x(&[0xbb; 20]))).await,
        json!({"kind": "sender", "id": hex0x(&[0xbb; 20])})
    );
    assert_eq!(search("zzz".into()).await, json!({"kind": "none"}));

    let config = ok(&app, "/api/config").await;
    assert_eq!(config["rpc_url"], "http://rpc.test:1234");
    assert_eq!(config["max_limit"], 100);
}

#[actix_web::test]
async fn search_digit_only_hex_is_not_a_slot() {
    let dir = TempDir::new().unwrap();
    let writer = LedgerWriter::open(dir.path()).unwrap();
    write_block(&writer, 7, vec![Lane::Txs(vec![tx(0x11, 0, "digits")])]);
    let explorer = start(dir.path(), IndexConfig::default(), fast_loader());
    explorer.wait_booted();
    let app = explorer.app().await;
    let q = hex::encode([0x11; 20]);
    assert_eq!(
        ok(&app, &format!("/api/search?q={q}")).await,
        json!({"kind": "sender", "id": hex0x(&[0x11; 20])})
    );
    assert_eq!(
        ok(&app, "/api/search?q=7").await,
        json!({"kind": "block", "id": 7})
    );
}

#[actix_web::test]
async fn tx_payload_read_failure_is_reported() {
    let f = fixture();
    let app = f.explorer.app().await;
    let lane = f.explorer.state.reader.block_dir(4).join(lane_file_name(0));
    fs::remove_file(lane).unwrap();
    let t = ok(&app, &format!("/api/tx/{}", hex0x(&f.hello.hash()))).await;
    assert_eq!(t["payload"], Value::Null);
    assert!(
        t["payload_error"]
            .as_str()
            .unwrap()
            .contains("lane file is missing")
    );
    // the summary still comes from the index.
    assert_eq!(t["preview_text"], "hello chorus");
}

// demo(tx-timeline): a fallback block has no fast voting phase; finalizing starts at the deadline,
// and a skewed rpc clock gives a negative mempool phase
#[actix_web::test]
async fn tx_phases_on_a_fallback_block() {
    let dir = TempDir::new().unwrap();
    let writer = LedgerWriter::open(dir.path()).unwrap();
    let mut late = tx(0xcc, 1, "late");
    late.received_at_ns = received_at(1_003);
    write_block(&writer, 10, vec![Lane::Txs(vec![late.clone()])]);
    let explorer = start(dir.path(), IndexConfig::default(), fast_loader());
    explorer.wait_booted();
    let app = explorer.app().await;
    let t = ok(&app, &format!("/api/tx/{}", hex0x(&late.hash()))).await;
    let phases_json = &t["inclusions"][0]["phases"];
    assert_eq!(*phases_json, phases(10, Some(1_003.0)));
    let names: Vec<_> = phases_json
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].clone())
        .collect();
    assert_eq!(names, ["mempool", "proposing", "finalizing"]);
    assert_eq!(phases_json[0]["duration_ms"], -23.0);
    assert_eq!(phases_json[2]["duration_ms"], 12.0);
}

// demo(tx-timeline): a lane without a seal time drops the phases that start or end on it
#[actix_web::test]
async fn tx_phases_without_a_seal_time() {
    let dir = TempDir::new().unwrap();
    let writer = LedgerWriter::open(dir.path()).unwrap();
    let mut unsealed = tx(0xdd, 1, "unsealed");
    unsealed.received_at_ns = received_at(250);
    let payload = encode_batch(0, std::slice::from_ref(&unsealed));
    write_block(&writer, 3, vec![Lane::Raw(payload)]);
    let explorer = start(dir.path(), IndexConfig::default(), fast_loader());
    explorer.wait_booted();
    let app = explorer.app().await;
    let t = ok(&app, &format!("/api/tx/{}", hex0x(&unsealed.hash()))).await;
    let names: Vec<_> = t["inclusions"][0]["phases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].clone())
        .collect();
    assert_eq!(names, ["fast_voting", "finalizing"]);
}

#[actix_web::test]
async fn static_assets_revalidate() {
    let f = fixture();
    let app = f.explorer.app().await;
    for (uri, ty) in [
        ("/", "text/html"),
        ("/app.css", "text/css"),
        ("/app.js", "application/javascript"),
    ] {
        let resp = atest::call_service(&app, atest::TestRequest::get().uri(uri).to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK, "{uri}");
        let headers = resp.headers();
        assert!(
            headers
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(ty)
        );
        let etag = headers.get(header::ETAG).unwrap().clone();
        assert!(!atest::read_body(resp).await.is_empty());
        let again = atest::TestRequest::get()
            .uri(uri)
            .insert_header((header::IF_NONE_MATCH, etag))
            .to_request();
        let resp = atest::call_service(&app, again).await;
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED, "{uri}");
        assert!(atest::read_body(resp).await.is_empty());
    }
    let resp = atest::call_service(&app, atest::TestRequest::get().uri("/").to_request()).await;
    let html = String::from_utf8(atest::read_body(resp).await.to_vec()).unwrap();
    let gz = atest::TestRequest::get()
        .uri("/")
        .insert_header((header::ACCEPT_ENCODING, "br;q=1, gzip;q=0.8"))
        .to_request();
    let resp = atest::call_service(&app, gz).await;
    assert_eq!(
        resp.headers().get(header::CONTENT_ENCODING).unwrap(),
        "gzip"
    );
    let body = atest::read_body(resp).await;
    let mut unzipped = String::new();
    std::io::Read::read_to_string(&mut flate2::read::GzDecoder::new(&body[..]), &mut unzipped)
        .unwrap();
    assert_eq!(unzipped, html);
    for id in [
        "live-toggle",
        "refresh-button",
        "live-status",
        "search-form",
        "view",
    ] {
        assert!(html.contains(&format!("data-testid=\"{id}\"")), "{id}");
    }
}

#[actix_web::test]
async fn limit_is_clamped_and_lists_carry_no_payloads() {
    let dir = TempDir::new().unwrap();
    let writer = LedgerWriter::open(dir.path()).unwrap();
    let big = Bytes::from(vec![b'p'; MAX_TX_PAYLOAD]);
    for slot in 1..=60 {
        let txs = (0..3).map(|n| tx(slot as u8, n, big.clone())).collect();
        write_block(&writer, slot, vec![Lane::Txs(txs)]);
    }
    for slot in 61..=130 {
        write_raw_empty(dir.path(), slot, 5);
    }
    let explorer = start(dir.path(), IndexConfig::default(), fast_loader());
    explorer.wait_booted();
    let app = explorer.app().await;

    let (_, blocks, size) = get(&app, "/api/blocks?limit=1000").await;
    assert_eq!(blocks["blocks"].as_array().unwrap().len(), 100);
    assert_eq!(blocks["has_more"], true);
    let (_, default, size20) = get(&app, "/api/blocks").await;
    assert_eq!(default["blocks"].as_array().unwrap().len(), 20);
    assert!(size20 < 8 * 1024, "20 blocks took {size20} bytes");
    assert!(size < 40 * 1024, "100 blocks took {size} bytes");
    let (_, one, _) = get(&app, "/api/blocks?limit=0").await;
    assert_eq!(one["blocks"].as_array().unwrap().len(), 1);

    let (_, txs, size) = get(&app, "/api/txs?limit=500").await;
    let list = txs["txs"].as_array().unwrap();
    assert_eq!(list.len(), 100);
    let (_, _, size20) = get(&app, "/api/txs").await;
    assert!(size20 < 6 * 1024, "20 txs took {size20} bytes");
    assert!(size < 35 * 1024, "100 txs took {size} bytes");
    let full = hex0x(&big);
    for uri in [
        "/api/txs?limit=100".to_string(),
        "/api/block/10/lane/0?limit=100".into(),
        format!(
            "/api/payload/{}?limit=100",
            hex0x(&monad_mcp_chorus::ledger::payload_hash(&big))
        ),
        format!("/api/sender/{}?limit=100", hex0x(&[10; 20])),
        "/api/block/10".into(),
    ] {
        let (status, body, size) = get(&app, &uri).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        let text = body.to_string();
        assert!(!text.contains(&full), "{uri} leaked a full payload");
        assert!(!text.contains("\"payload\""), "{uri} has a payload field");
        assert!(size < 40 * 1024, "{uri} took {size} bytes");
    }
    let p = ok(
        &app,
        &format!(
            "/api/payload/{}?limit=1000",
            hex0x(&monad_mcp_chorus::ledger::payload_hash(&big))
        ),
    )
    .await;
    assert_eq!(p["tx_count"], 180);
    assert_eq!(p["txs"].as_array().unwrap().len(), 100);
    let block = ok(&app, "/api/block/10").await;
    assert_eq!(block["lanes"][0]["txs"].as_array().unwrap().len(), 3);
    let (_, stats, size) = get(&app, "/api/stats").await;
    assert!(size < 4 * 1024, "stats took {size} bytes: {stats}");
    // the full payload comes only from the tx endpoint.
    let hash = list[0]["hash"].as_str().unwrap();
    let t = ok(&app, &format!("/api/tx/{hash}")).await;
    assert_eq!(t["payload"], full);
}

#[actix_web::test]
async fn boot_respects_retention() {
    let dir = TempDir::new().unwrap();
    let writer = LedgerWriter::open(dir.path()).unwrap();
    for slot in 1..=30 {
        if slot % 10 == 0 {
            write_block(&writer, slot, vec![Lane::Txs(vec![tx(1, slot, "t")])]);
        } else {
            write_empty(&writer, slot);
        }
    }
    let config = IndexConfig {
        retain_slots: 12,
        max_txs: 100,
    };
    let explorer = start(dir.path(), config, fast_loader());
    explorer.wait_booted();
    let app = explorer.app().await;
    let stats = ok(&app, "/api/stats").await;
    assert_eq!(
        (stats["oldest"].as_u64(), stats["head"].as_u64()),
        (Some(19), Some(30))
    );
    assert_eq!(stats["retained_blocks"], 12);
    assert_eq!(stats["totals"]["txs"], 2);
    assert_eq!(stats["indexing"]["total"], 12);
    let (status, _, _) = get(&app, "/api/block/18").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let s = ok(&app, &format!("/api/sender/{}", hex0x(&[1; 20]))).await;
    assert_eq!(s["tx_count"], 2);

    // tailing keeps the window at retain_slots.
    write_empty(&writer, 31);
    wait_until("slot 31", Duration::from_secs(10), || {
        explorer.index.read().contains(31)
    });
    let index = explorer.index.read();
    assert_eq!(index.len(), 12);
    assert_eq!(index.oldest().map(|b| b.slot), Some(20));
    assert_eq!(index.tx_len(), 2);
}

#[actix_web::test]
async fn tx_bounded_window_is_stable_across_rescans() {
    let dir = TempDir::new().unwrap();
    let writer = LedgerWriter::open(dir.path()).unwrap();
    // one tx every 4th slot, empty blocks between, a real gap at 50.
    for slot in (1..=60).filter(|s| *s != 50) {
        if slot % 4 == 0 {
            write_block(&writer, slot, vec![Lane::Txs(vec![tx(1, slot, "t")])]);
        } else {
            write_empty(&writer, slot);
        }
    }
    // corrupt below the window, so it is never recorded as skipped.
    let blocks = dir.path().join(BLOCKS_DIR);
    fs::write(blocks.join(block_dir_name(3)).join("meta.rlp"), b"garbage").unwrap();
    let config = IndexConfig {
        retain_slots: 1000,
        max_txs: 5,
    };
    let loader = LoaderConfig {
        batch: 8,
        boot_threads: 4,
        ..fast_loader()
    };
    let explorer = start(dir.path(), config, loader);
    explorer.wait_booted();
    let app = explorer.app().await;
    // the newest 5 tx blocks are 44..=60; the empty blocks above 40 fit too.
    for _ in 0..10 {
        let stats = ok(&app, "/api/stats").await;
        assert_eq!(
            (stats["oldest"].as_u64(), stats["head"].as_u64()),
            (Some(41), Some(60))
        );
        assert_eq!(stats["missing_slots"], 1);
        assert_eq!(stats["retained_blocks"], 19);
        assert_eq!(stats["totals"]["txs"], 5);
        {
            let index = explorer.index.read();
            assert_eq!(index.floor(), 41);
            assert_eq!(index.skipped_len(), 0);
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // a new tx block evicts the oldest tx block and the empty blocks under it.
    write_block(&writer, 61, vec![Lane::Txs(vec![tx(1, 61, "t")])]);
    wait_until("slot 61", Duration::from_secs(10), || {
        explorer.index.read().contains(61)
    });
    std::thread::sleep(Duration::from_millis(200));
    let index = explorer.index.read();
    assert_eq!(index.oldest().map(|b| b.slot), Some(45));
    assert_eq!((index.len(), index.tx_len()), (16, 5));
}

#[actix_web::test]
async fn tailing_picks_up_new_late_and_distant_blocks() {
    let dir = TempDir::new().unwrap();
    let writer = LedgerWriter::open(dir.path()).unwrap();
    for slot in [10, 11] {
        write_empty(&writer, slot);
    }
    let explorer = start(dir.path(), IndexConfig::default(), fast_loader());
    explorer.wait_booted();
    let contains = |slot| explorer.index.read().contains(slot);

    write_empty(&writer, 12);
    write_block(&writer, 13, vec![Lane::Txs(vec![tx(5, 0, "tail")])]);
    wait_until("new blocks", Duration::from_secs(10), || {
        contains(12) && contains(13)
    });
    // a lower slot written late, and one past a gap wider than the probe window.
    write_empty(&writer, 8);
    write_empty(&writer, 500);
    write_empty(&writer, 501);
    wait_until("late and distant blocks", Duration::from_secs(10), || {
        contains(8) && contains(500) && contains(501)
    });
    let app = explorer.app().await;
    let txs = ok(&app, "/api/txs").await;
    assert_eq!(txs["txs"][0]["preview_text"], "tail");
    let stats = ok(&app, "/api/stats").await;
    assert_eq!(stats["head"], 501);
    assert_eq!(stats["retained_blocks"], 7);
}

#[actix_web::test]
async fn tailing_follows_the_pruner() {
    let dir = TempDir::new().unwrap();
    let writer = LedgerWriter::open(dir.path()).unwrap();
    for slot in 1..=6 {
        write_block(&writer, slot, vec![Lane::Txs(vec![tx(slot as u8, 0, "p")])]);
    }
    let explorer = start(dir.path(), IndexConfig::default(), fast_loader());
    explorer.wait_booted();
    let blocks = dir.path().join(BLOCKS_DIR);
    for slot in 1..=3 {
        fs::remove_dir_all(blocks.join(block_dir_name(slot))).unwrap();
    }
    write_empty(&writer, 7);
    wait_until("prune", Duration::from_secs(10), || {
        let index = explorer.index.read();
        index.oldest().map(|b| b.slot) == Some(4) && index.contains(7)
    });
    assert_eq!(explorer.index.read().tx_len(), 3);
}

#[actix_web::test]
async fn corrupt_and_partial_entries_are_skipped() {
    let dir = TempDir::new().unwrap();
    let writer = LedgerWriter::open(dir.path()).unwrap();
    write_empty(&writer, 1);
    write_block(&writer, 2, vec![Lane::Txs(vec![tx(1, 0, "x")])]);
    write_block(&writer, 3, vec![Lane::Txs(vec![tx(1, 1, "y")])]);
    write_empty(&writer, 6);
    let blocks = dir.path().join(BLOCKS_DIR);
    // 0: garbage meta below every readable block; 2: lane file missing; 3: lane file
    // truncated; 4: no meta; 5: garbage meta.
    fs::create_dir(blocks.join(block_dir_name(0))).unwrap();
    fs::write(blocks.join(block_dir_name(0)).join("meta.rlp"), b"garbage").unwrap();
    fs::remove_file(blocks.join(block_dir_name(2)).join(lane_file_name(0))).unwrap();
    fs::write(
        blocks.join(block_dir_name(3)).join(lane_file_name(0)),
        b"\xc1",
    )
    .unwrap();
    fs::create_dir(blocks.join(block_dir_name(4))).unwrap();
    fs::create_dir(blocks.join(block_dir_name(5))).unwrap();
    fs::write(blocks.join(block_dir_name(5)).join("meta.rlp"), b"garbage").unwrap();
    // foreign and temp entries are not blocks at all.
    fs::create_dir(blocks.join("12345")).unwrap();
    fs::create_dir(blocks.join(format!(".{}.tmp", block_dir_name(7)))).unwrap();
    fs::write(blocks.join(block_dir_name(8)), b"a file").unwrap();

    let explorer = start(dir.path(), IndexConfig::default(), fast_loader());
    explorer.wait_booted();
    // let the tail rescan run over the skipped entries.
    std::thread::sleep(Duration::from_millis(200));
    let index = explorer.index.read();
    let slots: Vec<u64> = index.recent(100).map(|b| b.slot).collect();
    assert_eq!(slots, vec![1, 6]);
    assert_eq!(index.skipped_len(), 5);
    assert!([0, 2, 3, 4, 5].iter().all(|s| index.is_known(*s)));
    assert_eq!(index.tx_len(), 0);
    drop(index);

    // a skipped block repaired in place is not retried, but a new block still is.
    write_raw_empty(dir.path(), 4, 1);
    write_empty(&writer, 9);
    wait_until("slot 9", Duration::from_secs(10), || {
        explorer.index.read().contains(9)
    });
    std::thread::sleep(Duration::from_millis(200));
    assert!(!explorer.index.read().contains(4));
}

#[test]
fn boot_tolerates_the_pruner_deleting_old_blocks() {
    const N: u64 = 20_000;
    let dir = TempDir::new().unwrap();
    for slot in 1..=N {
        write_raw_empty(dir.path(), slot, 3);
    }
    let explorer = start(dir.path(), IndexConfig::default(), fast_loader());
    // deletes oldest first, as cruft does, racing the newest-first boot.
    let blocks = dir.path().join(BLOCKS_DIR);
    let pruner = std::thread::spawn(move || {
        for slot in 1..=N / 2 {
            fs::remove_dir_all(blocks.join(block_dir_name(slot))).unwrap();
        }
    });
    explorer.wait_booted();
    pruner.join().unwrap();
    wait_until("prune after boot", Duration::from_secs(30), || {
        let index = explorer.index.read();
        index.oldest().map(|b| b.slot) == Some(N / 2 + 1) && index.len() == (N / 2) as usize
    });
    let index = explorer.index.read();
    assert_eq!(index.newest().map(|b| b.slot), Some(N));
    assert_eq!(index.skipped_len(), 0);
}

#[actix_web::test]
async fn missing_ledger_dir_boots_empty_then_fills() {
    let dir = TempDir::new().unwrap();
    let explorer = start(
        &dir.path().join("ledger"),
        IndexConfig::default(),
        fast_loader(),
    );
    explorer.wait_booted();
    let app = explorer.app().await;
    let stats = ok(&app, "/api/stats").await;
    assert_eq!(stats["head"], Value::Null);
    assert_eq!(stats["windows"], json!([]));
    assert_eq!(ok(&app, "/api/blocks").await["blocks"], json!([]));

    let writer = LedgerWriter::open(dir.path().join("ledger")).unwrap();
    write_empty(&writer, 40);
    wait_until("first block", Duration::from_secs(10), || {
        explorer.index.read().contains(40)
    });
}

// a stand-in rpc that records each POST /tx body; `{}` gets a 400, anything else a 202.
fn stub_rpc() -> (String, Arc<Mutex<Vec<Bytes>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorder = seen.clone();
    let server = HttpServer::new(move || {
        let recorder = recorder.clone();
        App::new().route(
            "/tx",
            web::post().to(move |body: Bytes| {
                let recorder = recorder.clone();
                async move {
                    let bad = body.as_ref() == b"{}";
                    recorder.lock().unwrap().push(body);
                    if bad {
                        HttpResponse::BadRequest().json(json!({"error": "no payload"}))
                    } else {
                        HttpResponse::Accepted().json(json!({"tx_hash": "0xab", "status": "sent"}))
                    }
                }
            }),
        )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let addr = server.addrs()[0];
    actix_web::rt::spawn(server.run());
    (format!("http://{addr}"), seen)
}

async fn post<S, B>(app: &S, body: impl Into<Bytes>) -> (StatusCode, Option<String>, Bytes)
where
    S: actix_web::dev::Service<
            actix_http::Request,
            Response = actix_web::dev::ServiceResponse<B>,
            Error = actix_web::Error,
        >,
    B: actix_web::body::MessageBody,
{
    let req = atest::TestRequest::post()
        .uri("/api/tx")
        .set_payload(body.into())
        .to_request();
    let resp = atest::call_service(app, req).await;
    let ct = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap().to_owned());
    (resp.status(), ct, atest::read_body(resp).await)
}

#[actix_web::test]
async fn send_is_forwarded_to_the_rpc_with_its_reply() {
    let dir = TempDir::new().unwrap();
    let (rpc, seen) = stub_rpc();
    let explorer = start_with_rpc(dir.path(), IndexConfig::default(), fast_loader(), &rpc);
    let app = explorer.app().await;

    let body = r#"{"payload_utf8":"via the explorer"}"#;
    let (status, ct, reply) = post(&app, body).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(ct.as_deref(), Some("application/json"));
    let reply: Value = serde_json::from_slice(&reply).unwrap();
    assert_eq!(reply, json!({"tx_hash": "0xab", "status": "sent"}));

    // an rpc rejection keeps its status and error body
    let (status, _, reply) = post(&app, "{}").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<Value>(&reply).unwrap(),
        json!({"error": "no payload"})
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec![Bytes::from(body), Bytes::from_static(b"{}")]
    );
}

#[actix_web::test]
async fn send_without_a_reachable_rpc_is_an_error() {
    let dir = TempDir::new().unwrap();

    let explorer = start_with_rpc(dir.path(), IndexConfig::default(), fast_loader(), "");
    let app = explorer.app().await;
    let (status, _, reply) = post(&app, "{}").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        serde_json::from_slice::<Value>(&reply).unwrap(),
        json!({"error": "no rpc url configured"})
    );

    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let explorer = start_with_rpc(dir.path(), IndexConfig::default(), fast_loader(), &url);
    let app = explorer.app().await;
    let (status, _, reply) = post(&app, "{}").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let reply: Value = serde_json::from_slice(&reply).unwrap();
    assert!(reply["error"].as_str().unwrap().contains(&url), "{reply}");
}

#[actix_web::test]
async fn send_body_over_the_cap_is_not_forwarded() {
    let dir = TempDir::new().unwrap();
    let (rpc, seen) = stub_rpc();
    let explorer = start_with_rpc(dir.path(), IndexConfig::default(), fast_loader(), &rpc);
    let app = explorer.app().await;
    let (status, _, _) = post(&app, vec![b'a'; MAX_SEND_BODY + 1]).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let (status, _, _) = post(&app, vec![b'a'; MAX_SEND_BODY]).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[test]
fn large_ledger_boots_within_bound() {
    const N: u64 = 100_000;
    let dir = TempDir::new().unwrap();
    for slot in 1..=N {
        write_raw_empty(dir.path(), slot, 5);
    }
    let started = std::time::Instant::now();
    let explorer = start(dir.path(), IndexConfig::default(), fast_loader());
    wait_until("boot", Duration::from_secs(60), || {
        explorer.progress.is_complete()
    });
    let elapsed = started.elapsed();
    let index = explorer.index.read();
    assert_eq!(index.len(), N as usize);
    assert_eq!(index.totals().positive_lanes, 3 * N);
    assert!(elapsed < Duration::from_secs(15), "boot took {elapsed:?}");
    eprintln!("booted {N} blocks in {elapsed:?}");
}
