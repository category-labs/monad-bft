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

//! The `mcp-tx` binary against fake udp validators and a real rpc.

mod common;

use std::{
    process::{Command, Output},
    time::Duration,
};

use common::*;
use monad_mcp_chorus::ledger::MAX_TX_PAYLOAD;
use monad_mcp_node::chorus::types::NodeId;
use serde_json::Value;

async fn mcp_tx(args: Vec<String>) -> Output {
    tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_mcp-tx"))
            .args(args)
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}

fn args(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| s.to_string()).collect()
}

fn lines(output: &Output) -> Vec<Value> {
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_oversized_payload_is_refused_before_anything_is_sent() {
    let node = FakeNode::spawn().await;
    let addr = node.addr.to_string();
    let over = "a".repeat(MAX_TX_PAYLOAD + 1);
    for target in [
        vec!["--node", &addr, "--sender-id", "0"],
        vec!["--rpc", "http://127.0.0.1:1"],
    ] {
        let output = mcp_tx(args(&[&["send", "--payload", &over][..], &target].concat())).await;
        assert!(!output.status.success());
        let message = stderr(&output);
        assert!(message.contains("payload is 1025 bytes"), "{message}");
        assert!(message.contains("1024-byte limit"), "{message}");
        assert!(message.contains("not sending"), "{message}");
        assert!(output.stdout.is_empty());
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(node.frames() + node.junk() as usize, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_burst_goes_straight_to_a_node_over_udp() {
    let node = FakeNode::spawn().await;
    let sender = "33".repeat(20);
    let output = mcp_tx(args(&[
        "send",
        "--node",
        &node.addr.to_string(),
        "--sender-id",
        "3",
        "--sender",
        &sender,
        "--nonce",
        "10",
        "--count",
        "3",
        "--interval",
        "20",
        "--payload",
        "hello",
    ]))
    .await;
    assert!(output.status.success(), "{}", stderr(&output));
    let lines = lines(&output);
    assert_eq!(lines.len(), 3);
    let arrivals = eventually(Duration::from_secs(2), "three frames", || async {
        let arrivals = node.arrivals();
        (arrivals.len() == 3).then_some(arrivals)
    })
    .await;
    for (i, (line, arrival)) in lines.iter().zip(&arrivals).enumerate() {
        assert_eq!(line["status"], "sent", "{line}");
        assert_eq!(line["node"], node.addr.to_string(), "{line}");
        assert_eq!(line["sender_id"], 3, "{line}");
        assert_eq!(line["tx_hash"], hash_hex(&arrival.tx));
        assert_eq!(arrival.from, NodeId::dummy(3));
        assert_eq!(arrival.tx.nonce, 10 + i as u64);
        assert_eq!(arrival.tx.sender, [0x33; 20]);
        assert_eq!(&arrival.tx.payload[..], b"hello");
    }
    assert_eq!(node.junk(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_sends_need_a_node_and_a_sender_id_and_nothing_else() {
    for bad in [
        vec!["--node", "127.0.0.1:9"],
        vec!["--sender-id", "0"],
        vec!["--node", "not-an-address", "--sender-id", "0"],
        vec!["--node", "127.0.0.1:9", "--sender-id", "x"],
        vec![
            "--node",
            "127.0.0.1:9",
            "--sender-id",
            "0",
            "--rpc",
            "http://x",
        ],
        vec!["--node", "127.0.0.1:9", "--sender-id", "0", "--wait", "5"],
        // the unix socket ingress is gone
        vec!["--socket", "/tmp/ingress.sock"],
    ] {
        let output = mcp_tx(args(&[&["send", "--payload", "x"][..], &bad].concat())).await;
        assert!(!output.status.success(), "{bad:?}");
        assert!(output.stdout.is_empty(), "{bad:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_wait_and_status_go_through_the_rpc() {
    let swarm = FakeSwarm::spawn(1).await;
    let dir = tempfile::tempdir().unwrap();
    let config = rpc_config(&swarm.config(), dir.path(), 5_000, 3);
    let ledger = config.ledger_dir.clone();
    let (rpc, _) = spawn_rpc(config);

    let send = tokio::spawn(mcp_tx(args(&[
        "send",
        "--rpc",
        &format!("{rpc}/"),
        "--count",
        "2",
        "--payload-hex",
        "0xc0ffee",
        "--wait",
        "10",
    ])));
    let txs = eventually(Duration::from_secs(5), "two frames", || async {
        let arrivals = swarm.nodes[0].arrivals();
        (arrivals.len() == 2).then(|| arrivals.into_iter().map(|a| a.tx).collect::<Vec<_>>())
    })
    .await;
    write_block(&ledger, 3, &[&txs]);
    let output = send.await.unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
    let printed = lines(&output);
    assert_eq!(printed.len(), 4, "{printed:?}");
    for (sent, done) in printed[..2].iter().zip(&printed[2..]) {
        assert_eq!(sent["status"], "sent");
        assert_eq!(sent["leader"], 0);
        assert_eq!(sent["known"], false);
        assert_eq!(done["tx_hash"], sent["tx_hash"]);
        assert_eq!(done["state"], "committed");
        assert_eq!(done["slot"], 3);
        assert_eq!(done["payload_len"], 3);
    }
    assert_eq!(printed[0]["sender"], printed[1]["sender"]);

    let hash = printed[0]["tx_hash"].as_str().unwrap().to_owned();
    let output = mcp_tx(args(&["status", &hash, "--rpc", &rpc])).await;
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(lines(&output)[0]["state"], "committed");

    let unknown = format!("0x{}", "ab".repeat(32));
    let output = mcp_tx(args(&["status", &unknown, "--rpc", &rpc])).await;
    assert!(!output.status.success());
    assert!(stderr(&output).contains("404"), "{}", stderr(&output));

    let output = mcp_tx(args(&["status", "0x12", "--rpc", &rpc])).await;
    assert!(!output.status.success());
    assert!(stderr(&output).contains("32 bytes"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiting_on_a_tx_that_never_commits_fails() {
    let swarm = FakeSwarm::spawn(1).await;
    let dir = tempfile::tempdir().unwrap();
    let (rpc, _) = spawn_rpc(rpc_config(&swarm.config(), dir.path(), 5_000, 3));
    let output = mcp_tx(args(&[
        "send",
        "--rpc",
        &rpc,
        "--payload",
        "x",
        "--wait",
        "1",
    ]))
    .await;
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("1 of 1 txs not committed"),
        "{}",
        stderr(&output)
    );
    let lines = lines(&output);
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[1]["state"], "pending");
}
