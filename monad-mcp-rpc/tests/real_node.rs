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

//! L4b: the rpc in front of real in-process nodes over udp, including a node
//! restart and a swarm where the rpc picks among several proposers.

mod common;

use std::{
    collections::BTreeSet,
    net::{SocketAddr, UdpSocket},
    path::Path,
    time::{Duration, Instant},
};

use common::*;
use monad_mcp_chorus::ledger::{LedgerReader, Tx, decode_batch};
use monad_mcp_node::{
    chorus::types::Timestamp,
    config::{LedgerConfig, NodeConfig},
    run_node,
};
use monad_mcp_rpc::api::TxView;
use tokio::task::JoinHandle;

const COMMIT_WITHIN: Duration = Duration::from_secs(20);

fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn with_ledger(mut config: NodeConfig, dir: &Path, node: u64) -> NodeConfig {
    config.ledger = LedgerConfig {
        dir: ledger_dir(dir, node),
    };
    config
}

fn ledger_dir(dir: &Path, node: u64) -> std::path::PathBuf {
    dir.join(format!("ledger-{node}"))
}

fn spawn_node(config: NodeConfig) -> JoinHandle<()> {
    tokio::spawn(async move { run_node(config).await.unwrap() })
}

async fn kill(node: JoinHandle<()>, port: u16) {
    node.abort();
    assert!(node.await.unwrap_err().is_cancelled());
    let deadline = Instant::now() + Duration::from_secs(5);
    while UdpSocket::bind(("0.0.0.0", port)).is_err() {
        assert!(Instant::now() < deadline, "the port outlived the node");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// the committed lane file really holds the tx; returns the lane's proposer
fn assert_in_ledger(ledger: &Path, view: &TxView, tx: &Tx) -> Option<u64> {
    let reader = LedgerReader::new(ledger);
    let (slot, lane) = (view.slot.unwrap(), view.lane.unwrap());
    let payload = reader
        .read_lane(slot, lane)
        .unwrap()
        .expect("a positive lane");
    let mut stamped = tx.clone(); // demo(tx-timeline)
    stamped.received_at_ns = view.received_at_ns; // demo(tx-timeline)
    assert!(view.received_at_ns > 0); // demo(tx-timeline)
    assert!(decode_batch(&payload).unwrap().contains(&stamped)); // demo(tx-timeline)
    reader.read_meta(slot).unwrap().lanes[lane as usize].proposer
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn txs_commit_through_a_real_node_and_survive_its_restart() {
    let dir = tempfile::tempdir().unwrap();
    let genesis = Timestamp::from_millis(unix_millis() + 2_000);
    let port = free_udp_port();
    let config = || NodeConfig::single_node(port, genesis, ledger_dir(dir.path(), 0));
    let node = spawn_node(config());
    let (rpc, _) = spawn_rpc(monad_mcp_rpc::RpcConfig {
        ledger_dir: ledger_dir(dir.path(), 0),
        ..rpc_config(&config(), dir.path(), 1_000, 30)
    });
    let ledger = ledger_dir(dir.path(), 0);

    let first = tx(1);
    let (code, reply) = post(&rpc, &body(&first)).await;
    assert_eq!(code, 200, "{reply}");
    assert_eq!(reply["status"], "sent");
    assert_eq!(reply["leader"], 0);
    let view = wait_state(&rpc, &hash_hex(&first), "committed", COMMIT_WITHIN).await;
    assert_eq!(assert_in_ledger(&ledger, &view, &first), Some(0));

    // a dead node swallows the datagram; only a resend can land it
    kill(node, port).await;
    let second = tx(2);
    let (code, reply) = post(&rpc, &body(&second)).await;
    assert_eq!(code, 200, "{reply}");
    assert_eq!(reply["status"], "sent");
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let node = spawn_node(config());
    let view = wait_state(&rpc, &hash_hex(&second), "committed", COMMIT_WITHIN).await;
    assert!(view.attempts >= 2, "committed without a resend: {view:?}");
    assert!(view.history.iter().all(|attempt| attempt.status == "sent"));
    assert_in_ledger(&ledger, &view, &second);
    assert!(view.slot > get(&rpc, &hash_hex(&first)).await.unwrap().slot);
    node.abort();
}

// four validators; the rpc beside node 0 sends each tx to the proposer with
// the most tenure left, and the tx lands in that proposer's lane everywhere
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_tx_lands_in_the_lane_of_the_leader_the_rpc_chose() {
    const NODES: u64 = 4;
    let dir = tempfile::tempdir().unwrap();
    let genesis = Timestamp::from_millis(unix_millis() + 2_000);
    let addresses: Vec<SocketAddr> = (0..NODES)
        .map(|_| SocketAddr::from(([127, 0, 0, 1], free_udp_port())))
        .collect();
    let config = |node: u64| with_ledger(node_config(node, &addresses, genesis), dir.path(), node);
    let nodes: Vec<_> = (0..NODES).map(|node| spawn_node(config(node))).collect();
    let (rpc, _) = spawn_rpc(monad_mcp_rpc::RpcConfig {
        ledger_dir: ledger_dir(dir.path(), 0),
        ..rpc_config(&config(0), dir.path(), 2_000, 10)
    });
    // past the genesis ramp-up, so every lane has had a proposer
    tokio::time::sleep(Duration::from_millis(2_000 + 4_000)).await;

    let mut sent = Vec::new();
    for nonce in 0..16 {
        let tx = tx(nonce);
        let (code, reply) = post(&rpc, &body(&tx)).await;
        assert_eq!(code, 200, "{reply}");
        let leader = reply["leader"].as_u64().unwrap();
        let lane = reply["target_lane"].as_u64().unwrap();
        sent.push((tx, leader, lane));
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let leaders: BTreeSet<u64> = sent.iter().map(|(_, leader, _)| *leader).collect();
    assert!(leaders.len() >= 2, "one leader over 40 slots: {leaders:?}");
    for (tx, leader, lane) in &sent {
        let view = wait_state(&rpc, &hash_hex(tx), "committed", COMMIT_WITHIN).await;
        if view.attempts > 1 {
            continue;
        }
        assert_eq!(view.lane, Some(*lane as u32), "tx {}: {view:?}", tx.nonce);
        for node in 0..NODES {
            let ledger = ledger_dir(dir.path(), node);
            let proposer = eventually(COMMIT_WITHIN, "the block on every node", || async {
                LedgerReader::new(&ledger)
                    .read_meta(view.slot.unwrap())
                    .ok()
                    .map(|_| assert_in_ledger(&ledger, &view, tx))
            })
            .await;
            assert_eq!(proposer, Some(*leader), "tx {} on node {node}", tx.nonce);
        }
    }
    for node in nodes {
        node.abort();
    }
}
