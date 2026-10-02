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

#![allow(dead_code)]

use std::{
    future::Future,
    net::SocketAddr,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use monad_mcp_chorus::{
    ledger::{CommittedLane, FinalizationPath, LedgerWriter, NewBlock, NewLane, Tx, encode_batch},
    spec::vote::KeyPair as _,
};
use monad_mcp_node::{
    NodeProposerSchedule,
    chorus::types::{KeyPair, NodeId, ProposerSchedule, Slot, Timestamp},
    config::{NodeConfig, ValidatorConfig},
    network::{Packet, decode_frame},
};
use monad_mcp_rpc::{RpcConfig, RpcState, api::TxView, start};
use serde_json::Value;
use tokio::net::UdpSocket;

// an rpc on its own actix system thread, left running until the test process ends
pub fn spawn_rpc(config: RpcConfig) -> (String, Arc<RpcState>) {
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        actix_web::rt::System::new().block_on(async move {
            let rpc = start(config).unwrap();
            sender.send((rpc.addrs[0], rpc.state.clone())).unwrap();
            rpc.server.await.unwrap();
        })
    });
    let (addr, state): (SocketAddr, _) = receiver.recv().unwrap();
    (format!("http://{addr}"), state)
}

pub fn rpc_config(node: &NodeConfig, dir: &Path, resend_ms: u64, max_attempts: u32) -> RpcConfig {
    RpcConfig {
        http_addr: "127.0.0.1:0".into(),
        resend_after: Duration::from_millis(resend_ms),
        max_attempts,
        poll_interval: Duration::from_millis(50),
        ..RpcConfig::colocated(node, dir.join("ledger")).unwrap()
    }
}

pub fn unix_now() -> Timestamp {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    Timestamp::from_nanos(since.as_nanos())
}

pub fn unix_millis() -> u64 {
    (unix_now().as_nanos() / 1_000_000) as u64
}

// validator `self_id` of a set at `addresses`, with the 100 ms demo parameters
pub fn node_config(self_id: u64, addresses: &[SocketAddr], genesis: Timestamp) -> NodeConfig {
    let mut config =
        NodeConfig::single_node(addresses[self_id as usize].port(), genesis, "unused-ledger");
    config.validators = addresses
        .iter()
        .enumerate()
        .map(|(id, address)| ValidatorConfig {
            node_id: NodeId::dummy(id as u64),
            stake: 1,
            chorus_pubkey: KeyPair::dummy(id as u64).pubkey(),
            address: *address,
        })
        .collect();
    config.node_id = NodeId::dummy(self_id);
    config.proposal_key_pair = monad_mcp_node::da::ProposalKeyPair::dummy(NodeId::dummy(self_id));
    config.cadence_key_pair = KeyPair::dummy(self_id);
    config
}

pub fn schedule_of(node: &NodeConfig) -> Arc<NodeProposerSchedule> {
    node.epoch_handle().unwrap().proposers
}

// K · (y + z), from the schedule's own parameters
pub fn horizon_of(schedule: &NodeProposerSchedule) -> u64 {
    let cfg = schedule.config();
    cfg.concurrent_proposers as u64 * cfg.slots_per_rotation()
}

// slots from `from` on, up to `horizon`, that `node` keeps holding `lane`
pub fn run_of(
    schedule: &(impl ProposerSchedule + ?Sized),
    from: Slot,
    lane: usize,
    node: NodeId,
    horizon: u64,
) -> u64 {
    (0..horizon)
        .take_while(|k| {
            schedule
                .proposers_at(Slot(from.0 + k))
                .is_ok_and(|set| set.proposer(lane) == Some(node))
        })
        .count() as u64
}

// the reference rule: the longest run from `slot`, ties to the lower lane
pub fn most_tenured(
    schedule: &(impl ProposerSchedule + ?Sized),
    slot: Slot,
    horizon: u64,
) -> Option<(usize, NodeId)> {
    let set = schedule.proposers_at(slot).ok()?;
    let mut best: Option<(u64, usize, NodeId)> = None;
    for (lane, proposer) in set.iter() {
        let Some(node) = proposer else {
            continue;
        };
        let run = run_of(schedule, slot, lane, node, horizon);
        if best.is_none_or(|(longest, _, _)| run > longest) {
            best = Some((run, lane, node));
        }
    }
    best.map(|(_, lane, node)| (lane, node))
}

pub fn tx(nonce: u64) -> Tx {
    Tx {
        sender: [0x77; 20],
        nonce,
        payload: Bytes::from(format!("rpc test {nonce}")),
        sent_at_ns: 0,             // demo(tx-timeline)
        rpc_received_at_ns: 0,     // demo(tx-timeline)
        mempool_admitted_at_ns: 0, // demo(tx-timeline)
    }
}

pub fn body(tx: &Tx) -> Value {
    serde_json::json!({
        "sender": format!("0x{}", hex::encode(tx.sender)),
        "nonce": tx.nonce,
        "payload_hex": format!("0x{}", hex::encode(&tx.payload)),
        "sent_at_ns": tx.sent_at_ns, // demo(tx-timeline)
    })
}

pub fn hash_hex(tx: &Tx) -> String {
    format!("0x{}", hex::encode(tx.hash()))
}

pub async fn post(base: &str, body: &Value) -> (u16, Value) {
    let response = reqwest::Client::new()
        .post(format!("{base}/tx"))
        .json(body)
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    (status, response.json().await.unwrap())
}

pub async fn get(base: &str, hash: &str) -> Option<TxView> {
    let response = reqwest::get(format!("{base}/tx/{hash}")).await.unwrap();
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return None;
    }
    assert!(response.status().is_success(), "{}", response.status());
    Some(response.json().await.unwrap())
}

// the rpc's view as sent, for fields a typed view may not carry
pub async fn get_json(base: &str, hash: &str) -> Option<Value> {
    let response = reqwest::get(format!("{base}/tx/{hash}")).await.unwrap();
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return None;
    }
    assert!(response.status().is_success(), "{}", response.status());
    Some(response.json().await.unwrap())
}

pub async fn eventually<T, F: Future<Output = Option<T>>>(
    within: Duration,
    what: &str,
    mut probe: impl FnMut() -> F,
) -> T {
    let deadline = Instant::now() + within;
    loop {
        if let Some(value) = probe().await {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub async fn wait_state(base: &str, hash: &str, state: &str, within: Duration) -> TxView {
    eventually(within, state, || async {
        get(base, hash).await.filter(|view| view.state == state)
    })
    .await
}

pub fn write_block(ledger: &Path, slot: u64, lanes: &[&[Tx]]) {
    let lanes = lanes
        .iter()
        .map(|txs| NewLane {
            proposer: Some(0),
            committed: Some(CommittedLane {
                root: [9; 20],
                payload: encode_batch(1, txs), // demo(tx-timeline)
            }),
        })
        .collect();
    LedgerWriter::open(ledger)
        .unwrap()
        .write(&NewBlock {
            slot,
            deadline_ns: Some(1_000_000),
            finalized_at_ns: 2_000_000,
            path: FinalizationPath::Fast,
            fast_block_at_ns: None,     // demo(tx-timeline)
            lane_decoded_at_ns: vec![], // demo(tx-timeline)
            lanes,
            proof: Bytes::from_static(b"proof"),
        })
        .unwrap();
}

#[derive(Clone, Debug)]
pub struct Arrival {
    pub at: Instant,
    // the frame's sender id
    pub from: NodeId,
    pub tx: Tx,
}

// a stand-in validator: a udp socket that decodes every frame it gets
pub struct FakeNode {
    pub addr: SocketAddr,
    arrivals: Arc<Mutex<Vec<Arrival>>>,
    // datagrams that were not a well-formed `Packet::Tx`
    junk: Arc<AtomicU32>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeNode {
    pub async fn spawn() -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let arrivals = Arc::new(Mutex::new(Vec::new()));
        let junk = Arc::new(AtomicU32::new(0));
        let task = tokio::spawn({
            let (arrivals, junk) = (arrivals.clone(), junk.clone());
            async move {
                let mut buffer = vec![0; 2048];
                loop {
                    let Ok((len, _)) = socket.recv_from(&mut buffer).await else {
                        continue;
                    };
                    let decoded = decode_frame(&buffer[..len]).and_then(|(from, packet)| {
                        let Packet::Tx(bytes) = packet else {
                            return None;
                        };
                        Some((from, Tx::decode_exact(&bytes).ok()?))
                    });
                    match decoded {
                        Some((from, tx)) => arrivals.lock().unwrap().push(Arrival {
                            at: Instant::now(),
                            from,
                            tx,
                        }),
                        None => {
                            junk.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }
        });
        Self {
            addr,
            arrivals,
            junk,
            task,
        }
    }

    pub fn arrivals(&self) -> Vec<Arrival> {
        self.arrivals.lock().unwrap().clone()
    }

    pub fn frames(&self) -> usize {
        self.arrivals.lock().unwrap().len()
    }

    pub fn junk(&self) -> u32 {
        self.junk.load(Ordering::SeqCst)
    }
}

impl Drop for FakeNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// fake validators 0..n; the rpc is colocated with validator 0
pub struct FakeSwarm {
    pub nodes: Vec<FakeNode>,
    pub genesis: Timestamp,
}

impl FakeSwarm {
    // genesis well in the past, so the targets are past the ramp-up
    pub async fn spawn(n: usize) -> Self {
        let mut nodes = Vec::new();
        for _ in 0..n {
            nodes.push(FakeNode::spawn().await);
        }
        let genesis = Timestamp::from_millis(unix_millis() - 60_000);
        Self { nodes, genesis }
    }

    pub fn addresses(&self) -> Vec<SocketAddr> {
        self.nodes.iter().map(|node| node.addr).collect()
    }

    pub fn config(&self) -> NodeConfig {
        node_config(0, &self.addresses(), self.genesis)
    }

    // every arrival with the index of the node it reached, oldest first
    pub fn arrivals(&self) -> Vec<(u64, Arrival)> {
        let mut all: Vec<(u64, Arrival)> = self
            .nodes
            .iter()
            .enumerate()
            .flat_map(|(i, node)| node.arrivals().into_iter().map(move |a| (i as u64, a)))
            .collect();
        all.sort_by_key(|(_, arrival)| arrival.at);
        all
    }

    pub fn arrivals_of(&self, tx: &Tx) -> Vec<(u64, Arrival)> {
        self.arrivals()
            .into_iter()
            .filter(|(_, arrival)| arrival.tx.hash() == tx.hash()) // demo(tx-timeline)
            .collect()
    }

    pub fn frames(&self) -> usize {
        self.nodes.iter().map(FakeNode::frames).sum()
    }

    pub fn junk(&self) -> u32 {
        self.nodes.iter().map(FakeNode::junk).sum()
    }
}
