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

//! The node with a temp config: `Packet::Tx` frames sent to its udp port
//! under a validator's id show up in its ledger; any other sender is dropped.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::{SocketAddr, UdpSocket},
    process::{Child, Command},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use monad_mcp_chorus::{
    ledger::{LedgerReader, MAX_TX_PAYLOAD, Tx, decode_batch, lane_json_file_name},
    spec::vote::KeyPair as _,
};
use monad_mcp_node::{
    chorus::types::{KeyPair, NodeId, Timestamp},
    config::{NodeConfig, ValidatorConfig},
    da::ProposalKeyPair,
    network::{Packet, encode_frame},
    run_node,
};
use tempfile::TempDir;

const MAX_TXS: usize = 8;
const GENESIS_DELAY_MS: u64 = 2_000;
const LEDGER_TIMEOUT: Duration = Duration::from_secs(20);

struct NodeProcess {
    child: Child,
    dir: TempDir,
    port: u16,
}

impl NodeProcess {
    fn spawn(source: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let port = free_udp_port();
        let config = config_text(port, source, &dir.path().join("ledger"));
        let config_path = dir.path().join("node.toml");
        fs::write(&config_path, config).unwrap();
        let log = fs::File::create(dir.path().join("node.log")).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_monad-mcp-node"))
            .arg(&config_path)
            .env("RUST_LOG", "info")
            .stderr(log.try_clone().unwrap())
            .stdout(log)
            .spawn()
            .unwrap();
        Self { child, dir, port }
    }

    fn ledger(&self) -> LedgerReader {
        LedgerReader::new(self.dir.path().join("ledger"))
    }

    // udp gives no answer, so readiness is the node's own log line
    fn wait_bound(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.log().contains("udp bound") {
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!("node exited with {status}:\n{}", self.log());
            }
            assert!(
                Instant::now() < deadline,
                "udp never bound:\n{}",
                self.log()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn address(&self) -> SocketAddr {
        ([127, 0, 0, 1], self.port).into()
    }

    fn log(&self) -> String {
        fs::read_to_string(self.dir.path().join("node.log")).unwrap_or_default()
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

fn config_text(port: u16, source: &str, ledger: &std::path::Path) -> String {
    let genesis = unix_millis() + GENESIS_DELAY_MS;
    format!(
        r#"
node_id = 0
proposal_key_pair = 0
cadence_key_pair = 0
genesis_deadline = {genesis}

[[validators]]
node_id = 0
stake = 1
chorus_pubkey = 0
address = "127.0.0.1:{port}"

[network]
port = {port}

[cadence]
delta = 50
slot_interval = 100

[proposal]
source = "{source}"
propose_before_deadline = 200
max_payload_bytes = 16384

[mempool]
max_txs = {MAX_TXS}

[ledger]
dir = "{ledger}"
"#,
        ledger = ledger.display(),
    )
}

fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn tx(nonce: u64) -> Tx {
    Tx {
        sender: [0x5a; 20],
        nonce,
        payload: Bytes::from(format!("udp-ledger {nonce}")),
        sent_at_ns: 0,             // demo(tx-timeline)
        rpc_received_at_ns: 0,     // demo(tx-timeline)
        mempool_admitted_at_ns: 0, // demo(tx-timeline)
    }
}

// one datagram, as the rpc frames it: the sender id is a validator's
fn send(to: SocketAddr, sender: u64, tx: &Tx) {
    let frame = encode_frame(NodeId::dummy(sender), &Packet::Tx(tx.to_rlp()));
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.send_to(&frame, to).unwrap();
}

// nonce -> times it appears in the lane files on disk
fn ledger_txs(ledger: &LedgerReader) -> BTreeMap<u64, usize> {
    let mut nonces = BTreeMap::new();
    for slot in ledger.scan().unwrap() {
        let meta = ledger.read_meta(slot).unwrap();
        for lane in meta.lanes.iter().filter(|lane| lane.is_positive()) {
            let Some(payload) = ledger.read_lane_for(&meta, lane.index).unwrap() else {
                continue;
            };
            let json = ledger.block_dir(slot).join(lane_json_file_name(lane.index));
            assert!(json.exists(), "{} is missing", json.display());
            let Ok(txs) = decode_batch(&payload) else {
                continue;
            };
            for tx in txs {
                *nonces.entry(tx.nonce).or_default() += 1;
            }
        }
    }
    nonces
}

// the proposer of the lane holding `nonce`, from the node-written meta; its
// deadline is set, which the explorer's latency stats read
fn proposer_of(ledger: &LedgerReader, nonce: u64) -> Option<u64> {
    for slot in ledger.scan().unwrap() {
        let meta = ledger.read_meta(slot).unwrap();
        for lane in meta.lanes.iter().filter(|lane| lane.is_positive()) {
            let payload = ledger.read_lane_for(&meta, lane.index).unwrap().unwrap();
            if !decode_batch(&payload)
                .unwrap()
                .iter()
                .any(|tx| tx.nonce == nonce)
            {
                continue;
            }
            let deadline = meta.deadline_ns.expect("a deadline");
            assert!(deadline <= meta.finalized_at_ns, "{meta:?}");
            return lane.proposer;
        }
    }
    None
}

async fn wait_for(ledger: &LedgerReader, expected: &BTreeSet<u64>, log: impl Fn() -> String) {
    let deadline = Instant::now() + LEDGER_TIMEOUT;
    loop {
        let found: BTreeSet<u64> = ledger_txs(ledger).into_keys().collect();
        if expected.is_subset(&found) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "missing {:?} after {LEDGER_TIMEOUT:?}:\n{}",
            expected.difference(&found).collect::<Vec<_>>(),
            log()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tx_frames_reach_the_ledger_and_a_flood_is_capped_by_the_mempool() {
    let mut node = NodeProcess::spawn("mempool");
    node.wait_bound();
    let to = node.address();

    // before genesis nothing drains, so the mempool bound is exact
    let flood: BTreeSet<u64> = (0..MAX_TXS as u64 * 4).collect();
    for nonce in &flood {
        send(to, 0, &tx(*nonce));
    }
    let oversized = Tx {
        payload: Bytes::from(vec![1; MAX_TX_PAYLOAD + 1]),
        ..tx(999)
    };
    send(to, 0, &oversized);
    // a sender id outside the validator set, from any address, is dropped
    send(to, 7, &tx(777));

    let ledger = node.ledger();
    let deadline = Instant::now() + LEDGER_TIMEOUT;
    let landed = loop {
        let landed: BTreeSet<u64> = ledger_txs(&ledger)
            .into_keys()
            .filter(|nonce| flood.contains(nonce))
            .collect();
        if landed.len() >= MAX_TXS {
            break landed;
        }
        assert!(
            Instant::now() < deadline,
            "flood not committed:\n{}",
            node.log()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(
        landed.len(),
        MAX_TXS,
        "the mempool took more than its bound"
    );

    // once the flood drained, a late tx lands in the node's own lane
    let late = 10_000;
    send(to, 0, &tx(late));
    wait_for(&ledger, &BTreeSet::from([late]), || node.log()).await;
    assert_eq!(proposer_of(&ledger, late), Some(0));
    // a resend of a committed tx is not committed again
    send(to, 0, &tx(late));
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let counts = ledger_txs(&ledger);
    assert_eq!(counts.get(&late), Some(&1), "{counts:?}");
    assert!(counts.values().all(|times| *times == 1), "{counts:?}");
    assert!(!counts.contains_key(&999), "an oversized tx was committed");
    assert!(
        !counts.contains_key(&777),
        "a stranger's frame was committed"
    );
    // the chain keeps finalizing: no gaps in the stored slots
    let slots = ledger.scan().unwrap();
    assert!(
        slots.windows(2).all(|pair| pair[1] == pair[0] + 1),
        "{slots:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_random_source_node_drops_tx_frames() {
    let mut node = NodeProcess::spawn("random");
    node.wait_bound();
    send(node.address(), 0, &tx(1));

    // random payloads are stored, flagged as undecodable
    let ledger = node.ledger();
    let deadline = Instant::now() + LEDGER_TIMEOUT;
    loop {
        let flagged = ledger.scan().unwrap().into_iter().any(|slot| {
            let meta = ledger.read_meta(slot).unwrap();
            meta.lanes.iter().any(|lane| lane.decode_error)
        });
        if flagged {
            break;
        }
        assert!(Instant::now() < deadline, "no random lane:\n{}", node.log());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(node.child.try_wait().unwrap().is_none(), "{}", node.log());
    assert!(ledger_txs(&ledger).is_empty());
}

// the rpc now builds the schedule itself; the node takes only a config path
#[test]
fn the_schedule_flag_is_gone() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("node.toml");
    fs::write(&config, config_text(free_udp_port(), "mempool", dir.path())).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_monad-mcp-node"))
        .arg("--schedule")
        .arg(&config)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty(), "{output:?}");
}

// how the rpc's tests embed a node: run_node on the caller's runtime,
// stopped by dropping its task
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_node_serves_in_process_and_frees_its_port_on_abort() {
    let dir = tempfile::tempdir().unwrap();
    let genesis = Timestamp::from_millis(unix_millis() + GENESIS_DELAY_MS);
    let port = free_udp_port();
    let config = NodeConfig::single_node(port, genesis, dir.path().join("ledger-0"));
    let node = tokio::spawn(run_node(config));
    let to = SocketAddr::from(([127, 0, 0, 1], port));

    // resent until it lands, as the rpc does: the first frames may beat the bind
    let ledger = LedgerReader::new(dir.path().join("ledger-0"));
    let deadline = Instant::now() + LEDGER_TIMEOUT;
    while !ledger_txs(&ledger).contains_key(&7) {
        assert!(!node.is_finished(), "node stopped");
        assert!(Instant::now() < deadline, "tx 7 never landed");
        send(to, 0, &tx(7));
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(proposer_of(&ledger, 7), Some(0));

    node.abort();
    assert!(node.await.unwrap_err().is_cancelled());
    let deadline = Instant::now() + Duration::from_secs(5);
    while UdpSocket::bind(("0.0.0.0", port)).is_err() {
        assert!(Instant::now() < deadline, "the port outlived the node");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// four validators in one process: a frame sent straight to node 2 is proposed
// in node 2's lane and stored by every ledger; a stranger's frame nowhere
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tx_sent_to_a_peer_lands_in_its_lane_on_every_ledger() {
    const NODES: u64 = 4;
    let dir = tempfile::tempdir().unwrap();
    let genesis = Timestamp::from_millis(unix_millis() + GENESIS_DELAY_MS);
    let ports: Vec<u16> = (0..NODES).map(|_| free_udp_port()).collect();
    let validators = |node: u64| -> NodeConfig {
        let mut config = NodeConfig::single_node(
            ports[node as usize],
            genesis,
            dir.path().join(format!("ledger-{node}")),
        );
        config.validators = (0..NODES)
            .map(|id| ValidatorConfig {
                node_id: NodeId::dummy(id),
                stake: 1,
                chorus_pubkey: KeyPair::dummy(id).pubkey(),
                address: ([127, 0, 0, 1], ports[id as usize]).into(),
            })
            .collect();
        config.node_id = NodeId::dummy(node);
        config.proposal_key_pair = ProposalKeyPair::dummy(NodeId::dummy(node));
        config.cadence_key_pair = KeyPair::dummy(node);
        config
    };
    let nodes: Vec<_> = (0..NODES)
        .map(|node| tokio::spawn(run_node(validators(node))))
        .collect();
    tokio::time::sleep(Duration::from_millis(GENESIS_DELAY_MS)).await;

    let to = |node: usize| SocketAddr::from(([127, 0, 0, 1], ports[node]));
    // sender id 0 addressing node 2, as an rpc colocated with node 0 does
    let (ours, stranger) = (tx(1), tx(2));
    send(to(1), 9, &stranger);
    send(to(2), 0, &ours);

    let ledgers: Vec<_> = (0..NODES)
        .map(|node| LedgerReader::new(dir.path().join(format!("ledger-{node}"))))
        .collect();
    for ledger in &ledgers {
        wait_for(ledger, &BTreeSet::from([1]), String::new).await;
        assert_eq!(proposer_of(ledger, 1), Some(2));
    }
    for ledger in &ledgers {
        let counts = ledger_txs(ledger);
        assert_eq!(counts.get(&1), Some(&1), "{counts:?}");
        assert!(!counts.contains_key(&2), "a stranger's frame was committed");
    }
    for node in nodes {
        node.abort();
    }
}
