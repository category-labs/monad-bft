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

//! A few seconds of a 4-node pq swarm in one process over loopback UDP:
//! `run_node` with the async node, recorder and ledger writer, checked
//! through the ledgers it leaves.

use std::{
    collections::BTreeMap,
    net::{SocketAddr, UdpSocket},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use monad_mcp_chorus::{
    ledger::{LanePid, LedgerReader},
    spec::vote::KeyPair as _,
};
use monad_mcp_node::config::ValidatorConfig;
use monad_pq_node::{
    chorus::types::{KeyPair, NodeId, Timestamp, TimestampDelta},
    config::PqNodeConfig,
    run_node,
};

const NODES: u64 = 4;
const GENESIS_DELAY: TimestampDelta = TimestampDelta::from_millis(1_000);
const RUN_FOR: Duration = Duration::from_secs(4);
// 100 ms slots for 3 s after genesis, with ample slack for a loaded host
const MIN_BLOCKS: usize = 10;

// ports the OS just handed out, so parallel runs do not collide
fn free_ports(count: u64) -> Vec<u16> {
    let sockets: Vec<UdpSocket> = (0..count)
        .map(|_| UdpSocket::bind("127.0.0.1:0").expect("an ephemeral port"))
        .collect();
    sockets
        .iter()
        .map(|socket| socket.local_addr().expect("bound").port())
        .collect()
}

fn config(i: u64, ports: &[u16], genesis_deadline: Timestamp, ledger: PathBuf) -> PqNodeConfig {
    let port = ports[i as usize];
    let mut config = PqNodeConfig::single_node(port, genesis_deadline, ledger);
    let base = &mut config.base;
    base.node_id = NodeId::dummy(i);
    base.proposal_key_pair = monad_mcp_node::da::ProposalKeyPair::dummy(NodeId::dummy(i));
    base.cadence_key_pair = KeyPair::dummy(i);
    base.validators = (0..NODES)
        .map(|v| ValidatorConfig {
            node_id: NodeId::dummy(v),
            stake: 1,
            chorus_pubkey: KeyPair::dummy(v).pubkey(),
            address: SocketAddr::from(([127, 0, 0, 1], ports[v as usize])),
        })
        .collect();
    config.proposal_key = i;
    config
}

fn genesis_deadline() -> Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past 1970");
    Timestamp::from_nanos(now.as_nanos())
        .checked_add_delta(GENESIS_DELAY)
        .expect("no overflow")
}

// slot -> each lane's committed pid, as stored
fn read_ledger(dir: &Path) -> BTreeMap<u64, Vec<Option<LanePid>>> {
    let reader = LedgerReader::new(dir);
    reader
        .scan()
        .expect("the ledger scans")
        .into_iter()
        .map(|slot| {
            let meta = reader.read_meta(slot).expect("a stored block reads back");
            meta.check().expect("a consistent block");
            (slot, meta.lanes.iter().map(|lane| lane.pid).collect())
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_loopback_swarm_finalizes_the_same_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let ports = free_ports(NODES);
    let genesis_deadline = genesis_deadline();
    let ledgers: Vec<PathBuf> = (0..NODES)
        .map(|i| dir.path().join(format!("ledger-{i}")))
        .collect();

    let runs: Vec<_> = (0..NODES)
        .map(|i| {
            let config = config(i, &ports, genesis_deadline, ledgers[i as usize].clone());
            tokio::spawn(run_node(config))
        })
        .collect();
    tokio::time::sleep(RUN_FOR).await;
    for run in &runs {
        assert!(!run.is_finished(), "a node stopped early");
        run.abort();
    }
    for run in runs {
        assert!(run.await.unwrap_err().is_cancelled());
    }
    // the ledger writers drain their queues on their own threads
    tokio::time::sleep(Duration::from_millis(300)).await;

    let stored: Vec<_> = ledgers.iter().map(|dir| read_ledger(dir)).collect();
    for (i, blocks) in stored.iter().enumerate() {
        assert!(
            blocks.len() >= MIN_BLOCKS,
            "node {i} stored {} blocks",
            blocks.len()
        );
    }
    let mut common = 0;
    for (slot, pids) in &stored[0] {
        let others: Vec<_> = stored[1..].iter().map(|blocks| blocks.get(slot)).collect();
        for other in others.iter().flatten() {
            assert_eq!(*other, pids, "nodes disagree on slot {slot}");
        }
        if others.iter().all(Option::is_some) {
            common += 1;
        }
    }
    assert!(
        common >= MIN_BLOCKS,
        "only {common} slots stored by every node"
    );
}
