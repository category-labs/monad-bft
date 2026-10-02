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

//! L4a: the rpc against fake udp validators and a ledger dir the test writes.
//! A fake never answers; "dropping" a frame is not writing its block.

mod common;

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use common::*;
use monad_mcp_node::chorus::types::{NodeId, Slot, Timestamp};
use monad_mcp_rpc::{RpcConfig, api::TxView, schedule::Planner};
use serde_json::Value;

const RESEND_MS: u64 = 300;
const RESEND: Duration = Duration::from_millis(RESEND_MS);
const SWARM: usize = 5;

// a gap is one resend interval, late by at most a poll tick plus scheduling
fn assert_resend_gaps(arrivals: &[(u64, Arrival)]) {
    for pair in arrivals.windows(2) {
        let gap = pair[1].1.at - pair[0].1.at;
        assert!(gap >= RESEND, "resent early: {gap:?}");
        assert!(gap < RESEND * 2, "resent late: {gap:?}");
    }
}

// the slot a send at `now` targets, by the formula alone: the first deadline
// at or after now + lead, with the demo lead of 200 + 50 + 100 ms; in ns, as
// whole ms would put a send just past a boundary one slot early
fn formula_target(swarm: &FakeSwarm, now: Timestamp) -> u64 {
    let (lead, slot) = (350 * 1_000_000, 100 * 1_000_000);
    let since = (now.as_nanos() + lead).saturating_sub(swarm.genesis.as_nanos());
    since.div_ceil(slot) as u64
}

// what the rpc reported for a send agrees with the rule for its target slot
fn assert_routed(swarm: &FakeSwarm, target: u64, leader: u64, lane: u64) {
    let schedule = schedule_of(&swarm.config());
    let expected = most_tenured(schedule.as_ref(), Slot(target), horizon_of(&schedule));
    assert_eq!(
        Some((lane as usize, NodeId::dummy(leader))),
        expected,
        "slot {target}"
    );
}

fn reported(reply: &Value) -> (u64, u64, u64) {
    let field = |name: &str| {
        reply[name]
            .as_u64()
            .unwrap_or_else(|| panic!("no {name}: {reply}"))
    };
    (field("target_slot"), field("leader"), field("target_lane"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_send_reaches_exactly_the_most_tenured_leader_under_the_colocated_id() {
    let swarm = FakeSwarm::spawn(SWARM).await;
    let dir = tempfile::tempdir().unwrap();
    let (rpc, _) = spawn_rpc(rpc_config(&swarm.config(), dir.path(), 5_000, 3));

    let txs: Vec<_> = (0..24).map(tx).collect();
    let mut leaders = BTreeSet::new();
    for tx in &txs {
        let before = unix_now();
        let (code, reply) = post(&rpc, &body(tx)).await;
        let after = unix_now();
        assert_eq!(code, 200, "{reply}");
        assert_eq!(reply["status"], "sent", "{reply}");
        assert_eq!(reply["state"], "pending");
        assert_eq!(reply["tx_hash"], hash_hex(tx));
        let (target, leader, lane) = reported(&reply);
        assert!(
            (formula_target(&swarm, before)..=formula_target(&swarm, after)).contains(&target),
            "target {target} outside the clock's window: {reply}"
        );
        assert_routed(&swarm, target, leader, lane);
        leaders.insert(leader);

        let arrivals = eventually(Duration::from_secs(2), "the frame", || async {
            let arrivals = swarm.arrivals_of(tx);
            (!arrivals.is_empty()).then_some(arrivals)
        })
        .await;
        let [(node, arrival)] = &arrivals[..] else {
            panic!("tx {} reached {} nodes", tx.nonce, arrivals.len());
        };
        assert_eq!(
            *node, leader,
            "sent to a node other than the reported leader"
        );
        assert_eq!(
            arrival.from,
            NodeId::dummy(0),
            "not the colocated node's id"
        );
        // a slot and a half apart: the targets walk through the rotation
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(swarm.frames(), txs.len(), "one frame per send");
    assert_eq!(swarm.junk(), 0);
    assert!(leaders.len() >= 2, "every send went to {leaders:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unseen_tx_is_resent_each_time_to_the_leader_of_that_moment() {
    let swarm = FakeSwarm::spawn(SWARM).await;
    let dir = tempfile::tempdir().unwrap();
    let config = rpc_config(&swarm.config(), dir.path(), RESEND_MS, 5);
    let ledger = config.ledger_dir.clone();
    let (rpc, _) = spawn_rpc(config);

    let mut tx = tx(1);
    tx.sent_at_ns = 5; // demo(tx-timeline)
    let hash = hash_hex(&tx);
    let (code, first) = post(&rpc, &body(&tx)).await;
    assert_eq!(code, 200, "{first}");
    assert_eq!(first["known"], false);
    assert_eq!(first["attempts"], 1);

    // no block is written for the first two sends
    let arrivals = eventually(Duration::from_secs(5), "two resends", || async {
        let arrivals = swarm.arrivals_of(&tx);
        (arrivals.len() >= 3).then_some(arrivals)
    })
    .await;
    let view = eventually(Duration::from_secs(1), "the third attempt", || async {
        get_json(&rpc, &hash)
            .await
            .filter(|view| view["attempts"] == 3)
    })
    .await;
    assert_eq!(view["state"], "pending");
    assert_resend_gaps(&arrivals[..3]);
    // demo(tx-timeline): the rpc stamps the tx once and every resend carries that stamp
    let rpc_received = arrivals[0].1.tx.rpc_received_at_ns; // demo(tx-timeline)
    assert!(rpc_received > 0); // demo(tx-timeline)
    assert!(arrivals.iter().all(|(_, a)| {
        (a.tx.sent_at_ns, a.tx.rpc_received_at_ns) == (tx.sent_at_ns, rpc_received)
    })); // demo(tx-timeline)
    assert_eq!(view["rpc_received_at_ns"], rpc_received); // demo(tx-timeline)
    assert_eq!(view["sent_at_ns"], tx.sent_at_ns); // demo(tx-timeline)
    assert_eq!(view["mempool_admitted_at_ns"], 0); // demo(tx-timeline)
    let history = view["history"].as_array().unwrap();
    let mut targets = Vec::new();
    for (attempt, (node, arrival)) in history.iter().zip(&arrivals) {
        assert_eq!(attempt["status"], "sent", "{attempt}");
        let (target, leader, lane) = reported(attempt);
        assert_eq!(*node, leader, "resent to a stale leader");
        assert_eq!(arrival.from, NodeId::dummy(0));
        assert_routed(&swarm, target, leader, lane);
        targets.push(target);
    }
    // each resend re-targets from its own time, three slots on
    assert!(
        targets.windows(2).all(|pair| pair[1] >= pair[0] + 2),
        "{targets:?}"
    );
    assert_eq!(view["target_slot"], history.last().unwrap()["target_slot"]);
    assert_eq!(view["target_lane"], history.last().unwrap()["target_lane"]);

    write_block(&ledger, 42, &[&[], std::slice::from_ref(&tx)]);
    let committed = wait_state(&rpc, &hash, "committed", Duration::from_secs(2)).await;
    assert_eq!((committed.slot, committed.lane), (Some(42), Some(1)));
    assert_eq!(committed.path.as_deref(), Some("fast"));
    assert_eq!(committed.finalized_at_ms, Some(2));
    assert_eq!(committed.error, None);

    // committed means no more resends
    let sent = swarm.arrivals_of(&tx).len();
    tokio::time::sleep(RESEND * 2).await;
    assert_eq!(swarm.arrivals_of(&tx).len(), sent);
}

// a tx resent over more than a rotation follows the handover to a new leader
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resends_across_a_rotation_change_leaders() {
    let swarm = FakeSwarm::spawn(SWARM).await;
    let dir = tempfile::tempdir().unwrap();
    // 12 resends, 200 ms apart: 24 slots, three rotations of 8
    let (rpc, _) = spawn_rpc(rpc_config(&swarm.config(), dir.path(), 200, 12));
    let tx = tx(2);
    post(&rpc, &body(&tx)).await;
    let view = wait_state(&rpc, &hash_hex(&tx), "failed", Duration::from_secs(10)).await;
    assert_eq!(view.attempts, 12);
    let arrivals = swarm.arrivals_of(&tx);
    assert_eq!(arrivals.len(), 12);
    let leaders: BTreeSet<u64> = arrivals.iter().map(|(node, _)| *node).collect();
    assert!(
        leaders.len() >= 2,
        "one leader over three rotations: {leaders:?}"
    );
    for (attempt, (node, _)) in view.history.iter().zip(&arrivals) {
        assert_eq!(attempt.leader, Some(*node));
    }
}

// by latency the nearest proposer would keep the tx; each resend leaves the last leader
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_latency_an_unseen_tx_is_resent_to_another_leader() {
    let swarm = FakeSwarm::spawn(SWARM).await;
    let dir = tempfile::tempdir().unwrap();
    let node = swarm.config();
    let latency = (0..SWARM as u64)
        .map(|id| (NodeId::dummy(id), Duration::from_millis(10 * id)))
        .collect();
    let planner = Planner::new(&node, None).unwrap().with_latency(latency, 2);
    let (rpc, _) = spawn_rpc(RpcConfig {
        planner: Arc::new(planner),
        ..rpc_config(&node, dir.path(), RESEND_MS, 4)
    });
    let tx = tx(3);
    post(&rpc, &body(&tx)).await;
    let view = wait_state(&rpc, &hash_hex(&tx), "failed", Duration::from_secs(5)).await;
    let arrivals = swarm.arrivals_of(&tx);
    assert_eq!(arrivals.len(), 4);
    for pair in arrivals.windows(2) {
        assert_ne!(pair[0].0, pair[1].0, "resent to the last leader");
    }
    for (attempt, (node, _)) in view.history.iter().zip(&arrivals) {
        assert_eq!(attempt.leader, Some(*node));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tx_fails_after_max_attempts_and_a_late_commit_still_wins() {
    let swarm = FakeSwarm::spawn(SWARM).await;
    let dir = tempfile::tempdir().unwrap();
    let config = rpc_config(&swarm.config(), dir.path(), RESEND_MS, 3);
    let ledger = config.ledger_dir.clone();
    let (rpc, state) = spawn_rpc(config);

    let tx = tx(3);
    let hash = hash_hex(&tx);
    assert_eq!(post(&rpc, &body(&tx)).await.0, 200);
    let failed = wait_state(&rpc, &hash, "failed", RESEND * 5).await;
    assert_eq!(failed.attempts, 3);
    assert_eq!(
        failed.error.as_deref(),
        Some(
            "not in the ledger after 3 attempts (are its leaders on proposal.source = \"mempool\"?)"
        )
    );
    assert_eq!(swarm.arrivals_of(&tx).len(), 3);
    assert_resend_gaps(&swarm.arrivals_of(&tx));
    assert_eq!(state.counts(), (0, 1));

    tokio::time::sleep(RESEND * 2).await;
    assert_eq!(swarm.arrivals_of(&tx).len(), 3, "a failed tx is not resent");

    // posting it again starts it over
    let (code, retry) = post(&rpc, &body(&tx)).await;
    assert_eq!(code, 200, "{retry}");
    assert_eq!(
        (retry["known"].clone(), retry["attempts"].clone()),
        (false.into(), 1.into())
    );
    assert_eq!(retry["state"], "pending");
    assert_eq!(swarm.arrivals_of(&tx).len(), 4);

    write_block(&ledger, 7, &[std::slice::from_ref(&tx)]);
    let committed = wait_state(&rpc, &hash, "committed", Duration::from_secs(2)).await;
    assert_eq!(committed.slot, Some(7));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_known_tx_is_not_sent_again_on_a_second_post() {
    let swarm = FakeSwarm::spawn(SWARM).await;
    let dir = tempfile::tempdir().unwrap();
    let (rpc, _) = spawn_rpc(rpc_config(&swarm.config(), dir.path(), 5_000, 3));
    let tx = tx(4);
    let (code, first) = post(&rpc, &body(&tx)).await;
    assert_eq!(code, 200, "{first}");
    let (code, again) = post(&rpc, &body(&tx)).await;
    assert_eq!(code, 200);
    assert_eq!(again["known"], true);
    assert_eq!(again["attempts"], 1);
    assert_eq!(again["leader"], first["leader"]);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(swarm.arrivals_of(&tx).len(), 1);
}

// a colocated lone validator is always the leader: the rpc sends to itself
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lone_validator_gets_every_tx() {
    let swarm = FakeSwarm::spawn(1).await;
    let dir = tempfile::tempdir().unwrap();
    let (rpc, _) = spawn_rpc(rpc_config(&swarm.config(), dir.path(), 5_000, 3));
    for nonce in 10..20 {
        let (code, reply) = post(&rpc, &body(&tx(nonce))).await;
        assert_eq!(code, 200, "{reply}");
        let (target, leader, lane) = reported(&reply);
        assert_eq!((leader, lane), (0, 0), "{reply}");
        assert_routed(&swarm, target, leader, lane);
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    eventually(Duration::from_secs(2), "ten frames", || async {
        (swarm.nodes[0].frames() == 10).then_some(())
    })
    .await;
    assert!(
        swarm.nodes[0]
            .arrivals()
            .iter()
            .all(|arrival| arrival.from == NodeId::dummy(0))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_txs_in_one_block_commit_together_and_strangers_are_ignored() {
    let swarm = FakeSwarm::spawn(SWARM).await;
    let dir = tempfile::tempdir().unwrap();
    let config = rpc_config(&swarm.config(), dir.path(), 5_000, 3);
    let ledger = config.ledger_dir.clone();
    let (rpc, state) = spawn_rpc(config);

    let txs: Vec<_> = (10..30).map(tx).collect();
    for tx in &txs {
        assert_eq!(post(&rpc, &body(tx)).await.0, 200);
    }
    eventually(Duration::from_secs(2), "20 frames", || async {
        (swarm.frames() == 20).then_some(())
    })
    .await;
    let stranger = tx(99);
    write_block(
        &ledger,
        5,
        &[&txs[..10], std::slice::from_ref(&stranger), &txs[10..]],
    );
    for (i, tx) in txs.iter().enumerate() {
        let view: TxView =
            wait_state(&rpc, &hash_hex(tx), "committed", Duration::from_secs(2)).await;
        assert_eq!(view.lane, Some(if i < 10 { 0 } else { 2 }));
    }
    assert!(get(&rpc, &hash_hex(&stranger)).await.is_none());
    assert_eq!(state.counts(), (0, 20));
    assert_eq!(state.ledger_status().highest_slot, Some(5));
    assert_eq!(state.ledger_status().commits_seen, 20);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocks_present_at_startup_are_not_rescanned() {
    let swarm = FakeSwarm::spawn(SWARM).await;
    let dir = tempfile::tempdir().unwrap();
    let config = rpc_config(&swarm.config(), dir.path(), 5_000, 3);
    let ledger = config.ledger_dir.clone();
    let tx = tx(5);
    write_block(&ledger, 1, &[std::slice::from_ref(&tx)]);
    let (rpc, state) = spawn_rpc(config);
    post(&rpc, &body(&tx)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(get(&rpc, &hash_hex(&tx)).await.unwrap().state, "pending");
    assert_eq!(state.ledger_status().blocks_seen, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broken_ledger_turns_health_off_until_it_reads_again() {
    let swarm = FakeSwarm::spawn(SWARM).await;
    let dir = tempfile::tempdir().unwrap();
    let config = rpc_config(&swarm.config(), dir.path(), RESEND_MS, 3);
    let ledger = config.ledger_dir.clone();
    let (rpc, state) = spawn_rpc(config);
    let health = || async {
        reqwest::get(format!("{rpc}/health"))
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap()
    };
    let up = health().await;
    assert_eq!(up["ok"], true, "{up}");
    assert_eq!(up["node"]["id"], 0, "{up}");

    // a file where the blocks dir belongs fails every poll
    std::fs::create_dir_all(&ledger).unwrap();
    std::fs::write(ledger.join("blocks"), b"not a dir").unwrap();
    let broken = eventually(Duration::from_secs(2), "a ledger error", || async {
        Some(health().await).filter(|health| !health["ledger"]["error"].is_null())
    })
    .await;
    assert_eq!(broken["ok"], false, "{broken}");

    std::fs::remove_file(ledger.join("blocks")).unwrap();
    let tx = tx(6);
    post(&rpc, &body(&tx)).await;
    write_block(&ledger, 3, &[std::slice::from_ref(&tx)]);
    wait_state(&rpc, &hash_hex(&tx), "committed", Duration::from_secs(2)).await;
    let healed = health().await;
    assert_eq!(healed["ok"], true, "{healed}");
    assert_eq!(healed["node"]["last_status"], "sent", "{healed}");
    assert!(state.maintenance_age() < state.maintenance_deadline());
}
