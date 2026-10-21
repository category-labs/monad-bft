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

//! A lone pq validator (n = 1, K = 5) stepped in virtual time: txs arriving
//! as `Packet::Tx` reach the ledger through proposal, DA and Quantum
//! Chorus, with no tokio and no sockets.

use std::collections::BTreeMap;

use bytes::Bytes;
use monad_mcp_chorus::ledger::{FinalizationPath, LedgerReader, LedgerWriter, Tx, decode_batch};
use monad_pq_chorus::{PqPath, PqProof};
use monad_pq_node::{
    PqFinalizedSlot, PqNode, PqNodeOutput, PqNodeRuntime, PqRuntime,
    chorus::types::{NodeId, ProposerSchedule as _, Slot, Timestamp, TimestampDelta},
    component::Component as _,
    config::{LedgerProof, PqNodeConfig},
    ledger::pq_new_block,
    network::{Inbound, Outbound, Packet},
};

const GENESIS_MS: u64 = 1_000_000;

fn at(ms: u64) -> Timestamp {
    Timestamp::from_millis(ms)
}

fn tx(nonce: u64) -> Tx {
    Tx {
        sender: [0x42; 20],
        nonce,
        payload: Bytes::from(format!("in-process {nonce}")),
        sent_at_ns: 0,
        rpc_received_at_ns: 0,
        mempool_admitted_at_ns: 0,
    }
}

struct Harness<R = PqNodeRuntime> {
    node: PqNode<R>,
    now: Timestamp,
    finalized: Vec<PqFinalizedSlot>,
}

impl Harness {
    fn new(config: &PqNodeConfig) -> Self {
        Self {
            node: PqNode::new(config).unwrap(),
            now: at(GENESIS_MS - 1_000),
            finalized: Vec::new(),
        }
    }

    fn pooled(&self) -> usize {
        self.node.runtime().mempool().unwrap().lock().len()
    }
}

impl<R: PqRuntime> Harness<R> {
    // what the rpc sends: a tx frame under a validator's id
    fn deliver(&mut self, tx: &Tx) {
        let inbound = Inbound {
            from: NodeId::dummy(0),
            packet: Packet::Tx(tx.to_rlp()),
        };
        self.node.handle(self.now, inbound);
        self.drain();
    }

    // steps every timer up to `until`; a unicast to ourselves loops back
    fn run_until(&mut self, until: Timestamp) {
        while let Some(due) = self.node.next_due() {
            if due > until {
                break;
            }
            self.now = self.now.max(due);
            self.node.handle_due(self.now);
            self.drain();
        }
        self.now = self.now.max(until);
    }

    fn drain(&mut self) {
        let self_id = NodeId::dummy(0);
        while let Some(output) = self.node.poll() {
            match output {
                PqNodeOutput::Finalized(finalized) => self.finalized.push(finalized),
                PqNodeOutput::Send(Outbound::Unicast(to, packet)) if to == self_id => {
                    let inbound = Inbound {
                        from: self_id,
                        packet,
                    };
                    self.node.handle(self.now, inbound);
                }
                PqNodeOutput::Send(_) => {}
                PqNodeOutput::Evidence(slot, evidence) => {
                    panic!("a lone correct node filed evidence at {slot:?}: {evidence:?}")
                }
            }
        }
    }

    // nonce -> slots whose committed lanes carry it
    fn committed(&self) -> BTreeMap<u64, Vec<u64>> {
        let mut committed = BTreeMap::<u64, Vec<u64>>::new();
        for finalized in &self.finalized {
            for payload in finalized.proposals.clone().into_iter().flatten() {
                for tx in decode_batch(&payload).unwrap() {
                    committed
                        .entry(tx.nonce)
                        .or_default()
                        .push(finalized.slot.0);
                }
            }
        }
        committed
    }
}

fn config() -> PqNodeConfig {
    PqNodeConfig::single_node(0, at(GENESIS_MS), "unused-ledger")
}

// M4 (§10.1): with n = 1 and K = 5 most lanes are vacant, and every slot
// still finalizes on the fast path from the node's own chunks
#[test]
fn a_lone_node_finalizes_every_slot_on_the_fast_path() {
    let config = config();
    let mut harness = Harness::new(&config);
    harness.run_until(at(GENESIS_MS + 3_000));

    let slots: Vec<u64> = harness.finalized.iter().map(|f| f.slot.0).collect();
    assert!(slots.len() >= 28, "slots keep finalizing: {slots:?}");
    assert_eq!(slots, (0..slots.len() as u64).collect::<Vec<_>>());
    for finalized in &harness.finalized {
        assert_eq!(finalized.path, PqPath::Fast, "slot {:?}", finalized.slot);
        assert_eq!(finalized.metablock.size(), 5);
        // a lone node finalizes at its early vote, at about the seal
        let deadline = finalized.deadline.unwrap();
        assert!(
            finalized.at <= deadline,
            "slot {:?} after its deadline",
            finalized.slot
        );
    }
    assert!(
        harness
            .finalized
            .iter()
            .any(|f| f.pids().into_iter().flatten().count() >= 1),
        "some lane is occupied"
    );
}

#[test]
fn a_delivered_tx_is_finalized_and_written_to_the_ledger() {
    let config = config();
    let mut harness = Harness::new(&config);

    let admitted_at = harness.now.as_nanos() as u64;
    harness.deliver(&tx(1));
    assert_eq!(harness.pooled(), 1);
    // a resend from the rpc is a duplicate, not a second entry
    harness.deliver(&tx(1));
    assert_eq!(harness.pooled(), 1);

    harness.run_until(at(GENESIS_MS + 3_000));
    let committed = harness.committed();
    assert_eq!(committed.len(), 1);
    let [slot] = committed[&1][..] else {
        panic!("committed exactly once: {committed:?}");
    };

    let finalized = harness
        .finalized
        .iter()
        .find(|finalized| finalized.slot.0 == slot)
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let writer = LedgerWriter::open(dir.path()).unwrap();
    let proposers = harness
        .node
        .runtime()
        .epoch_handle()
        .proposers
        .proposers_at(finalized.slot)
        .unwrap();
    let block = pq_new_block(finalized, Some(&proposers), LedgerProof::PathVotes);
    let written = writer.write(&block).unwrap();

    let reader = LedgerReader::new(dir.path());
    assert_eq!(reader.scan().unwrap(), [slot]);
    let meta = reader.read_meta(slot).unwrap();
    assert_eq!(meta, written);
    meta.check().unwrap();
    assert_eq!(meta.path, FinalizationPath::Fast);
    assert_eq!(meta.tx_count(), 1);
    assert_eq!(meta.num_lanes as usize, config.base.proposal.num_proposals);
    assert_eq!(
        meta.deadline_ns,
        finalized.deadline.map(Timestamp::as_nanos)
    );

    let lane = meta.lanes.iter().find(|lane| lane.tx_count == 1).unwrap();
    assert_eq!(lane.proposer, Some(0));
    let payload = reader.read_lane(slot, lane.index).unwrap().unwrap();
    let admitted = Tx {
        mempool_admitted_at_ns: admitted_at,
        ..tx(1)
    };
    assert_eq!(decode_batch(&payload).unwrap(), [admitted]);
    // the proof is rlp(PqProof) with the fast path's own path vote
    let proof: PqProof = alloy_rlp::decode_exact(reader.read_proof(slot).unwrap()).unwrap();
    assert_eq!(proof.path, PqPath::Fast);
    assert_eq!(proof.metablock, finalized.metablock);
    assert_eq!(proof.path_votes.len(), 1);
    for lane in meta.lanes.iter().filter(|lane| !lane.is_positive()) {
        assert_eq!(reader.read_lane(slot, lane.index).unwrap(), None);
    }
}

#[test]
fn a_stream_of_txs_commits_each_exactly_once_and_in_order() {
    let config = config();
    let mut harness = Harness::new(&config);
    harness.run_until(at(GENESIS_MS));
    let interval = TimestampDelta::from_millis(37);
    let mut sent_at = BTreeMap::new();
    for nonce in 0..60 {
        sent_at.insert(nonce, harness.now);
        harness.deliver(&tx(nonce));
        let next = harness.now.checked_add_delta(interval).unwrap();
        harness.run_until(next);
    }
    harness.run_until(
        harness
            .now
            .checked_add_delta(TimestampDelta::from_millis(2_000))
            .unwrap(),
    );

    let committed = harness.committed();
    assert_eq!(
        committed.keys().copied().collect::<Vec<_>>(),
        (0..60).collect::<Vec<_>>()
    );
    let slots: Vec<u64> = committed
        .values()
        .map(|slots| {
            assert_eq!(slots.len(), 1, "committed once");
            slots[0]
        })
        .collect();
    assert!(slots.is_sorted(), "fifo across slots: {slots:?}");
    let mempool = harness.node.runtime().mempool().unwrap().lock();
    assert!(mempool.is_empty());
    assert_eq!(mempool.in_flight(), 0);
    drop(mempool);

    // §8.2 single node: e2e ≈ t_s + L, well under a second
    let finalized_at: BTreeMap<u64, Timestamp> =
        harness.finalized.iter().map(|f| (f.slot.0, f.at)).collect();
    for (nonce, slots) in &committed {
        let latency = finalized_at[&slots[0]]
            .duration_since(sent_at[nonce])
            .unwrap();
        assert!(
            latency <= TimestampDelta::from_millis(400),
            "tx {nonce} took {latency:?}"
        );
    }
}

#[test]
fn a_random_source_node_drops_txs() {
    let mut config = config();
    config.base.proposal.source = monad_mcp_node::config::SourceKind::Random;
    config.base.proposal.max_payload_bytes = Some(4096);
    let mut harness = Harness::new(&config);
    assert!(harness.node.runtime().mempool().is_none());
    harness.deliver(&tx(1));

    harness.run_until(at(GENESIS_MS + 1_000));
    let payloads: Vec<Bytes> = harness
        .finalized
        .iter()
        .flat_map(|finalized| finalized.proposals.clone().into_iter().flatten())
        .collect();
    assert!(!payloads.is_empty());
    assert!(payloads.iter().all(|payload| payload.len() <= 4096));
}

// the cap keeps moving: slots halt, complete and leave the slot manager
#[test]
fn halted_slots_complete_and_the_chain_rolls_over_windows() {
    let mut config = config();
    config.base.cadence.slots_per_window = 10.try_into().unwrap();
    config.base.cadence.sync_boundary_slots = 8.try_into().unwrap();
    let mut harness = Harness::new(&config);
    harness.run_until(at(GENESIS_MS + 5_000));
    let last = harness.finalized.last().unwrap().slot;
    assert!(last >= Slot(45), "rolled over several windows: {last:?}");
}
