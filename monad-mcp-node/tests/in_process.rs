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

//! A lone validator stepped in virtual time: txs arriving as `Packet::Tx`
//! reach the ledger through proposal, DA and finalization, with no tokio and
//! no sockets.

use std::collections::BTreeMap;

use bytes::Bytes;
use monad_mcp_chorus::ledger::{FinalizationPath, LedgerReader, LedgerWriter, Tx, decode_batch};
use monad_mcp_node::{
    Component as _, Dispatch, Effect, FinalizedSlot, Node, NodeOutput, NodeRuntime, Runtime,
    chorus::{
        slot::chorus::ChorusMessage,
        types::{NodeId, ProposerSchedule as _, Timestamp, TimestampDelta},
    },
    component::{CadenceOutput, ProposingOutput, RepeaterOutput},
    config::{NodeConfig, SourceKind},
    da::DAOutput,
    ledger::new_block,
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
        sent_at_ns: 0,             // demo(tx-timeline)
        rpc_received_at_ns: 0,     // demo(tx-timeline)
        mempool_admitted_at_ns: 0, // demo(tx-timeline)
    }
}

struct Harness<R = NodeRuntime> {
    node: Node<R>,
    now: Timestamp,
    finalized: Vec<FinalizedSlot>,
}

impl Harness {
    fn new(config: &NodeConfig) -> Self {
        Self::with(Node::new(config).unwrap())
    }
}

impl Harness {
    fn pooled(&self) -> usize {
        self.node.runtime().mempool().unwrap().lock().len()
    }
}

impl<R: Runtime> Harness<R> {
    // what the rpc sends: a tx frame under a validator's id
    fn deliver(&mut self, tx: &Tx) {
        let inbound = Inbound {
            from: NodeId::dummy(0),
            packet: Packet::Tx(tx.to_rlp()),
        };
        self.node.handle(self.now, inbound);
        self.drain();
    }

    fn with(node: Node<R>) -> Self {
        Self {
            node,
            now: at(GENESIS_MS - 1_000),
            finalized: Vec::new(),
        }
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
                NodeOutput::Finalized(finalized) => self.finalized.push(finalized),
                NodeOutput::Send(Outbound::Unicast(to, packet)) if to == self_id => {
                    let inbound = Inbound {
                        from: self_id,
                        packet,
                    };
                    self.node.handle(self.now, inbound);
                }
                NodeOutput::Send(_) => {}
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

#[test]
fn a_delivered_tx_is_finalized_and_written_to_the_ledger() {
    let config = NodeConfig::single_node(0, at(GENESIS_MS), "unused-ledger");
    let mut harness = Harness::new(&config);

    let admitted_at = harness.now.as_nanos() as u64; // demo(tx-timeline)
    harness.deliver(&tx(1));
    assert_eq!(harness.pooled(), 1);
    // a resend from the rpc is a duplicate, not a second entry
    harness.deliver(&tx(1));
    assert_eq!(harness.pooled(), 1);

    harness.run_until(at(GENESIS_MS + 3_000));
    assert!(harness.finalized.len() >= 25, "slots keep finalizing");
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
    let written = writer
        .write(&new_block(finalized, Some(&proposers)))
        .unwrap();

    let reader = LedgerReader::new(dir.path());
    assert_eq!(reader.scan().unwrap(), [slot]);
    let meta = reader.read_meta(slot).unwrap();
    assert_eq!(meta, written);
    assert_eq!(meta.path, FinalizationPath::Fast);
    assert_eq!(meta.tx_count(), 1);
    assert_eq!(meta.num_lanes as usize, config.proposal.num_proposals);
    assert_eq!(
        meta.deadline_ns,
        finalized.deadline.map(Timestamp::as_nanos)
    );
    assert!(meta.deadline_ns.unwrap() <= meta.finalized_at_ns);
    // demo(tx-timeline): the fast block forms between the deadline and finalization
    let fast_block_at = finalized.fast_block_at.unwrap().as_nanos(); // demo(tx-timeline)
    let timeline = reader.read_timeline(slot).unwrap().unwrap(); // demo(tx-timeline)
    assert_eq!(timeline.fast_block_at_ns, Some(fast_block_at)); // demo(tx-timeline)
    assert!(meta.deadline_ns.unwrap() <= fast_block_at); // demo(tx-timeline)
    assert!(fast_block_at <= meta.finalized_at_ns); // demo(tx-timeline)
    // demo(tx-timeline): a decode time for each positive lane, none for a negative one
    assert_eq!(timeline.lane_decoded_at_ns.len(), meta.lanes.len());
    for (lane, decoded_at) in meta.lanes.iter().zip(&timeline.lane_decoded_at_ns) {
        assert_eq!(lane.is_positive(), decoded_at.is_some());
        assert!(decoded_at.is_none_or(|at| at <= meta.finalized_at_ns));
    }

    let lane = meta.lanes.iter().find(|lane| lane.tx_count == 1).unwrap();
    assert_eq!(lane.proposer, Some(0));
    let payload = reader.read_lane(slot, lane.index).unwrap().unwrap();
    // demo(tx-timeline): the mempool stamped its admission
    let admitted = Tx {
        mempool_admitted_at_ns: admitted_at,
        ..tx(1)
    };
    assert_eq!(decode_batch(&payload).unwrap(), [admitted]);
    // the proof is the finalization's certificate as it crosses the wire
    let proof = reader.read_proof(slot).unwrap();
    let certificate: ChorusMessage = alloy_rlp::decode_exact(&proof).unwrap();
    assert_eq!(certificate, finalized.finalization.certificate_message());
    // negative lanes have no lane file
    for lane in meta.lanes.iter().filter(|lane| !lane.is_positive()) {
        assert_eq!(reader.read_lane(slot, lane.index).unwrap(), None);
    }
}

#[test]
fn a_stream_of_txs_commits_each_exactly_once_and_in_order() {
    let config = NodeConfig::single_node(0, at(GENESIS_MS), "unused-ledger");
    let mut harness = Harness::new(&config);
    let interval = TimestampDelta::from_millis(37);
    for nonce in 0..60 {
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
    assert!(harness.node.runtime().mempool().unwrap().lock().is_empty());
    assert_eq!(
        harness.node.runtime().mempool().unwrap().lock().in_flight(),
        0
    );
}

#[test]
fn a_random_source_node_drops_txs() {
    let mut config = NodeConfig::single_node(0, at(GENESIS_MS), "unused-ledger");
    config.proposal.source = SourceKind::Random;
    config.proposal.max_payload_bytes = Some(4096);
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

// drops our first non-empty proposal, so its lane finalizes negative
struct DropFirstBatch {
    inner: NodeRuntime,
    dropped: Option<u64>,
}

impl Runtime for DropFirstBatch {
    fn handle_inbound(
        &mut self,
        now: Timestamp,
        inbound: Inbound,
        effects: &mut impl Dispatch<Effect>,
    ) {
        self.inner.handle_inbound(now, inbound, effects);
    }

    fn handle_cadence(&mut self, output: CadenceOutput, effects: &mut impl Dispatch<Effect>) {
        self.inner.handle_cadence(output, effects);
    }

    fn handle_da(&mut self, now: Timestamp, output: DAOutput, effects: &mut impl Dispatch<Effect>) {
        self.inner.handle_da(now, output, effects);
    }

    fn handle_proposal(
        &mut self,
        now: Timestamp,
        proposal: ProposingOutput,
        effects: &mut impl Dispatch<Effect>,
    ) {
        let (slot, _, payload) = &proposal;
        if self.dropped.is_none() && !decode_batch(payload).unwrap().is_empty() {
            self.dropped = Some(slot.0);
            return;
        }
        self.inner.handle_proposal(now, proposal, effects);
    }

    fn handle_repeat(
        &mut self,
        repeat: RepeaterOutput<ChorusMessage>,
        effects: &mut impl Dispatch<Effect>,
    ) {
        self.inner.handle_repeat(repeat, effects);
    }
}

#[test]
fn a_batch_left_out_of_its_block_is_proposed_again() {
    let config = NodeConfig::single_node(0, at(GENESIS_MS), "unused-ledger");
    let node = Node::new(&config)
        .unwrap()
        .map_runtime(|inner| DropFirstBatch {
            inner,
            dropped: None,
        });
    let mut harness = Harness::with(node);
    for nonce in 0..3 {
        harness.deliver(&tx(nonce));
    }
    harness.run_until(at(GENESIS_MS + 3_000));

    let dropped = harness.node.runtime().dropped.expect("a batch was dropped");
    let block = harness
        .finalized
        .iter()
        .find(|finalized| finalized.slot.0 == dropped)
        .expect("the slot finalized");
    assert!(
        block
            .proposals
            .clone()
            .into_iter()
            .all(|lane| lane.is_none())
    );

    let committed = harness.committed();
    assert_eq!(committed.keys().copied().collect::<Vec<_>>(), [0, 1, 2]);
    for slots in committed.values() {
        let [slot] = slots[..] else {
            panic!("committed once: {slots:?}");
        };
        assert!(slot > dropped);
    }
}

#[test]
fn an_oversized_tx_is_never_committed() {
    let config = NodeConfig::single_node(0, at(GENESIS_MS), "unused-ledger");
    let mut harness = Harness::new(&config);
    let oversized = Tx {
        payload: Bytes::from(vec![3; monad_mcp_chorus::ledger::MAX_TX_PAYLOAD + 1]),
        ..tx(9)
    };
    harness.deliver(&oversized);
    harness.deliver(&tx(1));
    assert_eq!(harness.pooled(), 1);
    harness.run_until(at(GENESIS_MS + 2_000));
    assert_eq!(harness.committed().keys().copied().collect::<Vec<_>>(), [1]);
}
