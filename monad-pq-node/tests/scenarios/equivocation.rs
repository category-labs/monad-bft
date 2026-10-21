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

//! §14.3 B1 with the real pq DA: an equivocating proposer sends root r1 to
//! some validators and r2 to the others, each with f+1 positive votes, and
//! a correct node misses every copy of r1 until after D + Δ. It has to
//! resolve r1 through `PullRoot` to obtain clean sets and input Coda.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use monad_mcp_chorus::spec::ProposalHeader as _;
use monad_pq_chorus::{Evidence, PqPath};
use monad_pq_node::{
    PqEffect, PqNode, PqNodeRuntime, PqRuntime,
    chorus::types::{NodeId, ProposalId, ProposalIndex, ProposerSchedule as _, Slot, Timestamp},
    component::{PqDAInput, ProposingOutput},
    da::{AssembledProposal, read_envelope},
    network::{Outbound, Packet},
    runtime::unix_seconds,
};
use monad_sim_swarm::Network;

use crate::{
    harness::{
        DELTA, SimNode, assert_consistent_progress, build_swarm, deadline_of_slot, node_config,
        run_through, time_of,
    },
    tamper::Tampered,
};

const VALIDATORS: u64 = 4;
const SLOTS: u64 = 80;
const EQUIVOCATOR: u64 = 0;
// r1 goes to node 1, r2 to nodes 2 and 3; the equivocator votes r1
const GETS_R2: [u64; 2] = [2, 3];
// misses every copy of r1 until D + Δ
const LAGGARD: u64 = 3;

type Roots = Arc<Mutex<BTreeMap<(Slot, ProposalIndex), (ProposalId, ProposalId)>>>;

// the first `count` slots from `from` on, at least ten apart, where the
// equivocator holds a lane
fn equivocator_slots(from: u64, count: usize) -> Vec<Slot> {
    let handle = node_config(EQUIVOCATOR, VALIDATORS)
        .base
        .epoch_handle()
        .unwrap();
    let me = NodeId::dummy(EQUIVOCATOR);
    let mut slots = Vec::new();
    let mut slot = from;
    while slots.len() < count {
        let set = handle.proposers.proposers_at(Slot(slot)).unwrap();
        if set.iter().any(|(_, proposer)| proposer == Some(me)) {
            slots.push(Slot(slot));
            slot += 10;
        } else {
            slot += 1;
        }
    }
    slots
}

// splits each proposal of a target slot into two roots: r1 is the honest
// payload, r2 the same payload with its last byte flipped
fn equivocate(
    targets: BTreeSet<Slot>,
    roots: Roots,
) -> impl FnMut(&mut PqNodeRuntime, Timestamp, ProposingOutput) -> Vec<PqEffect> {
    move |runtime, now, (slot, index, message)| {
        let mut effects = Vec::new();
        if !targets.contains(&slot) {
            runtime.handle_proposal(now, (slot, index, message), &mut effects);
            return effects;
        }
        let mut rival = message.to_vec();
        *rival.last_mut().expect("a batch is never empty") ^= 0xff;
        let da = runtime.da_handle();
        let build = |message: &[u8]| {
            AssembledProposal::build_s11(da, slot, index, message, unix_seconds(now)).unwrap()
        };
        let (r1, r2) = (build(&message), build(&Bytes::from(rival)));
        roots
            .lock()
            .unwrap()
            .insert((slot, index), (r1.header().pid(), r2.header().pid()));

        let me = NodeId::dummy(EQUIVOCATOR);
        let mut first_hop = r1.disseminate();
        if let Some(envelope) = first_hop.split_off(&me) {
            effects.push(PqEffect::DA(PqDAInput::Envelope(me, envelope)));
        }
        effects.push(PqEffect::DA(PqDAInput::Envelope(me, first_hop.envelope())));
        let to_r2 = |to: &NodeId| GETS_R2.contains(&u64::from(*to));
        let r1_packets = first_hop
            .into_packets()
            .into_iter()
            .filter(|(to, _)| !to_r2(to));
        let r2_packets = r2
            .disseminate()
            .into_packets()
            .into_iter()
            .filter(|(to, _)| to_r2(to));
        for (to, packet) in r1_packets.chain(r2_packets) {
            let packet = Packet::Chunk(packet);
            effects.push(PqEffect::Network(Outbound::Unicast(*to, packet)));
        }
        effects
    }
}

fn chunk_pid(packet: &Packet) -> Option<(Slot, ProposalId)> {
    let Packet::Chunk(bytes) = packet else {
        return None;
    };
    let envelope = read_envelope(bytes.clone()).ok()?;
    let header = envelope.header();
    Some((Slot(header.slot()), header.pid()))
}

#[test]
fn a_split_root_is_pulled_by_the_node_that_missed_it() {
    let targets = equivocator_slots(20, 2);
    let roots: Roots = Arc::default();

    let shared = Rc::new(RefCell::new(roots.clone()));
    let first = targets[0];
    let blind_until = time_of(deadline_of_slot(first.0) + DELTA);
    let network = Network::reliable(Duration::from_millis(10)).drop_if(move |link, packet| {
        if u64::from(link.to) != LAGGARD || link.now >= blind_until {
            return false;
        }
        let Some((slot, pid)) = chunk_pid(packet) else {
            return false;
        };
        let roots = shared.borrow();
        let roots = roots.lock().unwrap();
        roots
            .iter()
            .any(|(&(s, _), &(r1, _))| s == first && slot == first && pid == r1)
    });

    let nodes = (0..VALIDATORS)
        .map(|id| {
            let node_id = NodeId::dummy(id);
            let node = PqNode::new(&node_config(id, VALIDATORS)).expect("config builds");
            let sim_node = if id == EQUIVOCATOR {
                let handler = equivocate(targets.iter().copied().collect(), roots.clone());
                SimNode::new(
                    node_id,
                    node.map_runtime(|r| Tampered::from(r).proposing(handler)),
                )
            } else {
                SimNode::new(node_id, node)
            };
            (node_id, sim_node)
        })
        .collect();
    let mut swarm = build_swarm(0xB1, network, nodes);
    run_through(&mut swarm, SLOTS);

    let roots = roots.lock().unwrap().clone();
    for slot in &targets {
        assert!(
            roots.keys().any(|(s, _)| s == slot),
            "the equivocator proposed in {slot:?}"
        );
    }
    let all: Vec<NodeId> = (0..VALIDATORS).map(NodeId::dummy).collect();
    assert_consistent_progress(&swarm, &all, Slot(SLOTS - 10));

    // each split lane is ⊥ or one of the two roots, the same everywhere
    let laggard = NodeId::dummy(LAGGARD);
    let finalized = swarm.with_node(&laggard, |node| node.finalized().clone());
    for (&(slot, j), &(r1, r2)) in &roots {
        // no root had a fast quorum: the slot went through Coda
        assert_eq!(finalized[&slot].path, PqPath::Coda, "{slot:?}");
        let committed = finalized[&slot].pattern[j];
        assert!(
            committed.is_none_or(|pid| pid == r1 || pid == r2),
            "{slot:?} lane {j} committed {committed:?}"
        );
    }

    // M2: no Coda cascade; the slots after a Coda slot take the fast path
    for slot in (first.0 + 1..first.0 + 10).map(Slot) {
        assert_eq!(
            finalized[&slot].path,
            PqPath::Fast,
            "{slot:?} after {first:?}"
        );
    }

    // whoever saw both headers proves the proposer faulty
    let convicted = all.iter().any(|id| {
        swarm.with_node(id, |node| {
            node.evidence().iter().any(|(slot, evidence)| {
                targets.contains(slot)
                    && matches!(evidence, Evidence::EquivocatingHeaders { a, .. }
                        if u64::from(a.sig.signer) == EQUIVOCATOR)
            })
        })
    });
    assert!(convicted, "no EquivocatingHeaders evidence");
}
