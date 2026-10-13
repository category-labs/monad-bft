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

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use monad_mcp_chorus::spec::ProposalHeader as _;
use monad_mcp_node::{
    Effect, Node, Runtime,
    chorus::{
        Outbound,
        slot::chorus::{ChorusDAEvent, ProposalDAEvent},
        types::{MerkleRoot, NodeId, ProposalIndex, Slot, Timestamp},
    },
    component::{CadenceInput, DAInput, ProposingOutput},
};
use monad_sim::Time;
use monad_sim_swarm::{Network, Swarm};

use crate::{
    harness::{
        SLOT_INTERVAL, SimNode, assert_consistent_progress, deadline_of_slot, node_config, time_of,
    },
    tamper::Tampered,
};

// small payloads keep a proposal to a few chunks
const MAX_PROPOSAL_LEN: usize = 2000;

// each equivocation and the root of its third proposal
type Equivocations = Arc<Mutex<Vec<(Slot, ProposalIndex, MerkleRoot)>>>;
type Reports = Arc<Mutex<Vec<(Slot, ProposalIndex, ProposalDAEvent)>>>;

// every proposal goes out under three roots: two rivals first, as one
// chunk each that its receiver does not own, then everyone's own chunks
fn equivocating<R: Runtime>(
    runtime: R,
    others: Vec<NodeId>,
    equivocations: Equivocations,
) -> Tampered<R> {
    let mut tampered = Tampered::from(runtime);
    tampered.proposal_handler = Some(Box::new(
        move |runtime: &mut R, now, proposal: ProposingOutput| {
            let mut effects = Vec::new();
            for (variant, shift) in [(1, 1), (2, 2)] {
                let rival = propose_variant(runtime, now, &proposal, variant);
                effects.extend(misrouted(rival, &others, shift));
            }
            let third = propose_variant(runtime, now, &proposal, 3);

            let (slot, index, _) = proposal;
            let equivocation = (slot, index, proposed_root(&third));
            equivocations
                .lock()
                .expect("not poisoned")
                .push(equivocation);
            effects.extend(third);
            effects
        },
    ));
    tampered
}

// the proposal tagged with `variant`, so each variant has its own root
fn propose_variant<R: Runtime>(
    runtime: &mut R,
    now: Timestamp,
    (slot, index, message): &ProposingOutput,
    variant: u8,
) -> Vec<Effect> {
    let mut tagged = message[..message.len().min(MAX_PROPOSAL_LEN)].to_vec();
    tagged.push(variant);
    let mut effects = Vec::new();
    runtime.handle_proposal(now, (*slot, *index, tagged.into()), &mut effects);
    effects
}

fn proposed_root(effects: &[Effect]) -> MerkleRoot {
    for effect in effects {
        if let Effect::DA(DAInput::Envelope(_, envelope)) = effect {
            return *envelope.header().root();
        }
    }
    panic!("an author ingests its own proposal");
}

// each receiver's first chunk goes to the one `shift` places after it, who does not own it
fn misrouted(effects: Vec<Effect>, others: &[NodeId], shift: usize) -> Vec<Effect> {
    let mut rerouted = Vec::new();
    let mut first_hop_receivers = HashSet::new();
    for effect in effects {
        let Effect::Network(Outbound::Unicast(to, packet)) = effect else {
            rerouted.push(effect);
            continue;
        };
        if !first_hop_receivers.insert(to) {
            continue;
        }
        let position = others
            .iter()
            .position(|node| *node == to)
            .expect("the first hop goes to the other validators");
        let next = others[(position + shift) % others.len()];
        rerouted.push(Effect::Network(Outbound::Unicast(next, packet)));
    }
    rerouted
}

// every effect passes through, and what DA reports to consensus is kept
fn reporting<R: Runtime>(runtime: R, reports: Reports) -> Tampered<R> {
    Tampered::from(runtime).filtered(move |effect| {
        if let Effect::Cadence(CadenceInput::DAEvent(slot, ChorusDAEvent { j, event })) = &effect {
            let report = (*slot, *j, event.clone());
            reports.lock().expect("not poisoned").push(report);
        }
        vec![effect]
    })
}

fn reported_root(event: &ProposalDAEvent) -> MerkleRoot {
    match event {
        ProposalDAEvent::HeaderSeen(header) => *header.root(),
        ProposalDAEvent::ProposerObligationFulfilled(root)
        | ProposalDAEvent::Decoded(root)
        | ProposalDAEvent::DecodingFailed(root) => *root,
    }
}

// 4 validators, 1 equivocating proposer whose third root reaches the
// others after two rivals: their own chunks under it count for nothing
#[test]
fn own_chunks_under_a_third_root_win_no_vote() {
    const VALIDATORS: u64 = 4;
    const EQUIVOCATOR: u64 = 0;
    const SLOTS: u64 = 60;

    let equivocations = Equivocations::default();
    let others: Vec<NodeId> = (0..VALIDATORS)
        .filter(|id| *id != EQUIVOCATOR)
        .map(NodeId::dummy)
        .collect();
    let mut honest_reports = Vec::new();

    let network = Network::reliable(Duration::from_millis(10));
    let mut nodes = Vec::new();
    for id in 0..VALIDATORS {
        let node_id = NodeId::dummy(id);
        let node = Node::new(&node_config(id, VALIDATORS)).expect("config builds");
        let sim_node = if id == EQUIVOCATOR {
            let (others, equivocations) = (others.clone(), equivocations.clone());
            let node = node.map_runtime(|runtime| equivocating(runtime, others, equivocations));
            SimNode::new(node_id, node)
        } else {
            let reports = Reports::default();
            honest_reports.push((node_id, reports.clone()));
            let node = node.map_runtime(|runtime| reporting(runtime, reports));
            SimNode::new(node_id, node)
        };
        nodes.push((node_id, sim_node));
    }

    let mut swarm = Swarm::build(0xC0FFEE, network, nodes);
    for id in swarm.node_ids() {
        let handle = swarm.handle(&id).expect("node just built");
        let sim = swarm.sim();
        sim.schedule(handle, Time(0), "init", |node, ctx| node.init(ctx));
    }

    let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
    swarm.run_until_time(time_of(deadline_of_slot(SLOTS) + margin));
    assert_consistent_progress(&swarm, Slot(SLOTS - 20));

    let equivocations = equivocations.lock().expect("not poisoned").clone();
    assert!(!equivocations.is_empty(), "the equivocator never proposed");

    // DA reports both rivals and nothing of the third root
    for (id, reports) in &honest_reports {
        let reports = reports.lock().expect("not poisoned");
        let mut slots_with_both_rivals = 0;
        for (slot, index, third_root) in &equivocations {
            let mut headers = 0;
            for (report_slot, report_index, event) in reports.iter() {
                if (report_slot, report_index) != (slot, index) {
                    continue;
                }
                assert_ne!(
                    reported_root(event),
                    *third_root,
                    "node {id:?} reported {event:?} in {slot:?}"
                );
                if let ProposalDAEvent::HeaderSeen(_) = event {
                    headers += 1;
                }
            }
            if headers == 2 {
                slots_with_both_rivals += 1;
            }
        }
        assert!(
            slots_with_both_rivals > 0,
            "node {id:?} never saw both rivals"
        );
    }

    // and no node commits a third root
    for id in swarm.node_ids() {
        let finalized = swarm.with_node(&id, |node| node.finalized().clone());
        for (slot, index, third_root) in &equivocations {
            let Some(pattern) = finalized.get(slot) else {
                continue;
            };
            assert_ne!(
                pattern[*index],
                Some(*third_root),
                "node {id:?} committed the third root in {slot:?}"
            );
        }
    }
}
