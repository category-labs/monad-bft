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

//! Fault scenarios (§14.3): loss, partitions, equivocating proposers and
//! voters, and halting through the Repeater's Finalized messages alone.

mod common;

use std::sync::Arc;

use common::{LanePlan, Params, Scenario, lane_header, lane_pid};
use monad_mcp_chorus::stub::{
    CadenceMessage, SlotOutput,
    types::{NodeId, Slot},
};
use monad_mcp_chorus_sim::SimMessage;
use monad_pq_chorus::{
    Evidence, PqMessage, PqPath,
    message::VoteBody,
    sig::{Signed, StubPq},
    types::Entry,
};

fn ms(params: &Params, slot: u64) -> u64 {
    params.deadline(slot).as_nanos() as u64 / 1_000_000
}

#[test]
fn lossy_network_keeps_agreement_and_progress() {
    for (n, loss, seed) in [(4, 0.1, 3), (7, 0.1, 4)] {
        let mut params = Params::new(n, 4);
        params.slots = 32;
        let mut scenario = Scenario::new(params.clone());
        scenario.loss = loss;
        scenario.seed = seed;
        let outcome = scenario.run_until_ms(ms(&params, 32));
        outcome.assert_safety();
        for node in 0..n {
            let finalized = outcome.finalized_slots(node);
            assert!(
                (0..28).all(|slot| finalized.contains(&slot)),
                "n = {n}, node {node}: {finalized:?}"
            );
        }
    }
}

// f = 1 of 4 cut off for a second: the other three keep the fast path;
// after the heal the laggard catches up through the Repeater's Finalized
// messages or a cap jump
#[test]
fn partitioned_minority_catches_up_after_heal() {
    let mut params = Params::new(4, 4);
    params.slots = 40;
    let mut scenario = Scenario::new(params.clone());
    scenario.partitions = vec![(250..1250, vec![3])];
    let outcome = scenario.run_until_ms(ms(&params, 38));
    outcome.assert_safety();
    for node in 0..3 {
        let finalized = outcome.finalized_slots(node);
        assert!((0..36).all(|slot| finalized.contains(&slot)), "node {node}");
        for slot in 0..36 {
            assert_eq!(outcome.final_of(node, slot).unwrap().2, PqPath::Fast);
        }
    }
    let laggard = outcome.finalized_slots(3);
    assert!(
        (20..36).all(|slot| laggard.contains(&slot)),
        "laggard resumed: {laggard:?}"
    );
    // slots whose votes all fell into the partition: learned through the
    // Repeater's Finalized messages, or skipped by a cap jump
    for slot in (3..11).filter(|slot| laggard.contains(slot)) {
        assert_eq!(outcome.final_of(3, slot).unwrap().2, PqPath::Gadget);
    }
}

// lane 0's proposer signs root A for validators 0 and 1 and root B for 2
// and 3: each root has f+1 positive votes, each validator pulls the rival
fn equivocating_plan(params: &Params, slot: u64) -> common::Plan {
    let (lead, h) = (params.lead as i64, params.latency as i64);
    let timely = params.timely();
    Arc::new(move |node, s, j| {
        if s != Slot(slot) || j != 0 {
            return timely(node, s, j);
        }
        let variant = u8::from(u64::from(node) >= 2);
        vec![LanePlan {
            header: lane_header(s, j, variant),
            fulfilled_at: Some(-lead + h),
            decoded_at: Some(-lead + 2 * h),
            valid: true,
        }]
    })
}

fn assert_equivocation_handled(outcome: &common::Outcome, slot: u64, nodes: &[u64]) {
    let roots = [0, 1].map(|variant| Entry::Root(lane_pid(Slot(slot), 0, variant)));
    for node in nodes {
        let (_, metablock, _) = outcome.final_of(*node, slot).expect("finalized");
        assert!(
            metablock[0] == Entry::Bot || roots.contains(&metablock[0]),
            "node {node}"
        );
        let log = outcome.log(*node);
        assert!(
            log.evidence.iter().any(|(s, evidence)| *s == Slot(slot)
                && matches!(evidence, Evidence::EquivocatingHeaders { j: 0, .. })),
            "node {node} holds both headers"
        );
        assert_eq!(log.halts[&Slot(slot)].len(), 1);
    }
}

#[test]
fn equivocating_proposer_leaves_evidence_and_agreement() {
    let params = Params::new(4, 4);
    let mut scenario = Scenario::new(params.clone());
    scenario.plan = equivocating_plan(&params, 3);
    let outcome = scenario.run_until_ms(ms(&params, 15));
    outcome.assert_safety();
    assert_equivocation_handled(&outcome, 3, &[0, 1, 2, 3]);
    for node in 0..4 {
        assert_eq!(outcome.final_of(node, 3).unwrap().2, PqPath::Coda);
    }
}

// B1 (§4.3): the equivocation above, with validator 3 cut off over
// [D, D + Δ]: it learns the rival root only through PullRoot
#[test]
fn equivocation_with_a_partitioned_validator_still_inputs_coda() {
    const SLOT: u64 = 3;
    let params = Params::new(4, 4);
    let deadline = ms(&params, SLOT);
    let delta = params.config.delta.as_millis();
    let mut scenario = Scenario::new(params.clone());
    scenario.plan = equivocating_plan(&params, SLOT);
    scenario.partitions = vec![(deadline..deadline + delta, vec![3])];
    let outcome = scenario.run_until_ms(ms(&params, 15));
    outcome.assert_safety();
    assert_equivocation_handled(&outcome, SLOT, &[0, 1, 2, 3]);
    for node in 0..4 {
        assert!(outcome.finalized_slots(node).contains(&14));
    }
}

// validator 3 sends validator 0 two different signed votes, everyone else
// its real one
#[test]
fn equivocating_voter_is_convicted_by_its_receiver() {
    let params = Params::new(4, 4);
    let mut scenario = Scenario::new(params.clone());
    scenario.faulty.insert(
        3,
        Arc::new(|me, slot, output| match output {
            SlotOutput::Broadcast(PqMessage::Vote(vote)) => {
                let mut entries = vote.body.entries.clone();
                entries[1] = Entry::Bot;
                let signer = StubPq::default().signer(me);
                let rival = Signed::sign(slot, me, VoteBody { entries }, &signer);
                let mut sends: Vec<_> = (0..4)
                    .map(|to| SlotOutput::Unicast {
                        to: NodeId::dummy(to),
                        message: PqMessage::Vote(vote.clone()),
                    })
                    .collect();
                sends.push(SlotOutput::Unicast {
                    to: NodeId::dummy(0),
                    message: PqMessage::Vote(rival),
                });
                sends
            }
            other => vec![other],
        }),
    );
    let outcome = scenario.run_until_ms(ms(&params, 10));
    outcome.assert_safety();
    let convicted = |node: u64| {
        outcome.log(node).evidence.iter().any(|(_, evidence)| {
            matches!(evidence, Evidence::ConflictingVotes { signer, .. } if *signer == NodeId::dummy(3))
        })
    };
    assert!(convicted(0));
    assert!(!convicted(1) && !convicted(2));
    for node in 0..3 {
        assert!((0..9).all(|slot| outcome.final_of(node, slot).is_some()));
    }
}

// validator 3 hears nothing of the slots but Finalized messages: the
// halting gadget alone finalizes and halts it, once per slot
#[test]
fn finalized_messages_alone_finalize_and_halt() {
    let params = Params::new(4, 4);
    let mut scenario = Scenario::new(params.clone());
    scenario.drop_if = Some(Arc::new(|link, message| {
        u64::from(link.to) == 3
            && matches!(
                message,
                SimMessage::Cadence(CadenceMessage::Slot(_, slot_message))
                    if !matches!(slot_message, PqMessage::Finalized(_))
            )
    }));
    let outcome = scenario.run_until_ms(ms(&params, 20));
    outcome.assert_safety();
    for slot in 0..18 {
        let (_, _, path) = outcome.final_of(3, slot).expect("finalized");
        assert_eq!(path, PqPath::Gadget, "slot {slot}");
        assert_eq!(outcome.log(3).halts[&Slot(slot)].len(), 1);
        for node in 0..3 {
            assert_eq!(outcome.final_of(node, slot).unwrap().2, PqPath::Fast);
        }
    }
}
