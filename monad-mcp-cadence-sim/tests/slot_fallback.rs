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

//! Four validators' slot fallback instances -- the dummy vote beside a led
//! MVBA -- on a simulated network, with `n = 4`, `f = 1` and `Δ = 100 ms`.
//! Scenario numbers S1-S14 are those of `tasks/plans/mvba-v2.md`. Every
//! scenario asserts agreement on entries and that no node reports a second,
//! different decision

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use cadence::{
    slot::fallback::{
        EnterFallbackCert, Entry, FallbackView, Metablock, Votable,
        leaderless::{DummyVote, LeaderlessMessage, NfCert, NotFull},
        monad_mvba::MvbaMessage,
        slot_fallback::{CertifiedMetablock, SlotCommitProof, SlotFallback, SlotFallbackMessage},
    },
    types::{StrongQc, ValidatorData, VoteMsg},
};
use monad_mcp_cadence::stub as cadence;
use monad_mcp_cadence_sim::{
    Proposal, SimMessage, SlotFallbackSwarm, SlotFallbackSwarmBuilder, commit_view,
    forge_pre_prepare, prepare, proposal, tc_view, timeout_view,
};
use monad_mcp_common::{Lanes, TimestampDelta, stub::NodeId};
use monad_sim::{Time, dist::uniform_duration};
use monad_sim_swarm::{Link, Network};

// only the fixtures are used here
#[allow(dead_code)]
mod helper;

use helper::fixtures;

type Input = (Metablock, Option<EnterFallbackCert>);
type Net = Network<NodeId, SimMessage<SlotFallbackMessage>>;

/// The protocol's Δ, and every link's latency: messages arrive at the bound
const DELTA_MILLIS: u64 = 100;
const DELTA: Duration = Duration::from_millis(DELTA_MILLIS);
const LATENCY: Duration = DELTA;

/// `T = Δ_R + 5Δ` with `Δ_R = 2Δ`
const VIEW_TIMEOUT: Duration = Duration::from_millis(7 * DELTA_MILLIS);

/// The resend loop's interval, standing in for the `Repeater`'s
const RESEND: Duration = DELTA;

fn deltas(n: u32) -> Time {
    Time(0) + n * DELTA
}

fn views(n: u32) -> Time {
    Time(0) + n * VIEW_TIMEOUT
}

fn slot_fallback(node: NodeId, validator_data: &Arc<ValidatorData>) -> SlotFallback {
    fixtures::mvba(
        node,
        validator_data,
        TimestampDelta::from_millis(DELTA_MILLIS),
    )
}

/// A view's number; the type keeps it private, its wire encoding does not
fn view_number(view: FallbackView) -> u64 {
    let bytes = alloy_rlp::encode(view);
    <u64 as alloy_rlp::Decodable>::decode(&mut bytes.as_slice())
        .expect("a view encodes as its number")
}

fn entries(block: &Metablock) -> Lanes<Entry> {
    Votable::entries(block)
}

/// Every node gets its input, starting at `start_of(node)`
fn build(
    seed: u64,
    network: Net,
    inputs: &BTreeMap<NodeId, Input>,
    start_of: impl Fn(NodeId) -> Time,
    configure: impl FnOnce(&mut SlotFallbackSwarmBuilder),
) -> SlotFallbackSwarm {
    let validator_data = fixtures::validator_data();
    let mut builder = SlotFallbackSwarmBuilder::new();
    builder.set_seed(seed).set_network(network);
    for (node, (block, cert)) in inputs {
        builder.add_node(
            *node,
            slot_fallback(*node, &validator_data),
            block.clone(),
            cert.clone(),
            start_of(*node),
        );
    }
    configure(&mut builder);
    builder.build()
}

fn all_at_zero(_: NodeId) -> Time {
    Time(0)
}

fn nothing(_: &mut SlotFallbackSwarmBuilder) {}

/// `fast` nodes get the fast input, the rest a non-fast input of their own
fn mixed_inputs(fast: &[NodeId]) -> BTreeMap<NodeId, Input> {
    let validator_data = fixtures::validator_data();
    fixtures::nodes()
        .into_iter()
        .enumerate()
        .map(|(i, node)| {
            let input = if fast.contains(&node) {
                fixtures::fast_input(1, &validator_data)
            } else {
                fixtures::non_fast_input(10 + i as u64, &validator_data)
            };
            (node, input)
        })
        .collect()
}

fn fast_entries() -> Lanes<Entry> {
    entries(&fixtures::fast_metablock(1, &fixtures::validator_data()))
}

/// Agreement on entries among `honest`, no second decision, and external
/// validity; returns the agreed entries
fn expect_agreement(swarm: &SlotFallbackSwarm, honest: &[NodeId]) -> Lanes<Entry> {
    for node in honest {
        assert!(
            !swarm.conflicted(node),
            "{node:?} reported a second, different decision"
        );
    }
    let decisions: Vec<_> = swarm
        .decisions()
        .into_iter()
        .filter(|(node, _)| honest.contains(node))
        .collect();
    let (_, first) = decisions.first().expect("no honest node decided");
    let agreed = entries(&first.block);
    for (node, decision) in &decisions {
        assert_eq!(
            entries(&decision.block),
            agreed,
            "{node:?} decided different entries"
        );
        if let Some(SlotCommitProof::Dummy(qc)) = &decision.proof {
            assert_eq!(
                qc.verdict.0, agreed,
                "{node:?}'s DummyQC certifies other entries"
            );
        }
    }
    assert!(
        swarm
            .inputs()
            .values()
            .any(|input| entries(input) == agreed),
        "the decided entries were nobody's input"
    );
    agreed
}

fn expect_all_decided(swarm: &SlotFallbackSwarm, nodes: &[NodeId]) {
    for node in nodes {
        assert!(
            swarm.decision_of(node).is_some(),
            "{node:?} did not decide by {:?}",
            swarm.now()
        );
    }
}

fn is_dummy_proof(proof: &Option<SlotCommitProof>) -> bool {
    matches!(proof, Some(SlotCommitProof::Dummy(_)))
}

/// The view of an MVBA decision's `CommitQC`, `None` for any other proof
fn mvba_view(proof: &Option<SlotCommitProof>) -> Option<u64> {
    match proof {
        Some(SlotCommitProof::Mvba(qc)) => Some(view_number(qc.scope.view)),
        _ => None,
    }
}

fn leaderless(message: &SlotFallbackMessage) -> Option<&LeaderlessMessage> {
    match message {
        SlotFallbackMessage::Leaderless(message) => Some(message),
        SlotFallbackMessage::Mvba(_) => None,
    }
}

fn is_dummy_vote(message: &SlotFallbackMessage) -> bool {
    matches!(leaderless(message), Some(LeaderlessMessage::DummyVote(_)))
}

fn is_not_full(message: &SlotFallbackMessage) -> bool {
    matches!(leaderless(message), Some(LeaderlessMessage::NotFull(_)))
}

fn is_fast_input(message: &SlotFallbackMessage) -> bool {
    matches!(leaderless(message), Some(LeaderlessMessage::FastInput(_)))
}

fn is_dummy_decision(message: &SlotFallbackMessage) -> bool {
    matches!(
        leaderless(message),
        Some(LeaderlessMessage::DummyDecision { .. })
    )
}

fn is_pre_prepare(message: &SlotFallbackMessage) -> bool {
    proposal(message).is_some()
}

fn is_tc(message: &SlotFallbackMessage) -> bool {
    tc_view(message).is_some()
}

fn is_commit_qc(message: &SlotFallbackMessage) -> bool {
    matches!(message, SlotFallbackMessage::Mvba(MvbaMessage::CommitQc(_)))
}

/// Every proposal `node` sent, with when
fn proposals_of(swarm: &SlotFallbackSwarm, node: &NodeId) -> Vec<(Time, Proposal)> {
    swarm
        .sent(node)
        .into_iter()
        .filter_map(|sent| proposal(&sent.message).map(|p| (sent.at, p)))
        .collect()
}

fn sent_any(
    swarm: &SlotFallbackSwarm,
    nodes: &[NodeId],
    pred: impl Fn(&SlotFallbackMessage) -> bool,
) -> bool {
    nodes
        .iter()
        .any(|node| swarm.sent(node).iter().any(|sent| pred(&sent.message)))
}

fn cadence_message(message: &SimMessage<SlotFallbackMessage>) -> Option<&SlotFallbackMessage> {
    match message {
        SimMessage::Cadence(message) => Some(message),
        SimMessage::Da(_) => None,
    }
}

/// S1. Four fast inputs: the `DummyQC` decides within Δ of the last entry,
/// while the view-1 leader's fast proposal is in flight and decides nothing more
#[test]
fn s1_four_fast_inputs_decide_by_dummy_qc_within_delta_beside_the_leader_s_proposal() {
    let inputs = mixed_inputs(&fixtures::nodes());
    let mut swarm = build(0, Network::reliable(LATENCY), &inputs, all_at_zero, nothing);
    let nodes = fixtures::nodes();

    assert!(
        swarm.run_until_all_decided(deltas(1)),
        "not everyone decided within Δ: {:?}",
        swarm.decisions().keys()
    );
    // long enough for the leader's proposal to have run its three rounds
    swarm.run_until_or(views(2), |_| false);

    assert_eq!(expect_agreement(&swarm, &nodes), fast_entries());
    for (node, decision) in swarm.decisions() {
        assert!(decision.at <= deltas(1), "{node:?} decided late");
        assert!(
            is_dummy_proof(&decision.proof),
            "{node:?} did not decide by DummyQC"
        );
    }

    let leader = fixtures::leader_of(1);
    let proposals = proposals_of(&swarm, &leader);
    let (at, p) = proposals.first().expect("the view-1 leader never proposed");
    assert_eq!(*at, Time(0), "the view-1 leader did not propose at entry");
    assert_eq!(p.view, 1);
    assert_eq!(
        entries(&p.value.block),
        fast_entries(),
        "the view-1 leader did not propose the fast block"
    );
    assert!(
        !sent_any(&swarm, &nodes, is_commit_qc),
        "the core decided too, after the DummyQC abandoned it"
    );
}

/// S2. Four identical non-fast inputs: no `DummyVote`; the view-1 leader holds
/// the `NfCert` within Δ, proposes its input, and all decide in view 1 by 4Δ
/// without a Timeout
#[test]
fn s2_identical_non_fast_inputs_decide_in_view_1_on_the_leader_s_nf_cert_without_a_timeout() {
    let validator_data = fixtures::validator_data();
    let input = fixtures::non_fast_input(1, &validator_data);
    let inputs: BTreeMap<NodeId, Input> = fixtures::nodes()
        .into_iter()
        .map(|node| (node, input.clone()))
        .collect();
    let mut swarm = build(0, Network::reliable(LATENCY), &inputs, all_at_zero, nothing);
    let nodes = fixtures::nodes();

    assert!(
        swarm.run_until_all_decided(deltas(4)),
        "not everyone decided by 4Δ: {:?}",
        swarm.decisions().keys()
    );
    swarm.run_until_or(views(2), |_| false);

    assert_eq!(expect_agreement(&swarm, &nodes), entries(&input.0));
    for (node, decision) in swarm.decisions() {
        assert!(decision.at <= deltas(4), "{node:?} decided late");
        assert_eq!(mvba_view(&decision.proof), Some(1), "{node:?}");
    }

    assert!(
        !sent_any(&swarm, &nodes, is_dummy_vote),
        "a DummyVote was sent"
    );
    for node in &nodes {
        assert!(
            swarm
                .sent(node)
                .iter()
                .any(|sent| is_not_full(&sent.message)),
            "{node:?} did not declare NotFull"
        );
    }

    let leader = fixtures::leader_of(1);
    let proposals = proposals_of(&swarm, &leader);
    let (at, p) = proposals.first().expect("the view-1 leader never proposed");
    assert!(
        *at <= deltas(1),
        "the leader proposed at {at:?}, not within Δ"
    );
    assert_eq!(p.view, 1);
    assert_eq!(
        p.value.block, input.0,
        "the leader did not propose its input"
    );
    assert!(
        p.value.fbcert.is_some(),
        "a non-fast value without its FBCert"
    );
    let cert = p
        .cert
        .as_ref()
        .expect("an unlocked non-fast proposal needs the NfCert");
    assert!(cert.verify(&validator_data), "the NfCert does not verify");

    assert!(
        !sent_any(&swarm, &nodes, |m| timeout_view(m).is_some()),
        "a Timeout was sent"
    );
}

/// S3. Three fast, one non-fast: the `DummyQC` decides the fast entries, the
/// non-fast node too, from the received block or the `DummyDecision`
#[test]
fn s3_three_fast_and_one_non_fast_input_decide_the_fast_entries_by_dummy_qc() {
    let nodes = fixtures::nodes();
    // the non-fast node leads view 1, so it also holds a received fast block
    let non_fast = fixtures::leader_of(1);
    let fast: Vec<NodeId> = nodes.iter().copied().filter(|n| *n != non_fast).collect();
    let inputs = mixed_inputs(&fast);
    let mut swarm = build(0, Network::reliable(LATENCY), &inputs, all_at_zero, nothing);

    assert!(
        swarm.run_until_all_decided(views(1)),
        "not everyone decided in view 1's time: {:?}",
        swarm.decisions().keys()
    );
    swarm.run_until_or(views(2), |_| false);

    assert_eq!(expect_agreement(&swarm, &nodes), fast_entries());
    for (node, decision) in swarm.decisions() {
        assert!(
            is_dummy_proof(&decision.proof),
            "{node:?} did not decide by DummyQC"
        );
    }
    assert!(
        !swarm
            .sent(&non_fast)
            .iter()
            .any(|sent| is_dummy_vote(&sent.message)),
        "the non-fast node cast a DummyVote"
    );
}

/// S4. Two fast, two non-fast, with a fast and then a non-fast view-1 leader:
/// no `DummyQC`, no `NfCert`; the leader proposes the fast block, its own or a
/// received one, and all decide it by `CommitQC` in view 1
#[test]
fn s4_two_fast_and_two_non_fast_inputs_decide_the_fast_block_by_commit_qc_in_view_1() {
    let nodes = fixtures::nodes();
    let leader = fixtures::leader_of(1);
    let others: Vec<NodeId> = nodes.iter().copied().filter(|n| *n != leader).collect();

    for fast_leader in [true, false] {
        let fast = match fast_leader {
            true => vec![leader, others[0]],
            false => vec![others[0], others[1]],
        };
        let inputs = mixed_inputs(&fast);
        let mut swarm = build(0, Network::reliable(LATENCY), &inputs, all_at_zero, nothing);
        let case = format!("fast leader: {fast_leader}");

        assert!(
            swarm.run_until_all_decided(views(1)),
            "{case}: not everyone decided in view 1: {:?}",
            swarm.decisions().keys()
        );

        assert_eq!(expect_agreement(&swarm, &nodes), fast_entries(), "{case}");
        for (node, decision) in swarm.decisions() {
            assert_eq!(mvba_view(&decision.proof), Some(1), "{case}: {node:?}");
        }
        assert!(
            !sent_any(&swarm, &nodes, is_dummy_decision),
            "{case}: a DummyQC formed"
        );

        let proposals = proposals_of(&swarm, &leader);
        let (_, p) = proposals.first().expect("the view-1 leader never proposed");
        assert_eq!(p.view, 1, "{case}");
        assert_eq!(
            p.value.block,
            fixtures::fast_metablock(1, &fixtures::validator_data()),
            "{case}: the leader did not propose the fast block"
        );
        assert!(
            p.value.fbcert.is_none(),
            "{case}: a fast value with an FBCert"
        );
        for node in &nodes {
            for (_, p) in proposals_of(&swarm, node) {
                assert!(p.cert.is_none(), "{case}: {node:?} proposed with an NfCert");
            }
        }
    }
}

/// S5. S2 with a silent view-1 leader: `TC_1` at `T`, then the view-2 leader
/// proposes its input with the `NfCert` it obtained during view 1
#[test]
fn s5_a_silent_view_1_leader_is_replaced_by_the_view_2_leader_with_its_nf_cert() {
    let validator_data = fixtures::validator_data();
    let input = fixtures::non_fast_input(1, &validator_data);
    let inputs: BTreeMap<NodeId, Input> = fixtures::nodes()
        .into_iter()
        .map(|node| (node, input.clone()))
        .collect();
    let mut swarm = build(0, Network::reliable(LATENCY), &inputs, all_at_zero, nothing);

    let silent = fixtures::leader_of(1);
    swarm.swarm_mut().remove_node(&silent);
    let live: Vec<NodeId> = fixtures::nodes()
        .into_iter()
        .filter(|node| *node != silent)
        .collect();

    // the Timeouts cross once, then view 2 runs its three rounds
    let deadline = views(1) + 4 * LATENCY;
    assert!(
        swarm.run_until_all_decided(deadline),
        "the survivors did not decide by {deadline:?}: {:?}",
        swarm.decisions().keys()
    );

    assert_eq!(expect_agreement(&swarm, &live), entries(&input.0));
    for (node, decision) in swarm.decisions() {
        assert!(decision.at > views(1), "{node:?} decided before timing out");
        assert_eq!(mvba_view(&decision.proof), Some(2), "{node:?}");
    }
    for node in &live {
        let first_timeout = swarm
            .sent(node)
            .into_iter()
            .find(|sent| timeout_view(&sent.message) == Some(1))
            .unwrap_or_else(|| panic!("{node:?} never timed out view 1"));
        assert_eq!(
            first_timeout.at,
            views(1),
            "{node:?} timed out view 1 off T"
        );
    }

    let leader = fixtures::leader_of(2);
    let proposals = proposals_of(&swarm, &leader);
    let (_, p) = proposals
        .iter()
        .find(|(_, p)| p.view == 2)
        .expect("the view-2 leader never proposed");
    assert!(p.justified, "a view-2 proposal without TC_1");
    assert_eq!(
        p.value.block, input.0,
        "the view-2 leader did not propose its input"
    );
    let cert = p
        .cert
        .as_ref()
        .expect("an unlocked non-fast proposal needs the NfCert");
    assert!(cert.verify(&validator_data), "the NfCert does not verify");
}

/// S6. Non-fast inputs; view 1 forms a prepare certificate but its Commits are
/// lost. The view-2 leader re-proposes the locked value, carrying its `FBCert`
/// inside the value and no `NfCert`, and all decide the locked entries
#[test]
fn s6_a_value_locked_in_view_1_is_re_proposed_with_its_fbcert_and_without_an_nf_cert() {
    let inputs = mixed_inputs(&[]);
    let view_1_ends = views(1);
    let network = Network::reliable(LATENCY).drop_if(move |link: &Link<NodeId>, message| {
        link.now < view_1_ends && cadence_message(message).and_then(commit_view).is_some()
    });
    let mut swarm = build(0, network, &inputs, all_at_zero, nothing);
    let nodes = fixtures::nodes();

    assert!(
        swarm.run_until_all_decided(views(3)),
        "not everyone decided: {:?}",
        swarm.decisions().keys()
    );

    let first_leader = fixtures::leader_of(1);
    let locked = &inputs[&first_leader].0;
    assert_eq!(expect_agreement(&swarm, &nodes), entries(locked));
    for (node, decision) in swarm.decisions() {
        assert_eq!(mvba_view(&decision.proof), Some(2), "{node:?}");
    }

    // a Commit is sent only on a prepare certificate
    let committers = nodes
        .iter()
        .filter(|node| {
            swarm
                .sent(node)
                .iter()
                .any(|sent| commit_view(&sent.message) == Some(1))
        })
        .count();
    assert!(committers >= 3, "view 1 formed no prepare certificate");

    let second_leader = fixtures::leader_of(2);
    assert_ne!(
        &inputs[&second_leader].0, locked,
        "the test needs distinct inputs"
    );
    let proposals = proposals_of(&swarm, &second_leader);
    let (_, p) = proposals
        .iter()
        .find(|(_, p)| p.view == 2)
        .expect("the view-2 leader never proposed");
    assert!(p.justified, "a view-2 proposal without TC_1");
    assert_eq!(&p.value.block, locked, "the view-2 leader broke the lock");
    assert!(
        p.value.fbcert.is_some(),
        "the locked non-fast value lost its FBCert"
    );
    assert!(p.cert.is_none(), "a locked proposal carries an NfCert");
}

/// S7. A validator enters after the others reached view 3: it sends its
/// evidence, and its `DummyVote` if its input is fast, jumps on a `Tc` without
/// any Pre-Prepare, and decides with the rest
#[test]
fn s7_a_validator_entering_past_view_3_sends_evidence_jumps_on_a_tc_and_decides_with_the_rest() {
    let nodes = fixtures::nodes();
    // the late node leads view 1; the others have no fast block, and their
    // declarations are lost until it enters, so no view before then can decide
    let late = fixtures::leader_of(1);
    let late_start = views(3);

    for late_fast in [true, false] {
        let mut inputs = mixed_inputs(&[]);
        if late_fast {
            inputs.insert(late, fixtures::fast_input(1, &fixtures::validator_data()));
        }
        let network = Network::reliable(LATENCY).drop_if(move |link: &Link<NodeId>, message| {
            link.now < late_start && cadence_message(message).is_some_and(is_not_full)
        });
        let mut swarm = build(
            0,
            network,
            &inputs,
            |node| if node == late { late_start } else { Time(0) },
            |builder| {
                builder.set_resend(RESEND);
            },
        );
        let case = format!("late input fast: {late_fast}");

        assert!(
            swarm.run_until_all_decided(late_start + 3 * VIEW_TIMEOUT),
            "{case}: not everyone decided: {:?}",
            swarm.decisions().keys()
        );
        expect_agreement(&swarm, &nodes);

        for node in nodes.iter().filter(|node| **node != late) {
            let left_view_2 = swarm.sent(node).iter().any(|sent| {
                sent.at < late_start && timeout_view(&sent.message).is_some_and(|v| v >= 2)
            });
            assert!(left_view_2, "{case}: {node:?} had not reached view 3");
            assert!(
                swarm.decision_of(node).expect("decided").at > late_start,
                "{case}: {node:?} decided before the late node entered"
            );
        }

        let late_sent = swarm.sent(&late);
        let at_entry = |pred: fn(&SlotFallbackMessage) -> bool| {
            late_sent
                .iter()
                .any(|s| s.at == late_start && pred(&s.message))
        };
        if late_fast {
            assert!(
                at_entry(is_fast_input),
                "{case}: no fast block sent at entry"
            );
            assert!(
                at_entry(is_dummy_vote),
                "{case}: no DummyVote sent at entry"
            );
        } else {
            assert!(at_entry(is_not_full), "{case}: no NotFull sent at entry");
            assert!(
                !late_sent.iter().any(|s| is_dummy_vote(&s.message)),
                "{case}: a non-fast input cast a DummyVote"
            );
        }
        assert!(
            !late_sent
                .iter()
                .any(|s| timeout_view(&s.message).is_some_and(|v| v < 3)),
            "{case}: the late node ran through the views it should have jumped"
        );

        let received = swarm.received(&late);
        let first_tc = received.iter().position(|r| is_tc(&r.message));
        let first_pre_prepare = received.iter().position(|r| is_pre_prepare(&r.message));
        match (first_tc, first_pre_prepare) {
            (None, _) => panic!("{case}: the late node never received a Tc"),
            (Some(tc), Some(pp)) => assert!(tc < pp, "{case}: a Pre-Prepare came before any Tc"),
            (Some(_), None) => {}
        }
    }
}

/// S8. Three fast nodes decide; the fourth, also fast, enters later and decides
/// at entry from retained votes or the `DummyDecision`
#[test]
fn s8_a_late_fast_validator_decides_at_entry_from_retained_votes_or_the_dummy_decision() {
    let nodes = fixtures::nodes();
    let late = nodes[3];
    let late_start = views(1);
    let inputs = mixed_inputs(&nodes);
    let mut swarm = build(
        0,
        Network::reliable(LATENCY),
        &inputs,
        |node| if node == late { late_start } else { Time(0) },
        nothing,
    );

    assert!(
        swarm.run_until_all_decided(late_start + 2 * LATENCY),
        "not everyone decided: {:?}",
        swarm.decisions().keys()
    );

    assert_eq!(expect_agreement(&swarm, &nodes), fast_entries());
    for (node, decision) in swarm.decisions() {
        assert!(
            is_dummy_proof(&decision.proof),
            "{node:?} did not decide by DummyQC"
        );
        if node != late {
            assert!(decision.at <= deltas(1), "{node:?} decided late");
        }
    }
    let late_at = swarm.decision_of(&late).expect("decided").at;
    assert_eq!(late_at, late_start, "the late node did not decide at entry");
}

/// The fast nodes of a mixed run: one, two or three, by seed
fn fast_nodes_of(seed: u64) -> Vec<NodeId> {
    let count = 1 + (seed % 3) as usize;
    fixtures::nodes()[..count].to_vec()
}

/// S9. Mixed inputs, 20% message loss, 20 seeds, resend loop on: every node
/// decides and agreement holds
#[test]
fn s9_every_node_decides_and_agrees_through_twenty_percent_loss() {
    let nodes = fixtures::nodes();
    for seed in 0..20 {
        let inputs = mixed_inputs(&fast_nodes_of(seed));
        let network = Network::reliable(LATENCY).loss(0.2);
        let mut swarm = build(seed, network, &inputs, all_at_zero, |builder| {
            builder.set_resend(RESEND);
        });

        assert!(
            swarm.run_until_all_decided(views(30)),
            "seed {seed}: only {} of 4 decided",
            swarm.decisions().len()
        );
        expect_agreement(&swarm, &nodes);
    }
}

/// S10. Mixed inputs, latency sampled within Δ, 20 seeds: agreement holds, and
/// once any node holds a `DummyQC` every decision is the fast entries
#[test]
fn s10_agreement_holds_under_randomized_latency() {
    let nodes = fixtures::nodes();
    for seed in 0..20 {
        let inputs = mixed_inputs(&fast_nodes_of(seed));
        let network = Network::with_latency(uniform_duration(LATENCY / 4, LATENCY));
        let mut swarm = build(seed, network, &inputs, all_at_zero, nothing);

        swarm.run_until_or(views(10), SlotFallbackSwarm::all_decided);
        let agreed = expect_agreement(&swarm, &nodes);
        let dummy = swarm
            .decisions()
            .values()
            .any(|decision| is_dummy_proof(&decision.proof));
        if dummy {
            assert_eq!(
                agreed,
                fast_entries(),
                "seed {seed}: a DummyQC beside other entries"
            );
        }
    }
}

/// A `NotFull` signed by `node`, whatever its input
fn not_full_from(node: NodeId) -> SlotFallbackMessage {
    let vote = VoteMsg::new_signed(fixtures::SLOT, NotFull, &node.keypair());
    SlotFallbackMessage::Leaderless(LeaderlessMessage::NotFull(vote))
}

/// S11. A Byzantine node sends both a `NotFull` and a `DummyVote`, beside two
/// honest fast nodes and one honest non-fast node: decisions agree, and no
/// honest node prepares an unlocked non-fast proposal
#[test]
fn s11_a_byzantine_signer_of_both_not_full_and_a_dummy_vote_unlocks_no_non_fast_proposal() {
    let nodes = fixtures::nodes();
    // the honest non-fast node leads view 1
    let non_fast = fixtures::leader_of(1);
    let byzantine = *nodes.iter().find(|n| **n != non_fast).expect("four nodes");
    let honest: Vec<NodeId> = nodes.iter().copied().filter(|n| *n != byzantine).collect();
    let fast: Vec<NodeId> = nodes.iter().copied().filter(|n| *n != non_fast).collect();
    let inputs = mixed_inputs(&fast);

    let mut swarm = build(
        0,
        Network::reliable(LATENCY),
        &inputs,
        all_at_zero,
        |builder| {
            builder.set_outbound(byzantine, move |_, message| {
                if is_dummy_vote(&message) {
                    vec![message, not_full_from(byzantine)]
                } else {
                    vec![message]
                }
            });
        },
    );

    swarm.run_until_or(views(3), |_| false);
    expect_all_decided(&swarm, &honest);
    assert_eq!(expect_agreement(&swarm, &honest), fast_entries());

    assert!(
        swarm
            .sent(&byzantine)
            .iter()
            .any(|s| is_not_full(&s.message)),
        "the Byzantine node never declared NotFull"
    );
    let non_fast_entries = entries(&inputs[&non_fast].0);
    for node in &honest {
        for sent in swarm.sent(node) {
            if let Some((view, prepared)) = prepare(&sent.message) {
                assert_ne!(
                    prepared, non_fast_entries,
                    "{node:?} prepared the non-fast input in view {view}"
                );
            }
        }
    }
}

/// The ways S12's Byzantine leader dresses up a non-fast metablock
#[derive(Clone, Copy, Debug)]
enum Forgery {
    NoNfCert,
    NfCertBelowQuorum,
    NoFbCert,
}

/// An `NfCert` over the declarations of `signers`, however few
fn nf_cert_of(signers: &[NodeId], validator_data: &ValidatorData) -> NfCert {
    let pool = fixtures::vote_pool(fixtures::SLOT, NotFull, signers);
    let tally = pool.tally(validator_data);
    let (_, sigcol) = tally.groups().next().expect("the signers declared");
    StrongQc {
        scope: fixtures::SLOT,
        verdict: NotFull,
        sigcol: sigcol.clone(),
    }
}

/// S12. Three honest fast nodes cast `DummyVote`s whose certificate is delayed,
/// and view 1 fails. A Byzantine view-2 leader proposes a non-fast metablock
/// with an `FBCert` and no `NfCert`, with an `NfCert` below quorum, and with no
/// `FBCert`: no honest node prepares any of them, and a later view decides the
/// fast entries, matching the delayed `DummyQC`
#[test]
fn s12_a_byzantine_view_2_leader_s_unadmitted_non_fast_proposal_gets_no_honest_prepare() {
    let nodes = fixtures::nodes();
    let byzantine = fixtures::leader_of(2);
    let honest: Vec<NodeId> = nodes.iter().copied().filter(|n| *n != byzantine).collect();
    let validator_data = fixtures::validator_data();
    let non_fast = fixtures::mixed_evidence_metablock(2, &validator_data);
    assert_ne!(entries(&non_fast), fast_entries());

    for forgery in [
        Forgery::NoNfCert,
        Forgery::NfCertBelowQuorum,
        Forgery::NoFbCert,
    ] {
        // the Byzantine leader runs honest code; only its view-2 proposal is forged
        let inputs = mixed_inputs(&nodes);
        let (fbcert, cert) = match forgery {
            Forgery::NoNfCert => (Some(fixtures::enter_fallback_cert(&validator_data)), None),
            // a stake check, not whose declarations they are, has to refuse it
            Forgery::NfCertBelowQuorum => (
                Some(fixtures::enter_fallback_cert(&validator_data)),
                Some(nf_cert_of(&[byzantine, honest[0]], &validator_data)),
            ),
            Forgery::NoFbCert => (None, None),
        };
        let forged = CertifiedMetablock {
            block: non_fast.clone(),
            fbcert,
        };

        let release = views(3);
        let network = Network::reliable(LATENCY).drop_if(move |link: &Link<NodeId>, message| {
            let Some(message) = cadence_message(message) else {
                return false;
            };
            let view_1_proposal = proposal(message).is_some_and(|p| p.view == 1);
            view_1_proposal || (link.now < release && is_dummy_vote(message))
        });
        let mut swarm = build(0, network, &inputs, all_at_zero, |builder| {
            builder
                .set_resend(RESEND)
                .set_outbound(byzantine, move |_, message| {
                    vec![forge_pre_prepare(
                        message,
                        forged.clone(),
                        cert.clone(),
                        byzantine,
                    )]
                });
        });
        let case = format!("{forgery:?}");

        swarm.run_until_or(views(6), |swarm| {
            honest.iter().all(|node| swarm.decision_of(node).is_some())
        });
        expect_all_decided(&swarm, &honest);
        let agreed = expect_agreement(&swarm, &honest);
        assert_eq!(agreed, fast_entries(), "{case}");

        let forged_views: Vec<u64> = proposals_of(&swarm, &byzantine)
            .into_iter()
            .filter(|(_, p)| p.value.block == non_fast)
            .map(|(_, p)| p.view)
            .collect();
        assert!(
            !forged_views.is_empty(),
            "{case}: the Byzantine leader never sent its forgery"
        );
        for node in &honest {
            for sent in swarm.sent(node) {
                if let Some((view, prepared)) = prepare(&sent.message) {
                    assert!(
                        !forged_views.contains(&view),
                        "{case}: {node:?} prepared in the forged view {view}"
                    );
                    assert_ne!(prepared, entries(&non_fast), "{case}: {node:?}");
                }
            }
        }

        for node in &honest {
            let decision = swarm.decision_of(node).expect("decided");
            let later_view = mvba_view(&decision.proof).is_some_and(|v| v >= 3);
            assert!(
                later_view || is_dummy_proof(&decision.proof),
                "{case}: {node:?} decided by {:?}",
                decision.proof
            );
            for sent in swarm.sent(node) {
                if let Some(LeaderlessMessage::DummyVote(vote)) = leaderless(&sent.message) {
                    assert_eq!(vote.vote, DummyVote(agreed.clone()), "{case}: {node:?}");
                }
            }
        }
    }
}

/// S13. Four non-fast inputs, every `NotFull` lost until `TC_1` forms, resend
/// loop on: nobody proposes while no `NfCert` can exist; once declarations get
/// through, a later unlocked leader proposes with the `NfCert` and all decide
#[test]
fn s13_a_tc_1_formed_before_the_nf_cert_waits_for_declarations_then_a_later_leader_proposes() {
    let nodes = fixtures::nodes();
    let inputs = mixed_inputs(&[]);
    let tc_seen = Arc::new(AtomicBool::new(false));
    let seen = tc_seen.clone();
    let network = Network::reliable(LATENCY).drop_if(move |_: &Link<NodeId>, message| {
        let Some(message) = cadence_message(message) else {
            return false;
        };
        if is_tc(message) {
            seen.store(true, Ordering::SeqCst);
        }
        is_not_full(message) && !seen.load(Ordering::SeqCst)
    });
    let mut swarm = build(0, network, &inputs, all_at_zero, |builder| {
        builder.set_resend(RESEND);
    });

    assert!(
        swarm.run_until_all_decided(views(6)),
        "not everyone decided: {:?}",
        swarm.decisions().keys()
    );
    assert!(tc_seen.load(Ordering::SeqCst), "TC_1 never formed");
    let agreed = expect_agreement(&swarm, &nodes);

    let first_declaration = nodes
        .iter()
        .flat_map(|node| swarm.received(node))
        .filter(|received| is_not_full(&received.message))
        .map(|received| received.at)
        .min()
        .expect("no declaration ever got through");
    for node in &nodes {
        assert!(
            swarm
                .sent(node)
                .iter()
                .any(|s| timeout_view(&s.message) == Some(1) && s.at < first_declaration),
            "{node:?} did not leave view 1 by timing out"
        );
        for (at, p) in proposals_of(&swarm, node) {
            assert!(
                at >= first_declaration,
                "{node:?} proposed in view {} before any NfCert could exist",
                p.view
            );
        }
    }

    let released = nodes
        .iter()
        .flat_map(|node| {
            proposals_of(&swarm, node)
                .into_iter()
                .map(move |(_, p)| (*node, p))
        })
        .find(|(node, p)| {
            p.view >= 2
                && p.justified
                && p.cert.is_some()
                && *node == fixtures::leader_of(p.view)
                && p.value.block == inputs[node].0
        });
    let (leader, p) = released.expect("no later unlocked leader proposed its input with an NfCert");
    let cert = p.cert.as_ref().expect("just matched");
    assert!(
        cert.verify(&fixtures::validator_data()),
        "{leader:?}'s NfCert does not verify"
    );
    assert_eq!(agreed, entries(&p.value.block));
    for (node, decision) in swarm.decisions() {
        assert!(
            mvba_view(&decision.proof).is_some_and(|v| v >= 2),
            "{node:?} decided by {:?}",
            decision.proof
        );
    }
}

/// S14. The lossy run of S9 twice on one seed: identical decisions and
/// identical traffic
#[test]
fn s14_a_seed_replays_its_run_exactly() {
    let nodes = fixtures::nodes();
    let run = || {
        let inputs = mixed_inputs(&fast_nodes_of(1));
        let network = Network::reliable(LATENCY).loss(0.2);
        let mut swarm = build(7, network, &inputs, all_at_zero, |builder| {
            builder.set_resend(RESEND);
        });
        assert!(swarm.run_until_all_decided(views(30)));
        expect_agreement(&swarm, &nodes);
        let decisions: BTreeMap<_, _> = swarm
            .decisions()
            .into_iter()
            .map(|(node, d)| (node, (d.at, d.block, d.proof)))
            .collect();
        let traffic: BTreeMap<_, Vec<_>> = nodes
            .iter()
            .map(|node| {
                let sent = swarm.sent(node);
                (
                    *node,
                    sent.into_iter()
                        .map(|s| (s.at, s.to, s.message, s.resent))
                        .collect(),
                )
            })
            .collect();
        (decisions, traffic)
    };

    let first = run();
    // something was lost and sent again, or the run would prove little
    assert!(
        first
            .1
            .values()
            .any(|sent| sent.iter().any(|(_, _, _, resent)| *resent))
    );
    assert_eq!(first, run());
}
