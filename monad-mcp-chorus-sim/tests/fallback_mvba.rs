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

//! Four validators' MVBA instances on a simulated network, nothing else around
//! them, run from inputs the fast path already formed. What is asserted is what
//! the protocol promises: agreement, integrity, external validity, termination

use std::{cell::Cell, collections::BTreeMap, rc::Rc, time::Duration};

use chorus::{slot::fallback::Metablock, types::NodeId};
use monad_mcp_chorus::stub as chorus;
use monad_mcp_chorus_sim::{Message, MvbaSwarm, MvbaSwarmBuilder};
use monad_sim::{RunOutcome, Time, dist::uniform_duration};
use monad_sim_swarm::Network;

/// The inputs the fast path would have formed
mod fixtures {
    use std::{collections::HashMap, sync::Arc};

    use chorus::{
        env::MerkleHash,
        slot::fallback::{
            CertifiedEntry, EnterFallbackCert, EnterFallbackVote, Entry, FallbackEntry, Metablock,
            Mvba as _, monad_mvba::MvbaContext,
        },
        types::{
            HeaderAuth, IsVote, MerkleRoot, NodeId, ProposalMap, Slot, Stake, StrongQc,
            TimestampDelta, ValidatorData, VoteMsg, VotePool, WeakQc,
        },
    };
    use monad_mcp_chorus::{spec::KeyPair as _, stub as chorus};
    use monad_mcp_chorus_sim::MonadMvba;

    const NUM_NODES: u64 = 4;

    /// A root whose hash spells out `n`, so distinct seeds give distinct roots
    fn root(n: u64) -> MerkleRoot {
        let mut hash = [0u8; 20];
        hash[..8].copy_from_slice(&n.to_le_bytes());
        MerkleRoot(MerkleHash(hash))
    }
    const NUM_PROPOSALS: usize = 2;
    const SLOT: Slot = Slot(0);
    pub const DELTA: TimestampDelta = TimestampDelta::from_millis(super::DELTA_MILLIS);

    pub fn nodes() -> Vec<NodeId> {
        (0..NUM_NODES).map(NodeId::dummy).collect()
    }

    /// A supermajority of the four: three signers
    fn quorum() -> Vec<NodeId> {
        nodes()[..3].to_vec()
    }

    pub fn validator_data() -> Arc<ValidatorData> {
        let valset: HashMap<_, _> = nodes()
            .into_iter()
            .map(|node| (node, Stake::from(1u64)))
            .collect();
        let mapping = nodes()
            .into_iter()
            .map(|node| (node, node.keypair().pubkey()))
            .collect();

        Arc::new(ValidatorData::new(valset, mapping))
    }

    /// The same round-robin the implementation uses, restated so the tests do
    /// not take their expectations from the code under test
    /// TODO: wire up actual leader election
    pub fn leader_of(view: u64) -> NodeId {
        let index = (SLOT.get() + view) % NUM_NODES;
        nodes()[index as usize]
    }

    pub fn mvba(node: NodeId, validator_data: &Arc<ValidatorData>) -> MonadMvba {
        MonadMvba::new(MvbaContext {
            slot: SLOT,
            num_proposals: NUM_PROPOSALS,
            node_id: node,
            key: Arc::new(node.keypair()),
            validator_data: validator_data.clone(),
            header_auth: Arc::new(HeaderAuth::new(|_, _| None)),
            delta: DELTA,
        })
    }

    /// A valid metablock whose entries are determined by `seed`, so two seeds
    /// give two metablocks that cannot be confused for one another
    pub fn fast_metablock(seed: u64, validator_data: &ValidatorData) -> Metablock {
        Metablock::new(ProposalMap::new(NUM_PROPOSALS, |j| {
            let entry = Entry::Positive(root(seed * 100 + j as u64));
            CertifiedEntry::FastQc(strong_qc((SLOT, j), entry, &quorum(), validator_data))
        }))
    }

    /// Valid but *not* a fast metablock: its first entry rests on a fallback
    /// quorum. `entries()` matches [`metablock`] on the same seed, so the two
    /// differ only in their evidence
    pub fn mixed_evidence_metablock(seed: u64, validator_data: &ValidatorData) -> Metablock {
        Metablock::new(ProposalMap::new(NUM_PROPOSALS, |j| {
            let entry = Entry::Positive(root(seed * 100 + j as u64));
            if j == 0 {
                CertifiedEntry::FallbackQc(weak_qc(
                    (SLOT, j),
                    FallbackEntry(entry),
                    &quorum(),
                    validator_data,
                ))
            } else {
                CertifiedEntry::FastQc(strong_qc((SLOT, j), entry, &quorum(), validator_data))
            }
        }))
    }

    /// A genuine fallback certificate for this slot
    pub fn enter_fallback_cert(validator_data: &ValidatorData) -> EnterFallbackCert {
        strong_qc(SLOT, EnterFallbackVote, &quorum(), validator_data)
    }

    fn vote_pool<V: IsVote>(scope: V::Scope, vote: V, signers: &[NodeId]) -> VotePool<V> {
        let mut pool = VotePool::new(scope.clone());
        for node in signers {
            pool.add_vote(
                *node,
                VoteMsg::new_signed(scope.clone(), vote.clone(), &node.keypair()),
            );
        }
        pool
    }

    /// Aggregate a genuine supermajority certificate over `vote`
    fn strong_qc<V: IsVote>(
        scope: V::Scope,
        vote: V,
        signers: &[NodeId],
        validator_data: &ValidatorData,
    ) -> StrongQc<V> {
        vote_pool(scope, vote, signers)
            .try_form_strong_qc(validator_data)
            .expect("the signers hold a supermajority of stake")
    }

    /// Aggregate a genuine `f+1` certificate over `vote`
    fn weak_qc<V: IsVote>(
        scope: V::Scope,
        vote: V,
        signers: &[NodeId],
        validator_data: &ValidatorData,
    ) -> WeakQc<V> {
        vote_pool(scope, vote, signers)
            .try_form_weak_qc(validator_data)
            .expect("the signers hold more than an honest threshold of stake")
            .left()
            .expect("the signers all voted the same way")
    }
}

/// The protocol's Δ: the assumed upper bound on one-way message latency
const DELTA_MILLIS: u64 = 150;
const DELTA: Duration = Duration::from_millis(DELTA_MILLIS);

/// Every deadline below is a multiple of the state machine's own view timeout,
/// so the tests hold whatever Δ and link are chosen, as long as the link keeps
/// the assumption Δ rests on
const LATENCY: Duration = Duration::from_millis(100);
const _: () = assert!(LATENCY.as_millis() <= DELTA.as_millis());

/// The view timeout the state machine derives from Δ: proposal, prepare, commit
const VIEW_TIMEOUT: Duration = Duration::from_millis(3 * DELTA_MILLIS);

/// One leg of the agreement: pre-prepare, prepare, commit
fn rounds(n: u32) -> Time {
    Time(0) + n * LATENCY
}

/// `n` view timeouts in
fn views(n: u32) -> Time {
    Time(0) + n * VIEW_TIMEOUT
}

/// Four validators, each with the fallback input `block_of` gives it
fn swarm(
    seed: u64,
    network: Network<NodeId, Message>,
    block_of: impl Fn(NodeId) -> Metablock,
) -> MvbaSwarm {
    let validator_data = fixtures::validator_data();
    let mut builder = MvbaSwarmBuilder::new();
    builder.set_seed(seed).set_network(network);

    for node in fixtures::nodes() {
        let mvba = fixtures::mvba(node, &validator_data);
        let cert = fixtures::enter_fallback_cert(&validator_data);
        builder.add_node(node, mvba, block_of(node), Some(cert), Time(0));
    }

    builder.build()
}

/// Agreement, integrity and external validity over every node that decided
fn expect_agreement(swarm: &MvbaSwarm) -> Metablock {
    let decisions = swarm.decisions();
    assert!(!decisions.is_empty(), "nobody decided");
    assert!(
        !swarm.any_conflicted(),
        "a node reported a second, different decision"
    );

    let (_, first) = decisions.iter().next().expect("just checked non-empty");
    for (node, decision) in &decisions {
        assert_eq!(
            decision.block, first.block,
            "{node:?} decided a different metablock"
        );
    }

    // external validity: what was agreed is a metablock some validator held.
    assert!(
        swarm.inputs().values().any(|input| *input == first.block),
        "the decided metablock was nobody's input"
    );

    first.block.clone()
}

fn expect_all_decided(swarm: &MvbaSwarm, expected: &[NodeId]) -> BTreeMap<NodeId, Time> {
    let decisions = swarm.decisions();
    for node in expected {
        assert!(decisions.contains_key(node), "{node:?} did not decide");
    }
    assert_eq!(
        decisions.len(),
        expected.len(),
        "unexpected set of deciding nodes"
    );

    decisions
        .into_iter()
        .map(|(node, decision)| (node, decision.at))
        .collect()
}

/// T1. Same input everywhere on a reliable network: the first view's leader
/// proposes and all four decide it three rounds later
#[test]
fn identical_inputs_decide_in_the_first_view() {
    let validator_data = fixtures::validator_data();
    let block = fixtures::fast_metablock(1, &validator_data);

    let mut swarm = swarm(0, Network::reliable(LATENCY), |_| block.clone());
    assert!(
        swarm.run_until_all_decided(views(1)),
        "not everyone decided: {:?}",
        swarm.decisions().keys()
    );

    let decided = expect_agreement(&swarm);
    assert_eq!(decided, block);

    // pre-prepare, prepare, commit -- one link crossing each, the leader
    // included, since it hears its own broadcast off the wire like anyone else.
    for (node, at) in expect_all_decided(&swarm, &fixtures::nodes()) {
        assert_eq!(at, rounds(3), "{node:?} decided late");
    }
}

/// T2. Every validator holds a different, equally valid metablock, as partial
/// dissemination leaves them. Nothing is locked, so the leader's input wins
#[test]
fn divergent_inputs_agree_on_the_leader_s_metablock() {
    let validator_data = fixtures::validator_data();
    let inputs: BTreeMap<NodeId, Metablock> = fixtures::nodes()
        .into_iter()
        .enumerate()
        .map(|(i, node)| {
            (
                node,
                fixtures::mixed_evidence_metablock(i as u64 + 1, &validator_data),
            )
        })
        .collect();

    // the inputs really do differ, or the test would prove nothing
    let distinct: Vec<&Metablock> = inputs.values().collect();
    for (i, block) in distinct.iter().enumerate() {
        for other in &distinct[i + 1..] {
            assert_ne!(block, other, "two validators were given the same input");
        }
    }

    let mut swarm = swarm(0, Network::reliable(LATENCY), |node| inputs[&node].clone());
    assert!(
        swarm.run_until_all_decided(views(1)),
        "not everyone decided: {:?}",
        swarm.decisions().keys()
    );

    let decided = expect_agreement(&swarm);
    let leader = fixtures::leader_of(1);
    assert_eq!(decided, inputs[&leader], "the leader's input did not win");

    for (node, at) in expect_all_decided(&swarm, &fixtures::nodes()) {
        assert_eq!(at, rounds(3), "{node:?} decided late");
    }
}

/// T3. The first view's leader never proposes. The rest time out and the second
/// view's leader, locked by nothing, proposes its own input
#[test]
fn a_silent_first_leader_is_replaced_by_the_next_view() {
    let validator_data = fixtures::validator_data();
    let inputs: BTreeMap<NodeId, Metablock> = fixtures::nodes()
        .into_iter()
        .enumerate()
        .map(|(i, node)| {
            (
                node,
                fixtures::mixed_evidence_metablock(i as u64 + 1, &validator_data),
            )
        })
        .collect();

    let mut swarm = swarm(0, Network::reliable(LATENCY), |node| inputs[&node].clone());

    // removed before the simulation runs, so it never even proposes: steps
    // already scheduled for it are skipped, and messages to it are dropped.
    let silent = fixtures::leader_of(1);
    swarm.swarm_mut().remove_node(&silent);

    let live: Vec<NodeId> = fixtures::nodes()
        .into_iter()
        .filter(|node| *node != silent)
        .collect();

    // after the view timeout the timeouts have to cross the network before the
    // second view's three rounds can run.
    let deadline = views(1) + 4 * LATENCY;
    assert!(
        swarm.run_until_all_decided(deadline),
        "the survivors did not decide by {deadline:?}: {:?}",
        swarm.decisions().keys()
    );

    let decided = expect_agreement(&swarm);
    let second_leader = fixtures::leader_of(2);
    assert_eq!(
        decided, inputs[&second_leader],
        "the second view's leader proposed something other than its own input"
    );

    let times = expect_all_decided(&swarm, &live);
    for (node, at) in times {
        assert!(at > views(1), "{node:?} decided before timing out");
        assert!(at <= deadline, "{node:?} decided late: {at:?}");
    }
}

/// T4. One message in eight is lost. No vote is ever retransmitted, so liveness
/// rests on the view timeout; agreement must hold whatever the losses
#[test]
fn agreement_survives_a_lossy_network() {
    let validator_data = fixtures::validator_data();
    let block = fixtures::fast_metablock(1, &validator_data);

    for seed in 0..8 {
        let network = Network::reliable(LATENCY).loss(0.125);
        let mut swarm = swarm(seed, network, |_| block.clone());

        // room for a good many views
        let decided = swarm.run_until_all_decided(views(100));
        let block_decided = expect_agreement(&swarm);
        assert_eq!(block_decided, block, "seed {seed} decided a foreign block");
        assert!(
            decided,
            "seed {seed}: only {} of 4 decided in 100 views",
            swarm.decisions().len()
        );
    }
}

/// T5. A validator cut off for the first view's value-carrying rounds learns
/// the verdict from the deciders' one-shot commit certificate -- broadcast
/// after the partition lifts -- and the block from a peer
#[test]
fn a_partitioned_validator_catches_up_through_block_sync() {
    let validator_data = fixtures::validator_data();
    // divergent, so the decided block is one the laggard never held: proposing
    // remembers a validator's own input, leaving identical inputs nothing to fetch
    let inputs: BTreeMap<NodeId, Metablock> = fixtures::nodes()
        .into_iter()
        .enumerate()
        .map(|(i, node)| {
            (
                node,
                fixtures::mixed_evidence_metablock(i as u64 + 1, &validator_data),
            )
        })
        .collect();

    let nodes = fixtures::nodes();
    let leader = fixtures::leader_of(1);
    let laggard = *nodes
        .iter()
        .find(|node| **node != leader)
        .expect("four validators, one leader");

    // the pre-prepare and prepare votes are sent inside the window and lost; the
    // commit certificates go out after it and are kept: block sync, not slow
    // recovery
    let window = Time(0)..(Time(0) + 5 * LATENCY / 2);
    let network = Network::reliable(LATENCY).partition(window, [[laggard]]);
    let mut swarm = swarm(0, network, |node| inputs[&node].clone());

    // a block response is the only unicast, so counting those steps separates
    // catching up through block sync from merely hearing the round late
    let responses = Rc::new(Cell::new(0usize));
    let counter = responses.clone();
    swarm.swarm_mut().sim().on_step(move |step| {
        if step.label.source == Some("unicast") {
            counter.set(counter.get() + 1);
        }
    });

    let majority: Vec<NodeId> = nodes
        .iter()
        .copied()
        .filter(|node| *node != laggard)
        .collect();

    // the majority decides on its own first
    swarm.run_until_or(views(1), |swarm| {
        majority
            .iter()
            .all(|node| swarm.decision_of(node).is_some())
    });
    let majority_at = expect_all_decided(&swarm, &majority);
    assert!(
        swarm.decision_of(&laggard).is_none(),
        "the partitioned validator decided without ever seeing the proposal"
    );

    // then the laggard, off the broadcast certificate plus a fetched block
    assert!(
        swarm.run_until_all_decided(views(3)),
        "the partitioned validator never caught up"
    );

    let decided = expect_agreement(&swarm);
    assert_eq!(decided, inputs[&leader]);
    assert!(
        responses.get() > 0,
        "no block was ever fetched, so block sync was not what caught the laggard up"
    );

    let laggard_at = swarm
        .decision_of(&laggard)
        .expect("just asserted it decided")
        .at;
    for (node, at) in majority_at {
        assert!(
            laggard_at > at,
            "the laggard decided no later than {node:?}, so it cannot have caught up"
        );
    }
}

/// T6. T1 and T2 again with the latency of every message sampled inside the Δ
/// the protocol assumes: arrival order is no longer the send order. Three
/// crossings still bound the first view, so the decision, its view and its
/// timing all have to survive the jitter
#[test]
fn agreement_holds_under_randomized_latency() {
    let validator_data = fixtures::validator_data();
    let identical = fixtures::fast_metablock(1, &validator_data);
    let divergent: BTreeMap<NodeId, Metablock> = fixtures::nodes()
        .into_iter()
        .enumerate()
        .map(|(i, node)| {
            (
                node,
                fixtures::mixed_evidence_metablock(i as u64 + 1, &validator_data),
            )
        })
        .collect();
    let leader = fixtures::leader_of(1);

    for seed in 0..16 {
        for divergent_inputs in [false, true] {
            let validator_data = fixtures::validator_data();
            let mut builder = MvbaSwarmBuilder::new();
            // no sample exceeds Δ, so the three rounds fit the view timeout
            // however they land
            let latency = uniform_duration(LATENCY / 4, LATENCY);
            builder
                .set_seed(seed)
                .set_network(Network::with_latency(latency));

            for node in fixtures::nodes() {
                let mvba = fixtures::mvba(node, &validator_data);
                let cert = fixtures::enter_fallback_cert(&validator_data);
                let block = if divergent_inputs {
                    divergent[&node].clone()
                } else {
                    identical.clone()
                };
                builder.add_node(node, mvba, block, Some(cert), Time(0));
            }

            let mut swarm = builder.build();
            let case = format!("seed {seed} (divergent: {divergent_inputs})");
            assert!(
                swarm.run_until_all_decided(views(1)),
                "{case}: only {} of 4 decided",
                swarm.decisions().len()
            );

            let decided = expect_agreement(&swarm);
            let expected = if divergent_inputs {
                &divergent[&leader]
            } else {
                &identical
            };
            assert_eq!(decided, *expected, "{case} agreed on the wrong metablock");

            // no view change happened: every crossing costs at most one LATENCY,
            // so three of them are the worst the first view can take
            for (node, at) in expect_all_decided(&swarm, &fixtures::nodes()) {
                assert!(at <= rounds(3), "{case}: {node:?} decided late: {at:?}");
            }
        }
    }
}

/// Two runs of one seed decide at the same instants. Worth pinning because the
/// state machine holds state in hash maps, whose order must not reach the wire
#[test]
fn a_seed_reproduces_its_run() {
    let validator_data = fixtures::validator_data();
    let block = fixtures::fast_metablock(1, &validator_data);

    let decisions_of = || {
        let network = Network::reliable(LATENCY).loss(0.125);
        let mut swarm = swarm(4, network, |_| block.clone());
        assert!(swarm.run_until_all_decided(views(100)));
        swarm
            .decisions()
            .into_iter()
            .map(|(node, decision)| (node, decision.at))
            .collect::<BTreeMap<_, _>>()
    };

    let first = decisions_of();
    // several views, or the run would be too short to tell anything apart
    assert!(first.values().all(|at| *at > rounds(3)));
    assert_eq!(first, decisions_of());
}

/// T7. Three validators hold a fallback input, the fourth none: it hears every
/// message, the commit certificate included, and still never decides, since
/// participation is gated on the input. Three of four is exactly a supermajority
#[test]
fn a_validator_without_an_input_listens_without_deciding() {
    let validator_data = fixtures::validator_data();
    let block = fixtures::fast_metablock(1, &validator_data);

    let nodes = fixtures::nodes();
    let leader = fixtures::leader_of(1);
    // the leader must be one of the three that can propose, or the first view
    // would time out and the test would be about view changes instead.
    let listener = *nodes
        .iter()
        .rev()
        .find(|node| **node != leader)
        .expect("four validators, one leader");
    let proposers: Vec<NodeId> = nodes
        .iter()
        .copied()
        .filter(|node| *node != listener)
        .collect();

    let mut builder = MvbaSwarmBuilder::new();
    builder.set_seed(0).set_network(Network::reliable(LATENCY));
    for node in &nodes {
        let mvba = fixtures::mvba(*node, &validator_data);
        if *node == listener {
            builder.add_listener(*node, mvba);
        } else {
            let cert = fixtures::enter_fallback_cert(&validator_data);
            builder.add_node(*node, mvba, block.clone(), Some(cert), Time(0));
        }
    }
    let mut swarm = builder.build();

    // long past the deciders' commit certificate, one view timeout after they
    // decide: whatever the listener learns, it stays out.
    let deadline = views(3);
    let outcome = swarm.run_until_or(deadline, |_| false);
    assert_eq!(
        outcome,
        RunOutcome::Drained,
        "the run did not reach {deadline:?}"
    );

    let decided = expect_agreement(&swarm);
    assert_eq!(decided, block);

    // exactly the three proposers decided, and in the first view: three of four
    // validators are a supermajority, so the missing input costs nothing here.
    for (node, at) in expect_all_decided(&swarm, &proposers) {
        assert_eq!(at, rounds(3), "{node:?} decided late");
    }
    assert!(
        swarm.decision_of(&listener).is_none(),
        "a validator with no fallback input reported a decision"
    );
}

/// T8. The four inputs are handed over at four different times, the view-1
/// leader's last: nothing can happen before the leader starts, and the three
/// early starters have to sit on their inputs without timing their first view out
#[test]
fn staggered_starts_wait_on_the_leader_s_own_start() {
    // timed off Δ rather than LATENCY, so the stagger and the three crossings
    // both fit inside the first view whatever the link costs
    const LINK: Duration = Duration::from_millis(DELTA_MILLIS / 4);

    let validator_data = fixtures::validator_data();
    let block = fixtures::fast_metablock(1, &validator_data);

    let leader = fixtures::leader_of(1);
    let base = views(1);
    let leader_start = base + 3 * LINK;
    // the followers go first, one link apart; the leader last
    let mut starts: BTreeMap<NodeId, Time> = fixtures::nodes()
        .into_iter()
        .filter(|node| *node != leader)
        .enumerate()
        .map(|(i, node)| (node, base + i as u32 * LINK))
        .collect();
    starts.insert(leader, leader_start);

    let mut builder = MvbaSwarmBuilder::new();
    builder.set_network(Network::reliable(LINK));
    for node in fixtures::nodes() {
        let mvba = fixtures::mvba(node, &validator_data);
        let cert = fixtures::enter_fallback_cert(&validator_data);
        builder.add_node(node, mvba, block.clone(), Some(cert), starts[&node]);
    }

    let mut swarm = builder.build();
    let deadline = leader_start + 4 * LINK;
    assert!(
        swarm.run_until_all_decided(deadline),
        "not everyone decided by {deadline:?}: {:?}",
        swarm.decisions().keys()
    );
    assert_eq!(expect_agreement(&swarm), block);

    // the leader's start, not anyone else's, is what the run waited on
    for (node, at) in expect_all_decided(&swarm, &fixtures::nodes()) {
        assert_eq!(
            at,
            leader_start + 3 * LINK,
            "{node:?} did not decide three crossings after the leader started"
        );
    }
}
