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

//! A deterministic Coda-only swarm: virtual time, a seeded network with
//! delay, loss and partitions, retransmission of alive messages, and
//! Byzantine nodes that tamper with their own sends or inject messages.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use rand::{Rng, SeedableRng, rngs::StdRng};

use super::{Coda, CodaOutput};
use crate::{
    message::CodaMessage,
    quorum::Quorum,
    types::{CodaMetablock, CodaValue, NodeId, ProposalMap, Slot},
};

type Mb = CodaMetablock<u8>;
type Msg = CodaMessage<u8>;
/// Rewrites (or drops, with `None`) a faulty node's message to `to`.
type Tamper = Box<dyn FnMut(usize, &Msg) -> Option<Msg>>;

const DELTA: u64 = 10;
const TIMEOUT: u64 = 5 * DELTA;

#[derive(Clone)]
struct Net {
    delay: (u64, u64),
    loss: f64,
    gst: u64,
    // delay bound before GST
    pre_gst_max: u64,
}

impl Net {
    fn fixed(delay: u64) -> Self {
        Self {
            delay: (delay, delay),
            loss: 0.0,
            gst: 0,
            pre_gst_max: delay,
        }
    }

    fn jittery(loss: f64) -> Self {
        Self {
            delay: (1, DELTA),
            loss,
            gst: 0,
            pre_gst_max: DELTA,
        }
    }
}

struct Partition {
    nodes: BTreeSet<usize>,
    until: u64,
}

enum Event {
    Start(usize),
    Deliver { to: usize, from: usize, msg: Msg },
    Timeout { node: usize, view: u64 },
    Retransmit(usize),
}

struct Node {
    coda: Coda<u8>,
    input: Mb,
    tamper: Option<Tamper>,
    silent: bool,
    alive: Vec<Msg>,
    decided: Vec<(u64, u64, Mb)>,
    // (time, view) of every view entry
    entered: Vec<(u64, u64)>,
}

impl Node {
    fn is_correct(&self) -> bool {
        self.tamper.is_none() && !self.silent
    }
}

struct Sim {
    now: u64,
    seq: u64,
    queue: BTreeMap<(u64, u64), Event>,
    rng: StdRng,
    net: Net,
    partitions: Vec<Partition>,
    nodes: Vec<Node>,
    k: usize,
    // invariant checks after every event (memory bound)
    check_memory: bool,
}

fn id(index: usize) -> NodeId {
    NodeId::dummy(index as u64)
}

fn mb(values: &[Option<u8>]) -> Mb {
    ProposalMap::new(values.len(), |j| match values[j] {
        Some(v) => CodaValue::Value(v),
        None => CodaValue::Empty,
    })
}

fn uniform(k: usize, v: u8) -> Mb {
    ProposalMap::new(k, |_| CodaValue::Value(v))
}

impl Sim {
    fn new(slot: u64, inputs: Vec<Mb>, net: Net, seed: u64) -> Self {
        let n = inputs.len();
        let k = inputs[0].size();
        let validators: Arc<[NodeId]> = (0..n).map(id).collect();
        let q = Quorum::new(n);
        let nodes = inputs
            .into_iter()
            .enumerate()
            .map(|(i, input)| Node {
                coda: Coda::new(Slot(slot), id(i), validators.clone(), q, k),
                input,
                tamper: None,
                silent: false,
                alive: Vec::new(),
                decided: Vec::new(),
                entered: Vec::new(),
            })
            .collect();
        let mut sim = Self {
            now: 0,
            seq: 0,
            queue: BTreeMap::new(),
            rng: StdRng::seed_from_u64(seed),
            net,
            partitions: Vec::new(),
            nodes,
            k,
            check_memory: false,
        };
        for node in 0..n {
            sim.schedule(0, Event::Start(node));
        }
        sim
    }

    fn silence(&mut self, node: usize) {
        self.nodes[node].silent = true;
    }

    fn tamper(&mut self, node: usize, tamper: Tamper) {
        self.nodes[node].tamper = Some(tamper);
    }

    fn delay_start(&mut self, node: usize, at: u64) {
        self.queue
            .retain(|_, event| !matches!(event, Event::Start(i) if *i == node));
        self.schedule(at, Event::Start(node));
    }

    fn partition(&mut self, nodes: &[usize], until: u64) {
        self.partitions.push(Partition {
            nodes: nodes.iter().copied().collect(),
            until,
        });
    }

    fn inject(&mut self, at: u64, from: usize, to: usize, msg: Msg) {
        self.schedule(at, Event::Deliver { to, from, msg });
    }

    fn schedule(&mut self, at: u64, event: Event) {
        self.seq += 1;
        self.queue.insert((at, self.seq), event);
    }

    fn cut(&self, from: usize, to: usize) -> bool {
        self.partitions
            .iter()
            .any(|p| self.now < p.until && (p.nodes.contains(&from) != p.nodes.contains(&to)))
    }

    fn transmit(&mut self, from: usize, to: usize, msg: Msg) {
        if from == to {
            // self-delivery is local and immediate
            self.schedule(self.now, Event::Deliver { to, from, msg });
            return;
        }
        if self.cut(from, to) || self.rng.gen_bool(self.net.loss) {
            return;
        }
        let max = if self.now < self.net.gst {
            self.net.pre_gst_max
        } else {
            self.net.delay.1
        };
        let delay = self
            .rng
            .gen_range(self.net.delay.0..=max.max(self.net.delay.0));
        self.schedule(self.now + delay, Event::Deliver { to, from, msg });
    }

    fn send_all(&mut self, from: usize, msg: &Msg, include_self: bool) {
        for to in 0..self.nodes.len() {
            if to == from && !include_self {
                continue;
            }
            let msg = match &mut self.nodes[from].tamper {
                Some(tamper) => match tamper(to, msg) {
                    Some(msg) => msg,
                    None => continue,
                },
                None => msg.clone(),
            };
            self.transmit(from, to, msg);
        }
    }

    fn drain(&mut self, node: usize) {
        while let Some(output) = self.nodes[node].coda.poll() {
            match output {
                CodaOutput::Broadcast(msg) => {
                    if self.nodes[node].silent {
                        continue;
                    }
                    self.nodes[node].alive.push(msg.clone());
                    self.send_all(node, &msg, true);
                }
                CodaOutput::ScheduleTimeout(view) => {
                    self.nodes[node].entered.push((self.now, view));
                    self.schedule(self.now + TIMEOUT, Event::Timeout { node, view });
                }
                CodaOutput::Abandon { below } => {
                    self.nodes[node].alive.retain(|msg| msg.view() >= below);
                }
                CodaOutput::Decide { metablock, view } => {
                    self.nodes[node].decided.push((self.now, view, metablock));
                }
            }
        }
    }

    fn step(&mut self) -> bool {
        let Some(((at, _), event)) = self.queue.pop_first() else {
            return false;
        };
        self.now = at;
        match event {
            Event::Start(node) => {
                let input = self.nodes[node].input.clone();
                self.nodes[node].coda.start(input);
                self.drain(node);
                self.schedule(self.now + DELTA, Event::Retransmit(node));
            }
            Event::Deliver { to, from, msg } => {
                self.nodes[to].coda.handle_message(id(from), msg);
                self.drain(to);
            }
            Event::Timeout { node, view } => {
                self.nodes[node].coda.on_timeout(view);
                self.drain(node);
            }
            Event::Retransmit(node) => {
                for msg in self.nodes[node].alive.clone() {
                    self.send_all(node, &msg, false);
                }
                self.schedule(self.now + DELTA, Event::Retransmit(node));
            }
        }
        if self.check_memory {
            self.assert_memory_bound();
        }
        true
    }

    fn run_until(&mut self, limit: u64) {
        while self
            .queue
            .first_key_value()
            .is_some_and(|((at, _), _)| *at <= limit)
        {
            self.step();
        }
    }

    // runs until every correct node decided; panics past `limit`
    fn run_until_decided(&mut self, limit: u64) -> u64 {
        while !self.correct().all(|i| !self.nodes[i].decided.is_empty()) {
            assert!(
                self.now <= limit,
                "no termination by {limit}: {}",
                self.status()
            );
            assert!(self.step(), "event queue drained");
        }
        self.now
    }

    fn correct(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.nodes.len()).filter(|i| self.nodes[*i].is_correct())
    }

    fn status(&self) -> String {
        self.nodes
            .iter()
            .enumerate()
            .map(|(i, node)| format!("{i}: view {} decided {:?}", node.coda.view(), node.decided))
            .collect::<Vec<_>>()
            .join("; ")
    }

    fn decision(&self, node: usize) -> &Mb {
        &self.nodes[node].decided.first().expect("decided").2
    }

    fn decision_view(&self, node: usize) -> u64 {
        self.nodes[node].decided.first().expect("decided").1
    }

    // agreement, obligation and strong unanimity per proposer, decide once
    fn assert_safety(&self) {
        let correct: Vec<usize> = self.correct().collect();
        let decisions: Vec<&Mb> = correct
            .iter()
            .filter_map(|i| self.nodes[*i].decided.first().map(|d| &d.2))
            .collect();
        for i in &correct {
            assert!(self.nodes[*i].decided.len() <= 1, "node {i} decided twice");
        }
        for pair in decisions.windows(2) {
            assert_eq!(pair[0], pair[1], "agreement: {}", self.status());
        }
        let Some(decided) = decisions.first() else {
            return;
        };
        for j in 0..self.k {
            let inputs: BTreeSet<&CodaValue<u8>> =
                correct.iter().map(|i| &self.nodes[*i].input[j]).collect();
            match &decided[j] {
                CodaValue::Empty => {
                    assert!(inputs.len() > 1, "strong unanimity violated at {j}");
                }
                value => assert!(inputs.contains(value), "obligation violated at {j}"),
            }
        }
    }

    fn assert_memory_bound(&self) {
        for (i, node) in self.nodes.iter().enumerate() {
            assert!(
                node.coda.own_views() <= 2,
                "node {i} keeps own state of > 2 views"
            );
            for sender in 0..self.nodes.len() {
                assert!(
                    node.coda.views_holding(&id(sender)) <= 2,
                    "node {i} holds > 2 views of {sender}"
                );
            }
        }
    }
}

#[test]
fn unanimous_inputs_decide_in_view_one_after_two_hops() {
    for n in [1, 4, 7, 8] {
        let mut sim = Sim::new(0, vec![uniform(3, 1); n], Net::fixed(3), 1);
        let at = sim.run_until_decided(1_000);
        // two hops of 3; a single node hears only itself
        assert_eq!(at, if n == 1 { 0 } else { 6 }, "n = {n}");
        for i in 0..n {
            assert_eq!(sim.decision_view(i), 1);
            assert_eq!(sim.decision(i), &uniform(3, 1));
        }
        sim.assert_safety();
    }
}

// one proposer's inputs split; the others are unanimous
fn split_inputs(n: usize) -> Vec<Mb> {
    (0..n)
        .map(|i| mb(&[Some(1), Some(10 + (i % 2) as u8), Some(3)]))
        .collect()
}

#[test]
fn split_inputs_for_one_proposer() {
    for (n, seed) in [(4, 1), (7, 2), (8, 3)] {
        let mut sim = Sim::new(0, split_inputs(n), Net::jittery(0.0), seed);
        sim.run_until_decided(5_000);
        for i in sim.correct().collect::<Vec<_>>() {
            assert!(
                sim.decision_view(i) >= 2,
                "no view-1 quorum on split inputs"
            );
            let decided = sim.decision(i);
            assert_eq!(decided[0], CodaValue::Value(1));
            assert_eq!(decided[2], CodaValue::Value(3));
            assert!(matches!(
                decided[1],
                CodaValue::Empty | CodaValue::Value(10) | CodaValue::Value(11)
            ));
        }
        sim.assert_safety();
    }
}

// slot 1, n = 4: leader(V) = validators[(1 + V) % 4], so view 2 is led by 3
#[test]
fn silent_leader_of_view_two() {
    let mut sim = Sim::new(1, split_inputs(4), Net::fixed(2), 4);
    sim.silence(3);
    sim.run_until_decided(5_000);
    let entry = sim.nodes[0]
        .entered
        .iter()
        .find(|(_, view)| *view == 3)
        .expect("view 3 entered")
        .0;
    for i in 0..3 {
        assert_eq!(sim.decision_view(i), 3, "first view with a correct leader");
        assert!(sim.nodes[i].decided[0].0 <= entry + TIMEOUT);
    }
    sim.assert_safety();
}

#[test]
fn equivocating_leader_of_view_two() {
    let mut sim = Sim::new(1, split_inputs(4), Net::fixed(2), 5);
    sim.tamper(
        3,
        Box::new(|to, msg| match msg {
            CodaMessage::Prepare { view, x } if to % 2 == 1 => {
                let mut x = x.clone();
                x[0] = CodaValue::Value(77);
                Some(CodaMessage::Prepare { view: *view, x })
            }
            other => Some(other.clone()),
        }),
    );
    sim.run_until_decided(5_000);
    for i in 0..3 {
        assert!(sim.decision_view(i) <= 3, "{}", sim.status());
        assert_ne!(sim.decision(i)[0], CodaValue::Value(77));
    }
    sim.assert_safety();
}

#[test]
fn f_silent_validators_terminate() {
    for (n, seed) in [(4, 6), (7, 7), (8, 8)] {
        let mut sim = Sim::new(seed, split_inputs(n), Net::jittery(0.0), seed);
        for faulty in 0..Quorum::new(n).f {
            sim.silence(n - 1 - faulty);
        }
        sim.run_until_decided(20_000);
        sim.assert_safety();
    }
}

#[test]
fn lossy_network_terminates() {
    for (n, loss, seed) in [(4, 0.1, 9), (4, 0.3, 10), (7, 0.2, 11), (8, 0.3, 12)] {
        let mut sim = Sim::new(seed, split_inputs(n), Net::jittery(loss), seed);
        sim.run_until_decided(50_000);
        sim.run_until(sim.now + 10 * TIMEOUT);
        sim.assert_safety();
    }
}

#[test]
fn partitioned_node_catches_up_after_heal() {
    let mut sim = Sim::new(0, vec![uniform(2, 4); 4], Net::jittery(0.0), 13);
    sim.partition(&[3], 300);
    sim.run_until(299);
    assert!(sim.nodes[3].decided.is_empty());
    assert!(sim.nodes[0..3].iter().all(|node| !node.decided.is_empty()));
    sim.run_until_decided(2_000);
    assert!(sim.nodes[3].coda.view() > 1, "caught up past view 1");
    sim.assert_safety();
}

#[test]
fn catch_up_far_ahead_in_one_step() {
    let mut sim = Sim::new(0, vec![uniform(2, 4); 4], Net::fixed(2), 14);
    sim.partition(&[3], 100_000);
    while sim.nodes[0].coda.view() < 21 {
        assert!(sim.step());
    }
    sim.partitions.clear();
    let ahead = sim.nodes[0].coda.view();
    sim.run_until_decided(sim.now + 10 * TIMEOUT);
    let entered: Vec<u64> = sim.nodes[3].entered.iter().map(|(_, view)| *view).collect();
    assert_eq!(entered[0], 1);
    assert!(
        entered[1] + 1 >= ahead,
        "jumped from 1 to {} (others at {ahead})",
        entered[1]
    );
    sim.assert_safety();
}

#[test]
fn byzantine_echo_flood_respects_obligation() {
    let n = 4;
    let mut sim = Sim::new(0, split_inputs(n), Net::jittery(0.0), 15);
    sim.silence(3);
    let poison = uniform(3, 99);
    for at in (0..2_000).step_by(DELTA as usize) {
        for to in 0..3 {
            for view in 1..12 {
                for j in 0..3 {
                    let v = CodaValue::Value(99);
                    sim.inject(at, 3, to, CodaMessage::Echo { view, j, v });
                }
                sim.inject(
                    at,
                    3,
                    to,
                    CodaMessage::Vote1 {
                        view,
                        x: poison.clone(),
                    },
                );
                let lock = Some(poison.clone());
                sim.inject(
                    at,
                    3,
                    to,
                    CodaMessage::Vote2 {
                        view,
                        lock,
                        guard: poison.clone(),
                    },
                );
                sim.inject(
                    at,
                    3,
                    to,
                    CodaMessage::Prepare {
                        view,
                        x: poison.clone(),
                    },
                );
            }
        }
    }
    sim.run_until_decided(20_000);
    for i in 0..3 {
        assert!((0..3).all(|j| sim.decision(i)[j] != CodaValue::Value(99)));
    }
    sim.assert_safety();
}

#[test]
fn memory_bounded_across_views_and_spam() {
    let mut sim = Sim::new(0, split_inputs(4), Net::jittery(0.1), 16);
    sim.check_memory = true;
    sim.silence(3);
    let spam_views = [1_000, 10_000, 100_000, 1_000_000];
    for (step, view) in spam_views.iter().enumerate() {
        for to in 0..3 {
            let at = 100 * step as u64;
            sim.inject(
                at,
                3,
                to,
                CodaMessage::Echo {
                    view: *view,
                    j: 0,
                    v: CodaValue::Value(5),
                },
            );
            sim.inject(
                at,
                3,
                to,
                CodaMessage::Vote1 {
                    view: *view,
                    x: uniform(3, 5),
                },
            );
        }
    }
    while sim.nodes[0].coda.view() < 50 {
        assert!(sim.step(), "{}", sim.status());
        for node in &sim.nodes[0..3] {
            // at most two views per sender, the spammer's included
            assert!(
                node.coda.received_views() <= 2 * 4,
                "{}",
                node.coda.received_views()
            );
        }
    }
    sim.assert_safety();
}

#[test]
fn distinct_echo_flood_is_capped_per_sender() {
    let (n, k) = (4, 3);
    let mut sim = Sim::new(0, split_inputs(n), Net::jittery(0.0), 18);
    sim.silence(3);
    // node 2 buffers the flood before it starts
    sim.delay_start(2, 500);
    for at in [0, 200, 400] {
        for to in 0..3 {
            for view in 1..=2 {
                for j in 0..k as u16 {
                    for v in 0..=u8::MAX {
                        let v = CodaValue::Value(v);
                        sim.inject(at, 3, to, CodaMessage::Echo { view, j, v });
                    }
                }
            }
        }
    }
    // n + 1 values per (view, j), for the spammer's two views
    let bound = 2 * k * (n + 1);
    while sim.now < 450 {
        assert!(sim.step());
        for node in &sim.nodes[0..3] {
            assert!(node.coda.echoes_held(&id(3)) <= bound);
        }
    }
    assert_eq!(sim.nodes[2].coda.echoes_held(&id(3)), bound);
    sim.run_until_decided(20_000);
    sim.assert_safety();
}

#[test]
fn decide_reports_the_view_of_its_vote2_quorum() {
    let validators: Arc<[NodeId]> = (0..4).map(id).collect();
    let mut coda = Coda::new(Slot(0), id(0), validators, Quorum::new(4), 1);
    let x = uniform(1, 7);
    coda.start(x.clone());
    for sender in 1..4 {
        let v = CodaValue::Value(7);
        coda.handle_message(id(sender), CodaMessage::Echo { view: 1, j: 0, v });
    }
    assert_eq!(coda.view(), 2, "a strong echo certificate moves on");
    for sender in 1..4 {
        let lock = Some(x.clone());
        let guard = x.clone();
        coda.handle_message(
            id(sender),
            CodaMessage::Vote2 {
                view: 1,
                lock,
                guard,
            },
        );
    }
    let decided = std::iter::from_fn(|| coda.poll()).find_map(|output| match output {
        CodaOutput::Decide { metablock, view } => Some((metablock, view)),
        _ => None,
    });
    assert_eq!(decided, Some((x, 1)));
    assert_eq!(coda.view(), 2);
}

#[test]
fn staggered_starts_buffer_early_messages() {
    let mut sim = Sim::new(0, vec![uniform(2, 2); 4], Net::fixed(2), 17);
    sim.delay_start(2, 30);
    sim.delay_start(3, 35);
    sim.run_until_decided(1_000);
    for i in 0..4 {
        assert_eq!(sim.decision_view(i), 1);
    }
    sim.assert_safety();
}

mod properties {
    use proptest::prelude::*;
    // proptest's prelude exports its own rand's Rng
    use rand::Rng;

    use super::*;

    fn inputs(n: usize, k: usize, domain: u8, seed: u64) -> Vec<Mb> {
        let mut rng = StdRng::seed_from_u64(seed);
        let unanimous: Vec<bool> = (0..k).map(|_| rng.gen_bool(0.5)).collect();
        (0..n)
            .map(|_| {
                ProposalMap::new(k, |j| match unanimous[j] {
                    true => CodaValue::Value(j as u8),
                    false => CodaValue::Value(rng.gen_range(0..domain)),
                })
            })
            .collect()
    }

    // an input value, EMPTY, or a value no correct node inputs
    fn random_value(rng: &mut StdRng, domain: u8) -> CodaValue<u8> {
        match rng.gen_range(0..domain + 2) {
            v if v < domain => CodaValue::Value(v),
            v if v == domain => CodaValue::Empty,
            _ => CodaValue::Value(FOREIGN),
        }
    }

    fn random_mb(rng: &mut StdRng, k: usize, domain: u8) -> Mb {
        ProposalMap::new(k, |_| random_value(rng, domain))
    }

    const FOREIGN: u8 = 200;

    // equivocates per receiver: random Prepares and Vote1s, Vote2s with
    // unsupported locks, echoes of random values, and dropped sends
    fn byzantine(k: usize, domain: u8, seed: u64) -> Tamper {
        let mut rng = StdRng::seed_from_u64(seed);
        Box::new(move |_, msg| {
            if rng.gen_bool(0.1) {
                return None;
            }
            let view = msg.view();
            Some(match msg {
                CodaMessage::Prepare { .. } => CodaMessage::Prepare {
                    view,
                    x: random_mb(&mut rng, k, domain),
                },
                CodaMessage::Vote1 { .. } => CodaMessage::Vote1 {
                    view,
                    x: random_mb(&mut rng, k, domain),
                },
                CodaMessage::Vote2 { .. } => CodaMessage::Vote2 {
                    view,
                    lock: rng.gen_bool(0.5).then(|| random_mb(&mut rng, k, domain)),
                    guard: random_mb(&mut rng, k, domain),
                },
                CodaMessage::Echo { j, .. } => CodaMessage::Echo {
                    view,
                    j: *j,
                    v: random_value(&mut rng, domain),
                },
            })
        })
    }

    #[derive(Clone, Copy, Debug)]
    enum Faults {
        None,
        Silent,
        Byzantine,
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        #[test]
        fn agreement_unanimity_obligation_termination(
            n in prop::sample::select(vec![4usize, 7, 8]),
            k in 1usize..=3,
            domain in 1u8..=3,
            loss in prop::sample::select(vec![0.0, 0.15]),
            gst in 0u64..300,
            faults in prop::sample::select(vec![Faults::None, Faults::Silent, Faults::Byzantine]),
            seed in any::<u64>(),
        ) {
            let net = Net { delay: (1, DELTA), loss, gst, pre_gst_max: 4 * DELTA };
            let mut sim = Sim::new(seed % 97, inputs(n, k, domain, seed), net, seed);
            for faulty in 0..Quorum::new(n).f {
                match faults {
                    Faults::None => {}
                    Faults::Silent => sim.silence(faulty),
                    Faults::Byzantine => {
                        sim.tamper(faulty, byzantine(k, domain, seed ^ faulty as u64));
                    }
                }
            }
            sim.check_memory = true;
            sim.run_until_decided(gst + 60_000);
            sim.run_until(sim.now + 3 * TIMEOUT);
            sim.assert_safety();
        }
    }
}
