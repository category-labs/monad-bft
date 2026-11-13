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

//! Parallel swarms: the lookahead comes from the network's lower bounds, and a
//! gossip protocol over a lossy, rate-limited network gives the same results
//! for every thread count and grouping.

use std::time::Duration;

use monad_sim::{
    time::{micros, millis},
    Ctx, Grouping, Handle, StepLabel, Time,
};
use monad_sim_swarm::{LatencyMatrix, Net, Network, Parallel, SimClient, Swarm};
use rand::distributions::Uniform;

type Addr = u32;
/// A gossip rumor: its origin and a hop count.
type Msg = (Addr, u32);

/// Forwards each rumor it hears for the first time to every node, and records
/// what it heard when.
struct Gossip {
    net: Option<Handle<Net<Addr, Msg>>>,
    heard: Vec<(Time, Addr, Addr, u32)>,
    seen: std::collections::BTreeSet<Addr>,
}

impl Gossip {
    fn new() -> Self {
        Gossip {
            net: None,
            heard: Vec::new(),
            seen: Default::default(),
        }
    }

    fn spread(&mut self, ctx: &mut Ctx, rumor: Msg) {
        if !self.seen.insert(rumor.0) {
            return;
        }
        let net = self.net.expect("wired");
        ctx.schedule(
            net,
            ctx.now(),
            StepLabel::source("gossip"),
            move |net, ctx| net.broadcast(ctx, (rumor.0, rumor.1 + 1)),
        );
    }
}

impl SimClient for Gossip {
    type Addr = Addr;
    type Message = Msg;

    fn wire(&mut self, _me: Handle<Self>, net: Handle<Net<Addr, Msg>>) {
        self.net = Some(net);
    }

    fn receive(&mut self, from: Addr, rumor: Msg, ctx: &mut Ctx) {
        self.heard.push((ctx.now(), from, rumor.0, rumor.1));
        self.spread(ctx, rumor);
    }
}

const NODES: Addr = 24;

/// Per node, every rumor it heard: when, from whom, whose, after how many hops.
type Heard = Vec<Vec<(Time, Addr, Addr, u32)>>;

/// Four regions, 2 ms apart within, 20–35 ms across; nodes round-robin.
fn matrix() -> LatencyMatrix<Addr> {
    let one_way: Vec<Vec<Duration>> = (0..4)
        .map(|a: u64| {
            (0..4)
                .map(|b: u64| {
                    if a == b {
                        millis(2)
                    } else {
                        millis(15 + 5 * (a + b))
                    }
                })
                .collect()
        })
        .collect();
    (0..NODES).fold(LatencyMatrix::new(one_way), |m, addr| {
        m.place(addr, addr as usize % 4)
    })
}

fn network() -> Network<Addr, Msg> {
    Network::with_matrix(matrix(), Uniform::new(Duration::ZERO, millis(3)))
        .loss(0.05)
        .egress(10_000_000, |_| 1_250)
}

fn kick(swarm: &mut Swarm<Gossip>, origins: &[Addr]) {
    for &origin in origins {
        let handle = swarm.handle(&origin).expect("a node");
        let at = Time(0) + millis(u64::from(origin));
        swarm
            .sim()
            .schedule(handle, at, StepLabel::source("start"), move |g, ctx| {
                g.spread(ctx, (origin, 0))
            });
    }
}

fn heard(swarm: &Swarm<Gossip>) -> Heard {
    swarm
        .node_ids()
        .iter()
        .map(|addr| swarm.with_node(addr, |g| g.heard.clone()))
        .collect()
}

fn run(parallel: Parallel<Addr>) -> (Heard, u64) {
    run_on(network(), parallel)
}

fn run_on(network: Network<Addr, Msg>, parallel: Parallel<Addr>) -> (Heard, u64) {
    let nodes = (0..NODES).map(|addr| (addr, Gossip::new()));
    let mut swarm = Swarm::build_parallel(3, network, nodes, parallel);
    kick(&mut swarm, &[0, 7, 13]);
    swarm.run_until_time(Time(0) + millis(500));
    (heard(&swarm), swarm.simulation().step_count())
}

#[test]
fn gossip_does_not_depend_on_threads_or_grouping() {
    let reference = run(Parallel::default().threads(1).grouping(Grouping::Single));
    assert!(reference.1 > 1_000, "real work: {} steps", reference.1);
    for parallel in [
        Parallel::default().threads(1),
        Parallel::default().threads(4),
        Parallel::default()
            .threads(2)
            .grouping(Grouping::Auto { groups: 3 }),
        Parallel::default().threads(4).domains(|addr| addr % 4),
    ] {
        assert_eq!(run(parallel), reference);
    }
}

/// Priority classes hold messages back and wake the stack: still the same
/// run on every thread count and grouping. A rumor's first hop is urgent.
#[test]
fn priority_classes_do_not_depend_on_threads_or_grouping() {
    let prioritized = || network().priority(|rumor: &Msg| u8::from(rumor.1 > 0), 1);
    let single = || Parallel::default().threads(1).grouping(Grouping::Single);
    let reference = run_on(prioritized(), single());
    assert_ne!(reference.0, run(single()).0, "priority changes the run");
    for parallel in [
        Parallel::default().threads(4),
        Parallel::default().threads(4).domains(|addr| addr % 4),
    ] {
        assert_eq!(run_on(prioritized(), parallel), reference);
    }
}

#[test]
fn region_domains_get_the_cross_region_lookahead() {
    let nodes = (0..NODES).map(|addr| (addr, Gossip::new()));
    let mut swarm = Swarm::build_parallel(
        0,
        network(),
        nodes,
        Parallel::default()
            .domains(|addr| addr % 4)
            .grouping(Grouping::Auto { groups: 4 }),
    );
    kick(&mut swarm, &[0, 7, 13]);
    swarm.run_until_time(Time(0) + millis(50));
    assert_eq!(swarm.simulation().groups(), 4);
    // the closest regions, 0 and 1, are 20 ms apart
    assert_eq!(swarm.simulation().window(), millis(20));
}

#[test]
fn run_until_stops_at_a_window() {
    let run = |threads| {
        let nodes = (0..NODES).map(|addr| (addr, Gossip::new()));
        let mut swarm =
            Swarm::build_parallel(3, network(), nodes, Parallel::default().threads(threads));
        kick(&mut swarm, &[0, 7, 13]);
        swarm.run_until(|swarm| swarm.with_node(&5, |g| !g.heard.is_empty()));
        (swarm.simulation().now(), heard(&swarm))
    };
    let one = run(1);
    assert!(!one.1[5].is_empty());
    assert_eq!(run(4), one);
}

#[test]
#[should_panic(expected = "minimum delay between nodes of different domains")]
fn a_network_without_a_bound_cannot_run_in_parallel() {
    let nodes = (0..4).map(|addr| (addr, Gossip::new()));
    let network = Network::with_latency(Uniform::new(millis(1), millis(5)));
    Swarm::build_parallel(0, network, nodes, Parallel::default());
}

#[test]
#[should_panic(expected = "below the declared minimum")]
fn a_declared_bound_is_checked() {
    let nodes = (0..4).map(|addr| (addr, Gossip::new()));
    let network =
        Network::with_latency(Uniform::new(micros(500), millis(5))).min_latency(millis(1));
    let mut swarm = Swarm::build_parallel(0, network, nodes, Parallel::default().threads(1));
    kick(&mut swarm, &[0]);
    swarm.run_until_time(Time(0) + millis(100));
}

// a per-link bound: the link's own lower bound, here a matrix entry under a
// delay that only adds to it
#[test]
fn a_per_link_bound_is_the_lookahead_of_its_link() {
    use monad_sim_swarm::Wiring;

    let bound = |from: &Addr, to: &Addr| millis(1 + u64::from(from.abs_diff(*to)));
    let network =
        Network::with_link_latency(move |link, _| bound(&link.from, &link.to) + millis(2))
            .link_min_latency(bound);
    let wiring: Wiring<Addr, Msg> = Wiring::new(0, network);
    assert_eq!(wiring.min_delay(&0, &1), millis(2));
    assert_eq!(wiring.min_delay(&0, &3), millis(4));

    let nodes = (0..4).map(|addr| (addr, Gossip::new()));
    let network =
        Network::with_link_latency(move |link, _| bound(&link.from, &link.to) + millis(2))
            .link_min_latency(bound);
    let mut swarm = Swarm::build_parallel(0, network, nodes, Parallel::default().threads(2));
    kick(&mut swarm, &[0]);
    swarm.run_until_time(Time(0) + millis(100));
}

#[test]
#[should_panic(expected = "below the declared minimum")]
fn a_declared_per_link_bound_is_checked() {
    let nodes = (0..4).map(|addr| (addr, Gossip::new()));
    let network =
        Network::with_link_latency(|_, _| millis(2)).link_min_latency(|from: &Addr, to: &Addr| {
            if from.abs_diff(*to) == 3 {
                millis(3)
            } else {
                millis(1)
            }
        });
    let mut swarm = Swarm::build_parallel(0, network, nodes, Parallel::default().threads(1));
    kick(&mut swarm, &[0]);
    swarm.run_until_time(Time(0) + millis(100));
}

#[test]
#[should_panic(expected = "below the swarm's lookahead")]
fn a_new_network_cannot_lower_the_bound() {
    let nodes = (0..4).map(|addr| (addr, Gossip::new()));
    let mut swarm =
        Swarm::build_parallel(0, Network::reliable(millis(5)), nodes, Parallel::default());
    swarm.set_network(Network::reliable(millis(2)));
}

#[test]
fn nodes_can_restart_in_a_parallel_swarm() {
    let nodes = (0..4).map(|addr| (addr, Gossip::new()));
    let mut swarm = Swarm::build_parallel(
        0,
        Network::reliable(millis(5)),
        nodes,
        Parallel::default().threads(2),
    );
    let removed = swarm.remove_node(&2).expect("a node");
    assert!(removed.heard.is_empty());
    kick(&mut swarm, &[0]);
    swarm.run_until_time(Time(0) + millis(30));
    swarm.add_node(2, Gossip::new());
    let three = swarm.handle(&3).expect("a node");
    swarm
        .sim()
        .schedule(three, Time(0) + millis(40), "late", |g, ctx| {
            g.spread(ctx, (3, 0))
        });
    swarm.run_until_time(Time(0) + millis(100));
    // the restarted node heard only the late rumor, first from its origin
    let heard = swarm.with_node(&2, |g| g.heard.clone());
    assert_eq!(heard[0], (Time(0) + millis(45), 3, 3, 1));
    assert!(heard.iter().all(|(_, _, origin, _)| *origin == 3));
    // everyone else heard both
    assert!(swarm.with_node(&1, |g| g.seen.len()) == 2);
}
