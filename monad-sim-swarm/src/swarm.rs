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

//! A minimal swarm builder over [`monad_sim`]: spawn a set of nodes, each with
//! its own [`Net`] stack, wire them together, and forward run-control to the
//! engine. The builder is generic over the node process type `N` and knows
//! nothing about a node's internals beyond the [`SimClient`] seam.
//!
//! [`Swarm::build`] runs sequentially. [`Swarm::build_parallel`] places the
//! nodes in domains and derives the lookahead between the domains from the
//! network's lower bound on link delays.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use monad_sim::{Ctx, Domain, Grouping, Handle, RunOutcome, Simulation, StepLabel, Time};

use crate::net::{Deliver, Net, Network, Wiring};

/// A node that can be plugged into a simulated network.
///
/// The framework owns the node's state as a `monad_sim` process. A message
/// addressed to the node arrives as a scheduled [`receive`](SimClient::receive)
/// step. The node learns its own handle and its network stack's handle once,
/// via [`wire`](SimClient::wire), so it can schedule self-timers and send
/// messages; a node that does neither can leave `wire` as the default no-op.
pub trait SimClient: Sized + Send + 'static {
    /// Address type used to route to this node on the network.
    type Addr: Ord + Copy + Send + Sync + 'static;
    /// The wire message type. All nodes on one network share it.
    type Message: Clone + Send + 'static;

    /// Called once, just after the node is spawned, with its own process handle
    /// and its network stack's handle. Default: ignore both.
    fn wire(&mut self, me: Handle<Self>, net: Handle<Net<Self::Addr, Self::Message>>) {
        let _ = (me, net);
    }

    /// Handle a message delivered by the network from peer `from`.
    fn receive(&mut self, from: Self::Addr, msg: Self::Message, ctx: &mut Ctx);
}

/// Build the type-erased delivery thunk for a concrete node handle: it schedules
/// `N::receive` against `handle`, capturing the concrete type so the network can
/// forget it. See [`Deliver`].
pub fn deliver_to<N: SimClient>(handle: Handle<N>) -> Deliver<N::Addr, N::Message> {
    Arc::new(
        move |ctx: &mut Ctx, at: Time, from: N::Addr, msg: N::Message| {
            ctx.schedule(
                handle,
                at,
                StepLabel::source("deliver"),
                move |node: &mut N, ctx| node.receive(from, msg, ctx),
            );
        },
    )
}

type DomainFn<Addr> = Box<dyn Fn(&Addr) -> u32>;

/// How a parallel swarm runs; see [`Swarm::build_parallel`].
pub struct Parallel<Addr> {
    threads: Option<usize>,
    grouping: Grouping,
    domain_of: Option<DomainFn<Addr>>,
}

impl<Addr> Default for Parallel<Addr> {
    fn default() -> Self {
        Parallel {
            threads: None,
            grouping: Grouping::default(),
            domain_of: None,
        }
    }
}

impl<Addr> Parallel<Addr> {
    /// Run on a dedicated pool of `threads` threads (default: rayon's global
    /// pool). Results do not depend on it.
    pub fn threads(mut self, threads: usize) -> Self {
        self.threads = Some(threads);
        self
    }

    /// How domains are grouped for execution (default [`Grouping::default`]).
    pub fn grouping(mut self, grouping: Grouping) -> Self {
        self.grouping = grouping;
        self
    }

    /// Place nodes in domains, numbered densely from 0 (default: one domain per
    /// node). Nodes in one domain may exchange messages with any delay; the
    /// network's bound between nodes of different domains must be positive.
    pub fn domains(mut self, domain_of: impl Fn(&Addr) -> u32 + 'static) -> Self {
        self.domain_of = Some(Box::new(domain_of));
        self
    }
}

/// A parallel swarm's placement, fixed when it is built.
struct Domains<Addr> {
    of: BTreeMap<Addr, Domain>,
    /// The lookahead between domains, rows are senders.
    lookahead: Vec<Vec<Duration>>,
}

/// A node and its network stack.
struct Member<N: SimClient> {
    node: Handle<N>,
    net: Handle<Net<N::Addr, N::Message>>,
}

/// A running swarm: the [`Simulation`], the network's [`Wiring`], and the
/// handles of the nodes and their stacks, keyed by address. Homogeneous in the
/// node type `N` for now; the network's delivery boundary is already
/// type-erased, so a heterogeneous variant can reuse it unchanged.
pub struct Swarm<N: SimClient> {
    sim: Simulation,
    wiring: Wiring<N::Addr, N::Message>,
    nodes: BTreeMap<N::Addr, Member<N>>,
    /// `None` for a sequential swarm (every process in domain 0).
    domains: Option<Domains<N::Addr>>,
}

impl<N: SimClient> Swarm<N> {
    /// A sequential swarm: spawn the supplied nodes and a stack for each, wire
    /// each node's handles and register its delivery thunk with the network.
    pub fn build(
        seed: u64,
        network: Network<N::Addr, N::Message>,
        nodes: impl IntoIterator<Item = (N::Addr, N)>,
    ) -> Self {
        let mut swarm = Swarm {
            sim: Simulation::new(seed),
            wiring: Wiring::new(seed, network),
            nodes: BTreeMap::new(),
            domains: None,
        };
        swarm.join(nodes);
        swarm
    }

    /// A parallel swarm: like [`build`](Self::build), with the nodes placed in
    /// domains (see [`Parallel::domains`]). The lookahead between two domains
    /// is the network's smallest lower bound between their nodes
    /// ([`crate::NetworkModel::min_delay`]); it must be positive, so a network
    /// with sampled latency needs a declared [`Network::min_latency`]. Results
    /// are the same for every thread count and grouping.
    ///
    /// The placement is fixed: [`add_node`](Self::add_node) only re-adds
    /// addresses the swarm was built with.
    pub fn build_parallel(
        seed: u64,
        network: Network<N::Addr, N::Message>,
        nodes: impl IntoIterator<Item = (N::Addr, N)>,
        parallel: Parallel<N::Addr>,
    ) -> Self {
        let nodes: Vec<(N::Addr, N)> = nodes.into_iter().collect();
        let wiring = Wiring::new(seed, network);
        let of: BTreeMap<N::Addr, Domain> = match &parallel.domain_of {
            Some(domain_of) => nodes
                .iter()
                .map(|(addr, _)| (*addr, Domain(domain_of(addr))))
                .collect(),
            None => {
                let mut addrs: Vec<N::Addr> = nodes.iter().map(|(addr, _)| *addr).collect();
                addrs.sort();
                addrs
                    .into_iter()
                    .enumerate()
                    .map(|(index, addr)| (addr, Domain(index as u32)))
                    .collect()
            }
        };
        let count = of.values().map(|d| d.0 as usize + 1).max().unwrap_or(1);
        for domain in 0..count {
            assert!(
                of.values().any(|d| d.0 as usize == domain),
                "domain {domain} has no node: number domains densely from 0"
            );
        }
        let mut lookahead = vec![vec![Duration::from_nanos(u64::MAX); count]; count];
        for (from, a) in &of {
            for (to, b) in &of {
                if a == b {
                    continue;
                }
                let bound = wiring.min_delay(from, to);
                assert!(
                    !bound.is_zero(),
                    "the network's minimum delay between nodes of different domains \
                     ({a:?}, {b:?}) is zero: declare Network::min_latency, or place them \
                     in one domain"
                );
                let entry = &mut lookahead[a.0 as usize][b.0 as usize];
                *entry = (*entry).min(bound);
            }
        }
        for (domain, row) in lookahead.iter_mut().enumerate() {
            row[domain] = Duration::ZERO;
        }
        let mut builder = Simulation::builder(seed)
            .lookahead(&lookahead)
            .grouping(parallel.grouping);
        if let Some(threads) = parallel.threads {
            builder = builder.threads(threads);
        }
        let mut swarm = Swarm {
            sim: builder.build(),
            wiring,
            nodes: BTreeMap::new(),
            domains: Some(Domains { of, lookahead }),
        };
        swarm.join(nodes);
        swarm
    }

    /// Spawn nodes, register them all, then give each its stack (which shares
    /// the complete routing table).
    fn join(&mut self, nodes: impl IntoIterator<Item = (N::Addr, N)>) {
        let spawned: Vec<(N::Addr, Handle<N>)> = nodes
            .into_iter()
            .map(|(addr, node)| {
                let domain = self.domain(&addr);
                let handle = self.sim.spawn_in(domain, node);
                self.wiring.register(addr, deliver_to(handle));
                (addr, handle)
            })
            .collect();
        for (addr, node) in spawned {
            let domain = self.domain(&addr);
            let net = self.sim.spawn_in(domain, self.wiring.stack(addr));
            self.sim.with_mut(net, |stack| stack.set_handle(net));
            self.sim.with_mut(node, |n| n.wire(node, net));
            self.nodes.insert(addr, Member { node, net });
        }
        self.refresh_stacks();
    }

    fn domain(&self, addr: &N::Addr) -> Domain {
        match &self.domains {
            None => Domain(0),
            Some(domains) => *domains
                .of
                .get(addr)
                .expect("a parallel swarm's domains are fixed when it is built"),
        }
    }

    /// Bring every stack up to date with the wiring.
    fn refresh_stacks(&mut self) {
        let Swarm {
            sim, wiring, nodes, ..
        } = self;
        for member in nodes.values() {
            sim.with_mut(member.net, |net| wiring.refresh(net));
        }
    }

    /// Add a node to a running swarm (e.g. a restart). Returns its handle.
    pub fn add_node(&mut self, addr: N::Addr, node: N) -> Handle<N> {
        self.join([(addr, node)]);
        self.nodes[&addr].node
    }

    /// Remove a node from the swarm, returning its state. Messages already in
    /// flight to it are dropped when they arrive.
    pub fn remove_node(&mut self, addr: &N::Addr) -> Option<N> {
        let member = self.nodes.remove(addr)?;
        self.wiring.deregister(addr);
        self.refresh_stacks();
        self.sim.despawn(member.net);
        Some(self.sim.despawn(member.node))
    }

    /// Replace the network model between steps. In a parallel swarm the new
    /// model must keep every lower bound the lookahead was derived from.
    pub fn set_network(&mut self, network: Network<N::Addr, N::Message>) {
        self.wiring.set_network(network);
        if let Some(domains) = &self.domains {
            for (from, a) in &domains.of {
                for (to, b) in &domains.of {
                    if a != b {
                        let bound = self.wiring.min_delay(from, to);
                        assert!(
                            bound >= domains.lookahead[a.0 as usize][b.0 as usize],
                            "the new network's minimum delay {bound:?} between {a:?} and \
                             {b:?} is below the swarm's lookahead"
                        );
                    }
                }
            }
        }
        self.refresh_stacks();
    }

    /// The underlying engine, for scheduling kickoff steps and custom run loops.
    pub fn sim(&mut self) -> &mut Simulation {
        &mut self.sim
    }

    /// Read-only view of the engine (clock, metrics).
    pub fn simulation(&self) -> &Simulation {
        &self.sim
    }

    /// The network stack of a node, if present.
    pub fn net(&self, addr: &N::Addr) -> Option<Handle<Net<N::Addr, N::Message>>> {
        self.nodes.get(addr).map(|member| member.net)
    }

    /// The addresses of all live nodes.
    pub fn node_ids(&self) -> Vec<N::Addr> {
        self.nodes.keys().copied().collect()
    }

    /// The handle for a node, if present.
    pub fn handle(&self, addr: &N::Addr) -> Option<Handle<N>> {
        self.nodes.get(addr).map(|member| member.node)
    }

    /// Inspect a node's state between steps.
    pub fn with_node<R>(&self, addr: &N::Addr, f: impl FnOnce(&N) -> R) -> R {
        self.sim.with(self.nodes[addr].node, f)
    }

    /// Mutate a node's state between steps (input injection).
    pub fn with_node_mut<R>(&mut self, addr: &N::Addr, f: impl FnOnce(&mut N) -> R) -> R {
        let handle = self.nodes[addr].node;
        self.sim.with_mut(handle, f)
    }

    pub fn run_to_completion(&mut self) -> RunOutcome {
        self.sim.run_to_completion()
    }

    pub fn run_until_time(&mut self, t: Time) -> RunOutcome {
        self.sim.run_until_time(t)
    }

    pub fn run_steps(&mut self, n: u64) -> RunOutcome {
        self.sim.run_steps(n)
    }

    pub fn run_at_least_steps(&mut self, n: u64) -> RunOutcome {
        self.sim.run_at_least_steps(n)
    }

    /// Run until `done` returns true, checked at every synchronization point
    /// (before each step in a sequential swarm, between windows in a parallel
    /// one), so the predicate can inspect node state via `&Self`. Returns
    /// [`RunOutcome::Stopped`] when it fires, or [`RunOutcome::Drained`] if the
    /// queue empties first.
    pub fn run_until(&mut self, mut done: impl FnMut(&Self) -> bool) -> RunOutcome {
        loop {
            if done(self) {
                return RunOutcome::Stopped;
            }
            if !self.sim.run_window() {
                return RunOutcome::Drained;
            }
        }
    }
}
