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

//! A minimal swarm builder over [`monad_sim`]: spawn a set of nodes plus a
//! [`Net`], wire them together, and forward run-control to the engine. The
//! builder is generic over the node process type `N` and knows nothing about a
//! node's internals beyond the [`SimClient`] seam.

use std::{collections::BTreeMap, rc::Rc};

use monad_sim::{Ctx, Handle, RunOutcome, Simulation, StepLabel, Time};

use crate::net::{Deliver, Net, Network};

/// A node that can be plugged into a simulated network.
///
/// The framework owns the node's state as a `monad_sim` process. A message
/// addressed to the node arrives as a scheduled [`receive`](SimClient::receive)
/// step. The node learns its own handle and the network handle once, via
/// [`wire`](SimClient::wire), so it can schedule self-timers and send messages;
/// a node that does neither can leave `wire` as the default no-op.
pub trait SimClient: Sized + 'static {
    /// Address type used to route to this node on the network.
    type Addr: Ord + Copy + 'static;
    /// The wire message type. All nodes on one network share it.
    type Message: Clone + 'static;

    /// Called once, just after the node is spawned, with its own process handle
    /// and the network handle. Default: ignore both.
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
    Rc::new(
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

/// A running swarm: the [`Simulation`], the network handle, and the node
/// handles keyed by address. Homogeneous in the node type `N` for now; the
/// [`Net`] delivery boundary is already type-erased, so a heterogeneous variant
/// can reuse it without changing the network.
pub struct Swarm<N: SimClient> {
    sim: Simulation,
    net: Handle<Net<N::Addr, N::Message>>,
    nodes: BTreeMap<N::Addr, Handle<N>>,
}

impl<N: SimClient> Swarm<N> {
    /// Spawn a `Net` with the given model and the supplied nodes, wiring each
    /// node's handles and registering its delivery thunk with the network.
    pub fn build(
        seed: u64,
        network: Network<N::Addr, N::Message>,
        nodes: impl IntoIterator<Item = (N::Addr, N)>,
    ) -> Self {
        let mut sim = Simulation::new(seed);
        let net = sim.spawn(Net::new(seed, network));
        let mut map = BTreeMap::new();
        for (addr, node) in nodes {
            let handle = sim.spawn(node);
            sim.with_mut(handle, |n| n.wire(handle, net));
            let deliver = deliver_to(handle);
            sim.with_mut(net, move |n| n.register(addr, deliver));
            map.insert(addr, handle);
        }
        Self {
            sim,
            net,
            nodes: map,
        }
    }

    /// Add a node to a running swarm (e.g. a restart). Returns its handle.
    pub fn add_node(&mut self, addr: N::Addr, node: N) -> Handle<N> {
        let net = self.net;
        let handle = self.sim.spawn(node);
        self.sim.with_mut(handle, |n| n.wire(handle, net));
        let deliver = deliver_to(handle);
        self.sim.with_mut(net, move |n| n.register(addr, deliver));
        self.nodes.insert(addr, handle);
        handle
    }

    /// Remove a node from the swarm, returning its state. Messages already in
    /// flight to it are dropped when they arrive.
    pub fn remove_node(&mut self, addr: &N::Addr) -> Option<N> {
        let handle = self.nodes.remove(addr)?;
        let net = self.net;
        self.sim.with_mut(net, |n| n.deregister(addr));
        Some(self.sim.despawn(handle))
    }

    /// Replace the network model between steps.
    pub fn set_network(&mut self, network: Network<N::Addr, N::Message>) {
        let net = self.net;
        self.sim.with_mut(net, |n| n.set_model(network));
    }

    /// The underlying engine, for scheduling kickoff steps and custom run loops.
    pub fn sim(&mut self) -> &mut Simulation {
        &mut self.sim
    }

    /// Read-only view of the engine (clock, metrics).
    pub fn simulation(&self) -> &Simulation {
        &self.sim
    }

    /// The network process handle.
    pub fn net(&self) -> Handle<Net<N::Addr, N::Message>> {
        self.net
    }

    /// The addresses of all live nodes.
    pub fn node_ids(&self) -> Vec<N::Addr> {
        self.nodes.keys().copied().collect()
    }

    /// The handle for a node, if present.
    pub fn handle(&self, addr: &N::Addr) -> Option<Handle<N>> {
        self.nodes.get(addr).copied()
    }

    /// Inspect a node's state between steps.
    pub fn with_node<R>(&self, addr: &N::Addr, f: impl FnOnce(&N) -> R) -> R {
        self.sim.with(self.nodes[addr], f)
    }

    /// Mutate a node's state between steps (input injection).
    pub fn with_node_mut<R>(&mut self, addr: &N::Addr, f: impl FnOnce(&mut N) -> R) -> R {
        let handle = self.nodes[addr];
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

    /// Run until `done` returns true (checked before each step), so the
    /// predicate can inspect node state via `&Self`. Returns
    /// [`RunOutcome::Stopped`] when it fires, or [`RunOutcome::Drained`] if the
    /// queue empties first.
    pub fn run_until(&mut self, mut done: impl FnMut(&Self) -> bool) -> RunOutcome {
        loop {
            if done(self) {
                return RunOutcome::Stopped;
            }
            if self.sim.next_step().is_none() {
                return RunOutcome::Drained;
            }
        }
    }
}
