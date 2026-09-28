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

//! Worked examples for `monad-sim-swarm`, with toy app-defined node types.
//! `Sender`/`Receiver`/`Counter` live here, not in the crate — the swarm
//! infrastructure never names a concrete node type. These exercise the two
//! guarantees a protocol adapter relies on: a single network carrying
//! *different* node process types (the type-erased delivery boundary), and the
//! `Swarm` builder over one node type with a real latency model.

use monad_sim::{time::millis, Ctx, Handle, Simulation, StepLabel, Time};
use monad_sim_swarm::{deliver_to, Net, Network, SimClient, Swarm};

type Addr = u32;
type Msg = u64;

/// A node that only ever sends.
struct Sender {
    addr: Addr,
    target: Addr,
    net: Option<Handle<Net<Addr, Msg>>>,
}

impl Sender {
    fn emit(&mut self, ctx: &mut Ctx) {
        let (net, from, to) = (self.net.unwrap(), self.addr, self.target);
        ctx.schedule(
            net,
            ctx.now(),
            StepLabel::source("route"),
            move |net, ctx| net.send(ctx, from, to, 42),
        );
    }
}

impl SimClient for Sender {
    type Addr = Addr;
    type Message = Msg;
    fn wire(&mut self, _me: Handle<Self>, net: Handle<Net<Addr, Msg>>) {
        self.net = Some(net);
    }
    fn receive(&mut self, _from: Addr, _msg: Msg, _ctx: &mut Ctx) {}
}

/// A node that only ever receives.
struct Receiver {
    got: u32,
    last: Option<Msg>,
}

impl SimClient for Receiver {
    type Addr = Addr;
    type Message = Msg;
    fn receive(&mut self, _from: Addr, msg: Msg, _ctx: &mut Ctx) {
        self.got += 1;
        self.last = Some(msg);
    }
}

/// A homogeneous node that broadcasts once and counts what it receives.
struct Counter {
    addr: Addr,
    net: Option<Handle<Net<Addr, Msg>>>,
    got: u32,
}

impl Counter {
    fn new(addr: Addr) -> Self {
        Self {
            addr,
            net: None,
            got: 0,
        }
    }
    fn emit(&mut self, ctx: &mut Ctx) {
        let (net, from) = (self.net.unwrap(), self.addr);
        ctx.schedule(
            net,
            ctx.now(),
            StepLabel::source("route"),
            move |net, ctx| net.broadcast(ctx, from, 7),
        );
    }
}

impl SimClient for Counter {
    type Addr = Addr;
    type Message = Msg;
    fn wire(&mut self, _me: Handle<Self>, net: Handle<Net<Addr, Msg>>) {
        self.net = Some(net);
    }
    fn receive(&mut self, _from: Addr, _msg: Msg, _ctx: &mut Ctx) {
        self.got += 1;
    }
}

/// Two *different* node types (`Sender`, `Receiver`) share one `Net<Addr, Msg>`:
/// the network only names `Addr`/`Msg`, and the concrete node type is erased
/// behind the delivery thunk. This is the seam for heterogeneous swarms.
#[test]
fn heterogeneous_nodes_share_one_network() {
    let mut sim = Simulation::new(0);
    let net = sim.spawn(Net::new(0, Network::reliable(millis(10))));
    let recv = sim.spawn(Receiver { got: 0, last: None });
    let send = sim.spawn(Sender {
        addr: 2,
        target: 1,
        net: None,
    });
    sim.with_mut(send, |s| s.wire(send, net));
    sim.with_mut(net, |n| n.register(1, deliver_to(recv)));
    sim.with_mut(net, |n| n.register(2, deliver_to(send)));

    sim.schedule(send, Time(0), StepLabel::source("start"), |s, ctx| {
        s.emit(ctx)
    });
    sim.run_to_completion();

    assert_eq!(sim.with(recv, |r| r.got), 1);
    assert_eq!(sim.with(recv, |r| r.last), Some(42));
    // One 10ms network hop.
    assert_eq!(sim.now(), Time(0) + millis(10));
}

/// The `Swarm` builder wires a set of homogeneous nodes to a network; a
/// broadcast (including to self) reaches everyone after one latency hop.
#[test]
fn swarm_broadcast_reaches_all() {
    let nodes = (0..3).map(|a| (a, Counter::new(a)));
    let mut swarm = Swarm::build(0, Network::reliable(millis(5)), nodes);

    let node0 = swarm.handle(&0).unwrap();
    swarm
        .sim()
        .schedule(node0, Time(0), StepLabel::source("start"), |c, ctx| {
            c.emit(ctx)
        });
    swarm.run_to_completion();

    for a in 0..3 {
        assert_eq!(swarm.with_node(&a, |c| c.got), 1);
    }
    assert_eq!(swarm.simulation().now(), Time(0) + millis(5));
}

/// `drop_if` removes a message before delivery.
#[test]
fn drop_if_suppresses_delivery() {
    let mut sim = Simulation::new(0);
    let net = sim.spawn(Net::new(
        0,
        Network::reliable(millis(10)).drop_if(|link, _msg| link.from == 2),
    ));
    let recv = sim.spawn(Receiver { got: 0, last: None });
    let send = sim.spawn(Sender {
        addr: 2,
        target: 1,
        net: None,
    });
    sim.with_mut(send, |s| s.wire(send, net));
    sim.with_mut(net, |n| n.register(1, deliver_to(recv)));
    sim.with_mut(net, |n| n.register(2, deliver_to(send)));

    sim.schedule(send, Time(0), StepLabel::source("start"), |s, ctx| {
        s.emit(ctx)
    });
    sim.run_to_completion();

    assert_eq!(sim.with(recv, |r| r.got), 0);
}
