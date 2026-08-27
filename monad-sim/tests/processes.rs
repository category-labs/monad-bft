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

//! Worked examples on top of `monad-sim` using toy, app-defined processes.
//!
//! `Msg`, `Node`, `Net`, and `Watchdog` are all defined here, not in the crate:
//! the simulation framework never names a message or component-state type. These
//! exercise the patterns protocol harnesses rely on — cross-process message
//! passing, a swappable network process, sampled latency, and a resettable
//! timer.

use std::{collections::HashMap, time::Duration};

use monad_sim::{
    dist::uniform_duration, time::millis, CancelToken, Ctx, Handle, RunOutcome, Simulation,
    StepLabel, Time,
};

// ---------------------------------------------------------------------------
// A two-node ping/pong over a network process.
// ---------------------------------------------------------------------------

type NodeId = u32;

/// Stop after this many round trips.
const BUDGET: u32 = 3;

#[derive(Clone, Copy)]
enum Msg {
    Ping(u32),
    Pong(u32),
}

impl Msg {
    fn seq(self) -> u32 {
        match self {
            Msg::Ping(n) | Msg::Pong(n) => n,
        }
    }
}

struct Node {
    net: Handle<Net>,
    peer: NodeId,
    pongs: u32,
    /// (arrival time, sequence number) of every message received.
    log: Vec<(Time, u32)>,
}

impl Node {
    fn send(&self, ctx: &mut Ctx, msg: Msg) {
        let (net, to, now) = (self.net, self.peer, ctx.now());
        ctx.schedule(net, now, StepLabel::source("send"), move |net, ctx| {
            net.deliver(ctx, to, msg)
        });
    }

    fn recv(&mut self, ctx: &mut Ctx, msg: Msg) {
        self.log.push((ctx.now(), msg.seq()));
        match msg {
            Msg::Ping(n) => self.send(ctx, Msg::Pong(n)),
            Msg::Pong(n) => {
                self.pongs += 1;
                if n + 1 < BUDGET {
                    self.send(ctx, Msg::Ping(n + 1));
                }
            }
        }
    }
}

enum Latency {
    Const(Duration),
    Uniform(Duration, Duration),
}

/// The network: a routing table and a latency model. Swapping this process out
/// is how a different network model would be plugged in.
struct Net {
    routes: HashMap<NodeId, Handle<Node>>,
    latency: Latency,
}

impl Net {
    fn deliver(&self, ctx: &mut Ctx, to: NodeId, msg: Msg) {
        let dst = self.routes[&to];
        let delay = match self.latency {
            Latency::Const(d) => d,
            Latency::Uniform(lo, hi) => ctx.sample(uniform_duration(lo, hi)),
        };
        let at = ctx.now() + delay;
        ctx.schedule(dst, at, StepLabel::source("deliver"), move |node, ctx| {
            node.recv(ctx, msg)
        });
    }
}

/// Spawn the network and two nodes, wire up routing, and kick off the first ping.
fn build_ping_pong(sim: &mut Simulation, latency: Latency) -> (Handle<Node>, Handle<Node>) {
    let net = sim.spawn(Net {
        routes: HashMap::new(),
        latency,
    });
    let a = sim.spawn(Node {
        net,
        peer: 1,
        pongs: 0,
        log: Vec::new(),
    });
    let b = sim.spawn(Node {
        net,
        peer: 0,
        pongs: 0,
        log: Vec::new(),
    });
    sim.with_mut(net, |net| {
        net.routes.insert(0, a);
        net.routes.insert(1, b);
    });
    sim.schedule(a, Time(0), StepLabel::source("start"), |node, ctx| {
        node.send(ctx, Msg::Ping(0))
    });
    (a, b)
}

#[test]
fn ping_pong_completes() {
    let mut sim = Simulation::new(0);
    let (a, b) = build_ping_pong(&mut sim, Latency::Const(millis(10)));

    assert_eq!(sim.run_to_completion(), RunOutcome::Drained);

    // BUDGET round trips, each 2 * 10ms, the last pong landing at 60ms.
    assert_eq!(sim.with(a, |n| n.pongs), BUDGET);
    assert_eq!(sim.with(a, |n| n.log.len()), BUDGET as usize);
    assert_eq!(sim.with(b, |n| n.log.len()), BUDGET as usize);
    assert_eq!(sim.now(), Time(0) + millis(60));
}

#[test]
fn run_until_time_stops_mid_exchange() {
    let mut sim = Simulation::new(0);
    let (a, b) = build_ping_pong(&mut sim, Latency::Const(millis(10)));

    // First pong lands at 20ms; the second ping is delivered at 30ms.
    assert_eq!(
        sim.run_until_time(Time(0) + millis(25)),
        RunOutcome::ReachedTime(Time(0) + millis(25))
    );
    assert_eq!(sim.with(a, |n| n.pongs), 1);
    assert_eq!(sim.with(a, |n| n.log.len()), 1);
    assert_eq!(sim.with(b, |n| n.log.len()), 1);
    assert_eq!(sim.now(), Time(0) + millis(25));
}

#[test]
fn random_latency_is_deterministic_per_seed() {
    let run = |seed| {
        let mut sim = Simulation::new(seed);
        let (a, _) = build_ping_pong(&mut sim, Latency::Uniform(millis(5), millis(15)));
        sim.run_to_completion();
        sim.with(a, |n| n.log.clone())
    };
    assert_eq!(run(11), run(11));
    assert_ne!(run(11), run(12));
}

// ---------------------------------------------------------------------------
// A watchdog timer that resets on every heartbeat (the pacemaker pattern).
// ---------------------------------------------------------------------------

struct Watchdog {
    timeout: Duration,
    armed: Option<CancelToken>,
    fired: u32,
}

impl Watchdog {
    /// Cancel any pending timeout and arm a fresh one.
    fn heartbeat(&mut self, me: Handle<Watchdog>, ctx: &mut Ctx) {
        if let Some(pending) = self.armed.take() {
            pending.cancel();
        }
        let token = ctx.schedule_after(me, self.timeout, StepLabel::source("timeout"), |w, _| {
            w.fired += 1;
        });
        self.armed = Some(token);
    }
}

#[test]
fn timeout_resets_on_heartbeat() {
    let mut sim = Simulation::new(0);
    let wd = sim.spawn(Watchdog {
        timeout: millis(10),
        armed: None,
        fired: 0,
    });

    // Heartbeats every 5ms keep re-arming the 10ms timeout; the last one is at
    // 20ms, so the only timeout that survives fires at 30ms.
    for t in [0, 5, 10, 15, 20] {
        sim.schedule(
            wd,
            Time(0) + millis(t),
            StepLabel::source("heartbeat"),
            move |w, ctx| w.heartbeat(wd, ctx),
        );
    }

    sim.run_until_time(Time(0) + millis(29));
    assert_eq!(sim.with(wd, |w| w.fired), 0);
    assert_eq!(sim.metrics().steps_cancelled, 4);

    sim.run_until_time(Time(0) + millis(31));
    assert_eq!(sim.with(wd, |w| w.fired), 1);
}
