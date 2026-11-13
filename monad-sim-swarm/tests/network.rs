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

//! Network model features: per-link latency from a matrix, per-sender
//! egress sequencing by message size, and priority classes on the egress.

use std::time::Duration;

use monad_sim::{
    time::{micros, millis},
    Ctx, Handle, Simulation, StepLabel, Time,
};
use monad_sim_swarm::{deliver_to, LatencyMatrix, Net, Network, SimClient, Wiring};

type Addr = u32;
/// The message is its own size in bytes.
type Msg = usize;

/// Sends a scripted list of `(to, size)` messages, all at its start time.
struct Sender {
    script: Vec<(Addr, Msg)>,
    net: Option<Handle<Net<Addr, Msg>>>,
}

impl Sender {
    fn emit(&mut self, ctx: &mut Ctx) {
        let net = self.net.unwrap();
        for (to, size) in self.script.clone() {
            ctx.schedule(
                net,
                ctx.now(),
                StepLabel::source("route"),
                move |net, ctx| net.send(ctx, to, size),
            );
        }
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

/// Records the arrival time of every message.
#[derive(Default)]
struct Recorder {
    arrivals: Vec<(Addr, Msg, Time)>,
}

impl SimClient for Recorder {
    type Addr = Addr;
    type Message = Msg;
    fn receive(&mut self, from: Addr, msg: Msg, ctx: &mut Ctx) {
        self.arrivals.push((from, msg, ctx.now()));
    }
}

/// One sender at address 0 and recorders at `receivers`; returns the
/// arrivals per recorder, in recorder order.
fn run(
    network: Network<Addr, Msg>,
    script: Vec<(Addr, Msg)>,
    receivers: &[Addr],
) -> Vec<Vec<Time>> {
    let mut sim = Simulation::new(0);
    let mut wiring = Wiring::new(0, network);
    let send = sim.spawn(Sender { script, net: None });
    wiring.register(0, deliver_to(send));
    let recorders: Vec<_> = receivers
        .iter()
        .map(|&addr| {
            let handle = sim.spawn(Recorder::default());
            wiring.register(addr, deliver_to(handle));
            handle
        })
        .collect();
    let net = sim.spawn(wiring.stack(0));
    sim.with_mut(net, |stack| stack.set_handle(net));
    sim.with_mut(send, |s| s.wire(send, net));

    sim.schedule(send, Time(0), StepLabel::source("start"), |s, ctx| {
        s.emit(ctx)
    });
    sim.run_to_completion();

    recorders
        .into_iter()
        .map(|r| sim.with(r, |r| r.arrivals.iter().map(|(_, _, at)| *at).collect()))
        .collect()
}

fn no_jitter() -> impl rand::distributions::Distribution<Duration> {
    rand::distributions::Uniform::new_inclusive(Duration::ZERO, Duration::ZERO)
}

/// Each sender/receiver pair gets the latency of its regions.
#[test]
fn matrix_latency_follows_regions() {
    let matrix = LatencyMatrix::new(vec![
        vec![millis(1), millis(40)],
        vec![millis(45), millis(2)],
    ])
    .place(0, 0)
    .place(1, 0)
    .place(2, 1);
    let arrivals = run(
        Network::with_matrix(matrix, no_jitter()),
        vec![(1, 100), (2, 100)],
        &[1, 2],
    );
    assert_eq!(
        arrivals,
        vec![vec![Time(0) + millis(1)], vec![Time(0) + millis(40)]]
    );
}

/// Rows are senders: the reverse direction may differ.
#[test]
fn matrix_is_directional() {
    let matrix = LatencyMatrix::new(vec![
        vec![millis(1), millis(40)],
        vec![millis(45), millis(2)],
    ])
    .place(0u32, 0)
    .place(2, 1);
    assert_eq!(matrix.latency(&0, &2), millis(40));
    assert_eq!(matrix.latency(&2, &0), millis(45));
}

#[test]
#[should_panic(expected = "not placed")]
fn matrix_rejects_unplaced_addresses() {
    let matrix: LatencyMatrix<Addr> = LatencyMatrix::new(vec![vec![millis(1)]]).place(0, 0);
    matrix.latency(&0, &1);
}

/// 10 Mbit/s: 1250 bytes take 1 ms. Three messages sent at once leave one
/// after another and arrive 1 ms apart, after the 5 ms latency.
#[test]
fn egress_sequences_messages_by_size() {
    let arrivals = run(
        Network::reliable(millis(5)).egress(10_000_000, |size: &Msg| *size),
        vec![(1, 1250), (1, 1250), (1, 1250)],
        &[1],
    );
    assert_eq!(
        arrivals,
        vec![vec![
            Time(0) + millis(6),
            Time(0) + millis(7),
            Time(0) + millis(8)
        ]]
    );
}

/// Unicasts to different receivers share the sender's uplink too.
#[test]
fn egress_is_shared_across_receivers() {
    let arrivals = run(
        Network::reliable(millis(5)).egress(10_000_000, |size: &Msg| *size),
        vec![(1, 2500), (2, 1250)],
        &[1, 2],
    );
    assert_eq!(
        arrivals,
        vec![vec![Time(0) + millis(7)], vec![Time(0) + millis(8)]]
    );
}

/// A message lost in the network was still transmitted: the next one waits.
#[test]
fn dropped_messages_still_use_the_uplink() {
    let arrivals = run(
        Network::reliable(millis(5))
            .egress(10_000_000, |size: &Msg| *size)
            .drop_if(|link, _| link.to == 1),
        vec![(1, 1250), (2, 1250)],
        &[1, 2],
    );
    assert_eq!(arrivals, vec![vec![], vec![Time(0) + millis(7)]]);
}

/// A per-sender rate overrides the default.
#[test]
fn egress_rate_overrides_per_sender() {
    let arrivals = run(
        Network::reliable(millis(5))
            .egress(10_000_000, |size: &Msg| *size)
            .egress_rate(0, 100_000_000),
        vec![(1, 1250)],
        &[1],
    );
    assert_eq!(arrivals, vec![vec![Time(0) + millis(5) + micros(100)]]);
}

/// A node's messages to itself do not occupy its uplink.
#[test]
fn self_sends_bypass_egress() {
    let arrivals = run(
        Network::reliable(millis(5)).egress(10_000_000, |size: &Msg| *size),
        vec![(0, 1250), (1, 1250)],
        &[1],
    );
    assert_eq!(arrivals, vec![vec![Time(0) + millis(6)]]);
}

/// Priority classes by size, at 10 Mbit/s: 1250 bytes (1 ms on the wire) are
/// class 0, 2000 bytes (1.6 ms) class 1, 2500 bytes (2 ms) class 2.
fn class_of(size: &Msg) -> u8 {
    match size {
        1250 => 0,
        2000 => 1,
        _ => 2,
    }
}

fn prioritized(depth: usize) -> Network<Addr, Msg> {
    Network::reliable(millis(5))
        .egress(10_000_000, |size: &Msg| *size)
        .priority(class_of, depth)
}

fn at(ms: f64) -> Time {
    Time(0) + Duration::from_secs_f64(ms / 1000.0)
}

/// With a ring of 1, a class-0 message sent after three class-2 ones waits
/// only for the one on the wire; the others follow it, one at a time.
#[test]
fn class_zero_overtakes_waiting_messages() {
    let script = vec![(1, 2500), (1, 2500), (1, 2500), (1, 1250)];
    let fifo = run(
        Network::reliable(millis(5)).egress(10_000_000, |size: &Msg| *size),
        script.clone(),
        &[1],
    );
    assert_eq!(fifo, vec![vec![at(7.0), at(9.0), at(11.0), at(12.0)]]);
    let arrivals = run(prioritized(1), script, &[1]);
    assert_eq!(arrivals, vec![vec![at(7.0), at(8.0), at(10.0), at(12.0)]]);
}

/// With a ring of 2, the class-0 message waits behind two.
#[test]
fn the_ring_depth_bounds_the_wait() {
    let script = vec![(1, 2500), (1, 2500), (1, 2500), (1, 1250)];
    let arrivals = run(prioritized(2), script, &[1]);
    assert_eq!(arrivals, vec![vec![at(7.0), at(9.0), at(10.0), at(12.0)]]);
}

/// Among waiting messages, the more urgent class goes first.
#[test]
fn waiting_messages_leave_by_class() {
    let script = vec![(1, 2500), (1, 2500), (1, 2000)];
    let arrivals = run(prioritized(1), script, &[1]);
    assert_eq!(arrivals, vec![vec![at(7.0), at(8.6), at(10.6)]]);
}

/// One class only: the same arrivals as one queue in the order sent.
#[test]
fn one_class_is_the_plain_egress() {
    let script = vec![(1, 2500), (2, 2500), (1, 2500), (2, 2500)];
    let plain = run(
        Network::reliable(millis(5)).egress(10_000_000, |size: &Msg| *size),
        script.clone(),
        &[1, 2],
    );
    assert_eq!(run(prioritized(1), script, &[1, 2]), plain);
}

/// Class 0 never waits for a wake-up: a burst of it leaves back to back,
/// as without priority.
#[test]
fn a_class_zero_burst_leaves_back_to_back() {
    let script = vec![(1, 1250), (2, 1250), (1, 1250)];
    let plain = run(
        Network::reliable(millis(5)).egress(10_000_000, |size: &Msg| *size),
        script.clone(),
        &[1, 2],
    );
    assert_eq!(run(prioritized(1), script, &[1, 2]), plain);
}

/// A stack that holds messages back wakes itself, so it needs its handle.
#[test]
#[should_panic(expected = "set_handle")]
fn holding_back_needs_the_stack_handle() {
    let mut sim = Simulation::new(0);
    let mut wiring = Wiring::new(0, prioritized(1));
    let send = sim.spawn(Sender {
        script: vec![(1, 2500), (1, 2500)],
        net: None,
    });
    wiring.register(0, deliver_to(send));
    let recorder = sim.spawn(Recorder::default());
    wiring.register(1, deliver_to(recorder));
    let net = sim.spawn(wiring.stack(0));
    sim.with_mut(send, |s| s.wire(send, net));
    sim.schedule(send, Time(0), StepLabel::source("start"), |s, ctx| {
        s.emit(ctx)
    });
    sim.run_to_completion();
}
