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

//! Network model features: per-link latency from a matrix, and per-sender
//! egress sequencing by message size.

use std::time::Duration;

use monad_sim::{
    time::{micros, millis},
    Ctx, Handle, Simulation, StepLabel, Time,
};
use monad_sim_swarm::{deliver_to, LatencyMatrix, Net, Network, SimClient};

type Addr = u32;
/// The message is its own size in bytes.
type Msg = usize;

/// Sends a scripted list of `(to, size)` messages, all at its start time.
struct Sender {
    addr: Addr,
    script: Vec<(Addr, Msg)>,
    net: Option<Handle<Net<Addr, Msg>>>,
}

impl Sender {
    fn emit(&mut self, ctx: &mut Ctx) {
        let (net, from) = (self.net.unwrap(), self.addr);
        for (to, size) in self.script.clone() {
            ctx.schedule(
                net,
                ctx.now(),
                StepLabel::source("route"),
                move |net, ctx| net.send(ctx, from, to, size),
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
    let net = sim.spawn(Net::new(0, network));
    let send = sim.spawn(Sender {
        addr: 0,
        script,
        net: None,
    });
    sim.with_mut(send, |s| s.wire(send, net));
    sim.with_mut(net, |n| n.register(0, deliver_to(send)));
    let recorders: Vec<_> = receivers
        .iter()
        .map(|&addr| {
            let handle = sim.spawn(Recorder::default());
            sim.with_mut(net, |n| n.register(addr, deliver_to(handle)));
            handle
        })
        .collect();

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
