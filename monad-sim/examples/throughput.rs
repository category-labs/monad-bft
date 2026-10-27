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

//! Scheduler throughput: `processes` processes pass `tokens` tokens each to
//! random peers, one domain per process, each step burning `work` rounds of a
//! synthetic cost (about 1 ns per round).
//!
//! ```text
//! cargo run --release -p monad-sim --example throughput -- \
//!     [processes=64] [tokens=16] [work=0] [threads=0] [millis=1000]
//! ```
//!
//! `threads = 0` runs a single-domain `Simulation::new` (sequential); `threads >=
//! 1` runs one domain per process with a 1 ms lookahead on that many threads.
//! Prints steps, wall time, steps per second and a digest of the final state,
//! which is the same for every thread count.

use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use monad_sim::{Ctx, Domain, Grouping, Handle, Simulation, StepLabel, Time, UniformLookahead};
use rand::distributions::Uniform;

const LOOKAHEAD: Duration = Duration::from_millis(1);

struct Peer {
    peers: Vec<Handle<Peer>>,
    work: u64,
    digest: u64,
}

fn mix(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn burn(rounds: u64, mut x: u64) -> u64 {
    for _ in 0..rounds {
        x = black_box(x.rotate_left(7) ^ 0x94d0_49bb);
    }
    x
}

fn receive(peer: &mut Peer, ctx: &mut Ctx, token: u64) {
    peer.digest = mix(burn(peer.work, peer.digest) ^ token);
    let target = peer.peers[ctx.sample(Uniform::new(0, peer.peers.len()))];
    let jitter = Duration::from_micros(ctx.sample(Uniform::new(0u64, 1_000)));
    let at = ctx.now() + LOOKAHEAD + jitter;
    let token = peer.digest;
    ctx.schedule(target, at, StepLabel::source("token"), move |p, ctx| {
        receive(p, ctx, token)
    });
}

fn main() {
    let args: Vec<u64> = std::env::args()
        .skip(1)
        .map(|arg| arg.parse().expect("numeric arguments"))
        .collect();
    let arg = |i: usize, default: u64| args.get(i).copied().unwrap_or(default);
    let processes = arg(0, 64) as usize;
    let tokens = arg(1, 16);
    let work = arg(2, 0);
    let threads = arg(3, 0) as usize;
    let horizon = Time(0) + Duration::from_millis(arg(4, 1_000));

    let mut sim = if threads == 0 {
        Simulation::new(1)
    } else {
        Simulation::builder(1)
            .lookahead(&UniformLookahead {
                domains: processes,
                min_delay: LOOKAHEAD,
            })
            .grouping(Grouping::Auto {
                groups: processes.min(4 * threads),
            })
            .threads(threads)
            .build()
    };
    let handles: Vec<Handle<Peer>> = (0..processes)
        .map(|i| {
            let domain = if threads == 0 { 0 } else { i as u32 };
            sim.spawn_in(
                Domain(domain),
                Peer {
                    peers: Vec::new(),
                    work,
                    digest: i as u64,
                },
            )
        })
        .collect();
    for (i, &handle) in handles.iter().enumerate() {
        let peers = handles.clone();
        sim.with_mut(handle, |p| p.peers = peers);
        for token in 0..tokens {
            let at = Time(0) + Duration::from_micros(token * 1_000 / tokens);
            sim.schedule(handle, at, "start", move |p, ctx| {
                receive(p, ctx, i as u64 * tokens + token)
            });
        }
    }

    let started = Instant::now();
    sim.run_until_time(horizon);
    let elapsed = started.elapsed();
    let steps = sim.step_count();
    let digest = handles
        .iter()
        .fold(0u64, |acc, &h| mix(acc ^ sim.with(h, |p| p.digest)));
    println!(
        "processes {processes} tokens {tokens} work {work} threads {threads} groups {} \
         window {:?}: \
         {steps} steps in {:.3}s, {:.0} steps/s, digest {digest:016x}",
        sim.groups(),
        sim.window(),
        elapsed.as_secs_f64(),
        steps as f64 / elapsed.as_secs_f64(),
    );
}
