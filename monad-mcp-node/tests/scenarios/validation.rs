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

//! Validation of the adaptive lead against the model (docs/0005 §7.8):
//! long runs on the public-internet table that export the network and one
//! record per own slot, so the model (`leadsim`) can run on the identical
//! network and the two can be compared slot by slot.
//!
//! Output, under `VAL_OUT`, for a run named `VAL_NAME`:
//! - `<name>.topology.json`: the network (node-level one-way latencies,
//!   last miles included; stakes; clock offsets; egress; timing);
//! - `<name>.slots.jsonl`: one line per own slot, the planner's report
//!   (`adaptive lead: own slot`, warm seals only) joined with what the
//!   harness saw (lead, verdicts, path);
//! - `<name>.validators.jsonl`: per own slot, each validator's first-hop
//!   arrivals and vote, in ms of sim time from the slot's nominal deadline.
//!
//! The long run is configured through the environment (see `Setup`) and
//! runs with
//! `VAL_OUT=<dir> MCP_SIM_THREADS=10 cargo test --release -p monad-mcp-node
//!     --test scenarios validation::long_run -- --ignored --nocapture`.

use std::{
    collections::BTreeMap,
    io::Write as _,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use monad_mcp_common::{Lane, Slot, TimestampDelta};
use monad_mcp_node::{Node, cadence::slot::chorus::FinalizationPath, common::NodeId};
use monad_sim::{ClockDiff, Time};
use monad_sim_swarm::{Link, RegionTable, Swarm};
use rand::{Rng as _, SeedableRng as _};
use rand_chacha::ChaCha8Rng;
use rapidhash::v3::rapidhash_v3;
use serde_json::{Map, Value, json};

use crate::harness::{
    DELTA, GBIT, GENESIS_DEADLINE, GEO_JITTER_MAX, LinkDelay, SLOT_INTERVAL, SimNode,
    assert_progress, deadline_of_slot, geo_delayed, node_config, parallel_from_env, run_digest,
    start_swarm, time_of,
};

/// One run's configuration.
#[derive(Clone, Debug)]
struct Setup {
    name: String,
    seed: u64,
    validators: u64,
    slots: u64,
    payload: usize,
    // the adaptive lead; off: the static lead in DA's own order
    adaptive: bool,
    static_lead_ms: u64,
    theta: f64,
    epsilon: f64,
    block_ms: f64,
    calib_window: usize,
    // the clock spread the planner assumes for its common clock term (ms);
    // 0: off
    clock_sd_ms: f64,
    // own slots whose residuals the margin leaves out
    calib_skip: usize,
    // re-plan waiting seals on every own-slot vote
    refresh_on_vote: bool,
    // strict first-hop priority on the uplink; off: one FIFO
    priority: bool,
    // per-node clock offsets, normal with this standard deviation
    clock_sigma_ms: f64,
    // a delay shared by the validators of a region, redrawn per slot:
    // exponential with this mean, on every message into the region from
    // outside it (correlated.py's shared per-slot delay)
    shared_ms: f64,
    // a one-direction step: from the deadline of `step_slot` on, every
    // message into `step_region` from outside it takes `step_ms` longer
    step_ms: f64,
    step_region: String,
    step_slot: u64,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            name: "run".into(),
            seed: 1,
            validators: 64,
            slots: 1000,
            payload: 256 << 10,
            adaptive: true,
            static_lead_ms: 150,
            theta: 0.25,
            epsilon: 1e-3,
            block_ms: 5.0,
            calib_window: 1000,
            clock_sd_ms: 4.0,
            calib_skip: 8,
            refresh_on_vote: true,
            priority: true,
            clock_sigma_ms: 4.0,
            shared_ms: 0.0,
            step_ms: 0.0,
            step_region: "NYC".into(),
            step_slot: 0,
        }
    }
}

impl Setup {
    // VAL_<FIELD> in upper case overrides each field
    fn from_env() -> Self {
        fn var<T: std::str::FromStr>(name: &str, default: T) -> T {
            match std::env::var(format!("VAL_{name}")) {
                Ok(value) => value
                    .parse()
                    .unwrap_or_else(|_| panic!("VAL_{name}={value} does not parse")),
                Err(_) => default,
            }
        }
        let d = Self::default();
        Self {
            name: var("NAME", d.name),
            seed: var("SEED", d.seed),
            validators: var("VALIDATORS", d.validators),
            slots: var("SLOTS", d.slots),
            payload: var("PAYLOAD", d.payload),
            adaptive: var::<u8>("ADAPTIVE", 1) != 0,
            static_lead_ms: var("STATIC_LEAD_MS", d.static_lead_ms),
            theta: var("THETA", d.theta),
            epsilon: var("EPSILON", d.epsilon),
            block_ms: var("BLOCK_MS", d.block_ms),
            calib_window: var("CALIB_WINDOW", d.calib_window),
            clock_sd_ms: var("CLOCK_SD_MS", d.clock_sd_ms),
            calib_skip: var("CALIB_SKIP", d.calib_skip),
            refresh_on_vote: var::<u8>("REFRESH_ON_VOTE", 1) != 0,
            priority: var::<u8>("PRIORITY", 1) != 0,
            clock_sigma_ms: var("CLOCK_SIGMA_MS", d.clock_sigma_ms),
            shared_ms: var("SHARED_MS", d.shared_ms),
            step_ms: var("STEP_MS", d.step_ms),
            step_region: var("STEP_REGION", d.step_region),
            step_slot: var("STEP_SLOT", d.step_slot),
        }
    }

    // per node, its clock offset in ms: normal(0, σ), seeded
    fn clock_offsets(&self) -> Vec<f64> {
        let mut rng = ChaCha8Rng::seed_from_u64(self.seed ^ 0xC10C_0FF5);
        (0..self.validators)
            .map(|_| {
                if self.clock_sigma_ms == 0.0 {
                    return 0.0;
                }
                // Box–Muller
                let u1: f64 = 1.0 - rng.r#gen::<f64>();
                let u2: f64 = rng.r#gen();
                let z = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
                // whole microseconds: the offsets in the export are exact
                (z * self.clock_sigma_ms * 1000.0).round() / 1000.0
            })
            .collect()
    }

    // the time-varying part of the network, if any
    fn link_delay(
        &self,
        region_names: Vec<String>,
    ) -> Option<impl FnOnce(&BTreeMap<NodeId, u32>) -> LinkDelay> {
        if self.shared_ms <= 0.0 && self.step_ms <= 0.0 {
            return None;
        }
        let setup = self.clone();
        Some(move |region_of: &BTreeMap<NodeId, u32>| -> LinkDelay {
            let region_of = region_of.clone();
            let step_region = region_names
                .iter()
                .position(|name| *name == setup.step_region);
            let step_from = time_of(deadline_of_slot(setup.step_slot));
            Box::new(move |link: &Link<NodeId>| {
                let to = region_of[&link.to];
                if region_of[&link.from] == to {
                    return Duration::ZERO;
                }
                let mut ms = 0.0;
                if setup.shared_ms > 0.0 {
                    ms += shared_delay_ms(setup.seed, setup.shared_ms, link.now, to);
                }
                if setup.step_ms > 0.0
                    && link.now >= step_from
                    && step_region.is_some_and(|region| region as u32 == to)
                {
                    ms += setup.step_ms;
                }
                Duration::from_nanos((ms * 1e6).round() as u64)
            })
        })
    }
}

// the delay region `region` shares in the slot interval `now` falls in:
// exponential with mean `mean_ms`, drawn from a hash of (seed, interval,
// region), so it does not depend on the order messages are sent in
fn shared_delay_ms(seed: u64, mean_ms: f64, now: Time, region: u32) -> f64 {
    let since = (now.0 - time_of(GENESIS_DEADLINE).0).max(0);
    let interval = (since / SLOT_INTERVAL.as_nanos() as i128) as u64;
    let bytes = [
        seed.to_le_bytes(),
        interval.to_le_bytes(),
        u64::from(region).to_le_bytes(),
    ]
    .concat();
    let u = (rapidhash_v3(&bytes) >> 11) as f64 / (1u64 << 53) as f64;
    -mean_ms * (1.0 - u).ln()
}

// Captures the planner's own-slot reports (the only events with a
// `predicted_bound` field); every other event and span is disabled at its
// callsite, so the rest of the node logs nothing and costs nothing.
mod capture {
    use std::sync::{Mutex, OnceLock};

    use serde_json::{Map, Value};
    use tracing::{
        Event, Metadata, Subscriber,
        field::{Field, Visit},
        level_filters::LevelFilter,
        span,
        subscriber::Interest,
    };

    static EVENTS: Mutex<Vec<Map<String, Value>>> = Mutex::new(Vec::new());
    static GLOBAL: OnceLock<()> = OnceLock::new();

    struct Capture;

    fn wanted(meta: &Metadata<'_>) -> bool {
        meta.is_event() && meta.fields().field("predicted_bound").is_some()
    }

    impl Subscriber for Capture {
        fn register_callsite(&self, meta: &'static Metadata<'static>) -> Interest {
            if wanted(meta) {
                Interest::always()
            } else {
                Interest::never()
            }
        }

        fn enabled(&self, meta: &Metadata<'_>) -> bool {
            wanted(meta)
        }

        fn max_level_hint(&self) -> Option<LevelFilter> {
            Some(LevelFilter::INFO)
        }

        fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
            span::Id::from_u64(1)
        }

        fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

        fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

        fn event(&self, event: &Event<'_>) {
            let mut fields = Fields(Map::new());
            event.record(&mut fields);
            EVENTS.lock().expect("not poisoned").push(fields.0);
        }

        fn enter(&self, _: &span::Id) {}

        fn exit(&self, _: &span::Id) {}
    }

    struct Fields(Map<String, Value>);

    impl Visit for Fields {
        fn record_f64(&mut self, field: &Field, value: f64) {
            let value = serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number);
            self.0.insert(field.name().into(), value);
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.0.insert(field.name().into(), value.into());
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            self.0.insert(field.name().into(), value.into());
        }

        fn record_bool(&mut self, field: &Field, value: bool) {
            self.0.insert(field.name().into(), value.into());
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().into(), value.into());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().into(), format!("{value:?}").into());
        }
    }

    // runs `f` with the capture on, and returns the reports it saw. A
    // parallel run logs from worker threads, so it needs the global
    // subscriber: run such tests alone (they are ignored by default)
    pub fn run<R>(parallel: bool, f: impl FnOnce() -> R) -> (R, Vec<Map<String, Value>>) {
        let result = if parallel {
            GLOBAL.get_or_init(|| {
                tracing::subscriber::set_global_default(Capture).expect("no global subscriber yet")
            });
            EVENTS.lock().expect("not poisoned").clear();
            f()
        } else {
            tracing::subscriber::with_default(Capture, f)
        };
        let events = std::mem::take(&mut *EVENTS.lock().expect("not poisoned"));
        (result, events)
    }
}

fn ms_of_time(time: Time) -> f64 {
    time.0 as f64 / 1e6
}

fn ms_of_clock(diff: ClockDiff) -> f64 {
    diff.0 as f64 / 1e6
}

fn round3(ms: f64) -> f64 {
    (ms * 1000.0).round() / 1000.0
}

// `parallel`: on the threads of MCP_SIM_THREADS, else sequential
fn build(setup: &Setup, parallel: bool) -> (Swarm<SimNode>, Value) {
    use monad_mcp_node::config::SourceKind;

    let table = RegionTable::public_internet();
    let names = table.regions().to_vec();
    let geo = geo_delayed(
        &table,
        setup.validators,
        setup.seed,
        setup.link_delay(names),
    );
    let offsets = setup.clock_offsets();
    let n = setup.validators;
    let nodes = (0..n)
        .map(|id| {
            let node_id = NodeId::dummy(id);
            let mut config = node_config(setup.seed, id, n);
            config.proposal.source = SourceKind::Fixed;
            config.proposal.max_payload_bytes = Some(setup.payload);
            config.proposal.propose_before_deadline =
                TimestampDelta::from_millis(setup.static_lead_ms);
            let adaptive = &mut config.proposal.adaptive;
            adaptive.enabled = setup.adaptive;
            adaptive.max_lead_ms = DELTA.as_millis() as f64;
            adaptive.theta = setup.theta;
            adaptive.epsilon = setup.epsilon;
            adaptive.withhold_epsilon = setup.epsilon;
            adaptive.block_ms = setup.block_ms;
            adaptive.calib_window = setup.calib_window;
            adaptive.clock_sd_ms = setup.clock_sd_ms;
            adaptive.calib_skip = setup.calib_skip;
            adaptive.refresh_on_vote = setup.refresh_on_vote;
            let lanes = config.proposal.num_lanes;
            let node = Node::new(&config).expect("config builds");
            let offset = ClockDiff::from_nanos((offsets[id as usize] * 1e6).round() as i128);
            let sim_node = SimNode::new(node_id, node)
                .with_clock_offset(offset)
                .recording();
            let sim_node = if setup.priority {
                sim_node.first_hop_priority(GBIT, n)
            } else {
                sim_node
            };
            ((node_id, sim_node), lanes)
        })
        .collect::<Vec<_>>();
    let lanes = nodes.first().map_or(0, |(_, lanes)| *lanes);
    let nodes = nodes.into_iter().map(|(node, _)| node).collect();

    let topology = json!({
        "n": n,
        "labels": geo.labels,
        "stake": vec![1.0 / n as f64; n as usize],
        "one_way_ms": geo.one_way.iter().map(|row| {
            row.iter().map(|d| round3(d.as_nanos() as f64 / 1e6)).collect::<Vec<_>>()
        }).collect::<Vec<_>>(),
        "jitter_max_ms": GEO_JITTER_MAX.as_nanos() as f64 / 1e6,
        "clock_offset_ms": offsets,
        "egress_bps": GBIT as f64,
        "lanes": lanes,
        "delta_ms": DELTA.as_millis() as f64,
        "slot_ms": SLOT_INTERVAL.as_millis() as f64,
        "payload_bytes": setup.payload,
        "seed": setup.seed,
        // the run, beyond the network
        "slots": setup.slots,
        "adaptive": setup.adaptive,
        "static_lead_ms": setup.static_lead_ms,
        "theta": setup.theta,
        "epsilon": setup.epsilon,
        "block_ms": setup.block_ms,
        "calib_window": setup.calib_window,
        "clock_sd_ms": setup.clock_sd_ms,
        "calib_skip": setup.calib_skip,
        "refresh_on_vote": setup.refresh_on_vote,
        "first_hop_priority": setup.priority,
        "clock_sigma_ms": setup.clock_sigma_ms,
        "shared_ms": setup.shared_ms,
        "step": {"ms": setup.step_ms, "region": setup.step_region, "from_slot": setup.step_slot},
        "notes": "one_way_ms: base latency per node pair, last miles included; every \
                  message adds uniform jitter in [0, jitter_max_ms]. A node's clock reads \
                  sim time + clock_offset_ms. shared_ms: exponential delay per (slot \
                  interval of the send time, receiver region), on messages into a region \
                  from outside it. Equal stakes.",
    });
    let parallel = parallel.then(parallel_from_env).flatten();
    let swarm = start_swarm(setup.seed, geo.network, nodes, parallel);
    (swarm, topology)
}

// the records: one per own slot (all of them; the planner's report only for
// warm seals), and the per-validator detail
fn records(
    swarm: &Swarm<SimNode>,
    horizon: u64,
    reports: Vec<Map<String, Value>>,
) -> (Vec<Value>, Vec<Value>) {
    let ids = swarm.node_ids();
    let n = ids.len();
    let stake = 1.0 / n as f64;
    let reports: BTreeMap<(u64, u64), Map<String, Value>> = reports
        .into_iter()
        .map(|report| {
            let slot = report["slot"].as_u64().expect("slot");
            let lane = report["lane"].as_u64().expect("lane");
            ((slot, lane), report)
        })
        .collect();
    let paths: BTreeMap<Slot, FinalizationPath> =
        swarm.with_node(&ids[0], |node| node.paths().clone());
    // per receiver: first-hop arrivals per (slot, lane)
    type Arrivals = BTreeMap<(Slot, Lane), (NodeId, Vec<Time>)>;
    let arrivals: Vec<Arrivals> = ids
        .iter()
        .map(|id| {
            swarm.with_node(id, |node| {
                node.records()
                    .map(|r| r.first_hop_in.clone())
                    .unwrap_or_default()
            })
        })
        .collect();

    let mut slots_out = Vec::new();
    let mut validators_out = Vec::new();
    for (p, id) in ids.iter().enumerate() {
        let (first_hops, own_lanes, votes, offset, finalized) = swarm.with_node(id, |node| {
            (
                node.first_hops().clone(),
                node.own_lanes().clone(),
                node.records()
                    .map(|r| r.votes_in.clone())
                    .unwrap_or_default(),
                node.clock_offset(),
                node.finalized().clone(),
            )
        });
        for (slot, sealed) in first_hops.range(..Slot(horizon)) {
            let lane = own_lanes[slot];
            let deadline = time_of(deadline_of_slot(slot.0));
            // the lead on the proposer's clock
            let lead = ms_of_time(deadline) - ms_of_time(*sealed) - ms_of_clock(offset);
            let votes = votes.get(slot).cloned().unwrap_or_default();
            let negative = votes.values().filter(|(_, yes)| !yes).count() as f64 * stake;
            let positive = votes.values().filter(|(_, yes)| *yes).count() as f64 * stake;
            let missing = ((n - 1) as f64 * stake - negative - positive).max(0.0);
            let committed = finalized
                .get(slot)
                .and_then(|pattern| pattern.get(usize::from(lane)).map(Option::is_some));
            let mut record = Map::new();
            record.insert("slot".into(), slot.0.into());
            record.insert("lane".into(), usize::from(lane).into());
            record.insert("proposer".into(), p.into());
            record.insert("lead_ms_sim".into(), round3(lead).into());
            record.insert("N".into(), negative.into());
            record.insert("N_positive".into(), positive.into());
            record.insert("N_missing".into(), missing.into());
            record.insert(
                "slot_path".into(),
                paths.get(slot).map(|p| format!("{p:?}")).into(),
            );
            record.insert("lane_committed".into(), committed.into());
            let report = reports.get(&(slot.0, usize::from(lane) as u64));
            record.insert("warm".into(), report.is_some().into());
            if let Some(report) = report {
                for (key, value) in report {
                    if key != "slot" && key != "lane" && key != "message" {
                        record.insert(key.clone(), value.clone());
                    }
                }
                // z_eff at the seal: (bound − M)/√V
                let mean = report["predicted_negative"].as_f64().unwrap_or(f64::NAN);
                let bound = report["predicted_bound"].as_f64().unwrap_or(f64::NAN);
                let sd = report["predicted_sd"].as_f64().unwrap_or(f64::NAN);
                let z_eff = (sd > 0.0).then(|| (bound - mean) / sd);
                record.insert("z_eff".into(), z_eff.into());
            }
            slots_out.push(Value::Object(record));

            // per validator, ms from the nominal deadline (sim time)
            let mut count = Vec::with_capacity(n);
            let mut quorum = Vec::with_capacity(n);
            let mut last = Vec::with_capacity(n);
            let mut vote = Vec::with_capacity(n);
            let mut vote_at = Vec::with_capacity(n);
            for (v, voter) in ids.iter().enumerate() {
                let times = arrivals[v]
                    .get(&(*slot, lane))
                    .filter(|(from, _)| from == id)
                    .map(|(_, times)| times.clone())
                    .unwrap_or_default();
                let rel = |t: &Time| round3(ms_of_time(*t) - ms_of_time(deadline));
                count.push(times.len());
                // the yes vote needs ⌈10·k/11⌉ of the k chunks (d33)
                let need = (10 * times.len()).div_ceil(11);
                let mut sorted = times;
                sorted.sort();
                quorum.push(need.checked_sub(1).and_then(|i| sorted.get(i)).map(rel));
                last.push(sorted.last().map(rel));
                let entry = votes.get(voter);
                vote.push(entry.map(|(_, yes)| u8::from(*yes)));
                vote_at.push(entry.map(|(at, _)| rel(at)));
            }
            validators_out.push(json!({
                "slot": slot.0,
                "lane": usize::from(lane),
                "proposer": p,
                "first_hop_chunks": count,
                "first_hop_quorum_ms": quorum,
                "first_hop_last_ms": last,
                "vote": vote,
                "vote_at_ms": vote_at,
            }));
        }
    }
    slots_out.sort_by_key(|r| (r["slot"].as_u64(), r["lane"].as_u64()));
    validators_out.sort_by_key(|r| (r["slot"].as_u64(), r["lane"].as_u64()));
    (slots_out, validators_out)
}

fn write_lines(path: &Path, lines: &[Value]) {
    let file = std::fs::File::create(path).expect("create output");
    let mut out = std::io::BufWriter::new(file);
    for line in lines {
        serde_json::to_writer(&mut out, line).expect("write");
        out.write_all(b"\n").expect("write");
    }
}

// peak resident memory of this process, MB (macOS reports bytes, Linux KB)
fn peak_rss_mb() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc_rusage::RUsage>::zeroed();
    // SAFETY: getrusage fills the struct it is given
    let usage = unsafe {
        libc_rusage::getrusage(0, usage.as_mut_ptr());
        usage.assume_init()
    };
    let max = usage.ru_maxrss as f64;
    if cfg!(target_os = "macos") {
        max / 1e6
    } else {
        max / 1e3
    }
}

// getrusage without the libc crate: the leading fields of `struct rusage`,
// the same on macOS and Linux (64-bit)
mod libc_rusage {
    #[repr(C)]
    pub struct RUsage {
        pub ru_utime: [i64; 2],
        pub ru_stime: [i64; 2],
        pub ru_maxrss: i64,
        pub rest: [i64; 13],
    }

    unsafe extern "C" {
        pub fn getrusage(who: i32, usage: *mut RUsage) -> i32;
    }
}

struct Summary {
    own: usize,
    warm: usize,
    mean_lead: f64,
    over_theta: usize,
    over_bound: usize,
    splits: usize,
    fallbacks: usize,
}

fn summarize(setup: &Setup, slots: &[Value], from: u64) -> Summary {
    let steady: Vec<&Value> = slots
        .iter()
        .filter(|r| r["slot"].as_u64().unwrap_or(0) >= from)
        .collect();
    let warm: Vec<&&Value> = steady.iter().filter(|r| r["warm"] == true).collect();
    let n_of = |r: &Value| r["N"].as_f64().unwrap_or(0.0);
    let leads: Vec<f64> = steady
        .iter()
        .map(|r| r["lead_ms_sim"].as_f64().unwrap_or(0.0))
        .collect();
    let mut fallback_slots: Vec<u64> = steady
        .iter()
        .filter(|r| r["slot_path"].as_str() == Some("Fallback"))
        .filter_map(|r| r["slot"].as_u64())
        .collect();
    fallback_slots.dedup();
    Summary {
        own: steady.len(),
        warm: warm.len(),
        mean_lead: leads.iter().sum::<f64>() / leads.len().max(1) as f64,
        over_theta: steady.iter().filter(|r| n_of(r) > setup.theta).count(),
        over_bound: warm
            .iter()
            .filter(|r| n_of(r) > r["predicted_bound"].as_f64().unwrap_or(f64::MAX))
            .count(),
        splits: steady.iter().filter(|r| n_of(r) > 1.0 / 3.0).count(),
        fallbacks: fallback_slots.len(),
    }
}

// one run: simulate, export, summarize
// `parallel`: on the threads of MCP_SIM_THREADS, with the global capture
// (run such a test alone); else sequential, with this thread's capture
fn validate(setup: &Setup, out: Option<&Path>, parallel: bool) -> (Summary, u64) {
    let started = Instant::now();
    let parallel = parallel && parallel_from_env().is_some();
    let ((swarm, topology), reports) = capture::run(parallel, || {
        let (mut swarm, topology) = build(setup, parallel);
        let margin = SLOT_INTERVAL.checked_mul(5).expect("in range");
        // in chunks, for progress on long runs
        let chunk = 200;
        let mut done = 0;
        while done < setup.slots {
            done = (done + chunk).min(setup.slots);
            let until = if done == setup.slots {
                deadline_of_slot(done) + margin
            } else {
                deadline_of_slot(done)
            };
            swarm.run_until_time(time_of(until));
            if out.is_some() {
                eprintln!(
                    "{}: slot {done}/{}, {:.1} s, peak RSS {:.0} MB",
                    setup.name,
                    setup.slots,
                    started.elapsed().as_secs_f64(),
                    peak_rss_mb()
                );
            }
        }
        (swarm, topology)
    });
    let elapsed = started.elapsed().as_secs_f64();
    let horizon = setup.slots.saturating_sub(5);
    assert_progress(&swarm, Slot(horizon));
    let digest = run_digest(&swarm);
    let (slots, validators) = records(&swarm, horizon, reports);
    // warm-up (20 observed slots) plus margin
    let summary = summarize(setup, &slots, 30);
    if let Some(out) = out {
        std::fs::create_dir_all(out).expect("output directory");
        let name = &setup.name;
        let topology_path = out.join(format!("{name}.topology.json"));
        std::fs::write(
            &topology_path,
            serde_json::to_string_pretty(&topology).expect("json"),
        )
        .expect("write topology");
        write_lines(&out.join(format!("{name}.slots.jsonl")), &slots);
        write_lines(&out.join(format!("{name}.validators.jsonl")), &validators);
        let line = format!(
            "{name}: {} slots in {elapsed:.0} s ({:.3} s/slot), peak RSS {:.0} MB, digest {digest:016x}; \
             steady own slots {} ({} warm), mean lead {:.1} ms, N>θ {}, N>bound {}, N>1/3 {}, \
             fallback slots {}\n",
            setup.slots,
            elapsed / setup.slots as f64,
            peak_rss_mb(),
            summary.own,
            summary.warm,
            summary.mean_lead,
            summary.over_theta,
            summary.over_bound,
            summary.splits,
            summary.fallbacks,
        );
        eprint!("{line}");
        std::fs::write(out.join(format!("{name}.summary.txt")), &line).expect("write summary");
    }
    (summary, digest)
}

// the plumbing, small and quick: records exist for the warm own slots, and
// the harness's view of the votes agrees with the planner's
#[test]
fn records_match_the_planner() {
    let setup = Setup {
        name: "smoke".into(),
        validators: 16,
        slots: 60,
        payload: 64 << 10,
        ..Setup::default()
    };
    let started = Instant::now();
    // sequential, so the capture is this thread's: other tests may run
    // alongside in this process
    let ((swarm, _), reports) = capture::run(false, || {
        let (mut swarm, topology) = build(&setup, false);
        swarm.run_until_time(time_of(deadline_of_slot(setup.slots)));
        (swarm, topology)
    });
    let horizon = setup.slots - 8;
    let (slots, validators) = records(&swarm, horizon, reports);
    let warm: Vec<&Value> = slots.iter().filter(|r| r["warm"] == true).collect();
    assert!(warm.len() >= 20, "{} warm own slots", warm.len());
    for record in &warm {
        // the planner's count of no votes is the harness's
        let planner = record["negative"].as_f64().expect("negative");
        let harness = record["N"].as_f64().expect("N");
        assert!((planner - harness).abs() < 1e-9, "{record}");
        // and its lead, up to rounding to the nanosecond
        let lead = record["lead_ms"].as_f64().expect("lead");
        let seen = record["lead_ms_sim"].as_f64().expect("lead seen");
        assert!((lead - seen).abs() < 0.01, "{record}");
    }
    // every peer got its first-hop share of every proposal
    for record in &validators {
        let proposer = record["proposer"].as_u64().expect("proposer") as usize;
        let counts = record["first_hop_chunks"].as_array().expect("counts");
        for (v, count) in counts.iter().enumerate() {
            let count = count.as_u64().expect("count");
            assert_eq!(count == 0, v == proposer, "{v}: {count} chunks");
        }
    }
    eprintln!("smoke: {:.1} s", started.elapsed().as_secs_f64());
}

// the time-varying hooks (e, f) leave the network's bounds intact and
// still finalize
#[test]
fn delayed_networks_finalize() {
    let setup = Setup {
        name: "delays".into(),
        validators: 16,
        slots: 40,
        payload: 64 << 10,
        shared_ms: 2.0,
        step_ms: 20.0,
        step_slot: 20,
        ..Setup::default()
    };
    let (summary, _) = validate(&setup, None, false);
    assert!(summary.own > 0);
}

#[test]
#[ignore = "long: run with --release, VAL_OUT and MCP_SIM_THREADS"]
fn long_run() {
    let setup = Setup::from_env();
    let out = PathBuf::from(std::env::var("VAL_OUT").expect("VAL_OUT: the output directory"));
    eprintln!("{setup:?}");
    validate(&setup, Some(&out), true);
}
