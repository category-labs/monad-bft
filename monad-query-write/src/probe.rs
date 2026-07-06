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

//! Targeted timing probe for the ingest pipeline (off unless the
//! `ingest::timing=trace` target is enabled).

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use super::FamilyRanges;

/// Target for the timing instrumentation. Off by default; enable with
/// `RUST_LOG=info,ingest::timing=trace`.
pub(crate) const TIMING_TARGET: &str = "ingest::timing";

/// Timing probe for the ingest pipeline. Counters are cumulative nanoseconds;
/// [`run_timing_reporter`] logs per-interval deltas. The bottleneck signal is
/// backpressure, not per-op time: `producer_send_blocked` high => a downstream
/// track is the limiter; `*_recv_blocked` high => that track is starved.
/// Gated on [`TIMING_TARGET`] at TRACE, so near-zero cost when off.
#[derive(Default)]
pub(crate) struct IngestProbe {
    pub(crate) enabled: bool,
    blocks: AtomicU64,
    /// Producer time blocked on `tx_data.send().await` (data track full).
    pub(crate) producer_send_data_blocked_ns: AtomicU64,
    /// Producer time blocked on `tx_index.send().await` (index track full).
    /// The two are timed separately so the log names the bottleneck track.
    pub(crate) producer_send_index_blocked_ns: AtomicU64,
    /// Aggregate fetch+decode task time. Divided by wall this is the average
    /// number of in-flight fetch tasks (Little's law) — compare against the
    /// configured `fetch_concurrency` to read the fetch duty cycle.
    pub(crate) fetch_decode_ns: AtomicU64,
    /// Data track blocked on `rx.recv().await` (starved => not the bottleneck).
    pub(crate) data_recv_blocked_ns: AtomicU64,
    /// Data track row encode (`encode_pack_entry`, CPU); aggregated across the
    /// concurrent encode tasks, so like `fetch_decode` it can exceed wall-clock.
    pub(crate) data_encode_ns: AtomicU64,
    /// Data track pack flush (`start_flush`'s `stage_pack`, the store write).
    pub(crate) data_flush_ns: AtomicU64,
    /// Index track blocked on `rx.recv().await` (starved => not the bottleneck).
    pub(crate) index_recv_blocked_ns: AtomicU64,
    /// Index track total per-message work: CPU (accumulate + seal/flush compute)
    /// PLUS the meta-store writes and the checkpoint's cross-track wait.
    /// `index_work - index_write - index_ckpt_wait` is the CPU portion.
    pub(crate) index_work_ns: AtomicU64,
    /// Subset of `index_work` spent awaiting meta-store writes (`write_all` +
    /// snapshot persist) — i.e. index-side store I/O, any backend.
    pub(crate) index_write_ns: AtomicU64,
    /// Subset of `index_work` spent in the `Checkpoint` arm awaiting the data
    /// track's durability signal — a cross-track wait, not index CPU or I/O.
    pub(crate) index_ckpt_wait_ns: AtomicU64,
    /// Primary-entity counts ingested (for tx/s, log/s, call-frame/s rates).
    txs: AtomicU64,
    logs: AtomicU64,
    callframes: AtomicU64,
}

impl IngestProbe {
    /// Sole constructor; `Default` exists only to zero the counters.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            enabled: tracing::enabled!(target: TIMING_TARGET, tracing::Level::TRACE),
            ..Self::default()
        })
    }

    /// Start timing a section, or `None` when the probe is disabled (no clock read).
    #[inline]
    pub(crate) fn start(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }

    /// Add the elapsed time since `start` to `counter` (no-op when disabled).
    #[inline]
    pub(crate) fn record(&self, counter: &AtomicU64, start: Option<Instant>) {
        if let Some(start) = start {
            counter.fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Count one block plus its primary entities (logs/txs/call-frames).
    #[inline]
    pub(crate) fn count_block(&self, ranges: &FamilyRanges) {
        if self.enabled {
            self.blocks.fetch_add(1, Ordering::Relaxed);
            self.logs
                .fetch_add(ranges.log_count as u64, Ordering::Relaxed);
            self.txs
                .fetch_add(ranges.tx_count as u64, Ordering::Relaxed);
            self.callframes
                .fetch_add(ranges.trace_count as u64, Ordering::Relaxed);
        }
    }
}

/// Takes the counter name list ONCE and expands the field-wise boilerplate:
/// [`IngestProbe::snapshot`] (relaxed loads) and the snapshot delta
/// (`Sub`, saturating). The struct definitions stay hand-written so the
/// per-field docs survive.
macro_rules! probe_counters {
    ($($field:ident),* $(,)?) => {
        impl IngestProbe {
            /// One point-in-time copy of every counter.
            fn snapshot(&self) -> ProbeSnapshot {
                ProbeSnapshot {
                    $($field: self.$field.load(Ordering::Relaxed),)*
                }
            }
        }

        impl std::ops::Sub for ProbeSnapshot {
            type Output = ProbeSnapshot;

            /// Field-wise saturating per-interval delta.
            fn sub(self, prev: ProbeSnapshot) -> ProbeSnapshot {
                ProbeSnapshot {
                    $($field: self.$field.saturating_sub(prev.$field),)*
                }
            }
        }
    };
}

probe_counters!(
    blocks,
    producer_send_data_blocked_ns,
    producer_send_index_blocked_ns,
    fetch_decode_ns,
    data_recv_blocked_ns,
    data_encode_ns,
    data_flush_ns,
    index_recv_blocked_ns,
    index_work_ns,
    index_write_ns,
    index_ckpt_wait_ns,
    txs,
    logs,
    callframes,
);

/// One point-in-time copy of every probe counter; the reporter logs the
/// field-wise difference between consecutive snapshots.
#[derive(Debug, Clone, Copy, Default)]
struct ProbeSnapshot {
    blocks: u64,
    producer_send_data_blocked_ns: u64,
    producer_send_index_blocked_ns: u64,
    fetch_decode_ns: u64,
    data_recv_blocked_ns: u64,
    data_encode_ns: u64,
    data_flush_ns: u64,
    index_recv_blocked_ns: u64,
    index_work_ns: u64,
    index_write_ns: u64,
    index_ckpt_wait_ns: u64,
    txs: u64,
    logs: u64,
    callframes: u64,
}

/// Periodic reporter: every 5s, log the per-interval throughput and each
/// single-threaded stage's share of wall-clock under [`TIMING_TARGET`]. Spawned
/// only when the probe is enabled, and aborted on `run_ingest` exit.
pub(crate) async fn run_timing_reporter(probe: Arc<IngestProbe>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.tick().await; // first tick is immediate; use it as t0
    let mut prev = probe.snapshot();
    let mut prev_t = Instant::now();
    loop {
        ticker.tick().await;
        let now = probe.snapshot();
        let dt = prev_t.elapsed().as_secs_f64().max(1e-9);
        let d = now - prev;
        // Share of wall-clock for a single-threaded stage (ns over the interval).
        let pct = |ns: u64| (ns as f64 / 1e9 / dt * 100.0).round() as u64;
        let per_s = |c: u64| (c as f64 / dt).round() as u64;
        // Index CPU = total work minus its store-I/O and cross-track-wait parts.
        let index_cpu_ns = d
            .index_work_ns
            .saturating_sub(d.index_write_ns)
            .saturating_sub(d.index_ckpt_wait_ns);
        tracing::trace!(
            target: TIMING_TARGET,
            blocks_per_s = per_s(d.blocks),
            txs_per_s = per_s(d.txs),
            logs_per_s = per_s(d.logs),
            callframes_per_s = per_s(d.callframes),
            producer_send_data_blocked_pct = pct(d.producer_send_data_blocked_ns),
            producer_send_index_blocked_pct = pct(d.producer_send_index_blocked_ns),
            // Average in-flight fetch tasks (aggregate task time / wall); read
            // against fetch_concurrency for duty cycle, not as a stage share.
            fetch_avg_inflight = (d.fetch_decode_ns as f64 / 1e9 / dt).round() as u64,
            data_recv_blocked_pct = pct(d.data_recv_blocked_ns),
            data_encode_pct = pct(d.data_encode_ns),
            data_flush_pct = pct(d.data_flush_ns),
            index_recv_blocked_pct = pct(d.index_recv_blocked_ns),
            index_work_pct = pct(d.index_work_ns),
            index_cpu_pct = pct(index_cpu_ns), // accumulate + seal/flush compute
            index_write_pct = pct(d.index_write_ns), // meta-store write I/O
            index_ckpt_wait_pct = pct(d.index_ckpt_wait_ns), // cross-track durability wait
            "ingest timing (last {dt:.0}s; pct = share of wall per single-threaded stage)"
        );
        prev = now;
        prev_t = Instant::now();
    }
}
