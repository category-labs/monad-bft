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

//! Shared ingest engine for the backfill and live pipelines.
//!
//! ```text
//! producer (fetch, assign ids, emit signals) ─┬─► data track  (pack rows, write blobs)
//!                                              └─► index engine (accumulate, seal, flush, write)
//!                       publisher: head = min(data_durable, index_visible)
//! ```
//!
//! Four tasks: `producer` (fetch + id-assignment + signal emission fused), the
//! `data` and `index` tracks (kept separate so row compression runs parallel to
//! index building), and the `publisher`. The index engine writes its artifacts
//! inline (concurrently per seal/flush burst) — no separate writer task or queue.
//!
//! The index engine is **branchless** across modes. It keeps two accumulators:
//!
//! * [`OpenTail`] — bits added since the last `BatchFlush` (the current delta).
//!   Accumulation always goes here.
//! * [`OpenState`] — bits already written as fragments in prior `BatchFlush`es
//!   but whose page/bucket hasn't sealed yet (the carry-over).
//!
//! Three operations, identical in both modes:
//!
//! * **accumulate(block)** → insert ids into `OpenTail`.
//! * **seal** (continuous, frontier-driven) → for each completed granule, write
//!   `OpenState[g] ∪ OpenTail[g]` as an artifact and remove from both.
//! * **batch_flush** → for each `OpenTail` entry, write it as a fragment AND
//!   carry it into `OpenState`; clear `OpenTail`; advance the reader horizon to
//!   the accumulated tip.
//! * **checkpoint** → wait for the data track's pack flush to be durable, then
//!   snapshot `OpenState` + `OpenTail`. It does NOT write fragments or advance the
//!   head — the data barrier is what lets the snapshot's resume block be safe.
//!
//! Seal/flush artifacts are written inline (and durable on return), so
//! `batch_flush` advances the head without a sink barrier.
//!
//! There is no "backfill" vs "live" mode: the engine **always follows the tip**.
//! A run that starts far behind catches up in parallel and then settles into
//! tip-following; "backfill" is just that catch-up phase. The only behavioural
//! knobs are the [`SignalPolicy`] cadences — how often the producer emits
//! `BatchFlush` and `Checkpoint`. An optional `stop_at` ceiling bounds a run (for
//! tests and "ingest up to block X then stop"); it is a run bound, not a mode.

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
#[cfg(not(feature = "serial-data-track"))]
use futures::stream::FuturesOrdered;
use futures::{stream, StreamExt};
use roaring::RoaringBitmap;
use tokio::{
    sync::{mpsc, oneshot, Notify, Semaphore},
    task::JoinSet,
};
use tracing::Instrument;

use crate::{
    engine::{
        authority::HeadPublisher,
        bitmap::{
            encode_bitmap_blob, global_page_start, local_page_start, BitmapBlob,
            BitmapPageArtifact, BitmapPageMeta, STREAM_PAGE_LOCAL_ID_SPAN,
        },
        digest::EMPTY_CHECKSUM,
        family::Family,
        primary_dir::{
            PrimaryDirBucket, PrimaryDirEntry, PrimaryDirFragment, DIRECTORY_BUCKET_SIZE,
        },
        row_codec::RowCodec,
        tables::Tables,
    },
    error::{MonadChainDataError, Result},
    family::{FinalizedBlock, Hash32},
    ingest_helpers::{encode_pack_entry, persist_snapshot, stage_artifact_write, stage_pack},
    ingest_source::ChainDataIngestSource,
    logs::stream_entries_for_log,
    primitives::{
        state::{BlockBlobHeader, BlockRecord, LogId, PrimaryId, TraceId, TxId},
        EvmBlockHeader,
    },
    store::{BlobStore, MetaStore, TableId},
    traces::stream_entries_for_trace,
    txs::{stream_entries_for_tx, TxLocation},
};

pub(crate) const PAGE_SPAN: u64 = STREAM_PAGE_LOCAL_ID_SPAN as u64;
pub(crate) const BUCKET_SPAN: u64 = DIRECTORY_BUCKET_SIZE;
/// The bitmap page and directory bucket share one seal boundary (see
/// [`DIRECTORY_BUCKET_SIZE`]). The whole branchless seal path relies on this, so
/// pin it at compile time rather than trusting the two constants to agree.
const _: () = assert!(PAGE_SPAN == BUCKET_SPAN);
/// The unified per-family seal granule: a primary id seals its page and its
/// bucket together at every multiple of this span.
const SEAL_SPAN: u64 = PAGE_SPAN;

/// The seal boundary at/below `frontier`: the largest multiple of [`SEAL_SPAN`]
/// not exceeding it. Every granule below this is sealed into an artifact; the
/// granule starting here is the open one. Shared by the seal path
/// ([`seal_family`]) and fragment-based recovery ([`crate::ingest_recover`]) so
/// the two can never compute a different open page.
pub(crate) fn seal_boundary(frontier: u64) -> u64 {
    (frontier / SEAL_SPAN) * SEAL_SPAN
}

// ---------------------------------------------------------------------------
// Id assignment (the one ordered, cross-block step)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub(crate) struct FamilyRanges {
    pub log_first: LogId,
    pub log_count: u32,
    pub tx_first: TxId,
    pub tx_count: u32,
    pub trace_first: TraceId,
    pub trace_count: u32,
}

impl FamilyRanges {
    fn log_end(&self) -> u64 {
        PrimaryId::from(self.log_first).as_u64() + self.log_count as u64
    }
    fn tx_end(&self) -> u64 {
        PrimaryId::from(self.tx_first).as_u64() + self.tx_count as u64
    }
    fn trace_end(&self) -> u64 {
        PrimaryId::from(self.trace_first).as_u64() + self.trace_count as u64
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FamilyFrontier {
    pub log: u64,
    pub tx: u64,
    pub trace: u64,
}

impl FamilyFrontier {
    pub(crate) fn assign(&mut self, block: &FinalizedBlock) -> FamilyRanges {
        let log_count: u32 = block.logs_by_tx.iter().map(|t| t.len() as u32).sum();
        let tx_count = block.txs.len() as u32;
        let trace_count = block.traces.len() as u32;

        let ranges = FamilyRanges {
            log_first: LogId::from(PrimaryId::new(self.log)),
            log_count,
            tx_first: TxId::from(PrimaryId::new(self.tx)),
            tx_count,
            trace_first: TraceId::from(PrimaryId::new(self.trace)),
            trace_count,
        };
        self.log += log_count as u64;
        self.tx += tx_count as u64;
        self.trace += trace_count as u64;
        ranges
    }
}

pub(crate) struct AssignedBlock {
    pub number: u64,
    pub ranges: FamilyRanges,
    pub block: FinalizedBlock,
}

// ---------------------------------------------------------------------------
// Signals + controller
// ---------------------------------------------------------------------------

/// Stream consumed by both the data track and the index engine. The only mode
/// difference is *when* the controller injects the two signals.
pub(crate) enum IngestMsg {
    Block(Arc<AssignedBlock>),
    BatchFlush,
    Checkpoint(CheckpointHalf),
}

/// Cross-track rendezvous for a `Checkpoint`. The data track gets the `Signal`
/// half and fires it once its pack flush is durable; the index track gets the
/// `Await` half and blocks on it before persisting the snapshot. This ordering is
/// what guarantees the snapshot's resume block never outruns durable row data.
pub(crate) enum CheckpointHalf {
    /// Data-track half: signalled after the pack flush is durable.
    Signal(oneshot::Sender<()>),
    /// Index-track half: awaited before the snapshot is persisted.
    Await(oneshot::Receiver<()>),
}

/// When the producer emits signals. The flush cadence is a smooth function of the
/// distance to the uploaded tip (`d = tip - last_fed_block`), measured per block:
///
/// ```text
/// flush_interval(d) = max(d / tip_lag_divisor, 1)
/// ```
///
/// So at the tip (`d = 0`) it flushes every block, and it coarsens the farther
/// behind it is — no tiers, no mode switch, no special-case drain-flush. The
/// published head trails the ingested frontier by at most ~`1/tip_lag_divisor` of
/// the remaining distance to the tip. There is no cap: `OpenTail` memory is NOT a
/// factor (the index `seal` drains completed pages continuously, frontier-driven,
/// ~64K ids), so the cadence is purely a reader-freshness / publish-frequency knob.
#[derive(Debug, Clone, Copy)]
pub struct SignalPolicy {
    /// Divides the distance to the tip to get the flush interval (min 1). Larger =
    /// fresher head (flush more often for a given distance); 1 means "flush
    /// interval == distance to tip". A very large value flushes every block.
    pub tip_lag_divisor: u64,
    /// Emit `Checkpoint` every N blocks (bounds fragment-replay on recovery).
    pub checkpoint_every_blocks: u64,
}

/// Shared fetch parallelism for the producer, used IDENTICALLY by both modes:
/// `concurrency` caps how many block fetch+decode tasks run at once (each is
/// spawned so the CPU-bound decode runs across worker threads), and `buffer`
/// (>= `concurrency`) is the deeper ordered look-ahead window of decoded blocks
/// so the engine never starves while fetchers run ahead. Delivery to
/// id-assignment is always in range order. `concurrency == buffer == 1` is the
/// old sequential producer.
#[derive(Debug, Clone, Copy)]
pub struct Prefetch {
    pub concurrency: usize,
    pub buffer: usize,
}

/// Holds the cross-block id frontier, the downstream senders, and the signal
/// cadence counters. Drives both fetch loops so the signal logic lives once.
struct Signaller {
    tx_data: mpsc::Sender<IngestMsg>,
    tx_index: mpsc::Sender<IngestMsg>,
    frontier: FamilyFrontier,
    policy: SignalPolicy,
    since_flush: u64,
    since_ckpt: u64,
    /// Last block number fed downstream — the near side of the distance-to-tip
    /// measurement that selects the adaptive flush cadence.
    last_block: u64,
    /// Uploaded tip observed by the producer's most recent poll — the far side of
    /// the distance-to-tip measurement. `None` before the first poll.
    observed_tip: Option<u64>,
    probe: Arc<IngestProbe>,
}

impl Signaller {
    /// Assign ids and fan the block out to both tracks. Returns `false` if a
    /// downstream channel is closed — which only happens when that track has
    /// already ended (i.e. errored), so the producer should stop and let the
    /// track's own error surface via the `JoinSet`.
    async fn feed(&mut self, block: FinalizedBlock) -> bool {
        self.last_block = block.block_number();
        let open = send_block(
            &mut self.frontier,
            &self.tx_data,
            &self.tx_index,
            block,
            &self.probe,
        )
        .await;
        self.since_flush += 1;
        self.since_ckpt += 1;
        open
    }

    /// The active `BatchFlush` interval: `clamp(distance_to_tip / tip_lag_divisor,
    /// 1, max_flush_blocks)`. At the tip (`distance 0`) this is 1, so the fetcher
    /// flushes every block once caught up — no separate drain-flush needed. Before
    /// the first tip poll (`observed_tip` is `None`) the interval is the cap (or
    /// uncapped → effectively never), but nothing has been fed yet either.
    fn flush_interval(&self) -> u64 {
        let distance = self
            .observed_tip
            .map_or(u64::MAX, |tip| tip.saturating_sub(self.last_block));
        (distance / self.policy.tip_lag_divisor.max(1)).max(1)
    }

    /// Emit `BatchFlush` / `Checkpoint` if their cadences are due. They are
    /// INDEPENDENT — a checkpoint neither flushes fragments nor advances the head —
    /// so both may fire together. Returns `false` if a downstream channel is closed
    /// (see [`Self::feed`]).
    async fn maybe_signal(&mut self) -> bool {
        let mut open = true;
        if self.since_flush >= self.flush_interval() {
            open &= emit_batch_flush(&self.tx_data, &self.tx_index).await;
            self.since_flush = 0;
        }
        if self.since_ckpt >= self.policy.checkpoint_every_blocks {
            open &= emit_checkpoint(&self.tx_data, &self.tx_index).await;
            self.since_ckpt = 0;
        }
        open
    }

    /// Terminal (bounded fetch only): a final BatchFlush so the head reaches the
    /// last block, then a Checkpoint so resume is cheap.
    async fn terminate(&mut self) {
        emit_batch_flush(&self.tx_data, &self.tx_index).await;
        emit_checkpoint(&self.tx_data, &self.tx_index).await;
    }
}

/// Map a source-fetch error (the `ChainDataIngestSource` trait reports
/// `eyre::Report`) into the crate error the pipeline propagates.
fn source_err(e: eyre::Report) -> MonadChainDataError {
    MonadChainDataError::Backend(format!("ingest source: {e}"))
}

/// A spawned pipeline task (fetch or encode) that panicked or was cancelled.
/// Surface it as a backend error so the pipeline aborts and the writer steps
/// down, rather than silently dropping a block from the contiguous range.
fn task_join_err(e: tokio::task::JoinError) -> MonadChainDataError {
    MonadChainDataError::Backend(format!("ingest spawned task: {e}"))
}

/// Target for the timing instrumentation. Off by default; enable with
/// `RUST_LOG=info,ingest::timing=trace`.
const TIMING_TARGET: &str = "ingest::timing";

/// Targeted timing probe for the ingest pipeline. All counters are cumulative
/// nanoseconds; [`run_timing_reporter`] logs per-interval deltas (and the derived
/// %-of-wall for each single-threaded stage) under the [`TIMING_TARGET`] target.
///
/// The discriminating signal for "where is the bottleneck" in this producer ->
/// channel -> track pipeline is backpressure, not raw per-op time:
/// `producer_send_blocked` high => a downstream track is the limiter;
/// `*_recv_blocked` high for a track => that track is *starved* (not the
/// bottleneck). Within the data track, `encode` vs `flush` splits CPU from
/// blob-store writes.
///
/// Measurement is gated on the target being enabled at TRACE, so it is zero-cost
/// in normal runs (a single predictable branch per measurement when off).
pub(crate) struct IngestProbe {
    enabled: bool,
    blocks: AtomicU64,
    /// Producer time blocked on `tx_{data,index}.send().await` (downstream full).
    producer_send_blocked_ns: AtomicU64,
    /// Aggregate fetch+decode busy time across the concurrent fetch tasks
    /// (overlaps wall-clock; >100% of wall means multi-core decode).
    fetch_decode_ns: AtomicU64,
    /// Data track blocked on `rx.recv().await` (starved => not the bottleneck).
    data_recv_blocked_ns: AtomicU64,
    /// Data track row encode (`encode_pack_entry`, CPU).
    data_encode_ns: AtomicU64,
    /// Data track pack flush (`flush_pack`, the blob-store write).
    data_flush_ns: AtomicU64,
    /// Index track blocked on `rx.recv().await` (starved => not the bottleneck).
    index_recv_blocked_ns: AtomicU64,
    /// Index track total per-message work: CPU (accumulate + seal/flush compute)
    /// PLUS the meta-store writes. `index_work - index_write` is the CPU portion.
    index_work_ns: AtomicU64,
    /// Subset of `index_work` spent awaiting meta-store writes (`write_all` +
    /// snapshot persist) — i.e. index-side store I/O, any backend.
    index_write_ns: AtomicU64,
    /// Primary-entity counts ingested (for tx/s, log/s, call-frame/s rates).
    txs: AtomicU64,
    logs: AtomicU64,
    callframes: AtomicU64,
}

impl IngestProbe {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            enabled: tracing::enabled!(target: TIMING_TARGET, tracing::Level::TRACE),
            blocks: AtomicU64::new(0),
            producer_send_blocked_ns: AtomicU64::new(0),
            fetch_decode_ns: AtomicU64::new(0),
            data_recv_blocked_ns: AtomicU64::new(0),
            data_encode_ns: AtomicU64::new(0),
            data_flush_ns: AtomicU64::new(0),
            index_recv_blocked_ns: AtomicU64::new(0),
            index_work_ns: AtomicU64::new(0),
            index_write_ns: AtomicU64::new(0),
            txs: AtomicU64::new(0),
            logs: AtomicU64::new(0),
            callframes: AtomicU64::new(0),
        })
    }

    /// Start timing a section, or `None` when the probe is disabled (no clock read).
    #[inline]
    fn start(&self) -> Option<Instant> {
        self.enabled.then(Instant::now)
    }

    /// Add the elapsed time since `start` to `counter` (no-op when disabled).
    #[inline]
    fn record(&self, counter: &AtomicU64, start: Option<Instant>) {
        if let Some(start) = start {
            counter.fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
    }

    /// Count one block plus its primary entities (logs/txs/call-frames). Called
    /// once per block from the producer after id assignment.
    #[inline]
    fn count_block(&self, ranges: &FamilyRanges) {
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

    fn snapshot(&self) -> [u64; 12] {
        let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
        [
            g(&self.blocks),
            g(&self.producer_send_blocked_ns),
            g(&self.fetch_decode_ns),
            g(&self.data_recv_blocked_ns),
            g(&self.data_encode_ns),
            g(&self.data_flush_ns),
            g(&self.index_recv_blocked_ns),
            g(&self.index_work_ns),
            g(&self.txs),
            g(&self.logs),
            g(&self.callframes),
            g(&self.index_write_ns),
        ]
    }
}

/// Periodic reporter: every 5s, log the per-interval throughput and each
/// single-threaded stage's share of wall-clock under [`TIMING_TARGET`]. Spawned
/// only when the probe is enabled, and aborted on `run_ingest` exit.
async fn run_timing_reporter(probe: Arc<IngestProbe>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.tick().await; // first tick is immediate; use it as t0
    let mut prev = probe.snapshot();
    let mut prev_t = Instant::now();
    loop {
        ticker.tick().await;
        let now = probe.snapshot();
        let dt = prev_t.elapsed().as_secs_f64().max(1e-9);
        let d: Vec<u64> = now
            .iter()
            .zip(prev)
            .map(|(n, p)| n.saturating_sub(p))
            .collect();
        // Share of wall-clock for a single-threaded stage (ns over the interval).
        let pct = |ns: u64| (ns as f64 / 1e9 / dt * 100.0).round() as u64;
        let per_s = |c: u64| (c as f64 / dt).round() as u64;
        tracing::trace!(
            target: TIMING_TARGET,
            blocks_per_s = per_s(d[0]),
            txs_per_s = per_s(d[8]),
            logs_per_s = per_s(d[9]),
            callframes_per_s = per_s(d[10]),
            producer_send_blocked_pct = pct(d[1]),
            fetch_decode_busy_pct = pct(d[2]), // aggregate across fetch tasks; >100 = multi-core
            data_recv_blocked_pct = pct(d[3]),
            data_encode_pct = pct(d[4]),
            data_flush_pct = pct(d[5]),
            index_recv_blocked_pct = pct(d[6]),
            index_work_pct = pct(d[7]),
            index_cpu_pct = pct(d[7].saturating_sub(d[11])), // accumulate + seal/flush compute
            index_write_pct = pct(d[11]),                    // meta-store write I/O
            "ingest timing (last {dt:.0}s; pct = share of wall per single-threaded stage)"
        );
        prev = now;
        prev_t = Instant::now();
    }
}

/// Producer task: always follow the tip. Fetch blocks, assign ids, fan out, and
/// emit signals. Replaces the old separate fetch + controller tasks AND the
/// backfill/live split — "backfill" is just the catch-up phase of this loop.
///
/// On each poll it fetches the available backlog `[next..=target]` in parallel
/// (with the shared prefetch), where `target = min(uploaded tip, stop_at)`. At the
/// steady tip the backlog is ~1 block (degrading naturally to one-at-a-time); a
/// catch-up gap or a burst between polls is fetched in parallel instead. With
/// `stop_at = Some(end)` the run terminates (terminal flush + checkpoint) once it
/// passes `end`; with `stop_at = None` it follows the tip forever.
// TODO(fetch-retry): `fetch_finalized_block`/`get_latest_uploaded` errors here
// propagate straight up and abort the whole pipeline (the writer steps down and
// the supervisor restarts it). The old runner wrapped fetches in bounded
// retry+jittered-backoff (see the removed `fetch_block_with_retry`). Fold a retry
// policy back in — ideally INSIDE the `ChainDataIngestSource` impl so the engine
// stays transport-agnostic, or as a thin wrapper here — so a transient upstream
// blip doesn't bounce the lease. Deferred for now.
async fn run_producer<S>(
    source: S,
    start: u64,
    stop_at: Option<u64>,
    poll_ms: u64,
    prefetch: Prefetch,
    mut sig: Signaller,
) -> Result<()>
where
    S: ChainDataIngestSource,
{
    let mut next = start;
    loop {
        let latest = source.get_latest_uploaded().await.map_err(source_err)?;
        // Record the tip for the adaptive flush interval (per-block `maybe_signal`
        // in `fetch_range` scales the flush interval by distance to this tip).
        sig.observed_tip = latest;
        // Fetch up to min(uploaded tip, stop_at). `caught_tip` means we've drained
        // the uploaded tip — NOT merely reached the stop ceiling.
        let target = match (latest, stop_at) {
            (Some(l), Some(s)) => Some(l.min(s)),
            (Some(l), None) => Some(l),
            (None, _) => None,
        };
        if let Some(target) = target.filter(|t| *t >= next) {
            if !fetch_range(&source, &mut sig, next, target, prefetch).await? {
                return Ok(());
            }
            next = target + 1;
            let caught_tip = latest.is_some_and(|l| next > l);
            if !sig.maybe_signal().await {
                return Ok(());
            }
            // Loop immediately to keep catching up; only sleep once drained.
            if !caught_tip {
                continue;
            }
        } else if !sig.maybe_signal().await {
            // Nothing new to fetch: drain any pending flush before waiting.
            return Ok(());
        }
        // Bounded run: once we've fed past the ceiling, terminal flush + return.
        if stop_at.is_some_and(|s| next > s) {
            sig.terminate().await;
            return Ok(());
        }
        // Caught up: poll the tip again after a fixed interval. (This sleep only
        // ever runs at the tip — catch-up streams the whole backlog without
        // polling.) Small enough for a fresh head; idle polls are cheap.
        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
    }
}

/// Fetch `[start, end]` with bounded, ordered prefetch + parallel decode, feeding
/// each block downstream in range order. Returns `Ok(false)` if a downstream track
/// closed mid-range (the caller should stop and let that track's error surface),
/// `Ok(true)` once the whole range was fed. Shared by bounded backfill and the
/// tip-follower (which calls it once per tip-poll over the available backlog).
///
/// Each block fetch is spawned so the CPU-bound decode inside
/// `fetch_finalized_block` runs across worker threads instead of inline on the
/// producer task. A semaphore caps *active* fetch+decode tasks at `concurrency`;
/// `buffered` keeps a wider window (the deeper look-ahead buffer) so completed
/// blocks queue up and the engine never starves during a flush/checkpoint stall.
/// `buffered` (not `buffer_unordered`) yields in range order, so id-assignment in
/// `feed` stays strictly sequential.
async fn fetch_range<S>(
    source: &S,
    sig: &mut Signaller,
    start: u64,
    end: u64,
    prefetch: Prefetch,
) -> Result<bool>
where
    S: ChainDataIngestSource,
{
    let permits = Arc::new(Semaphore::new(prefetch.concurrency.max(1)));
    let window = prefetch.buffer.max(prefetch.concurrency).max(1);
    let probe = sig.probe.clone();
    let mut fetched = stream::iter(start..=end)
        .map(|number| {
            let source = source.clone();
            let permits = permits.clone();
            let probe = probe.clone();
            tokio::spawn(
                async move {
                    let _permit = permits
                        .acquire_owned()
                        .await
                        .expect("fetch semaphore is never closed");
                    // Busy time of fetch + decode for this block; aggregated across
                    // the concurrent tasks (so it can exceed wall-clock).
                    let decode_start = probe.start();
                    let result = source
                        .fetch_finalized_block(number)
                        .await
                        .map_err(source_err);
                    probe.record(&probe.fetch_decode_ns, decode_start);
                    result
                }
                .instrument(tracing::trace_span!(
                    target: TIMING_TARGET,
                    "fetch_block",
                    block = number
                )),
            )
        })
        .buffered(window);
    while let Some(joined) = fetched.next().await {
        // Outer `?`: a spawned fetch panicked/was cancelled. Inner `?`: the fetch
        // itself errored (after its own retries). Either aborts the run.
        let block = joined.map_err(task_join_err)??;
        if !sig.feed(block).await || !sig.maybe_signal().await {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Fan one block to both tracks. Returns `true` while both channels are open;
/// `false` once either is closed (its track has ended).
async fn send_block(
    frontier: &mut FamilyFrontier,
    tx_data: &mpsc::Sender<IngestMsg>,
    tx_index: &mpsc::Sender<IngestMsg>,
    block: FinalizedBlock,
    probe: &IngestProbe,
) -> bool {
    let ranges = {
        let _span = tracing::trace_span!(target: TIMING_TARGET, "assign").entered();
        frontier.assign(&block)
    };
    probe.count_block(&ranges);
    let assigned = Arc::new(AssignedBlock {
        number: block.block_number(),
        ranges,
        block,
    });
    // Time spent here is backpressure: a full track channel means that track is
    // the bottleneck (the producer can fetch faster than it can be consumed).
    let send_start = probe.start();
    let data_open = tx_data
        .send(IngestMsg::Block(assigned.clone()))
        .await
        .is_ok();
    let index_open = tx_index.send(IngestMsg::Block(assigned)).await.is_ok();
    probe.record(&probe.producer_send_blocked_ns, send_start);
    data_open && index_open
}

async fn emit_batch_flush(
    tx_data: &mpsc::Sender<IngestMsg>,
    tx_index: &mpsc::Sender<IngestMsg>,
) -> bool {
    let data_open = tx_data.send(IngestMsg::BatchFlush).await.is_ok();
    let index_open = tx_index.send(IngestMsg::BatchFlush).await.is_ok();
    data_open && index_open
}

/// Emit a `Checkpoint` to both tracks wired through a fresh oneshot: the data
/// track signals after its pack flush is durable, the index track awaits that
/// before snapshotting. Returns `true` while both channels are open.
async fn emit_checkpoint(
    tx_data: &mpsc::Sender<IngestMsg>,
    tx_index: &mpsc::Sender<IngestMsg>,
) -> bool {
    let (tx, rx) = oneshot::channel();
    let data_open = tx_data
        .send(IngestMsg::Checkpoint(CheckpointHalf::Signal(tx)))
        .await
        .is_ok();
    let index_open = tx_index
        .send(IngestMsg::Checkpoint(CheckpointHalf::Await(rx)))
        .await
        .is_ok();
    data_open && index_open
}

// ---------------------------------------------------------------------------
// Progress + publisher
// ---------------------------------------------------------------------------

#[derive(Default)]
pub(crate) struct Progress {
    data_durable: AtomicU64,
    /// Highest block whose index is durable AND reader-visible (set by the engine
    /// at `batch_flush`, after its inline artifact writes return durable). For
    /// both modes this is the accumulated tip at the last flush.
    index_visible: AtomicU64,
    /// Pulsed whenever a durable frontier advances, so the publisher is pushed
    /// (not polled) toward the freshest head. `notify_one` stores a permit when
    /// no waiter is parked, so a pulse that races the publisher's
    /// read-publish-rewait cycle is never lost; bursts coalesce because the
    /// publisher always re-reads `publishable_head()` on wake.
    advance: Notify,
}

impl Progress {
    pub(crate) fn set_data_durable(&self, block: u64) {
        self.data_durable.fetch_max(block, Ordering::Relaxed);
        self.advance.notify_one();
    }
    pub(crate) fn set_index_visible(&self, block: u64) {
        self.index_visible.fetch_max(block, Ordering::Relaxed);
        self.advance.notify_one();
    }
    pub(crate) fn data_durable(&self) -> u64 {
        self.data_durable.load(Ordering::Relaxed)
    }
    pub(crate) fn publishable_head(&self) -> u64 {
        self.data_durable
            .load(Ordering::Relaxed)
            .min(self.index_visible.load(Ordering::Relaxed))
    }
    /// Resolves the next time a durable frontier advances (or immediately if a
    /// pulse was stored since the last wait).
    pub(crate) async fn changed(&self) {
        self.advance.notified().await;
    }
}

pub(crate) async fn run_publisher<M>(
    progress: Arc<Progress>,
    publisher: Arc<HeadPublisher<M>>,
    mut published: u64,
    mut shutdown: mpsc::Receiver<()>,
) -> Result<()>
where
    M: MetaStore,
{
    // Publish any head already durable before parking on the first pulse — e.g. a
    // warm resume seeds `data_durable` and the terminal flush may have run before
    // this task registered a waiter.
    let head = progress.publishable_head();
    if head > published {
        publisher.publish(head, EMPTY_CHECKSUM).await?;
        published = head;
    }

    loop {
        tokio::select! {
            // Push: a track advanced a durable frontier. Re-read the latest
            // `publishable_head()` (bursts during an in-flight write coalesce into
            // one publish of the freshest head).
            _ = progress.changed() => {
                let head = progress.publishable_head();
                if head > published {
                    publisher.publish(head, EMPTY_CHECKSUM).await?;
                    published = head;
                }
            }
            _ = shutdown.recv() => {
                let head = progress.publishable_head();
                if head > published {
                    publisher.publish(head, EMPTY_CHECKSUM).await?;
                }
                return Ok(());
            }
        }
    }
}

/// Aborts a background task when dropped, so it can't outlive `run_ingest` on any
/// return path.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// ---------------------------------------------------------------------------
// Artifact writes
// ---------------------------------------------------------------------------

/// A single index write the engine produces. The engine applies these inline
/// (see [`IndexEngine::write_all`]) — sealing/flushing computes the set of
/// writes synchronously, then issues them concurrently against the store. No
/// separate writer task or queue: inline writes are durable on return (so
/// `batch_flush` needs no barrier) and are naturally backpressured.
pub(crate) enum ArtifactWrite {
    Page {
        family: Family,
        stream_id: String,
        page_start_local: u32,
        artifact: BitmapPageArtifact,
    },
    Bucket {
        family: Family,
        bucket_start: u64,
        bucket: PrimaryDirBucket,
    },
    PageFragment {
        family: Family,
        stream_id: String,
        page_start_local: u32,
        marker: u64,
        blob: Bytes,
    },
    DirFragment {
        family: Family,
        fragment: PrimaryDirFragment,
    },
    /// The streams first seen in the current open page this batch — the inventory
    /// the reader uses to discover open-page fragments (bitmap fragments are
    /// partitioned per stream, so they can't be enumerated by a scan). Written as
    /// one-or-more chunked delta rows keyed by `(marker, chunk_idx)`.
    OpenStreams {
        family: Family,
        global_page_start: u64,
        marker: u64,
        streams: Vec<String>,
    },
}

// ---------------------------------------------------------------------------
// Open accumulators
// ---------------------------------------------------------------------------

/// Current-batch delta (since the last `BatchFlush`). Accumulation target.
#[derive(Default)]
pub(crate) struct FamilyTail {
    pub pages: HashMap<(String, u64), RoaringBitmap>,
    pub dir: Vec<(u64, u64, u64)>, // (block, first_id, end_id)
}

/// Carry-over: bits already fragmented in prior flushes, page/bucket not yet
/// sealed; plus the per-family sealed frontier. Because pages and buckets share
/// one boundary ([`SEAL_SPAN`]), a single `sealed_id` (a multiple of `SEAL_SPAN`)
/// tracks both: granules below it are sealed into artifacts and removed from the
/// maps; everything at/above it is still open.
#[derive(Default)]
pub(crate) struct FamilyState {
    pub pages: HashMap<(String, u64), RoaringBitmap>,
    pub dir: VecDeque<(u64, u64, u64)>,
    pub sealed_id: u64,
    /// Streams already inventoried in `open_streams` for the current open page, so
    /// a stream is recorded once per page instead of on every `batch_flush`.
    /// Cleared when the page rolls (in `seal_family`).
    pub open_streams_seen: HashSet<String>,
}

#[derive(Default)]
pub(crate) struct OpenTail {
    pub log: FamilyTail,
    pub tx: FamilyTail,
    pub trace: FamilyTail,
}

#[derive(Default)]
pub(crate) struct OpenState {
    pub log: FamilyState,
    pub tx: FamilyState,
    pub trace: FamilyState,
}

impl OpenTail {
    fn family_mut(&mut self, family: Family) -> &mut FamilyTail {
        match family {
            Family::Log => &mut self.log,
            Family::Tx => &mut self.tx,
            Family::Trace => &mut self.trace,
        }
    }
}

// ---------------------------------------------------------------------------
// Index engine (branchless)
// ---------------------------------------------------------------------------

/// The index engine writes artifacts inline against the store, so it carries the
/// store generics. Sealing/flushing computes a set of [`ArtifactWrite`]s
/// synchronously (mutating the accumulators), then [`write_all`](Self::write_all)
/// issues them concurrently — overlapping the I/O burst without a writer task.
pub(crate) struct IndexEngine<M: MetaStore, B: BlobStore> {
    state: OpenState,
    tail: OpenTail,
    /// Running next-id per family after the latest accumulated block — the seal
    /// boundary input.
    frontier: FamilyFrontier,
    last_block: u64,
    tables: Arc<Tables<M, B>>,
    progress: Arc<Progress>,
    snapshots: SnapshotStore<M>,
    probe: Arc<IngestProbe>,
}

impl<M, B> IndexEngine<M, B>
where
    M: MetaStore,
    B: BlobStore,
{
    fn new(
        state: OpenState,
        tail: OpenTail,
        frontier: FamilyFrontier,
        resume: u64,
        tables: Arc<Tables<M, B>>,
        progress: Arc<Progress>,
        snapshots: SnapshotStore<M>,
        probe: Arc<IngestProbe>,
    ) -> Self {
        Self {
            state,
            tail,
            frontier,
            // Seed from the recovered resume block, not 0: a `Checkpoint` that
            // fires before the first post-recovery block must snapshot the real
            // resume block, and an empty-tail flush must advance the head to it.
            last_block: resume,
            tables,
            progress,
            snapshots,
            probe,
        }
    }

    /// Commit a seal/flush burst as one meta batch. The store layer
    /// (`MetaStore::apply_writes`) is responsible for respecting backend batch
    /// limits — e.g. the dynamo backend splits into backend-sized chunks and
    /// applies them concurrently — so we hand it the whole burst rather than
    /// chunking here. (Per-*row* size is bounded where it matters, e.g.
    /// open-streams delta rows.)
    async fn write_all(&self, writes: Vec<ArtifactWrite>) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        // Time spent here is index-side store I/O (any backend) — the subset of
        // `index_work` that is NOT bitmap/seal CPU.
        let io_start = self.probe.start();
        let result = self
            .tables
            .with_writes(|w| {
                Box::pin(async move {
                    for write in writes {
                        stage_artifact_write(w, write)?;
                    }
                    Ok(())
                })
            })
            .await;
        self.probe.record(&self.probe.index_write_ns, io_start);
        result
    }

    /// Insert one block's ids into the tail and advance the seal frontier. All
    /// three families run the one [`accumulate_family`] routine, differing only
    /// in their record iterator and stream-id extractor (the per-family
    /// `stream_entries_for_*` shared with the legacy path). Tx extraction decodes
    /// the envelope and so is fallible, which makes this fallible too.
    fn accumulate(&mut self, b: &AssignedBlock) -> Result<()> {
        accumulate_family(
            self.tail.family_mut(Family::Log),
            PrimaryId::from(b.ranges.log_first).as_u64(),
            b.ranges.log_count,
            b.number,
            b.block.logs_by_tx.iter().flatten(),
            |log, id| {
                Ok(stream_entries_for_log(
                    log.address.as_slice(),
                    log.data.topics(),
                    LogId::from(PrimaryId::new(id)),
                ))
            },
        )?;
        accumulate_family(
            self.tail.family_mut(Family::Tx),
            PrimaryId::from(b.ranges.tx_first).as_u64(),
            b.ranges.tx_count,
            b.number,
            b.block.txs.iter(),
            |tx, id| stream_entries_for_tx(tx, TxId::from(PrimaryId::new(id))),
        )?;
        accumulate_family(
            self.tail.family_mut(Family::Trace),
            PrimaryId::from(b.ranges.trace_first).as_u64(),
            b.ranges.trace_count,
            b.number,
            b.block.traces.iter(),
            |trace, id| {
                Ok(stream_entries_for_trace(
                    trace,
                    TraceId::from(PrimaryId::new(id)),
                ))
            },
        )?;
        self.frontier.log = b.ranges.log_end();
        self.frontier.tx = b.ranges.tx_end();
        self.frontier.trace = b.ranges.trace_end();
        self.last_block = b.number;
        Ok(())
    }

    /// Seal every granule the frontier just completed, combining carry-over and
    /// current-delta bits, and write the resulting artifacts inline. Continuous
    /// (per block).
    async fn seal_ready(&mut self) -> Result<()> {
        let mut writes = seal_family(
            Family::Log,
            &mut self.state.log,
            &mut self.tail.log,
            self.frontier.log,
        )?;
        writes.append(&mut seal_family(
            Family::Tx,
            &mut self.state.tx,
            &mut self.tail.tx,
            self.frontier.tx,
        )?);
        writes.append(&mut seal_family(
            Family::Trace,
            &mut self.state.trace,
            &mut self.tail.trace,
            self.frontier.trace,
        )?);
        self.write_all(writes).await
    }

    /// Flush the tail as reader-visible fragments and carry it into the state;
    /// once the fragments are durable, advance the reader horizon to the tip.
    async fn batch_flush(&mut self) -> Result<()> {
        let mut writes = flush_family(
            Family::Log,
            &mut self.state.log,
            &mut self.tail.log,
            self.last_block,
        )?;
        writes.append(&mut flush_family(
            Family::Tx,
            &mut self.state.tx,
            &mut self.tail.tx,
            self.last_block,
        )?);
        writes.append(&mut flush_family(
            Family::Trace,
            &mut self.state.trace,
            &mut self.tail.trace,
            self.last_block,
        )?);
        // Inline writes are durable on return — no barrier needed. The horizon
        // advances unconditionally so a run of empty blocks (or a terminal flush
        // right after a full one) still moves the head; an empty `writes` makes a
        // stray/early flush a safe no-op.
        self.write_all(writes).await?;
        self.progress.set_index_visible(self.last_block);
        Ok(())
    }

    /// Recovery-only: snapshot the full open working set (state + tail). Does NOT
    /// flush fragments or advance the head — so backfill can checkpoint cheaply
    /// mid-run without touching the published head or writing fragments. First
    /// awaits the data track's pack flush for this checkpoint (`data_durable`), so
    /// the snapshot's resume block never outruns durable row data.
    async fn checkpoint(&mut self, data_durable: oneshot::Receiver<()>) -> Result<()> {
        data_durable.await.map_err(|_| {
            MonadChainDataError::Backend("data track stopped before checkpoint".into())
        })?;
        // Snapshot persist is index-side store I/O too.
        let io_start = self.probe.start();
        let result = persist_snapshot(
            &self.snapshots,
            &self.state,
            &self.tail,
            self.last_block,
            self.frontier,
        )
        .await;
        self.probe.record(&self.probe.index_write_ns, io_start);
        result
    }
}

pub(crate) async fn run_index_track<M, B>(
    mut rx: mpsc::Receiver<IngestMsg>,
    mut engine: IndexEngine<M, B>,
    probe: Arc<IngestProbe>,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
{
    loop {
        // Time blocked here = the index track is starved (upstream/itself fast).
        let recv_start = probe.start();
        let msg = rx.recv().await;
        probe.record(&probe.index_recv_blocked_ns, recv_start);
        let Some(msg) = msg else { break };
        // Time here = the index track's own work (accumulate + seal + meta-store put).
        let work_start = probe.start();
        match msg {
            IngestMsg::Block(b) => {
                engine.accumulate(&b)?;
                engine.seal_ready().await?;
            }
            IngestMsg::BatchFlush => engine.batch_flush().await?,
            IngestMsg::Checkpoint(CheckpointHalf::Await(rx)) => engine.checkpoint(rx).await?,
            // Data-side half is never routed to the index track.
            IngestMsg::Checkpoint(CheckpointHalf::Signal(_)) => {}
        }
        probe.record(&probe.index_work_ns, work_start);
    }
    Ok(())
}

/// Seal one family's completed granules: `state[g] ∪ tail[g] → artifact`, removed
/// from both. Reads the live bitmaps; serializes each page once, here. Returns
/// the writes to issue (the caller writes them concurrently).
fn seal_family(
    family: Family,
    state: &mut FamilyState,
    tail: &mut FamilyTail,
    frontier: u64,
) -> Result<Vec<ArtifactWrite>> {
    let from = state.sealed_id;
    let open = seal_boundary(frontier);
    let mut writes = Vec::new();
    if open <= from {
        return Ok(writes); // no new page/bucket boundary completed this block
    }

    // --- bitmap pages: every page fully below the frontier ---
    let ready: BTreeSet<(String, u64)> = state
        .pages
        .keys()
        .chain(tail.pages.keys())
        .filter(|(_, page_global)| page_global + PAGE_SPAN <= frontier)
        .cloned()
        .collect();
    for key in ready {
        let mut bm = state.pages.remove(&key).unwrap_or_default();
        if let Some(t) = tail.pages.remove(&key) {
            bm |= t;
        }
        let (stream_id, page_global) = key;
        let (min_local, max_local, count) = (
            bm.min().unwrap_or(0),
            bm.max().unwrap_or(0),
            bm.len() as u32,
        );
        let bitmap_blob = encode_bitmap_blob(&BitmapBlob {
            min_local,
            max_local,
            count,
            bitmap: bm,
        })?;
        writes.push(ArtifactWrite::Page {
            family,
            stream_id,
            page_start_local: local_page_start(page_global),
            artifact: BitmapPageArtifact {
                meta: BitmapPageMeta {
                    min_local,
                    max_local,
                    count,
                },
                bitmap_blob,
            },
        });
    }

    // --- directory buckets [from, open): same boundary as the pages above ---
    seal_family_dir(family, state, tail, from, open, &mut writes)?;

    state.sealed_id = open;
    // The open page rolled, so its inventory restarts: streams in the new open
    // page must be recorded afresh.
    state.open_streams_seen.clear();
    Ok(writes)
}

/// Flush one family's tail: each touched page/dir entry is written as a delta
/// fragment AND unioned into the carry-over state; the tail is drained. Returns
/// the fragment writes to issue.
fn flush_family(
    family: Family,
    state: &mut FamilyState,
    tail: &mut FamilyTail,
    marker: u64,
) -> Result<Vec<ArtifactWrite>> {
    let mut writes = Vec::new();
    // All drained page keys share the one open (frontier) page — seal removed
    // every page fully below the frontier before this flush, and the open span
    // `[open, frontier)` is narrower than a page (SEAL_SPAN == PAGE_SPAN) — so we
    // record the open page's newly-seen streams once, under that single page.
    let mut open_page: Option<u64> = None;
    let mut new_streams = Vec::new();
    for (key, bm) in std::mem::take(&mut tail.pages) {
        // Carry into the state (union by ref so `bm` survives for the fragment).
        *state.pages.entry(key.clone()).or_default() |= &bm;
        let (stream_id, page_global) = key;
        debug_assert!(
            open_page.map_or(true, |p| p == page_global),
            "flush_family expects a single open page per family; saw {page_global} and {open_page:?}"
        );
        open_page = Some(page_global);
        if !state.open_streams_seen.contains(&stream_id) {
            state.open_streams_seen.insert(stream_id.clone());
            new_streams.push(stream_id.clone());
        }
        let (min_local, max_local, count) = (
            bm.min().unwrap_or(0),
            bm.max().unwrap_or(0),
            bm.len() as u32,
        );
        let blob = encode_bitmap_blob(&BitmapBlob {
            min_local,
            max_local,
            count,
            bitmap: bm,
        })?;
        writes.push(ArtifactWrite::PageFragment {
            family,
            stream_id,
            page_start_local: local_page_start(page_global),
            marker,
            blob,
        });
    }
    if let Some(open_page) = open_page {
        if !new_streams.is_empty() {
            writes.push(ArtifactWrite::OpenStreams {
                family,
                global_page_start: open_page,
                marker,
                streams: new_streams,
            });
        }
    }
    for entry in std::mem::take(&mut tail.dir) {
        writes.push(ArtifactWrite::DirFragment {
            family,
            fragment: PrimaryDirFragment {
                block_number: entry.0,
                first_primary_id: entry.1,
                end_primary_id_exclusive: entry.2,
            },
        });
        state.dir.push_back(entry);
    }
    Ok(writes)
}

// ---------------------------------------------------------------------------
// Per-family accumulation (one routine, per-family extractor)
// ---------------------------------------------------------------------------

/// Insert one block's records for a single family into the tail. Each record is
/// assigned the next sequential primary id starting at `first_pid`; `entries_of`
/// expands it into `(stream_id, local_id)` pairs (the per-family
/// `stream_entries_for_*` extractor), and each pair sets the record's local bit
/// in that stream's open page. A single directory entry spans the block's id
/// range. This is the one routine all three families share — they differ only in
/// `records` and `entries_of` (see [`IndexEngine::accumulate`]).
fn accumulate_family<R, F>(
    tail: &mut FamilyTail,
    first_pid: u64,
    count: u32,
    block: u64,
    records: impl IntoIterator<Item = R>,
    mut entries_of: F,
) -> Result<()>
where
    F: FnMut(R, u64) -> Result<Vec<(String, u32)>>,
{
    let mut id = first_pid;
    for record in records {
        let page_global = global_page_start(id);
        for (stream, local) in entries_of(record, id)? {
            tail.pages
                .entry((stream, page_global))
                .or_default()
                .insert(local);
        }
        id = id.checked_add(1).expect("primary id overflow");
    }
    if count > 0 {
        tail.dir
            .push((block, first_pid, first_pid + u64::from(count)));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Data track (packs row blobs; flushes on threshold + signal)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct PackCfg {
    pub target_bytes: usize,
    pub max_blocks: usize,
}

/// The per-epoch row codecs the data track needs to frame a block's rows.
#[derive(Clone)]
pub struct Codecs {
    pub log: RowCodec,
    pub tx: RowCodec,
    pub trace: RowCodec,
}

impl Codecs {
    pub fn new(log: RowCodec, tx: RowCodec, trace: RowCodec) -> Self {
        Self { log, tx, trace }
    }
}

/// Resolves the row codecs for an epoch (`version`), injected into the engine
/// rather than built into it: making a codec ready can train a dictionary, which
/// reads back the prior epoch's blocks — a reader/Api-level capability the engine
/// must not embed. The Api-backed impl is `ensure_epoch_dicts` + `write_codec`.
pub trait CodecResolver: Send + Sync + 'static {
    /// Resolve (training if necessary) the codecs for `version`, blocking until
    /// ready. The data track calls this lazily, once per epoch boundary. When
    /// [`prewarm`](Self::prewarm) has already trained the dict, this is a
    /// single-flight fast path; otherwise it trains synchronously here.
    fn resolve(&self, version: u32) -> impl Future<Output = Result<Codecs>> + Send;

    /// Fire-and-forget hint that the corpus for `version` is now durable, so its
    /// dictionary can be trained *ahead* of the epoch boundary — turning the
    /// eventual `resolve(version)` into a fast path instead of a multi-second
    /// stall on the hot path. Implementations should kick off training in the
    /// background and return immediately (the Api-backed impl spawns
    /// `ensure_epoch_dicts`, which is single-flight so a later `resolve` coalesces
    /// with it). Default no-op for resolvers that don't pre-train.
    fn prewarm(&self, version: u32) {
        let _ = version;
    }
}

/// One block's fully-encoded persistence artifacts, held until the pack flushes.
/// Everything staging needs is captured here (the source block is long gone by
/// flush time): the combined row blob, the three offset headers, the block
/// record + EVM header, and the tx-hash → location entries.
pub(crate) struct PackEntry {
    pub combined_blob: Vec<u8>,
    pub log_header: BlockBlobHeader,
    pub tx_header: BlockBlobHeader,
    pub trace_header: BlockBlobHeader,
    pub record: BlockRecord,
    pub evm_header: EvmBlockHeader,
    pub hash_locations: Vec<(Hash32, TxLocation)>,
}

struct BlobPacker {
    cfg: PackCfg,
    entries: Vec<PackEntry>,
    last_block: u64,
    bytes: usize,
}

impl BlobPacker {
    fn new(cfg: PackCfg) -> Self {
        Self {
            cfg,
            entries: Vec::new(),
            last_block: 0,
            bytes: 0,
        }
    }
    fn push(&mut self, number: u64, entry: PackEntry) {
        self.bytes += entry.combined_blob.len();
        self.last_block = number;
        self.entries.push(entry);
    }
    fn should_flush(&self) -> bool {
        self.bytes >= self.cfg.target_bytes || self.entries.len() >= self.cfg.max_blocks
    }
    /// Take the held entries and the highest block they cover, resetting the
    /// packer. The physical object + offsets are derived at flush time by the
    /// store's block-blob coalescer, so there is nothing to pack here.
    fn take(&mut self) -> Option<(Vec<PackEntry>, u64)> {
        if self.entries.is_empty() {
            return None;
        }
        let entries = std::mem::take(&mut self.entries);
        let last_block = self.last_block;
        self.bytes = 0;
        Some((entries, last_block))
    }
}

/// Serial data track: encode one block inline at a time. The original impl,
/// kept for A/B comparison. Opt in with `--features serial-data-track`; the
/// default build uses the concurrent [`run_data_track`] below.
#[cfg(feature = "serial-data-track")]
pub(crate) async fn run_data_track<M, B, R>(
    mut rx: mpsc::Receiver<IngestMsg>,
    tables: Arc<Tables<M, B>>,
    resolver: R,
    progress: Arc<Progress>,
    cfg: PackCfg,
    probe: Arc<IngestProbe>,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
    R: CodecResolver,
{
    // Codecs are per-epoch; resolve lazily and cache the current epoch's set. A
    // contiguous run touches one epoch, so this re-resolves only at boundaries —
    // the one place data_durable is allowed to stall (waiting on the new epoch's
    // dictionary), which is correct since rows can't be framed without it.
    let dict_config = tables.dicts().config();
    let epoch_blocks = dict_config.epoch_blocks;
    let sample_span = dict_config.sample_span;
    debug_assert!(epoch_blocks > 0, "epoch_blocks must be positive");
    let mut packer = BlobPacker::new(cfg);
    let mut codecs: Option<(u32, Codecs)> = None;
    let mut last_prewarmed = 0u32;
    loop {
        // Time blocked here = the data track is starved (upstream/itself fast).
        let recv_start = probe.start();
        let msg = rx.recv().await;
        probe.record(&probe.data_recv_blocked_ns, recv_start);
        let Some(msg) = msg else { break };
        match msg {
            IngestMsg::Block(b) => {
                let version = u32::try_from(b.number / epoch_blocks)
                    .map_err(|_| MonadChainDataError::Decode("dict epoch/version overflow"))?;
                if codecs.as_ref().map(|(v, _)| *v) != Some(version) {
                    // Crossing into a new epoch. Flush first so every prior-epoch
                    // block is durable before the resolver may read them back to
                    // train this epoch's dictionary — otherwise training could run
                    // on a timing-dependent subset of the prior epoch and produce
                    // a nondeterministic dict (no-op on the first block: the packer
                    // is empty). Then verify the resolver actually returned codecs
                    // for the requested epoch (a wrong version would silently
                    // mis-stamp dict_version on every header of this epoch).
                    flush_pack(&tables, &mut packer, &progress, &probe).await?;
                    let resolved = resolver.resolve(version).await?;
                    debug_assert!(
                        resolved.log.version() == version
                            && resolved.tx.version() == version
                            && resolved.trace.version() == version,
                        "resolver returned codecs for the wrong epoch: requested {version}, \
                         got log={} tx={} trace={}",
                        resolved.log.version(),
                        resolved.tx.version(),
                        resolved.trace.version(),
                    );
                    codecs = Some((version, resolved));
                }
                let encode_start = probe.start();
                let entry = {
                    let _span =
                        tracing::trace_span!(target: TIMING_TARGET, "encode_pack").entered();
                    encode_pack_entry(&b, &codecs.as_ref().expect("just resolved").1)?
                };
                probe.record(&probe.data_encode_ns, encode_start);
                packer.push(b.number, entry);
                if packer.should_flush() {
                    flush_pack(&tables, &mut packer, &progress, &probe).await?;
                }
            }
            // BatchFlush: flush so data_durable reaches the tip before publish.
            IngestMsg::BatchFlush => {
                flush_pack(&tables, &mut packer, &progress, &probe).await?;
            }
            // Checkpoint: flush, then signal the index track that the row data for
            // this checkpoint is durable so it may persist the snapshot.
            IngestMsg::Checkpoint(half) => {
                flush_pack(&tables, &mut packer, &progress, &probe).await?;
                if let CheckpointHalf::Signal(done) = half {
                    let _ = done.send(());
                }
            }
        }
        // After any flush above may have advanced the durable frontier, kick off
        // pre-training of the next epoch's dictionary if its corpus is now durable
        // (keyed on the durable frontier, not the current block, because packs
        // flush behind the tip). Fire-and-forget; the boundary `resolve` coalesces.
        maybe_prewarm(
            &resolver,
            progress.data_durable(),
            epoch_blocks,
            sample_span,
            &mut last_prewarmed,
        );
    }
    flush_pack(&tables, &mut packer, &progress, &probe).await?;
    Ok(())
}

/// Concurrent data track (default): `encode_pack_entry` (CPU-bound zstd framing)
/// runs on spawned blocking tasks, up to `encode_concurrency` at once, with
/// results drained in strict block order into the packer. Profiling showed the
/// single-threaded encode was the ceiling once fetch was parallelized.
///
/// Ordering is preserved because every barrier — an epoch codec change,
/// `BatchFlush`, `Checkpoint`, and end-of-stream — first drains ALL in-flight
/// encodes into the packer, so a flush never makes `data_durable` claim a block
/// whose rows aren't packed yet. A mid-stream `should_flush` only flushes the
/// already-drained contiguous prefix, so it needs no full drain. Opt out with
/// `--features serial-data-track`.
#[cfg(not(feature = "serial-data-track"))]
pub(crate) async fn run_data_track<M, B, R>(
    mut rx: mpsc::Receiver<IngestMsg>,
    tables: Arc<Tables<M, B>>,
    resolver: R,
    progress: Arc<Progress>,
    cfg: PackCfg,
    probe: Arc<IngestProbe>,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
    R: CodecResolver,
{
    let dict_config = tables.dicts().config();
    let epoch_blocks = dict_config.epoch_blocks;
    let sample_span = dict_config.sample_span;
    debug_assert!(epoch_blocks > 0, "epoch_blocks must be positive");
    // Encode is pure CPU; cap parallelism at the core count.
    let encode_concurrency = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(1);
    let mut packer = BlobPacker::new(cfg);
    let mut codecs: Option<(u32, Codecs)> = None;
    let mut last_prewarmed = 0u32;
    // In-flight encodes, yielded in submission (block) order by FuturesOrdered.
    let mut inflight: FuturesOrdered<tokio::task::JoinHandle<Result<PackEntry>>> =
        FuturesOrdered::new();
    loop {
        // Time blocked here = the data track is starved (upstream/itself fast).
        let recv_start = probe.start();
        let msg = rx.recv().await;
        probe.record(&probe.data_recv_blocked_ns, recv_start);
        let Some(msg) = msg else { break };
        match msg {
            IngestMsg::Block(b) => {
                let version = u32::try_from(b.number / epoch_blocks)
                    .map_err(|_| MonadChainDataError::Decode("dict epoch/version overflow"))?;
                if codecs.as_ref().map(|(v, _)| *v) != Some(version) {
                    // Epoch barrier: every prior-epoch encode must be packed and
                    // flushed durable before the resolver may read those blocks
                    // back to train this epoch's dict (see the serial impl for the
                    // full rationale). No-op on the first block (packer empty).
                    drain_encodes(&mut inflight, &mut packer).await?;
                    flush_pack(&tables, &mut packer, &progress, &probe).await?;
                    let resolved = resolver.resolve(version).await?;
                    debug_assert!(
                        resolved.log.version() == version
                            && resolved.tx.version() == version
                            && resolved.trace.version() == version,
                        "resolver returned codecs for the wrong epoch: requested {version}, \
                         got log={} tx={} trace={}",
                        resolved.log.version(),
                        resolved.tx.version(),
                        resolved.trace.version(),
                    );
                    codecs = Some((version, resolved));
                }
                // Spawn the (CPU-bound) encode; `Arc<AssignedBlock>` + cloned
                // codecs make the task 'static. Aggregate encode busy is recorded
                // inside the task, so the probe's data_encode can exceed 100% of
                // wall (multi-core), like fetch_decode_busy.
                let block_codecs = codecs.as_ref().expect("just resolved").1.clone();
                let block = b.clone();
                let task_probe = probe.clone();
                inflight.push_back(tokio::task::spawn_blocking(move || {
                    let encode_start = task_probe.start();
                    let entry = encode_pack_entry(&block, &block_codecs);
                    task_probe.record(&task_probe.data_encode_ns, encode_start);
                    entry
                }));
                // Keep at most `encode_concurrency` encodes in flight; drain the
                // oldest (block order) when full, flushing on the size threshold.
                while inflight.len() >= encode_concurrency {
                    pop_encode(&mut inflight, &mut packer).await?;
                    if packer.should_flush() {
                        flush_pack(&tables, &mut packer, &progress, &probe).await?;
                    }
                }
            }
            // BatchFlush: flush so data_durable reaches the tip before publish —
            // but only after every in-flight encode is packed.
            IngestMsg::BatchFlush => {
                drain_encodes(&mut inflight, &mut packer).await?;
                flush_pack(&tables, &mut packer, &progress, &probe).await?;
            }
            // Checkpoint: drain + flush, then signal the index track that the row
            // data for this checkpoint is durable so it may persist the snapshot.
            IngestMsg::Checkpoint(half) => {
                drain_encodes(&mut inflight, &mut packer).await?;
                flush_pack(&tables, &mut packer, &progress, &probe).await?;
                if let CheckpointHalf::Signal(done) = half {
                    let _ = done.send(());
                }
            }
        }
        maybe_prewarm(
            &resolver,
            progress.data_durable(),
            epoch_blocks,
            sample_span,
            &mut last_prewarmed,
        );
    }
    drain_encodes(&mut inflight, &mut packer).await?;
    flush_pack(&tables, &mut packer, &progress, &probe).await?;
    Ok(())
}

/// Await the oldest in-flight encode (FuturesOrdered yields in block order) and
/// push it to the packer. Used only by the concurrent data track.
#[cfg(not(feature = "serial-data-track"))]
async fn pop_encode(
    inflight: &mut FuturesOrdered<tokio::task::JoinHandle<Result<PackEntry>>>,
    packer: &mut BlobPacker,
) -> Result<()> {
    if let Some(joined) = inflight.next().await {
        let entry = joined.map_err(task_join_err)??;
        packer.push(entry.record.block_number, entry);
    }
    Ok(())
}

/// Drain all in-flight encodes into the packer, preserving block order. Called at
/// every ordering barrier so the subsequent flush covers a contiguous prefix.
#[cfg(not(feature = "serial-data-track"))]
async fn drain_encodes(
    inflight: &mut FuturesOrdered<tokio::task::JoinHandle<Result<PackEntry>>>,
    packer: &mut BlobPacker,
) -> Result<()> {
    while let Some(joined) = inflight.next().await {
        let entry = joined.map_err(task_join_err)??;
        packer.push(entry.record.block_number, entry);
    }
    Ok(())
}

/// Pre-train trigger: once the leading `sample_span` blocks of the current
/// durable epoch are durable, the corpus for the NEXT epoch's dictionary is
/// complete, so hint the resolver to train it ahead of the boundary. Mirrors the
/// legacy background pre-train (`n % epoch_blocks >= sample_span`), but keyed on
/// the durable frontier so the sampled blocks are guaranteed persisted before
/// training reads them back. Idempotent per epoch via `last_prewarmed`.
fn maybe_prewarm<R: CodecResolver>(
    resolver: &R,
    data_durable: u64,
    epoch_blocks: u64,
    sample_span: u64,
    last_prewarmed: &mut u32,
) {
    if epoch_blocks == 0 || data_durable % epoch_blocks < sample_span {
        return;
    }
    let next = data_durable / epoch_blocks + 1;
    let Ok(next) = u32::try_from(next) else {
        return;
    };
    if next > *last_prewarmed {
        *last_prewarmed = next;
        resolver.prewarm(next);
    }
}

async fn flush_pack<M, B>(
    tables: &Tables<M, B>,
    packer: &mut BlobPacker,
    progress: &Progress,
    probe: &IngestProbe,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
{
    let Some((entries, last_block)) = packer.take() else {
        return Ok(());
    };
    // One `with_writes` for the whole pack: the store's block-blob coalescer
    // turns the per-block blobs into physical objects and stamps each header's
    // physical_key/offset, so durability of the pack == durability of the head.
    let flush_start = probe.start();
    let span = tracing::trace_span!(target: TIMING_TARGET, "flush_pack", last_block);
    stage_pack(tables, entries).instrument(span).await?;
    probe.record(&probe.data_flush_ns, flush_start);
    progress.set_data_durable(last_block);
    Ok(())
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

/// Where ingest recovery snapshots live: a single meta KV row. Single-writer
/// (the index track at `checkpoint`) and single-reader (`recover` at startup),
/// so plain `get`/`put` — no CAS.
#[derive(Clone)]
pub struct SnapshotStore<M: MetaStore> {
    meta: M,
}

impl<M: MetaStore> SnapshotStore<M> {
    const TABLE: TableId = TableId::new("ingest_snapshot");
    const KEY: &'static [u8] = b"latest";

    pub fn new(meta: M) -> Self {
        Self { meta }
    }

    pub(crate) async fn load(&self) -> Result<Option<Bytes>> {
        self.meta.get(Self::TABLE, Self::KEY).await
    }

    pub(crate) async fn store(&self, bytes: Bytes) -> Result<()> {
        self.meta.put(Self::TABLE, Self::KEY, bytes).await
    }
}

// ---------------------------------------------------------------------------
// Wiring shared by both pipelines
// ---------------------------------------------------------------------------

/// Spawn the producer (fetch + assign + signals), the data track, the index
/// engine, and the publisher around a fetch source + signal policy. The index
/// engine writes artifacts inline, so there is no separate artifact-writer task.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_ingest<S, M, B, R>(
    source: S,
    tables: Arc<Tables<M, B>>,
    publisher: Arc<HeadPublisher<M>>,
    snapshots: SnapshotStore<M>,
    resolver: R,
    // Absolute end ceiling, or a resume-relative `count` resolved to one once the
    // begin block is known. `None`/`None` ⇒ follow the tip forever.
    stop_at: Option<u64>,
    count: Option<u64>,
    poll_ms: u64,
    cold_start: u64,
    policy: SignalPolicy,
    pack_cfg: PackCfg,
    track_buffer: usize,
    prefetch: Prefetch,
) -> Result<()>
where
    S: ChainDataIngestSource,
    M: MetaStore,
    B: BlobStore,
    R: CodecResolver,
{
    // A zero cadence would make `since_ckpt >= checkpoint_every_blocks` true on
    // every block (checkpoint after each block); callers must pass a positive
    // interval. The flush cadences are validated upstream (close/catchup >= 1).
    debug_assert!(
        policy.checkpoint_every_blocks > 0,
        "checkpoint_every_blocks must be positive"
    );

    let progress = Arc::new(Progress::default());

    // Read the authoritative published head before recovery so the fragment
    // rebuild path keys off the reader-visible watermark.
    let published = publisher.published_head().await?;

    // Resume from max(checkpoint, published head): the checkpoint wins for backfill
    // (its OpenTail runs ahead of the head); the fragments win for live (the head
    // runs ahead of the rare checkpoint). `begin` is the first block to fetch.
    let (state, tail, frontier, resume, durable_floor, begin) =
        match crate::ingest_recover::recover(&tables, &snapshots, published).await? {
            crate::ingest_recover::Recovered::Warm {
                state,
                tail,
                frontier,
                resume,
                durable_floor,
            } => (
                state,
                tail,
                frontier,
                resume,
                durable_floor,
                (resume + 1).max(cold_start),
            ),
            crate::ingest_recover::Recovered::Cold => (
                OpenState::default(),
                OpenTail::default(),
                FamilyFrontier::default(),
                cold_start.saturating_sub(1),
                0,
                cold_start,
            ),
        };

    // The data packs are durable through `durable_floor` (the resume block: a
    // checkpoint persists only after the data flush, and the rebuild head is a
    // flush boundary). Seed the data frontier so a restart with nothing left to
    // ingest can still publish its already-durable tail. `index_visible` is NOT
    // seeded — it only reflects fragments actually re-written by `batch_flush`, so
    // the published head can never cover fragments that aren't durable.
    progress.set_data_durable(durable_floor);

    // Now that the begin block is known, resolve the stop ceiling. `count` is
    // measured from the resume point, so it can only be turned into an absolute end
    // here; an explicit `stop_at` wins. At most one of stop_at/count is set
    // (validated upstream). Neither ⇒ follow the tip forever.
    let stop_at = match (stop_at, count) {
        (Some(end), _) => Some(end),
        (None, Some(count)) => Some(begin.saturating_add(count).saturating_sub(1)),
        (None, None) => None,
    };

    let (tx_data, rx_data) = mpsc::channel(track_buffer);
    let (tx_index, rx_index) = mpsc::channel(track_buffer);
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Targeted timing probe (off unless `ingest::timing=trace`). The reporter is
    // spawned only when enabled and aborted on exit, like the clock refresher.
    let probe = IngestProbe::new();
    let _timing_guard = probe
        .enabled
        .then(|| AbortOnDrop(tokio::spawn(run_timing_reporter(probe.clone()))));

    let engine = IndexEngine::new(
        state,
        tail,
        frontier,
        resume,
        tables.clone(),
        progress.clone(),
        snapshots,
        probe.clone(),
    );

    let sig = Signaller {
        tx_data,
        tx_index,
        frontier,
        policy,
        since_flush: 0,
        since_ckpt: 0,
        last_block: resume,
        observed_tip: None,
        probe: probe.clone(),
    };

    // The three pipeline tasks share a JoinSet so the first error or panic aborts
    // the rest — otherwise a seal error early in a long backfill would surface
    // only after the producer fetched the whole range. The publisher is kept
    // separate so it can always be shut down gracefully (final head publish).
    let mut pipeline = JoinSet::new();
    pipeline.spawn(run_producer(source, begin, stop_at, poll_ms, prefetch, sig));
    pipeline.spawn(run_data_track(
        rx_data,
        tables.clone(),
        resolver,
        progress.clone(),
        pack_cfg,
        probe.clone(),
    ));
    pipeline.spawn(run_index_track(rx_index, engine, probe));
    let publisher = tokio::spawn(run_publisher(
        progress.clone(),
        publisher,
        published,
        shutdown_rx,
    ));

    // Supervise the pipeline AND the publisher together. The publisher should not
    // resolve on its own unless its plain head write fails or it panics. A clean
    // pipeline drain triggers the publisher's graceful final publish.
    let mut publisher = publisher;
    let mut outcome = Ok(());
    loop {
        tokio::select! {
            joined = pipeline.join_next() => {
                match joined {
                    // Pipeline fully drained (bounded backfill complete): fall
                    // through to the graceful publisher shutdown below.
                    None => break,
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(e))) => {
                        if outcome.is_ok() {
                            outcome = Err(e);
                            pipeline.abort_all();
                        }
                    }
                    // A task we aborted after capturing the first failure.
                    Some(Err(e)) if e.is_cancelled() => {}
                    Some(Err(e)) => {
                        if outcome.is_ok() {
                            outcome = Err(MonadChainDataError::Backend(format!(
                                "ingest task panicked: {e}"
                            )));
                            pipeline.abort_all();
                        }
                    }
                }
            }
            pub_res = &mut publisher => {
                // The publisher resolved before we asked it to shut down: tear
                // down the pipeline and finish.
                let pub_err = match pub_res {
                    Ok(Ok(())) => None,
                    Ok(Err(e)) => Some(e),
                    Err(e) => Some(MonadChainDataError::Backend(format!(
                        "publisher task panicked: {e}"
                    ))),
                };
                if let Some(e) = pub_err {
                    if outcome.is_ok() {
                        outcome = Err(e);
                    }
                }
                pipeline.abort_all();
                while pipeline.join_next().await.is_some() {}
                return outcome; // publisher already consumed
            }
        }
    }

    // Graceful shutdown: on success the publisher publishes the final head; on a
    // pipeline failure it publishes whatever is already durable.
    shutdown_tx.send(()).await.ok();
    let pub_outcome = match publisher.await {
        Ok(r) => r,
        Err(e) => Err(MonadChainDataError::Backend(format!(
            "publisher task panicked: {e}"
        ))),
    };

    outcome.and(pub_outcome)
}

/// Seal one family's directory buckets over `[from, open)` (both multiples of
/// [`SEAL_SPAN`], supplied by [`seal_family`] which owns the shared frontier).
/// Each bucket is compacted from the entries overlapping it across `state.dir`
/// (carry-over) and `tail.dir` (current); a block whose id range straddles a
/// boundary is filed in every bucket it overlaps. Consumed entries — those that
/// can no longer overlap the open bucket — are then drained from the front of
/// both. The bucket writes are pushed onto `writes`; the leaf I/O lives in
/// [`crate::ingest_helpers`].
fn seal_family_dir(
    family: Family,
    state: &mut FamilyState,
    tail: &mut FamilyTail,
    from: u64,
    open: u64,
    writes: &mut Vec<ArtifactWrite>,
) -> Result<()> {
    let mut bs = from;
    while bs < open {
        let bucket = compact_dir_bucket(&state.dir, &tail.dir, bs)?;
        writes.push(ArtifactWrite::Bucket {
            family,
            bucket_start: bs,
            bucket,
        });
        bs += BUCKET_SPAN;
    }
    // Everything ending at/below the open bucket start now lives in a sealed
    // bucket artifact; drop it. Straddlers reaching into the open bucket
    // (`end > open`) stay — they're the open bucket's first entry and still get
    // fragmented at the next batch_flush.
    drain_sealed_dir_entries(state, tail, open);
    Ok(())
}

/// Compact the directory bucket `[bs, bs + BUCKET_SPAN)` from the entries that
/// overlap it across the carry-over (`state_dir`) then current (`tail_dir`)
/// chains. The two are a single ascending, id-contiguous sequence, so the
/// overlap is a contiguous run; the bucket's sentinel is the last overlapping
/// entry's `end` (which may exceed the bucket span for a straddler — expected).
fn compact_dir_bucket(
    state_dir: &VecDeque<(u64, u64, u64)>,
    tail_dir: &[(u64, u64, u64)],
    bs: u64,
) -> Result<PrimaryDirBucket> {
    let be = bs.saturating_add(BUCKET_SPAN);
    let mut entries = Vec::new();
    let mut sentinel = bs;
    let mut prev: Option<(u64, u64)> = None; // (block, end)
    for &(block, first, end) in state_dir.iter().chain(tail_dir.iter()) {
        if first >= be {
            break; // sorted by first id: no later entry can overlap
        }
        if end <= bs {
            continue; // overlaps only earlier (already sealed) buckets
        }
        if let Some((prev_block, prev_end)) = prev {
            // Defensive: our own id assignment must produce a contiguous,
            // strictly-increasing chain. A break here is an ingest bug, not data.
            if block <= prev_block {
                return Err(MonadChainDataError::Decode(
                    "inconsistent primary directory bucket block sequence",
                ));
            }
            if prev_end != first {
                return Err(MonadChainDataError::Decode(
                    "inconsistent primary directory bucket primary-id sequence",
                ));
            }
        }
        entries.push(PrimaryDirEntry {
            block_number: block,
            first_primary_id: first,
        });
        sentinel = end;
        prev = Some((block, end));
    }
    if entries.is_empty() {
        // A sealed 64K span always had its ids minted by some block.
        return Err(MonadChainDataError::MissingData(
            "sealed primary directory bucket has no overlapping entries",
        ));
    }
    PrimaryDirBucket::new(entries, sentinel)
}

/// Drop directory entries that overlap only sealed buckets — i.e. those ending
/// at or below `open_bucket_start`. Because `end` is monotonic across the
/// `state.dir` → `tail.dir` chain, the drop set is a prefix; `tail.dir` is only
/// touched once `state.dir` is exhausted.
fn drain_sealed_dir_entries(
    state: &mut FamilyState,
    tail: &mut FamilyTail,
    open_bucket_start: u64,
) {
    while state
        .dir
        .front()
        .is_some_and(|&(_, _, end)| end <= open_bucket_start)
    {
        state.dir.pop_front();
    }
    if state.dir.is_empty() {
        let cut = tail
            .dir
            .partition_point(|&(_, _, end)| end <= open_bucket_start);
        tail.dir.drain(0..cut);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    const SPAN: u64 = BUCKET_SPAN; // 2^16 after page/bucket alignment

    fn entry(block: u64, first: u64, end: u64) -> (u64, u64, u64) {
        (block, first, end)
    }

    fn bucket_entries(b: &PrimaryDirBucket) -> Vec<(u64, u64)> {
        b.entries
            .iter()
            .map(|e| (e.block_number, e.first_primary_id))
            .collect()
    }

    #[test]
    fn compact_single_bucket_from_tail() {
        let state: VecDeque<_> = VecDeque::new();
        let tail = vec![entry(1, 0, 30_000), entry(2, 30_000, 60_000)];
        let bucket = compact_dir_bucket(&state, &tail, 0).expect("compact");
        assert_eq!(bucket_entries(&bucket), vec![(1, 0), (2, 30_000)]);
        assert_eq!(bucket.end_primary_id_exclusive, 60_000);
    }

    #[test]
    fn straddler_appears_in_both_buckets() {
        // Block 3 spans the 2^16 boundary: id range [65_000, 70_000).
        let state: VecDeque<_> = VecDeque::new();
        let tail = vec![
            entry(1, 0, 30_000),
            entry(2, 30_000, 65_000),
            entry(3, 65_000, 70_000),
        ];

        // Bucket 0: straddler is the last entry; sentinel exceeds the span.
        let b0 = compact_dir_bucket(&state, &tail, 0).expect("bucket 0");
        assert_eq!(bucket_entries(&b0), vec![(1, 0), (2, 30_000), (3, 65_000)]);
        assert_eq!(b0.end_primary_id_exclusive, 70_000);

        // Bucket 1: straddler is the first entry, with first_id < bucket_start.
        let b1 = compact_dir_bucket(&state, &tail, SPAN).expect("bucket 1");
        assert_eq!(bucket_entries(&b1), vec![(3, 65_000)]);
        assert!(b1.entries[0].first_primary_id < SPAN);
        assert_eq!(b1.end_primary_id_exclusive, 70_000);
    }

    #[test]
    fn compaction_spans_state_tail_seam() {
        // Carry-over in state.dir, current batch in tail.dir; the chain (and a
        // straddler) must compact identically across the seam.
        let state: VecDeque<_> =
            VecDeque::from(vec![entry(1, 0, 30_000), entry(2, 30_000, 65_000)]);
        let tail = vec![entry(3, 65_000, 70_000)];
        let b0 = compact_dir_bucket(&state, &tail, 0).expect("bucket 0");
        assert_eq!(bucket_entries(&b0), vec![(1, 0), (2, 30_000), (3, 65_000)]);
        assert_eq!(b0.end_primary_id_exclusive, 70_000);
    }

    #[test]
    fn compaction_rejects_id_gap() {
        let state: VecDeque<_> = VecDeque::new();
        let tail = vec![entry(1, 0, 100), entry(2, 105, 200)]; // 100 != 105
        let err = compact_dir_bucket(&state, &tail, 0).expect_err("id gap");
        assert!(err.to_string().contains("primary-id sequence"));
    }

    #[test]
    fn compaction_rejects_out_of_order_blocks() {
        let state: VecDeque<_> = VecDeque::new();
        let tail = vec![entry(9, 0, 100), entry(7, 100, 200)];
        let err = compact_dir_bucket(&state, &tail, 0).expect_err("block order");
        assert!(err.to_string().contains("block sequence"));
    }

    #[test]
    fn empty_sealed_bucket_is_an_error() {
        let state: VecDeque<_> = VecDeque::new();
        let tail: Vec<(u64, u64, u64)> = vec![];
        compact_dir_bucket(&state, &tail, 0).expect_err("no entries");
    }

    #[test]
    fn drain_keeps_straddler_drops_sealed_across_seam() {
        let mut state = FamilyState {
            dir: VecDeque::from(vec![entry(1, 0, 30_000), entry(2, 30_000, 65_000)]),
            ..FamilyState::default()
        };
        let mut tail = FamilyTail {
            dir: vec![entry(3, 65_000, 70_000)],
            ..FamilyTail::default()
        };
        drain_sealed_dir_entries(&mut state, &mut tail, SPAN); // open bucket starts at 2^16
        assert!(state.dir.is_empty(), "fully-sealed carry-over dropped");
        assert_eq!(tail.dir, vec![entry(3, 65_000, 70_000)], "straddler kept");
    }

    #[test]
    fn drain_stops_at_first_open_entry_in_state() {
        let mut state = FamilyState {
            // entry 2 reaches into the open bucket, so neither it nor anything
            // after it (incl. all of tail) may be dropped.
            dir: VecDeque::from(vec![entry(1, 0, 60_000), entry(2, 60_000, 70_000)]),
            ..FamilyState::default()
        };
        let mut tail = FamilyTail {
            dir: vec![entry(3, 70_000, 80_000)],
            ..FamilyTail::default()
        };
        drain_sealed_dir_entries(&mut state, &mut tail, SPAN);
        assert_eq!(state.dir, VecDeque::from(vec![entry(2, 60_000, 70_000)]));
        assert_eq!(tail.dir, vec![entry(3, 70_000, 80_000)]);
    }

    #[test]
    fn seal_family_dir_seals_two_buckets_and_retains_open_straddler() {
        // Frontier 140_000 → open bucket starts at 2*2^16 = 131_072; buckets 0
        // and 1 seal. Block 5 reaches into the open bucket and must survive.
        let mut state = FamilyState::default();
        let mut tail = FamilyTail {
            dir: vec![
                entry(1, 0, 30_000),
                entry(2, 30_000, 65_000),
                entry(3, 65_000, 70_000),    // straddles bucket 0 / 1
                entry(4, 70_000, 2 * SPAN),  // fills bucket 1 to its end
                entry(5, 2 * SPAN, 140_000), // straddles bucket 1 / open
            ],
            ..FamilyTail::default()
        };
        let mut writes = Vec::new();
        seal_family_dir(Family::Log, &mut state, &mut tail, 0, 2 * SPAN, &mut writes)
            .expect("seal");

        let sealed: Vec<u64> = writes
            .iter()
            .map(|w| match w {
                ArtifactWrite::Bucket { bucket_start, .. } => *bucket_start,
                _ => panic!("expected bucket writes"),
            })
            .collect();
        assert_eq!(sealed, vec![0, SPAN]);

        // Only the block straddling into the open bucket remains.
        assert!(state.dir.is_empty());
        assert_eq!(tail.dir, vec![entry(5, 2 * SPAN, 140_000)]);
    }

    #[test]
    fn accumulate_family_sets_locals_and_one_dir_entry() {
        // 3 records starting at primary id 5, each contributing one stream; the
        // generic loop assigns sequential ids and a single spanning dir entry.
        let mut tail = FamilyTail::default();
        accumulate_family(&mut tail, 5, 3, 42, 0..3u64, |_record, id| {
            Ok(vec![("s".to_string(), PrimaryId::new(id).local())])
        })
        .expect("accumulate");

        assert_eq!(tail.dir, vec![(42, 5, 8)]);
        let page = global_page_start(5);
        let bm = tail.pages.get(&("s".to_string(), page)).expect("page");
        assert_eq!(bm.iter().collect::<Vec<_>>(), vec![5, 6, 7]);
    }

    #[test]
    fn accumulate_family_unions_multiple_streams_per_record() {
        // A record contributing several streams sets its local in each.
        let mut tail = FamilyTail::default();
        accumulate_family(&mut tail, 0, 1, 1, std::iter::once(()), |(), id| {
            let local = PrimaryId::new(id).local();
            Ok(vec![("a".to_string(), local), ("b".to_string(), local)])
        })
        .expect("accumulate");

        let page = global_page_start(0);
        for stream in ["a", "b"] {
            let bm = tail.pages.get(&(stream.to_string(), page)).expect("page");
            assert_eq!(bm.iter().collect::<Vec<_>>(), vec![0]);
        }
    }

    #[test]
    fn accumulate_family_empty_pushes_no_dir_entry() {
        let mut tail = FamilyTail::default();
        accumulate_family(&mut tail, 0, 0, 1, std::iter::empty::<u64>(), |_, _| {
            Ok(Vec::new())
        })
        .expect("accumulate");
        assert!(tail.dir.is_empty());
        assert!(tail.pages.is_empty());
    }

    #[test]
    fn maybe_prewarm_fires_once_per_epoch_when_corpus_durable() {
        use std::sync::Mutex;

        struct Fake(Mutex<Vec<u32>>);
        impl CodecResolver for Fake {
            fn resolve(&self, _v: u32) -> impl Future<Output = Result<Codecs>> + Send {
                // Never called by maybe_prewarm; present only to satisfy the trait.
                async { Err(MonadChainDataError::Backend("unused".into())) }
            }
            fn prewarm(&self, version: u32) {
                self.0.lock().unwrap().push(version);
            }
        }

        let fake = Fake(Mutex::new(Vec::new()));
        let mut last = 0u32;
        let (epoch_blocks, sample_span) = (100u64, 60u64);
        let fired = || fake.0.lock().unwrap().clone();

        // Before the sampling window completes: no pre-train.
        maybe_prewarm(&fake, 59, epoch_blocks, sample_span, &mut last);
        assert!(fired().is_empty());
        // Corpus for epoch 1 complete (durable pos 60 >= 60): pre-train epoch 1.
        maybe_prewarm(&fake, 60, epoch_blocks, sample_span, &mut last);
        assert_eq!(fired(), vec![1]);
        // Further durable progress in the same epoch: no repeat.
        maybe_prewarm(&fake, 99, epoch_blocks, sample_span, &mut last);
        assert_eq!(fired(), vec![1]);
        // Next epoch's corpus complete: pre-train epoch 2.
        maybe_prewarm(&fake, 160, epoch_blocks, sample_span, &mut last);
        assert_eq!(fired(), vec![1, 2]);
    }

    #[test]
    fn accumulate_family_propagates_extractor_error() {
        // A fallible extractor (e.g. tx envelope decode) aborts accumulation.
        let mut tail = FamilyTail::default();
        let err = accumulate_family(&mut tail, 0, 1, 1, 0..1u64, |_, _| {
            Err(MonadChainDataError::Decode("boom"))
        })
        .expect_err("extractor error propagates");
        assert!(err.to_string().contains("boom"));
    }
}
