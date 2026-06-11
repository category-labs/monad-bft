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

//! Shared ingest engine.
//!
//! ```text
//! producer (fetch, assign ids, emit signals) ─┬─► data track  (pack rows, write blobs)
//!                                              └─► index engine (accumulate, seal, flush, write)
//!                  publisher: head = newest flush boundary <= data_durable
//! ```
//!
//! The index engine is branchless across modes: bits accumulate in [`OpenTail`];
//! `seal` continuously writes completed granules (`OpenState[g] ∪ OpenTail[g]`);
//! `batch_flush` writes each `OpenTail` entry as a fragment, carries it into
//! [`OpenState`], and records the tip as a publishable flush boundary;
//! `checkpoint` snapshots both once the data track's flush is durable — it
//! neither writes fragments nor records a boundary. Artifacts are written
//! inline and durable on return, so `batch_flush` records the boundary without
//! a sink barrier.
//!
//! There is no backfill/live mode: the engine always follows the tip ("backfill"
//! is just the catch-up phase); the [`SignalPolicy`] cadences are the only knobs,
//! and `end` is a run bound, not a mode.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};

use crate::{
    engine::{
        digest::{ChainDigest, EMPTY_DIGEST},
        family::PerFamily,
        tables::{PublicationTables, Tables},
    },
    error::{MonadChainDataError, Result},
    ingest_types::FinalizedBlock,
    primitives::records::{LogId, PrimaryId, TraceId, TxId},
    store::{BlobStore, MetaStore},
};

pub mod data_track;
pub mod index;
pub(crate) mod probe;
pub mod producer;
pub mod publisher;
pub mod recover;
pub mod resolver;
#[cfg(test)]
mod rtt;
pub mod snapshot;
pub mod source;

use data_track::run_data_track;
pub use data_track::{CodecResolver, Codecs, PackConfig, PayloadMode};
use index::{run_index_track, IndexEngine, OpenState, OpenTail};
use probe::{run_timing_reporter, IngestProbe};
use producer::{run_producer, Signaller};
pub use producer::{Prefetch, SignalPolicy};
use publisher::{run_publisher, Progress};
pub use snapshot::SnapshotStore;
pub use source::ChainDataIngestSource;

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

/// Per-family next-primary-id frontier.
pub type FamilyFrontier = PerFamily<u64>;

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

/// Stream consumed by both the data track and the index engine. `C` is the
/// track's half of the `Checkpoint` rendezvous (see [`DataMsg`]/[`IndexMsg`]),
/// so misrouting a half is a compile error rather than a dead match arm.
pub(crate) enum IngestMsg<C> {
    Block(Arc<AssignedBlock>),
    BatchFlush,
    /// Cross-track durability rendezvous: the data track signals its half once
    /// its pack flush is durable; the index track awaits that before persisting
    /// the snapshot, so the snapshot's resume block never outruns durable rows.
    Checkpoint(C),
}

/// What the data track consumes: it signals checkpoint durability.
pub(crate) type DataMsg = IngestMsg<oneshot::Sender<()>>;
/// What the index track consumes: it awaits checkpoint durability.
pub(crate) type IndexMsg = IngestMsg<oneshot::Receiver<()>>;

/// Surface a spawned task's panic/cancellation as a backend error so the
/// pipeline aborts rather than silently dropping a block.
pub(crate) fn task_join_err(e: tokio::task::JoinError) -> MonadChainDataError {
    MonadChainDataError::Backend(format!("ingest spawned task: {e}"))
}

/// Aborts a spawned task when dropped, so it can't outlive its owner on any
/// return path. Awaiting yields the join result, so it stands in for the bare
/// `JoinHandle` inside an ordered stream (the producer's fetch window).
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = std::result::Result<T, tokio::task::JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}

/// Run knobs for [`run_ingest`]. There is one behaviour — always follow the
/// tip; "backfill" is just the catch-up phase, and `end`/`count` is a run
/// bound, not a mode.
#[derive(Debug, Clone, Copy)]
pub struct IngestRunConfig {
    /// Cold-start floor: the begin block when the store is empty. A warm resume
    /// overrides it with the snapshot's `last_block + 1`.
    pub start: u64,
    /// Inclusive absolute end ceiling. At most one of `end`/`count` is set;
    /// neither ⇒ follow the tip forever.
    pub end: Option<u64>,
    /// Block count from the begin (resume) block; resolved to an absolute
    /// `end` once the begin block is known.
    pub count: Option<u64>,
    /// `BatchFlush`/`Checkpoint` cadences (see [`SignalPolicy`]).
    pub policy: SignalPolicy,
    /// Data-track pack sizing (see [`PackConfig`]).
    pub pack: PackConfig,
    /// Where row payloads live (see [`PayloadMode`]). A store must be
    /// ingested under ONE mode for its whole life; the headers are
    /// per-block, but mixing modes has no supported migration story.
    pub payload: PayloadMode,
    pub track_buffer: usize,
    /// Tip poll interval (ms) once caught up to the uploaded tip.
    pub poll_ms: u64,
}

/// Per-regime boot values derived from recovery; everything the pipeline needs
/// to seed itself before the first block is fetched.
struct Boot {
    state: OpenState,
    tail: OpenTail,
    frontier: FamilyFrontier,
    /// Last durable block (`config.start - 1` on cold, where nothing is durable).
    resume: u64,
    /// Initial `data_durable` watermark.
    durable_floor: u64,
    /// First block to fetch.
    begin: u64,
    /// Artifact-checksum chain value as of `resume`.
    chain_seed: ChainDigest,
}

/// Spawn the producer, data track, index engine, and publisher around a fetch
/// source + run config. Recovery and the `count`→`end` resolution happen
/// in here, before the single-writer pipeline starts.
pub(crate) async fn run_ingest<S, M, B, R>(
    source: S,
    tables: Arc<Tables<M, B>>,
    publisher: Arc<PublicationTables<M>>,
    snapshots: SnapshotStore<M, B>,
    resolver: R,
    config: IngestRunConfig,
    prefetch: Prefetch,
) -> Result<()>
where
    S: ChainDataIngestSource,
    M: MetaStore,
    B: BlobStore,
    R: CodecResolver,
{
    // A zero cadence would checkpoint after every block.
    debug_assert!(
        config.policy.checkpoint_every_blocks > 0,
        "checkpoint_every_blocks must be positive"
    );

    // The fragment rebuild path keys off the reader-visible watermark.
    let published = publisher.published_head().await?;

    // Resume from max(checkpoint, published head); `begin` is the first block to
    // fetch. See `recover` for which side wins when. On a warm resume the data
    // frontier is durable through the resume block; cold has nothing durable.
    let Boot {
        state,
        tail,
        frontier,
        resume,
        durable_floor,
        begin,
        chain_seed,
    } = match recover::recover(&tables, &snapshots, published).await? {
        recover::Recovered::Warm {
            state,
            tail,
            frontier,
            resume,
        } => Boot {
            // Seed the artifact-checksum chain from the resume block's stored record.
            // Keyed on the recovery regime, NOT on `resume == 0`: a Warm resume whose
            // last durable block is genesis (block 0) must still fold block 0's record
            // (`resume == 0` then means "block 0 is durable", not "nothing durable"),
            // else every later `row_chain` would diverge from an uninterrupted
            // replica. Cold has nothing durable and re-ingests from `config.start`, so
            // it must seed `EMPTY_DIGEST` (loading a stale record would double-count
            // the re-ingested block). The `unwrap_or` covers the validated-`>= 1`
            // `unsafe_seed_begin` floor, which has no record below it. Identical across
            // both recovery regimes: the resume block's rows (and record) are durable.
            chain_seed: tables
                .blocks()
                .load_record(resume)
                .await?
                .map(|record| record.row_chain)
                .unwrap_or(EMPTY_DIGEST),
            durable_floor: resume,
            begin: (resume + 1).max(config.start),
            state,
            tail,
            frontier,
            resume,
        },
        recover::Recovered::Cold => Boot {
            state: OpenState::default(),
            tail: OpenTail::default(),
            frontier: FamilyFrontier::default(),
            resume: config.start.saturating_sub(1),
            durable_floor: 0,
            begin: config.start,
            chain_seed: EMPTY_DIGEST,
        },
    };

    // Seed the data frontier at the durable floor (the resume block on a warm
    // start) so the terminal/first flush boundary at the resume block promotes
    // immediately — a restart with nothing left to ingest still publishes its
    // already-durable tail. The head seeds at the stored published head, NOT
    // the floor: a checkpoint block above the last flush boundary has its tail
    // index only in the snapshot (no fragments), so re-publishing it at boot
    // would let indexed queries silently miss those blocks until the first
    // post-resume flush. Beyond the seed, only `batch_flush` records
    // publishable heads, so the published head can never cover index data that
    // isn't durable as fragments or sealed artifacts.
    let progress = Arc::new(Progress::new(durable_floor, published));

    // Resolve the stop ceiling now that the begin block is known; an explicit
    // `end` wins (at most one of end/count, validated upstream).
    let end = match (config.end, config.count) {
        (Some(end), _) => Some(end),
        (None, Some(count)) => Some(begin.saturating_add(count).saturating_sub(1)),
        (None, None) => None,
    };

    tracing::info!(
        resume,
        begin,
        end = ?end,
        published = ?published,
        "chain-data ingest resume point (begin = first block to fetch)"
    );

    let (tx_data, rx_data) = mpsc::channel(config.track_buffer);
    let (tx_index, rx_index) = mpsc::channel(config.track_buffer);
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Timing probe (off unless `ingest::timing=trace`); reporter spawned only
    // when enabled, aborted on exit.
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

    let sig = Signaller::new(
        tx_data,
        tx_index,
        frontier,
        config.policy,
        resume,
        probe.clone(),
    );

    // The pipeline tasks share a JoinSet so the first error/panic aborts the
    // rest. The publisher is kept separate so it can always be shut down
    // gracefully (final head publish).
    let mut pipeline = JoinSet::new();
    pipeline.spawn(run_producer(
        source,
        begin,
        end,
        config.poll_ms,
        prefetch,
        sig,
    ));
    pipeline.spawn(run_data_track(
        rx_data,
        tables.clone(),
        resolver,
        progress.clone(),
        data_track::DataTrackConfig {
            pack: config.pack,
            payload: config.payload,
        },
        probe,
        chain_seed,
    ));
    pipeline.spawn(run_index_track(rx_index, engine));
    let mut publisher_task = tokio::spawn(run_publisher(
        progress.clone(),
        tables,
        publisher,
        published,
        shutdown_rx,
    ));

    // Supervise the pipeline AND the publisher together; the publisher only
    // resolves on its own if its head write fails or it panics.
    let mut outcome = Ok(());
    loop {
        tokio::select! {
            joined = pipeline.join_next() => {
                let Some(joined) = joined else {
                    // Pipeline fully drained: graceful publisher shutdown below.
                    break;
                };
                // A task we aborted after capturing the first failure is the only
                // expected cancellation; surface any other join error as a panic.
                if let Some(result) = join_to_result(joined, "ingest task") {
                    capture_first(&mut outcome, &mut pipeline, result);
                }
            }
            pub_res = &mut publisher_task => {
                // Publisher resolved unprompted: tear down the pipeline and finish.
                if let Some(result) = join_to_result(pub_res, "publisher task") {
                    capture_first(&mut outcome, &mut pipeline, result);
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
    let pub_outcome = join_to_result(publisher_task.await, "publisher task").unwrap_or(Ok(()));

    outcome.and(pub_outcome)
}

/// Record the first failure and tear down the remaining pipeline tasks; once an
/// error is captured, later results (including the aborts' cancellations) are
/// ignored.
fn capture_first(outcome: &mut Result<()>, pipeline: &mut JoinSet<Result<()>>, result: Result<()>) {
    if let Err(e) = result {
        if outcome.is_ok() {
            *outcome = Err(e);
            pipeline.abort_all();
        }
    }
}

/// Normalize a joined task result into the task's own `Result`, or `None` when
/// the task was cancelled (an abort we issued after the first failure). A panic
/// becomes a `Backend` error tagged with `what`.
fn join_to_result(
    joined: std::result::Result<Result<()>, tokio::task::JoinError>,
    what: &str,
) -> Option<Result<()>> {
    match joined {
        Ok(result) => Some(result),
        Err(e) if e.is_cancelled() => None,
        Err(e) => Some(Err(MonadChainDataError::Backend(format!(
            "{what} panicked: {e}"
        )))),
    }
}
