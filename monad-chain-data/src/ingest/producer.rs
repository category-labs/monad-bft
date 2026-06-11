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

//! Producer task: fetch blocks, assign primary ids (the one ordered
//! cross-block step), fan out to both tracks, and emit the `BatchFlush` /
//! `Checkpoint` signals on their adaptive cadences.

use std::sync::Arc;

use futures::{stream, StreamExt};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tracing::Instrument;

use super::{
    probe::{IngestProbe, TIMING_TARGET},
    source::ChainDataIngestSource,
    task_join_err, AbortOnDrop, AssignedBlock, DataMsg, FamilyFrontier, IndexMsg, IngestMsg,
};
use crate::{
    error::{MonadChainDataError, Result},
    ingest_types::FinalizedBlock,
};

/// When the producer emits signals. The flush cadence is a smooth function of
/// the distance to the uploaded tip:
/// `flush_interval(d) = max(d / tip_lag_divisor, 1)` — every block at the tip,
/// coarser the farther behind. Purely a reader-freshness knob: `OpenTail`
/// memory is not a factor (seal drains completed pages continuously).
#[derive(Debug, Clone, Copy)]
pub struct SignalPolicy {
    /// Divides the distance to the tip to get the flush interval (min 1).
    /// Larger = fresher head; very large flushes every block.
    pub tip_lag_divisor: u64,
    /// Emit `Checkpoint` every N blocks (bounds fragment-replay on recovery).
    pub checkpoint_every_blocks: u64,
}

/// Producer fetch parallelism: `concurrency` caps active fetch+decode tasks;
/// `buffer` (>= `concurrency`) is the ordered look-ahead window of decoded
/// blocks. Delivery to id-assignment is always in range order.
#[derive(Debug, Clone, Copy)]
pub struct Prefetch {
    pub concurrency: usize,
    pub buffer: usize,
}

/// Cross-block id frontier, downstream senders, and signal cadence counters.
pub(crate) struct Signaller {
    tx_data: mpsc::Sender<DataMsg>,
    tx_index: mpsc::Sender<IndexMsg>,
    frontier: FamilyFrontier,
    policy: SignalPolicy,
    since_flush: u64,
    since_ckpt: u64,
    /// Last block fed downstream (near side of the distance-to-tip measurement).
    last_block: u64,
    /// Uploaded tip from the most recent poll; `None` before the first poll.
    observed_tip: Option<u64>,
    probe: Arc<IngestProbe>,
}

impl Signaller {
    /// `last_block` seeds the distance-to-tip measurement: the resume block.
    pub(crate) fn new(
        tx_data: mpsc::Sender<DataMsg>,
        tx_index: mpsc::Sender<IndexMsg>,
        frontier: FamilyFrontier,
        policy: SignalPolicy,
        last_block: u64,
        probe: Arc<IngestProbe>,
    ) -> Self {
        Self {
            tx_data,
            tx_index,
            frontier,
            policy,
            since_flush: 0,
            since_ckpt: 0,
            last_block,
            observed_tip: None,
            probe,
        }
    }

    /// Send one message to each track. Returns `true` while both channels are
    /// open; `false` once either is closed (its track has ended).
    async fn send_both(&self, data: DataMsg, index: IndexMsg) -> bool {
        // Timed per channel: blocking here is backpressure from that track.
        let data_start = self.probe.start();
        let data_open = self.tx_data.send(data).await.is_ok();
        self.probe
            .record(&self.probe.producer_send_data_blocked_ns, data_start);
        let index_start = self.probe.start();
        let index_open = self.tx_index.send(index).await.is_ok();
        self.probe
            .record(&self.probe.producer_send_index_blocked_ns, index_start);
        data_open && index_open
    }

    /// Assign ids and fan the block out to both tracks. Returns `false` if a
    /// downstream channel is closed (its track errored — stop and let that
    /// error surface via the `JoinSet`).
    async fn feed(&mut self, block: FinalizedBlock) -> bool {
        self.last_block = block.block_number();
        let ranges = {
            let _span = tracing::trace_span!(target: TIMING_TARGET, "assign").entered();
            self.frontier.assign(&block)
        };
        self.probe.count_block(&ranges);
        let assigned = Arc::new(AssignedBlock {
            number: block.block_number(),
            ranges,
            block,
        });
        let open = self
            .send_both(
                IngestMsg::Block(assigned.clone()),
                IngestMsg::Block(assigned),
            )
            .await;
        self.since_flush += 1;
        self.since_ckpt += 1;
        open
    }

    async fn emit_batch_flush(&self) -> bool {
        self.send_both(IngestMsg::BatchFlush, IngestMsg::BatchFlush)
            .await
    }

    /// Emit a `Checkpoint` to both tracks wired through a fresh oneshot: the
    /// data track signals after its pack flush is durable, the index track
    /// awaits that before snapshotting.
    async fn emit_checkpoint(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        self.send_both(IngestMsg::Checkpoint(tx), IngestMsg::Checkpoint(rx))
            .await
    }

    /// The active `BatchFlush` interval: `max(distance_to_tip / tip_lag_divisor,
    /// 1)` — 1 at the tip, so caught-up flushes every block with no separate
    /// drain-flush. Before the first tip poll the interval is effectively never,
    /// but nothing has been fed yet either.
    fn flush_interval(&self) -> u64 {
        let distance = self
            .observed_tip
            .map_or(u64::MAX, |tip| tip.saturating_sub(self.last_block));
        (distance / self.policy.tip_lag_divisor.max(1)).max(1)
    }

    /// Emit `BatchFlush` / `Checkpoint` if due. The cadences are independent, so
    /// both may fire together. Returns `false` if a downstream channel is closed.
    async fn maybe_signal(&mut self) -> bool {
        let mut open = true;
        if self.since_flush >= self.flush_interval() {
            open &= self.emit_batch_flush().await;
            self.since_flush = 0;
        }
        if self.since_ckpt >= self.policy.checkpoint_every_blocks {
            open &= self.emit_checkpoint().await;
            self.since_ckpt = 0;
        }
        open
    }

    /// Bounded-run terminal: a final BatchFlush so the head reaches the last
    /// block, then a Checkpoint so resume is cheap.
    async fn terminate(&mut self) {
        self.emit_batch_flush().await;
        self.emit_checkpoint().await;
    }
}

fn source_err(e: eyre::Report) -> MonadChainDataError {
    MonadChainDataError::Backend(format!("ingest source: {e}"))
}

/// Producer task: always follow the tip. On each poll it fetches the backlog
/// `[next..=min(uploaded tip, end)]` in parallel; at the steady tip this
/// degrades naturally to one-at-a-time. With a `Some(end)` ceiling the run
/// terminates (terminal flush + checkpoint) once it passes `end`.
// TODO(fetch-retry): source errors here abort the whole pipeline. Fold a
// retry+backoff policy back in — ideally inside the `ChainDataIngestSource`
// impl so the engine stays transport-agnostic — so a transient upstream blip
// doesn't abort the single-writer pipeline.
pub(crate) async fn run_producer<S>(
    source: S,
    start: u64,
    end: Option<u64>,
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
        sig.observed_tip = latest;
        let target = latest.map(|tip| end.map_or(tip, |end| tip.min(end)));
        if let Some(target) = target.filter(|t| *t >= next) {
            if !fetch_range(&source, &mut sig, next, target, prefetch).await? {
                return Ok(());
            }
            next = target + 1;
            // No signal check here: `fetch_range` ends every block with
            // `feed` + `maybe_signal` and nothing observable changes between
            // its last check and this point, so another would be a no-op.
            //
            // Loop immediately to keep catching up; only sleep once the
            // uploaded tip is drained (not merely the stop ceiling reached).
            if latest.is_some_and(|l| next <= l) {
                continue;
            }
        } else if !sig.maybe_signal().await {
            return Ok(());
        }
        if end.is_some_and(|end| next > end) {
            sig.terminate().await;
            return Ok(());
        }
        // This sleep only ever runs at the tip — catch-up streams the whole
        // backlog without polling.
        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
    }
}

/// Fetch `[start, end]` with bounded, ordered prefetch + parallel decode,
/// feeding each block downstream in range order. Returns `Ok(false)` if a
/// downstream track closed mid-range. A semaphore caps *active* fetch+decode
/// tasks at `concurrency`; `buffered` keeps a wider look-ahead window so the
/// engine never starves during a flush/checkpoint stall, and (unlike
/// `buffer_unordered`) yields in range order so id-assignment stays sequential.
///
/// Each spawned fetch is held in an [`AbortOnDrop`], so every early exit that
/// drops the stream — a fetch error, a closed downstream track, or the
/// pipeline `JoinSet` aborting this task — aborts the in-flight window instead
/// of detaching up to `window` orphaned fetches.
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
            AbortOnDrop(tokio::spawn(
                async move {
                    let _permit = permits
                        .acquire_owned()
                        .await
                        .expect("fetch semaphore is never closed");
                    // Aggregated across concurrent tasks, so it can exceed wall-clock.
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
            ))
        })
        .buffered(window);
    while let Some(joined) = fetched.next().await {
        // Outer `?`: spawned fetch panicked/cancelled. Inner `?`: fetch errored.
        let block = joined.map_err(task_join_err)??;
        if !sig.feed(block).await || !sig.maybe_signal().await {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;

    /// Fetches that never resolve; `live` counts fetch futures currently alive
    /// inside spawned tasks (decremented on drop, i.e. when the task aborts).
    #[derive(Clone)]
    struct StallSource {
        live: Arc<AtomicUsize>,
    }

    struct LiveGuard(Arc<AtomicUsize>);
    impl Drop for LiveGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl ChainDataIngestSource for StallSource {
        fn get_latest_uploaded(&self) -> impl Future<Output = eyre::Result<Option<u64>>> + Send {
            std::future::ready(Ok(None))
        }

        fn fetch_finalized_block(
            &self,
            _block_number: u64,
        ) -> impl Future<Output = eyre::Result<FinalizedBlock>> + Send {
            self.live.fetch_add(1, Ordering::SeqCst);
            let guard = LiveGuard(self.live.clone());
            async move {
                let _guard = guard;
                futures::future::pending().await
            }
        }
    }

    /// Dropping `fetch_range` mid-flight (what `pipeline.abort_all()` does to
    /// `run_producer`) must abort the spawned fetch tasks, not detach them.
    #[tokio::test]
    async fn dropping_fetch_range_aborts_inflight_fetch_tasks() {
        let live = Arc::new(AtomicUsize::new(0));
        let source = StallSource { live: live.clone() };
        let (tx_data, _rx_data) = mpsc::channel(8);
        let (tx_index, _rx_index) = mpsc::channel(8);
        let mut sig = Signaller::new(
            tx_data,
            tx_index,
            FamilyFrontier::default(),
            SignalPolicy {
                tip_lag_divisor: 1,
                checkpoint_every_blocks: u64::MAX,
            },
            0,
            IngestProbe::new(),
        );
        let prefetch = Prefetch {
            concurrency: 4,
            buffer: 8,
        };
        let range =
            tokio::spawn(async move { fetch_range(&source, &mut sig, 0, 99, prefetch).await });

        // Wait until the active fetch window is full (no fetch ever resolves).
        tokio::time::timeout(Duration::from_secs(5), async {
            while live.load(Ordering::SeqCst) < prefetch.concurrency {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fetch window must fill");

        range.abort();
        let _ = range.await;

        // Every in-flight fetch future must be dropped (task aborted) shortly
        // after the buffered stream is dropped.
        tokio::time::timeout(Duration::from_secs(5), async {
            while live.load(Ordering::SeqCst) > 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("in-flight fetch tasks must die with fetch_range");
    }
}
