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

//! The branchless index engine: accumulate ids into the open tail, seal
//! completed pages/buckets continuously, flush the tail as reader-visible
//! fragments on `BatchFlush`, and snapshot the working set on `Checkpoint`.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    sync::Arc,
};

use bytes::Bytes;
use monad_query_engine::{
    bitmap::{
        encode_bitmap_blob, encode_bitmap_page_artifact, encode_open_streams_delta,
        page_group_start, page_offset, page_start, page_start_in_group, BitmapPageArtifact,
        BitmapPageCounts, BitmapPageMeta, DecodedBitmapFragment, StreamKey,
        OPEN_STREAMS_DELTA_TARGET_BYTES, STREAM_PAGE_ID_SPAN,
    },
    digest::{chain, ChainDigest, SealDigest},
    family::{Family, PerFamily},
    primary_dir::{PrimaryDirBucket, PrimaryDirEntry, DIRECTORY_BUCKET_SIZE},
    seal::seal_boundary,
    tables::Tables,
    WriteSession,
};
use monad_query_errors::{QueryError, Result};
use monad_query_primitives::records::PrimaryId;
use monad_query_store::{BlobStore, MetaStore};
use roaring::RoaringBitmap;
use tokio::sync::{mpsc, oneshot};

use super::{
    probe::IngestProbe,
    publisher::Progress,
    snapshot::{persist_snapshot, SnapshotStore},
    AssignedBlock, FamilyFrontier, IndexMsg, IngestMsg,
};
use crate::{
    logs::stream_entries_for_log, traces::stream_entries_for_trace, txs::stream_entries_for_tx,
};

pub(crate) const PAGE_SPAN: u64 = STREAM_PAGE_ID_SPAN as u64;
pub(crate) const BUCKET_SPAN: u64 = DIRECTORY_BUCKET_SIZE;
/// The branchless seal path requires pages and buckets to share one boundary.
const _: () = assert!(PAGE_SPAN == BUCKET_SPAN);

/// A single index write. Sealing/flushing computes these synchronously, then
/// [`IndexEngine::write_all`] issues them concurrently inline — no writer task;
/// inline writes are durable on return, so `batch_flush` needs no barrier.
pub(crate) enum ArtifactWrite {
    Page {
        family: Family,
        stream_id: String,
        page_start: u64,
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
        page_start: u64,
        flush_block: u64,
        blob: Bytes,
    },
    DirFragment {
        family: Family,
        block: u64,
        first_id: u64,
        /// Exclusive upper bound: the entry count is `end_id - first_id`.
        end_id: u64,
    },
    /// A page's stream inventory — the reader's (and rebuild recovery's)
    /// listing for discovering that page's per-stream rows. Written as chunked
    /// delta rows keyed by `(marker_block, chunk_idx)`, twice over a page's
    /// life: at each flush (marker = the flush block), the streams first seen
    /// in the open page that batch; at seal (marker = the seal block), the
    /// sealed page's complete inventory (covering streams whose only bits
    /// arrived after the last flush), which is what lets recovery re-derive
    /// the open group's sealed page counts.
    OpenStreams {
        family: Family,
        page_start: u64,
        marker_block: u64,
        streams: Vec<String>,
    },
    /// The family's chained seal digest after span `span_start` sealed; staged
    /// in the same batch as that span's sealed artifacts.
    SealChain {
        family: Family,
        span_start: u64,
        value: ChainDigest,
    },
    /// One `(stream, page group)` page-count manifest row, written EXACTLY
    /// ONCE — when the seal boundary leaves the group — from the counts
    /// accumulated in [`FamilyState::sealed_page_counts`]. Never merged with a
    /// stored row; a recovery replay rewrites the identical value.
    PageCounts {
        family: Family,
        stream_id: String,
        group_start: u64,
        counts: BitmapPageCounts,
    },
}

/// Current-batch delta (since the last `BatchFlush`).
#[derive(Default)]
pub(crate) struct FamilyTail {
    pub pages: HashMap<(StreamKey, u64), RoaringBitmap>,
    pub dir: Vec<(u64, u64, u64)>, // (block, first_id, end_id exclusive)
}

/// Carry-over: bits fragmented in prior flushes but not yet sealed. `sealed_id`
/// (a multiple of [`SEAL_SPAN`]) tracks pages and buckets together: granules
/// below it are sealed into artifacts and removed; at/above it is open.
#[derive(Default)]
pub(crate) struct FamilyState {
    pub pages: HashMap<(StreamKey, u64), RoaringBitmap>,
    pub dir: VecDeque<(u64, u64, u64)>,
    pub sealed_id: u64,
    /// Streams already inventoried for the current open page (recorded once per
    /// page, not per flush). Cleared when the page rolls in `seal_family`.
    pub open_streams_seen: HashSet<StreamKey>,
    /// Running standby seal chain through the last sealed span
    /// (`last_sealed_span(sealed_id)`); `EMPTY_DIGEST` before the first seal.
    pub seal_chain: ChainDigest,
    /// Per-`(stream, group start)` accumulator of `(page_start_in_group,
    /// count)` pairs for pages already sealed in the OPEN page group. Drained
    /// into manifest rows by `seal_family` when the seal boundary leaves the
    /// group; carried by checkpoints, re-derived from sealed artifacts by the
    /// no-checkpoint rebuild ([`super::recover`]).
    pub sealed_page_counts: SealedPageCounts,
}

/// Sealed-page-count accumulator shape: `(stream, group start)` → ascending
/// `(page_start_in_group, count)` pairs.
pub(crate) type SealedPageCounts = BTreeMap<(StreamKey, u64), Vec<(u32, u32)>>;

pub(crate) type OpenTail = PerFamily<FamilyTail>;
pub(crate) type OpenState = PerFamily<FamilyState>;

/// Computes [`ArtifactWrite`]s synchronously (mutating the accumulators), then
/// issues them concurrently inline against the store — no writer task.
pub(crate) struct IndexEngine<M: MetaStore, B: BlobStore> {
    state: OpenState,
    tail: OpenTail,
    /// Next-id per family after the latest accumulated block (seal boundary input).
    frontier: FamilyFrontier,
    last_block: u64,
    tables: Arc<Tables<M, B>>,
    progress: Arc<Progress>,
    snapshots: SnapshotStore<M, B>,
    probe: Arc<IngestProbe>,
    /// Depth-1 write pipeline: the previous burst's commit, still in flight.
    /// Drained before the next burst is issued (bursts commit in order) and
    /// before anything that needs durability (flush horizon, checkpoint).
    inflight_write: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl<M, B> IndexEngine<M, B>
where
    M: MetaStore,
    B: BlobStore,
{
    pub(crate) fn new(
        state: OpenState,
        tail: OpenTail,
        frontier: FamilyFrontier,
        resume: u64,
        tables: Arc<Tables<M, B>>,
        progress: Arc<Progress>,
        snapshots: SnapshotStore<M, B>,
        probe: Arc<IngestProbe>,
    ) -> Self {
        Self {
            state,
            tail,
            frontier,
            // Seed from the recovered resume block, not 0: a Checkpoint or
            // empty-tail flush before the first new block must see it.
            last_block: resume,
            tables,
            progress,
            snapshots,
            probe,
            inflight_write: None,
        }
    }

    /// Commit a seal/flush burst as one meta batch, pipelined at depth 1:
    /// drain the previous burst (bursts commit in order — the seal chain and
    /// recovery invariants assume no gaps), then issue this one in the
    /// background so the commit overlaps subsequent accumulate/seal compute.
    /// The store layer is responsible for backend batch limits.
    async fn write_all(&mut self, writes: Vec<ArtifactWrite>) -> Result<()> {
        self.drain_write().await?;
        if writes.is_empty() {
            return Ok(());
        }
        let tables = self.tables.clone();
        self.inflight_write = Some(tokio::spawn(async move {
            tables
                .with_writes(|w| {
                    Box::pin(async move {
                        for write in writes {
                            stage_artifact_write(w, write)?;
                        }
                        Ok(())
                    })
                })
                .await
        }));
        Ok(())
    }

    /// Await the in-flight burst, if any. Only the blocked portion counts as
    /// `index_write` — commit time hidden behind compute no longer shows up,
    /// which is the point of the pipeline.
    async fn drain_write(&mut self) -> Result<()> {
        let Some(handle) = self.inflight_write.take() else {
            return Ok(());
        };
        let io_start = self.probe.start();
        let result = handle
            .await
            .map_err(|e| QueryError::Backend(format!("index write task: {e}")))?;
        self.probe.record(&self.probe.index_write_ns, io_start);
        result
    }

    /// Insert one block's ids into the tail and advance the seal frontier.
    /// Fallible because tx stream extraction decodes the envelope.
    fn accumulate(&mut self, b: &AssignedBlock) -> Result<()> {
        accumulate_family(
            self.tail.get_mut(Family::Log),
            PrimaryId::from(b.ranges.log_first).as_u64(),
            b.ranges.log_count,
            b.number,
            b.block.logs_by_tx.iter().flatten(),
            |log| {
                Ok(stream_entries_for_log(
                    log.address.as_slice(),
                    log.data.topics(),
                ))
            },
        )?;
        accumulate_family(
            self.tail.get_mut(Family::Tx),
            PrimaryId::from(b.ranges.tx_first).as_u64(),
            b.ranges.tx_count,
            b.number,
            b.block.txs.iter(),
            stream_entries_for_tx,
        )?;
        accumulate_family(
            self.tail.get_mut(Family::Trace),
            PrimaryId::from(b.ranges.trace_first).as_u64(),
            b.ranges.trace_count,
            b.number,
            b.block.traces.iter(),
            |trace| Ok(stream_entries_for_trace(trace)),
        )?;
        self.frontier.log = b.ranges.log_end();
        self.frontier.tx = b.ranges.tx_end();
        self.frontier.trace = b.ranges.trace_end();
        self.last_block = b.number;
        Ok(())
    }

    /// Seal every granule the frontier just completed (carry-over ∪ delta) and
    /// write the artifacts inline. Runs per block.
    async fn seal_ready(&mut self) -> Result<()> {
        let mut writes = Vec::new();
        for family in Family::ALL {
            writes.append(&mut seal_family(
                family,
                self.state.get_mut(family),
                self.tail.get_mut(family),
                *self.frontier.get(family),
                self.last_block,
            )?);
        }
        self.write_all(writes).await
    }

    /// Flush the tail as reader-visible fragments and carry it into the state;
    /// once the fragments are durable, record the tip as a publishable flush
    /// boundary.
    async fn batch_flush(&mut self) -> Result<()> {
        let mut writes = Vec::new();
        for family in Family::ALL {
            writes.append(&mut flush_family(
                family,
                self.state.get_mut(family),
                self.tail.get_mut(family),
                self.last_block,
            )?);
        }
        // Inline writes are durable on return — no barrier. The boundary is
        // recorded unconditionally so empty blocks / a terminal flush still
        // yield a publishable head once the data track covers it.
        self.write_all(writes).await?;
        // The boundary must not be recorded past writes still in flight.
        self.drain_write().await?;
        // Everything at/below `last_block` — this flush's fragments AND every
        // prior seal's artifacts — is durable here, so the block is safe for
        // the publisher to expose as a head (the fragment rebuild depends on
        // the head being a flush boundary; see `Progress`).
        self.progress.record_flush_boundary(self.last_block);
        Ok(())
    }

    /// Recovery-only: snapshot the open working set without flushing fragments
    /// or recording a flush boundary. Awaits `data_durable` first so the
    /// snapshot's resume block never outruns durable row data.
    async fn checkpoint(&mut self, data_durable: oneshot::Receiver<()>) -> Result<()> {
        // The snapshot's state assumes every issued burst is durable (a resume
        // must find the artifacts the checkpointed state says were sealed), so
        // the pipeline drains first.
        self.drain_write().await?;
        // A cross-track wait, recorded apart from work/write so derived index
        // CPU stays honest.
        let wait_start = self.probe.start();
        let awaited = data_durable.await;
        self.probe
            .record(&self.probe.index_ckpt_wait_ns, wait_start);
        awaited.map_err(|_| QueryError::Backend("data track stopped before checkpoint".into()))?;
        // Snapshot persist counts as index-side store I/O too.
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
    mut rx: mpsc::Receiver<IndexMsg>,
    mut engine: IndexEngine<M, B>,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
{
    let probe = engine.probe.clone();
    loop {
        // Time blocked here = the index track is starved.
        let recv_start = probe.start();
        let msg = rx.recv().await;
        probe.record(&probe.index_recv_blocked_ns, recv_start);
        let Some(msg) = msg else { break };
        // Time here = the index track's own work.
        let work_start = probe.start();
        match msg {
            IngestMsg::Block(b) => {
                engine.accumulate(&b)?;
                engine.seal_ready().await?;
            }
            IngestMsg::BatchFlush => engine.batch_flush().await?,
            IngestMsg::Checkpoint(rx) => engine.checkpoint(rx).await?,
        }
        probe.record(&probe.index_work_ns, work_start);
    }
    // Channel closed: surface any still-in-flight burst's outcome.
    engine.drain_write().await
}

/// Seal one family's completed granules: `state[g] ∪ tail[g] → artifact`,
/// removed from both. Returns the writes for the caller to issue. Each sealed
/// page's `(page, count)` accumulates in `state.sealed_page_counts` (counts
/// come from the sealed page content, never flush cadence); when the new seal
/// boundary leaves one or more page groups, the completed groups' per-stream
/// manifest rows are emitted in the same burst and their accumulator entries
/// dropped. Each sealed page also stages its complete stream inventory
/// (keyed by `seal_block`), the durable enumeration recovery's rebuild reads.
///
/// Each sealed span also folds into the family's standby seal chain
/// ([`SealDigest`] over the exact persisted artifact bytes), in ascending span
/// order, with the chained value staged alongside the span's artifacts. The
/// digest input is the span's FINAL sealed content, so the chain is
/// independent of how many flushes (fragments) preceded the seal.
fn seal_family(
    family: Family,
    state: &mut FamilyState,
    tail: &mut FamilyTail,
    frontier: u64,
    seal_block: u64,
) -> Result<Vec<ArtifactWrite>> {
    let from = state.sealed_id;
    let open = seal_boundary(frontier);
    let mut writes = Vec::new();
    if open <= from {
        return Ok(writes); // no new seal boundary completed this block
    }

    // Bitmap pages: every page fully below the frontier. `ready` keys are in
    // `[from, open)`: pages below `from` were removed by earlier seals.
    let ready: BTreeSet<(StreamKey, u64)> = state
        .pages
        .keys()
        .chain(tail.pages.keys())
        .filter(|(_, page)| page + PAGE_SPAN <= frontier)
        .copied()
        .collect();
    // Per-span page digest inputs: (canonical stream id, persisted bytes).
    let mut span_pages: BTreeMap<u64, Vec<(String, Bytes)>> = BTreeMap::new();
    for key in ready {
        let mut bm = state.pages.remove(&key).unwrap_or_default();
        if let Some(t) = tail.pages.remove(&key) {
            bm |= t;
        }
        let (stream_key, page) = key;
        let (meta, bitmap_blob) = encode_open_bitmap(bm)?;
        let artifact = BitmapPageArtifact { meta, bitmap_blob };
        let stream_id = stream_key.render();
        state
            .sealed_page_counts
            .entry((stream_key, page_group_start(page)))
            .or_default()
            .push((page_start_in_group(page), meta.count));
        span_pages
            .entry(page)
            .or_default()
            .push((stream_id.clone(), encode_bitmap_page_artifact(&artifact)));
        writes.push(ArtifactWrite::Page {
            family,
            stream_id,
            page_start: page,
            artifact,
        });
    }

    // Directory buckets [from, open): same boundary as the pages above. Fold
    // every sealed span (returned in ascending order) into the seal chain.
    for (span_start, bucket) in seal_family_dir(state, tail, from, open)? {
        let bucket_bytes = Bytes::from(bucket.encode());
        writes.push(ArtifactWrite::Bucket {
            family,
            bucket_start: span_start,
            bucket,
        });
        let mut digest = SealDigest::new(family, span_start);
        let mut pages = span_pages.remove(&span_start).unwrap_or_default();
        pages.sort_by(|a, b| a.0.cmp(&b.0));
        for (stream_id, artifact_bytes) in &pages {
            digest.page(stream_id, artifact_bytes);
        }
        state.seal_chain = chain(state.seal_chain, digest.finish(&bucket_bytes));
        writes.push(ArtifactWrite::SealChain {
            family,
            span_start,
            value: state.seal_chain,
        });
        // The sealed page's complete inventory: a span IS one page position
        // (SEAL_SPAN == PAGE_SPAN), keyed by the seal block, which is replay-
        // deterministic; `pages` is already stream-id sorted, so the chunked
        // rows are byte-identical across replays.
        if !pages.is_empty() {
            writes.push(ArtifactWrite::OpenStreams {
                family,
                page_start: span_start,
                marker_block: seal_block,
                streams: pages
                    .iter()
                    .map(|(stream_id, _)| stream_id.clone())
                    .collect(),
            });
        }
    }
    debug_assert!(
        span_pages.is_empty(),
        "sealed page outside any sealed span: {:?}",
        span_pages.keys()
    );

    // Manifest rows for completed page groups: a `(stream, group)` row is
    // written exactly once, when the seal boundary leaves the group, carrying
    // every sealed page accumulated for it. Handles a boundary jump across
    // multiple groups (the accumulator key routes each page to its group's row).
    let open_group = page_group_start(open);
    if page_group_start(from) < open_group {
        let completed: Vec<(StreamKey, u64)> = state
            .sealed_page_counts
            .keys()
            .filter(|(_, group_start)| *group_start < open_group)
            .copied()
            .collect();
        for key @ (stream_key, group_start) in completed {
            let pairs = state
                .sealed_page_counts
                .remove(&key)
                .expect("key listed from this map");
            writes.push(ArtifactWrite::PageCounts {
                family,
                stream_id: stream_key.render(),
                group_start,
                counts: BitmapPageCounts::from_pairs(pairs),
            });
        }
    }

    state.sealed_id = open;
    // The open page rolled, so its stream inventory restarts.
    state.open_streams_seen.clear();
    Ok(writes)
}

/// Encode an open-granule bitmap into its persisted blob, plus the meta a
/// sealed-page artifact wraps around it.
fn encode_open_bitmap(bm: RoaringBitmap) -> Result<(BitmapPageMeta, Bytes)> {
    let meta = BitmapPageMeta {
        min_offset: bm.min().unwrap_or(0),
        max_offset: bm.max().unwrap_or(0),
        count: bm.len() as u32,
    };
    let blob = encode_bitmap_blob(&DecodedBitmapFragment {
        min_offset: meta.min_offset,
        max_offset: meta.max_offset,
        count: meta.count,
        bitmap: bm,
    })?;
    Ok((meta, blob))
}

/// Flush one family's tail: each touched page/dir entry is written as a delta
/// fragment AND unioned into the carry-over state; the tail is drained.
fn flush_family(
    family: Family,
    state: &mut FamilyState,
    tail: &mut FamilyTail,
    flush_block: u64,
) -> Result<Vec<ArtifactWrite>> {
    let mut writes = Vec::new();
    // Invariant: all drained page keys share the one open page — seal already
    // removed every page fully below the frontier, and the open span is narrower
    // than a page — so newly-seen streams are recorded under that single page.
    let mut open_page: Option<u64> = None;
    let mut new_streams = Vec::new();
    for (key, bm) in std::mem::take(&mut tail.pages) {
        // Union by ref so `bm` survives for the fragment.
        *state.pages.entry(key).or_default() |= &bm;
        let (stream_key, page) = key;
        debug_assert!(
            open_page.is_none_or(|p| p == page),
            "flush_family expects a single open page per family; saw {page} and {open_page:?}"
        );
        open_page = Some(page);
        if state.open_streams_seen.insert(stream_key) {
            new_streams.push(stream_key);
        }
        let (_, blob) = encode_open_bitmap(bm)?;
        writes.push(ArtifactWrite::PageFragment {
            family,
            stream_id: stream_key.render(),
            page_start: page,
            flush_block,
            blob,
        });
    }
    // A non-empty `new_streams` implies the loop ran, so some page was seen.
    if !new_streams.is_empty() {
        writes.push(ArtifactWrite::OpenStreams {
            family,
            page_start: open_page.expect("a seen stream implies a seen page"),
            marker_block: flush_block,
            streams: new_streams.iter().map(StreamKey::render).collect(),
        });
    }
    for entry @ (block, first_id, end_id) in std::mem::take(&mut tail.dir) {
        writes.push(ArtifactWrite::DirFragment {
            family,
            block,
            first_id,
            end_id,
        });
        state.dir.push_back(entry);
    }
    Ok(writes)
}

/// Seal one family's directory buckets over `[from, open)` (multiples of
/// [`SEAL_SPAN`]). Each bucket compacts the entries overlapping it across
/// `state.dir` + `tail.dir`; a block straddling a boundary is filed in every
/// bucket it overlaps. Entries that can no longer reach the open bucket are
/// then drained from the front of both. Returns the sealed buckets in
/// ascending `bucket_start` order (the seal-chain fold order).
fn seal_family_dir(
    state: &mut FamilyState,
    tail: &mut FamilyTail,
    from: u64,
    open: u64,
) -> Result<Vec<(u64, PrimaryDirBucket)>> {
    let mut sealed = Vec::new();
    for bs in (from..open).step_by(BUCKET_SPAN as usize) {
        sealed.push((bs, compact_dir_bucket(&state.dir, &tail.dir, bs)?));
    }
    // Entries ending at/below `open` now live in sealed bucket artifacts; drop
    // them. Straddlers reaching into the open bucket (`end > open`) stay.
    drain_sealed_dir_entries(state, tail, open);
    Ok(sealed)
}

/// Compact bucket `[bs, bs + BUCKET_SPAN)` from the overlapping entries in the
/// `state_dir` → `tail_dir` chain (a single ascending, id-contiguous sequence).
/// The sentinel is the last overlapping `end` (may exceed the span — straddler).
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
            // Defensive: a break in the contiguous chain is an ingest bug.
            if block <= prev_block {
                return Err(QueryError::Decode(
                    "inconsistent primary directory bucket block sequence",
                ));
            }
            if prev_end != first {
                return Err(QueryError::Decode(
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
        return Err(QueryError::MissingData(
            "sealed primary directory bucket has no overlapping entries",
        ));
    }
    PrimaryDirBucket::new(entries, sentinel)
}

/// Drop entries ending at/below `open_bucket_start` (overlap only sealed
/// buckets). `end` is monotonic across the `state.dir` → `tail.dir` chain, so
/// the drop set is a prefix.
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

/// Insert one block's records for a single family: each record gets the next
/// sequential primary id from `first_pid`; `entries_of` expands it into its
/// indexed streams, each getting the id's page-relative bit set in that
/// stream's open page. One directory entry spans the block's id range.
fn accumulate_family<R, F>(
    tail: &mut FamilyTail,
    first_pid: u64,
    count: u32,
    block: u64,
    records: impl IntoIterator<Item = R>,
    mut entries_of: F,
) -> Result<()>
where
    F: FnMut(R) -> Result<Vec<StreamKey>>,
{
    let mut id = first_pid;
    for record in records {
        let page = page_start(id);
        let offset = page_offset(id);
        for stream in entries_of(record)? {
            tail.pages.entry((stream, page)).or_default().insert(offset);
        }
        id = id.checked_add(1).expect("primary id overflow");
    }
    if count > 0 {
        tail.dir
            .push((block, first_pid, first_pid + u64::from(count)));
    }
    Ok(())
}

/// Stage one artifact into the write session via the table `stage_*` methods,
/// so key derivation and encoding match the reader exactly.
pub(crate) fn stage_artifact_write<M, B>(
    w: &mut WriteSession<'_, M, B>,
    write: ArtifactWrite,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
{
    let tables = w.tables();
    match write {
        ArtifactWrite::Page {
            family,
            stream_id,
            page_start,
            artifact,
        } => tables
            .family(family)
            .bitmap()
            .stage_page_artifact(w, &stream_id, page_start, &artifact),
        ArtifactWrite::Bucket {
            family,
            bucket_start,
            bucket,
        } => tables
            .family(family)
            .dir()
            .stage_bucket(w, bucket_start, &bucket),
        ArtifactWrite::PageFragment {
            family,
            stream_id,
            page_start,
            flush_block,
            blob,
        } => tables.family(family).bitmap().stage_page_fragment(
            w,
            &stream_id,
            page_start,
            flush_block,
            blob,
        ),
        ArtifactWrite::DirFragment {
            family,
            block,
            first_id,
            end_id,
        } => {
            let count = u32::try_from(end_id - first_id)
                .map_err(|_| QueryError::Decode("primary directory fragment count exceeds u32"))?;
            tables
                .family(family)
                .dir()
                .stage_block_fragment(w, block, first_id, count)
        }
        ArtifactWrite::OpenStreams {
            family,
            page_start,
            marker_block,
            streams,
        } => stage_open_streams_chunks(w, family, page_start, marker_block, streams),
        ArtifactWrite::SealChain {
            family,
            span_start,
            value,
        } => tables.family(family).stage_seal_chain(w, span_start, value),
        ArtifactWrite::PageCounts {
            family,
            stream_id,
            group_start,
            counts,
        } => tables
            .family(family)
            .bitmap()
            .stage_page_counts(w, &stream_id, group_start, &counts),
    }
    Ok(())
}

/// Stage one burst's stream inventory as chunked delta rows, keyed by
/// `(marker_block, chunk_idx)`. Size is checked before pushing (header
/// included) so a chunk never exceeds the target — except a single id larger
/// than the whole target.
fn stage_open_streams_chunks<M, B>(
    w: &mut WriteSession<'_, M, B>,
    family: Family,
    page_start: u64,
    marker_block: u64,
    streams: Vec<String>,
) where
    M: MetaStore,
    B: BlobStore,
{
    const HEADER_BYTES: usize = 5; // version (1) + count (4)
    let bitmap = w.tables().family(family).bitmap();
    let emit = |w: &mut WriteSession<'_, M, B>, chunk_idx: u32, chunk: &[String]| {
        bitmap.stage_open_streams_delta(
            w,
            page_start,
            marker_block,
            chunk_idx,
            encode_open_streams_delta(chunk),
        );
    };
    let mut chunk: Vec<String> = Vec::new();
    let mut chunk_bytes = HEADER_BYTES;
    let mut chunk_idx = 0u32;
    for stream_id in streams {
        let entry_bytes = stream_id.len() + 4; // len prefix + utf8
        if !chunk.is_empty() && chunk_bytes + entry_bytes > OPEN_STREAMS_DELTA_TARGET_BYTES {
            emit(w, chunk_idx, &chunk);
            chunk.clear();
            chunk_bytes = HEADER_BYTES;
            chunk_idx += 1;
        }
        chunk_bytes += entry_bytes;
        chunk.push(stream_id);
    }
    if !chunk.is_empty() {
        emit(w, chunk_idx, &chunk);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use monad_query_engine::bitmap::PAGE_GROUP_ID_SPAN;

    use super::*;

    const GROUP: u64 = PAGE_GROUP_ID_SPAN; // ids per page group (2^24)

    /// Page-relative bits `[start, start + count)`.
    fn bits(start: u32, count: u32) -> RoaringBitmap {
        (start..start + count).collect()
    }

    /// One emitted manifest row: `(stream_id, group_start, pages)`.
    type ManifestRow = (String, u64, Vec<(u32, u32)>);

    /// The burst's manifest rows.
    fn manifest_writes(writes: &[ArtifactWrite]) -> Vec<ManifestRow> {
        writes
            .iter()
            .filter_map(|w| match w {
                ArtifactWrite::PageCounts {
                    stream_id,
                    group_start,
                    counts,
                    ..
                } => Some((stream_id.clone(), *group_start, counts.pages.clone())),
                _ => None,
            })
            .collect()
    }

    /// The burst's seal-time page inventories as `(page, stream ids)`.
    fn inventory_writes(writes: &[ArtifactWrite]) -> Vec<(u64, Vec<String>)> {
        writes
            .iter()
            .filter_map(|w| match w {
                ArtifactWrite::OpenStreams {
                    page_start,
                    streams,
                    ..
                } => Some((*page_start, streams.clone())),
                _ => None,
            })
            .collect()
    }

    /// Pins the group-seal manifest contract: NO row exists while the page
    /// group is open (counts accumulate in `FamilyState`), the seal that
    /// crosses the group boundary emits each stream's row exactly once with
    /// every accumulated page, every sealed page stages its full stream
    /// inventory (recovery's enumeration), and a replay of the crossing
    /// re-emits the identical rows.
    #[test]
    fn manifest_rows_emit_once_at_group_completion() {
        let stream_a = skey(0xa);
        let last_page_in_group = page_start_in_group(GROUP - SPAN);

        // A deterministic mid-group position: page 0 sealed for stream A, the
        // rest of group 0 still open.
        let mid_group = || {
            let mut state = FamilyState::default();
            let mut tail = FamilyTail {
                dir: vec![(1, 0, SPAN)],
                ..FamilyTail::default()
            };
            tail.pages.insert((stream_a, 0), bits(0, 3));
            let writes =
                seal_family(Family::Log, &mut state, &mut tail, SPAN, 1).expect("mid-group seal");
            (state, tail, writes)
        };

        let (mut state, mut tail, writes) = mid_group();
        // Mid-group: counts accumulate in state, no manifest row; the sealed
        // page staged its complete inventory.
        assert!(manifest_writes(&writes).is_empty());
        assert_eq!(
            state.sealed_page_counts.get(&(stream_a, 0)),
            Some(&vec![(0, 3)])
        );
        assert_eq!(
            inventory_writes(&writes),
            vec![(0, vec![stream_a.render()])]
        );

        // Fill out group 0 (stream A's last page, stream B's only page) and
        // seal across the group boundary.
        let cross_group = |state: &mut FamilyState, tail: &mut FamilyTail| {
            tail.pages.insert((stream_a, GROUP - SPAN), bits(0, 5));
            tail.pages.insert((skey(0xb), 2 * SPAN), bits(9, 7));
            tail.dir.push((2, SPAN, GROUP));
            seal_family(Family::Log, state, tail, GROUP, 2).expect("crossing seal")
        };
        let writes = cross_group(&mut state, &mut tail);
        let rows = manifest_writes(&writes);
        assert_eq!(
            rows,
            vec![
                (stream_a.render(), 0, vec![(0, 3), (last_page_in_group, 5)]),
                (skey(0xb).render(), 0, vec![(2 * SPAN as u32, 7)]),
            ]
        );
        assert!(
            state.sealed_page_counts.is_empty(),
            "completed group's accumulator entries dropped"
        );

        // Replay: recovery re-runs the crossing from the same mid-group state
        // (the checkpoint carries the accumulator) and must re-emit the
        // identical rows.
        let (mut state, mut tail, _) = mid_group();
        assert_eq!(manifest_writes(&cross_group(&mut state, &mut tail)), rows);
    }

    /// One seal whose boundary jumps multiple page groups emits every
    /// completed group's rows in that single burst — including two rows for
    /// the SAME stream, routed per accumulated group.
    #[test]
    fn manifest_emission_handles_multi_group_crossings() {
        let stream = skey(1);
        let mut state = FamilyState::default();
        let mut tail = FamilyTail {
            dir: vec![(1, 0, 2 * GROUP)],
            ..FamilyTail::default()
        };
        tail.pages.insert((stream, 0), bits(0, 2));
        tail.pages.insert((stream, GROUP + SPAN), bits(3, 4));

        let writes = seal_family(Family::Log, &mut state, &mut tail, 2 * GROUP, 1).expect("seal");
        assert_eq!(
            manifest_writes(&writes),
            vec![
                (stream.render(), 0, vec![(0, 2)]),
                (stream.render(), GROUP, vec![(SPAN as u32, 4)]),
            ]
        );
        assert!(state.sealed_page_counts.is_empty());
    }

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
        // Carry-over in state.dir + batch in tail.dir must compact identically across the seam.
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
            // Entry 2 reaches into the open bucket: it and everything after must survive.
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
        // Frontier 140_000 → open bucket starts at 2*2^16 = 131_072; buckets 0 and 1 seal.
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
        let sealed = seal_family_dir(&mut state, &mut tail, 0, 2 * SPAN).expect("seal");

        // The sealed buckets (the seal-chain digest inputs) cover each sealed
        // span in ascending order.
        assert_eq!(
            sealed.iter().map(|(bs, _)| *bs).collect::<Vec<_>>(),
            vec![0, SPAN]
        );

        assert!(state.dir.is_empty());
        assert_eq!(tail.dir, vec![entry(5, 2 * SPAN, 140_000)]);
    }

    /// An address-kind stream key from a repeated byte (test shorthand).
    fn skey(byte: u8) -> StreamKey {
        StreamKey::new(monad_query_engine::bitmap::IndexKind::Addr, &[byte; 20])
    }

    #[test]
    fn accumulate_family_sets_page_offsets_and_one_dir_entry() {
        let mut tail = FamilyTail::default();
        accumulate_family(&mut tail, 5, 3, 42, 0..3u64, |_record| Ok(vec![skey(1)]))
            .expect("accumulate");

        assert_eq!(tail.dir, vec![(42, 5, 8)]);
        let page = page_start(5);
        let bm = tail.pages.get(&(skey(1), page)).expect("page");
        assert_eq!(bm.iter().collect::<Vec<_>>(), vec![5, 6, 7]);
    }

    #[test]
    fn accumulate_family_stores_bits_page_relative_past_page_zero() {
        // Ids straddling a page boundary land in two pages, each bit relative
        // to ITS page's start.
        let mut tail = FamilyTail::default();
        accumulate_family(&mut tail, PAGE_SPAN - 1, 2, 7, 0..2u64, |_record| {
            Ok(vec![skey(1)])
        })
        .expect("accumulate");

        let last_of_page0 = tail.pages.get(&(skey(1), 0)).expect("page 0");
        assert_eq!(
            last_of_page0.iter().collect::<Vec<_>>(),
            vec![STREAM_PAGE_ID_SPAN - 1]
        );
        let first_of_page1 = tail.pages.get(&(skey(1), PAGE_SPAN)).expect("page 1");
        assert_eq!(first_of_page1.iter().collect::<Vec<_>>(), vec![0]);
    }

    #[test]
    fn accumulate_family_unions_multiple_streams_per_record() {
        let mut tail = FamilyTail::default();
        accumulate_family(&mut tail, 0, 1, 1, std::iter::once(()), |()| {
            Ok(vec![skey(0xa), skey(0xb)])
        })
        .expect("accumulate");

        let page = page_start(0);
        for stream in [skey(0xa), skey(0xb)] {
            let bm = tail.pages.get(&(stream, page)).expect("page");
            assert_eq!(bm.iter().collect::<Vec<_>>(), vec![0]);
        }
    }

    #[test]
    fn accumulate_family_empty_pushes_no_dir_entry() {
        let mut tail = FamilyTail::default();
        accumulate_family(&mut tail, 0, 0, 1, std::iter::empty::<u64>(), |_| {
            Ok(Vec::new())
        })
        .expect("accumulate");
        assert!(tail.dir.is_empty());
        assert!(tail.pages.is_empty());
    }

    #[test]
    fn accumulate_family_propagates_extractor_error() {
        let mut tail = FamilyTail::default();
        let err = accumulate_family(&mut tail, 0, 1, 1, 0..1u64, |_| {
            Err(QueryError::Decode("boom"))
        })
        .expect_err("extractor error propagates");
        assert!(err.to_string().contains("boom"));
    }
}
