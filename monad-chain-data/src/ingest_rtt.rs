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

//! Test harness + fixtures for end-to-end ingest round-trips: an in-memory
//! store, a `Vec`-backed block source, a plain-codec resolver, and block
//! builders. Kept in one place so the round-trip tests stay readable.
//!
//! Test blocks are kept within epoch 0 (default `epoch_blocks` is 1M), so the
//! resolver only ever needs the pre-installed version-0 plain codec — no dict
//! training, hence no `Api` dependency.

#![cfg(test)]

use std::{collections::HashSet, future::Future, sync::Arc};

use alloy_primitives::{Address, Bytes, Log, LogData, B256};
use roaring::RoaringBitmap;

use crate::{
    api::MonadChainDataService,
    engine::{
        authority::HeadPublisher,
        bitmap::{encode_bitmap_blob, BitmapBlob},
        family::Family,
        tables::{PublicationTables, Tables},
    },
    error::{MonadChainDataError, Result},
    family::{FinalizedBlock, Hash32},
    ingest_controller::IngestRunConfig,
    ingest_core::{ArtifactWrite, CodecResolver, Codecs, FamilyState, SnapshotStore},
    ingest_helpers::recover_checkpoint,
    ingest_source::ChainDataIngestSource,
    logs::{LogFilter, QueryLogsRequest},
    primitives::{
        limits::{QueryEnvelope, QueryLimits},
        page::QueryOrder,
        EvmBlockHeader,
    },
    store::{BlobStore, CacheConfig, InMemoryBlobStore, InMemoryMetaStore, MetaStore},
};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A `Vec`-backed [`ChainDataIngestSource`]: blocks `[start, start + len)`.
#[derive(Clone)]
struct VecSource {
    blocks: Arc<Vec<FinalizedBlock>>,
    start: u64,
}

impl ChainDataIngestSource for VecSource {
    fn get_latest_uploaded(&self) -> impl Future<Output = eyre::Result<Option<u64>>> + Send {
        let latest = self.start + self.blocks.len() as u64 - 1;
        async move { Ok(Some(latest)) }
    }

    fn fetch_finalized_block(
        &self,
        block_number: u64,
    ) -> impl Future<Output = eyre::Result<FinalizedBlock>> + Send {
        let block = block_number
            .checked_sub(self.start)
            .and_then(|idx| self.blocks.get(idx as usize).cloned());
        async move { block.ok_or_else(|| eyre::eyre!("no block {block_number}")) }
    }
}

/// A [`CodecResolver`] that always returns the pre-installed plain codecs. Valid
/// only within epoch 0 (`version == 0`), which is all the round-trip tests use.
#[derive(Clone)]
struct PlainResolver<M: MetaStore, B: BlobStore> {
    tables: Arc<Tables<M, B>>,
}

impl<M: MetaStore, B: BlobStore> PlainResolver<M, B> {
    fn resolve_sync(&self, version: u32) -> Result<Codecs> {
        let dicts = self.tables.dicts();
        let codec = |family| {
            dicts
                .write_codec(family, version)
                .ok_or(MonadChainDataError::MissingData(
                    "plain codec not installed",
                ))
        };
        Ok(Codecs::new(
            codec(Family::Log)?,
            codec(Family::Tx)?,
            codec(Family::Trace)?,
        ))
    }
}

impl<M: MetaStore, B: BlobStore> CodecResolver for PlainResolver<M, B> {
    fn resolve(&self, version: u32) -> impl Future<Output = Result<Codecs>> + Send {
        // Resolve synchronously (no await) so the returned future is trivially
        // Send and borrows nothing.
        let result = self.resolve_sync(version);
        async move { result }
    }
}

fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

fn topic(byte: u8) -> B256 {
    B256::repeat_byte(byte)
}

/// One block holding `logs` (each `(address_byte, topic0_byte)`) under a single
/// tx group. No txs/traces, so no signed-envelope construction is needed.
fn block_with_logs(number: u64, parent: Hash32, logs: &[(u8, u8)]) -> FinalizedBlock {
    let block_logs = logs
        .iter()
        .map(|&(a, t)| Log {
            address: addr(a),
            data: LogData::new_unchecked(vec![topic(t)], Bytes::new()),
        })
        .collect();
    FinalizedBlock {
        header: EvmBlockHeader {
            number,
            parent_hash: parent,
            ..EvmBlockHeader::default()
        },
        logs_by_tx: vec![block_logs],
        txs: Vec::new(),
        traces: Vec::new(),
    }
}

/// Builds a parent-linked chain (blocks 1..=specs.len()) from per-block log specs.
fn log_chain(specs: &[&[(u8, u8)]]) -> Vec<FinalizedBlock> {
    let mut parent = Hash32::ZERO;
    let mut blocks = Vec::with_capacity(specs.len());
    for (idx, spec) in specs.iter().enumerate() {
        let block = block_with_logs(idx as u64 + 1, parent, spec);
        parent = block.block_hash();
        blocks.push(block);
    }
    blocks
}

/// Standard in-memory wiring shared by the round-trip tests.
struct Harness {
    meta: InMemoryMetaStore,
    blob: InMemoryBlobStore,
    tables: Arc<Tables<InMemoryMetaStore, InMemoryBlobStore>>,
    publication: Arc<PublicationTables<InMemoryMetaStore>>,
    snapshots: SnapshotStore<InMemoryMetaStore>,
}

impl Harness {
    fn new() -> Self {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let tables = Arc::new(Tables::new(meta.clone(), blob.clone()));
        let publication = Arc::new(PublicationTables::new(meta.clone()));
        let snapshots = SnapshotStore::new(meta.clone());
        Self {
            meta,
            blob,
            tables,
            publication,
            snapshots,
        }
    }

    /// A read-only service over the SAME stores the ingest wrote to (clone shares
    /// the underlying Arc<RwLock> state). Reads the published head observationally.
    fn reader(&self) -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
        MonadChainDataService::new_reader_only(
            self.meta.clone(),
            self.blob.clone(),
            QueryLimits::UNLIMITED,
            CacheConfig::default(),
        )
    }

    fn resolver(&self) -> PlainResolver<InMemoryMetaStore, InMemoryBlobStore> {
        PlainResolver {
            tables: self.tables.clone(),
        }
    }

    async fn run_backfill(
        &self,
        blocks: Vec<FinalizedBlock>,
        config: IngestRunConfig,
    ) -> Result<()> {
        let source = VecSource {
            blocks: Arc::new(blocks),
            start: config.start,
        };
        let publisher = Arc::new(HeadPublisher::new(PublicationTables::new(
            self.meta.clone(),
        )));
        crate::ingest_controller::run_ingest_controller(
            source,
            self.tables.clone(),
            publisher,
            self.snapshots.clone(),
            self.resolver(),
            config,
            crate::testkit::TEST_PREFETCH,
        )
        .await
    }
}

fn backfill_config(start: u64, end: u64) -> IngestRunConfig {
    IngestRunConfig {
        start,
        stop_at: Some(end),
        count: None,
        pack_target_bytes: 1 << 20,
        pack_max_blocks: 8,
        // Flush interval == distance to tip (tip_lag_divisor 1): flushes through the
        // fixture, tightening to every block at the tip.
        tip_lag_divisor: 1,
        checkpoint_every_blocks: 3,
        track_buffer: 16,
        poll_ms: 1,
    }
}

fn backfill_config_count(start: u64, count: u64) -> IngestRunConfig {
    IngestRunConfig {
        count: Some(count),
        stop_at: None,
        ..backfill_config(start, 0)
    }
}

// ---------------------------------------------------------------------------
// Smoke test: the full pipeline runs end-to-end against in-memory stores.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn backfill_pipeline_runs_publishes_and_snapshots() {
    let h = Harness::new();
    let blocks = log_chain(&[
        &[(1, 10), (1, 11)],
        &[(2, 10)],
        &[(1, 10)],
        &[(3, 12)],
        &[(2, 11)],
    ]);

    h.run_backfill(blocks, backfill_config(1, 5))
        .await
        .expect("backfill ingest");

    // Producer → data/index tracks → publisher: the head reaches the last block.
    assert_eq!(h.publication.load_published_head().await.unwrap(), Some(5));
    // Both tracks actually wrote: block metadata (data) and index artifacts all
    // land in the shared meta store the reader will later read from.
    assert!(h.tables.blocks().load_record(5).await.unwrap().is_some());
    assert!(!h.meta.is_empty());

    // The terminal checkpoint persisted a snapshot at the last block.
    let (_, _, _, last_block) = recover_checkpoint(&h.snapshots)
        .await
        .unwrap()
        .expect("snapshot persisted");
    assert_eq!(last_block, 5);

    // Sanity: a fresh store cold-starts (recover → None).
    let cold = SnapshotStore::new(InMemoryMetaStore::default());
    assert!(recover_checkpoint(&cold).await.unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Restart recovery (plan invariant I7): a warm resume with no blocks left to
// ingest must still publish the already-durable tail.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn warm_resume_past_end_publishes_durable_tail() {
    let specs: &[&[(u8, u8)]] = &[&[(1, 10)], &[(2, 10)], &[(1, 11)], &[(3, 12)], &[(2, 11)]];
    let h = Harness::new();

    // First run completes: head reaches 5 and the terminal checkpoint snapshots
    // at block 5 (data + index durable through 5).
    h.run_backfill(log_chain(specs), backfill_config(1, 5))
        .await
        .expect("initial backfill");
    assert_eq!(h.publication.load_published_head().await.unwrap(), Some(5));
    let (_, _, _, last_block) = recover_checkpoint(&h.snapshots)
        .await
        .unwrap()
        .expect("snapshot");
    assert_eq!(last_block, 5);

    // Construct the post-crash state: the head regressed below the snapshot /
    // durable frontier, as if the process died after the final checkpoint but
    // before the publisher advanced the head to 5. Rewrite the publication row
    // directly, mutating only the head.
    let mut state = h
        .publication
        .load_state()
        .await
        .unwrap()
        .expect("publication state");
    state.indexed_finalized_head = 2;
    h.publication
        .store_state(state)
        .await
        .expect("regress head for setup");
    assert_eq!(h.publication.load_published_head().await.unwrap(), Some(2));

    // Restart: recover() → last_block 5, so resume = 6 > end = 5. The producer
    // fetches nothing; only the terminal BatchFlush runs. The head must climb
    // back to 5 — which is possible only because `data_durable` is seeded to the
    // resume point (without the seed it stays 0 and min(0, 5) leaves the head at
    // 2). `index_visible` reaches 5 via the unconditional terminal flush.
    h.run_backfill(log_chain(specs), backfill_config(1, 5))
        .await
        .expect("resume backfill");
    assert_eq!(h.publication.load_published_head().await.unwrap(), Some(5));
}

// ---------------------------------------------------------------------------
// `count` is measured from the resume point, not from the cold-start floor: a
// warm resume ingests `count` NEW blocks beyond the durable head rather than
// re-fetching from the beginning.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn backfill_count_is_relative_to_resume() {
    let specs: &[&[(u8, u8)]] = &[&[(1, 10)], &[(2, 10)], &[(1, 11)], &[(3, 12)], &[(2, 11)]];
    let h = Harness::new();

    // Cold start: begin = cold floor (1), count = 3 → ingest blocks 1..=3.
    h.run_backfill(log_chain(specs), backfill_config_count(1, 3))
        .await
        .expect("initial backfill");
    assert_eq!(h.publication.load_published_head().await.unwrap(), Some(3));

    // Warm resume: begin = last_block + 1 = 4, count = 2 → ingest blocks 4..=5
    // (resume + count), reaching head 5 — NOT blocks 1..=2 again.
    h.run_backfill(log_chain(specs), backfill_config_count(1, 2))
        .await
        .expect("resume backfill");
    assert_eq!(h.publication.load_published_head().await.unwrap(), Some(5));
    assert!(h.tables.blocks().load_record(5).await.unwrap().is_some());
}

// ---------------------------------------------------------------------------
// Fragment-based recovery: rebuild the open working set from durable fragments
// (live regime, no checkpoint) and pick max(checkpoint, head).
// ---------------------------------------------------------------------------

/// `OpenState` equality modulo `open_streams_seen` (the checkpoint restores it
/// empty; a rebuild repopulates it — both are correct, see the codec note).
fn assert_family_state_eq(rebuilt: &FamilyState, checkpoint: &FamilyState, family: &str) {
    assert_eq!(rebuilt.pages, checkpoint.pages, "{family} pages");
    assert_eq!(rebuilt.dir, checkpoint.dir, "{family} dir");
    assert_eq!(
        rebuilt.sealed_id, checkpoint.sealed_id,
        "{family} sealed_id"
    );
}

/// Backfill that flushes every block, so the open page accrues a fragment per
/// block under a distinct marker — exactly what a rebuild must union back.
fn flush_each_config(start: u64, end: u64) -> IngestRunConfig {
    IngestRunConfig {
        // A huge divisor makes the interval floor to 1 for any distance => every block.
        tip_lag_divisor: u64::MAX,
        ..backfill_config(start, end)
    }
}

#[tokio::test]
async fn rebuild_from_fragments_matches_checkpoint() {
    // Varied addresses/topics → several streams; multiple blocks → a multi-entry
    // open dir. All ids stay in page 0 (sealed_id == 0), so the whole working set
    // is open and must be reconstructed.
    let specs: &[&[(u8, u8)]] = &[
        &[(1, 10), (2, 11)],
        &[(1, 11)],
        &[(3, 12), (1, 10)],
        &[(2, 10)],
    ];
    let h = Harness::new();
    let n = specs.len() as u64;
    h.run_backfill(log_chain(specs), flush_each_config(1, n))
        .await
        .expect("backfill");
    let head = h.publication.load_published_head().await.unwrap().unwrap();
    assert_eq!(head, n);

    // Ground truth: the checkpoint backfill persisted (terminal flush ⇒ tail empty).
    let (ck_state, _ck_tail, ck_frontier, ck_block) = recover_checkpoint(&h.snapshots)
        .await
        .unwrap()
        .expect("checkpoint");
    assert_eq!(ck_block, head);

    // Rebuild path: same store, but NO checkpoint available (empty snapshot store)
    // — the live regime. `recover` must reconstruct OpenState from the fragments.
    let no_checkpoint = SnapshotStore::new(InMemoryMetaStore::default());
    let crate::ingest_recover::Recovered::Warm {
        state,
        tail,
        frontier,
        resume,
        durable_floor,
    } = crate::ingest_recover::recover(&h.tables, &no_checkpoint, head)
        .await
        .expect("recover")
    else {
        panic!("expected a warm rebuild");
    };

    assert_eq!(resume, head);
    assert_eq!(durable_floor, head);
    // Head is a flush boundary, so the rebuilt tail is empty.
    assert!(tail.log.pages.is_empty() && tail.log.dir.is_empty());
    assert!(tail.tx.pages.is_empty() && tail.trace.pages.is_empty());
    // Frontier reconstructed from the head block record matches the checkpoint's.
    assert_eq!(frontier.log, ck_frontier.log, "log frontier");
    assert_eq!(frontier.tx, ck_frontier.tx, "tx frontier");
    assert_eq!(frontier.trace, ck_frontier.trace, "trace frontier");
    // The reconstructed open state matches the checkpoint exactly.
    assert_family_state_eq(&state.log, &ck_state.log, "log");
    assert_family_state_eq(&state.tx, &ck_state.tx, "tx");
    assert_family_state_eq(&state.trace, &ck_state.trace, "trace");
    // Sanity: the rebuild actually reconstructed bits (not a trivial empty state).
    assert!(!state.log.pages.is_empty(), "log pages rebuilt");
}

#[tokio::test]
async fn rebuild_matches_checkpoint_across_page_boundary() {
    // One block with > 64K logs in a single stream seals page 0, so the OPEN page
    // is page 1 (page_start_local == 65536). Bits are stored shard-local, so the
    // above-head clamp cutoff must be shard-local too: a granule-relative cutoff
    // would treat every bit (all >= 65536) as above-head and wipe the open page.
    // This is the regression guard for that clamp-coordinate bug.
    let span = crate::engine::bitmap::STREAM_PAGE_LOCAL_ID_SPAN as usize;
    let logs: Vec<(u8, u8)> = std::iter::repeat((1u8, 10u8)).take(span + 8).collect();
    let h = Harness::new();
    h.run_backfill(
        vec![block_with_logs(1, Hash32::ZERO, &logs)],
        flush_each_config(1, 1),
    )
    .await
    .expect("backfill");
    let head = 1;

    let (ck_state, _, ck_frontier, _) = recover_checkpoint(&h.snapshots)
        .await
        .unwrap()
        .expect("checkpoint");
    assert!(
        ck_frontier.log > span as u64,
        "fixture must cross a page boundary"
    );

    let no_checkpoint = SnapshotStore::new(InMemoryMetaStore::default());
    let state = warm_state(
        crate::ingest_recover::recover(&h.tables, &no_checkpoint, head)
            .await
            .expect("recover"),
    );

    // sealed_id advanced past page 0, so the open page is page 1.
    assert_eq!(state.log.sealed_id, span as u64, "page 0 sealed");
    assert_family_state_eq(&state.log, &ck_state.log, "log");
    assert!(
        !state.log.pages.is_empty(),
        "open page beyond page 0 must rebuild non-empty (clamp-coordinate regression)"
    );
}

#[tokio::test]
async fn recover_prefers_checkpoint_when_ahead_of_head() {
    // Backfill leaves a checkpoint at block n. Simulate a crash where the head
    // regressed below it (checkpoint persisted, publisher hadn't caught up):
    // max(checkpoint, head) = checkpoint, so recover resumes from n, not the head.
    let specs: &[&[(u8, u8)]] = &[&[(1, 10)], &[(2, 11)], &[(1, 12)], &[(3, 10)]];
    let h = Harness::new();
    let n = specs.len() as u64;
    h.run_backfill(log_chain(specs), flush_each_config(1, n))
        .await
        .expect("backfill");

    let crate::ingest_recover::Recovered::Warm {
        resume,
        durable_floor,
        ..
    } = crate::ingest_recover::recover(&h.tables, &h.snapshots, n - 2)
        .await
        .expect("recover")
    else {
        panic!("expected a warm restore");
    };
    assert_eq!(resume, n, "checkpoint (ahead of head) wins");
    assert_eq!(durable_floor, n);
}

#[tokio::test]
async fn rebuild_clamps_fragments_above_head() {
    // Single stream keeps the assertion simple. sealed_id == 0, so a local id
    // equals its global id.
    let specs: &[&[(u8, u8)]] = &[&[(1, 10)], &[(1, 10)], &[(1, 10)]];
    let h = Harness::new();
    let n = specs.len() as u64;
    h.run_backfill(log_chain(specs), flush_each_config(1, n))
        .await
        .expect("backfill");
    let head = h.publication.load_published_head().await.unwrap().unwrap();

    // Baseline rebuild before any injected skew.
    let no_checkpoint = SnapshotStore::new(InMemoryMetaStore::default());
    let baseline = warm_state(
        crate::ingest_recover::recover(&h.tables, &no_checkpoint, head)
            .await
            .expect("baseline recover"),
    );

    // Simulate the index track flushing AHEAD of the published head: inject a
    // fragment carrying the next block's id (>= the head frontier) into the open
    // page, under a marker above the head. A naive union would publish it.
    let (_, _, frontier, _) = recover_checkpoint(&h.snapshots).await.unwrap().unwrap();
    let bound = frontier.log; // exclusive id frontier at head; sealed_id == 0
    let stream = h
        .tables
        .family(Family::Log)
        .load_open_bitmap_streams(0)
        .await
        .unwrap()
        .into_iter()
        .next()
        .expect("an open log stream");
    let mut bm = RoaringBitmap::new();
    bm.insert(bound as u32); // local id == bound ⇒ belongs to block head + 1
    let blob = encode_bitmap_blob(&BitmapBlob {
        min_local: bound as u32,
        max_local: bound as u32,
        count: 1,
        bitmap: bm,
    })
    .unwrap();
    let inject_page = ArtifactWrite::PageFragment {
        family: Family::Log,
        stream_id: stream,
        page_start_local: 0,
        marker: head + 1,
        blob,
    };
    // Also inject an above-head DIR fragment (block head+1's id range) — the other
    // half of the clamp. It lands in the open bucket and must be dropped.
    let inject_dir = ArtifactWrite::DirFragment {
        family: Family::Log,
        fragment: crate::engine::primary_dir::PrimaryDirFragment {
            block_number: head + 1,
            first_primary_id: bound,
            end_primary_id_exclusive: bound + 1,
        },
    };
    h.tables
        .with_writes(move |w| {
            Box::pin(async move {
                crate::ingest_helpers::stage_artifact_write(w, inject_page)?;
                crate::ingest_helpers::stage_artifact_write(w, inject_dir)
            })
        })
        .await
        .expect("inject out-of-range artifacts");

    // The rebuild must clamp both injected (above-head) artifacts, yielding exactly
    // the pre-injection state.
    let clamped = warm_state(
        crate::ingest_recover::recover(&h.tables, &no_checkpoint, head)
            .await
            .expect("clamped recover"),
    );
    assert_eq!(
        clamped.log.pages, baseline.log.pages,
        "clamped pages == baseline"
    );
    assert_eq!(clamped.log.dir, baseline.log.dir, "clamped dir == baseline");
    for bm in clamped.log.pages.values() {
        assert!(
            bm.max().unwrap() < bound as u32,
            "no bit at/above the head frontier survived the clamp"
        );
    }
    assert!(
        clamped.log.dir.iter().all(|&(_, first, _)| first < bound),
        "no dir entry at/above the head frontier survived the clamp"
    );
}

/// Unwrap a `Recovered::Warm` to its `OpenState` for assertions.
fn warm_state(recovered: crate::ingest_recover::Recovered) -> crate::ingest_core::OpenState {
    match recovered {
        crate::ingest_recover::Recovered::Warm { state, .. } => state,
        crate::ingest_recover::Recovered::Cold => panic!("expected a warm rebuild"),
    }
}

// ---------------------------------------------------------------------------
// Round-trip: ingest logs, then read them back through the query API.
// ---------------------------------------------------------------------------

/// An ascending log query filtering on a single address, over [from, to].
fn logs_by_address(from: u64, to: u64, address_byte: u8) -> QueryLogsRequest {
    QueryLogsRequest {
        envelope: QueryEnvelope {
            from_block: Some(from),
            to_block: Some(to),
            order: QueryOrder::Ascending,
            limit: 1000,
        },
        filter: LogFilter {
            address: Some(HashSet::from([addr(address_byte)])),
            topics: [None, None, None, None],
        },
        relations: Default::default(),
    }
}

#[tokio::test]
async fn round_trip_query_logs_by_address() {
    let h = Harness::new();
    // addr 1 emits in blocks 1 (x2) and 3 (x1); addr 2 in blocks 2 and 5;
    // addr 3 in block 4. All ids stay well under one page (2^16), so the whole
    // index lives in the open region — this exercises the fragment read path.
    let blocks = log_chain(&[
        &[(1, 10), (1, 11)],
        &[(2, 10)],
        &[(1, 10)],
        &[(3, 12)],
        &[(2, 11)],
    ]);
    h.run_backfill(blocks, backfill_config(1, 5))
        .await
        .expect("backfill ingest");

    let reader = h.reader();

    // addr 1 → three logs, in block order 1,1,3, all carrying addr(1).
    let resp = reader
        .query_logs(logs_by_address(1, 5, 1))
        .await
        .expect("query addr 1");
    assert_eq!(
        resp.logs.iter().map(|l| l.block_number).collect::<Vec<_>>(),
        vec![1, 1, 3],
    );
    assert!(resp.logs.iter().all(|l| l.address == addr(1)));
    // Topics round-trip: block 1's two logs are topic 10 then 11.
    assert_eq!(resp.logs[0].topics, vec![topic(10)]);
    assert_eq!(resp.logs[1].topics, vec![topic(11)]);

    // addr 2 → two logs in blocks 2 and 5.
    let resp = reader
        .query_logs(logs_by_address(1, 5, 2))
        .await
        .expect("query addr 2");
    assert_eq!(
        resp.logs.iter().map(|l| l.block_number).collect::<Vec<_>>(),
        vec![2, 5],
    );

    // An address that never emitted → no logs.
    let resp = reader
        .query_logs(logs_by_address(1, 5, 9))
        .await
        .expect("query addr 9");
    assert!(resp.logs.is_empty());

    // Range narrowing: addr 1 restricted to [2, 5] drops block 1's two logs.
    let resp = reader
        .query_logs(logs_by_address(2, 5, 1))
        .await
        .expect("query addr 1 narrowed");
    assert_eq!(
        resp.logs.iter().map(|l| l.block_number).collect::<Vec<_>>(),
        vec![3],
    );
}
