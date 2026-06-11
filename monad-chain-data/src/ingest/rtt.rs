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

//! End-to-end ingest round-trip tests over in-memory stores (epoch 0, plain codecs only).

use std::{collections::HashSet, sync::Arc};

use alloy_primitives::{Address, Bytes, Log, LogData, B256};
use roaring::RoaringBitmap;

use crate::{
    api::MonadChainDataService,
    engine::{
        bitmap::{
            encode_bitmap_blob, render_stream_id, DecodedBitmapFragment, StreamKey,
            STREAM_PAGE_ID_SPAN,
        },
        digest::EMPTY_DIGEST,
        family::Family,
        tables::{PublicationTables, Tables},
    },
    error::Result,
    ingest::{
        index::{stage_artifact_write, ArtifactWrite, FamilyState, OpenState},
        recover::{recover, Recovered},
        resolver::TablesCodecResolver,
        run_ingest,
        snapshot::recover_checkpoint,
        IngestRunConfig, PackConfig, PayloadMode, SignalPolicy, SnapshotStore,
    },
    ingest_types::{FinalizedBlock, Hash32},
    logs::{LogFilter, QueryLogsRequest},
    primitives::{
        limits::{QueryEnvelope, QueryLimits},
        order::QueryOrder,
        EvmBlockHeader,
    },
    store::{InMemoryBlobStore, InMemoryMetaStore},
    testkit::{VecSource, TEST_PREFETCH},
};

fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

fn topic(byte: u8) -> B256 {
    B256::repeat_byte(byte)
}

/// One block of `(address_byte, topic0_byte)` logs under a single tx group.
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
        external: None,
    }
}

/// Builds a parent-linked chain (blocks `first_number..`) from per-block log specs.
fn chain_from(first_number: u64, specs: &[&[(u8, u8)]]) -> Vec<FinalizedBlock> {
    let mut parent = Hash32::ZERO;
    let mut blocks = Vec::with_capacity(specs.len());
    for (idx, spec) in specs.iter().enumerate() {
        let block = block_with_logs(first_number + idx as u64, parent, spec);
        parent = block.block_hash();
        blocks.push(block);
    }
    blocks
}

/// Like [`chain_from`] but anchored at block 1.
fn log_chain(specs: &[&[(u8, u8)]]) -> Vec<FinalizedBlock> {
    chain_from(1, specs)
}

/// A snapshot store with no checkpoint: forces the live-rebuild regime.
fn empty_snapshots() -> SnapshotStore<InMemoryMetaStore, InMemoryBlobStore> {
    SnapshotStore::new(InMemoryMetaStore::default(), InMemoryBlobStore::default())
}

/// Standard in-memory wiring shared by the round-trip tests.
struct Harness {
    meta: InMemoryMetaStore,
    blob: InMemoryBlobStore,
    tables: Arc<Tables<InMemoryMetaStore, InMemoryBlobStore>>,
    publication: Arc<PublicationTables<InMemoryMetaStore>>,
    snapshots: SnapshotStore<InMemoryMetaStore, InMemoryBlobStore>,
}

impl Harness {
    fn new() -> Self {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let tables = Arc::new(Tables::new(meta.clone(), blob.clone()));
        let publication = Arc::new(PublicationTables::new(meta.clone()));
        let snapshots = SnapshotStore::new(meta.clone(), blob.clone());
        Self {
            meta,
            blob,
            tables,
            publication,
            snapshots,
        }
    }

    /// Read-only service over the same stores the ingest wrote to.
    fn reader(&self) -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
        MonadChainDataService::new(self.meta.clone(), self.blob.clone(), QueryLimits::UNLIMITED)
    }

    fn resolver(&self) -> TablesCodecResolver<InMemoryMetaStore, InMemoryBlobStore> {
        TablesCodecResolver::new(self.tables.clone())
    }

    async fn run_backfill(
        &self,
        blocks: Vec<FinalizedBlock>,
        config: IngestRunConfig,
    ) -> Result<()> {
        self.run_backfill_with_snapshots(blocks, config, self.snapshots.clone())
            .await
    }

    /// Like [`Self::run_backfill`] but with an explicit snapshot store, so a
    /// run can resume WITHOUT a checkpoint (the live-rebuild regime).
    async fn run_backfill_with_snapshots(
        &self,
        blocks: Vec<FinalizedBlock>,
        config: IngestRunConfig,
        snapshots: SnapshotStore<InMemoryMetaStore, InMemoryBlobStore>,
    ) -> Result<()> {
        let source = VecSource::new(blocks, config.start);
        let publisher = self.publication.clone();
        run_ingest(
            source,
            self.tables.clone(),
            publisher,
            snapshots,
            self.resolver(),
            config,
            TEST_PREFETCH,
        )
        .await
    }

    /// Block `n`'s stored chained artifact checksum.
    async fn checksum_at(&self, n: u64) -> Hash32 {
        self.tables
            .blocks()
            .load_record(n)
            .await
            .unwrap()
            .expect("block record")
            .row_chain
    }
}

fn backfill_config(start: u64, end: u64) -> IngestRunConfig {
    IngestRunConfig {
        start,
        end: Some(end),
        count: None,
        pack: PackConfig {
            target_bytes: 1 << 20,
            max_blocks: 8,
        },
        policy: SignalPolicy {
            tip_lag_divisor: 1,
            checkpoint_every_blocks: 3,
        },
        payload: PayloadMode::Native,
        track_buffer: 16,
        poll_ms: 1,
    }
}

fn backfill_config_count(start: u64, count: u64) -> IngestRunConfig {
    IngestRunConfig {
        count: Some(count),
        end: None,
        ..backfill_config(start, 0)
    }
}

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

    assert_eq!(h.publication.load_published_head().await.unwrap(), Some(5));
    assert!(h.tables.blocks().load_record(5).await.unwrap().is_some());
    assert!(!h.meta.is_empty());

    // The publisher carries the head block's row chain into the publication row.
    let state = h
        .publication
        .load_state()
        .await
        .unwrap()
        .expect("publication state");
    assert_eq!(state.head_row_chain, h.checksum_at(5).await);
    assert_ne!(state.head_row_chain, EMPTY_DIGEST);

    let (_, _, _, last_block) = recover_checkpoint(&h.snapshots)
        .await
        .unwrap()
        .expect("snapshot persisted");
    assert_eq!(last_block, 5);

    let cold = empty_snapshots();
    assert!(recover_checkpoint(&cold).await.unwrap().is_none());
}

// Plan invariant I7: a warm resume with nothing left to ingest still publishes the durable tail.
#[tokio::test]
async fn warm_resume_past_end_publishes_durable_tail() {
    let specs: &[&[(u8, u8)]] = &[&[(1, 10)], &[(2, 10)], &[(1, 11)], &[(3, 12)], &[(2, 11)]];
    let h = Harness::new();

    h.run_backfill(log_chain(specs), backfill_config(1, 5))
        .await
        .expect("initial backfill");
    assert_eq!(h.publication.load_published_head().await.unwrap(), Some(5));
    let (_, _, _, last_block) = recover_checkpoint(&h.snapshots)
        .await
        .unwrap()
        .expect("snapshot");
    assert_eq!(last_block, 5);

    // Simulate a crash after checkpoint but before publish: regress the head.
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

    // Resume = 6 > end = 5: only the terminal BatchFlush runs; the head recovers
    // to 5 because that flush records the resume block as a boundary already
    // covered by the seeded data frontier.
    h.run_backfill(log_chain(specs), backfill_config(1, 5))
        .await
        .expect("resume backfill");
    assert_eq!(h.publication.load_published_head().await.unwrap(), Some(5));
}

#[tokio::test]
async fn backfill_count_is_relative_to_resume() {
    let specs: &[&[(u8, u8)]] = &[&[(1, 10)], &[(2, 10)], &[(1, 11)], &[(3, 12)], &[(2, 11)]];
    let h = Harness::new();

    h.run_backfill(log_chain(specs), backfill_config_count(1, 3))
        .await
        .expect("initial backfill");
    assert_eq!(h.publication.load_published_head().await.unwrap(), Some(3));

    // Warm resume: begin = 4, count = 2 → blocks 4..=5, not 1..=2 again.
    h.run_backfill(log_chain(specs), backfill_config_count(1, 2))
        .await
        .expect("resume backfill");
    assert_eq!(h.publication.load_published_head().await.unwrap(), Some(5));
    assert!(h.tables.blocks().load_record(5).await.unwrap().is_some());
}

/// Equality modulo `open_streams_seen` (checkpoint restores it empty; rebuild repopulates it).
fn assert_family_state_eq(rebuilt: &FamilyState, checkpoint: &FamilyState, family: &str) {
    assert_eq!(rebuilt.pages, checkpoint.pages, "{family} pages");
    assert_eq!(rebuilt.dir, checkpoint.dir, "{family} dir");
    assert_eq!(
        rebuilt.sealed_id, checkpoint.sealed_id,
        "{family} sealed_id"
    );
    assert_eq!(
        rebuilt.seal_chain, checkpoint.seal_chain,
        "{family} seal_chain"
    );
}

fn flush_each_config(start: u64, end: u64) -> IngestRunConfig {
    let mut config = backfill_config(start, end);
    // u64::MAX divisor floors the flush interval to 1 (every block).
    config.policy.tip_lag_divisor = u64::MAX;
    config
}

#[tokio::test]
async fn rebuild_from_fragments_matches_checkpoint() {
    // All ids stay in page 0 (sealed_id == 0): the whole working set is open.
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

    let (ck_state, _ck_tail, ck_frontier, ck_block) = recover_checkpoint(&h.snapshots)
        .await
        .unwrap()
        .expect("checkpoint");
    assert_eq!(ck_block, head);

    let no_checkpoint = empty_snapshots();
    let Recovered::Warm {
        state,
        tail,
        frontier,
        resume,
    } = recover(&h.tables, &no_checkpoint, head)
        .await
        .expect("recover")
    else {
        panic!("expected a warm rebuild");
    };

    assert_eq!(resume, head);
    // Head is a flush boundary, so the rebuilt tail is empty.
    assert!(tail.log.pages.is_empty(), "log tail pages");
    assert!(tail.log.dir.is_empty(), "log tail dir");
    assert!(tail.tx.pages.is_empty(), "tx tail pages");
    assert!(tail.trace.pages.is_empty(), "trace tail pages");
    assert_eq!(frontier.log, ck_frontier.log, "log frontier");
    assert_eq!(frontier.tx, ck_frontier.tx, "tx frontier");
    assert_eq!(frontier.trace, ck_frontier.trace, "trace frontier");
    assert_family_state_eq(&state.log, &ck_state.log, "log");
    assert_family_state_eq(&state.tx, &ck_state.tx, "tx");
    assert_family_state_eq(&state.trace, &ck_state.trace, "trace");
    assert!(!state.log.pages.is_empty(), "log pages rebuilt");
}

#[tokio::test]
async fn rebuild_matches_checkpoint_across_page_boundary() {
    // Clamp-coordinate regression: bits are page-relative, so the above-head
    // clamp cutoff must be too — an absolute-id cutoff would wipe open page 1.
    let span = STREAM_PAGE_ID_SPAN as usize;
    let logs = vec![(1u8, 10u8); span + 8];
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

    let no_checkpoint = empty_snapshots();
    let state = warm_state(
        recover(&h.tables, &no_checkpoint, head)
            .await
            .expect("recover"),
    );

    assert_eq!(state.log.sealed_id, span as u64, "page 0 sealed");
    assert_family_state_eq(&state.log, &ck_state.log, "log");
    assert!(
        !state.log.pages.is_empty(),
        "open page beyond page 0 must rebuild non-empty (clamp-coordinate regression)"
    );
}

#[tokio::test]
async fn recover_prefers_checkpoint_when_ahead_of_head() {
    let specs: &[&[(u8, u8)]] = &[&[(1, 10)], &[(2, 11)], &[(1, 12)], &[(3, 10)]];
    let h = Harness::new();
    let n = specs.len() as u64;
    h.run_backfill(log_chain(specs), flush_each_config(1, n))
        .await
        .expect("backfill");

    let Recovered::Warm { resume, .. } = recover(&h.tables, &h.snapshots, n - 2)
        .await
        .expect("recover")
    else {
        panic!("expected a warm restore");
    };
    assert_eq!(resume, n, "checkpoint (ahead of head) wins");
}

#[tokio::test]
async fn rebuild_clamps_fragments_above_head() {
    // Single stream; sealed_id == 0, so a page offset equals its global id.
    let specs: &[&[(u8, u8)]] = &[&[(1, 10)], &[(1, 10)], &[(1, 10)]];
    let h = Harness::new();
    let n = specs.len() as u64;
    h.run_backfill(log_chain(specs), flush_each_config(1, n))
        .await
        .expect("backfill");
    let head = h.publication.load_published_head().await.unwrap().unwrap();

    let no_checkpoint = empty_snapshots();
    let baseline = warm_state(
        recover(&h.tables, &no_checkpoint, head)
            .await
            .expect("baseline recover"),
    );

    // Simulate the index track flushing ahead of the published head: inject a
    // fragment for the next block under a flush_block above the head.
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
    bm.insert(bound as u32); // page offset == bound (page 0) ⇒ block head + 1
    let blob = encode_bitmap_blob(&DecodedBitmapFragment {
        min_offset: bound as u32,
        max_offset: bound as u32,
        count: 1,
        bitmap: bm,
    })
    .unwrap();
    let inject_page = ArtifactWrite::PageFragment {
        family: Family::Log,
        stream_id: stream,
        page_start: 0,
        flush_block: head + 1,
        blob,
    };
    // Also inject an above-head DIR fragment — the other half of the clamp.
    let inject_dir = ArtifactWrite::DirFragment {
        family: Family::Log,
        block: head + 1,
        first_id: bound,
        end_id: bound + 1,
    };
    h.tables
        .with_writes(move |w| {
            Box::pin(async move {
                stage_artifact_write(w, inject_page)?;
                stage_artifact_write(w, inject_dir)
            })
        })
        .await
        .expect("inject out-of-range artifacts");

    let clamped = warm_state(
        recover(&h.tables, &no_checkpoint, head)
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

fn warm_state(recovered: Recovered) -> OpenState {
    match recovered {
        Recovered::Warm { state, .. } => state,
        Recovered::Cold => panic!("expected a warm rebuild"),
    }
}

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
    // All ids stay under one page (2^16): exercises the open-region fragment read path.
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

    let resp = reader
        .query_logs(logs_by_address(1, 5, 1))
        .await
        .expect("query addr 1");
    assert_eq!(
        resp.logs.iter().map(|l| l.block_number).collect::<Vec<_>>(),
        vec![1, 1, 3],
    );
    assert!(resp.logs.iter().all(|l| l.address == addr(1)));
    assert_eq!(resp.logs[0].topics, vec![topic(10)]);
    assert_eq!(resp.logs[1].topics, vec![topic(11)]);

    let resp = reader
        .query_logs(logs_by_address(1, 5, 2))
        .await
        .expect("query addr 2");
    assert_eq!(
        resp.logs.iter().map(|l| l.block_number).collect::<Vec<_>>(),
        vec![2, 5],
    );

    let resp = reader
        .query_logs(logs_by_address(1, 5, 9))
        .await
        .expect("query addr 9");
    assert!(resp.logs.is_empty());

    let resp = reader
        .query_logs(logs_by_address(2, 5, 1))
        .await
        .expect("query addr 1 narrowed");
    assert_eq!(
        resp.logs.iter().map(|l| l.block_number).collect::<Vec<_>>(),
        vec![3],
    );
}

// Standby checksum chains (batch i): the row chain and per-family seal chains
// must be deterministic functions of the logical block stream — independent of
// flush cadence, pack boundaries, and restarts — and sensitive to content.

/// A flush-every-block config with per-block packs and a different checkpoint
/// cadence: the maximally-different timing profile vs [`backfill_config`].
fn busy_cadence_config(start: u64, end: u64) -> IngestRunConfig {
    let mut config = flush_each_config(start, end);
    config.policy.checkpoint_every_blocks = 2;
    config.pack = PackConfig {
        target_bytes: 1,
        max_blocks: 1,
    };
    config
}

#[tokio::test]
async fn row_chain_is_flush_and_pack_cadence_independent() {
    let specs: &[&[(u8, u8)]] = &[
        &[(1, 10), (1, 11)],
        &[(2, 10)],
        &[(1, 12)],
        &[(3, 10), (2, 12)],
        &[(2, 11)],
        &[(1, 10)],
    ];
    let a = Harness::new();
    a.run_backfill(log_chain(specs), backfill_config(1, 6))
        .await
        .expect("run A");
    let b = Harness::new();
    b.run_backfill(log_chain(specs), busy_cadence_config(1, 6))
        .await
        .expect("run B");

    let mut prev = EMPTY_DIGEST;
    for n in 1..=6 {
        let expected = a.checksum_at(n).await;
        assert_eq!(b.checksum_at(n).await, expected, "height {n}");
        assert_ne!(expected, prev, "chain must advance at height {n}");
        prev = expected;
    }
}

#[tokio::test]
async fn row_chain_continues_across_restart_both_regimes() {
    let specs: &[&[(u8, u8)]] = &[&[(1, 10)], &[(2, 11)], &[(1, 12)], &[(3, 10)], &[(2, 12)]];
    let full = Harness::new();
    full.run_backfill(log_chain(specs), backfill_config(1, 5))
        .await
        .expect("uninterrupted run");

    // Checkpoint regime: the resumed leg seeds the chain from block 3's record
    // (resume comes from the terminal checkpoint).
    let ck = Harness::new();
    ck.run_backfill(log_chain(specs), backfill_config(1, 3))
        .await
        .expect("checkpoint leg 1");
    ck.run_backfill(log_chain(specs), backfill_config(1, 5))
        .await
        .expect("checkpoint resume");

    // Live-rebuild regime: no checkpoint, so recovery rebuilds from the head
    // and the chain seeds from the head block's stored record.
    let rb = Harness::new();
    rb.run_backfill(log_chain(specs), backfill_config(1, 3))
        .await
        .expect("rebuild leg 1");
    rb.run_backfill_with_snapshots(log_chain(specs), backfill_config(1, 5), empty_snapshots())
        .await
        .expect("rebuild resume");

    for n in 1..=5 {
        let expected = full.checksum_at(n).await;
        assert_eq!(
            ck.checksum_at(n).await,
            expected,
            "checkpoint regime at {n}"
        );
        assert_eq!(rb.checksum_at(n).await, expected, "rebuild regime at {n}");
    }
}

// Warm resume whose last durable block is genesis (block 0): the seed must
// fold block 0's record, not treat `resume == 0` as "nothing durable". Leg 1
// ingests only block 0 (terminal checkpoint lands at 0); the resumed leg seeds
// from block 0's stored record. Every height must match an uninterrupted 0..=N
// run — a `resume == 0 => EMPTY_DIGEST` seed would silently diverge here.
#[tokio::test]
async fn row_chain_warm_resume_at_genesis_matches_uninterrupted() {
    let specs: &[&[(u8, u8)]] = &[&[(1, 10)], &[(2, 11)], &[(1, 12)], &[(3, 10)]];
    // Genesis-anchored chain (block numbers 0..=N), unlike `log_chain` (from 1).
    let genesis_chain = || chain_from(0, specs);

    let full = Harness::new();
    full.run_backfill(genesis_chain(), backfill_config(0, 3))
        .await
        .expect("uninterrupted run");

    // Resumed run: leg 1 stops at genesis (terminal checkpoint at block 0).
    let resumed = Harness::new();
    resumed
        .run_backfill(genesis_chain(), backfill_config(0, 0))
        .await
        .expect("genesis leg");
    // The resume seeds the chain from block 0's record before ingesting 1..=3.
    resumed
        .run_backfill(genesis_chain(), backfill_config(0, 3))
        .await
        .expect("resume past genesis");

    for n in 0..=3 {
        assert_eq!(
            resumed.checksum_at(n).await,
            full.checksum_at(n).await,
            "warm-resume-at-genesis must match uninterrupted at height {n}"
        );
    }
}

#[tokio::test]
async fn row_chain_diverges_from_perturbed_height_onward() {
    let clean: &[&[(u8, u8)]] = &[&[(1, 10)], &[(2, 11)], &[(1, 12)], &[(3, 10)], &[(2, 12)]];
    // Same chain except one row of block 3.
    let perturbed: &[&[(u8, u8)]] = &[&[(1, 10)], &[(2, 11)], &[(1, 13)], &[(3, 10)], &[(2, 12)]];

    let a = Harness::new();
    a.run_backfill(log_chain(clean), backfill_config(1, 5))
        .await
        .expect("clean run");
    let p = Harness::new();
    p.run_backfill(log_chain(perturbed), backfill_config(1, 5))
        .await
        .expect("perturbed run");

    for n in 1..=2 {
        assert_eq!(
            a.checksum_at(n).await,
            p.checksum_at(n).await,
            "pre-perturbation height {n} must agree"
        );
    }
    for n in 3..=5 {
        assert_ne!(
            a.checksum_at(n).await,
            p.checksum_at(n).await,
            "height {n} must diverge after the perturbation"
        );
    }
}

/// Logs per bulk block; 8 blocks total 132_000 log ids, sealing spans 0 and 1
/// of the log family (span = 64K ids).
const BULK_BLOCK_LOGS: usize = 16_500;

/// Per-block log specs for 8 identical heavy blocks.
fn bulk_specs() -> Vec<Vec<(u8, u8)>> {
    let block: Vec<(u8, u8)> = (0..BULK_BLOCK_LOGS)
        .map(|i| ((i % 3) as u8 + 1, 10 + (i % 5) as u8))
        .collect();
    vec![block; 8]
}

fn bulk_blocks(specs: &[Vec<(u8, u8)>]) -> Vec<FinalizedBlock> {
    let refs: Vec<&[(u8, u8)]> = specs.iter().map(|v| v.as_slice()).collect();
    log_chain(&refs)
}

async fn log_seal_row(h: &Harness, span_start: u64) -> Hash32 {
    h.tables
        .family(Family::Log)
        .load_seal_chain(span_start)
        .await
        .unwrap()
        .expect("persisted seal chain row")
}

// The mandatory cadence-independence proof for the seal chains: a run that
// flushes every block writes many more open-page fragments than a coarse run,
// yet the sealed-span content (and therefore the chain) is identical. A
// fragments-based digest would fail this test. Also covers both restart
// regimes (span 1 seals in the resumed leg, continuing from span 0's chain)
// and content sensitivity.
#[tokio::test]
async fn seal_chains_are_cadence_independent_and_survive_restart() {
    let span = u64::from(STREAM_PAGE_ID_SPAN);
    let specs = bulk_specs();

    // A: coarse flush cadence (interval tracks the distance to the tip).
    let a = Harness::new();
    a.run_backfill(bulk_blocks(&specs), backfill_config(1, 8))
        .await
        .expect("run A");

    // B: flush every block, per-block packs, different checkpoint cadence.
    let b = Harness::new();
    b.run_backfill(bulk_blocks(&specs), busy_cadence_config(1, 8))
        .await
        .expect("run B");

    // C: live-rebuild restart after span 0 sealed (no checkpoint for the
    // resumed leg); span 1 seals later from the RECOVERED persisted chain row.
    let c = Harness::new();
    c.run_backfill(bulk_blocks(&specs), backfill_config(1, 5))
        .await
        .expect("run C leg 1");
    c.run_backfill_with_snapshots(
        bulk_blocks(&specs),
        backfill_config(1, 8),
        empty_snapshots(),
    )
    .await
    .expect("run C leg 2");

    // D: checkpoint-regime restart at the same point.
    let d = Harness::new();
    d.run_backfill(bulk_blocks(&specs), backfill_config(1, 5))
        .await
        .expect("run D leg 1");
    d.run_backfill(bulk_blocks(&specs), backfill_config(1, 8))
        .await
        .expect("run D leg 2");

    let span0 = log_seal_row(&a, 0).await;
    let span1 = log_seal_row(&a, span).await;
    assert_ne!(span0, EMPTY_DIGEST);
    assert_ne!(span1, span0);
    for (h, name) in [(&b, "B"), (&c, "C"), (&d, "D")] {
        assert_eq!(log_seal_row(h, 0).await, span0, "run {name} span 0");
        assert_eq!(log_seal_row(h, span).await, span1, "run {name} span 1");
    }

    // Group-seal manifest contract: spans 0 and 1 sealed, but page group 0
    // (2^24 ids) is still open, so NO manifest row exists in any regime — the
    // sealed-page counts accumulate in state instead, counts matching the
    // sealed artifacts (content-derived, never cadence-derived).
    let stream = render_stream_id("addr", addr(1).as_slice());
    for (h, name) in [(&a, "A"), (&b, "B"), (&c, "C"), (&d, "D")] {
        assert_eq!(
            h.tables
                .family(Family::Log)
                .load_bitmap_page_counts(&stream, 0)
                .await
                .unwrap(),
            None,
            "run {name}: no manifest row before the group completes"
        );
    }
    let fam = a.tables.family(Family::Log);
    let mut expected_pairs = Vec::new();
    for page_start in [0, span] {
        let artifact = fam
            .load_bitmap_page_artifact(&stream, page_start)
            .await
            .unwrap()
            .expect("sealed page artifact");
        expected_pairs.push((page_start as u32, artifact.meta.count));
    }
    // Checkpoint regime: the terminal checkpoint carries the accumulator.
    let (ckpt_state, _, _, _) = recover_checkpoint(&a.snapshots)
        .await
        .unwrap()
        .expect("terminal checkpoint");
    assert_eq!(
        ckpt_state
            .log
            .sealed_page_counts
            .get(&(StreamKey::parse(&stream).expect("stream key"), 0)),
        Some(&expected_pairs),
        "accumulator carries one (page, count) per sealed page"
    );
    // No-checkpoint live rebuild: the accumulator re-derives from the sealed
    // artifacts plus the seal-staged page inventories and must reproduce the
    // checkpoint regime's accumulator exactly (across every stream).
    let Recovered::Warm { state: rebuilt, .. } =
        recover(&a.tables, &empty_snapshots(), 8).await.unwrap()
    else {
        panic!("published head must recover warm");
    };
    assert_eq!(
        rebuilt.log.sealed_page_counts, ckpt_state.log.sealed_page_counts,
        "live rebuild reproduces the checkpoint accumulator"
    );
    // The row chain agrees at every height across all four runs too.
    for n in 1..=8 {
        let expected = a.checksum_at(n).await;
        for (h, name) in [(&b, "B"), (&c, "C"), (&d, "D")] {
            assert_eq!(h.checksum_at(n).await, expected, "run {name} height {n}");
        }
    }

    // The terminal checkpoint carries the running chain (== last sealed row).
    assert_eq!(ckpt_state.log.seal_chain, span1);

    // Verification accessor: row chain + per-family (last sealed span, chain).
    let digests = a
        .reader()
        .standby_digests(8)
        .await
        .unwrap()
        .expect("standby digests");
    assert_eq!(digests.row_chain, a.checksum_at(8).await);
    let log_point = digests.seal_chains.log.expect("log seal chain point");
    assert_eq!((log_point.span_start, log_point.value), (span, span1));
    assert!(digests.seal_chains.tx.is_none(), "tx family never sealed");
    assert!(
        digests.seal_chains.trace.is_none(),
        "trace family never sealed"
    );

    // Sensitivity: one perturbed row inside span 0 changes a sealed page, so
    // the seal chain diverges at that span. Four blocks suffice to seal it.
    let e = Harness::new();
    let mut perturbed = bulk_specs();
    perturbed[0][7] = (9, 99); // flips one log's streams inside sealed span 0
    e.run_backfill(bulk_blocks(&perturbed[..4]), backfill_config(1, 4))
        .await
        .expect("run E");
    assert_ne!(
        log_seal_row(&e, 0).await,
        span0,
        "perturbed span 0 diverges"
    );
}
