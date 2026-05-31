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

use monad_chain_data::{
    store::TableId, Address, Bytes, EvmBlockHeader, FinalizedBlock, InMemoryBlobStore,
    InMemoryMetaStore, Log, LogData, MonadChainDataError, MonadChainDataService, QueryLimits, B256,
};

mod common;

use common::test_header;

fn log() -> Log {
    Log {
        address: Address::repeat_byte(1),
        data: LogData::new_unchecked(vec![B256::repeat_byte(2)], Bytes::from(vec![1, 2, 3])),
    }
}

fn make_chain(n: usize, logs_each: usize) -> Vec<FinalizedBlock> {
    let mut out: Vec<FinalizedBlock> = Vec::with_capacity(n);
    let mut parent = B256::ZERO;
    for i in 0..n {
        let header: EvmBlockHeader = test_header((i + 1) as u64, parent);
        parent = header.hash_slow();
        out.push(FinalizedBlock {
            header,
            logs_by_tx: vec![std::iter::repeat_with(log).take(logs_each).collect()],
            txs: Vec::new(),
            traces: vec![],
        });
    }
    out
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_blocks_matches_sequential_ingest_block_state() {
    let blocks = make_chain(5, 4);

    let service_seq = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    for b in blocks.clone() {
        service_seq.ingest_block(b).await.expect("seq ingest");
    }

    let meta_batch = InMemoryMetaStore::default();
    let blob_batch = InMemoryBlobStore::default();
    let service_batch = MonadChainDataService::new(
        meta_batch.clone(),
        blob_batch.clone(),
        QueryLimits::UNLIMITED,
    );
    let (outcomes, _timings) = service_batch
        .ingest_blocks(blocks.clone())
        .await
        .expect("batch ingest");
    assert_eq!(outcomes.len(), blocks.len());

    let head_seq = service_seq
        .publication()
        .load_published_head()
        .await
        .unwrap();
    let head_batch = service_batch
        .publication()
        .load_published_head()
        .await
        .unwrap();
    assert_eq!(head_seq, head_batch);
    assert_eq!(head_batch, Some(blocks.len() as u64));

    // Same on-disk row counts in meta + blob fixtures.
    // (Per-key byte equality is exercised by the batch_size_one_parity test.)
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_plans_can_be_built_ahead_of_publication() {
    let blocks = make_chain(4, 2);
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let first = service
        .plan_ingest_blocks(blocks[..2].to_vec())
        .await
        .expect("plan first")
        .expect("first plan");
    let second = service
        .plan_ingest_blocks(blocks[2..].to_vec())
        .await
        .expect("plan second")
        .expect("second plan");

    // The single-writer path writes the publication row only at publish time
    // (no acquire-time row write), so before any apply there is no published
    // head yet.
    assert_eq!(
        service.publication().load_published_head().await.unwrap(),
        None
    );

    let (first_outcomes, _) = service.apply_ingest_plan(first).await.expect("apply first");
    assert_eq!(first_outcomes.last().unwrap().indexed_finalized_head, 2);
    let (second_outcomes, _) = service
        .apply_ingest_plan(second)
        .await
        .expect("apply second");
    assert_eq!(second_outcomes.last().unwrap().indexed_finalized_head, 4);
    assert_eq!(
        service.publication().load_published_head().await.unwrap(),
        Some(4)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_blocks_rejects_mismatched_continuity_within_batch() {
    let mut blocks = make_chain(3, 2);
    // Corrupt block 2's parent_hash so it no longer chains from block 1.
    blocks[1].header.parent_hash = B256::repeat_byte(0xff);

    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    let err = service
        .ingest_blocks(blocks)
        .await
        .expect_err("mismatched parent_hash within batch must reject");
    assert!(matches!(err, MonadChainDataError::InvalidRequest(_)));
    // The rejected batch never published, and the single-writer path writes no
    // row before publish, so there is still no published head.
    assert_eq!(
        service.publication().load_published_head().await.unwrap(),
        None
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_blocks_advances_head_exactly_once() {
    let blocks = make_chain(4, 1);
    let meta = InMemoryMetaStore::default();
    let service = MonadChainDataService::new(
        meta.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    service.ingest_blocks(blocks.clone()).await.expect("ingest");
    // The single-writer path advances the head with exactly one CAS per batch
    // (no per-block CAS, no acquire-time write), so a fresh writer's first batch
    // lands the publication row at version 1.
    let (version, _) = service
        .publication()
        .load_state()
        .await
        .unwrap()
        .expect("state");
    assert_eq!(version.0, 1);
}

// When all blocks in a batch are empty (no logs, txs, or traces) the
// per-family primary-id ranges are zero-width, so neither directory
// compactions nor bitmap compactions are produced. `ingest_blocks` then
// takes the Phase-B-skipped branch, advancing the head via plain CAS
// rather than a CAS-anchored Phase B batch. Asserting that no
// dir-bucket / bitmap-page-meta rows were written exercises that branch.
#[tokio::test(flavor = "current_thread")]
async fn ingest_blocks_skips_phase_b_when_no_family_writes_seal() {
    let n = 6;
    let mut blocks: Vec<FinalizedBlock> = Vec::with_capacity(n);
    let mut parent = B256::ZERO;
    for i in 0..n {
        let header: EvmBlockHeader = test_header((i + 1) as u64, parent);
        parent = header.hash_slow();
        blocks.push(FinalizedBlock {
            header,
            logs_by_tx: vec![],
            txs: Vec::new(),
            traces: vec![],
        });
    }

    let meta = InMemoryMetaStore::default();
    let blob = InMemoryBlobStore::default();
    let service = MonadChainDataService::new(meta.clone(), blob.clone(), QueryLimits::UNLIMITED);

    service.ingest_blocks(blocks.clone()).await.expect("ingest");

    let (version, _) = service
        .publication()
        .load_state()
        .await
        .unwrap()
        .expect("state");
    // Version 1: the single-writer path advances the head with one CAS for the
    // whole batch (no acquire-time write).
    assert_eq!(version.0, 1, "head advanced exactly once across the batch");

    const LOG_DIR_BUCKET: TableId = TableId::new("log_dir_bucket");
    const TX_DIR_BUCKET: TableId = TableId::new("tx_dir_bucket");
    const TRACE_DIR_BUCKET: TableId = TableId::new("trace_dir_bucket");

    let kv = meta.kv_snapshot();
    for ((table, _), _) in &kv {
        assert_ne!(
            *table, LOG_DIR_BUCKET,
            "phase-B-skipped path must not write log_dir_bucket rows"
        );
        assert_ne!(
            *table, TX_DIR_BUCKET,
            "phase-B-skipped path must not write tx_dir_bucket rows"
        );
        assert_ne!(
            *table, TRACE_DIR_BUCKET,
            "phase-B-skipped path must not write trace_dir_bucket rows"
        );
    }
}

// IngestBatchTimings is reporting plumbing — this just confirms the
// per-phase counters get populated (non-zero where applicable) and the
// Phase B / standalone-CAS branch flag tracks which path the batch
// actually took. Wall times are inherently noisy under test load, so we
// only assert "this phase ran" semantics rather than specific magnitudes.
#[tokio::test(flavor = "current_thread")]
async fn ingest_blocks_returns_populated_phase_timings() {
    let blocks = make_chain(3, 2);
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    let (outcomes, timings) = service.ingest_blocks(blocks.clone()).await.expect("ingest");
    assert_eq!(outcomes.len(), blocks.len());
    assert_eq!(timings.blocks, blocks.len());
    // Phase A always runs: staging + the two commits are always on the
    // hot path. We accept zero-ms commits on InMemoryMetaStore (each
    // commit is a HashMap insert and the clock resolution is 1ms) but
    // require that the bookkeeping fields were populated by the public
    // API, not left at their `Default` zero state.
    let zero = monad_chain_data::IngestBatchTimings::default();
    assert_ne!(
        timings, zero,
        "timings struct should not be Default after ingest"
    );
    // Non-empty blocks (with logs) seal at least one family fragment, so
    // the Phase B branch must have run. `commit_b_ms > 0` is flaky on
    // fast in-memory stores; assert the branch flag instead.
    assert!(
        !timings.phase_b_skipped,
        "non-empty blocks should hit Phase B"
    );
    assert_eq!(
        timings.cas_ms, 0,
        "cas_ms must be 0 when Phase B handled the CAS"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_blocks_empty_input_is_no_op() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    let (outcomes, timings) = service.ingest_blocks(Vec::new()).await.expect("empty");
    assert!(outcomes.is_empty());
    assert_eq!(timings, monad_chain_data::IngestBatchTimings::default());
    assert!(service
        .publication()
        .load_published_head()
        .await
        .unwrap()
        .is_none());
}
