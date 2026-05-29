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

use std::sync::Arc;

use monad_chain_data::{
    primitives::state::PublicationState, store::CasVersion, FinalizedBlock, InMemoryBlobStore,
    InMemoryMetaStore, MonadChainDataError, MonadChainDataService, QueryLimits, B256,
};

mod common;

use common::{minimal_ingest_tx, test_header};

fn empty_block(number: u64, parent: B256) -> FinalizedBlock {
    FinalizedBlock {
        header: test_header(number, parent),
        logs_by_tx: vec![vec![]],
        txs: vec![minimal_ingest_tx()],
        traces: vec![],
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cas_advance_with_stale_version_returns_fenced_out() {
    let meta_store = InMemoryMetaStore::default();
    let service = MonadChainDataService::new(
        meta_store.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    service
        .ingest_block(empty_block(1, B256::ZERO))
        .await
        .expect("first ingest succeeds");

    // Simulate a fenced-out writer driving the inner CAS primitive directly:
    // it captured `expected = None` before any publish, then tries to advance
    // after the publication row already exists. The version mismatch maps to
    // `FencedOut`.
    let outcome = service
        .publication()
        .cas_advance(
            None,
            PublicationState {
                indexed_finalized_head: 1,
                owner_id: 0,
                session_id: [0u8; 16],
                lease_valid_through_block: 0,
            },
        )
        .await;

    match outcome {
        Err(MonadChainDataError::FencedOut { current_head }) => {
            assert_eq!(current_head, Some(1));
        }
        other => panic!("expected FencedOut, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cas_advance_with_wrong_expected_version_returns_fenced_out() {
    let meta_store = InMemoryMetaStore::default();
    let service = MonadChainDataService::new(
        meta_store.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    service
        .ingest_block(empty_block(1, B256::ZERO))
        .await
        .expect("first ingest succeeds");

    // A failover writer that observed an even older state and is now stepping
    // on the active writer. Wrong version, should fail.
    let outcome = service
        .publication()
        .cas_advance(
            Some(CasVersion(99)),
            PublicationState {
                indexed_finalized_head: 1,
                owner_id: 0,
                session_id: [0u8; 16],
                lease_valid_through_block: 0,
            },
        )
        .await;

    assert!(matches!(
        outcome,
        Err(MonadChainDataError::FencedOut {
            current_head: Some(1)
        })
    ));
}

// Two distinct nodes contend over one store. Node A holds the lease (a fixed
// short window); once its lease expires against the observed upstream clock,
// node B takes over and publishes. Node A's subsequent publish must be fenced
// as `LeaseLost` — its owner/session no longer match the row.
#[tokio::test(flavor = "current_thread")]
async fn expired_leader_is_lease_lost_after_standby_takeover() {
    let meta_store = InMemoryMetaStore::default();

    // A shared observed-upstream clock both nodes read. lease_blocks = 4, so a
    // lease taken at observed=10 is valid through block 13.
    let clock_a = Arc::new(std::sync::atomic::AtomicU64::new(10));
    let clock_a_obs = clock_a.clone();
    let node_a = MonadChainDataService::new_reader_writer(
        meta_store.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
        1, // owner_id
        4, // lease_blocks
        1, // renew_threshold
        Arc::new(move || Some(clock_a_obs.load(std::sync::atomic::Ordering::SeqCst))),
    );

    let clock_b = Arc::new(std::sync::atomic::AtomicU64::new(20));
    let clock_b_obs = clock_b.clone();
    let node_b = MonadChainDataService::new_reader_writer(
        meta_store.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
        2, // owner_id
        4, // lease_blocks
        1, // renew_threshold
        Arc::new(move || Some(clock_b_obs.load(std::sync::atomic::Ordering::SeqCst))),
    );

    // Node A acquires and publishes block 1.
    node_a
        .ingest_block(empty_block(1, B256::ZERO))
        .await
        .expect("A publishes block 1");

    // The upstream clock advances past A's lease validity bound (13). B, which
    // observes 20 > 13, takes over and publishes block 2.
    let h1 = test_header(1, B256::ZERO);
    let mut h2 = test_header(2, B256::ZERO);
    h2.parent_hash = h1.hash_slow();
    node_b
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![]],
            txs: vec![minimal_ingest_tx()],
            traces: vec![],
        })
        .await
        .expect("B takes over and publishes block 2");
    assert_eq!(
        node_b.publication().load_published_head().await.unwrap(),
        Some(2)
    );

    // A is now stale. Its cached lease points at the pre-takeover version; the
    // upstream clock has also moved past A's window (advance A's view too), so
    // A's next publish must fail as LeaseLost rather than overwrite B.
    clock_a.store(20, std::sync::atomic::Ordering::SeqCst);
    let mut h2_a = test_header(2, B256::ZERO);
    h2_a.parent_hash = h1.hash_slow();
    let outcome = node_a
        .ingest_block(FinalizedBlock {
            header: h2_a,
            logs_by_tx: vec![vec![]],
            txs: vec![minimal_ingest_tx()],
            traces: vec![],
        })
        .await;
    assert!(
        matches!(outcome, Err(MonadChainDataError::LeaseLost)),
        "expected LeaseLost, got {outcome:?}"
    );

    // B still owns the head; A did not advance it.
    assert_eq!(
        node_b.publication().load_published_head().await.unwrap(),
        Some(2)
    );
}

// A standby observing a still-valid foreign lease must stay passive: its
// begin_write returns LeaseStillFresh and the head does not move.
#[tokio::test(flavor = "current_thread")]
async fn standby_stays_passive_while_lease_is_fresh() {
    let meta_store = InMemoryMetaStore::default();

    let node_a = MonadChainDataService::new_reader_writer(
        meta_store.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
        1,
        10,
        2,
        Arc::new(|| Some(100)),
    );
    // B observes a block well within A's lease window (100..=109).
    let node_b = MonadChainDataService::new_reader_writer(
        meta_store.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
        2,
        10,
        2,
        Arc::new(|| Some(105)),
    );

    node_a
        .ingest_block(empty_block(1, B256::ZERO))
        .await
        .expect("A publishes block 1");

    let h1 = test_header(1, B256::ZERO);
    let mut h2 = test_header(2, B256::ZERO);
    h2.parent_hash = h1.hash_slow();
    let outcome = node_b
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![]],
            txs: vec![minimal_ingest_tx()],
            traces: vec![],
        })
        .await;
    assert!(
        matches!(outcome, Err(MonadChainDataError::LeaseStillFresh)),
        "expected LeaseStillFresh, got {outcome:?}"
    );
    assert_eq!(
        node_a.publication().load_published_head().await.unwrap(),
        Some(1)
    );
}
