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
    primitives::state::PublicationState, store::CasVersion, FinalizedBlock, InMemoryBlobStore,
    InMemoryMetaStore, MonadChainDataError, MonadChainDataService, QueryLimits, B256,
};

mod common;

use common::{minimal_ingest_tx, test_header};

#[tokio::test(flavor = "current_thread")]
async fn cas_advance_with_stale_version_returns_fenced_out() {
    let meta_store = InMemoryMetaStore::default();
    let service = MonadChainDataService::new(
        meta_store.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![]],
            txs: vec![minimal_ingest_tx()],
        })
        .await
        .expect("first ingest succeeds");

    // Simulate a fenced-out writer: it captured `expected = None` before any
    // publish, then tries to advance after another writer already advanced
    // the head to version 1.
    let outcome = service
        .publication()
        .cas_advance(
            None,
            PublicationState {
                indexed_finalized_head: 1,
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
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![]],
            txs: vec![minimal_ingest_tx()],
        })
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

#[tokio::test(flavor = "current_thread")]
async fn second_writer_capturing_stale_state_is_fenced_at_publish() {
    // Two services over the same store. The "stale" writer captures the
    // current state, then a second writer races ahead and advances the head.
    // When the stale writer tries to publish its own block, CAS fails.
    let meta_store = InMemoryMetaStore::default();
    let stale_writer = MonadChainDataService::new(
        meta_store.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    let active_writer = MonadChainDataService::new(
        meta_store.clone(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    // Stale writer reads the empty initial state (expected_version = None).
    let stale_state = stale_writer.publication().load_state().await.expect("load");
    assert!(stale_state.is_none());

    // Active writer publishes block 1 first.
    active_writer
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![]],
            txs: vec![minimal_ingest_tx()],
        })
        .await
        .expect("active writer publishes block 1");

    // Stale writer now tries to publish using its stale (None) expected
    // version. Should be fenced out.
    let outcome = stale_writer
        .publication()
        .cas_advance(
            None,
            PublicationState {
                indexed_finalized_head: 1,
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
