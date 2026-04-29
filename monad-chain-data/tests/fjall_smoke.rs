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

#![cfg(feature = "fjall")]

use monad_chain_data::{
    primitives::state::PublicationState, store::FjallStore, Family, FinalizedBlock,
    MonadChainDataError, MonadChainDataService, QueryLimits, B256,
};
use tempfile::tempdir;

mod common;

use common::{chain_header, minimal_ingest_tx, test_header};

#[tokio::test(flavor = "current_thread")]
async fn fjall_round_trip_two_block_ingest() {
    let dir = tempdir().expect("tempdir");
    let store = FjallStore::open(dir.path()).expect("open fjall");
    let service = MonadChainDataService::new(store.clone(), store, QueryLimits::UNLIMITED);

    let h1 = test_header(1, B256::ZERO);
    let outcome1 = service
        .ingest_block(FinalizedBlock {
            header: h1.clone(),
            logs_by_tx: vec![vec![], vec![]],
            txs: vec![minimal_ingest_tx(), minimal_ingest_tx()],
        })
        .await
        .expect("ingest block 1");
    assert_eq!(outcome1.written_txs, 2);

    let h2 = chain_header(2, &h1);
    let outcome2 = service
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![]],
            txs: vec![minimal_ingest_tx()],
        })
        .await
        .expect("ingest block 2");
    assert_eq!(outcome2.written_txs, 1);
    assert_eq!(outcome2.indexed_finalized_head, 2);

    // Round-trip read of the published state.
    let published_head = service
        .publication()
        .load_published_head()
        .await
        .expect("load head");
    assert_eq!(published_head, Some(2));

    // Round-trip read of a per-family block header from the blob path.
    let tx_family = service.tables().family(Family::Tx);
    let header_bytes = tx_family
        .load_block_header(1)
        .await
        .expect("load tx header")
        .expect("present");
    assert!(!header_bytes.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn fjall_persists_across_reopen() {
    let dir = tempdir().expect("tempdir");

    {
        let store = FjallStore::open(dir.path()).expect("open fjall");
        let service = MonadChainDataService::new(store.clone(), store, QueryLimits::UNLIMITED);
        service
            .ingest_block(FinalizedBlock {
                header: test_header(1, B256::ZERO),
                logs_by_tx: vec![vec![]],
                txs: vec![minimal_ingest_tx()],
            })
            .await
            .expect("ingest");
    }

    let store = FjallStore::open(dir.path()).expect("reopen fjall");
    let service = MonadChainDataService::new(store.clone(), store, QueryLimits::UNLIMITED);
    let head = service
        .publication()
        .load_published_head()
        .await
        .expect("load head");
    assert_eq!(head, Some(1));
}

#[tokio::test(flavor = "current_thread")]
async fn fjall_cas_advance_with_stale_version_returns_fenced_out() {
    let dir = tempdir().expect("tempdir");
    let store = FjallStore::open(dir.path()).expect("open fjall");
    let service = MonadChainDataService::new(store.clone(), store, QueryLimits::UNLIMITED);

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![]],
            txs: vec![minimal_ingest_tx()],
        })
        .await
        .expect("first ingest");

    let outcome = service
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
