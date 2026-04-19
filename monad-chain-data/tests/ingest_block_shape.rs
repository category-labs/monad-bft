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
    FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, IngestTx, MonadChainDataError,
    MonadChainDataService, QueryLimits, B256,
};

mod common;

use common::test_header;

#[tokio::test(flavor = "current_thread")]
async fn ingest_rejects_mismatched_txs_and_logs_by_tx_lengths() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let err = service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![], vec![]],
            txs: vec![IngestTx::default()],
        })
        .await
        .expect_err("length mismatch should be rejected");

    assert!(
        matches!(err, MonadChainDataError::InvalidRequest(_)),
        "expected InvalidRequest, got {err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_accepts_matching_txs_and_logs_by_tx_lengths() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![], vec![]],
            txs: vec![IngestTx::default(), IngestTx::default()],
        })
        .await
        .expect("matching lengths should ingest");
}
