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

use monad_query_errors::QueryError;
use monad_query_primitives::{
    limits::{QueryEnvelope, QueryLimits},
    records::PublicationState,
};
use monad_query_read::{api::MonadChainDataService, logs::QueryLogsRequest};
use monad_query_store::{InMemoryBlobStore, InMemoryMetaStore};

#[tokio::test(flavor = "current_thread")]
async fn head_zero_is_treated_as_no_published_blocks() {
    // Block numbers start at 1, so a published head of 0 means "no published blocks".
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    service
        .publication()
        .store_state(PublicationState {
            indexed_finalized_head: 0,
            head_row_chain: Default::default(),
        })
        .await
        .expect("seed head-0 publication row");

    let err = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                limit: 10,
                ..QueryEnvelope::default()
            },
            ..QueryLogsRequest::default()
        })
        .await
        .expect_err("query against head 0 must report no published blocks");

    assert!(
        matches!(err, QueryError::MissingData("no published blocks")),
        "expected MissingData(no published blocks), got {err:?}"
    );
}
