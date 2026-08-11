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

//! `max_block_range` semantics of window resolution: explicit spans above the
//! cap are rejected, at-cap spans succeed, and open-ended (defaulted)
//! endpoints clamp to a bounded first page instead of failing.

use monad_query_engine::{
    range::ResolvedBlockWindow,
    tables::{DictConfig, QueryRuntimeConfig, Tables},
    test_util::stage_block,
};
use monad_query_errors::{LimitExceededKind, QueryError};
use monad_query_primitives::{
    limits::{QueryEnvelope, QueryLimits},
    order::QueryOrder,
};
use monad_query_store::{CacheConfig, InMemoryBlobStore, InMemoryMetaStore};

const HEAD: u64 = 3;

async fn three_block_tables() -> Tables<InMemoryMetaStore, InMemoryBlobStore> {
    let tables = Tables::with_all_configs(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        CacheConfig::default(),
        DictConfig::default(),
        QueryRuntimeConfig::default(),
    );
    for number in 1..=HEAD {
        stage_block(&tables, number).await;
    }
    tables
}

fn envelope(from: Option<u64>, to: Option<u64>, order: QueryOrder) -> QueryEnvelope {
    QueryEnvelope {
        from_block: from,
        to_block: to,
        order,
        limit: 10,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_span_above_max_block_range_is_rejected() {
    let tables = three_block_tables().await;
    let limits = QueryLimits::new(100, 2);

    let err = ResolvedBlockWindow::resolve(
        &envelope(Some(1), Some(3), QueryOrder::Ascending),
        HEAD,
        &limits,
        tables.blocks(),
    )
    .await
    .expect_err("span 3 must exceed max_block_range 2");
    assert!(
        matches!(
            err,
            QueryError::LimitExceeded {
                kind: LimitExceededKind::BlockRange,
                max_limit: 100,
                max_block_range: 2,
            }
        ),
        "got {err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_span_at_max_block_range_succeeds() {
    let tables = three_block_tables().await;
    let limits = QueryLimits::new(100, 2);

    let window = ResolvedBlockWindow::resolve(
        &envelope(Some(2), Some(3), QueryOrder::Ascending),
        HEAD,
        &limits,
        tables.blocks(),
    )
    .await
    .expect("at-cap span resolves");
    let (from, to) = window.request_endpoints(QueryOrder::Ascending);
    assert_eq!((from.number, to.number), (2, 3));
}

#[tokio::test(flavor = "current_thread")]
async fn defaulted_ascending_range_clamps_to_first_page() {
    let tables = three_block_tables().await;
    let limits = QueryLimits::new(100, 2);

    let window = ResolvedBlockWindow::resolve(
        &envelope(None, None, QueryOrder::Ascending),
        HEAD,
        &limits,
        tables.blocks(),
    )
    .await
    .expect("open-ended range clamps instead of failing");
    let (from, to) = window.request_endpoints(QueryOrder::Ascending);
    assert_eq!((from.number, to.number), (1, 2));
}

#[tokio::test(flavor = "current_thread")]
async fn defaulted_descending_range_clamps_to_newest_page() {
    let tables = three_block_tables().await;
    let limits = QueryLimits::new(100, 2);

    let window = ResolvedBlockWindow::resolve(
        &envelope(None, None, QueryOrder::Descending),
        HEAD,
        &limits,
        tables.blocks(),
    )
    .await
    .expect("open-ended descending range clamps instead of failing");
    let (from, to) = window.request_endpoints(QueryOrder::Descending);
    assert_eq!((from.number, to.number), (3, 2));
}
