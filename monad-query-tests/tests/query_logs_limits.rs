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
/// Asserts `err` is `LimitExceeded` with exactly these fields.
use monad_query_tests::prelude::*;
#[track_caller]
fn assert_limit_exceeded(
    err: MonadChainDataError,
    kind: LimitExceededKind,
    max_limit: usize,
    max_block_range: u64,
) {
    match err {
        MonadChainDataError::LimitExceeded {
            kind: got_kind,
            max_limit: got_max_limit,
            max_block_range: got_max_block_range,
        } => {
            assert_eq!(got_kind, kind);
            assert_eq!(got_max_limit, max_limit);
            assert_eq!(got_max_block_range, max_block_range);
        }
        other => panic!("expected LimitExceeded, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn limit_above_max_limit_returns_limit_exceeded() {
    let service = ingest_three_block_chain(QueryLimits::new(5, 1_000)).await;

    let err = service
        .query_logs(logs_request(
            ascending_envelope(1, 3, 10),
            LogFilter::default(),
        ))
        .await
        .expect_err("limit above max_limit should error");

    assert_limit_exceeded(err, LimitExceededKind::Limit, 5, 1_000);
}

#[tokio::test(flavor = "current_thread")]
async fn block_range_above_max_block_range_returns_limit_exceeded() {
    let service = ingest_three_block_chain(QueryLimits::new(100, 2)).await;

    let err = service
        .query_logs(logs_request(
            ascending_envelope(1, 3, 10),
            LogFilter::default(),
        ))
        .await
        .expect_err("block range above max_block_range should error");

    assert_limit_exceeded(err, LimitExceededKind::BlockRange, 100, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn block_range_at_max_block_range_succeeds() {
    let service = ingest_three_block_chain(QueryLimits::new(100, 3)).await;

    let page = service
        .query_logs(logs_request(
            ascending_envelope(1, 3, 10),
            LogFilter::default(),
        ))
        .await
        .expect("query at max should succeed");

    assert_eq!(page.span.from_block.number, 1);
    assert_eq!(page.span.to_block.number, 3);
}

#[tokio::test(flavor = "current_thread")]
async fn defaulted_block_range_is_bounded_by_max_block_range() {
    // Defaults expand to the full 3-block chain, exceeding the 2-block window.
    let service = ingest_three_block_chain(QueryLimits::new(100, 2)).await;

    let err = service
        .query_logs(QueryLogsRequest {
            // The defaulted (None) endpoints are the property under test.
            envelope: QueryEnvelope {
                from_block: None,
                to_block: None,
                limit: 10,
                ..QueryEnvelope::default()
            },
            ..QueryLogsRequest::default()
        })
        .await
        .expect_err("defaulted full-chain range should be bounded");

    assert!(
        matches!(
            err,
            MonadChainDataError::LimitExceeded {
                kind: LimitExceededKind::BlockRange,
                ..
            }
        ),
        "expected BlockRange LimitExceeded, got {err:?}"
    );
}

async fn ingest_three_block_chain(
    limits: QueryLimits,
) -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    let h3 = chain_header(3, &h2);

    let blocks: Vec<FinalizedBlock> = [h1, h2, h3]
        .into_iter()
        .map(|header| {
            block_with_logs(
                header,
                vec![vec![log(
                    Address::repeat_byte(1),
                    vec![B256::repeat_byte(1)],
                )]],
            )
        })
        .collect();

    let store = populate::populate_via_engine(blocks).await;
    store.reader_with_limits(limits)
}
