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
use monad_query_tests::prelude::*;
#[tokio::test(flavor = "current_thread")]
async fn from_block_above_head_returns_invalid_request() {
    let service = ingest_two_block_chain().await;

    let err = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(50),
                to_block: None,
                ..QueryEnvelope::default()
            },
            ..QueryLogsRequest::default()
        })
        .await
        .expect_err("from_block above head should not silently collapse");

    assert!(
        matches!(
            err,
            MonadChainDataError::InvalidRequest("block range starts above the published head")
        ),
        "expected InvalidRequest(block range starts above the published head), got {err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn to_block_above_head_clips_to_head() {
    let service = ingest_two_block_chain().await;

    let page = service
        .query_logs(logs_request(
            ascending_envelope(1, 50, 100),
            LogFilter::default(),
        ))
        .await
        .expect("query");

    assert_eq!(page.span.from_block.number, 1);
    assert_eq!(page.span.to_block.number, 2);
    assert_eq!(page.span.cursor_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn inverted_range_returns_invalid_request() {
    let service = ingest_two_block_chain().await;

    let err = service
        .query_logs(logs_request(
            ascending_envelope(2, 1, 10),
            LogFilter::default(),
        ))
        .await
        .expect_err("from > to should error");

    assert!(
        matches!(err, MonadChainDataError::InvalidRequest(_)),
        "expected InvalidRequest, got {err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn descending_to_block_above_head_returns_invalid_request() {
    let service = ingest_two_block_chain().await;

    // Descending: to_block is the lower bound; from_block is None to dodge the inverted-range path.
    let err = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                to_block: Some(50),
                order: QueryOrder::Descending,
                limit: 10,
                ..QueryEnvelope::default()
            },
            ..QueryLogsRequest::default()
        })
        .await
        .expect_err("to_block above head in desc should not silently collapse");

    assert!(
        matches!(err, MonadChainDataError::InvalidRequest(_)),
        "expected InvalidRequest, got {err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn descending_from_above_head_clips_to_head() {
    let service = ingest_two_block_chain().await;

    // Descending: from_block is the upper bound and clips to head.
    let page = service
        .query_logs(logs_request(
            descending_envelope(50, 1, 100),
            LogFilter::default(),
        ))
        .await
        .expect("query");

    assert_eq!(page.span.from_block.number, 2);
    assert_eq!(page.span.to_block.number, 1);
    assert_eq!(page.span.cursor_block.number, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn from_block_zero_floors_to_earliest_queryable_block() {
    let service = ingest_two_block_chain().await;

    let page = service
        .query_logs(logs_request(
            ascending_envelope(0, 2, 100),
            LogFilter::default(),
        ))
        .await
        .expect("query");

    assert_eq!(page.span.from_block.number, 1);
    assert_eq!(page.span.to_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn descending_inverted_range_returns_invalid_request() {
    let service = ingest_two_block_chain().await;

    // Descending: from_block is the upper bound, so from < to is inverted.
    let err = service
        .query_logs(logs_request(
            descending_envelope(1, 2, 10),
            LogFilter::default(),
        ))
        .await
        .expect_err("descending from < to should error");

    assert!(
        matches!(err, MonadChainDataError::InvalidRequest(_)),
        "expected InvalidRequest, got {err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn descending_defaulted_range_inverts_endpoints() {
    let service = ingest_two_block_chain().await;

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: None,
                to_block: None,
                order: QueryOrder::Descending,
                limit: 100,
            },
            ..QueryLogsRequest::default()
        })
        .await
        .expect("query");

    assert_eq!(page.span.from_block.number, 2);
    assert_eq!(page.span.to_block.number, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn defaulted_range_resolves_to_full_chain() {
    let service = ingest_two_block_chain().await;

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: None,
                to_block: None,
                order: QueryOrder::Ascending,
                limit: 100,
            },
            ..QueryLogsRequest::default()
        })
        .await
        .expect("query");

    assert_eq!(page.span.from_block.number, 1);
    assert_eq!(page.span.to_block.number, 2);
}

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
        matches!(err, MonadChainDataError::MissingData("no published blocks")),
        "expected MissingData(no published blocks), got {err:?}"
    );
}

async fn ingest_two_block_chain() -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let blocks = vec![
        block_with_logs(
            h1,
            vec![vec![log(
                Address::repeat_byte(1),
                vec![B256::repeat_byte(1)],
            )]],
        ),
        block_with_logs(
            h2,
            vec![vec![log(
                Address::repeat_byte(2),
                vec![B256::repeat_byte(2)],
            )]],
        ),
    ];

    let store = populate::populate_via_engine(blocks).await;
    store.reader()
}
