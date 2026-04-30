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
    Address, Bytes, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log, LogData, LogFilter,
    MonadChainDataError, MonadChainDataService, QueryLimits, QueryLogsRequest, QueryOrder, B256,
};

mod common;

use common::{chain_header, test_header};

#[tokio::test(flavor = "current_thread")]
async fn from_block_above_head_returns_invalid_request() {
    let service = ingest_two_block_chain().await;

    let err = service
        .query_logs(QueryLogsRequest {
            from_block: Some(50),
            to_block: Some(60),
            order: QueryOrder::Ascending,
            limit: 10,
            filter: LogFilter::default(),
        })
        .await
        .expect_err("from_block above head should not silently collapse");

    assert!(
        matches!(err, MonadChainDataError::InvalidRequest(_)),
        "expected InvalidRequest, got {err:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn to_block_above_head_clips_to_head() {
    let service = ingest_two_block_chain().await;

    let page = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(50),
            order: QueryOrder::Ascending,
            limit: 100,
            filter: LogFilter::default(),
        })
        .await
        .expect("query");

    assert_eq!(page.from_block.number, 1);
    assert_eq!(page.to_block.number, 2);
    assert_eq!(page.cursor_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn inverted_range_returns_invalid_request() {
    let service = ingest_two_block_chain().await;

    let err = service
        .query_logs(QueryLogsRequest {
            from_block: Some(2),
            to_block: Some(1),
            order: QueryOrder::Ascending,
            limit: 10,
            filter: LogFilter::default(),
        })
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

    // In descending order to_block is the lower numeric bound, so
    // to=50 against head=2 puts the lower bound above head. Leaving
    // from_block unspecified isolates the lower-bound-above-head path
    // from the inverted-range path.
    let err = service
        .query_logs(QueryLogsRequest {
            from_block: None,
            to_block: Some(50),
            order: QueryOrder::Descending,
            limit: 10,
            filter: LogFilter::default(),
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

    // In descending order from_block is the upper bound; values above
    // the published head clip to head, mirroring the ascending
    // to_block-above-head behavior.
    let page = service
        .query_logs(QueryLogsRequest {
            from_block: Some(50),
            to_block: Some(1),
            order: QueryOrder::Descending,
            limit: 100,
            filter: LogFilter::default(),
        })
        .await
        .expect("query");

    assert_eq!(page.from_block.number, 2);
    assert_eq!(page.to_block.number, 1);
    assert_eq!(page.cursor_block.number, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn from_block_zero_floors_to_earliest_queryable_block() {
    let service = ingest_two_block_chain().await;

    let page = service
        .query_logs(QueryLogsRequest {
            from_block: Some(0),
            to_block: Some(2),
            order: QueryOrder::Ascending,
            limit: 100,
            filter: LogFilter::default(),
        })
        .await
        .expect("query");

    assert_eq!(page.from_block.number, 1);
    assert_eq!(page.to_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn descending_inverted_range_returns_invalid_request() {
    let service = ingest_two_block_chain().await;

    // In descending order from_block is the upper bound; from < to is
    // the wrong shape.
    let err = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(2),
            order: QueryOrder::Descending,
            limit: 10,
            filter: LogFilter::default(),
        })
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
            from_block: None,
            to_block: None,
            order: QueryOrder::Descending,
            limit: 100,
            filter: LogFilter::default(),
        })
        .await
        .expect("query");

    assert_eq!(page.from_block.number, 2);
    assert_eq!(page.to_block.number, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn defaulted_range_resolves_to_full_chain() {
    let service = ingest_two_block_chain().await;

    let page = service
        .query_logs(QueryLogsRequest {
            from_block: None,
            to_block: None,
            order: QueryOrder::Ascending,
            limit: 100,
            filter: LogFilter::default(),
        })
        .await
        .expect("query");

    assert_eq!(page.from_block.number, 1);
    assert_eq!(page.to_block.number, 2);
}

async fn ingest_two_block_chain() -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    service
        .ingest_block(FinalizedBlock {
            header: h1,
            logs_by_tx: vec![vec![log(Address::repeat_byte(1), B256::repeat_byte(1))]],
        })
        .await
        .expect("ingest block 1");

    service
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![log(Address::repeat_byte(2), B256::repeat_byte(2))]],
        })
        .await
        .expect("ingest block 2");

    service
}

fn log(address: Address, topic0: B256) -> Log {
    Log {
        address,
        data: LogData::new_unchecked(vec![topic0], Bytes::from(vec![1, 2, 3])),
    }
}
