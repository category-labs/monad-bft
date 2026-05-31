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
    primitives::state::PublicationState, Address, Bytes, FinalizedBlock, InMemoryBlobStore,
    InMemoryMetaStore, Log, LogData, LogFilter, LogsRelations, MonadChainDataError,
    MonadChainDataService, QueryEnvelope, QueryLimits, QueryLogsRequest, QueryOrder, B256,
};

mod common;

use common::{chain_header, test_header};

#[tokio::test(flavor = "current_thread")]
async fn from_block_above_head_returns_invalid_request() {
    let service = ingest_two_block_chain().await;

    let err = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(50),
                to_block: Some(60),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
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
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(50),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
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
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(2),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
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
            envelope: QueryEnvelope {
                from_block: None,
                to_block: Some(50),
                order: QueryOrder::Descending,
                limit: 10,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
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
            envelope: QueryEnvelope {
                from_block: Some(50),
                to_block: Some(1),
                order: QueryOrder::Descending,
                limit: 100,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
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
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(0),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(page.span.from_block.number, 1);
    assert_eq!(page.span.to_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn descending_inverted_range_returns_invalid_request() {
    let service = ingest_two_block_chain().await;

    // In descending order from_block is the upper bound; from < to is
    // the wrong shape.
    let err = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(2),
                order: QueryOrder::Descending,
                limit: 10,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
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
            envelope: QueryEnvelope {
                from_block: None,
                to_block: None,
                order: QueryOrder::Descending,
                limit: 100,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
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
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(page.span.from_block.number, 1);
    assert_eq!(page.span.to_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn head_zero_is_treated_as_no_published_blocks() {
    // A lease writer that has acquired ownership but not yet published any
    // block leaves the publication row at head 0 (block numbers start at 1, so
    // head 0 means "nothing finalized yet"). A reader must treat this exactly
    // like an unpublished store — "no published blocks" — rather than resolving
    // a range against head 0, which would surface a confusing "block range
    // starts above the published head". Seed the head-0 row directly to isolate
    // the query-path behavior from the lease machinery.
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    service
        .publication()
        .cas_advance(
            None,
            PublicationState {
                indexed_finalized_head: 0,
                owner_id: 7,
                session_id: [1u8; 16],
                lease_valid_through_block: 42,
                head_artifact_checksum: Default::default(),
            },
        )
        .await
        .expect("seed head-0 publication row");

    let err = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: None,
                to_block: None,
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
        .await
        .expect_err("query against head 0 must report no published blocks");

    assert!(
        matches!(err, MonadChainDataError::MissingData("no published blocks")),
        "expected MissingData(no published blocks), got {err:?}"
    );
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
            txs: Vec::new(),
            traces: vec![],
        })
        .await
        .expect("ingest block 1");

    service
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![log(Address::repeat_byte(2), B256::repeat_byte(2))]],
            txs: Vec::new(),
            traces: vec![],
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
