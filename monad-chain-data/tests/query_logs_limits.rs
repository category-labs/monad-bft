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
    Address, Bytes, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, LimitExceededKind, Log,
    LogData, LogFilter, MonadChainDataError, MonadChainDataService, QueryLimits, QueryLogsRequest,
    QueryOrder, B256,
};

#[tokio::test(flavor = "current_thread")]
async fn limit_above_max_limit_returns_limit_exceeded() {
    let service = ingest_three_block_chain(QueryLimits::new(5, 1_000)).await;

    let err = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(3),
            order: QueryOrder::Ascending,
            limit: 10,
            filter: LogFilter::default(),
        })
        .await
        .expect_err("limit above max_limit should error");

    match err {
        MonadChainDataError::LimitExceeded {
            kind,
            max_limit,
            max_block_range,
        } => {
            assert_eq!(kind, LimitExceededKind::Limit);
            assert_eq!(max_limit, 5);
            assert_eq!(max_block_range, 1_000);
        }
        other => panic!("expected LimitExceeded, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn block_range_above_max_block_range_returns_limit_exceeded() {
    let service = ingest_three_block_chain(QueryLimits::new(100, 2)).await;

    let err = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(3),
            order: QueryOrder::Ascending,
            limit: 10,
            filter: LogFilter::default(),
        })
        .await
        .expect_err("block range above max_block_range should error");

    match err {
        MonadChainDataError::LimitExceeded {
            kind,
            max_limit,
            max_block_range,
        } => {
            assert_eq!(kind, LimitExceededKind::BlockRange);
            assert_eq!(max_limit, 100);
            assert_eq!(max_block_range, 2);
        }
        other => panic!("expected LimitExceeded, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn block_range_at_max_block_range_succeeds() {
    let service = ingest_three_block_chain(QueryLimits::new(100, 3)).await;

    let page = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(3),
            order: QueryOrder::Ascending,
            limit: 10,
            filter: LogFilter::default(),
        })
        .await
        .expect("query at max should succeed");

    assert_eq!(page.from_block.number, 1);
    assert_eq!(page.to_block.number, 3);
}

#[tokio::test(flavor = "current_thread")]
async fn defaulted_block_range_is_bounded_by_max_block_range() {
    // Limits allow a 2-block window; defaults expand to the full chain
    // (3 blocks), which should still be bounded.
    let service = ingest_three_block_chain(QueryLimits::new(100, 2)).await;

    let err = service
        .query_logs(QueryLogsRequest {
            from_block: None,
            to_block: None,
            order: QueryOrder::Ascending,
            limit: 10,
            filter: LogFilter::default(),
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
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        limits,
    );

    for (n, prev_hash) in [
        (1u64, B256::ZERO),
        (2, B256::repeat_byte(1)),
        (3, B256::repeat_byte(2)),
    ] {
        service
            .ingest_block(FinalizedBlock {
                block_number: n,
                block_hash: B256::repeat_byte(n as u8),
                parent_hash: prev_hash,
                logs_by_tx: vec![vec![log()]],
            })
            .await
            .expect("ingest block");
    }

    service
}

fn log() -> Log {
    Log {
        address: Address::repeat_byte(1),
        data: LogData::new_unchecked(vec![B256::repeat_byte(1)], Bytes::from(vec![1, 2, 3])),
    }
}
