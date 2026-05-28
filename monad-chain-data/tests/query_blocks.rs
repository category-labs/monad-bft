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
    Address, EvmBlockHeader, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore,
    LimitExceededKind, MonadChainDataError, MonadChainDataService, QueryBlocksRequest,
    QueryEnvelope, QueryLimits, QueryOrder, B256,
};

mod common;

use common::{chain_header, test_header};

#[tokio::test(flavor = "current_thread")]
async fn query_blocks_ascending_returns_full_range() {
    let service = ingest_three_block_chain(QueryLimits::UNLIMITED).await;

    let page = service
        .query_blocks(QueryBlocksRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(3),
                order: QueryOrder::Ascending,
                limit: 100,
            },
        })
        .await
        .expect("query");

    let numbers: Vec<u64> = page.blocks.iter().map(|b| b.header.number).collect();
    assert_eq!(numbers, vec![1, 2, 3]);
    assert_eq!(page.span.from_block.number, 1);
    assert_eq!(page.span.to_block.number, 3);
    assert_eq!(page.span.cursor_block.number, 3);
}

#[tokio::test(flavor = "current_thread")]
async fn query_blocks_descending_returns_newest_first() {
    let service = ingest_three_block_chain(QueryLimits::UNLIMITED).await;

    let page = service
        .query_blocks(QueryBlocksRequest {
            envelope: QueryEnvelope {
                from_block: Some(3),
                to_block: Some(1),
                order: QueryOrder::Descending,
                limit: 100,
            },
        })
        .await
        .expect("query");

    let numbers: Vec<u64> = page.blocks.iter().map(|b| b.header.number).collect();
    assert_eq!(numbers, vec![3, 2, 1]);
    assert_eq!(page.span.cursor_block.number, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn query_blocks_respects_limit_and_reports_cursor() {
    let (service, hashes) = ingest_three_block_chain_with_hashes(QueryLimits::UNLIMITED).await;

    let page = service
        .query_blocks(QueryBlocksRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(3),
                order: QueryOrder::Ascending,
                limit: 2,
            },
        })
        .await
        .expect("query");

    assert_eq!(page.blocks.len(), 2);
    assert_eq!(page.blocks[0].header.number, 1);
    assert_eq!(page.blocks[0].hash, hashes[0]);
    assert_eq!(page.blocks[1].header.number, 2);
    assert_eq!(page.blocks[1].hash, hashes[1]);
    assert_eq!(page.span.cursor_block.number, 2);
    assert_eq!(page.span.cursor_block.hash, hashes[1]);
}

#[tokio::test(flavor = "current_thread")]
async fn query_blocks_paginates_from_cursor_plus_one() {
    let service = ingest_three_block_chain(QueryLimits::UNLIMITED).await;

    let first = service
        .query_blocks(QueryBlocksRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(3),
                order: QueryOrder::Ascending,
                limit: 2,
            },
        })
        .await
        .expect("first page");

    let second = service
        .query_blocks(QueryBlocksRequest {
            envelope: QueryEnvelope {
                from_block: Some(first.span.cursor_block.number + 1),
                to_block: Some(3),
                order: QueryOrder::Ascending,
                limit: 2,
            },
        })
        .await
        .expect("second page");

    let numbers: Vec<u64> = second.blocks.iter().map(|b| b.header.number).collect();
    assert_eq!(numbers, vec![3]);
    assert_eq!(second.span.cursor_block.number, 3);
}

#[tokio::test(flavor = "current_thread")]
async fn query_blocks_paginates_from_cursor_minus_one_descending() {
    let service = ingest_three_block_chain(QueryLimits::UNLIMITED).await;

    let first = service
        .query_blocks(QueryBlocksRequest {
            envelope: QueryEnvelope {
                from_block: Some(3),
                to_block: Some(1),
                order: QueryOrder::Descending,
                limit: 2,
            },
        })
        .await
        .expect("first page");

    let first_numbers: Vec<u64> = first.blocks.iter().map(|b| b.header.number).collect();
    assert_eq!(first_numbers, vec![3, 2]);
    assert_eq!(first.span.cursor_block.number, 2);

    let second = service
        .query_blocks(QueryBlocksRequest {
            envelope: QueryEnvelope {
                from_block: Some(first.span.cursor_block.number - 1),
                to_block: Some(1),
                order: QueryOrder::Descending,
                limit: 2,
            },
        })
        .await
        .expect("second page");

    let second_numbers: Vec<u64> = second.blocks.iter().map(|b| b.header.number).collect();
    assert_eq!(second_numbers, vec![1]);
    assert_eq!(second.span.cursor_block.number, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn query_blocks_header_fields_round_trip() {
    let (service, hashes) = ingest_three_block_chain_with_hashes(QueryLimits::UNLIMITED).await;

    let page = service
        .query_blocks(QueryBlocksRequest {
            envelope: QueryEnvelope {
                from_block: Some(2),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 10,
            },
        })
        .await
        .expect("query");

    let block = &page.blocks[0];
    assert_eq!(block.hash, hashes[1]);
    assert_eq!(block.header.number, 2);
    assert_eq!(block.header.beneficiary, Address::repeat_byte(0x22));
    assert_eq!(block.header.timestamp, 1_700_000_002);
}

#[tokio::test(flavor = "current_thread")]
async fn query_blocks_limit_zero_returns_invalid_request() {
    let service = ingest_three_block_chain(QueryLimits::UNLIMITED).await;

    let err = service
        .query_blocks(QueryBlocksRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(3),
                order: QueryOrder::Ascending,
                limit: 0,
            },
        })
        .await
        .expect_err("limit=0 should error");

    assert!(matches!(err, MonadChainDataError::InvalidRequest(_)));
}

#[tokio::test(flavor = "current_thread")]
async fn query_blocks_limit_above_max_limit_returns_limit_exceeded() {
    let service = ingest_three_block_chain(QueryLimits::new(5, 1_000)).await;

    let err = service
        .query_blocks(QueryBlocksRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(3),
                order: QueryOrder::Ascending,
                limit: 10,
            },
        })
        .await
        .expect_err("limit above max should error");

    match err {
        MonadChainDataError::LimitExceeded { kind, .. } => {
            assert_eq!(kind, LimitExceededKind::Limit);
        }
        other => panic!("expected LimitExceeded, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn query_blocks_block_range_above_max_block_range_returns_limit_exceeded() {
    let service = ingest_three_block_chain(QueryLimits::new(100, 2)).await;

    let err = service
        .query_blocks(QueryBlocksRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(3),
                order: QueryOrder::Ascending,
                limit: 10,
            },
        })
        .await
        .expect_err("range above max should error");

    match err {
        MonadChainDataError::LimitExceeded { kind, .. } => {
            assert_eq!(kind, LimitExceededKind::BlockRange);
        }
        other => panic!("expected LimitExceeded, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn query_blocks_inverted_ascending_range_returns_invalid_request() {
    let service = ingest_three_block_chain(QueryLimits::UNLIMITED).await;

    let err = service
        .query_blocks(QueryBlocksRequest {
            envelope: QueryEnvelope {
                from_block: Some(3),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 10,
            },
        })
        .await
        .expect_err("from > to should error");

    assert!(matches!(err, MonadChainDataError::InvalidRequest(_)));
}

#[tokio::test(flavor = "current_thread")]
async fn query_blocks_defaulted_range_resolves_to_full_chain() {
    let service = ingest_three_block_chain(QueryLimits::UNLIMITED).await;

    let page = service
        .query_blocks(QueryBlocksRequest::default())
        .await
        .expect("query");

    assert_eq!(page.span.from_block.number, 1);
    assert_eq!(page.span.to_block.number, 3);
    assert_eq!(page.blocks.len(), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn query_blocks_on_empty_chain_returns_missing_data() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let err = service
        .query_blocks(QueryBlocksRequest::default())
        .await
        .expect_err("empty chain should error");

    assert!(matches!(err, MonadChainDataError::MissingData(_)));
}

async fn ingest_three_block_chain(
    limits: QueryLimits,
) -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
    let (service, _) = ingest_three_block_chain_with_hashes(limits).await;
    service
}

async fn ingest_three_block_chain_with_hashes(
    limits: QueryLimits,
) -> (
    MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore>,
    [B256; 3],
) {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        limits,
    );

    let h1 = decorate(test_header(1, B256::ZERO));
    let h2 = decorate(chain_header(2, &h1));
    let h3 = decorate(chain_header(3, &h2));
    let hashes = [h1.hash_slow(), h2.hash_slow(), h3.hash_slow()];

    for header in [h1, h2, h3] {
        service
            .ingest_block(FinalizedBlock {
                header,
                logs_by_tx: vec![vec![]],
                txs: Vec::new(),
            })
            .await
            .expect("ingest block");
    }

    (service, hashes)
}

fn decorate(mut header: EvmBlockHeader) -> EvmBlockHeader {
    header.beneficiary = Address::repeat_byte(((header.number << 4) | header.number) as u8);
    header.timestamp = 1_700_000_000 + header.number;
    header
}
