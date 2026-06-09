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

use std::collections::HashSet;

use monad_chain_data::{
    Address, Bytes, FinalizedBlock, Log, LogData, LogFilter, LogsRelations, MonadChainDataError,
    PrimaryId, QueryEnvelope, QueryLogsRequest, QueryOrder, B256,
};

mod common;

use common::{chain_header, test_header};

#[tokio::test(flavor = "current_thread")]
async fn query_logs_paginates_at_block_boundaries() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let store = common::populate::populate_via_engine(vec![
        FinalizedBlock {
            header: h1,
            logs_by_tx: vec![vec![
                log(Address::repeat_byte(7), B256::repeat_byte(9)),
                log(Address::repeat_byte(7), B256::repeat_byte(9)),
            ]],
            txs: Vec::new(),
            traces: vec![],
        },
        FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![log(Address::repeat_byte(7), B256::repeat_byte(9))]],
            txs: Vec::new(),
            traces: vec![],
        },
    ])
    .await;
    let service = store.reader();

    let first_page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 1,
            },
            filter: LogFilter {
                address: Some(HashSet::from([Address::repeat_byte(7)])),
                topics: [
                    Some(HashSet::from([B256::repeat_byte(9)])),
                    None,
                    None,
                    None,
                ],
            },
            relations: LogsRelations::default(),
        })
        .await
        .expect("first page");

    assert_eq!(first_page.logs.len(), 2);
    assert_eq!(first_page.span.cursor_block.number, 1);
    assert_eq!(first_page.logs[0].block_number, 1);
    assert_eq!(first_page.logs[1].block_number, 1);

    let second_page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(first_page.span.cursor_block.number + 1),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 1,
            },
            filter: LogFilter {
                address: Some(HashSet::from([Address::repeat_byte(7)])),
                topics: [
                    Some(HashSet::from([B256::repeat_byte(9)])),
                    None,
                    None,
                    None,
                ],
            },
            relations: LogsRelations::default(),
        })
        .await
        .expect("second page");

    assert_eq!(second_page.logs.len(), 1);
    assert_eq!(second_page.span.cursor_block.number, 2);
    assert_eq!(second_page.logs[0].block_number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn query_logs_descending_returns_newest_first() {
    let store = common::populate::populate_via_engine(vec![FinalizedBlock {
        header: test_header(1, B256::ZERO),
        logs_by_tx: vec![vec![
            log(Address::repeat_byte(5), B256::repeat_byte(8)),
            log(Address::repeat_byte(5), B256::repeat_byte(8)),
        ]],
        txs: Vec::new(),
        traces: vec![],
    }])
    .await;
    let service = store.reader();

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Descending,
                limit: 1,
            },
            filter: LogFilter {
                address: Some(HashSet::from([Address::repeat_byte(5)])),
                topics: [
                    Some(HashSet::from([B256::repeat_byte(8)])),
                    None,
                    None,
                    None,
                ],
            },
            relations: LogsRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(page.logs.len(), 2);
    assert_eq!(page.logs[0].log_index, 1);
    assert_eq!(page.logs[1].log_index, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn query_logs_rejects_from_block_above_published_head() {
    let store = common::populate::populate_via_engine(vec![FinalizedBlock {
        header: test_header(1, B256::ZERO),
        logs_by_tx: vec![vec![log(Address::repeat_byte(5), B256::repeat_byte(8))]],
        txs: Vec::new(),
        traces: vec![],
    }])
    .await;
    let service = store.reader();

    let err = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(2),
                to_block: None,
                ..QueryEnvelope::default()
            },
            ..QueryLogsRequest::default()
        })
        .await
        .expect_err("from_block above published head should error");

    assert!(matches!(
        err,
        MonadChainDataError::InvalidRequest("block range starts above the published head")
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_assigns_contiguous_log_id_windows_across_empty_blocks() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    let h3 = chain_header(3, &h2);

    let store = common::populate::populate_via_engine(vec![
        FinalizedBlock {
            header: h1,
            logs_by_tx: vec![vec![
                log(Address::repeat_byte(3), B256::repeat_byte(4)),
                log(Address::repeat_byte(3), B256::repeat_byte(4)),
            ]],
            txs: Vec::new(),
            traces: vec![],
        },
        FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![]],
            txs: Vec::new(),
            traces: vec![],
        },
        FinalizedBlock {
            header: h3,
            logs_by_tx: vec![vec![log(Address::repeat_byte(3), B256::repeat_byte(4))]],
            txs: Vec::new(),
            traces: vec![],
        },
    ])
    .await;
    let service = store.reader();

    let block_1 = service
        .tables()
        .blocks()
        .load_record(1)
        .await
        .expect("load block 1")
        .expect("block 1 record");
    let block_2 = service
        .tables()
        .blocks()
        .load_record(2)
        .await
        .expect("load block 2")
        .expect("block 2 record");
    let block_3 = service
        .tables()
        .blocks()
        .load_record(3)
        .await
        .expect("load block 3")
        .expect("block 3 record");

    assert_eq!(block_1.logs.first_primary_id, PrimaryId::new(0));
    assert_eq!(block_1.logs.count, 2);
    assert_eq!(block_2.logs.first_primary_id, PrimaryId::new(2));
    assert_eq!(block_2.logs.count, 0);
    assert_eq!(block_3.logs.first_primary_id, PrimaryId::new(2));
    assert_eq!(block_3.logs.count, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn block_scan_completes_current_block_when_limit_reached_mid_block() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let store = common::populate::populate_via_engine(vec![
        FinalizedBlock {
            header: h1,
            logs_by_tx: vec![vec![
                log(Address::repeat_byte(5), B256::repeat_byte(8)),
                log(Address::repeat_byte(5), B256::repeat_byte(8)),
                log(Address::repeat_byte(5), B256::repeat_byte(8)),
            ]],
            txs: Vec::new(),
            traces: vec![],
        },
        FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![log(Address::repeat_byte(5), B256::repeat_byte(8))]],
            txs: Vec::new(),
            traces: vec![],
        },
    ])
    .await;
    let service = store.reader();

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 1,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(page.logs.len(), 3);
    assert!(page.logs.iter().all(|l| l.block_number == 1));
    assert_eq!(page.span.cursor_block.number, 1);
}

/// The block-scan runner reads blocks concurrently (buffered over the window).
/// `buffered` preserves input order, so a multi-block unindexed scan must still
/// return blocks in query order and land the cursor on the block where `limit`
/// is reached — identical to a serial walk. Exercise both directions and the
/// block-aligned limit stop.
#[tokio::test(flavor = "current_thread")]
async fn block_scan_orders_blocks_and_lands_cursor_under_concurrency() {
    // Ingest a chain of blocks, each with a single log, so the unindexed scan
    // visits one record per block and the per-block order is unambiguous.
    let mut blocks = Vec::new();
    let mut prev = test_header(1, B256::ZERO);
    blocks.push(FinalizedBlock {
        header: prev.clone(),
        logs_by_tx: vec![vec![log(Address::repeat_byte(5), B256::repeat_byte(8))]],
        txs: Vec::new(),
        traces: vec![],
    });
    for n in 2..=5u64 {
        let h = chain_header(n, &prev);
        blocks.push(FinalizedBlock {
            header: h.clone(),
            logs_by_tx: vec![vec![log(Address::repeat_byte(5), B256::repeat_byte(8))]],
            txs: Vec::new(),
            traces: vec![],
        });
        prev = h;
    }

    let store = common::populate::populate_via_engine(blocks).await;
    let service = store.reader();

    // Ascending, unbounded: blocks 1..=5 in order.
    let asc = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(5),
                order: QueryOrder::Ascending,
                limit: 1_000,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
        .await
        .expect("ascending scan");
    let asc_blocks: Vec<u64> = asc.logs.iter().map(|l| l.block_number).collect();
    assert_eq!(asc_blocks, vec![1, 2, 3, 4, 5]);
    assert_eq!(asc.span.cursor_block.number, 5);

    // Descending, unbounded: blocks 5..=1 in order. For descending, `from_block`
    // is the high (newest) end and `to_block` the low end.
    let desc = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(5),
                to_block: Some(1),
                order: QueryOrder::Descending,
                limit: 1_000,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
        .await
        .expect("descending scan");
    let desc_blocks: Vec<u64> = desc.logs.iter().map(|l| l.block_number).collect();
    assert_eq!(desc_blocks, vec![5, 4, 3, 2, 1]);
    assert_eq!(desc.span.cursor_block.number, 1);

    // Limit stop is block-aligned: with one log per block, `limit = 3` consumes
    // exactly blocks 1..=3 ascending and the cursor lands on block 3.
    let asc_limited = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(5),
                order: QueryOrder::Ascending,
                limit: 3,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
        .await
        .expect("ascending limited scan");
    let limited_blocks: Vec<u64> = asc_limited.logs.iter().map(|l| l.block_number).collect();
    assert_eq!(limited_blocks, vec![1, 2, 3]);
    assert_eq!(asc_limited.span.cursor_block.number, 3);

    // Same in descending: `limit = 3` consumes blocks 5,4,3 and stops on 3.
    let desc_limited = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(5),
                to_block: Some(1),
                order: QueryOrder::Descending,
                limit: 3,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
        .await
        .expect("descending limited scan");
    let desc_limited_blocks: Vec<u64> = desc_limited.logs.iter().map(|l| l.block_number).collect();
    assert_eq!(desc_limited_blocks, vec![5, 4, 3]);
    assert_eq!(desc_limited.span.cursor_block.number, 3);
}

fn log(address: Address, topic0: B256) -> Log {
    Log {
        address,
        data: LogData::new_unchecked(vec![topic0], Bytes::from(vec![1, 2, 3])),
    }
}
