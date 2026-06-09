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
    engine::{bitmap::STREAM_PAGE_LOCAL_ID_SPAN, primary_dir::DIRECTORY_BUCKET_SIZE},
    Address, Bytes, FinalizedBlock, Log, LogData, LogFilter, LogsRelations, QueryEnvelope,
    QueryLogsRequest, QueryOrder, Topic, B256,
};

mod common;

use common::{chain_header, test_header};

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_logs_respects_and_or_filter_semantics() {
    let store = common::populate::populate_via_engine(vec![FinalizedBlock {
        header: test_header(1, B256::ZERO),
        logs_by_tx: vec![vec![
            log(Address::repeat_byte(1), vec![B256::repeat_byte(9)]),
            log(
                Address::repeat_byte(2),
                vec![B256::repeat_byte(9), B256::repeat_byte(10)],
            ),
            log(
                Address::repeat_byte(3),
                vec![B256::repeat_byte(11), B256::repeat_byte(10)],
            ),
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
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: LogFilter {
                address: Some(HashSet::from([
                    Address::repeat_byte(2),
                    Address::repeat_byte(3),
                ])),
                topics: [
                    Some(HashSet::from([B256::repeat_byte(9)])),
                    Some(HashSet::from([B256::repeat_byte(10)])),
                    None,
                    None,
                ],
            },
            relations: LogsRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(page.logs.len(), 1);
    assert_eq!(page.logs[0].address, Address::repeat_byte(2));
    assert_eq!(page.span.cursor_block.number, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_logs_descending_returns_newest_first() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let store = common::populate::populate_via_engine(vec![
        FinalizedBlock {
            header: h1,
            logs_by_tx: vec![vec![log(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
            )]],
            txs: Vec::new(),
            traces: vec![],
        },
        FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
            ]],
            txs: Vec::new(),
            traces: vec![],
        },
    ])
    .await;
    let service = store.reader();

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                // Spec semantics: in descending order from_block is the upper
                // bound and to_block is the lower bound.
                from_block: Some(2),
                to_block: Some(1),
                order: QueryOrder::Descending,
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
        .expect("query");

    assert_eq!(page.logs.len(), 2);
    assert_eq!(page.logs[0].block_number, 2);
    assert_eq!(page.logs[0].log_index, 1);
    assert_eq!(page.logs[1].block_number, 2);
    assert_eq!(page.logs[1].log_index, 0);
    assert_eq!(page.span.cursor_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_logs_paginates_at_block_boundaries() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let store = common::populate::populate_via_engine(vec![
        FinalizedBlock {
            header: h1,
            logs_by_tx: vec![vec![
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
            ]],
            txs: Vec::new(),
            traces: vec![],
        },
        FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![log(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
            )]],
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
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_logs_scans_across_bucket_and_page_boundaries() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let store = common::populate::populate_via_engine(vec![
        FinalizedBlock {
            header: h1,
            logs_by_tx: vec![repeated_logs(
                Address::repeat_byte(1),
                vec![B256::repeat_byte(3)],
                usize::try_from(STREAM_PAGE_LOCAL_ID_SPAN - 2).expect("page span fits usize"),
            )],
            txs: Vec::new(),
            traces: vec![],
        },
        FinalizedBlock {
            header: h2,
            logs_by_tx: vec![repeated_logs(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
                usize::try_from(DIRECTORY_BUCKET_SIZE + 4)
                    .expect("directory bucket size fits usize"),
            )],
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
                limit: 10,
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
        .expect("query");

    assert_eq!(
        page.logs.len(),
        usize::try_from(DIRECTORY_BUCKET_SIZE + 4).expect("directory bucket size fits usize")
    );
    assert!(page.logs.iter().all(|log| log.block_number == 2));
    assert_eq!(page.span.cursor_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_completes_current_block_when_limit_reached_mid_block() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    let h3 = chain_header(3, &h2);

    let store = common::populate::populate_via_engine(vec![
        FinalizedBlock {
            header: h1,
            logs_by_tx: vec![vec![log(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
            )]],
            txs: Vec::new(),
            traces: vec![],
        },
        FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
            ]],
            txs: Vec::new(),
            traces: vec![],
        },
        FinalizedBlock {
            header: h3,
            logs_by_tx: vec![vec![log(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
            )]],
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
                to_block: Some(3),
                order: QueryOrder::Ascending,
                limit: 2,
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
        .expect("query");

    assert_eq!(page.logs.len(), 6);
    assert!(page.logs.iter().take(1).all(|l| l.block_number == 1));
    assert!(page.logs.iter().skip(1).all(|l| l.block_number == 2));
    assert_eq!(page.span.cursor_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_completes_current_block_when_limit_reached_mid_block_descending() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    let h3 = chain_header(3, &h2);

    let store = common::populate::populate_via_engine(vec![
        FinalizedBlock {
            header: h1,
            logs_by_tx: vec![vec![log(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
            )]],
            txs: Vec::new(),
            traces: vec![],
        },
        FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
            ]],
            txs: Vec::new(),
            traces: vec![],
        },
        FinalizedBlock {
            header: h3,
            logs_by_tx: vec![vec![log(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
            )]],
            txs: Vec::new(),
            traces: vec![],
        },
    ])
    .await;
    let service = store.reader();

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(3),
                to_block: Some(1),
                order: QueryOrder::Descending,
                limit: 2,
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
        .expect("query");

    assert_eq!(page.logs.len(), 6);
    assert!(page.logs.iter().take(1).all(|l| l.block_number == 3));
    assert!(page.logs.iter().skip(1).all(|l| l.block_number == 2));
    assert_eq!(page.span.cursor_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_stops_at_block_when_limit_equals_block_match_count() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let store = common::populate::populate_via_engine(vec![
        FinalizedBlock {
            header: h1,
            logs_by_tx: vec![vec![
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
            ]],
            txs: Vec::new(),
            traces: vec![],
        },
        FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![log(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
            )]],
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
                limit: 2,
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
        .expect("query");

    assert_eq!(page.logs.len(), 2);
    assert!(page.logs.iter().all(|l| l.block_number == 1));
    assert_eq!(page.span.cursor_block.number, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn unindexed_query_logs_still_uses_block_scan_fallback() {
    let store = common::populate::populate_via_engine(vec![FinalizedBlock {
        header: test_header(1, B256::ZERO),
        logs_by_tx: vec![vec![
            log(Address::repeat_byte(5), vec![B256::repeat_byte(8)]),
            log(Address::repeat_byte(6), vec![B256::repeat_byte(9)]),
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
                order: QueryOrder::Ascending,
                limit: 1,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(page.logs.len(), 2);
    assert_eq!(page.span.cursor_block.number, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn historical_indexed_query_resolves_through_the_sealed_summary() {
    // Block 1 fills bucket 0 to one short of the 10k boundary; block 2 crosses
    // it, sealing bucket 0 (its compacted summary is flushed before the
    // publication CAS) and opening bucket 1. A query confined to the sealed
    // range below the frontier must resolve every candidate through the summary
    // path. We assert correctness across the full range to prove the analytic
    // routing (summary for the sealed bucket, fragments for the open one)
    // produces the same locations the probe-or-scan path did.
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let block_1_logs = usize::try_from(DIRECTORY_BUCKET_SIZE - 2).expect("bucket size fits usize");
    let store = common::populate::populate_via_engine(vec![
        FinalizedBlock {
            header: h1,
            logs_by_tx: vec![repeated_logs(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
                block_1_logs,
            )],
            txs: Vec::new(),
            traces: vec![],
        },
        FinalizedBlock {
            header: h2,
            logs_by_tx: vec![repeated_logs(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
                4,
            )],
            txs: Vec::new(),
            traces: vec![],
        },
    ])
    .await;
    let service = store.reader();

    // Bucket 0 is sealed once block 2 crosses the boundary; confirm the summary
    // exists so the resolver's sealed-bucket route has something to read.
    assert!(
        service
            .tables()
            .family(monad_chain_data::Family::Log)
            .load_bucket(0)
            .await
            .expect("load compacted bucket")
            .is_some(),
        "block 2 should have sealed bucket 0",
    );

    // Query the sealed range only (block 1): every candidate id lives in the
    // sealed bucket 0, so the resolver takes the summary path.
    let sealed_page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: usize::try_from(DIRECTORY_BUCKET_SIZE).expect("fits usize"),
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
        .expect("sealed-range query");

    assert_eq!(sealed_page.logs.len(), block_1_logs);
    assert!(sealed_page.logs.iter().all(|l| l.block_number == 1));
    assert_eq!(sealed_page.span.cursor_block.number, 1);

    // Query the full range (sealed bucket 0 + open bucket 1) and confirm the
    // mixed sealed/open routing still returns every log in order.
    let full_page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: usize::try_from(DIRECTORY_BUCKET_SIZE + 8).expect("fits usize"),
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
        .expect("full-range query");

    assert_eq!(full_page.logs.len(), block_1_logs + 4);
    assert!(full_page
        .logs
        .iter()
        .take(block_1_logs)
        .all(|l| l.block_number == 1));
    assert!(full_page
        .logs
        .iter()
        .skip(block_1_logs)
        .all(|l| l.block_number == 2));
    assert_eq!(full_page.span.cursor_block.number, 2);
}

/// The concurrent page/materialize pipeline must keep results globally
/// query-ordered with no sort. Spread matches across many blocks (and thus many
/// work-items) so stage-1 page futures and stage-2 materializations overlap,
/// then assert the emitted order is exactly ascending / descending by block.
#[tokio::test(flavor = "current_thread")]
async fn indexed_query_pipeline_preserves_global_order_across_blocks() {
    let addr = Address::repeat_byte(7);
    let topic = B256::repeat_byte(9);
    let block_count = 25u64;

    let mut blocks = Vec::new();
    let mut prev = test_header(1, B256::ZERO);
    blocks.push(FinalizedBlock {
        header: prev.clone(),
        logs_by_tx: vec![repeated_logs(addr, vec![topic], 3)],
        txs: Vec::new(),
        traces: vec![],
    });
    for number in 2..=block_count {
        let header = chain_header(number, &prev);
        blocks.push(FinalizedBlock {
            header: header.clone(),
            logs_by_tx: vec![repeated_logs(addr, vec![topic], 3)],
            txs: Vec::new(),
            traces: vec![],
        });
        prev = header;
    }

    let store = common::populate::populate_via_engine(blocks).await;
    let service = store.reader();

    let filter = LogFilter {
        address: Some(HashSet::from([addr])),
        topics: [Some(HashSet::from([topic])), None, None, None],
    };

    for order in [QueryOrder::Ascending, QueryOrder::Descending] {
        // from/to are the range start/end *in query order*: ascending runs
        // 1..=block_count, descending runs block_count..=1.
        let (from_block, to_block) = match order {
            QueryOrder::Ascending => (1, block_count),
            QueryOrder::Descending => (block_count, 1),
        };
        let page = service
            .query_logs(QueryLogsRequest {
                envelope: QueryEnvelope {
                    from_block: Some(from_block),
                    to_block: Some(to_block),
                    order,
                    limit: usize::try_from(block_count * 3).expect("fits usize"),
                },
                filter: filter.clone(),
                relations: LogsRelations::default(),
            })
            .await
            .expect("query");

        assert_eq!(page.logs.len(), usize::try_from(block_count * 3).unwrap());
        let mut blocks: Vec<u64> = page.logs.iter().map(|l| l.block_number).collect();
        let mut expected = blocks.clone();
        match order {
            QueryOrder::Ascending => expected.sort_unstable(),
            QueryOrder::Descending => expected.sort_unstable_by(|a, b| b.cmp(a)),
        }
        assert_eq!(blocks, expected, "pipeline must stay globally ordered");
        blocks.dedup();
        assert_eq!(
            blocks.len(),
            usize::try_from(block_count).unwrap(),
            "every block should contribute",
        );
    }
}

/// A single block whose matches span more than one 64K bitmap page produces a
/// multi-page work-list for one shard. Reaching `limit` mid-block must still
/// complete the block and report `cursor_block` for that block — proving the
/// block-aligned stop on the streamed consumer holds across a page boundary.
#[tokio::test(flavor = "current_thread")]
async fn indexed_query_cursor_completes_block_spanning_page_boundary() {
    let addr = Address::repeat_byte(7);
    let topic = B256::repeat_byte(9);
    // One past a full page so the single block's ids occupy pages 0 and 1.
    let span = usize::try_from(STREAM_PAGE_LOCAL_ID_SPAN).expect("page span fits usize");
    let log_count = span + 8;

    let store = common::populate::populate_via_engine(vec![FinalizedBlock {
        header: test_header(1, B256::ZERO),
        logs_by_tx: vec![repeated_logs(addr, vec![topic], log_count)],
        txs: Vec::new(),
        traces: vec![],
    }])
    .await;
    let service = store.reader();

    let filter = LogFilter {
        address: Some(HashSet::from([addr])),
        topics: [Some(HashSet::from([topic])), None, None, None],
    };

    // limit lands inside page 0, but all matches share block 1, so block
    // alignment must drain the whole block (both pages) and the cursor stays at
    // block 1.
    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 5,
            },
            filter,
            relations: LogsRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(page.logs.len(), log_count);
    assert!(page.logs.iter().all(|l| l.block_number == 1));
    assert_eq!(page.span.cursor_block.number, 1);
}

fn repeated_logs(address: Address, topics: Vec<Topic>, count: usize) -> Vec<Log> {
    std::iter::repeat_with(|| log(address, topics.clone()))
        .take(count)
        .collect()
}

fn log(address: Address, topics: Vec<Topic>) -> Log {
    Log {
        address,
        data: LogData::new_unchecked(topics, Bytes::from(vec![1, 2, 3])),
    }
}
