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
    engine::{bitmap::STREAM_PAGE_ID_SPAN, primary_dir::DIRECTORY_BUCKET_SIZE},
    Address, InMemoryBlobStore, InMemoryMetaStore, LogFilter, LogsRelations, MonadChainDataService,
    QueryLogsRequest, QueryOrder, B256,
};

mod common;

use common::{
    ascending_envelope, block_with_logs, chain_header, chain_of_blocks, descending_envelope, log,
    log_filter, logs_request, repeated_logs, test_header,
};

/// Lossless `usize` views of the compile-time span constants.
const PAGE_SPAN: usize = STREAM_PAGE_ID_SPAN as usize;
const BUCKET: usize = DIRECTORY_BUCKET_SIZE as usize;

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_logs_respects_and_or_filter_semantics() {
    let store = common::populate::populate_via_engine(vec![block_with_logs(
        test_header(1, B256::ZERO),
        vec![vec![
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
    )])
    .await;
    let service = store.reader();

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: ascending_envelope(1, 1, 10),
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
        block_with_logs(
            h1,
            vec![vec![log(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
            )]],
        ),
        block_with_logs(
            h2,
            vec![vec![
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
            ]],
        ),
    ])
    .await;
    let service = store.reader();

    // Spec semantics: in descending order from_block is the upper bound and
    // to_block is the lower bound.
    let page = service
        .query_logs(logs_request(
            descending_envelope(2, 1, 1),
            log_filter(Address::repeat_byte(7), B256::repeat_byte(9)),
        ))
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
        block_with_logs(
            h1,
            vec![vec![
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
            ]],
        ),
        block_with_logs(
            h2,
            vec![vec![log(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
            )]],
        ),
    ])
    .await;
    let service = store.reader();

    let first_page = service
        .query_logs(logs_request(
            ascending_envelope(1, 2, 1),
            log_filter(Address::repeat_byte(7), B256::repeat_byte(9)),
        ))
        .await
        .expect("first page");

    assert_eq!(first_page.logs.len(), 2);
    assert_eq!(first_page.span.cursor_block.number, 1);

    let second_page = service
        .query_logs(logs_request(
            ascending_envelope(first_page.span.cursor_block.number + 1, 2, 1),
            log_filter(Address::repeat_byte(7), B256::repeat_byte(9)),
        ))
        .await
        .expect("second page");

    assert_eq!(second_page.logs.len(), 1);
    assert_eq!(second_page.span.cursor_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_logs_scans_across_bucket_and_page_boundaries() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let block_2_logs = BUCKET + 4;
    let store = common::populate::populate_via_engine(vec![
        block_with_logs(
            h1,
            vec![repeated_logs(
                Address::repeat_byte(1),
                vec![B256::repeat_byte(3)],
                PAGE_SPAN - 2,
            )],
        ),
        block_with_logs(
            h2,
            vec![repeated_logs(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
                block_2_logs,
            )],
        ),
    ])
    .await;
    let service = store.reader();

    let page = service
        .query_logs(logs_request(
            ascending_envelope(1, 2, 10),
            log_filter(Address::repeat_byte(7), B256::repeat_byte(9)),
        ))
        .await
        .expect("query");

    assert_eq!(page.logs.len(), block_2_logs);
    assert!(page.logs.iter().all(|log| log.block_number == 2));
    assert_eq!(page.span.cursor_block.number, 2);
}

/// Three-block chain holding 1, 5, and 1 matching logs for addr 7 / topic 9.
async fn one_five_one_log_chain() -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
    let blocks = chain_of_blocks(3, |number| {
        let count = if number == 2 { 5 } else { 1 };
        vec![repeated_logs(
            Address::repeat_byte(7),
            vec![B256::repeat_byte(9)],
            count,
        )]
    });
    common::populate::populate_via_engine(blocks).await.reader()
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_completes_current_block_when_limit_reached_mid_block() {
    let service = one_five_one_log_chain().await;

    let page = service
        .query_logs(logs_request(
            ascending_envelope(1, 3, 2),
            log_filter(Address::repeat_byte(7), B256::repeat_byte(9)),
        ))
        .await
        .expect("query");

    assert_eq!(page.logs.len(), 6);
    assert_eq!(page.logs[0].block_number, 1);
    assert!(page.logs.iter().skip(1).all(|l| l.block_number == 2));
    assert_eq!(page.span.cursor_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_completes_current_block_when_limit_reached_mid_block_descending() {
    let service = one_five_one_log_chain().await;

    let page = service
        .query_logs(logs_request(
            descending_envelope(3, 1, 2),
            log_filter(Address::repeat_byte(7), B256::repeat_byte(9)),
        ))
        .await
        .expect("query");

    assert_eq!(page.logs.len(), 6);
    assert_eq!(page.logs[0].block_number, 3);
    assert!(page.logs.iter().skip(1).all(|l| l.block_number == 2));
    assert_eq!(page.span.cursor_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_stops_at_block_when_limit_equals_block_match_count() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let store = common::populate::populate_via_engine(vec![
        block_with_logs(
            h1,
            vec![vec![
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
            ]],
        ),
        block_with_logs(
            h2,
            vec![vec![log(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
            )]],
        ),
    ])
    .await;
    let service = store.reader();

    let page = service
        .query_logs(logs_request(
            ascending_envelope(1, 2, 2),
            log_filter(Address::repeat_byte(7), B256::repeat_byte(9)),
        ))
        .await
        .expect("query");

    assert_eq!(page.logs.len(), 2);
    assert!(page.logs.iter().all(|l| l.block_number == 1));
    assert_eq!(page.span.cursor_block.number, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn historical_indexed_query_resolves_through_the_sealed_summary() {
    // Block 1 fills bucket 0 to one short of the boundary; block 2 crosses it, sealing bucket 0.
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let block_1_logs = BUCKET - 2;
    let store = common::populate::populate_via_engine(vec![
        block_with_logs(
            h1,
            vec![repeated_logs(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
                block_1_logs,
            )],
        ),
        block_with_logs(
            h2,
            vec![repeated_logs(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
                4,
            )],
        ),
    ])
    .await;
    let service = store.reader();

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

    let sealed_page = service
        .query_logs(logs_request(
            ascending_envelope(1, 1, BUCKET),
            log_filter(Address::repeat_byte(7), B256::repeat_byte(9)),
        ))
        .await
        .expect("sealed-range query");

    assert_eq!(sealed_page.logs.len(), block_1_logs);
    assert!(sealed_page.logs.iter().all(|l| l.block_number == 1));
    assert_eq!(sealed_page.span.cursor_block.number, 1);

    let full_page = service
        .query_logs(logs_request(
            ascending_envelope(1, 2, BUCKET + 8),
            log_filter(Address::repeat_byte(7), B256::repeat_byte(9)),
        ))
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

/// Concurrent page/materialize stages must keep results globally query-ordered with no sort.
#[tokio::test(flavor = "current_thread")]
async fn indexed_query_pipeline_preserves_global_order_across_blocks() {
    let addr = Address::repeat_byte(7);
    let topic = B256::repeat_byte(9);
    let block_count = 25u64;

    let blocks = chain_of_blocks(block_count, |_| vec![repeated_logs(addr, vec![topic], 3)]);
    let store = common::populate::populate_via_engine(blocks).await;
    let service = store.reader();

    let filter = log_filter(addr, topic);

    for order in [QueryOrder::Ascending, QueryOrder::Descending] {
        let (from_block, to_block) = match order {
            QueryOrder::Ascending => (1, block_count),
            QueryOrder::Descending => (block_count, 1),
        };
        let page = service
            .query_logs(logs_request(
                common::envelope(from_block, to_block, order, block_count as usize * 3),
                filter.clone(),
            ))
            .await
            .expect("query");

        // Three logs per block, blocks in strict query order.
        let mut expected: Vec<u64> = (1..=block_count)
            .flat_map(|number| std::iter::repeat_n(number, 3))
            .collect();
        if order == QueryOrder::Descending {
            expected.reverse();
        }
        let actual: Vec<u64> = page.logs.iter().map(|l| l.block_number).collect();
        assert_eq!(actual, expected, "pipeline must stay globally ordered");
    }
}

/// Block-aligned limit stop must hold even when one block spans two bitmap pages.
#[tokio::test(flavor = "current_thread")]
async fn indexed_query_cursor_completes_block_spanning_page_boundary() {
    let addr = Address::repeat_byte(7);
    let topic = B256::repeat_byte(9);
    // span + 8 puts the single block's ids on pages 0 and 1.
    let log_count = PAGE_SPAN + 8;

    let store = common::populate::populate_via_engine(vec![block_with_logs(
        test_header(1, B256::ZERO),
        vec![repeated_logs(addr, vec![topic], log_count)],
    )])
    .await;
    let service = store.reader();

    let page = service
        .query_logs(logs_request(
            ascending_envelope(1, 1, 5),
            log_filter(addr, topic),
        ))
        .await
        .expect("query");

    assert_eq!(page.logs.len(), log_count);
    assert!(page.logs.iter().all(|l| l.block_number == 1));
    assert_eq!(page.span.cursor_block.number, 1);
}

/// Window resolution reads only the range's endpoint records, so a damaged
/// (missing) mid-range block-metadata row is observable only when that block
/// holds candidate rows — then materialization must fail loud rather than
/// serve a partial page. A damaged block holding NO matching rows is never
/// read at all and the query answers completely.
#[tokio::test(flavor = "current_thread")]
async fn indexed_query_fails_loud_on_missing_candidate_block_record() {
    use monad_chain_data::{engine::tables::BlockTables, MonadChainDataError};

    let addr = Address::repeat_byte(7);
    let topic = B256::repeat_byte(9);
    let populate = |blocks_with_logs: [u64; 2]| async move {
        let store = common::populate::populate_via_engine(chain_of_blocks(5, |number| {
            if blocks_with_logs.contains(&number) {
                vec![vec![log(addr, vec![topic])]]
            } else {
                vec![]
            }
        }))
        .await;
        // Simulate store damage below the published head: drop block 3's
        // metadata row while the range bounds (1 and 5) stay present.
        store.meta.clear_key(
            BlockTables::<InMemoryMetaStore>::BLOCK_METADATA_TABLE,
            &3u64.to_be_bytes(),
        );
        store
    };

    // The damaged block carries a matching row: the query must error.
    let store = populate([3, 5]).await;
    let err = store
        .reader()
        .query_logs(logs_request(
            ascending_envelope(1, 5, 10),
            log_filter(addr, topic),
        ))
        .await
        .expect_err("missing candidate block record must not yield a partial page");
    assert!(
        matches!(err, MonadChainDataError::MissingData(_)),
        "got {err:?}"
    );

    // The damaged block carries no matching rows: it is never read, and the
    // query answers completely.
    let store = populate([2, 4]).await;
    let page = store
        .reader()
        .query_logs(logs_request(
            ascending_envelope(1, 5, 10),
            log_filter(addr, topic),
        ))
        .await
        .expect("damaged irrelevant block must not affect the page");
    assert_eq!(
        page.logs.iter().map(|l| l.block_number).collect::<Vec<_>>(),
        vec![2, 4]
    );
}

/// The open-region fold caches (dir bucket + bitmap page) are shared across
/// requests and tagged with the published head they folded through. A reader
/// that queried at one head must see rows from blocks published after it:
/// the fold extends incrementally rather than serving stale state.
#[tokio::test(flavor = "current_thread")]
async fn open_region_folds_extend_when_the_head_advances() {
    let addr = Address::repeat_byte(7);
    let topic = B256::repeat_byte(9);
    let mut blocks =
        common::chain_of_blocks(8, |_| vec![vec![log(Address::repeat_byte(7), vec![topic])]]);
    let rest = blocks.split_off(4);

    let store = common::populate::populate_via_engine(blocks).await;
    let service = store.reader();
    let request = |to_block: u64| QueryLogsRequest {
        envelope: ascending_envelope(1, to_block, 100),
        filter: log_filter(addr, topic),
        relations: LogsRelations::default(),
    };

    // Warms the open dir-bucket and bitmap-page folds at head 4.
    let warm = service
        .query_logs(request(4))
        .await
        .expect("query at head 4");
    assert_eq!(warm.logs.len(), 4);

    common::populate::populate_more_via_engine(&store, rest).await;

    // Same service instance: the folds must extend through the new head.
    let extended = service
        .query_logs(request(8))
        .await
        .expect("query at head 8");
    assert_eq!(
        extended
            .logs
            .iter()
            .map(|l| l.block_number)
            .collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );

    // And agree exactly with a fold-cold reader over the same stores.
    let fresh = store
        .reader()
        .query_logs(request(8))
        .await
        .expect("fresh query");
    assert_eq!(extended.logs, fresh.logs);
}
