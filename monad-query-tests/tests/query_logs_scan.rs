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
// These tests target the block-scan path (`LogFilter::default()`), so the
// seeded address/topic bytes are arbitrary and never select.
use monad_query_tests::prelude::*;
#[tokio::test(flavor = "current_thread")]
async fn block_scan_paginates_at_block_boundaries() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let store = populate::populate_via_engine(vec![
        block_with_logs(
            h1,
            vec![vec![
                log(Address::repeat_byte(5), vec![B256::repeat_byte(8)]),
                log(Address::repeat_byte(5), vec![B256::repeat_byte(8)]),
            ]],
        ),
        block_with_logs(
            h2,
            vec![vec![log(
                Address::repeat_byte(5),
                vec![B256::repeat_byte(8)],
            )]],
        ),
    ])
    .await;
    let service = store.reader();

    let first_page = service
        .query_logs(logs_request(
            ascending_envelope(1, 2, 1),
            LogFilter::default(),
        ))
        .await
        .expect("first page");

    assert_eq!(first_page.logs.len(), 2);
    assert_eq!(first_page.span.cursor_block.number, 1);
    assert_eq!(first_page.logs[0].block_number, 1);
    assert_eq!(first_page.logs[1].block_number, 1);

    let second_page = service
        .query_logs(logs_request(
            ascending_envelope(first_page.span.cursor_block.number + 1, 2, 1),
            LogFilter::default(),
        ))
        .await
        .expect("second page");

    assert_eq!(second_page.logs.len(), 1);
    assert_eq!(second_page.span.cursor_block.number, 2);
    assert_eq!(second_page.logs[0].block_number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn block_scan_descending_returns_newest_first() {
    let store = populate::populate_via_engine(vec![block_with_logs(
        test_header(1, B256::ZERO),
        vec![vec![
            log(Address::repeat_byte(5), vec![B256::repeat_byte(8)]),
            log(Address::repeat_byte(5), vec![B256::repeat_byte(8)]),
        ]],
    )])
    .await;
    let service = store.reader();

    let page = service
        .query_logs(logs_request(
            descending_envelope(1, 1, 1),
            LogFilter::default(),
        ))
        .await
        .expect("query");

    assert_eq!(page.logs.len(), 2);
    assert_eq!(page.logs[0].log_index, 1);
    assert_eq!(page.logs[1].log_index, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn block_scan_completes_current_block_when_limit_reached_mid_block() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let store = populate::populate_via_engine(vec![
        block_with_logs(
            h1,
            vec![vec![
                log(Address::repeat_byte(5), vec![B256::repeat_byte(8)]),
                log(Address::repeat_byte(5), vec![B256::repeat_byte(8)]),
                log(Address::repeat_byte(5), vec![B256::repeat_byte(8)]),
            ]],
        ),
        block_with_logs(
            h2,
            vec![vec![log(
                Address::repeat_byte(5),
                vec![B256::repeat_byte(8)],
            )]],
        ),
    ])
    .await;
    let service = store.reader();

    let page = service
        .query_logs(logs_request(
            ascending_envelope(1, 2, 1),
            LogFilter::default(),
        ))
        .await
        .expect("query");

    assert_eq!(page.logs.len(), 3);
    assert!(page.logs.iter().all(|l| l.block_number == 1));
    assert_eq!(page.span.cursor_block.number, 1);
}

/// Concurrent block reads must still yield query order and a serial-walk cursor (`buffered` preserves input order).
#[tokio::test(flavor = "current_thread")]
async fn block_scan_orders_blocks_and_lands_cursor_under_concurrency() {
    let blocks = chain_of_blocks(5, |_| {
        vec![vec![log(
            Address::repeat_byte(5),
            vec![B256::repeat_byte(8)],
        )]]
    });

    let store = populate::populate_via_engine(blocks).await;
    let service = store.reader();

    let asc = service
        .query_logs(logs_request(
            ascending_envelope(1, 5, 1_000),
            LogFilter::default(),
        ))
        .await
        .expect("ascending scan");
    let asc_blocks: Vec<u64> = asc.logs.iter().map(|l| l.block_number).collect();
    assert_eq!(asc_blocks, vec![1, 2, 3, 4, 5]);
    assert_eq!(asc.span.cursor_block.number, 5);

    // In descending order from_block is the upper bound.
    let desc = service
        .query_logs(logs_request(
            descending_envelope(5, 1, 1_000),
            LogFilter::default(),
        ))
        .await
        .expect("descending scan");
    let desc_blocks: Vec<u64> = desc.logs.iter().map(|l| l.block_number).collect();
    assert_eq!(desc_blocks, vec![5, 4, 3, 2, 1]);
    assert_eq!(desc.span.cursor_block.number, 1);

    let asc_limited = service
        .query_logs(logs_request(
            ascending_envelope(1, 5, 3),
            LogFilter::default(),
        ))
        .await
        .expect("ascending limited scan");
    let limited_blocks: Vec<u64> = asc_limited.logs.iter().map(|l| l.block_number).collect();
    assert_eq!(limited_blocks, vec![1, 2, 3]);
    assert_eq!(asc_limited.span.cursor_block.number, 3);

    let desc_limited = service
        .query_logs(logs_request(
            descending_envelope(5, 1, 3),
            LogFilter::default(),
        ))
        .await
        .expect("descending limited scan");
    let desc_limited_blocks: Vec<u64> = desc_limited.logs.iter().map(|l| l.block_number).collect();
    assert_eq!(desc_limited_blocks, vec![5, 4, 3]);
    assert_eq!(desc_limited.span.cursor_block.number, 3);
}
