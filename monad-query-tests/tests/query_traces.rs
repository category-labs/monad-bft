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

use alloy_primitives::U256;
use monad_query_tests::prelude::*;
fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

/// Populates `blocks` via the engine and hands back a query service.
async fn service_for(
    blocks: Vec<FinalizedBlock>,
) -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
    populate::populate_via_engine(blocks).await.reader()
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_filters_by_from_across_blocks() {
    let alice = addr(0xaa);
    let bob = addr(0xbb);
    let recipient = addr(0x11);

    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    let blocks = vec![
        block_with_traces(
            h1.clone(),
            vec![
                top_level_call(0, alice, recipient, U256::from(10u64), vec![]),
                top_level_call(1, bob, recipient, U256::from(10u64), vec![]),
            ],
        ),
        block_with_traces(
            h2,
            vec![top_level_call(
                0,
                alice,
                recipient,
                U256::from(20u64),
                vec![],
            )],
        ),
    ];
    let service = service_for(blocks).await;

    let resp = service
        .query_traces(traces_request(
            ascending_envelope(1, 2, 10),
            TraceFilter {
                from: Some(HashSet::from([alice])),
                ..Default::default()
            },
        ))
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 2);
    assert!(resp.traces.iter().all(|t| t.from == alice));
    let block_numbers: Vec<u64> = resp.traces.iter().map(|t| t.block_number).collect();
    assert_eq!(block_numbers, vec![1, 2]);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_filters_by_selector() {
    let caller = addr(1);
    let callee = addr(2);
    let sel = vec![0xde, 0xad, 0xbe, 0xef];

    let blocks = vec![block_with_traces(
        test_header(1, B256::ZERO),
        vec![
            top_level_call(0, caller, callee, U256::ZERO, sel.clone()),
            top_level_call(1, caller, callee, U256::ZERO, vec![0x00, 0x11, 0x22, 0x33]),
        ],
    )];
    let service = service_for(blocks).await;

    let resp = service
        .query_traces(traces_request(
            ascending_envelope(1, 1, 10),
            TraceFilter {
                selector: Some(HashSet::from([[0xde, 0xad, 0xbe, 0xef]])),
                ..Default::default()
            },
        ))
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 1);
    assert_eq!(resp.traces[0].selector(), Some([0xde, 0xad, 0xbe, 0xef]));
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_is_top_level_true_keeps_roots_only() {
    let caller = addr(1);
    let callee = addr(2);

    let blocks = vec![block_with_traces(
        test_header(1, B256::ZERO),
        vec![
            top_level_call(0, caller, callee, U256::from(10u64), vec![]),
            nested_call(0, caller, callee, U256::from(5u64), vec![]),
            top_level_call(1, caller, callee, U256::ZERO, vec![]),
        ],
    )];
    let service = service_for(blocks).await;

    let resp = service
        .query_traces(traces_request(
            ascending_envelope(1, 1, 10),
            TraceFilter {
                is_top_level: Some(true),
                ..Default::default()
            },
        ))
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 2);
    assert!(resp.traces.iter().all(|t| t.is_top_level()));
}

#[tokio::test(flavor = "current_thread")]
async fn scan_query_is_top_level_false_keeps_non_roots() {
    let caller = addr(1);
    let callee = addr(2);

    let blocks = vec![block_with_traces(
        test_header(1, B256::ZERO),
        vec![
            top_level_call(0, caller, callee, U256::from(1u64), vec![]),
            nested_call(0, caller, callee, U256::from(1u64), vec![]),
            top_level_call(1, caller, callee, U256::ZERO, vec![]),
        ],
    )];
    let service = service_for(blocks).await;

    let resp = service
        .query_traces(traces_request(
            ascending_envelope(1, 1, 10),
            TraceFilter {
                is_top_level: Some(false),
                ..Default::default()
            },
        ))
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 1);
    assert!(!resp.traces[0].is_top_level());
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_combines_from_and_is_top_level_false() {
    // is_top_level=false is applied as a post-filter on the indexed candidates.
    let caller = addr(1);
    let callee = addr(2);

    let blocks = vec![block_with_traces(
        test_header(1, B256::ZERO),
        vec![
            top_level_call(0, caller, callee, U256::from(1u64), vec![]),
            nested_call(0, caller, callee, U256::from(1u64), vec![]),
        ],
    )];
    let service = service_for(blocks).await;

    let resp = service
        .query_traces(traces_request(
            ascending_envelope(1, 1, 10),
            TraceFilter {
                from: Some(HashSet::from([caller])),
                is_top_level: Some(false),
                ..Default::default()
            },
        ))
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 1);
    assert!(!resp.traces[0].is_top_level());
}

#[tokio::test(flavor = "current_thread")]
async fn scan_query_no_filter_returns_all_traces_in_block_order() {
    let caller = addr(1);
    let callee = addr(2);

    let blocks = vec![block_with_traces(
        test_header(1, B256::ZERO),
        vec![
            top_level_call(0, caller, callee, U256::from(1u64), vec![]),
            nested_call(0, caller, callee, U256::from(1u64), vec![]),
            top_level_call(1, caller, callee, U256::ZERO, vec![]),
        ],
    )];
    let service = service_for(blocks).await;

    let resp = service
        .query_traces(traces_request(
            ascending_envelope(1, 1, 10),
            TraceFilter::default(),
        ))
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 3);
    let tx_indices: Vec<u32> = resp.traces.iter().map(|t| t.tx_index).collect();
    assert_eq!(tx_indices, vec![0, 0, 1]);
}

#[tokio::test(flavor = "current_thread")]
async fn relations_attach_blocks_and_transactions() {
    let caller = addr(1);
    let callee = addr(2);

    let blocks = vec![FinalizedBlock {
        header: test_header(1, B256::ZERO),
        logs_by_tx: vec![vec![]],
        txs: vec![ingest_tx(caller, Some(callee), Vec::new())],
        traces: vec![top_level_call(0, caller, callee, U256::from(1u64), vec![])],
        external: None,
    }];
    let service = service_for(blocks).await;

    let resp = service
        .query_traces(QueryTracesRequest {
            envelope: ascending_envelope(1, 1, 10),
            filter: TraceFilter::default(),
            relations: TracesRelations {
                blocks: true,
                transactions: true,
            },
        })
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 1);
    let blocks = resp.blocks.as_ref().expect("blocks");
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].header.number, 1);
    let txs = resp.transactions.as_ref().expect("txs");
    assert_eq!(txs.len(), 1);
    assert_eq!((txs[0].block_number, txs[0].tx_index), (1, 0));
}
