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
use monad_chain_data::{
    Address, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, MonadChainDataService,
    QueryEnvelope, QueryLimits, QueryOrder, QueryTracesRequest, TraceFilter, TracesRelations, B256,
};

mod common;

use common::{chain_header, nested_call, test_header, top_level_call};

fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

fn build_service() -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
    MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_filters_by_from_across_blocks() {
    let service = build_service();
    let alice = addr(0xaa);
    let bob = addr(0xbb);
    let recipient = addr(0x11);

    let h1 = test_header(1, B256::ZERO);
    service
        .ingest_block(FinalizedBlock {
            header: h1.clone(),
            logs_by_tx: vec![],
            txs: vec![],
            traces: vec![
                top_level_call(0, alice, recipient, U256::from(10u64), vec![]),
                top_level_call(1, bob, recipient, U256::from(10u64), vec![]),
            ],
        })
        .await
        .expect("ingest 1");
    let h2 = chain_header(2, &h1);
    service
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![],
            txs: vec![],
            traces: vec![top_level_call(
                0,
                alice,
                recipient,
                U256::from(20u64),
                vec![],
            )],
        })
        .await
        .expect("ingest 2");

    let resp = service
        .query_traces(QueryTracesRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: TraceFilter {
                from: Some(HashSet::from([alice])),
                ..Default::default()
            },
            relations: TracesRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 2);
    assert!(resp.traces.iter().all(|t| t.from == alice));
    let block_numbers: Vec<u64> = resp.traces.iter().map(|t| t.block_number).collect();
    assert_eq!(block_numbers, vec![1, 2]);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_filters_by_selector() {
    let service = build_service();
    let from_ = addr(1);
    let to_ = addr(2);
    let sel = vec![0xde, 0xad, 0xbe, 0xef];

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![],
            txs: vec![],
            traces: vec![
                top_level_call(0, from_, to_, U256::ZERO, sel.clone()),
                top_level_call(1, from_, to_, U256::ZERO, vec![0x00, 0x11, 0x22, 0x33]),
            ],
        })
        .await
        .expect("ingest");

    let resp = service
        .query_traces(QueryTracesRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: TraceFilter {
                selector: Some(HashSet::from([[0xde, 0xad, 0xbe, 0xef]])),
                ..Default::default()
            },
            relations: TracesRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 1);
    assert_eq!(resp.traces[0].selector(), Some([0xde, 0xad, 0xbe, 0xef]));
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_is_top_level_true_keeps_roots_only() {
    let service = build_service();
    let from_ = addr(1);
    let to_ = addr(2);
    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![],
            txs: vec![],
            traces: vec![
                top_level_call(0, from_, to_, U256::from(10u64), vec![]),
                nested_call(0, from_, to_, U256::from(5u64), vec![]),
                top_level_call(1, from_, to_, U256::ZERO, vec![]),
            ],
        })
        .await
        .expect("ingest");

    let resp = service
        .query_traces(QueryTracesRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: TraceFilter {
                is_top_level: Some(true),
                ..Default::default()
            },
            relations: TracesRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 2);
    assert!(resp.traces.iter().all(|t| t.is_top_level()));
}

#[tokio::test(flavor = "current_thread")]
async fn scan_query_is_top_level_false_keeps_non_roots() {
    let service = build_service();
    let from_ = addr(1);
    let to_ = addr(2);
    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![],
            txs: vec![],
            traces: vec![
                top_level_call(0, from_, to_, U256::from(1u64), vec![]),
                nested_call(0, from_, to_, U256::from(1u64), vec![]),
                top_level_call(1, from_, to_, U256::ZERO, vec![]),
            ],
        })
        .await
        .expect("ingest");

    let resp = service
        .query_traces(QueryTracesRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: TraceFilter {
                is_top_level: Some(false),
                ..Default::default()
            },
            relations: TracesRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 1);
    assert!(!resp.traces[0].is_top_level());
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_combines_from_and_is_top_level_false() {
    // Verify the indexed runner's post-filter actually drops top-level
    // frames when `is_top_level: Some(false)` is combined with another
    // indexed clause.
    let service = build_service();
    let from_ = addr(1);
    let to_ = addr(2);
    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![],
            txs: vec![],
            traces: vec![
                top_level_call(0, from_, to_, U256::from(1u64), vec![]),
                nested_call(0, from_, to_, U256::from(1u64), vec![]),
            ],
        })
        .await
        .expect("ingest");

    let resp = service
        .query_traces(QueryTracesRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: TraceFilter {
                from: Some(HashSet::from([from_])),
                is_top_level: Some(false),
                ..Default::default()
            },
            relations: TracesRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 1);
    assert!(!resp.traces[0].is_top_level());
}

#[tokio::test(flavor = "current_thread")]
async fn scan_query_no_filter_returns_all_traces_in_block_order() {
    let service = build_service();
    let from_ = addr(1);
    let to_ = addr(2);
    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![],
            txs: vec![],
            traces: vec![
                top_level_call(0, from_, to_, U256::from(1u64), vec![]),
                nested_call(0, from_, to_, U256::from(1u64), vec![]),
                top_level_call(1, from_, to_, U256::ZERO, vec![]),
            ],
        })
        .await
        .expect("ingest");

    let resp = service
        .query_traces(QueryTracesRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: TraceFilter::default(),
            relations: TracesRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 3);
    let tx_indices: Vec<u32> = resp.traces.iter().map(|t| t.tx_index).collect();
    assert_eq!(tx_indices, vec![0, 0, 1]);
}

#[tokio::test(flavor = "current_thread")]
async fn relations_attach_blocks_and_transactions() {
    let service = build_service();
    let from_ = addr(1);
    let to_ = addr(2);

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![]],
            txs: vec![common::ingest_tx(from_, Some(to_), Vec::new())],
            traces: vec![top_level_call(0, from_, to_, U256::from(1u64), vec![])],
        })
        .await
        .expect("ingest");

    let resp = service
        .query_traces(QueryTracesRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: TraceFilter::default(),
            relations: TracesRelations {
                blocks: true,
                transactions: true,
            },
        })
        .await
        .expect("query");

    assert_eq!(resp.traces.len(), 1);
    assert_eq!(resp.blocks.as_ref().expect("blocks").len(), 1);
    assert_eq!(resp.transactions.as_ref().expect("txs").len(), 1);
}
