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

//! Row-cache tests; all metadata caches are off so blob fetch counts isolate row-cache misses.

use std::collections::HashSet;

use alloy_primitives::U256;
use monad_chain_data::{
    Address, CacheConfig, DictConfig, FinalizedBlock, MonadChainDataService, QueryLimits,
    QueryRuntimeConfig, QueryTracesRequest, QueryTransfersRequest, TraceFilter, TracesRelations,
    TransferFilter, TransfersRelations, B256,
};

mod common;

use common::{
    ascending_envelope, block_with_txs, ingest_tx, test_header, top_level_call, tx_query, with_hash,
};

fn row_cache_only_config() -> CacheConfig {
    CacheConfig {
        row_cache_bytes: 3 * 1024 * 1024,
        ..CacheConfig::from_total_mib(0)
    }
}

fn reader_with_row_cache(
    store: &common::populate::PopulatedStore,
) -> MonadChainDataService<monad_chain_data::InMemoryMetaStore, monad_chain_data::InMemoryBlobStore>
{
    MonadChainDataService::with_all_configs(
        store.meta.clone(),
        store.blob.clone(),
        QueryLimits::UNLIMITED,
        row_cache_only_config(),
        DictConfig::default(),
        QueryRuntimeConfig::default(),
    )
}

/// Xorshift noise: incompressible by zstd, so frame byte ranges stay wide apart.
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn get_transaction_second_read_hits_row_cache() {
    let alice = Address::repeat_byte(0xaa);
    let bob = Address::repeat_byte(0xbb);
    let hash = B256::repeat_byte(0x02);

    let store = common::populate::populate_via_engine(vec![block_with_txs(
        test_header(1, B256::ZERO),
        vec![
            ingest_tx(alice, Some(bob), Vec::new()),
            with_hash(ingest_tx(bob, Some(alice), Vec::new()), hash),
        ],
    )])
    .await;
    let service = reader_with_row_cache(&store);

    let before = store.blob.get_blob_calls();
    let first = service.get_transaction(hash).await.expect("lookup");
    let after_first = store.blob.get_blob_calls();
    assert!(first.is_some());
    assert!(after_first > before, "first read must hit the backend");

    let second = service.get_transaction(hash).await.expect("lookup");
    assert_eq!(first, second);
    assert_eq!(
        store.blob.get_blob_calls(),
        after_first,
        "second read must be served entirely from the row cache"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn repeat_indexed_query_issues_no_blob_reads() {
    let alice = Address::repeat_byte(0xaa);
    let recipient = Address::repeat_byte(0x11);

    let store = common::populate::populate_via_engine(vec![block_with_txs(
        test_header(1, B256::ZERO),
        vec![
            ingest_tx(alice, Some(recipient), Vec::new()),
            ingest_tx(alice, Some(recipient), Vec::new()),
            ingest_tx(alice, Some(recipient), Vec::new()),
        ],
    )])
    .await;
    let service = reader_with_row_cache(&store);

    let request = tx_query(alice, ascending_envelope(1, 1, 10));
    let first = service
        .query_transactions(request.clone())
        .await
        .expect("query");
    assert_eq!(first.txs.len(), 3);

    let before = store.blob.get_blob_calls();
    let second = service.query_transactions(request).await.expect("query");
    assert_eq!(first.txs, second.txs);
    assert_eq!(
        store.blob.get_blob_calls(),
        before,
        "repeat query must materialize every row from the cache"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn near_rows_share_one_read_far_rows_split() {
    let alice = Address::repeat_byte(0xaa);
    let bob = Address::repeat_byte(0xbb);
    let recipient = Address::repeat_byte(0x11);

    // Ten incompressible fillers of a quarter-gap each put ~2.5x the coalescing
    // gap limit between tx1 and tx12.
    let gap = QueryRuntimeConfig::default().materialize_span_max_gap_bytes;
    let mut txs = vec![
        ingest_tx(alice, Some(recipient), Vec::new()),
        ingest_tx(alice, Some(recipient), Vec::new()),
    ];
    for i in 0..10u64 {
        txs.push(ingest_tx(bob, Some(recipient), noise(i, gap / 4)));
    }
    txs.push(ingest_tx(alice, Some(recipient), Vec::new()));

    let store = common::populate::populate_via_engine(vec![block_with_txs(
        test_header(1, B256::ZERO),
        txs,
    )])
    .await;
    let service = reader_with_row_cache(&store);

    let before = store.blob.get_blob_calls();
    let response = service
        .query_transactions(tx_query(alice, ascending_envelope(1, 1, 10)))
        .await
        .expect("query");
    assert_eq!(response.txs.len(), 3);
    let reads = store.blob.get_blob_calls() - before;
    assert_eq!(
        reads, 2,
        "tx0+tx1 must share one coalesced read; tx12 is too far and gets its own"
    );

    let before = store.blob.get_blob_calls();
    let response = service
        .query_transactions(tx_query(bob, ascending_envelope(1, 1, 20)))
        .await
        .expect("query");
    assert_eq!(response.txs.len(), 10);
    assert!(
        store.blob.get_blob_calls() > before,
        "unread rows must not have been cached by the alice query"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn transfer_query_hits_rows_cached_by_trace_query() {
    let alice = Address::repeat_byte(0xaa);
    let recipient = Address::repeat_byte(0x11);

    let store = common::populate::populate_via_engine(vec![FinalizedBlock {
        header: test_header(1, B256::ZERO),
        logs_by_tx: vec![vec![]],
        txs: vec![ingest_tx(alice, Some(recipient), Vec::new())],
        traces: vec![top_level_call(
            0,
            alice,
            recipient,
            U256::from(7u64),
            vec![],
        )],
    }])
    .await;
    let service = reader_with_row_cache(&store);

    let traces = service
        .query_traces(QueryTracesRequest {
            envelope: ascending_envelope(1, 1, 10),
            filter: TraceFilter {
                from: Some(HashSet::from([alice])),
                ..Default::default()
            },
            relations: TracesRelations::default(),
        })
        .await
        .expect("traces query");
    assert_eq!(traces.traces.len(), 1);

    let before = store.blob.get_blob_calls();
    let transfers = service
        .query_transfers(QueryTransfersRequest {
            envelope: ascending_envelope(1, 1, 10),
            filter: TransferFilter {
                from: Some(HashSet::from([alice])),
                ..Default::default()
            },
            relations: TransfersRelations::default(),
        })
        .await
        .expect("transfers query");
    assert_eq!(transfers.transfers.len(), 1);
    assert_eq!(
        store.blob.get_blob_calls(),
        before,
        "the transfer row must come from the trace row cache"
    );
}
