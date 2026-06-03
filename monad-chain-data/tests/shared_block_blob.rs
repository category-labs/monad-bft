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

use alloy_primitives::{Log, LogData, U256};
use monad_chain_data::{
    engine::family::BLOCK_BLOB_TABLE, logs::BlockBlobHeader, FinalizedBlock, InMemoryBlobStore,
    InMemoryMetaStore, LogFilter, MonadChainDataService, QueryEnvelope, QueryLimits,
    QueryLogsRequest, QueryTracesRequest, QueryTransactionsRequest, TraceFilter, TxFilter, B256,
};

mod common;

use common::{ingest_tx, nested_call, test_header, top_level_call};

fn set<T: Eq + std::hash::Hash>(value: T) -> HashSet<T> {
    HashSet::from([value])
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_stores_one_shared_blob_with_family_relative_headers() {
    let blob_store = InMemoryBlobStore::default();
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        blob_store.clone(),
        QueryLimits::UNLIMITED,
    );

    let log_a = monad_chain_data::Address::repeat_byte(0xa1);
    let log_b = monad_chain_data::Address::repeat_byte(0xb2);
    let tx_sender_a = monad_chain_data::Address::repeat_byte(0xc3);
    let tx_sender_b = monad_chain_data::Address::repeat_byte(0xd4);
    let trace_from_a = monad_chain_data::Address::repeat_byte(0xe5);
    let trace_from_b = monad_chain_data::Address::repeat_byte(0xf6);
    let tx_hash_a = B256::repeat_byte(0x11);
    let tx_hash_b = B256::repeat_byte(0x22);

    let mut tx_a = ingest_tx(tx_sender_a, None, vec![0xaa, 0xbb, 0xcc, 0xdd]);
    tx_a.tx_hash = tx_hash_a;
    let mut tx_b = ingest_tx(tx_sender_b, None, vec![0x12, 0x34, 0x56, 0x78]);
    tx_b.tx_hash = tx_hash_b;

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![
                vec![Log {
                    address: log_a,
                    data: LogData::new_unchecked(
                        vec![B256::repeat_byte(0x31)],
                        monad_chain_data::Bytes::from_static(b"log-a"),
                    ),
                }],
                vec![Log {
                    address: log_b,
                    data: LogData::new_unchecked(
                        vec![B256::repeat_byte(0x32)],
                        monad_chain_data::Bytes::from_static(b"log-b"),
                    ),
                }],
            ],
            txs: vec![tx_a, tx_b],
            traces: vec![
                top_level_call(0, trace_from_a, trace_from_b, U256::from(7u64), vec![1; 4]),
                nested_call(1, trace_from_b, trace_from_a, U256::from(9u64), vec![2; 4]),
            ],
        })
        .await
        .expect("ingest block");

    let snapshot = blob_store.blob_snapshot();
    assert_eq!(snapshot.len(), 1, "one blob object per block");
    assert!(snapshot.contains_key(&(BLOCK_BLOB_TABLE, 1u64.to_be_bytes().to_vec())));

    let log_header = load_header(&service, monad_chain_data::Family::Log).await;
    let tx_header = load_header(&service, monad_chain_data::Family::Tx).await;
    let trace_header = load_header(&service, monad_chain_data::Family::Trace).await;
    assert_eq!(log_header.base_offset, 0);
    assert_eq!(tx_header.base_offset, *log_header.offsets.last().unwrap());
    assert_eq!(
        trace_header.base_offset,
        tx_header.base_offset + *tx_header.offsets.last().unwrap()
    );

    // The whole shared object is the concatenation of every family region;
    // read it straight from the raw blob fixture (the region cache only ever
    // holds per-family slices, never the combined object).
    let combined = snapshot
        .get(&(BLOCK_BLOB_TABLE, 1u64.to_be_bytes().to_vec()))
        .expect("shared blob present");
    assert_eq!(
        combined.len(),
        usize::try_from(trace_header.base_offset + *trace_header.offsets.last().unwrap()).unwrap()
    );

    let log_region = service
        .tables()
        .read_block_blob_range(1, log_header.region_range().0, log_header.region_range().1)
        .await
        .expect("read log region")
        .expect("log region present");
    assert_eq!(
        log_region.len(),
        usize::try_from(*log_header.offsets.last().unwrap()).unwrap()
    );

    let indexed_logs = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                ..QueryEnvelope::default()
            },
            filter: LogFilter {
                address: Some(set(log_b)),
                ..LogFilter::default()
            },
            ..QueryLogsRequest::default()
        })
        .await
        .expect("indexed log query");
    assert_eq!(indexed_logs.logs.len(), 1);
    assert_eq!(indexed_logs.logs[0].address, log_b);

    let indexed_txs = service
        .query_transactions(QueryTransactionsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                ..QueryEnvelope::default()
            },
            filter: TxFilter {
                from: Some(set(tx_sender_b)),
                ..TxFilter::default()
            },
            ..QueryTransactionsRequest::default()
        })
        .await
        .expect("indexed tx query");
    assert_eq!(indexed_txs.txs.len(), 1);
    assert_eq!(indexed_txs.txs[0].tx_hash, tx_hash_b);

    let indexed_traces = service
        .query_traces(QueryTracesRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                ..QueryEnvelope::default()
            },
            filter: TraceFilter {
                from: Some(set(trace_from_b)),
                ..TraceFilter::default()
            },
            ..QueryTracesRequest::default()
        })
        .await
        .expect("indexed trace query");
    assert_eq!(indexed_traces.traces.len(), 1);
    assert_eq!(indexed_traces.traces[0].from, trace_from_b);

    assert_eq!(
        service
            .query_logs(QueryLogsRequest::default())
            .await
            .expect("scan log query")
            .logs
            .len(),
        2
    );
    assert_eq!(
        service
            .query_transactions(QueryTransactionsRequest::default())
            .await
            .expect("scan tx query")
            .txs
            .len(),
        2
    );
    assert_eq!(
        service
            .query_traces(QueryTracesRequest::default())
            .await
            .expect("scan trace query")
            .traces
            .len(),
        2
    );
}

async fn load_header(
    service: &MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore>,
    family: monad_chain_data::Family,
) -> BlockBlobHeader {
    let bytes = service
        .tables()
        .family(family)
        .load_block_header(1)
        .await
        .expect("load family header")
        .expect("family header present");
    BlockBlobHeader::decode(&bytes).expect("decode family header")
}
