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
use monad_query_tests::prelude::*;
const LOG_A: Address = Address::repeat_byte(0xa1);
const LOG_B: Address = Address::repeat_byte(0xb2);
const TX_SENDER_A: Address = Address::repeat_byte(0xc3);
const TX_SENDER_B: Address = Address::repeat_byte(0xd4);
const TRACE_FROM_A: Address = Address::repeat_byte(0xe5);
const TRACE_FROM_B: Address = Address::repeat_byte(0xf6);
const TX_HASH_A: B256 = B256::repeat_byte(0x11);
const TX_HASH_B: B256 = B256::repeat_byte(0x22);

fn set<T: Eq + std::hash::Hash>(value: T) -> HashSet<T> {
    HashSet::from([value])
}

/// One block with two logs, two txs, and two traces, each pair distinguishable
/// by address/sender/from so per-family queries can pick out a single row.
fn fixture_block() -> FinalizedBlock {
    FinalizedBlock {
        header: test_header(1, B256::ZERO),
        logs_by_tx: vec![
            vec![Log {
                address: LOG_A,
                data: LogData::new_unchecked(
                    vec![B256::repeat_byte(0x31)],
                    Bytes::from_static(b"log-a"),
                ),
            }],
            vec![Log {
                address: LOG_B,
                data: LogData::new_unchecked(
                    vec![B256::repeat_byte(0x32)],
                    Bytes::from_static(b"log-b"),
                ),
            }],
        ],
        txs: vec![
            with_hash(
                ingest_tx(TX_SENDER_A, None, vec![0xaa, 0xbb, 0xcc, 0xdd]),
                TX_HASH_A,
            ),
            with_hash(
                ingest_tx(TX_SENDER_B, None, vec![0x12, 0x34, 0x56, 0x78]),
                TX_HASH_B,
            ),
        ],
        traces: vec![
            top_level_call(0, TRACE_FROM_A, TRACE_FROM_B, U256::from(7u64), vec![1; 4]),
            nested_call(1, TRACE_FROM_B, TRACE_FROM_A, U256::from(9u64), vec![2; 4]),
        ],
        external: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn blob_layout_one_object_with_contiguous_family_regions() {
    let store = populate::populate_via_engine(vec![fixture_block()]).await;
    let service = store.reader();
    let blob_store = store.blob.clone();

    let snapshot = blob_store.blob_snapshot();
    // The blob store also holds the recovery-snapshot payload (its own table);
    // the block table itself must hold exactly one object per block.
    let block_blobs = snapshot
        .keys()
        .filter(|(table, _)| *table == BLOCK_BLOB_TABLE)
        .count();
    assert_eq!(block_blobs, 1, "one blob object per block");
    assert!(snapshot.contains_key(&(BLOCK_BLOB_TABLE, 1u64.to_be_bytes().to_vec())));

    let log_header = load_header(&service, Family::Log).await;
    let tx_header = load_header(&service, Family::Tx).await;
    let trace_header = load_header(&service, Family::Trace).await;
    assert_eq!(log_header.base_offset, 0);
    assert_eq!(tx_header.base_offset, *log_header.offsets.last().unwrap());
    assert_eq!(
        trace_header.base_offset,
        tx_header.base_offset + *tx_header.offsets.last().unwrap()
    );

    // Read the raw shared object.
    let combined = snapshot
        .get(&(BLOCK_BLOB_TABLE, 1u64.to_be_bytes().to_vec()))
        .expect("shared blob present");
    assert_eq!(
        combined.len(),
        usize::try_from(trace_header.base_offset + *trace_header.offsets.last().unwrap()).unwrap()
    );

    let log_region = service
        .tables()
        .read_block_blob_header_range(
            1,
            &log_header,
            log_header.region_range().0,
            log_header.region_range().1,
        )
        .await
        .expect("read log region")
        .expect("log region present");
    assert_eq!(
        log_region.len(),
        usize::try_from(*log_header.offsets.last().unwrap()).unwrap()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_and_scan_queries_read_through_shared_blob() {
    let store = populate::populate_via_engine(vec![fixture_block()]).await;
    let service = store.reader();

    let indexed_logs = service
        .query_logs(logs_request(
            ascending_envelope(1, 1, 10),
            address_filter(LOG_B),
        ))
        .await
        .expect("indexed log query");
    assert_eq!(indexed_logs.logs.len(), 1);
    assert_eq!(indexed_logs.logs[0].address, LOG_B);

    let indexed_txs = service
        .query_transactions(tx_query(TX_SENDER_B, ascending_envelope(1, 1, 10)))
        .await
        .expect("indexed tx query");
    assert_eq!(indexed_txs.txs.len(), 1);
    assert_eq!(indexed_txs.txs[0].tx_hash, TX_HASH_B);

    let indexed_traces = service
        .query_traces(traces_request(
            ascending_envelope(1, 1, 10),
            TraceFilter {
                from: Some(set(TRACE_FROM_B)),
                ..TraceFilter::default()
            },
        ))
        .await
        .expect("indexed trace query");
    assert_eq!(indexed_traces.traces.len(), 1);
    assert_eq!(indexed_traces.traces[0].from, TRACE_FROM_B);

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
    family: Family,
) -> BlockBlobHeader {
    let header = service
        .tables()
        .family(family)
        .load_blob_header(1)
        .await
        .expect("load family header")
        .expect("family header present");
    (*header).clone()
}
