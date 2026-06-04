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

use std::{collections::HashSet, convert::TryFrom};

use bytes::Bytes as StorageBytes;
use monad_chain_data::{
    engine::bitmap::{sharded_stream_id, stream_page_key, STREAM_PAGE_LOCAL_ID_SPAN},
    store::MetaStore,
    Address, Bytes, Family, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log, LogData,
    LogFilter, LogsRelations, MonadChainDataService, QueryEnvelope, QueryLimits, QueryLogsRequest,
    QueryOrder, Topic, B256,
};

mod common;

use common::{chain_header, test_header};

// Fills a full bitmap page (~65k logs) to exercise the seal/compaction path;
// the in-memory store makes that slow, so gate behind --ignored.
#[tokio::test(flavor = "current_thread")]
#[ignore = "slow: ingests ~65k logs to force page seal"]
async fn ingest_compacts_sealed_pages_and_query_prefers_compacted_page_blobs() {
    let meta_store = InMemoryMetaStore::default();
    let blob_store = InMemoryBlobStore::default();
    let service =
        MonadChainDataService::new(meta_store.clone(), blob_store, QueryLimits::UNLIMITED);

    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    let old_address = Address::repeat_byte(7);
    let old_topic = B256::repeat_byte(9);
    service
        .ingest_block(FinalizedBlock {
            header: h1,
            logs_by_tx: vec![repeated_logs(
                old_address,
                vec![old_topic],
                usize::try_from(STREAM_PAGE_LOCAL_ID_SPAN - 2).expect("page span fits usize"),
            )],
            txs: Vec::new(),
            traces: vec![],
        })
        .await
        .expect("ingest block 1");

    let frontier_address = Address::repeat_byte(8);
    let frontier_topic = B256::repeat_byte(10);
    service
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![repeated_logs(frontier_address, vec![frontier_topic], 4)],
            txs: Vec::new(),
            traces: vec![],
        })
        .await
        .expect("ingest block 2");

    let old_stream = sharded_stream_id("addr", old_address.as_slice(), 0);
    let frontier_stream = sharded_stream_id("addr", frontier_address.as_slice(), 0);
    assert!(service
        .tables()
        .family(Family::Log)
        .load_bitmap_page_artifact(&old_stream, 0)
        .await
        .expect("load old page artifact")
        .is_some());
    assert!(service
        .tables()
        .family(Family::Log)
        .load_bitmap_page_artifact(&frontier_stream, 0)
        .await
        .expect("load sealed frontier page artifact")
        .is_some());
    assert!(service
        .tables()
        .family(Family::Log)
        .load_bitmap_page_artifact(&frontier_stream, STREAM_PAGE_LOCAL_ID_SPAN)
        .await
        .expect("load live frontier page artifact")
        .is_none());

    let partition = stream_page_key(&old_stream, 0);
    meta_store
        .scan_put(
            service.tables().family(Family::Log).bitmap_by_block_table(),
            &partition,
            &1u64.to_be_bytes(),
            StorageBytes::from_static(b"corrupt-fragment"),
        )
        .await
        .expect("corrupt retained fragment");

    let old_page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: LogFilter {
                address: Some(HashSet::from([old_address])),
                topics: [Some(HashSet::from([old_topic])), None, None, None],
            },
            relations: LogsRelations::default(),
        })
        .await
        .expect("query compacted page");
    assert_eq!(
        old_page.logs.len(),
        usize::try_from(STREAM_PAGE_LOCAL_ID_SPAN - 2).expect("page span fits usize")
    );

    let frontier_page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: LogFilter {
                address: Some(HashSet::from([frontier_address])),
                topics: [Some(HashSet::from([frontier_topic])), None, None, None],
            },
            relations: LogsRelations::default(),
        })
        .await
        .expect("query live frontier page");
    assert_eq!(frontier_page.logs.len(), 4);
}

// Re-touching streams already in the durable open-stream inventory must not
// re-write their (0-byte) rows: the planner diffs this batch's touched streams
// against the committed baseline, so a batch that adds no new streams on the
// frontier page issues zero `*_open_bitmap_stream` writes.
#[tokio::test(flavor = "current_thread")]
async fn re_touching_open_streams_writes_no_redundant_rows() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let address = Address::repeat_byte(7);
    let topic = B256::repeat_byte(9);

    let h1 = test_header(1, B256::ZERO);
    let (_o1, timings1) = service
        .ingest_blocks(vec![FinalizedBlock {
            header: h1.clone(),
            logs_by_tx: vec![repeated_logs(address, vec![topic], 4)],
            txs: Vec::new(),
            traces: vec![],
        }])
        .await
        .expect("ingest block 1");
    // First sight of these streams on the frontier page: their inventory rows
    // are genuinely new and must be written.
    assert!(
        timings1
            .phase_b_write_counts
            .ops_for_table("scan:log_open_bitmap_stream")
            > 0,
        "new streams must write open-stream rows on first appearance"
    );

    let h2 = chain_header(2, &h1);
    let (_o2, timings2) = service
        .ingest_blocks(vec![FinalizedBlock {
            header: h2,
            // Same address+topic, still on page 0 — no new streams, no page seal.
            logs_by_tx: vec![repeated_logs(address, vec![topic], 4)],
            txs: Vec::new(),
            traces: vec![],
        }])
        .await
        .expect("ingest block 2");
    assert_eq!(
        timings2
            .phase_b_write_counts
            .ops_for_table("scan:log_open_bitmap_stream"),
        0,
        "re-touching already-durable streams must not re-write their inventory rows"
    );
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
