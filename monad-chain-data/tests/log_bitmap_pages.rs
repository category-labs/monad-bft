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
    kernel::{
        bitmap::{sharded_stream_id, stream_page_key, STREAM_PAGE_LOCAL_ID_SPAN},
        tables::LogTables,
    },
    store::MetaStore,
    Address, Bytes, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log, LogData, LogFilter,
    MonadChainDataService, QueryLogsRequest, QueryOrder, Topic, B256,
};

// Fills a full bitmap page (~65k logs) to exercise the seal/compaction path;
// the in-memory store makes that slow, so gate behind --ignored.
#[tokio::test(flavor = "current_thread")]
#[ignore = "slow: ingests ~65k logs to force page seal"]
async fn ingest_compacts_sealed_pages_and_query_prefers_compacted_page_blobs() {
    let meta_store = InMemoryMetaStore::default();
    let blob_store = InMemoryBlobStore::default();
    let service = MonadChainDataService::new(meta_store.clone(), blob_store);

    let old_address = Address::repeat_byte(7);
    let old_topic = B256::repeat_byte(9);
    service
        .ingest_block(FinalizedBlock {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            parent_hash: B256::ZERO,
            logs_by_tx: vec![repeated_logs(
                old_address,
                vec![old_topic],
                usize::try_from(STREAM_PAGE_LOCAL_ID_SPAN - 2).expect("page span fits usize"),
            )],
        })
        .await
        .expect("ingest block 1");

    let frontier_address = Address::repeat_byte(8);
    let frontier_topic = B256::repeat_byte(10);
    service
        .ingest_block(FinalizedBlock {
            block_number: 2,
            block_hash: B256::repeat_byte(2),
            parent_hash: B256::repeat_byte(1),
            logs_by_tx: vec![repeated_logs(frontier_address, vec![frontier_topic], 4)],
        })
        .await
        .expect("ingest block 2");

    let old_stream = sharded_stream_id("addr", old_address.as_slice(), 0);
    let frontier_stream = sharded_stream_id("addr", frontier_address.as_slice(), 0);
    assert!(service
        .tables()
        .logs()
        .load_bitmap_page_meta(&old_stream, 0)
        .await
        .expect("load old page meta")
        .is_some());
    assert!(service
        .tables()
        .logs()
        .load_bitmap_page_blob(&old_stream, 0)
        .await
        .expect("load old page blob")
        .is_some());
    assert!(service
        .tables()
        .logs()
        .load_bitmap_page_meta(&frontier_stream, 0)
        .await
        .expect("load sealed frontier page meta")
        .is_some());
    assert!(service
        .tables()
        .logs()
        .load_bitmap_page_meta(&frontier_stream, STREAM_PAGE_LOCAL_ID_SPAN)
        .await
        .expect("load live frontier page meta")
        .is_none());

    let partition = stream_page_key(&old_stream, 0);
    meta_store
        .scan_put(
            LogTables::<InMemoryMetaStore, InMemoryBlobStore>::LOG_BITMAP_BY_BLOCK_TABLE,
            &partition,
            &1u64.to_be_bytes(),
            StorageBytes::from_static(b"corrupt-fragment"),
        )
        .await
        .expect("corrupt retained fragment");

    let old_page = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(2),
            order: QueryOrder::Ascending,
            limit: 10,
            filter: LogFilter {
                address: Some(HashSet::from([old_address])),
                topics: [Some(HashSet::from([old_topic])), None, None, None],
            },
        })
        .await
        .expect("query compacted page");
    assert_eq!(
        old_page.logs.len(),
        usize::try_from(STREAM_PAGE_LOCAL_ID_SPAN - 2).expect("page span fits usize")
    );

    let frontier_page = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(2),
            order: QueryOrder::Ascending,
            limit: 10,
            filter: LogFilter {
                address: Some(HashSet::from([frontier_address])),
                topics: [Some(HashSet::from([frontier_topic])), None, None, None],
            },
        })
        .await
        .expect("query live frontier page");
    assert_eq!(frontier_page.logs.len(), 4);
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "slow: ingests ~65k logs to force page seal"]
async fn query_errors_when_compacted_page_meta_exists_but_blob_is_missing() {
    let meta_store = InMemoryMetaStore::default();
    let blob_store = InMemoryBlobStore::default();
    let service = MonadChainDataService::new(meta_store.clone(), blob_store);

    let address = Address::repeat_byte(7);
    let topic = B256::repeat_byte(9);
    service
        .ingest_block(FinalizedBlock {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            parent_hash: B256::ZERO,
            logs_by_tx: vec![repeated_logs(
                address,
                vec![topic],
                usize::try_from(STREAM_PAGE_LOCAL_ID_SPAN - 2).expect("page span fits usize"),
            )],
        })
        .await
        .expect("ingest block 1");
    service
        .ingest_block(FinalizedBlock {
            block_number: 2,
            block_hash: B256::repeat_byte(2),
            parent_hash: B256::repeat_byte(1),
            logs_by_tx: vec![repeated_logs(
                Address::repeat_byte(8),
                vec![B256::repeat_byte(10)],
                4,
            )],
        })
        .await
        .expect("ingest block 2");

    let stream = sharded_stream_id("addr", address.as_slice(), 0);
    meta_store.clear_key(
        LogTables::<InMemoryMetaStore, InMemoryBlobStore>::LOG_BITMAP_PAGE_BLOB_TABLE,
        &stream_page_key(&stream, 0),
    );

    let error = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(2),
            order: QueryOrder::Ascending,
            limit: 10,
            filter: LogFilter {
                address: Some(HashSet::from([address])),
                topics: [Some(HashSet::from([topic])), None, None, None],
            },
        })
        .await
        .expect_err("missing page blob should error");

    assert_eq!(
        error.to_string(),
        "missing data: missing log bitmap page blob"
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
