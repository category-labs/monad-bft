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
    kernel::{bitmap::STREAM_PAGE_LOCAL_ID_SPAN, primary_dir::DIRECTORY_BUCKET_SIZE},
    Address, Bytes, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log, LogData, LogFilter,
    MonadChainDataService, QueryLogsRequest, QueryOrder, Topic, B256,
};

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_logs_respects_and_or_filter_semantics() {
    let service =
        MonadChainDataService::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());

    service
        .ingest_block(FinalizedBlock {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            parent_hash: B256::ZERO,
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
        })
        .await
        .expect("ingest block");

    let page = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(1),
            order: QueryOrder::Ascending,
            limit: 10,
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
        })
        .await
        .expect("query");

    assert_eq!(page.logs.len(), 1);
    assert_eq!(page.logs[0].address, Address::repeat_byte(2));
    assert_eq!(page.cursor_block.number, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_logs_descending_returns_newest_first() {
    let service =
        MonadChainDataService::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());

    service
        .ingest_block(FinalizedBlock {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            parent_hash: B256::ZERO,
            logs_by_tx: vec![vec![log(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
            )]],
        })
        .await
        .expect("ingest block 1");

    service
        .ingest_block(FinalizedBlock {
            block_number: 2,
            block_hash: B256::repeat_byte(2),
            parent_hash: B256::repeat_byte(1),
            logs_by_tx: vec![vec![
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
            ]],
        })
        .await
        .expect("ingest block 2");

    let page = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(2),
            order: QueryOrder::Descending,
            limit: 1,
            filter: LogFilter {
                address: Some(HashSet::from([Address::repeat_byte(7)])),
                topics: [
                    Some(HashSet::from([B256::repeat_byte(9)])),
                    None,
                    None,
                    None,
                ],
            },
        })
        .await
        .expect("query");

    assert_eq!(page.logs.len(), 2);
    assert_eq!(page.logs[0].block_number, 2);
    assert_eq!(page.logs[0].log_index, 1);
    assert_eq!(page.logs[1].block_number, 2);
    assert_eq!(page.logs[1].log_index, 0);
    assert_eq!(page.cursor_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_logs_paginates_at_block_boundaries() {
    let service =
        MonadChainDataService::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());

    service
        .ingest_block(FinalizedBlock {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            parent_hash: B256::ZERO,
            logs_by_tx: vec![vec![
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
            ]],
        })
        .await
        .expect("ingest block 1");

    service
        .ingest_block(FinalizedBlock {
            block_number: 2,
            block_hash: B256::repeat_byte(2),
            parent_hash: B256::repeat_byte(1),
            logs_by_tx: vec![vec![log(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
            )]],
        })
        .await
        .expect("ingest block 2");

    let first_page = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(2),
            order: QueryOrder::Ascending,
            limit: 1,
            filter: LogFilter {
                address: Some(HashSet::from([Address::repeat_byte(7)])),
                topics: [
                    Some(HashSet::from([B256::repeat_byte(9)])),
                    None,
                    None,
                    None,
                ],
            },
        })
        .await
        .expect("first page");

    assert_eq!(first_page.logs.len(), 2);
    assert_eq!(first_page.cursor_block.number, 1);

    let second_page = service
        .query_logs(QueryLogsRequest {
            from_block: Some(first_page.cursor_block.number + 1),
            to_block: Some(2),
            order: QueryOrder::Ascending,
            limit: 1,
            filter: LogFilter {
                address: Some(HashSet::from([Address::repeat_byte(7)])),
                topics: [
                    Some(HashSet::from([B256::repeat_byte(9)])),
                    None,
                    None,
                    None,
                ],
            },
        })
        .await
        .expect("second page");

    assert_eq!(second_page.logs.len(), 1);
    assert_eq!(second_page.cursor_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_logs_scans_across_bucket_and_page_boundaries() {
    let service =
        MonadChainDataService::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());

    service
        .ingest_block(FinalizedBlock {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            parent_hash: B256::ZERO,
            logs_by_tx: vec![repeated_logs(
                Address::repeat_byte(1),
                vec![B256::repeat_byte(3)],
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
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
                usize::try_from(DIRECTORY_BUCKET_SIZE + 4)
                    .expect("directory bucket size fits usize"),
            )],
        })
        .await
        .expect("ingest block 2");

    let page = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(2),
            order: QueryOrder::Ascending,
            limit: 10,
            filter: LogFilter {
                address: Some(HashSet::from([Address::repeat_byte(7)])),
                topics: [
                    Some(HashSet::from([B256::repeat_byte(9)])),
                    None,
                    None,
                    None,
                ],
            },
        })
        .await
        .expect("query");

    assert_eq!(
        page.logs.len(),
        usize::try_from(DIRECTORY_BUCKET_SIZE + 4).expect("directory bucket size fits usize")
    );
    assert!(page.logs.iter().all(|log| log.block_number == 2));
    assert_eq!(page.cursor_block.number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn unindexed_query_logs_still_uses_block_scan_fallback() {
    let service =
        MonadChainDataService::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());

    service
        .ingest_block(FinalizedBlock {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            parent_hash: B256::ZERO,
            logs_by_tx: vec![vec![
                log(Address::repeat_byte(5), vec![B256::repeat_byte(8)]),
                log(Address::repeat_byte(6), vec![B256::repeat_byte(9)]),
            ]],
        })
        .await
        .expect("ingest");

    let page = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(1),
            order: QueryOrder::Ascending,
            limit: 1,
            filter: LogFilter::default(),
        })
        .await
        .expect("query");

    assert_eq!(page.logs.len(), 2);
    assert_eq!(page.cursor_block.number, 1);
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
