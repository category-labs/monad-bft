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
    error::MonadChainDataError, Address, Bytes, FinalizedBlock, InMemoryBlobStore,
    InMemoryMetaStore, Log, LogData, LogFilter, MonadChainDataService, QueryLogsRequest,
    QueryOrder, B256,
};

#[tokio::test(flavor = "current_thread")]
async fn query_logs_paginates_at_block_boundaries() {
    let service =
        MonadChainDataService::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());

    service
        .ingest_block(FinalizedBlock {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            parent_hash: B256::ZERO,
            logs_by_tx: vec![vec![
                log(Address::repeat_byte(7), B256::repeat_byte(9)),
                log(Address::repeat_byte(7), B256::repeat_byte(9)),
            ]],
        })
        .await
        .expect("ingest block 1");

    service
        .ingest_block(FinalizedBlock {
            block_number: 2,
            block_hash: B256::repeat_byte(2),
            parent_hash: B256::repeat_byte(1),
            logs_by_tx: vec![vec![log(Address::repeat_byte(7), B256::repeat_byte(9))]],
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
    assert_eq!(first_page.logs[0].block_number, 1);
    assert_eq!(first_page.logs[1].block_number, 1);

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
    assert_eq!(second_page.logs[0].block_number, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn query_logs_descending_returns_newest_first() {
    let service =
        MonadChainDataService::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());

    service
        .ingest_block(FinalizedBlock {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            parent_hash: B256::ZERO,
            logs_by_tx: vec![vec![
                log(Address::repeat_byte(5), B256::repeat_byte(8)),
                log(Address::repeat_byte(5), B256::repeat_byte(8)),
            ]],
        })
        .await
        .expect("ingest");

    let page = service
        .query_logs(QueryLogsRequest {
            from_block: Some(1),
            to_block: Some(1),
            order: QueryOrder::Descending,
            limit: 1,
            filter: LogFilter {
                address: Some(HashSet::from([Address::repeat_byte(5)])),
                topics: [
                    Some(HashSet::from([B256::repeat_byte(8)])),
                    None,
                    None,
                    None,
                ],
            },
        })
        .await
        .expect("query");

    assert_eq!(page.logs.len(), 2);
    assert_eq!(page.logs[0].log_index, 1);
    assert_eq!(page.logs[1].log_index, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn query_logs_rejects_from_block_above_published_head() {
    let service =
        MonadChainDataService::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());

    service
        .ingest_block(FinalizedBlock {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            parent_hash: B256::ZERO,
            logs_by_tx: vec![vec![log(Address::repeat_byte(5), B256::repeat_byte(8))]],
        })
        .await
        .expect("ingest");

    let err = service
        .query_logs(QueryLogsRequest {
            from_block: Some(2),
            to_block: None,
            ..QueryLogsRequest::default()
        })
        .await
        .expect_err("from_block above published head should error");

    assert!(matches!(
        err,
        MonadChainDataError::InvalidRequest("from_block must be <= published head")
    ));
}

fn log(address: Address, topic0: B256) -> Log {
    Log {
        address,
        data: LogData::new_unchecked(vec![topic0], Bytes::from(vec![1, 2, 3])),
    }
}
