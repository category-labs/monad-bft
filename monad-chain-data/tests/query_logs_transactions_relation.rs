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
    Address, Bytes, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log, LogData, LogFilter,
    LogsRelations, MonadChainDataService, QueryEnvelope, QueryLimits, QueryLogsRequest, QueryOrder,
    B256,
};

mod common;

use common::{chain_header, ingest_tx, test_header};

#[tokio::test(flavor = "current_thread")]
async fn include_transactions_false_omits_transactions() {
    let service = build_service().await;

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
        .await
        .expect("query");

    assert!(!page.logs.is_empty());
    assert!(page.transactions.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn include_transactions_true_returns_deduped_txs_sorted() {
    let service = build_service().await;

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: LogFilter::default(),
            relations: LogsRelations {
                blocks: false,
                transactions: true,
            },
        })
        .await
        .expect("query");

    // Block 1 has two txs each emitting logs; block 2 has one tx with a log.
    // Block 1's first tx emits two logs, so dedup must collapse them.
    let transactions = page.transactions.expect("transactions relation");
    assert_eq!(transactions.len(), 3);
    let keys: Vec<(u64, u32)> = transactions
        .iter()
        .map(|t| (t.block_number, t.tx_idx))
        .collect();
    assert_eq!(keys, vec![(1, 0), (1, 1), (2, 0)]);
}

#[tokio::test(flavor = "current_thread")]
async fn include_transactions_true_filtered_logs_returns_only_referenced_txs() {
    let service = build_service().await;
    let target = Address::repeat_byte(0x77);

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: LogFilter {
                address: Some(HashSet::from([target])),
                topics: [None, None, None, None],
            },
            relations: LogsRelations {
                blocks: false,
                transactions: true,
            },
        })
        .await
        .expect("query");

    // Only the log on block 1 tx 1 matches `target`.
    assert_eq!(page.logs.len(), 1);
    let transactions = page.transactions.expect("transactions relation");
    assert_eq!(transactions.len(), 1);
    assert_eq!(transactions[0].block_number, 1);
    assert_eq!(transactions[0].tx_idx, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn include_transactions_true_empty_logs_returns_empty_transactions() {
    let service = build_service().await;

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: LogFilter {
                address: Some(HashSet::from([Address::repeat_byte(0xEE)])),
                topics: [None, None, None, None],
            },
            relations: LogsRelations {
                blocks: false,
                transactions: true,
            },
        })
        .await
        .expect("query");

    assert!(page.logs.is_empty());
    let transactions = page.transactions.expect("transactions relation");
    assert!(transactions.is_empty());
}

async fn build_service() -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let alice = Address::repeat_byte(0xaa);
    let bob = Address::repeat_byte(0xbb);
    let carol = Address::repeat_byte(0xcc);
    let addr_default = Address::repeat_byte(1);
    let target = Address::repeat_byte(0x77);

    let h1 = test_header(1, B256::ZERO);
    service
        .ingest_block(FinalizedBlock {
            header: h1.clone(),
            // tx0 emits two logs; tx1 emits one log matching `target`.
            logs_by_tx: vec![
                vec![log(addr_default), log(addr_default)],
                vec![log(target)],
            ],
            txs: vec![
                ingest_tx(alice, Some(addr_default), Vec::new()),
                ingest_tx(bob, Some(target), Vec::new()),
            ],
        })
        .await
        .expect("ingest block 1");

    let h2 = chain_header(2, &h1);
    service
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![log(addr_default)]],
            txs: vec![ingest_tx(carol, Some(addr_default), Vec::new())],
        })
        .await
        .expect("ingest block 2");

    service
}

fn log(address: Address) -> Log {
    Log {
        address,
        data: LogData::new_unchecked(vec![B256::repeat_byte(0xAA)], Bytes::from(vec![1, 2, 3])),
    }
}
