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
    Address, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, MonadChainDataService,
    QueryEnvelope, QueryLimits, QueryOrder, QueryTransactionsRequest, TxFilter, TxsRelations, B256,
};

mod common;

use common::{chain_header, ingest_tx, test_header};

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_transactions_filters_by_from_across_blocks() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let alice = Address::repeat_byte(0xaa);
    let bob = Address::repeat_byte(0xbb);
    let recipient = Address::repeat_byte(0x11);

    let h1 = test_header(1, B256::ZERO);
    service
        .ingest_block(FinalizedBlock {
            header: h1.clone(),
            logs_by_tx: vec![vec![], vec![]],
            txs: vec![
                ingest_tx(alice, Some(recipient), Vec::new()),
                ingest_tx(bob, Some(recipient), Vec::new()),
            ],
        })
        .await
        .expect("ingest block 1");

    let h2 = chain_header(2, &h1);
    service
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![]],
            txs: vec![ingest_tx(alice, Some(recipient), Vec::new())],
        })
        .await
        .expect("ingest block 2");

    let response = service
        .query_transactions(QueryTransactionsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: TxFilter {
                from: Some(HashSet::from([alice])),
                ..Default::default()
            },
            relations: TxsRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(response.txs.len(), 2);
    assert!(response.txs.iter().all(|t| t.sender == alice));
    let block_numbers: Vec<u64> = response.txs.iter().map(|t| t.block_number).collect();
    assert_eq!(block_numbers, vec![1, 2]);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_transactions_rejects_contract_creation_under_to_filter() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let sender = Address::repeat_byte(0x01);
    let target = Address::repeat_byte(0x22);

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![], vec![]],
            txs: vec![
                ingest_tx(sender, None, Vec::new()),
                ingest_tx(sender, Some(target), Vec::new()),
            ],
        })
        .await
        .expect("ingest");

    let response = service
        .query_transactions(QueryTransactionsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: TxFilter {
                to: Some(HashSet::from([target])),
                ..Default::default()
            },
            relations: TxsRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(response.txs.len(), 1);
    assert_eq!(response.txs[0].tx_idx, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_transactions_filters_by_selector() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let sender = Address::repeat_byte(0x01);
    let target = Address::repeat_byte(0x22);
    let selector = [0xde, 0xad, 0xbe, 0xef];

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![], vec![]],
            txs: vec![
                ingest_tx(sender, Some(target), vec![0xaa, 0xbb, 0xcc, 0xdd, 0x01]),
                ingest_tx(sender, Some(target), vec![0xde, 0xad, 0xbe, 0xef, 0x02]),
            ],
        })
        .await
        .expect("ingest");

    let response = service
        .query_transactions(QueryTransactionsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: TxFilter {
                selector: Some(HashSet::from([selector])),
                ..Default::default()
            },
            relations: TxsRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(response.txs.len(), 1);
    assert_eq!(response.txs[0].tx_idx, 1);
    assert_eq!(
        response.txs[0].selector().expect("selector"),
        Some(selector)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn indexed_query_transactions_selector_filter_skips_short_input() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let sender = Address::repeat_byte(0x01);
    let target = Address::repeat_byte(0x22);
    let selector = [0xde, 0xad, 0xbe, 0xef];

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![]],
            txs: vec![ingest_tx(sender, Some(target), vec![0xde, 0xad, 0xbe])],
        })
        .await
        .expect("ingest");

    let response = service
        .query_transactions(QueryTransactionsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: TxFilter {
                selector: Some(HashSet::from([selector])),
                ..Default::default()
            },
            relations: TxsRelations::default(),
        })
        .await
        .expect("query");

    assert!(response.txs.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn block_scan_query_transactions_returns_all_txs_in_block_order() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let a = Address::repeat_byte(1);
    let b = Address::repeat_byte(2);
    let c = Address::repeat_byte(3);

    let h1 = test_header(1, B256::ZERO);
    service
        .ingest_block(FinalizedBlock {
            header: h1.clone(),
            logs_by_tx: vec![vec![], vec![]],
            txs: vec![
                ingest_tx(a, None, Vec::new()),
                ingest_tx(b, None, Vec::new()),
            ],
        })
        .await
        .expect("ingest block 1");

    let h2 = chain_header(2, &h1);
    service
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![]],
            txs: vec![ingest_tx(c, None, Vec::new())],
        })
        .await
        .expect("ingest block 2");

    let response = service
        .query_transactions(QueryTransactionsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(2),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: TxFilter::default(),
            relations: TxsRelations::default(),
        })
        .await
        .expect("query");

    let senders: Vec<Address> = response.txs.iter().map(|t| t.sender).collect();
    assert_eq!(senders, vec![a, b, c]);
}
