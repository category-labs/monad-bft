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
    Address, Bytes, EvmBlockHeader, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log,
    LogData, LogFilter, LogsRelations, MonadChainDataService, QueryEnvelope, QueryLimits,
    QueryLogsRequest, QueryOrder, B256,
};

mod common;

use common::{chain_header, test_header};

#[tokio::test(flavor = "current_thread")]
async fn include_blocks_false_omits_block_headers() {
    let (service, _) = ingest_three_blocks().await;

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(3),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
        .await
        .expect("query");

    assert!(page.blocks.is_none());
    assert_eq!(page.logs.len(), 3);
}

#[tokio::test(flavor = "current_thread")]
async fn include_blocks_true_returns_deduped_headers_for_matched_blocks() {
    // Block 2 has no matching logs (address differs); only blocks 1 and 3
    // should appear in the relation.
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    let matching = Address::repeat_byte(7);
    let other = Address::repeat_byte(9);

    let h1 = tagged_header(1, B256::ZERO);
    let h2 = tagged_chain_header(2, &h1);
    let h3 = tagged_chain_header(3, &h2);
    let h1_hash = h1.hash_slow();
    let h3_hash = h3.hash_slow();

    ingest(&service, h1, vec![log(matching), log(matching)]).await;
    ingest(&service, h2, vec![log(other)]).await;
    ingest(&service, h3, vec![log(matching)]).await;

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(3),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: LogFilter {
                address: Some(HashSet::from([matching])),
                topics: [None, None, None, None],
            },
            relations: LogsRelations {
                blocks: true,
                transactions: false,
            },
        })
        .await
        .expect("query");

    let blocks = page.blocks.expect("blocks relation");
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].header.number, 1);
    assert_eq!(blocks[1].header.number, 3);
    assert_eq!(blocks[0].hash, h1_hash);
    assert_eq!(blocks[1].hash, h3_hash);
    assert_eq!(blocks[0].header.beneficiary, Address::repeat_byte(0x11));
    assert_eq!(blocks[1].header.beneficiary, Address::repeat_byte(0x33));
}

#[tokio::test(flavor = "current_thread")]
async fn descending_query_still_returns_blocks_ascending() {
    let (service, _) = ingest_three_blocks().await;

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(3),
                to_block: Some(1),
                order: QueryOrder::Descending,
                limit: 100,
            },
            filter: LogFilter::default(),
            relations: LogsRelations {
                blocks: true,
                transactions: false,
            },
        })
        .await
        .expect("query");

    let blocks = page.blocks.expect("blocks relation");
    let numbers: Vec<u64> = blocks.iter().map(|b| b.header.number).collect();
    assert_eq!(numbers, vec![1, 2, 3]);
}

#[tokio::test(flavor = "current_thread")]
async fn include_blocks_empty_result_returns_empty_blocks() {
    let (service, _) = ingest_three_blocks().await;

    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(3),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: LogFilter {
                address: Some(HashSet::from([Address::repeat_byte(0xEE)])),
                topics: [None, None, None, None],
            },
            relations: LogsRelations {
                blocks: true,
                transactions: false,
            },
        })
        .await
        .expect("query");

    assert!(page.logs.is_empty());
    let blocks = page.blocks.expect("blocks relation");
    assert!(blocks.is_empty());
}

async fn ingest_three_blocks() -> (
    MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore>,
    [B256; 3],
) {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );
    let addr = Address::repeat_byte(7);

    let h1 = tagged_header(1, B256::ZERO);
    let h2 = tagged_chain_header(2, &h1);
    let h3 = tagged_chain_header(3, &h2);
    let hashes = [h1.hash_slow(), h2.hash_slow(), h3.hash_slow()];

    for header in [h1, h2, h3] {
        ingest(&service, header, vec![log(addr)]).await;
    }
    (service, hashes)
}

async fn ingest(
    service: &MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore>,
    header: EvmBlockHeader,
    logs: Vec<Log>,
) {
    service
        .ingest_block(FinalizedBlock {
            header,
            logs_by_tx: vec![logs],
            txs: Vec::new(),
        })
        .await
        .expect("ingest");
}

fn tagged_header(number: u64, parent_hash: B256) -> EvmBlockHeader {
    let mut header = test_header(number, parent_hash);
    header.beneficiary = Address::repeat_byte(tag_byte(number));
    header
}

fn tagged_chain_header(number: u64, parent: &EvmBlockHeader) -> EvmBlockHeader {
    let mut header = chain_header(number, parent);
    header.beneficiary = Address::repeat_byte(tag_byte(number));
    header
}

fn tag_byte(n: u64) -> u8 {
    // Distinct non-default byte per block so header.beneficiary asserts catch drift.
    ((n << 4) | n) as u8
}

fn log(address: Address) -> Log {
    Log {
        address,
        data: LogData::new_unchecked(vec![B256::repeat_byte(0xAA)], Bytes::from(vec![1, 2, 3])),
    }
}
