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

//! End-to-end coverage for the epoch-based row-level dictionary lifecycle.
//! With a small epoch (`epoch_blocks = 8`), ingesting across epoch boundaries
//! drives deterministic per-epoch dictionary publication: epoch-0 blocks are
//! version 0 (plain bootstrap), and later epochs train (or publish the
//! empty-dict sentinel) before any of their blocks are written. Every block
//! round-trips through the point-read and block-scan materialization paths.

use std::collections::HashSet;

use monad_chain_data::{
    engine::{family::Family, tables::DictConfig},
    store::CacheConfig,
    Address, Bytes, EvmBlockHeader, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log,
    LogData, LogFilter, LogsRelations, MonadChainDataService, QueryEnvelope, QueryLimits,
    QueryLogsRequest, QueryOrder, B256,
};

mod common;

use common::{chain_header, test_header};

const EPOCH_BLOCKS: u64 = 8;

/// A small-epoch config that trains over a handful of blocks, so the epoch
/// lifecycle runs end-to-end in a unit test.
fn small_dict_config() -> DictConfig {
    DictConfig {
        epoch_blocks: EPOCH_BLOCKS,
        sample_span: EPOCH_BLOCKS,
        max_dict_size_bytes: 16 * 1024,
        // Tiny so a few blocks' worth of rows trains a real dictionary.
        min_training_samples: 2,
    }
}

fn service(dict: DictConfig) -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
    MonadChainDataService::with_configs(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
        CacheConfig::default(),
        dict,
    )
}

/// A log whose payload shares structure across blocks so a dictionary is
/// trainable. `seed` varies the topic/data so rows are not identical.
fn structured_log(addr: Address, topic: B256, seed: u8) -> Log {
    let mut data = b"row-shared-prefix-padding-padding-".to_vec();
    data.push(seed);
    data.extend_from_slice(b"-shared-suffix-padding-padding-padding");
    Log {
        address: addr,
        data: LogData::new_unchecked(vec![topic], Bytes::from(data)),
    }
}

/// Ingests blocks `1..=count` (each with one structured log) and returns the
/// header chain so callers can keep extending it.
async fn ingest_run(
    service: &MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore>,
    count: u64,
    addr: Address,
    topic: B256,
) -> Vec<EvmBlockHeader> {
    let mut headers: Vec<EvmBlockHeader> = Vec::new();
    for n in 1..=count {
        let header = if n == 1 {
            test_header(1, B256::ZERO)
        } else {
            chain_header(n, &headers[(n - 2) as usize])
        };
        service
            .ingest_block(FinalizedBlock {
                header: header.clone(),
                logs_by_tx: vec![vec![structured_log(addr, topic, (n % 251) as u8)]],
                txs: Vec::new(),
                traces: vec![],
            })
            .await
            .unwrap_or_else(|e| panic!("ingest block {n}: {e:?}"));
        headers.push(header);
    }
    headers
}

#[tokio::test(flavor = "current_thread")]
async fn epochs_stamp_versions_and_round_trip_across_boundaries() {
    let service = service(small_dict_config());
    let addr = Address::repeat_byte(7);
    let topic = B256::repeat_byte(9);

    // 24 blocks => epoch 0 (1..=7), epoch 1 (8..=15), epoch 2 (16..=23, 24).
    ingest_run(&service, 24, addr, topic).await;

    // Per-block header dict_version matches the epoch number.
    for n in 1..=24u64 {
        let raw = service
            .tables()
            .family(Family::Log)
            .load_block_header(n)
            .await
            .expect("load header")
            .expect("header present");
        let header = monad_chain_data::logs::BlockBlobHeader::decode(&raw).expect("decode header");
        assert_eq!(
            u64::from(header.dict_version),
            n / EPOCH_BLOCKS,
            "block {n} dict_version should equal its epoch"
        );
    }

    // Indexed query spanning all three epochs: each row decodes under its own
    // header-stamped dict version (0 plain, 1 and 2 trained/sentinel).
    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(24),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: LogFilter {
                address: Some(HashSet::from([addr])),
                topics: [Some(HashSet::from([topic])), None, None, None],
            },
            relations: LogsRelations::default(),
        })
        .await
        .expect("query");
    assert_eq!(page.logs.len(), 24, "one matching log per block");
    for (i, entry) in page.logs.iter().enumerate() {
        assert_eq!(entry.block_number, (i + 1) as u64);
        assert_eq!(entry.address, addr);
        assert_eq!(entry.topics, vec![topic]);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn full_scan_decodes_epoch_dictionary_frames() {
    let service = service(small_dict_config());
    let addr = Address::repeat_byte(1);
    let topic = B256::repeat_byte(3);

    // Reach epoch 1 so block 8+ is dictionary-backed (or sentinel-plain).
    ingest_run(&service, 9, addr, topic).await;

    // No indexed clause => block-scan path, decompressing every frame with the
    // per-block decoder resolved from the header's dict version.
    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(8),
                to_block: Some(9),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: LogFilter::default(),
            relations: LogsRelations::default(),
        })
        .await
        .expect("query");
    assert_eq!(page.logs.len(), 2);
    assert_eq!(page.logs[0].block_number, 8);
    assert_eq!(page.logs[1].block_number, 9);
    assert_eq!(page.logs[0].address, addr);
}

#[tokio::test(flavor = "current_thread")]
async fn ensure_epoch_dict_is_idempotent() {
    let service = service(small_dict_config());
    let addr = Address::repeat_byte(2);
    let topic = B256::repeat_byte(4);

    // Populate epoch 0 so version 1 has blocks to train from.
    ingest_run(&service, 7, addr, topic).await;

    service
        .ensure_epoch_dict(Family::Log, 1)
        .await
        .expect("first ensure");
    let bytes1 = service
        .tables()
        .family(Family::Log)
        .load_dict(1)
        .await
        .expect("load dict")
        .expect("dict published");

    // Repeated calls are no-ops: same published bytes, no panic, codec present.
    service
        .ensure_epoch_dict(Family::Log, 1)
        .await
        .expect("second ensure");
    let bytes2 = service
        .tables()
        .family(Family::Log)
        .load_dict(1)
        .await
        .expect("load dict")
        .expect("dict published");
    assert_eq!(bytes1, bytes2);
    assert!(service.tables().dicts().has_codec(Family::Log, 1));
}

#[tokio::test(flavor = "current_thread")]
async fn low_data_family_publishes_empty_sentinel_and_round_trips() {
    // Txs/traces are empty in this run, so the Tx family never accumulates the
    // 2 samples needed and must publish the empty-dict sentinel for epoch 1.
    let service = service(small_dict_config());
    let addr = Address::repeat_byte(5);
    let topic = B256::repeat_byte(6);

    // Ingest into epoch 1 so the Tx family's version-1 dict gets resolved.
    ingest_run(&service, 9, addr, topic).await;

    // The Tx epoch-1 dict is the empty sentinel: present-but-empty.
    let tx_dict = service
        .tables()
        .family(Family::Tx)
        .load_dict(1)
        .await
        .expect("load tx dict")
        .expect("tx dict published (sentinel)");
    assert!(
        tx_dict.is_empty(),
        "low-data family should publish sentinel"
    );

    // Logs still round-trip across the boundary regardless.
    let page = service
        .query_logs(QueryLogsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(9),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: LogFilter {
                address: Some(HashSet::from([addr])),
                topics: [Some(HashSet::from([topic])), None, None, None],
            },
            relations: LogsRelations::default(),
        })
        .await
        .expect("query");
    assert_eq!(page.logs.len(), 9);
}
