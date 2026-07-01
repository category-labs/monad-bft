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

//! Epoch-based row-dictionary lifecycle, end-to-end with a small epoch.

use monad_chain_data::{
    engine::{
        family::Family,
        tables::{DictConfig, QueryRuntimeConfig},
    },
    store::CacheConfig,
    Address, Bytes, InMemoryBlobStore, InMemoryMetaStore, Log, LogData, LogFilter,
    MonadChainDataService, QueryLimits, B256,
};

mod common;

use common::{ascending_envelope, chain_of_blocks, log_filter, logs_request};

const EPOCH_BLOCKS: u64 = 8;

fn small_dict_config() -> DictConfig {
    DictConfig {
        epoch_blocks: EPOCH_BLOCKS,
        sample_span_blocks: EPOCH_BLOCKS,
        max_dict_size_bytes: 16 * 1024,
        // Tiny so a few blocks' worth of rows trains a real dictionary.
        min_training_samples: 2,
    }
}

/// Service sharing the small-epoch dict config so epoch boundaries match what the engine stamped.
fn dict_service(
    store: &common::populate::PopulatedStore,
) -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
    MonadChainDataService::with_all_configs(
        store.meta.clone(),
        store.blob.clone(),
        QueryLimits::UNLIMITED,
        CacheConfig::default(),
        small_dict_config(),
        QueryRuntimeConfig::default(),
    )
}

/// Log payload sharing structure across blocks so a dictionary is trainable.
fn structured_log(addr: Address, topic: B256, seed: u8) -> Log {
    let mut data = b"row-shared-prefix-padding-padding-".to_vec();
    data.push(seed);
    data.extend_from_slice(b"-shared-suffix-padding-padding-padding");
    Log {
        address: addr,
        data: LogData::new_unchecked(vec![topic], Bytes::from(data)),
    }
}

async fn populate_run(count: u64, addr: Address, topic: B256) -> common::populate::PopulatedStore {
    let blocks = chain_of_blocks(count, |n| vec![vec![structured_log(addr, topic, n as u8)]]);
    common::populate::populate_via_engine_with_dict(blocks, small_dict_config()).await
}

#[tokio::test(flavor = "current_thread")]
async fn epochs_stamp_versions_and_round_trip_across_boundaries() {
    let addr = Address::repeat_byte(7);
    let topic = B256::repeat_byte(9);

    let store = populate_run(24, addr, topic).await;
    let service = store.reader();

    for n in 1..=24u64 {
        let header = service
            .tables()
            .family(Family::Log)
            .load_blob_header(n)
            .await
            .expect("load header")
            .expect("header present");
        assert_eq!(
            u64::from(header.dict_version),
            n / EPOCH_BLOCKS,
            "block {n} dict_version should equal its epoch"
        );
    }

    let page = service
        .query_logs(logs_request(
            ascending_envelope(1, 24, 100),
            log_filter(addr, topic),
        ))
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
    let addr = Address::repeat_byte(1);
    let topic = B256::repeat_byte(3);

    // Reach epoch 1 so block 8+ is dictionary-backed (or sentinel-plain).
    let store = populate_run(9, addr, topic).await;
    let service = store.reader();

    // No indexed clause => block-scan path.
    let page = service
        .query_logs(logs_request(
            ascending_envelope(8, 9, 100),
            LogFilter::default(),
        ))
        .await
        .expect("query");
    assert_eq!(page.logs.len(), 2);
    assert_eq!(page.logs[0].block_number, 8);
    assert_eq!(page.logs[1].block_number, 9);
    assert_eq!(page.logs[0].address, addr);
}

#[tokio::test(flavor = "current_thread")]
async fn ensure_epoch_dict_is_idempotent() {
    let addr = Address::repeat_byte(2);
    let topic = B256::repeat_byte(4);

    // Populate epoch 0 so version 1 has blocks to train from.
    let store = populate_run(7, addr, topic).await;
    let service = dict_service(&store);

    service
        .tables()
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

    service
        .tables()
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
    // No txs ingested, so the Tx family lacks training samples and must publish the sentinel.
    let addr = Address::repeat_byte(5);
    let topic = B256::repeat_byte(6);

    let store = populate_run(9, addr, topic).await;
    let service = store.reader();

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

    let page = service
        .query_logs(logs_request(
            ascending_envelope(1, 9, 100),
            log_filter(addr, topic),
        ))
        .await
        .expect("query");
    assert_eq!(page.logs.len(), 9);
}
