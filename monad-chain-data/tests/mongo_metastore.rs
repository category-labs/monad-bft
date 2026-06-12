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

//! Run: `docker run -p 27017:27017 mongo`, then
//! `CHAIN_DATA_MONGO_TEST_URL=mongodb://127.0.0.1:27017 \
//!   cargo test -p monad-chain-data --features mongo --test mongo_metastore -- --ignored`
#![cfg(feature = "mongo")]

mod common;

use std::collections::HashSet;

use alloy_primitives::Address;
use alloy_rlp::Encodable;
use bytes::Bytes;
use common::{ingest_tx, test_header};
use monad_chain_data::{
    config::{
        ChainDataArchiveBackendConfig, ChainDataArchiveMongoConfig, ChainDataEngineConfig,
        ChainDataMetaBackendConfig, ChainDataMongoMetaConfig, ChainDataPayloadConfig,
        ChainDataStoreConfig, Redacted,
    },
    open_configured_chain_data_reader, run_configured_chain_data_engine_ingest,
    store::{
        MetaStore, MetaWriteOp, MongoMetaStore, MongoMetaStoreConfig, ScannableTableId, TableId,
    },
    testkit::VecSource,
    ExternalFamilyRegion, ExternalPayloadSpec, FinalizedBlock, QueryEnvelope, QueryLimits,
    QueryOrder, QueryTransactionsRequest, TxFilter,
};

const KV: TableId = TableId::new("it_kv");
const SCAN: ScannableTableId = ScannableTableId::new("it_scan");
const OTHER_SCAN: ScannableTableId = ScannableTableId::new("it_scan_other");

const SENDER: Address = Address::repeat_byte(0x77);

fn test_url() -> Option<String> {
    std::env::var("CHAIN_DATA_MONGO_TEST_URL").ok()
}

fn unique_database(test: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("chain_data_it_{test}_{nanos}")
}

/// `None` (silent skip) when `CHAIN_DATA_MONGO_TEST_URL` is unset.
async fn fresh_store(test: &str) -> Option<(MongoMetaStore, String)> {
    let url = test_url()?;
    let database = unique_database(test);
    let store = MongoMetaStore::new(MongoMetaStoreConfig::new(
        url,
        database.clone(),
        "chain_data_meta",
    ))
    .await
    .expect("build store");
    Some((store, database))
}

async fn drop_database(database: &str) {
    let client = mongodb::Client::with_uri_str(&test_url().unwrap())
        .await
        .expect("client");
    client.database(database).drop().await.expect("drop db");
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_MONGO_TEST_URL (a running MongoDB)"]
async fn get_put_round_trip() {
    let Some((store, database)) = fresh_store("getput").await else {
        return;
    };

    assert_eq!(store.get(KV, b"missing").await.unwrap(), None);
    store.put(KV, b"k", Bytes::from_static(b"v")).await.unwrap();
    assert_eq!(
        store.get(KV, b"k").await.unwrap(),
        Some(Bytes::from_static(b"v"))
    );
    store
        .put(KV, b"k", Bytes::from_static(b"v2"))
        .await
        .unwrap();
    assert_eq!(
        store.get(KV, b"k").await.unwrap(),
        Some(Bytes::from_static(b"v2"))
    );
    // Empty keys and empty values are both representable.
    store.put(KV, b"", Bytes::new()).await.unwrap();
    assert_eq!(store.get(KV, b"").await.unwrap(), Some(Bytes::new()));

    assert_eq!(store.scan_get(SCAN, b"p", b"c").await.unwrap(), None);
    store
        .scan_put(SCAN, b"p", b"c", Bytes::from_static(b"sv"))
        .await
        .unwrap();
    assert_eq!(
        store.scan_get(SCAN, b"p", b"c").await.unwrap(),
        Some(Bytes::from_static(b"sv"))
    );

    drop_database(&database).await;
}

#[tokio::test]
#[ignore = "requires CHAIN_DATA_MONGO_TEST_URL (a running MongoDB)"]
async fn scan_keys_drains_whole_partition_in_clustering_order() {
    let Some((store, database)) = fresh_store("scankeys").await else {
        return;
    };

    let part = 7u64.to_be_bytes();
    let value = Bytes::from(vec![0u8; 4096]);
    // Insert in shuffled order so the returned order is the index's, not
    // insertion's. Crosses the write-concurrency batch size (400 > 64).
    let mut ops: Vec<MetaWriteOp> = (0u16..400)
        .rev()
        .map(|i| MetaWriteOp::ScanPut {
            table: SCAN,
            partition: part.to_vec(),
            clustering: i.to_be_bytes().to_vec(),
            value: value.clone(),
        })
        .collect();
    // Neighboring partitions of the same table, and the same partition of
    // another table, must not leak into the scan.
    ops.push(MetaWriteOp::ScanPut {
        table: SCAN,
        partition: 6u64.to_be_bytes().to_vec(),
        clustering: vec![0xff, 0xff],
        value: Bytes::from_static(b"x"),
    });
    ops.push(MetaWriteOp::ScanPut {
        table: SCAN,
        partition: 8u64.to_be_bytes().to_vec(),
        clustering: vec![0, 0],
        value: Bytes::from_static(b"x"),
    });
    ops.push(MetaWriteOp::ScanPut {
        table: OTHER_SCAN,
        partition: part.to_vec(),
        clustering: vec![0, 1],
        value: Bytes::from_static(b"x"),
    });
    store.apply_writes(ops).await.unwrap();

    let keys = store.scan_keys(SCAN, &part).await.unwrap();
    let expected: Vec<Vec<u8>> = (0u16..400).map(|i| i.to_be_bytes().to_vec()).collect();
    assert_eq!(keys, expected, "whole partition in clustering byte order");

    drop_database(&database).await;
}

/// Encodes one `TxEnvelopeWithSender` archive item (mirrors monad-eth-types:
/// list of `[rlp-string(network tx), sender]`).
fn tx_item(tx: &monad_chain_data::IngestTx) -> Vec<u8> {
    use alloy_eips::eip2718::Decodable2718;

    let envelope =
        alloy_consensus::TxEnvelope::decode_2718(&mut tx.signed_tx_bytes.as_ref()).unwrap();
    let mut network = Vec::new();
    envelope.encode(&mut network);
    let mut out = Vec::new();
    let payload: &[&dyn Encodable] = &[&network.as_slice(), &tx.sender];
    alloy_rlp::encode_list::<_, dyn Encodable>(payload, &mut out);
    out
}

/// An empty external block (the genesis shape: zero containers everywhere).
fn empty_fixture_at(number: u64, parent_hash: alloy_primitives::B256) -> FinalizedBlock {
    let region = |prefix: &str| ExternalFamilyRegion {
        key: format!("{prefix}/{number:012}").into_bytes(),
        base_offset: 0,
        container_offsets: vec![0],
        container_rows: Vec::new(),
        container_status: Vec::new(),
    };
    FinalizedBlock {
        header: test_header(number, parent_hash),
        logs_by_tx: Vec::new(),
        txs: Vec::new(),
        traces: Vec::new(),
        external: Some(ExternalPayloadSpec {
            block_number: number,
            txs: region("block"),
            logs: region("receipts"),
            traces: region("traces"),
        }),
    }
}

/// One-tx external block (no logs, no traces): the receipt/trace regions
/// point at placeholder ranges holding zero rows, so they are never read.
fn fixture_at(number: u64, parent_hash: alloy_primitives::B256) -> (FinalizedBlock, Vec<u8>) {
    use alloy_eips::eip2718::Decodable2718;

    let mut tx = ingest_tx(
        SENDER,
        Some(Address::repeat_byte(0x88)),
        vec![1, 2, 3, number as u8],
    );
    tx.tx_hash = *alloy_consensus::TxEnvelope::decode_2718(&mut tx.signed_tx_bytes.as_ref())
        .unwrap()
        .tx_hash();
    let mut block = FinalizedBlock {
        header: test_header(number, parent_hash),
        logs_by_tx: vec![vec![]],
        txs: vec![tx],
        traces: Vec::new(),
        external: None,
    };
    let item = tx_item(&block.txs[0]);
    block.external = Some(ExternalPayloadSpec {
        block_number: number,
        txs: ExternalFamilyRegion {
            key: format!("block/{number:012}").into_bytes(),
            base_offset: 0,
            container_offsets: vec![0, item.len() as u32],
            container_rows: Vec::new(),
            container_status: Vec::new(),
        },
        logs: ExternalFamilyRegion {
            key: format!("receipts/{number:012}").into_bytes(),
            base_offset: 0,
            container_offsets: vec![0, 4],
            container_rows: vec![0],
            container_status: Vec::new(),
        },
        traces: ExternalFamilyRegion {
            key: format!("traces/{number:012}").into_bytes(),
            base_offset: 0,
            container_offsets: vec![0, 4],
            container_rows: vec![0],
            container_status: vec![0],
        },
    });
    (block, item)
}

/// Inserts an archive object into `block_level` in monad-archive's
/// `KeyValueDocument` shape (`{_id, value: Binary}`).
async fn insert_archive_object(
    collection: &mongodb::Collection<mongodb::bson::Document>,
    key: &str,
    object: &[u8],
) {
    collection
        .insert_one(mongodb::bson::doc! {
            "_id": key,
            "value": mongodb::bson::Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: object.to_vec(),
            },
        })
        .await
        .expect("insert archive object");
}

fn store_config(url: &str, database: &str) -> ChainDataStoreConfig {
    ChainDataStoreConfig {
        meta: ChainDataMetaBackendConfig::Mongo(ChainDataMongoMetaConfig {
            url: Redacted(Some(url.to_string())),
            database: Some(database.to_string()),
            ..ChainDataMongoMetaConfig::default()
        }),
        blob: None,
        archive: Some(ChainDataArchiveBackendConfig::Mongo(
            ChainDataArchiveMongoConfig {
                url: Redacted(Some(url.to_string())),
                database: Some(database.to_string()),
                ..ChainDataArchiveMongoConfig::default()
            },
        )),
        ..ChainDataStoreConfig::default()
    }
}

fn engine_config(end: u64) -> ChainDataEngineConfig {
    ChainDataEngineConfig {
        payload: ChainDataPayloadConfig::ExternalArchive,
        end: Some(end),
        ..ChainDataEngineConfig::default()
    }
}

/// Full configured-path round trip against one MongoDB database: archive
/// objects in `block_level`, external-archive ingest through the configured
/// dispatch into the mongo meta collection, a SECOND run that resumes by
/// rebuilding open state from the fragments (the checkpoint-less recovery
/// path, all reads against mongo), then queries through the configured
/// reader, whose row payloads range-read `block_level`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires CHAIN_DATA_MONGO_TEST_URL (a running MongoDB)"]
async fn engine_ingests_and_queries_through_mongo() {
    let Some(url) = test_url() else {
        return;
    };
    let database = unique_database("engine");
    let client = mongodb::Client::with_uri_str(&url).await.expect("client");
    let block_level = client
        .database(&database)
        .collection::<mongodb::bson::Document>("block_level");

    // Block 0 is empty (the genesis shape: the engine starts ingest at
    // genesis on an empty store); blocks 1..=11 each carry one tx.
    let genesis = empty_fixture_at(0, alloy_primitives::B256::ZERO);
    let mut parent_hash = genesis.header.hash_slow();
    let mut blocks = vec![genesis];
    for number in 1..=11u64 {
        let (block, item) = fixture_at(number, parent_hash);
        insert_archive_object(&block_level, &format!("block/{number:012}"), &item).await;
        parent_hash = block.header.hash_slow();
        blocks.push(block);
    }
    let tx_hashes: Vec<_> = blocks[1..].iter().map(|b| b.txs[0].tx_hash).collect();
    let source = VecSource::new(blocks, 0);

    // First run covers 0..=7; the second resumes from store state (no
    // checkpoint exists on a blob-less store) and extends to 11.
    run_configured_chain_data_engine_ingest(
        store_config(&url, &database),
        engine_config(7),
        source.clone(),
    )
    .await
    .expect("first ingest run");
    run_configured_chain_data_engine_ingest(
        store_config(&url, &database),
        engine_config(11),
        source,
    )
    .await
    .expect("resumed ingest run");

    let reader =
        open_configured_chain_data_reader(store_config(&url, &database), QueryLimits::UNLIMITED)
            .await
            .expect("open reader");
    assert_eq!(reader.load_published_head().await.unwrap(), Some(11));

    let envelope = QueryEnvelope {
        from_block: Some(0),
        to_block: Some(11),
        order: QueryOrder::Ascending,
        limit: 100,
    };
    // Indexed (from clause) and block-scan (no clause) paths both resolve
    // rows through the mongo archive reader.
    for filter in [
        TxFilter {
            from: Some(HashSet::from([SENDER])),
            ..Default::default()
        },
        TxFilter::default(),
    ] {
        let response = reader
            .query_transactions(QueryTransactionsRequest {
                envelope,
                filter,
                ..Default::default()
            })
            .await
            .expect("tx query");
        assert_eq!(response.txs.len(), 11);
        assert_eq!(
            response
                .txs
                .iter()
                .map(|tx| tx.block_number)
                .collect::<Vec<_>>(),
            (1..=11u64).collect::<Vec<_>>()
        );
        assert_eq!(
            response.txs.iter().map(|tx| tx.tx_hash).collect::<Vec<_>>(),
            tx_hashes
        );
        assert!(response.txs.iter().all(|tx| tx.sender == SENDER));
    }

    let (entry, header) = reader
        .get_transaction(tx_hashes[8])
        .await
        .expect("get_transaction")
        .expect("indexed tx");
    assert_eq!(entry.block_number, 9);
    assert_eq!(header.number, 9);

    drop_database(&database).await;
}
