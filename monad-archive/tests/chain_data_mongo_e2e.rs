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

//! Full configured-path round trip of a Mongo-backed chain-data store, wired
//! exactly as the binaries wire it: archive objects in `block_level`,
//! external-archive ingest through chain-data's configured dispatch into the
//! mongo meta collection, a SECOND run that resumes by rebuilding open state
//! from the fragments (the checkpoint-less recovery path), then queries
//! through the configured reader — whose row payloads range-read
//! `block_level` via this crate's [`ArchiveExternalReader`].
//!
//! Run: `docker run -p 27017:27017 mongo`, then
//! `CHAIN_DATA_MONGO_TEST_URL=mongodb://127.0.0.1:27017 \
//!   cargo test -p monad-archive --test chain_data_mongo_e2e -- --ignored`
#![cfg(feature = "chain-data-ingest")]

use std::collections::HashSet;

use alloy_primitives::{Address, B256, U256};
use alloy_rlp::Encodable;
use monad_archive::chain_data_external::build_archive_external_reader;
use monad_query_config::{
    open_configured_chain_data_reader, run_configured_chain_data_engine_ingest,
    ChainDataArchiveBackendConfig, ChainDataArchiveMongoConfig, ChainDataEngineConfig,
    ChainDataMetaBackendConfig, ChainDataMongoMetaConfig, ChainDataPayloadConfig,
    ChainDataStoreConfig, Redacted,
};
use monad_query_primitives::{
    limits::{QueryEnvelope, QueryLimits},
    order::QueryOrder,
    EvmBlockHeader,
};
use monad_query_read::txs::{QueryTransactionsRequest, TxFilter};
use monad_query_testkit::VecSource;
use monad_query_types::{
    ingest_types::{FinalizedBlock, IngestTx},
    ExternalFamilyRegion, ExternalPayloadSpec,
};

const SENDER: Address = Address::repeat_byte(0x77);

fn test_url() -> Option<String> {
    std::env::var("CHAIN_DATA_MONGO_TEST_URL").ok()
}

fn test_header(number: u64, parent_hash: B256) -> EvmBlockHeader {
    EvmBlockHeader {
        number,
        parent_hash,
        ..EvmBlockHeader::default()
    }
}

fn ingest_tx(sender: Address, to: Option<Address>, input: Vec<u8>) -> IngestTx {
    use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::{Signature, TxKind};

    let signed = TxLegacy {
        chain_id: Some(1),
        nonce: 0,
        gas_price: 0,
        gas_limit: 21_000,
        to: to.map_or(TxKind::Create, TxKind::Call),
        value: U256::ZERO,
        input: input.into(),
    }
    .into_signed(Signature::test_signature());

    let mut signed_tx_bytes = Vec::new();
    TxEnvelope::Legacy(signed).encode_2718(&mut signed_tx_bytes);

    IngestTx {
        tx_hash: B256::ZERO,
        sender,
        signed_tx_bytes: signed_tx_bytes.into(),
    }
}

/// Encodes one `TxEnvelopeWithSender` archive item (mirrors monad-eth-types:
/// list of `[rlp-string(network tx), sender]`).
fn tx_item(tx: &IngestTx) -> Vec<u8> {
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
fn empty_fixture_at(number: u64, parent_hash: B256) -> FinalizedBlock {
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
fn fixture_at(number: u64, parent_hash: B256) -> (FinalizedBlock, Vec<u8>) {
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

/// Inserts an archive object into `block_level` in this crate's
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

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires CHAIN_DATA_MONGO_TEST_URL (a running MongoDB)"]
async fn engine_ingests_and_queries_through_mongo() {
    let Some(url) = test_url() else {
        return;
    };
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let database = format!("chain_data_e2e_{nanos}");
    let client = mongodb::Client::with_uri_str(&url).await.expect("client");
    let block_level = client
        .database(&database)
        .collection::<mongodb::bson::Document>("block_level");

    // Block 0 is empty (the genesis shape: the engine starts ingest at
    // genesis on an empty store); blocks 1..=11 each carry one tx.
    let genesis = empty_fixture_at(0, B256::ZERO);
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

    let external = |config: &ChainDataStoreConfig| {
        let archive = config.archive.clone();
        async move {
            build_archive_external_reader(&archive)
                .await
                .expect("build external reader")
        }
    };

    // First run covers 0..=7; the second resumes from store state (no
    // checkpoint exists on a blob-less store) and extends to 11.
    let config = store_config(&url, &database);
    let reader = external(&config).await;
    run_configured_chain_data_engine_ingest(config, engine_config(7), source.clone(), reader)
        .await
        .expect("first ingest run");
    let config = store_config(&url, &database);
    let reader = external(&config).await;
    run_configured_chain_data_engine_ingest(config, engine_config(11), source, reader)
        .await
        .expect("resumed ingest run");

    let config = store_config(&url, &database);
    let external_reader = external(&config).await;
    let reader = open_configured_chain_data_reader(config, QueryLimits::UNLIMITED, external_reader)
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

    client.database(&database).drop().await.expect("drop db");
}
