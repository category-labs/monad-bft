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

//! External-payload ingest validation and a minimal end-to-end fixture over
//! hand-encoded archive containers. The full-fidelity guard (real archive
//! encoders + scanner) is the cross-crate round-trip test in `monad-archive`;
//! these tests cover the chain-data-side invariants it cannot: spec/block
//! mismatches must hard-fail at ingest.
use std::{collections::HashSet, sync::Arc};

use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::Address;
use alloy_rlp::Encodable;
use monad_query_tests::prelude::*;

const SENDER: Address = Address::repeat_byte(0x77);

/// Encodes one `TxEnvelopeWithSender` archive item (mirrors monad-eth-types:
/// list of `[rlp-string(network tx), sender]`).
fn tx_item(tx: &IngestTx) -> Vec<u8> {
    let envelope =
        alloy_consensus::TxEnvelope::decode_2718(&mut tx.signed_tx_bytes.as_ref()).unwrap();
    let mut network = Vec::new();
    envelope.encode(&mut network);
    let mut out = Vec::new();
    let payload: &[&dyn Encodable] = &[&network.as_slice(), &tx.sender];
    alloy_rlp::encode_list::<_, dyn Encodable>(payload, &mut out);
    out
}

/// One-tx block (no logs, no traces) plus a consistent external spec; the
/// receipt/trace regions point at placeholder objects that hold zero rows, so
/// they are never read. The tx input varies with `number` so tx hashes are
/// unique across a chain of these blocks.
fn fixture_at(number: u64, parent_hash: alloy_primitives::B256) -> (FinalizedBlock, Vec<u8>) {
    let mut tx = ingest_tx(
        SENDER,
        Some(Address::repeat_byte(0x88)),
        vec![1, 2, 3, number as u8],
    );
    // External reads derive `tx_hash` from the envelope; the source's
    // caller-authoritative hash must agree (as the archive worker's does).
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
    let spec = ExternalPayloadSpec {
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
    };
    block.external = Some(spec);
    (block, item)
}

fn fixture() -> (FinalizedBlock, Vec<u8>) {
    fixture_at(1, alloy_primitives::B256::ZERO)
}

/// Parent-linked one-tx external blocks for `numbers`, each archive object
/// inserted into `reader`. Returns the blocks plus the last header hash (for
/// chaining a continuation).
fn external_chain(
    numbers: std::ops::RangeInclusive<u64>,
    mut parent_hash: alloy_primitives::B256,
    reader: &InMemoryExternalBlobReader,
) -> (Vec<FinalizedBlock>, alloy_primitives::B256) {
    let mut blocks = Vec::new();
    for number in numbers {
        let (block, item) = fixture_at(number, parent_hash);
        reader.insert(
            block
                .external
                .as_ref()
                .expect("fixture spec")
                .txs
                .key
                .clone(),
            item,
        );
        parent_hash = block.header.hash_slow();
        blocks.push(block);
    }
    (blocks, parent_hash)
}

#[tokio::test(flavor = "multi_thread")]
async fn external_fixture_round_trips_through_queries() {
    let (block, item) = fixture();
    let reader = Arc::new(InMemoryExternalBlobReader::default());
    reader.insert(b"block/000000000001".to_vec(), item);
    let tx_hash = block.txs[0].tx_hash;

    let store = populate_via_engine_external(vec![block], reader).await;
    let service = store.reader();

    let envelope = QueryEnvelope {
        from_block: Some(1),
        to_block: Some(1),
        order: QueryOrder::Ascending,
        limit: 10,
    };
    // Indexed (from clause) and block-scan (no clause) both resolve the row
    // through the external container.
    for filter in [
        TxFilter {
            from: Some(HashSet::from([SENDER])),
            ..Default::default()
        },
        TxFilter::default(),
    ] {
        let response = service
            .query_transactions(QueryTransactionsRequest {
                envelope,
                filter,
                ..Default::default()
            })
            .await
            .expect("external tx query");
        assert_eq!(response.txs.len(), 1);
        assert_eq!(response.txs[0].sender, SENDER);
        assert_eq!(response.txs[0].tx_hash, tx_hash);
    }

    let (entry, header) = service
        .get_transaction(tx_hash)
        .await
        .expect("get_transaction")
        .expect("indexed tx");
    assert_eq!(entry.block_number, 1);
    assert_eq!(header.number, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn external_ingest_requires_a_spec_on_every_block() {
    let (mut block, _) = fixture();
    block.external = None;
    let err = match try_populate_via_engine_external(
        vec![block],
        Arc::new(InMemoryExternalBlobReader::default()),
    )
    .await
    {
        Ok(_) => panic!("spec-less block must fail external ingest"),
        Err(err) => err,
    };
    assert!(
        matches!(err, QueryError::InvalidBlock(_)),
        "expected InvalidBlock, got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn external_ingest_rejects_specs_that_disagree_with_the_block() {
    let breakages: Vec<fn(&mut ExternalPayloadSpec)> = vec![
        // Wrong container count for the tx family.
        |spec| spec.txs.container_offsets = vec![0],
        // Non-ascending container partition.
        |spec| spec.txs.container_offsets = vec![0, 0],
        // Log manifest claims a log the block does not have.
        |spec| spec.logs.container_rows = vec![1],
        // Missing archive key.
        |spec| spec.traces.key = Vec::new(),
        // Status bitvec with a stray pad bit.
        |spec| spec.traces.container_status = vec![0b10],
        // Identity family must not carry a row manifest.
        |spec| spec.txs.container_rows = vec![1],
        // Spec scanned from a different block.
        |spec| spec.block_number = 2,
    ];
    for (i, breakage) in breakages.into_iter().enumerate() {
        let (mut block, _) = fixture();
        breakage(block.external.as_mut().expect("fixture spec"));
        let err = match try_populate_via_engine_external(
            vec![block],
            Arc::new(InMemoryExternalBlobReader::default()),
        )
        .await
        {
            Ok(_) => panic!("breakage {i}: spec/block mismatch must fail ingest"),
            Err(err) => err,
        };
        assert!(
            matches!(err, QueryError::InvalidBlock(_)),
            "breakage {i}: expected InvalidBlock, got {err:?}"
        );
    }
}

/// End-to-end over a store with NO blob backend: external ingest over
/// `NullBlobStore` (checkpoints disabled) completes and queries answer.
/// `NullBlobStore` errors on EVERY access, so the successful populate and
/// queries are themselves the assertion that an external-archive store issues
/// zero blob-store calls.
#[tokio::test(flavor = "multi_thread")]
async fn no_blob_store_serves_external_queries() {
    let reader = Arc::new(InMemoryExternalBlobReader::default());
    let (blocks, _) = external_chain(1..=3, alloy_primitives::B256::ZERO, &reader);
    let tx_hashes: Vec<_> = blocks.iter().map(|b| b.txs[0].tx_hash).collect();

    let store = populate_via_engine_external_no_blob(blocks, reader).await;
    let service = store.reader();

    assert_eq!(
        service.publication().load_published_head().await.unwrap(),
        Some(3)
    );

    let envelope = QueryEnvelope {
        from_block: Some(1),
        to_block: Some(3),
        order: QueryOrder::Ascending,
        limit: 10,
    };
    // Indexed (from clause) and block-scan (no clause) paths both resolve rows
    // through the external containers — never the (absent) blob store.
    for filter in [
        TxFilter {
            from: Some(HashSet::from([SENDER])),
            ..Default::default()
        },
        TxFilter::default(),
    ] {
        let response = service
            .query_transactions(QueryTransactionsRequest {
                envelope,
                filter,
                ..Default::default()
            })
            .await
            .expect("blob-less external tx query");
        assert_eq!(response.txs.len(), 3);
    }

    for (i, tx_hash) in tx_hashes.iter().enumerate() {
        let (entry, _) = service
            .get_transaction(*tx_hash)
            .await
            .expect("get_transaction")
            .expect("indexed tx");
        assert_eq!(entry.block_number, i as u64 + 1);
    }
}

/// Restartability without checkpoints: with no blob store the engine never
/// snapshots, so a SECOND engine over the same meta store must resume from
/// the published head by rebuilding its open state from fragments — and the
/// continuation blocks must land seamlessly after the originals.
#[tokio::test(flavor = "multi_thread")]
async fn no_blob_store_resumes_from_published_head_without_checkpoints() {
    let reader = Arc::new(InMemoryExternalBlobReader::default());
    let (first, last_hash) = external_chain(1..=3, alloy_primitives::B256::ZERO, &reader);

    // First engine: blocks 1..=3, then dropped.
    let store = populate_via_engine_external_no_blob(first, reader.clone()).await;

    // Checkpoints disabled means NO snapshot manifest was ever committed; the
    // resume below is forced through the fragments rebuild.
    assert_eq!(
        store
            .meta
            .get(TableId::new("ingest_snapshot"), b"latest")
            .await
            .unwrap(),
        None,
        "a blob-less ingest must never write a checkpoint snapshot"
    );

    // Second engine over the SAME meta store: continuation blocks 4..=6.
    // The resume point is self-verifying: the VecSource only holds 4..=6, so
    // an engine that cold-started (or resumed anywhere below 4) would fetch a
    // block the source cannot serve and fail the populate outright.
    let (more, _) = external_chain(4..=6, last_hash, &reader);
    populate_more_via_engine_external_no_blob(&store, more).await;

    let service = store.reader();
    assert_eq!(
        service.publication().load_published_head().await.unwrap(),
        Some(6)
    );
    let response = service
        .query_transactions(QueryTransactionsRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(6),
                order: QueryOrder::Ascending,
                limit: 10,
            },
            filter: TxFilter {
                from: Some(HashSet::from([SENDER])),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .expect("post-resume tx query");
    assert_eq!(response.txs.len(), 6);
    assert_eq!(
        response
            .txs
            .iter()
            .map(|tx| tx.block_number)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6]
    );
}

/// Migration: a store ingested with a BLOB-backed config (which committed a
/// checkpoint manifest) is continued after dropping `[store.blob]`. The
/// leftover manifest must be ignored (head covers it) instead of wedging
/// recovery on an unreadable blob payload.
#[tokio::test(flavor = "multi_thread")]
async fn dropping_the_blob_store_does_not_wedge_an_external_store() {
    let reader = Arc::new(InMemoryExternalBlobReader::default());
    let (first, last_hash) = external_chain(1..=3, alloy_primitives::B256::ZERO, &reader);

    // Blob-backed external ingest: the bounded run's terminal checkpoint
    // commits a snapshot manifest at the head.
    let store = populate_via_engine_external(first, reader.clone()).await;
    assert!(
        store
            .meta
            .get(TableId::new("ingest_snapshot"), b"latest")
            .await
            .unwrap()
            .is_some(),
        "blob-backed populate must leave a checkpoint manifest behind"
    );

    // Same META store, blob store dropped.
    let migrated = PopulatedStore {
        meta: store.meta.clone(),
        blob: NullBlobStore::default(),
        external: store.external.clone(),
    };
    let (more, _) = external_chain(4..=6, last_hash, &reader);
    populate_more_via_engine_external_no_blob(&migrated, more).await;

    let service = migrated.reader();
    assert_eq!(
        service.publication().load_published_head().await.unwrap(),
        Some(6)
    );
}
