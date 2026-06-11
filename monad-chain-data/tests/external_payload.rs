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

mod common;

use std::{collections::HashSet, sync::Arc};

use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::Address;
use alloy_rlp::Encodable;
use common::{ingest_tx, test_header};
use monad_chain_data::{
    testkit::{populate_via_engine_external, try_populate_via_engine_external},
    ExternalFamilyRegion, ExternalPayloadSpec, FinalizedBlock, InMemoryExternalBlobReader,
    MonadChainDataError, QueryEnvelope, QueryOrder, QueryTransactionsRequest, TxFilter,
};

const SENDER: Address = Address::repeat_byte(0x77);

/// Encodes one `TxEnvelopeWithSender` archive item (mirrors monad-eth-types:
/// list of `[rlp-string(network tx), sender]`).
fn tx_item(tx: &monad_chain_data::IngestTx) -> Vec<u8> {
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
/// they are never read.
fn fixture() -> (FinalizedBlock, Vec<u8>) {
    let mut tx = ingest_tx(SENDER, Some(Address::repeat_byte(0x88)), vec![1, 2, 3, 4]);
    // External reads derive `tx_hash` from the envelope; the source's
    // caller-authoritative hash must agree (as the archive worker's does).
    tx.tx_hash = *alloy_consensus::TxEnvelope::decode_2718(&mut tx.signed_tx_bytes.as_ref())
        .unwrap()
        .tx_hash();
    let mut block = FinalizedBlock {
        header: test_header(1, alloy_primitives::B256::ZERO),
        logs_by_tx: vec![vec![]],
        txs: vec![tx],
        traces: Vec::new(),
        external: None,
    };
    let item = tx_item(&block.txs[0]);
    let spec = ExternalPayloadSpec {
        block_number: 1,
        txs: ExternalFamilyRegion {
            key: b"block/000000000001".to_vec(),
            base_offset: 0,
            container_offsets: vec![0, item.len() as u32],
            container_rows: Vec::new(),
            container_status: Vec::new(),
        },
        logs: ExternalFamilyRegion {
            key: b"receipts/000000000001".to_vec(),
            base_offset: 0,
            container_offsets: vec![0, 4],
            container_rows: vec![0],
            container_status: Vec::new(),
        },
        traces: ExternalFamilyRegion {
            key: b"traces/000000000001".to_vec(),
            base_offset: 0,
            container_offsets: vec![0, 4],
            container_rows: vec![0],
            container_status: vec![0],
        },
    };
    block.external = Some(spec);
    (block, item)
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
        matches!(err, MonadChainDataError::InvalidBlock(_)),
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
            matches!(err, MonadChainDataError::InvalidBlock(_)),
            "breakage {i}: expected InvalidBlock, got {err:?}"
        );
    }
}
