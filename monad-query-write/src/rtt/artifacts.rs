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

//! Ingest storage-contract pins: contiguous per-family primary-id windows
//! (empty blocks included), persisted family artifacts, rejection of
//! undecodable transactions, and the legacy-table freeze.

use alloy_primitives::U256;

use super::prelude::*;

fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_assigns_contiguous_log_id_windows_across_empty_blocks() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    let h3 = chain_header(3, &h2);

    let store = populate::populate_via_engine(vec![
        block_with_logs(
            h1,
            vec![vec![
                log(Address::repeat_byte(3), vec![B256::repeat_byte(4)]),
                log(Address::repeat_byte(3), vec![B256::repeat_byte(4)]),
            ]],
        ),
        block_with_logs(h2, vec![vec![]]),
        block_with_logs(
            h3,
            vec![vec![log(
                Address::repeat_byte(3),
                vec![B256::repeat_byte(4)],
            )]],
        ),
    ])
    .await;
    let service = reader(&store);

    let service = &service;
    let load_block = |number: u64| async move {
        service
            .tables()
            .blocks()
            .load_record(number)
            .await
            .expect("load block")
            .expect("block record")
    };

    let block_1 = load_block(1).await;
    let block_2 = load_block(2).await;
    let block_3 = load_block(3).await;

    assert_eq!(block_1.logs.first_primary_id, PrimaryId::new(0));
    assert_eq!(block_1.logs.count, 2);
    assert_eq!(block_2.logs.first_primary_id, PrimaryId::new(2));
    assert_eq!(block_2.logs.count, 0);
    assert_eq!(block_3.logs.first_primary_id, PrimaryId::new(2));
    assert_eq!(block_3.logs.count, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_persists_tx_artifacts_for_block_with_txs() {
    let store = populate::populate_via_engine(vec![block_with_txs(
        test_header(1, B256::ZERO),
        vec![minimal_ingest_tx(), minimal_ingest_tx()],
    )])
    .await;
    let service = reader(&store);

    let record = load_record(&service, 1).await;
    assert_eq!(record.txs.count, 2);
    assert_eq!(record.txs.first_primary_id, PrimaryId::ZERO);

    let tx_family = service.tables().family(Family::Tx);
    let tx_header = tx_family
        .load_blob_header(1)
        .await
        .expect("load tx header")
        .expect("tx header present");
    assert_eq!(tx_header.row_count(), 2);

    let blob = service
        .tables()
        .read_block_blob_region(1, &tx_header)
        .await
        .expect("load tx region")
        .expect("tx region present");
    assert_eq!(
        blob.len(),
        usize::try_from(*tx_header.offsets.last().unwrap()).unwrap()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn tx_id_window_advances_across_blocks() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    let store = populate::populate_via_engine(vec![
        block_with_txs(h1, vec![minimal_ingest_tx(), minimal_ingest_tx()]),
        block_with_txs(h2, vec![minimal_ingest_tx()]),
    ])
    .await;
    let service = reader(&store);

    let record1 = load_record(&service, 1).await;
    assert_eq!(record1.txs.first_primary_id, PrimaryId::ZERO);
    assert_eq!(record1.txs.count, 2);

    let record2 = load_record(&service, 2).await;
    assert_eq!(record2.txs.first_primary_id, PrimaryId::new(2));
    assert_eq!(record2.txs.count, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_rejects_invalid_signed_tx_bytes() {
    // An undecodable signed-tx envelope must abort ingest, not be silently accepted.
    let result = populate::try_populate_via_engine(vec![block_with_txs(
        test_header(1, B256::ZERO),
        vec![IngestTx {
            tx_hash: B256::repeat_byte(0x33),
            signed_tx_bytes: vec![0x01].into(),
            ..Default::default()
        }],
    )])
    .await;
    let Err(err) = result else {
        panic!("invalid signed tx should fail ingest")
    };

    assert!(
        matches!(err, QueryError::Decode("invalid signed tx envelope")),
        "expected invalid envelope decode error, got {err:?}"
    );
}


#[tokio::test(flavor = "current_thread")]
async fn ingest_persists_trace_artifacts_for_block_with_traces() {
    let traces = vec![
        top_level_call(0, addr(1), addr(2), U256::from(100u64), vec![0xaa; 8]),
        nested_call(0, addr(2), addr(3), U256::from(50u64), vec![]),
        top_level_call(1, addr(4), addr(5), U256::ZERO, vec![]),
    ];

    let store =
        populate::populate_via_engine(vec![block_with_traces(test_header(1, B256::ZERO), traces)])
            .await;
    let service = reader(&store);

    let record = load_record(&service, 1).await;
    assert_eq!(record.traces.count, 3);
    assert_eq!(record.traces.first_primary_id, PrimaryId::ZERO);

    let trace_family = service.tables().family(Family::Trace);
    let trace_header = trace_family
        .load_blob_header(1)
        .await
        .expect("load trace header")
        .expect("trace header present");
    assert_eq!(trace_header.row_count(), 3);

    // The region starts at the family's base_offset, so the last relative offset is its length.
    let blob = service
        .tables()
        .read_block_blob_region(1, &trace_header)
        .await
        .expect("load trace region")
        .expect("trace region present");
    assert_eq!(
        blob.len(),
        usize::try_from(*trace_header.offsets.last().unwrap()).unwrap()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn trace_id_window_advances_across_blocks() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    let store = populate::populate_via_engine(vec![
        block_with_traces(
            h1,
            vec![
                top_level_call(0, addr(1), addr(2), U256::from(1u64), vec![]),
                top_level_call(1, addr(3), addr(4), U256::ZERO, vec![]),
            ],
        ),
        block_with_traces(
            h2,
            vec![top_level_call(0, addr(1), addr(2), U256::ZERO, vec![])],
        ),
    ])
    .await;
    let service = reader(&store);

    let record1 = load_record(&service, 1).await;
    assert_eq!(record1.traces.first_primary_id, PrimaryId::ZERO);
    assert_eq!(record1.traces.count, 2);

    let record2 = load_record(&service, 2).await;
    assert_eq!(record2.traces.first_primary_id, PrimaryId::new(2));
    assert_eq!(record2.traces.count, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_handles_empty_traces() {
    let store = populate::populate_via_engine(vec![block_with_txs(
        test_header(1, B256::ZERO),
        vec![minimal_ingest_tx()],
    )])
    .await;
    let service = reader(&store);

    let record = load_record(&service, 1).await;
    assert_eq!(record.traces.count, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_writes_no_legacy_tables() {
    let store = populate::populate_via_engine(vec![empty_block(test_header(1, B256::ZERO))]).await;

    let kv = store.meta.kv_snapshot();
    assert!(kv
        .keys()
        .any(|(table, _)| *table == TableId::new("block_metadata")));
    for legacy_table in [
        "block_record",
        "block_header",
        "log_block_header",
        "tx_block_header",
        "trace_block_header",
    ] {
        assert!(
            !kv.keys()
                .any(|(table, _)| *table == TableId::new(legacy_table)),
            "ingest should not write legacy {legacy_table} rows"
        );
    }
}
