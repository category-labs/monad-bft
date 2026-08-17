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

use monad_chain_data::{Family, IngestTx, MonadChainDataError, PrimaryId, B256};

mod common;

use common::{block_with_txs, chain_header, load_record, minimal_ingest_tx, test_header};

#[tokio::test(flavor = "current_thread")]
async fn ingest_persists_tx_artifacts_for_block_with_txs() {
    let store = common::populate::populate_via_engine(vec![block_with_txs(
        test_header(1, B256::ZERO),
        vec![minimal_ingest_tx(), minimal_ingest_tx()],
    )])
    .await;
    let service = store.reader();

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
    let store = common::populate::populate_via_engine(vec![
        block_with_txs(h1, vec![minimal_ingest_tx(), minimal_ingest_tx()]),
        block_with_txs(h2, vec![minimal_ingest_tx()]),
    ])
    .await;
    let service = store.reader();

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
    let result = common::populate::try_populate_via_engine(vec![block_with_txs(
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
        matches!(
            err,
            MonadChainDataError::Decode("invalid signed tx envelope")
        ),
        "expected invalid envelope decode error, got {err:?}"
    );
}
