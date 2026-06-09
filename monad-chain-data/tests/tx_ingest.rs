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

use monad_chain_data::{Family, FinalizedBlock, IngestTx, MonadChainDataError, PrimaryId, B256};

mod common;

use common::{chain_header, minimal_ingest_tx, test_header};

#[tokio::test(flavor = "current_thread")]
async fn ingest_persists_tx_artifacts_for_block_with_txs() {
    let store = common::populate::populate_via_engine(vec![FinalizedBlock {
        header: test_header(1, B256::ZERO),
        logs_by_tx: vec![vec![], vec![]],
        txs: vec![minimal_ingest_tx(), minimal_ingest_tx()],
        traces: vec![],
    }])
    .await;
    let service = store.reader();

    let record = service
        .tables()
        .blocks()
        .load_record(1)
        .await
        .expect("load record")
        .expect("record present");
    assert_eq!(record.txs.count, 2);
    assert_eq!(record.txs.first_primary_id, PrimaryId::ZERO);

    let tx_family = service.tables().family(Family::Tx);
    let tx_header = tx_family
        .load_block_header(1)
        .await
        .expect("load tx header")
        .expect("tx header present");
    assert_eq!(tx_header.row_count(), 2);

    // Read this family's whole region through the region-cache path; for a
    // single-family block it equals the entire shared object.
    let blob = service
        .tables()
        .read_block_blob_region(Family::Tx, 1, &tx_header)
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
        FinalizedBlock {
            header: h1,
            logs_by_tx: vec![vec![], vec![]],
            txs: vec![minimal_ingest_tx(), minimal_ingest_tx()],
            traces: vec![],
        },
        FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![]],
            txs: vec![minimal_ingest_tx()],
            traces: vec![],
        },
    ])
    .await;
    let service = store.reader();

    let record1 = service
        .tables()
        .blocks()
        .load_record(1)
        .await
        .expect("load record 1")
        .expect("record 1 present");
    assert_eq!(record1.txs.first_primary_id, PrimaryId::ZERO);
    assert_eq!(record1.txs.count, 2);

    let record2 = service
        .tables()
        .blocks()
        .load_record(2)
        .await
        .expect("load record 2")
        .expect("record 2 present");
    assert_eq!(record2.txs.first_primary_id, PrimaryId::new(2));
    assert_eq!(record2.txs.count, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_rejects_invalid_signed_tx_bytes() {
    // The engine decodes the signed-tx envelope while encoding the tx row, so an
    // undecodable envelope aborts the backfill run rather than being silently
    // accepted.
    let result = common::populate::try_populate_via_engine(vec![FinalizedBlock {
        header: test_header(1, B256::ZERO),
        logs_by_tx: vec![vec![]],
        txs: vec![IngestTx {
            tx_hash: B256::repeat_byte(0x33),
            signed_tx_bytes: vec![0x01].into(),
            ..Default::default()
        }],
        traces: vec![],
    }])
    .await;
    let err = match result {
        Ok(_) => panic!("invalid signed tx should fail ingest"),
        Err(err) => err,
    };

    assert!(
        matches!(
            err,
            MonadChainDataError::Decode("invalid signed tx envelope")
        ),
        "expected invalid envelope decode error, got {err:?}"
    );
}
