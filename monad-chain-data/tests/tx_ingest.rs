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

use monad_chain_data::{
    txs::BlockTxHeader, Family, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, IngestTx,
    MonadChainDataError, MonadChainDataService, PrimaryId, QueryLimits, B256,
};

mod common;

use common::{chain_header, minimal_ingest_tx, test_header};

#[tokio::test(flavor = "current_thread")]
async fn ingest_persists_tx_artifacts_for_block_with_txs() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let outcome = service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![], vec![]],
            txs: vec![minimal_ingest_tx(), minimal_ingest_tx()],
        })
        .await
        .expect("ingest block 1");

    assert_eq!(outcome.written_txs, 2);
    assert_eq!(outcome.block_record.txs.count, 2);
    assert_eq!(outcome.block_record.txs.first_primary_id, PrimaryId::ZERO);

    let tx_family = service.tables().family(Family::Tx);
    let header_bytes = tx_family
        .load_block_header(1)
        .await
        .expect("load tx header")
        .expect("tx header present");
    let tx_header = BlockTxHeader::decode(&header_bytes).expect("decode tx header");
    assert_eq!(tx_header.tx_count(), 2);

    let blob = tx_family
        .load_block_blob(1)
        .await
        .expect("load tx blob")
        .expect("tx blob present");
    assert_eq!(
        blob.len(),
        usize::try_from(*tx_header.offsets.last().unwrap()).unwrap()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn tx_id_window_advances_across_blocks() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let h1 = test_header(1, B256::ZERO);
    let outcome1 = service
        .ingest_block(FinalizedBlock {
            header: h1.clone(),
            logs_by_tx: vec![vec![], vec![]],
            txs: vec![minimal_ingest_tx(), minimal_ingest_tx()],
        })
        .await
        .expect("ingest block 1");
    assert_eq!(outcome1.block_record.txs.first_primary_id, PrimaryId::ZERO);
    assert_eq!(outcome1.block_record.txs.count, 2);

    let h2 = chain_header(2, &h1);
    let outcome2 = service
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![vec![]],
            txs: vec![minimal_ingest_tx()],
        })
        .await
        .expect("ingest block 2");
    assert_eq!(
        outcome2.block_record.txs.first_primary_id,
        PrimaryId::new(2)
    );
    assert_eq!(outcome2.block_record.txs.count, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_rejects_invalid_signed_tx_bytes_before_writing_tx_artifacts() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let err = service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![]],
            txs: vec![IngestTx {
                tx_hash: B256::repeat_byte(0x33),
                signed_tx_bytes: vec![0x01].into(),
                ..Default::default()
            }],
        })
        .await
        .expect_err("invalid signed tx should fail ingest");

    assert!(
        matches!(
            err,
            MonadChainDataError::Decode("invalid signed tx envelope")
        ),
        "expected invalid envelope decode error, got {err:?}"
    );

    let tx_family = service.tables().family(Family::Tx);
    assert!(tx_family
        .load_block_header(1)
        .await
        .expect("load tx header")
        .is_none());
    assert!(tx_family
        .load_block_blob(1)
        .await
        .expect("load tx blob")
        .is_none());
}
