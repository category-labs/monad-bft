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

#![cfg(feature = "fjall")]

use bytes::Bytes;
use monad_chain_data::{
    engine::tables::PublicationTables,
    store::{
        BlobStore, BlobTableId, BlobWriteBatch, CasOutcome, FjallStore, FjallTuning, MetaStore,
        MetaStoreCas, MetaWriteBatch, ScannableTableId, TableId,
    },
    FinalizedBlock, IngestTx, MonadChainDataService, QueryLimits, B256,
};

mod common;
use common::{minimal_ingest_tx, test_header};

const T_KV: TableId = TableId::new("fjall_batch_atomicity_kv");
const T_SCAN: ScannableTableId = ScannableTableId::new("fjall_batch_atomicity_scan");
const T_CAS: TableId = TableId::new("fjall_batch_atomicity_cas");
const T_BLOB: BlobTableId = BlobTableId::new("fjall_batch_atomicity_blob");

#[tokio::test(flavor = "current_thread")]
async fn meta_batch_writes_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("open");
        let mut b = MetaStore::begin_batch(&store);
        b.put(T_KV, b"k", Bytes::from_static(b"v"));
        b.scan_put(T_SCAN, b"p", b"c", Bytes::from_static(b"sv"));
        b.commit().await.expect("commit");
    }
    let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("reopen");
    assert_eq!(
        store.get(T_KV, b"k").await.unwrap().as_deref(),
        Some(&b"v"[..])
    );
    assert_eq!(
        store.scan_get(T_SCAN, b"p", b"c").await.unwrap().as_deref(),
        Some(&b"sv"[..])
    );
}

#[tokio::test(flavor = "current_thread")]
async fn blob_batch_writes_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("open");
        let mut b = BlobStore::begin_batch(&store);
        b.put_blob(T_BLOB, b"k", Bytes::from_static(b"v"));
        b.commit().await.expect("commit");
    }
    let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("reopen");
    assert_eq!(
        store.get_blob(T_BLOB, b"k").await.unwrap().as_deref(),
        Some(&b"v"[..])
    );
}

#[tokio::test(flavor = "current_thread")]
async fn commit_with_cas_conflict_does_not_persist_meta_writes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("open");
    // Seed CAS row at version 1.
    store
        .cas_put(T_CAS, b"head", None, Bytes::from_static(b"v0"))
        .await
        .expect("seed");

    let mut b = MetaStore::begin_batch(&store);
    b.put(T_KV, b"orphan", Bytes::from_static(b"x"));
    let outcome = b
        .commit_with_cas(T_CAS, b"head", None, Bytes::from_static(b"v1"))
        .await
        .expect("commit_with_cas");
    assert!(matches!(outcome, CasOutcome::Conflict { .. }));

    // Reopen and check the orphan write did not land.
    drop(store);
    let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("reopen");
    assert!(store.get(T_KV, b"orphan").await.unwrap().is_none());
    let (_, value) = store
        .cas_get(T_CAS, b"head")
        .await
        .unwrap()
        .expect("seeded");
    assert_eq!(value.as_ref(), b"v0");
}

fn block_with_tx(number: u64, parent_hash: B256, sender_byte: u8) -> FinalizedBlock {
    let header = test_header(number, parent_hash);
    let mut tx: IngestTx = minimal_ingest_tx();
    tx.sender = alloy_primitives::Address::repeat_byte(sender_byte);
    FinalizedBlock {
        header,
        logs_by_tx: vec![vec![]],
        txs: vec![tx],
        traces: vec![],
    }
}

// Phase A (data writes) commits before Phase B (CAS-anchored publication
// advance). A crash in that window leaves data rows on disk with the
// publication head unchanged. We simulate the window by ingesting once,
// removing the CAS row (the only thing Phase B contributed for a no-seal
// run, plus the publication state for any run), reopening, and asserting
// that re-running `ingest_blocks` on the same input completes successfully
// and reaches the same final on-disk head.
#[tokio::test(flavor = "current_thread")]
async fn ingest_blocks_retries_idempotently_after_phase_a_only_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let blocks = {
        let mut out = Vec::new();
        let mut parent = B256::ZERO;
        for i in 0..3 {
            let b = block_with_tx((i + 1) as u64, parent, (i + 1) as u8);
            parent = b.header.hash_slow();
            out.push(b);
        }
        out
    };

    {
        let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("open");
        let service = MonadChainDataService::new(store.clone(), store, QueryLimits::UNLIMITED);
        service
            .ingest_blocks(blocks.clone())
            .await
            .expect("first ingest");
    }

    // Wipe the CAS row to reproduce a Phase-A-only crash window: Phase A
    // rows remain on disk, but the publication head row never landed.
    {
        let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("reopen for cas clear");
        store
            .clear_cas_key(
                PublicationTables::<FjallStore>::PUBLICATION_STATE_TABLE,
                PublicationTables::<FjallStore>::PUBLICATION_STATE_KEY,
            )
            .await
            .expect("clear cas");
        let head = store
            .cas_get(
                PublicationTables::<FjallStore>::PUBLICATION_STATE_TABLE,
                PublicationTables::<FjallStore>::PUBLICATION_STATE_KEY,
            )
            .await
            .unwrap();
        assert!(head.is_none(), "CAS row must be gone after clear");
    }

    // Reopen with a fresh service. The publication head is missing so
    // `ingest_blocks` sees `expected_version = None`. It must re-stage
    // the same Phase A rows (idempotent overwrite) and then advance the
    // head via the CAS path.
    let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("reopen for retry");
    let service = MonadChainDataService::new(store.clone(), store, QueryLimits::UNLIMITED);

    assert!(
        service.publication().load_published_head().await.unwrap().is_none(),
        "head must be absent before retry"
    );

    service
        .ingest_blocks(blocks.clone())
        .await
        .expect("retry ingest must succeed idempotently");

    let head = service.publication().load_published_head().await.unwrap();
    assert_eq!(head, Some(blocks.len() as u64), "head advanced on retry");
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_blocks_idempotent_after_reopen_with_published_head() {
    let dir = tempfile::tempdir().expect("tempdir");
    let blocks = {
        let mut out = Vec::new();
        let mut parent = B256::ZERO;
        for i in 0..4 {
            let b = block_with_tx((i + 1) as u64, parent, (i + 1) as u8);
            parent = b.header.hash_slow();
            out.push(b);
        }
        out
    };

    {
        let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("open");
        let service = MonadChainDataService::new(store.clone(), store, QueryLimits::UNLIMITED);
        service
            .ingest_blocks(blocks.clone())
            .await
            .expect("ingest");
    }

    let store = FjallStore::open(dir.path(), FjallTuning::default()).expect("reopen");
    let service = MonadChainDataService::new(store.clone(), store, QueryLimits::UNLIMITED);
    let head = service.publication().load_published_head().await.unwrap();
    assert_eq!(head, Some(blocks.len() as u64));
}
