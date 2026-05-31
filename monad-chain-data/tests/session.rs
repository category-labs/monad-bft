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

mod common;

use std::time::Duration;

use bytes::Bytes;
use monad_chain_data::{
    engine::{
        family::Family,
        tables::{BlockTables, PublicationTables, Tables},
    },
    error::MonadChainDataError,
    family::Hash32,
    store::{
        CacheConfig, CasOutcome, CasVersion, InMemoryBlobStore, MetaStore, PublicationCasParams,
    },
};

use crate::common::observed_store::{ObservedBlobStore, ObservedMetaStore};

fn cache() -> CacheConfig {
    CacheConfig {
        block_header_entries: 64,
        block_hash_to_number_entries: 64,
        dir_by_block_entries: 64,
        dir_bucket_entries: 64,
        bitmap_by_block_entries: 64,
        bitmap_page_blob_entries: 64,
        bitmap_page_counts_entries: 64,
        open_bitmap_stream_entries: 64,
        tx_hash_index_entries: 64,
        block_blob_entries: 64,
    }
}

// Tests stage a hash-index entry as a stand-in for any "did this write
// reach the backend?" check: the hash index accepts arbitrary 32-byte keys
// with 8-byte values and is otherwise read via [`MetaStore::get`] on
// `BLOCK_HASH_TO_NUMBER_INDEX_TABLE`, so the test can verify presence
// without going through a typed loader.
fn test_hash(byte: u8) -> Hash32 {
    Hash32::from([byte; 32])
}

fn block_record(number: u64) -> monad_chain_data::BlockRecord {
    let window = monad_chain_data::FamilyWindowRecord {
        first_primary_id: monad_chain_data::PrimaryId::ZERO,
        count: 0,
    };
    monad_chain_data::BlockRecord {
        block_number: number,
        block_hash: Default::default(),
        parent_hash: Default::default(),
        logs: window,
        txs: window,
        traces: window,
        artifact_checksum: Default::default(),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn closure_error_does_not_flush() {
    let meta = ObservedMetaStore::new();
    let blob = InMemoryBlobStore::default();
    let tables = Tables::with_cache_config(meta.clone(), blob, cache());
    let tables_ref = &tables;
    let hash = test_hash(0xAB);
    let hash_ref = &hash;

    let result = tables
        .with_writes(|w| {
            Box::pin(async move {
                tables_ref.blocks().stage_hash_index(w, hash_ref, 7);
                Err(MonadChainDataError::Backend("intentional".into()))
            })
        })
        .await;
    assert!(result.is_err());
    assert!(meta
        .get(
            BlockTables::<ObservedMetaStore>::BLOCK_HASH_TO_NUMBER_INDEX_TABLE,
            hash.as_slice(),
        )
        .await
        .unwrap()
        .is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn closure_error_evicts_populated_cache_entries() {
    let meta = ObservedMetaStore::new();
    let blob = InMemoryBlobStore::default();
    let tables = Tables::with_cache_config(meta, blob, cache());
    let tables_ref = &tables;
    let header = monad_chain_data::EvmBlockHeader {
        number: 1,
        ..Default::default()
    };
    let header_ref = &header;
    let record = block_record(1);
    let record_ref = &record;

    let _ = tables
        .with_writes(|w| {
            Box::pin(async move {
                tables_ref.blocks().stage_metadata(
                    w,
                    1,
                    record_ref,
                    header_ref,
                    Bytes::new(),
                    Bytes::new(),
                    Bytes::new(),
                );
                Err(MonadChainDataError::Backend("intentional".into()))
            })
        })
        .await;

    let read = tables.blocks().load_header(1).await.unwrap();
    assert!(
        read.is_none(),
        "cache must be invalidated after closure error"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn partial_flush_failure_evicts_cache() {
    let meta = ObservedMetaStore::new();
    meta.mode.fail_apply_writes_once();
    let blob = InMemoryBlobStore::default();
    let tables = Tables::with_cache_config(meta, blob, cache());
    let tables_ref = &tables;
    let header = monad_chain_data::EvmBlockHeader {
        number: 1,
        ..Default::default()
    };
    let header_ref = &header;
    let record = block_record(1);
    let record_ref = &record;

    let result = tables
        .with_writes(|w| {
            Box::pin(async move {
                tables_ref.blocks().stage_metadata(
                    w,
                    1,
                    record_ref,
                    header_ref,
                    Bytes::new(),
                    Bytes::new(),
                    Bytes::new(),
                );
                Ok(())
            })
        })
        .await;
    assert!(result.is_err());

    let read = tables.blocks().load_header(1).await.unwrap();
    assert!(read.is_none(), "partial flush failure must evict cache");
}

#[tokio::test(flavor = "current_thread")]
async fn closure_error_then_retry_is_idempotent() {
    let meta = ObservedMetaStore::new();
    let blob = InMemoryBlobStore::default();
    let tables = Tables::with_cache_config(meta.clone(), blob, cache());
    let tables_ref = &tables;
    let hash = test_hash(0xCD);
    let hash_ref = &hash;

    let _ = tables
        .with_writes(|w| {
            Box::pin(async move {
                tables_ref.blocks().stage_hash_index(w, hash_ref, 42);
                Err(MonadChainDataError::Backend("intentional".into()))
            })
        })
        .await;

    tables
        .with_writes(|w| {
            Box::pin(async move {
                tables_ref.blocks().stage_hash_index(w, hash_ref, 42);
                Ok(())
            })
        })
        .await
        .expect("retry succeeds");

    let stored = meta
        .get(
            BlockTables::<ObservedMetaStore>::BLOCK_HASH_TO_NUMBER_INDEX_TABLE,
            hash.as_slice(),
        )
        .await
        .unwrap();
    assert_eq!(stored.as_deref(), Some(&42u64.to_be_bytes()[..]));
}

#[tokio::test(flavor = "current_thread")]
async fn with_writes_and_cas_flushes_before_cas_conflict() {
    let meta = ObservedMetaStore::new();
    let blob = InMemoryBlobStore::default();
    let tables = Tables::with_cache_config(meta.clone(), blob, cache());
    let tables_ref = &tables;

    let cas = PublicationCasParams {
        table: PublicationTables::<ObservedMetaStore>::PUBLICATION_STATE_TABLE,
        key: PublicationTables::<ObservedMetaStore>::PUBLICATION_STATE_KEY.to_vec(),
        expected: Some(CasVersion(999)),
        value: Bytes::from_static(b"new_state"),
    };

    let hash = test_hash(0xEF);
    let hash_ref = &hash;
    let outcome = tables
        .with_writes_and_cas(cas, |w| {
            Box::pin(async move {
                tables_ref.blocks().stage_hash_index(w, hash_ref, 1);
                Ok(())
            })
        })
        .await
        .expect("call ok, outcome is Conflict");
    assert!(matches!(outcome, CasOutcome::Conflict { .. }));

    let stored = meta
        .get(
            BlockTables::<ObservedMetaStore>::BLOCK_HASH_TO_NUMBER_INDEX_TABLE,
            hash.as_slice(),
        )
        .await
        .unwrap();
    assert_eq!(
        stored.as_deref(),
        Some(&1u64.to_be_bytes()[..]),
        "data writes land before the publication CAS"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cas_conflict_keeps_populated_cache_entries() {
    let meta = ObservedMetaStore::new();
    let blob = InMemoryBlobStore::default();
    let tables = Tables::with_cache_config(meta, blob, cache());
    let tables_ref = &tables;
    let header = monad_chain_data::EvmBlockHeader {
        number: 1,
        ..Default::default()
    };
    let header_ref = &header;
    let record = block_record(1);
    let record_ref = &record;

    let cas = PublicationCasParams {
        table: PublicationTables::<ObservedMetaStore>::PUBLICATION_STATE_TABLE,
        key: PublicationTables::<ObservedMetaStore>::PUBLICATION_STATE_KEY.to_vec(),
        expected: Some(CasVersion(999)),
        value: Bytes::from_static(b"v"),
    };
    let outcome = tables
        .with_writes_and_cas(cas, |w| {
            Box::pin(async move {
                tables_ref.blocks().stage_metadata(
                    w,
                    1,
                    record_ref,
                    header_ref,
                    Bytes::new(),
                    Bytes::new(),
                    Bytes::new(),
                );
                Ok(())
            })
        })
        .await
        .expect("conflict still returns Ok(outcome)");
    assert!(matches!(outcome, CasOutcome::Conflict { .. }));

    let read = tables.blocks().load_header(1).await.unwrap();
    assert_eq!(
        read.as_ref().map(|h| h.number),
        Some(1),
        "cache remains valid because writes landed before the CAS conflict"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn with_writes_and_cas_flushes_blobs_before_cas_conflict() {
    let meta = ObservedMetaStore::new();
    let blob = InMemoryBlobStore::default();
    let blob_for_check = blob.clone();
    let tables = Tables::with_cache_config(meta, blob, cache());
    let tables_ref = &tables;

    let cas = PublicationCasParams {
        table: PublicationTables::<ObservedMetaStore>::PUBLICATION_STATE_TABLE,
        key: PublicationTables::<ObservedMetaStore>::PUBLICATION_STATE_KEY.to_vec(),
        expected: Some(CasVersion(999)),
        value: Bytes::from_static(b"v"),
    };
    let outcome = tables
        .with_writes_and_cas(cas, |w| {
            Box::pin(async move {
                tables_ref
                    .family(Family::Log)
                    .stage_block_blob(w, 1, b"payload".to_vec());
                Ok(())
            })
        })
        .await
        .expect("conflict still returns Ok(outcome)");
    assert!(matches!(outcome, CasOutcome::Conflict { .. }));

    let ids = Family::Log.table_ids();
    let stored = blob_for_check
        .blob_snapshot()
        .get(&(ids.block_blob, 1u64.to_be_bytes().to_vec()))
        .cloned();
    assert_eq!(
        stored.as_deref(),
        Some(&b"payload"[..]),
        "blob writes land before the publication CAS"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn panic_in_closure_drops_pending() {
    let meta = ObservedMetaStore::new();
    let blob = InMemoryBlobStore::default();
    let tables = Tables::with_cache_config(meta.clone(), blob, cache());
    let tables_ref = &tables;
    let hash = test_hash(0x11);
    let hash_ref = &hash;

    let panicked = std::panic::AssertUnwindSafe(async {
        tables
            .with_writes(|w| {
                Box::pin(async move {
                    tables_ref.blocks().stage_hash_index(w, hash_ref, 1);
                    panic!("intentional panic");
                    #[allow(unreachable_code)]
                    Ok(())
                })
            })
            .await
    });

    let r = futures::FutureExt::catch_unwind(panicked).await;
    assert!(r.is_err(), "panic must propagate");
    assert!(
        meta.get(
            BlockTables::<ObservedMetaStore>::BLOCK_HASH_TO_NUMBER_INDEX_TABLE,
            hash.as_slice(),
        )
        .await
        .unwrap()
        .is_none(),
        "pending writes dropped on panic"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn panic_in_closure_evicts_populated_cache_entries() {
    let meta = ObservedMetaStore::new();
    let blob = InMemoryBlobStore::default();
    let tables = Tables::with_cache_config(meta, blob, cache());
    let tables_ref = &tables;
    let header = monad_chain_data::EvmBlockHeader {
        number: 1,
        ..Default::default()
    };
    let header_ref = &header;
    let record = block_record(1);
    let record_ref = &record;

    let panicked = std::panic::AssertUnwindSafe(async {
        tables
            .with_writes(|w| {
                Box::pin(async move {
                    tables_ref.blocks().stage_metadata(
                        w,
                        1,
                        record_ref,
                        header_ref,
                        Bytes::new(),
                        Bytes::new(),
                        Bytes::new(),
                    );
                    panic!("intentional panic");
                    #[allow(unreachable_code)]
                    Ok(())
                })
            })
            .await
    });

    let r = futures::FutureExt::catch_unwind(panicked).await;
    assert!(r.is_err(), "panic must propagate");

    // Reads against the cached accessor must miss after the panic:
    // backend never received the write and Drop must have evicted the
    // populated cache entry.
    let read = tables.blocks().load_header(1).await.unwrap();
    assert!(
        read.is_none(),
        "cache must be evicted when WriteSession drops during panic unwind"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn parallel_meta_and_blob_flush() {
    let delay = Duration::from_millis(50);
    let meta = ObservedMetaStore::timed(delay);
    let blob = ObservedBlobStore::timed(delay);
    let blob_for_check = blob.clone();
    let meta_for_check = meta.clone();
    let tables = Tables::with_cache_config(meta, blob, cache());
    let tables_ref = &tables;

    let header = monad_chain_data::EvmBlockHeader {
        number: 1,
        ..Default::default()
    };
    let header_ref = &header;
    let record = block_record(1);
    let record_ref = &record;
    tables
        .with_writes(|w| {
            Box::pin(async move {
                // stage_block_blob covers the BlobStore side, stage_metadata
                // covers the MetaStore side; both writes flush concurrently
                // because the framework fires apply_writes on both stores
                // before awaiting either future.
                tables_ref
                    .family(monad_chain_data::engine::family::Family::Log)
                    .stage_block_blob(w, 1, b"payload".to_vec());
                tables_ref.blocks().stage_metadata(
                    w,
                    1,
                    record_ref,
                    header_ref,
                    Bytes::new(),
                    Bytes::new(),
                    Bytes::new(),
                );
                Ok(())
            })
        })
        .await
        .expect("with_writes");

    let meta_start = meta_for_check
        .timings
        .apply_started_at
        .lock()
        .unwrap()
        .unwrap();
    let meta_end = meta_for_check
        .timings
        .apply_finished_at
        .lock()
        .unwrap()
        .unwrap();
    let blob_start = blob_for_check
        .timings
        .apply_started_at
        .lock()
        .unwrap()
        .unwrap();
    let blob_end = blob_for_check
        .timings
        .apply_finished_at
        .lock()
        .unwrap()
        .unwrap();

    let overlap_start = meta_start.max(blob_start);
    let overlap_end = meta_end.min(blob_end);
    assert!(
        overlap_end > overlap_start,
        "meta and blob apply_writes must overlap"
    );
}
