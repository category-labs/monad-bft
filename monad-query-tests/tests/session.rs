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
use std::time::Duration;

use bytes::Bytes;
use monad_query_tests::prelude::*;

use crate::{
    observed_store::{ObservedBlobStore, ObservedMetaStore},
    stage_block_header, test_cache_config,
};

fn test_hash(byte: u8) -> Hash32 {
    Hash32::from([byte; 32])
}

fn session_tables<B: BlobStore>(meta: ObservedMetaStore, blob: B) -> Tables<ObservedMetaStore, B> {
    Tables::with_all_configs(
        meta,
        blob,
        test_cache_config(),
        DictConfig::default(),
        QueryRuntimeConfig::default(),
    )
}

// Hash-index entries act as a write-reached-backend probe, readable via plain `MetaStore::get`.
async fn stored_hash_index(meta: &ObservedMetaStore, hash: &Hash32) -> Option<Bytes> {
    meta.get(
        BlockTables::<ObservedMetaStore>::BLOCK_HASH_TO_NUMBER_INDEX_TABLE,
        hash.as_slice(),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn closure_error_does_not_flush() {
    let meta = ObservedMetaStore::new();
    let tables = session_tables(meta.clone(), InMemoryBlobStore::default());
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
    assert!(stored_hash_index(&meta, &hash).await.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn closure_error_evicts_populated_cache_entries() {
    let tables = session_tables(ObservedMetaStore::new(), InMemoryBlobStore::default());
    let tables_ref = &tables;

    let _ = tables
        .with_writes(|w| {
            Box::pin(async move {
                stage_block_header(tables_ref, w, 1);
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
    let tables = session_tables(meta, InMemoryBlobStore::default());
    let tables_ref = &tables;

    let result = tables
        .with_writes(|w| {
            Box::pin(async move {
                stage_block_header(tables_ref, w, 1);
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
    let tables = session_tables(meta.clone(), InMemoryBlobStore::default());
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

    let stored = stored_hash_index(&meta, &hash).await;
    assert_eq!(stored.as_deref(), Some(&42u64.to_be_bytes()[..]));
}

#[tokio::test(flavor = "current_thread")]
async fn panic_in_closure_drops_pending() {
    let meta = ObservedMetaStore::new();
    let tables = session_tables(meta.clone(), InMemoryBlobStore::default());
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
        stored_hash_index(&meta, &hash).await.is_none(),
        "pending writes dropped on panic"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn panic_in_closure_evicts_populated_cache_entries() {
    let tables = session_tables(ObservedMetaStore::new(), InMemoryBlobStore::default());
    let tables_ref = &tables;

    let panicked = std::panic::AssertUnwindSafe(async {
        tables
            .with_writes(|w| {
                Box::pin(async move {
                    stage_block_header(tables_ref, w, 1);
                    panic!("intentional panic");
                    #[allow(unreachable_code)]
                    Ok(())
                })
            })
            .await
    });

    let r = futures::FutureExt::catch_unwind(panicked).await;
    assert!(r.is_err(), "panic must propagate");

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
    let tables = session_tables(meta.clone(), blob.clone());
    let tables_ref = &tables;

    tables
        .with_writes(|w| {
            Box::pin(async move {
                // Flush fires apply_writes on both stores before awaiting either.
                tables_ref.stage_block_blob(w, 1, b"payload".to_vec());
                stage_block_header(tables_ref, w, 1);
                Ok(())
            })
        })
        .await
        .expect("with_writes");

    let (meta_start, meta_end) = meta.timings.window();
    let (blob_start, blob_end) = blob.timings.window();

    let overlap_start = meta_start.max(blob_start);
    let overlap_end = meta_end.min(blob_end);
    assert!(
        overlap_end > overlap_start,
        "meta and blob apply_writes must overlap"
    );
}
