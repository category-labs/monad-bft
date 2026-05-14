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

use bytes::Bytes;
use monad_chain_data::{
    store::{
        BlobStore, BlobTableId, BlobWriteBatch, CasOutcome, CasVersion, MetaStore, MetaStoreCas,
        MetaWriteBatch, ScannableTableId, TableId,
    },
    InMemoryBlobStore, InMemoryMetaStore,
};

const T_KV: TableId = TableId::new("batch_api_kv");
const T_SCAN: ScannableTableId = ScannableTableId::new("batch_api_scan");
const T_CAS: TableId = TableId::new("batch_api_cas");
const T_BLOB: BlobTableId = BlobTableId::new("batch_api_blob");

#[tokio::test(flavor = "current_thread")]
async fn meta_batch_commits_kv_and_scan_atomically_visible() {
    let meta = InMemoryMetaStore::default();
    let mut batch = meta.begin_batch();
    batch.put(T_KV, b"k", Bytes::from_static(b"v"));
    batch.scan_put(T_SCAN, b"part", b"clu", Bytes::from_static(b"sv"));
    batch.commit().await.expect("commit");

    assert_eq!(meta.get(T_KV, b"k").await.unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(
        meta.scan_get(T_SCAN, b"part", b"clu").await.unwrap().as_deref(),
        Some(&b"sv"[..])
    );
}

#[tokio::test(flavor = "current_thread")]
async fn meta_batch_dropped_without_commit_is_no_op() {
    let meta = InMemoryMetaStore::default();
    {
        let mut batch = meta.begin_batch();
        batch.put(T_KV, b"k", Bytes::from_static(b"v"));
        batch.scan_put(T_SCAN, b"p", b"c", Bytes::from_static(b"v"));
    }
    assert!(meta.get(T_KV, b"k").await.unwrap().is_none());
    assert!(meta.scan_get(T_SCAN, b"p", b"c").await.unwrap().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn meta_commit_with_cas_conflict_drops_all_buffered_writes() {
    let meta = InMemoryMetaStore::default();
    // Plant a CAS row so an `expected = None` commit conflicts.
    meta.cas_put(T_CAS, b"head", None, Bytes::from_static(b"v0"))
        .await
        .expect("seed cas");

    let mut batch = meta.begin_batch();
    batch.put(T_KV, b"orphan", Bytes::from_static(b"x"));
    batch.scan_put(T_SCAN, b"p", b"c", Bytes::from_static(b"x"));
    let outcome = batch
        .commit_with_cas(T_CAS, b"head", None, Bytes::from_static(b"v1"))
        .await
        .expect("commit_with_cas");
    assert!(matches!(outcome, CasOutcome::Conflict { .. }));

    assert!(meta.get(T_KV, b"orphan").await.unwrap().is_none());
    assert!(meta.scan_get(T_SCAN, b"p", b"c").await.unwrap().is_none());
    let (version, value) = meta.cas_get(T_CAS, b"head").await.unwrap().expect("seeded");
    assert_eq!(version, CasVersion(1));
    assert_eq!(value.as_ref(), b"v0");
}

#[tokio::test(flavor = "current_thread")]
async fn meta_commit_with_cas_applied_persists_writes_and_advances_version() {
    let meta = InMemoryMetaStore::default();
    let mut batch = meta.begin_batch();
    batch.put(T_KV, b"k", Bytes::from_static(b"v"));
    let outcome = batch
        .commit_with_cas(T_CAS, b"head", None, Bytes::from_static(b"v0"))
        .await
        .expect("commit_with_cas");
    assert_eq!(
        outcome,
        CasOutcome::Applied {
            new_version: CasVersion(1),
        }
    );
    assert_eq!(meta.get(T_KV, b"k").await.unwrap().as_deref(), Some(&b"v"[..]));
}

#[tokio::test(flavor = "current_thread")]
async fn blob_batch_commits_independently_of_meta() {
    let blob = InMemoryBlobStore::default();
    let mut b = blob.begin_batch();
    b.put_blob(T_BLOB, b"k1", Bytes::from_static(b"v1"));
    b.put_blob(T_BLOB, b"k2", Bytes::from_static(b"v2"));
    b.commit().await.expect("commit");
    assert_eq!(blob.get_blob(T_BLOB, b"k1").await.unwrap().as_deref(), Some(&b"v1"[..]));
    assert_eq!(blob.get_blob(T_BLOB, b"k2").await.unwrap().as_deref(), Some(&b"v2"[..]));
}

#[tokio::test(flavor = "current_thread")]
async fn blob_batch_dropped_without_commit_is_no_op() {
    let blob = InMemoryBlobStore::default();
    {
        let mut b = blob.begin_batch();
        b.put_blob(T_BLOB, b"k", Bytes::from_static(b"v"));
    }
    assert!(blob.get_blob(T_BLOB, b"k").await.unwrap().is_none());
}
