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

//! Page-aware abstraction over the seq_num → BlockId index.
//!
//! Local migration writes one key per seq_num via `KvIndexBackend`. The S3
//! upload phase and the live archiver use a paged backend (added in a later
//! commit) that packs `INDEX_PAGE_SIZE` entries into a single contiguous
//! 32-byte-stride blob per page. Both impls speak the same trait so the
//! indexer doesn't have to care which one is in use.

use std::{collections::BTreeMap, sync::Arc};

use enum_dispatch::enum_dispatch;
use eyre::{eyre, Context, Result};
use monad_types::{BlockId, Hash};
use tokio::sync::Mutex;

use crate::{
    kvstore::{KVReader, KVStore, KVStoreErased, WritePolicy},
    model::bft_paths::{self, INDEX_ENTRY_BYTES},
};

#[enum_dispatch]
pub trait IndexBackend {
    async fn get_id(&self, seq_num: u64) -> Result<Option<BlockId>>;
    async fn put_id(&self, seq_num: u64, id: BlockId) -> Result<()>;
    async fn put_ids_batch(&self, entries: &[(u64, BlockId)]) -> Result<()>;
}

#[enum_dispatch(IndexBackend)]
#[derive(Clone)]
pub enum IndexBackendErased {
    KvIndexBackend,
    PagedIndexBackend,
}

#[derive(Clone)]
pub struct KvIndexBackend {
    pub kv: KVStoreErased,
}

impl KvIndexBackend {
    pub fn new(kv: KVStoreErased) -> Self {
        Self { kv }
    }

    fn encode_id(id: BlockId) -> Vec<u8> {
        hex::encode(id.0).into_bytes()
    }

    fn decode_id(key: &str, value: &[u8]) -> Result<BlockId> {
        let bytes = hex::decode(value)
            .wrap_err_with(|| format!("Failed to decode hex block id at {key}"))?;
        let len = bytes.len();
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| eyre!("Invalid block id length at {key}: expected 32, got {len}"))?;
        Ok(BlockId(Hash(arr)))
    }
}

impl IndexBackend for KvIndexBackend {
    async fn get_id(&self, seq_num: u64) -> Result<Option<BlockId>> {
        let key = bft_paths::index_per_key_path(seq_num);
        let Some(value) = self.kv.get(&key).await? else {
            return Ok(None);
        };
        Ok(Some(Self::decode_id(&key, &value)?))
    }

    async fn put_id(&self, seq_num: u64, id: BlockId) -> Result<()> {
        let key = bft_paths::index_per_key_path(seq_num);
        self.kv
            .put(key, Self::encode_id(id), WritePolicy::AllowOverwrite)
            .await?;
        Ok(())
    }

    async fn put_ids_batch(&self, entries: &[(u64, BlockId)]) -> Result<()> {
        let kvs = entries
            .iter()
            .map(|(seq, id)| (bft_paths::index_per_key_path(*seq), Self::encode_id(*id)));
        self.kv
            .bulk_put(kvs, WritePolicy::AllowOverwrite)
            .await?;
        Ok(())
    }
}

/// Paged index backend: each page covers `INDEX_PAGE_SIZE` consecutive
/// seq_nums and is stored as one contiguous, fixed-stride blob keyed by
/// `bft_paths::index_page_path`. Random reads slice by byte-range offset;
/// writes are read-modify-write of the page blob and serialized through a
/// single mutex so concurrent writers don't drop each other's edits.
///
/// A page may be shorter than the full `INDEX_PAGE_SIZE * INDEX_ENTRY_BYTES`
/// length while it is still being filled — the live archiver rewrites the
/// current page in full on every committed block, so partial pages only
/// occur at the live tip. `get_id` returns `None` when the page is missing,
/// when the requested offset lies past the page's current length, or when
/// the slot is all-zero (the unindexed sentinel — collision with a real
/// BlockId is cryptographically impossible). The all-zero check matters
/// when a partial migration or canary leaves intra-page gaps: the page
/// extends past the gap because a higher-offset slot was written, and the
/// gap reads back as zero bytes that must not be confused with a valid id.
#[derive(Clone)]
pub struct PagedIndexBackend {
    pub kv: KVStoreErased,
    write_lock: Arc<Mutex<()>>,
}

impl PagedIndexBackend {
    pub fn new(kv: KVStoreErased) -> Self {
        Self {
            kv,
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    fn write_entry_into_page(buf: &mut Vec<u8>, offset: usize, id: BlockId) {
        if buf.len() < offset + INDEX_ENTRY_BYTES {
            buf.resize(offset + INDEX_ENTRY_BYTES, 0);
        }
        buf[offset..offset + INDEX_ENTRY_BYTES].copy_from_slice(&id.0 .0);
    }

    async fn read_page(&self, page_path: &str) -> Result<Vec<u8>> {
        Ok(self
            .kv
            .get(page_path)
            .await?
            .map(|b| b.to_vec())
            .unwrap_or_default())
    }
}

impl IndexBackend for PagedIndexBackend {
    async fn get_id(&self, seq_num: u64) -> Result<Option<BlockId>> {
        let page_path = bft_paths::index_page_path(seq_num);
        let offset = bft_paths::index_page_offset(seq_num);
        let Some(bytes) = self.kv.get(&page_path).await? else {
            return Ok(None);
        };
        if bytes.len() < offset + INDEX_ENTRY_BYTES {
            return Ok(None);
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes[offset..offset + INDEX_ENTRY_BYTES]);
        // All-zero is the "not yet indexed" sentinel for intra-page gaps
        // (e.g. partial migration or canary with non-contiguous writes).
        // Collision with a real BlockId is cryptographically impossible.
        if arr == [0u8; 32] {
            return Ok(None);
        }
        Ok(Some(BlockId(Hash(arr))))
    }

    async fn put_id(&self, seq_num: u64, id: BlockId) -> Result<()> {
        let page_path = bft_paths::index_page_path(seq_num);
        let offset = bft_paths::index_page_offset(seq_num);
        let _guard = self.write_lock.lock().await;
        let mut buf = self.read_page(&page_path).await?;
        Self::write_entry_into_page(&mut buf, offset, id);
        self.kv
            .put(page_path, buf, WritePolicy::AllowOverwrite)
            .await?;
        Ok(())
    }

    async fn put_ids_batch(&self, entries: &[(u64, BlockId)]) -> Result<()> {
        let mut by_page: BTreeMap<u64, Vec<(usize, BlockId)>> = BTreeMap::new();
        for (seq, id) in entries {
            let page = bft_paths::index_page_for(*seq);
            let offset = bft_paths::index_page_offset(*seq);
            by_page.entry(page).or_default().push((offset, *id));
        }
        let _guard = self.write_lock.lock().await;
        for (page, edits) in by_page {
            let path = bft_paths::index_page_path_for_page(page);
            let mut buf = self.read_page(&path).await?;
            for (offset, id) in &edits {
                Self::write_entry_into_page(&mut buf, *offset, *id);
            }
            self.kv
                .put(path, buf, WritePolicy::AllowOverwrite)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kvstore::memory::MemoryStorage;

    fn block_id(byte: u8) -> BlockId {
        BlockId(Hash([byte; 32]))
    }

    fn fresh_kv_backend() -> KvIndexBackend {
        let kv: KVStoreErased = MemoryStorage::new("idx").into();
        KvIndexBackend::new(kv)
    }

    #[tokio::test]
    async fn get_returns_none_for_missing() {
        let b = fresh_kv_backend();
        assert!(b.get_id(0).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn put_then_get_round_trip() {
        let b = fresh_kv_backend();
        let id = block_id(0xAB);
        b.put_id(42, id).await.unwrap();
        assert_eq!(b.get_id(42).await.unwrap(), Some(id));
    }

    #[tokio::test]
    async fn put_ids_batch_writes_all_entries() {
        let b = fresh_kv_backend();
        let entries = vec![(1, block_id(0x01)), (2, block_id(0x02)), (3, block_id(0x03))];
        b.put_ids_batch(&entries).await.unwrap();
        for (seq, expected) in entries {
            assert_eq!(b.get_id(seq).await.unwrap(), Some(expected));
        }
    }

    #[tokio::test]
    async fn put_id_overwrites() {
        let b = fresh_kv_backend();
        b.put_id(7, block_id(0x01)).await.unwrap();
        b.put_id(7, block_id(0x02)).await.unwrap();
        assert_eq!(b.get_id(7).await.unwrap(), Some(block_id(0x02)));
    }

    fn fresh_paged_backend() -> PagedIndexBackend {
        let kv: KVStoreErased = MemoryStorage::new("paged").into();
        PagedIndexBackend::new(kv)
    }

    #[tokio::test]
    async fn paged_get_returns_none_for_missing_page() {
        let b = fresh_paged_backend();
        assert!(b.get_id(0).await.unwrap().is_none());
        assert!(b.get_id(999).await.unwrap().is_none());
        assert!(b.get_id(1_000).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn paged_put_then_get_round_trip() {
        let b = fresh_paged_backend();
        let id = block_id(0xCD);
        b.put_id(500, id).await.unwrap();
        assert_eq!(b.get_id(500).await.unwrap(), Some(id));
    }

    #[tokio::test]
    async fn paged_put_writes_within_correct_page() {
        let b = fresh_paged_backend();
        // Two entries in different pages: page 0 (seq 1) and page 1 (seq 1000).
        b.put_id(1, block_id(0x01)).await.unwrap();
        b.put_id(1_000, block_id(0x02)).await.unwrap();
        assert_eq!(b.get_id(1).await.unwrap(), Some(block_id(0x01)));
        assert_eq!(b.get_id(1_000).await.unwrap(), Some(block_id(0x02)));
        // Untouched offsets within page 0 read as None because the page only
        // extends through seq 1's slot (not seq 999).
        assert!(b.get_id(999).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn paged_batch_writes_all_entries_with_one_rmw_per_page() {
        let b = fresh_paged_backend();
        let entries = vec![
            (0, block_id(0x10)),
            (5, block_id(0x15)),
            (1_000, block_id(0x20)),
            (1_999, block_id(0x21)),
        ];
        b.put_ids_batch(&entries).await.unwrap();
        for (seq, expected) in entries {
            assert_eq!(b.get_id(seq).await.unwrap(), Some(expected));
        }
    }

    #[tokio::test]
    async fn paged_overwrite_replaces_slot() {
        let b = fresh_paged_backend();
        b.put_id(42, block_id(0x01)).await.unwrap();
        b.put_id(42, block_id(0x02)).await.unwrap();
        assert_eq!(b.get_id(42).await.unwrap(), Some(block_id(0x02)));
    }

    #[tokio::test]
    async fn paged_get_returns_none_past_partial_page_high_water() {
        let b = fresh_paged_backend();
        b.put_id(3, block_id(0xFF)).await.unwrap();
        // Slot 7 is past the high water of 3, so the page is shorter than
        // (7+1)*32 bytes — read returns None even though page exists.
        assert!(b.get_id(7).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn paged_get_returns_none_for_intra_page_zero_gap() {
        let b = fresh_paged_backend();
        // Write slot 0 and slot 2 — slot 1 is now within the page's length
        // but still all-zero. Without the sentinel check we'd return a
        // valid-looking BlockId(0) here.
        b.put_id(0, block_id(0x10)).await.unwrap();
        b.put_id(2, block_id(0x20)).await.unwrap();
        assert_eq!(b.get_id(0).await.unwrap(), Some(block_id(0x10)));
        assert!(b.get_id(1).await.unwrap().is_none());
        assert_eq!(b.get_id(2).await.unwrap(), Some(block_id(0x20)));
    }
}
