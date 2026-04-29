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
use futures::stream::{self, BoxStream, StreamExt};
use monad_types::{BlockId, Hash};
use tokio::sync::Mutex;

use crate::{
    kvstore::{KVReader, KVStore, KVStoreErased, WritePolicy},
    model::bft_paths::{self, INDEX_ENTRY_BYTES, INDEX_PAGE_SIZE},
};

#[enum_dispatch]
pub trait IndexBackend {
    async fn get_id(&self, seq_num: u64) -> Result<Option<BlockId>>;
    async fn put_id(&self, seq_num: u64, id: BlockId) -> Result<()>;

    /// Bulk write. **Backend-defined semantics:**
    /// - `KvIndexBackend`: per-key bulk put; no clobber implications since
    ///   each seq_num has its own key.
    /// - `PagedIndexBackend`: unconditional full-page write. The provided
    ///   entries define the entire content of each touched page;
    ///   previously written entries on those pages that are not in this
    ///   batch are clobbered (read back as None via the zero sentinel).
    ///   No GET, no mutex — pages run independently and the caller is
    ///   responsible for not racing with `put_id` on the same page.
    async fn put_ids_batch(&self, entries: &[(u64, BlockId)]) -> Result<()>;

    /// Stream every present (seq_num, id) entry in `[min..=max_inclusive]`,
    /// in ascending seq order. Missing slots are skipped — callers wanting
    /// gap detection should compare expected vs yielded counts.
    fn iter_range(
        &self,
        min: u64,
        max_inclusive: u64,
    ) -> BoxStream<'_, Result<(u64, BlockId)>>;
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
        let id = Self::decode_id(&key, &value)?;
        // Symmetric with PagedIndexBackend's zero sentinel: an all-zero
        // BlockId is impossible for a real header hash, so treat it as
        // unindexed. Keeps the trait's get_id semantics consistent
        // across impls.
        if id.0 .0 == [0u8; 32] {
            return Ok(None);
        }
        Ok(Some(id))
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

    fn iter_range(
        &self,
        min: u64,
        max_inclusive: u64,
    ) -> BoxStream<'_, Result<(u64, BlockId)>> {
        let this = self.clone();
        // One key per seq_num: query each, skip None, propagate errors.
        let s = stream::iter(min..=max_inclusive).then(move |seq| {
            let this = this.clone();
            async move { this.get_id(seq).await.map(|opt| (seq, opt)) }
        });
        s.filter_map(|res| async move {
            match res {
                Ok((seq, Some(id))) => Some(Ok((seq, id))),
                Ok((_, None)) => None,
                Err(e) => Some(Err(e)),
            }
        })
        .boxed()
    }
}

/// Paged index backend: each page covers `INDEX_PAGE_SIZE` consecutive
/// seq_nums and is stored as one contiguous, fixed-stride blob keyed by
/// `bft_paths::index_page_path`. Random reads slice by byte-range offset.
///
/// Two write paths with **different** semantics:
/// - `put_id` is read-modify-write — preserves untouched slots on the
///   page. Serialized via the internal `write_lock` so concurrent
///   `put_id`s don't drop each other's edits. Used by the live archiver
///   for per-block updates to the active tip page.
/// - `put_ids_batch` is unconditional — the provided entries define the
///   full content of each touched page, non-provided slots become zero.
///   No GET, no mutex; pages run independently for caller-side
///   parallelism. Used by the upload tool, which knows the full page
///   state from the local index.
///
/// Mixing the two on the same `PagedIndexBackend` instance is safe only
/// if the caller serializes externally; the migration plan keeps them
/// in different processes against different page ranges.
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
        // Group by page; each page is built from scratch (zero-init,
        // splice in the entries' offsets) and PUT unconditionally. No
        // GET on the existing page, no shared mutex. Caller owns the
        // assertion that these entries are the intended full state of
        // each touched page.
        let mut by_page: BTreeMap<u64, Vec<(usize, BlockId)>> = BTreeMap::new();
        for (seq, id) in entries {
            let page = bft_paths::index_page_for(*seq);
            let offset = bft_paths::index_page_offset(*seq);
            by_page.entry(page).or_default().push((offset, *id));
        }
        for (page, edits) in by_page {
            let path = bft_paths::index_page_path_for_page(page);
            // Size to the largest offset; trailing zeros at lower offsets
            // are valid and read back as None via the zero sentinel.
            let max_offset = edits.iter().map(|(o, _)| *o).max().unwrap_or(0);
            let mut buf = vec![0u8; max_offset + INDEX_ENTRY_BYTES];
            for (offset, id) in &edits {
                buf[*offset..*offset + INDEX_ENTRY_BYTES].copy_from_slice(&id.0 .0);
            }
            self.kv
                .put(path, buf, WritePolicy::AllowOverwrite)
                .await?;
        }
        Ok(())
    }

    fn iter_range(
        &self,
        min: u64,
        max_inclusive: u64,
    ) -> BoxStream<'_, Result<(u64, BlockId)>> {
        // Order-preserving page fetches with limited in-flight overlap so
        // S3 GET latency overlaps across pages. `buffered` (vs
        // `buffer_unordered`) keeps the output sorted by page → seq_num
        // ascending, which the reconciliation pass relies on.
        const PAGE_FETCH_CONCURRENCY: usize = 32;
        let this = self.clone();
        let first_page = bft_paths::index_page_for(min);
        let last_page = bft_paths::index_page_for(max_inclusive);
        let pages = stream::iter(first_page..=last_page);
        pages
            .map(move |page| {
                let this = this.clone();
                let path = bft_paths::index_page_path_for_page(page);
                let page_min = page * INDEX_PAGE_SIZE;
                let slot_lo = min.saturating_sub(page_min) as usize;
                let slot_hi = (max_inclusive - page_min).min(INDEX_PAGE_SIZE - 1) as usize;
                async move {
                    let bytes = match this.kv.get(&path).await? {
                        Some(b) => b,
                        None => return Ok::<Vec<(u64, BlockId)>, eyre::Report>(Vec::new()),
                    };
                    let mut out = Vec::new();
                    for slot in slot_lo..=slot_hi {
                        let offset = slot * INDEX_ENTRY_BYTES;
                        if bytes.len() < offset + INDEX_ENTRY_BYTES {
                            break;
                        }
                        let mut arr = [0u8; 32];
                        arr.copy_from_slice(&bytes[offset..offset + INDEX_ENTRY_BYTES]);
                        if arr == [0u8; 32] {
                            continue;
                        }
                        out.push((page_min + slot as u64, BlockId(Hash(arr))));
                    }
                    Ok(out)
                }
            })
            .buffered(PAGE_FETCH_CONCURRENCY)
            .flat_map(|res| match res {
                Ok(entries) => stream::iter(entries.into_iter().map(Ok)).boxed(),
                Err(e) => stream::iter(std::iter::once(Err(e))).boxed(),
            })
            .boxed()
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
    async fn kv_get_id_treats_all_zero_as_unindexed() {
        // Symmetric with the paged backend's zero sentinel.
        let b = fresh_kv_backend();
        b.put_id(11, BlockId(Hash([0u8; 32]))).await.unwrap();
        assert!(b.get_id(11).await.unwrap().is_none());
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
    async fn paged_batch_writes_all_entries_one_put_per_page() {
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
    async fn paged_batch_clobbers_prior_entries_on_same_page() {
        // Documents the unconditional-write contract: put_ids_batch
        // treats its entries as the full state of each touched page.
        // Slots not in the batch are zeroed (read back as None).
        let b = fresh_paged_backend();
        b.put_id(1, block_id(0x01)).await.unwrap();
        assert_eq!(b.get_id(1).await.unwrap(), Some(block_id(0x01)));
        b.put_ids_batch(&[(5, block_id(0x05))]).await.unwrap();
        assert_eq!(b.get_id(5).await.unwrap(), Some(block_id(0x05)));
        assert!(
            b.get_id(1).await.unwrap().is_none(),
            "slot 1 should be clobbered by the unconditional batch write"
        );
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

    async fn collect_iter(
        s: futures::stream::BoxStream<'_, Result<(u64, BlockId)>>,
    ) -> Vec<(u64, BlockId)> {
        s.collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect()
    }

    #[tokio::test]
    async fn kv_iter_range_yields_present_entries_in_order() {
        let b = fresh_kv_backend();
        b.put_id(1, block_id(0x01)).await.unwrap();
        b.put_id(3, block_id(0x03)).await.unwrap();
        b.put_id(5, block_id(0x05)).await.unwrap();
        let got = collect_iter(b.iter_range(0, 10)).await;
        assert_eq!(
            got,
            vec![(1, block_id(0x01)), (3, block_id(0x03)), (5, block_id(0x05))]
        );
    }

    #[tokio::test]
    async fn paged_iter_range_walks_pages_and_skips_gaps() {
        let b = fresh_paged_backend();
        // Three entries split across two pages, with intra-page gaps.
        b.put_id(0, block_id(0x01)).await.unwrap();
        b.put_id(2, block_id(0x02)).await.unwrap();
        b.put_id(1_000, block_id(0x03)).await.unwrap();
        b.put_id(1_500, block_id(0x04)).await.unwrap();
        let got = collect_iter(b.iter_range(0, 1_999)).await;
        assert_eq!(
            got,
            vec![
                (0, block_id(0x01)),
                (2, block_id(0x02)),
                (1_000, block_id(0x03)),
                (1_500, block_id(0x04)),
            ]
        );
    }

    #[tokio::test]
    async fn paged_iter_range_respects_inclusive_bounds() {
        let b = fresh_paged_backend();
        for seq in [3, 5, 7] {
            b.put_id(seq, block_id(seq as u8)).await.unwrap();
        }
        let got = collect_iter(b.iter_range(4, 6)).await;
        assert_eq!(got, vec![(5, block_id(5))]);
    }

    #[tokio::test]
    async fn iter_range_on_missing_pages_yields_empty() {
        let b = fresh_paged_backend();
        let got = collect_iter(b.iter_range(0, 4_999)).await;
        assert!(got.is_empty());
    }
}
