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

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::{
    error::{MonadChainDataError, Result},
    primitives::state::PrimaryId,
    store::{
        blob::BlobStore, CachedKvTable, CachedScannableTable, MetaStore, MetaWriteOp,
        ScannableTableId, WriteSession,
    },
};

pub const LOCAL_ID_BITS: u32 = PrimaryId::LOCAL_ID_BITS;
pub const STREAM_PAGE_LOCAL_ID_SPAN: u32 = 64 * 1024;
/// Number of [`STREAM_PAGE_LOCAL_ID_SPAN`]-wide pages in one shard's local-id
/// space (`2^LOCAL_ID_BITS / STREAM_PAGE_LOCAL_ID_SPAN`). With `LOCAL_ID_BITS
/// = 24` this is 256 pages of 64K, and it bounds the entries in one stream's
/// page-count manifest.
pub const STREAM_PAGES_PER_SHARD: u32 = (1u32 << LOCAL_ID_BITS) / STREAM_PAGE_LOCAL_ID_SPAN;
const BITMAP_BLOB_VERSION: u8 = 2;
const BITMAP_PAGE_COUNTS_VERSION: u8 = 1;
const BITMAP_BLOB_HEADER_LEN: usize = 1 + 4 * 3;
const BITMAP_PAGE_ARTIFACT_VERSION: u8 = 3;
const BITMAP_PAGE_ARTIFACT_HEADER_LEN: usize = 1 + 4 * 3;

#[derive(Debug, Clone)]
pub struct BitmapBlob {
    pub min_local: u32,
    pub max_local: u32,
    pub count: u32,
    pub bitmap: RoaringBitmap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmapFragmentWrite {
    pub stream_id: String,
    pub page_start_local: u32,
    pub bitmap_blob: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmapPageArtifact {
    pub meta: BitmapPageMeta,
    pub bitmap_blob: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitmapPageMeta {
    pub min_local: u32,
    pub max_local: u32,
    pub count: u32,
}

pub struct BitmapTables<M: MetaStore> {
    fragments: CachedScannableTable<M>,
    page_blobs: CachedKvTable<M>,
    page_counts: CachedKvTable<M>,
    open_streams: CachedScannableTable<M>,
}

impl<M: MetaStore> BitmapTables<M> {
    pub fn new(
        fragments: CachedScannableTable<M>,
        page_blobs: CachedKvTable<M>,
        page_counts: CachedKvTable<M>,
        open_streams: CachedScannableTable<M>,
    ) -> Self {
        Self {
            fragments,
            page_blobs,
            page_counts,
            open_streams,
        }
    }

    pub fn fragments_table(&self) -> ScannableTableId {
        self.fragments.table_id()
    }

    pub(crate) fn fragments_cache(&self) -> &CachedScannableTable<M> {
        &self.fragments
    }

    pub(crate) fn page_blobs_cache(&self) -> &CachedKvTable<M> {
        &self.page_blobs
    }

    pub(crate) fn page_counts_cache(&self) -> &CachedKvTable<M> {
        &self.page_counts
    }

    pub(crate) fn open_streams_cache(&self) -> &CachedScannableTable<M> {
        &self.open_streams
    }

    pub fn stage_fragments_for_global_page<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        fragments: &[BitmapFragmentWrite],
        block_number: u64,
        global_page_start: u64,
    ) -> Result<(u64, u64)> {
        let clustering = block_number_key(block_number);
        let mut ops = Vec::new();
        let mut total = 0u64;
        let mut written = 0u64;

        for fragment in fragments {
            total = total.saturating_add(1);
            let page_global_start =
                stream_page_global_start(&fragment.stream_id, fragment.page_start_local)?;
            if page_global_start != global_page_start {
                continue;
            }

            written = written.saturating_add(1);
            ops.push(MetaWriteOp::ScanPut {
                table: self.fragments.table_id(),
                partition: stream_page_key(&fragment.stream_id, fragment.page_start_local),
                clustering: clustering.to_vec(),
                value: fragment.bitmap_blob.clone(),
            });
        }

        w.extend_meta_uncached(ops);
        Ok((total, written))
    }

    /// Loads all retained fragments for one stream page.
    ///
    /// Keeps scan-keys + point gets explicit: point-table caches often make
    /// this faster than value-bearing scans, and the store API can stay simple
    /// until a backend proves it needs batched value reads.
    pub async fn load_fragments(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Vec<Bytes>> {
        let partition = stream_page_key(stream_id, page_start_local);
        let page = self
            .fragments
            .list_prefix(&partition, &[], None, usize::MAX)
            .await?;
        let mut fragments = Vec::with_capacity(page.keys.len());

        for clustering in page.keys {
            let bytes = self.fragments.get(&partition, &clustering).await?.ok_or(
                MonadChainDataError::MissingData("missing log bitmap fragment"),
            )?;
            fragments.push(bytes);
        }

        Ok(fragments)
    }

    pub(crate) async fn list_fragments_for_rebuild(
        &self,
        stream_id: &str,
        page_start_local: u32,
        published_head: u64,
    ) -> Result<BTreeMap<u64, Bytes>> {
        let partition = stream_page_key(stream_id, page_start_local);
        let page = self
            .fragments
            .list_prefix(&partition, &[], None, usize::MAX)
            .await?;
        let mut out = BTreeMap::new();
        for key in page.keys {
            let block = u64_from_key(&key)?;
            if block <= published_head {
                let bytes = self.fragments.get(&partition, &key).await?.ok_or(
                    MonadChainDataError::MissingData("missing log bitmap fragment"),
                )?;
                out.insert(block, bytes);
            }
        }
        Ok(out)
    }

    /// Loads a compacted bitmap page from its combined artifact row.
    pub async fn load_page_artifact(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Option<BitmapPageArtifact>> {
        let key = stream_page_key(stream_id, page_start_local);
        let Some(bytes) = self.page_blobs.get(&key).await? else {
            return Ok(None);
        };

        decode_bitmap_page_artifact(bytes.as_ref())?
            .ok_or(MonadChainDataError::Decode("invalid bitmap page artifact"))
            .map(Some)
    }

    pub fn stage_page_artifact<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        stream_id: &str,
        page_start_local: u32,
        artifact: &BitmapPageArtifact,
    ) {
        let key = stream_page_key(stream_id, page_start_local);
        w.put(
            &self.page_blobs,
            &key,
            encode_bitmap_page_artifact(artifact),
        );
    }

    /// Loads the sealed-shard page-count manifest for one stream, if its
    /// shard has fully sealed. The key is the `stream_id` itself, which
    /// already encodes the shard (see [`sharded_stream_id`]).
    pub async fn load_page_counts(&self, stream_id: &str) -> Result<Option<BitmapPageCounts>> {
        let Some(bytes) = self.page_counts.get(stream_id.as_bytes()).await? else {
            return Ok(None);
        };
        Ok(Some(BitmapPageCounts::decode(&bytes)?))
    }

    pub fn stage_page_counts<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        stream_id: &str,
        counts: &BitmapPageCounts,
    ) {
        w.put(&self.page_counts, stream_id.as_bytes(), counts.encode());
    }

    /// Loads the open stream inventory for one frontier page.
    pub async fn load_open_streams(&self, global_page_start: u64) -> Result<Vec<String>> {
        let partition = global_page_start.to_be_bytes();
        let page = self
            .open_streams
            .list_prefix(&partition, &[], None, usize::MAX)
            .await?;
        let mut streams = Vec::with_capacity(page.keys.len());

        for key in page.keys {
            let stream = String::from_utf8(key)
                .map_err(|_| MonadChainDataError::Decode("invalid open stream id"))?;
            streams.push(stream);
        }

        Ok(streams)
    }

    /// Records any newly touched streams in the open inventory for one page.
    ///
    /// This is intentionally append-only in the current slice so replay can
    /// never lose open-stream membership through a partial delete+rewrite.
    pub async fn record_open_streams(
        &self,
        global_page_start: u64,
        streams: &BTreeSet<String>,
    ) -> Result<()> {
        let partition = global_page_start.to_be_bytes();

        for stream_id in streams {
            self.open_streams
                .put(&partition, stream_id.as_bytes(), Bytes::new())
                .await?;
        }

        Ok(())
    }

    pub fn stage_open_streams<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        global_page_start: u64,
        streams: &BTreeSet<String>,
    ) {
        let partition = global_page_start.to_be_bytes();
        for stream_id in streams {
            w.scan_put(
                &self.open_streams,
                &partition,
                stream_id.as_bytes(),
                Bytes::new(),
            );
        }
    }
}

/// Encodes one bitmap blob into the stored fragment/page format.
pub fn encode_bitmap_blob(blob: &BitmapBlob) -> Result<Bytes> {
    let mut payload = Vec::new();
    blob.bitmap
        .serialize_into(&mut payload)
        .map_err(|e| MonadChainDataError::Backend(format!("serialize bitmap blob: {e}")))?;

    let mut out = Vec::with_capacity(BITMAP_BLOB_HEADER_LEN + payload.len());
    out.push(BITMAP_BLOB_VERSION);
    out.extend_from_slice(&blob.min_local.to_be_bytes());
    out.extend_from_slice(&blob.max_local.to_be_bytes());
    out.extend_from_slice(&blob.count.to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(Bytes::from(out))
}

/// Decodes one stored bitmap blob and validates its framing header.
pub fn decode_bitmap_blob(bytes: &[u8]) -> Result<BitmapBlob> {
    let header = bytes
        .get(..BITMAP_BLOB_HEADER_LEN)
        .ok_or(MonadChainDataError::Decode("bitmap blob too short"))?;
    if header[0] != BITMAP_BLOB_VERSION {
        return Err(MonadChainDataError::Decode(
            "unsupported bitmap blob version",
        ));
    }

    let min_local = u32::from_be_bytes(
        header[1..5]
            .try_into()
            .map_err(|_| MonadChainDataError::Decode("bitmap blob min_local"))?,
    );
    let max_local = u32::from_be_bytes(
        header[5..9]
            .try_into()
            .map_err(|_| MonadChainDataError::Decode("bitmap blob max_local"))?,
    );
    let count = u32::from_be_bytes(
        header[9..13]
            .try_into()
            .map_err(|_| MonadChainDataError::Decode("bitmap blob count"))?,
    );

    let bitmap = RoaringBitmap::deserialize_from(&bytes[BITMAP_BLOB_HEADER_LEN..])
        .map_err(|e| MonadChainDataError::Backend(format!("deserialize bitmap blob: {e}")))?;

    // The framing header duplicates the bitmap's own bounds and cardinality.
    // Query-time skip decisions trust `min_local`/`max_local` (see
    // `engine::query::bitmap::overlaps`), so a corrupted header with a
    // too-narrow range would silently drop a page from the result rather than
    // surface an error. Validate the header against the decoded payload and
    // fail loudly on any drift. We already paid for the full deserialize, so
    // recomputing the bounds here is effectively free.
    let header_matches_payload = match bitmap.min().zip(bitmap.max()) {
        Some((actual_min, actual_max)) => {
            u64::from(count) == bitmap.len() && min_local == actual_min && max_local == actual_max
        }
        None => count == 0,
    };
    if !header_matches_payload {
        return Err(MonadChainDataError::Decode(
            "bitmap blob header does not match payload",
        ));
    }

    Ok(BitmapBlob {
        min_local,
        max_local,
        count,
        bitmap,
    })
}

/// Encodes one compacted bitmap page into a single KV value. The legacy layout
/// wrote page metadata and page blob as two rows; version 3 keeps the page blob
/// format intact and prefixes the query metadata so old split rows remain
/// readable while new compactions write one row per page.
pub fn encode_bitmap_page_artifact(artifact: &BitmapPageArtifact) -> Bytes {
    let mut out = Vec::with_capacity(BITMAP_PAGE_ARTIFACT_HEADER_LEN + artifact.bitmap_blob.len());
    out.push(BITMAP_PAGE_ARTIFACT_VERSION);
    out.extend_from_slice(&artifact.meta.min_local.to_be_bytes());
    out.extend_from_slice(&artifact.meta.max_local.to_be_bytes());
    out.extend_from_slice(&artifact.meta.count.to_be_bytes());
    out.extend_from_slice(&artifact.bitmap_blob);
    Bytes::from(out)
}

pub fn decode_bitmap_page_artifact(bytes: &[u8]) -> Result<Option<BitmapPageArtifact>> {
    if bytes.first().copied() != Some(BITMAP_PAGE_ARTIFACT_VERSION) {
        return Ok(None);
    }

    let header =
        bytes
            .get(..BITMAP_PAGE_ARTIFACT_HEADER_LEN)
            .ok_or(MonadChainDataError::Decode(
                "bitmap page artifact too short",
            ))?;
    let meta = BitmapPageMeta {
        min_local: u32::from_be_bytes(
            header[1..5]
                .try_into()
                .map_err(|_| MonadChainDataError::Decode("bitmap page artifact min_local"))?,
        ),
        max_local: u32::from_be_bytes(
            header[5..9]
                .try_into()
                .map_err(|_| MonadChainDataError::Decode("bitmap page artifact max_local"))?,
        ),
        count: u32::from_be_bytes(
            header[9..13]
                .try_into()
                .map_err(|_| MonadChainDataError::Decode("bitmap page artifact count"))?,
        ),
    };

    Ok(Some(BitmapPageArtifact {
        meta,
        bitmap_blob: Bytes::copy_from_slice(&bytes[BITMAP_PAGE_ARTIFACT_HEADER_LEN..]),
    }))
}

/// Sparse per-stream roll-up of the compacted per-page `count`s for one
/// sealed shard. Built once a shard fully seals (every page in the shard is
/// compacted) and immutable thereafter, so query time can answer "is clause C
/// empty in page P?" and "which clause is most selective in page P?" without
/// fetching a single bitmap. Only non-empty pages are listed (≤
/// [`STREAM_PAGES_PER_SHARD`] entries), kept sorted by `page_start_local` so a
/// lookup is a binary search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmapPageCounts {
    /// `(page_start_local, count)` pairs for the stream's non-empty pages,
    /// sorted ascending by `page_start_local`.
    pub pages: Vec<(u32, u32)>,
}

impl BitmapPageCounts {
    /// Builds a manifest from `(page_start_local, count)` pairs, dropping
    /// zero-count pages and sorting by page so [`Self::count_for_page`] can
    /// binary-search. Pages are expected unique; on a duplicate the last write
    /// wins, mirroring the immutable-once-sealed contract.
    pub fn from_pairs(pairs: impl IntoIterator<Item = (u32, u32)>) -> Self {
        let mut pages: Vec<(u32, u32)> =
            pairs.into_iter().filter(|(_, count)| *count != 0).collect();
        pages.sort_by_key(|(page_start_local, _)| *page_start_local);
        pages.dedup_by_key(|(page_start_local, _)| *page_start_local);
        Self { pages }
    }

    /// Per-page count for `page_start_local`. `Some(0)` is never returned —
    /// zero-count pages are absent from the manifest, so a missing page means
    /// the stream contributes nothing there.
    pub fn count_for_page(&self, page_start_local: u32) -> Option<u32> {
        self.pages
            .binary_search_by_key(&page_start_local, |(page, _)| *page)
            .ok()
            .map(|idx| self.pages[idx].1)
    }

    /// Encodes the manifest with a version byte followed by length-prefixed,
    /// fixed-width `(page_start_local, count)` pairs. Mirrors the explicit
    /// big-endian framing used by [`encode_bitmap_blob`] rather than RLP so
    /// the on-disk layout stays self-describing and bounded.
    pub fn encode(&self) -> Bytes {
        let mut out = Vec::with_capacity(1 + 4 + self.pages.len() * 8);
        out.push(BITMAP_PAGE_COUNTS_VERSION);
        out.extend_from_slice(&(self.pages.len() as u32).to_be_bytes());
        for (page_start_local, count) in &self.pages {
            out.extend_from_slice(&page_start_local.to_be_bytes());
            out.extend_from_slice(&count.to_be_bytes());
        }
        Bytes::from(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let version = bytes
            .first()
            .copied()
            .ok_or(MonadChainDataError::Decode("bitmap page counts too short"))?;
        if version != BITMAP_PAGE_COUNTS_VERSION {
            return Err(MonadChainDataError::Decode(
                "unsupported bitmap page counts version",
            ));
        }
        let len_bytes = bytes
            .get(1..5)
            .ok_or(MonadChainDataError::Decode("bitmap page counts too short"))?;
        let len = u32::from_be_bytes(
            len_bytes
                .try_into()
                .map_err(|_| MonadChainDataError::Decode("bitmap page counts length"))?,
        ) as usize;
        let body = &bytes[5..];
        if body.len() != len * 8 {
            return Err(MonadChainDataError::Decode(
                "bitmap page counts length mismatch",
            ));
        }
        let mut pages = Vec::with_capacity(len);
        for chunk in body.chunks_exact(8) {
            let page_start_local = u32::from_be_bytes(
                chunk[0..4]
                    .try_into()
                    .map_err(|_| MonadChainDataError::Decode("bitmap page counts page"))?,
            );
            let count = u32::from_be_bytes(
                chunk[4..8]
                    .try_into()
                    .map_err(|_| MonadChainDataError::Decode("bitmap page counts count"))?,
            );
            pages.push((page_start_local, count));
        }
        Ok(Self { pages })
    }
}

/// Groups `(stream_id, local_id)` pairs by stream and page, builds a roaring
/// bitmap per page, and serializes each into a [`BitmapFragmentWrite`].
///
/// A single block may contribute entries to many streams (one per indexed
/// address or topic) and each stream may span multiple pages if the block's
/// local-ID range crosses a page boundary.
pub fn encode_grouped_bitmap_fragments(
    values: impl IntoIterator<Item = (String, u32)>,
) -> Result<Vec<BitmapFragmentWrite>> {
    let mut grouped = BTreeMap::<String, BTreeMap<u32, RoaringBitmap>>::new();

    for (stream_id, local_id) in values {
        let page_start_local = page_start_local(local_id);
        grouped
            .entry(stream_id)
            .or_default()
            .entry(page_start_local)
            .or_default()
            .insert(local_id);
    }

    let mut out = Vec::new();
    for (stream_id, pages) in grouped {
        for (page_start_local, bitmap) in pages {
            let Some(bitmap_blob) = compacted_bitmap_blob(bitmap, page_start_local)? else {
                continue;
            };
            out.push(BitmapFragmentWrite {
                stream_id: stream_id.clone(),
                page_start_local,
                bitmap_blob: encode_bitmap_blob(&bitmap_blob)?,
            });
        }
    }

    Ok(out)
}

/// Merges retained bitmap fragments for one page into the compacted page meta
/// and blob artifacts written once that page seals. A stream only enters the
/// open-page inventory after fragment writes for that page, so an empty merge
/// here indicates corrupted compaction state and is reported as missing data.
pub(crate) fn compact_bitmap_page<I, T>(
    page_start_local: u32,
    fragments: I,
) -> Result<(BitmapPageMeta, Bytes)>
where
    I: IntoIterator<Item = T>,
    T: AsRef<[u8]>,
{
    let mut merged = RoaringBitmap::new();
    for fragment in fragments {
        merged |= decode_bitmap_blob(fragment.as_ref())?.bitmap;
    }

    let bitmap_blob = compacted_bitmap_blob(merged, page_start_local)?.ok_or(
        MonadChainDataError::MissingData("missing sealed log bitmap page fragments"),
    )?;
    let meta = BitmapPageMeta {
        min_local: bitmap_blob.min_local,
        max_local: bitmap_blob.max_local,
        count: bitmap_blob.count,
    };

    Ok((meta, encode_bitmap_blob(&bitmap_blob)?))
}

pub fn page_start_local(local_id: u32) -> u32 {
    (local_id / STREAM_PAGE_LOCAL_ID_SPAN) * STREAM_PAGE_LOCAL_ID_SPAN
}

pub fn sharded_stream_id(index_kind: &str, value: &[u8], shard: u64) -> String {
    let shard_hex_width = ((64 - PrimaryId::LOCAL_ID_BITS) as usize).div_ceil(4);
    format!(
        "{index_kind}/{}/{:0width$x}",
        alloy_primitives::hex::encode(value),
        shard,
        width = shard_hex_width
    )
}

pub fn stream_page_key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
    let mut key = format!("{stream_id}/").into_bytes();
    key.extend_from_slice(&u64::from(page_start_local).to_be_bytes());
    key
}

pub(crate) fn global_page_start(primary_id: u64) -> u64 {
    (primary_id / u64::from(STREAM_PAGE_LOCAL_ID_SPAN)) * u64::from(STREAM_PAGE_LOCAL_ID_SPAN)
}

pub(crate) fn stream_page_global_start(stream_id: &str, page_start_local: u32) -> Result<u64> {
    let shard = parse_stream_shard(stream_id)?;
    Ok(PrimaryId::from_parts(shard, page_start_local)?.as_u64())
}

pub(crate) fn touched_streams_by_page(
    fragments: &[BitmapFragmentWrite],
) -> Result<BTreeMap<u64, BTreeSet<String>>> {
    let mut out = BTreeMap::<u64, BTreeSet<String>>::new();

    for fragment in fragments {
        let page_start = stream_page_global_start(&fragment.stream_id, fragment.page_start_local)?;
        out.entry(page_start)
            .or_default()
            .insert(fragment.stream_id.clone());
    }

    Ok(out)
}

pub(crate) fn local_page_start(global_page_start: u64) -> u32 {
    (global_page_start % (1u64 << PrimaryId::LOCAL_ID_BITS)) as u32
}

fn u64_from_key(key: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = key
        .try_into()
        .map_err(|_| MonadChainDataError::Decode("invalid u64 clustering key"))?;
    Ok(u64::from_be_bytes(bytes))
}

fn compacted_bitmap_blob(
    bitmap: RoaringBitmap,
    page_start_local: u32,
) -> Result<Option<BitmapBlob>> {
    if bitmap.is_empty() {
        return Ok(None);
    }

    let count = u32::try_from(bitmap.len())
        .map_err(|_| MonadChainDataError::Decode("bitmap fragment count overflow"))?;
    let min_local = bitmap.min().unwrap_or(page_start_local);
    let max_local = bitmap.max().unwrap_or(page_start_local);
    Ok(Some(BitmapBlob {
        min_local,
        max_local,
        count,
        bitmap,
    }))
}

fn block_number_key(block_number: u64) -> [u8; 8] {
    block_number.to_be_bytes()
}

pub(crate) fn parse_stream_shard(stream_id: &str) -> Result<u64> {
    let shard_hex = stream_id
        .rsplit('/')
        .next()
        .ok_or(MonadChainDataError::Decode("empty stream id"))?;
    u64::from_str_radix(shard_hex, 16)
        .map_err(|_| MonadChainDataError::Decode("invalid stream id shard"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_blob() -> BitmapBlob {
        let mut bitmap = RoaringBitmap::new();
        for v in [3u32, 7, 42, 1000] {
            bitmap.insert(v);
        }
        BitmapBlob {
            min_local: 3,
            max_local: 1000,
            count: 4,
            bitmap,
        }
    }

    #[test]
    fn bitmap_blob_round_trips() {
        let encoded = encode_bitmap_blob(&sample_blob()).unwrap();
        let decoded = decode_bitmap_blob(encoded.as_ref()).unwrap();
        assert_eq!(decoded.min_local, 3);
        assert_eq!(decoded.max_local, 1000);
        assert_eq!(decoded.count, 4);
        assert_eq!(decoded.bitmap.len(), 4);
    }

    #[test]
    fn decode_rejects_header_with_wrong_max_local() {
        let mut encoded = encode_bitmap_blob(&sample_blob()).unwrap().to_vec();
        // Corrupt `max_local` (bytes 5..9) to a too-narrow value that would
        // make the query-time overlap check skip a page that actually matches.
        encoded[5..9].copy_from_slice(&10u32.to_be_bytes());
        assert!(matches!(
            decode_bitmap_blob(&encoded),
            Err(MonadChainDataError::Decode(
                "bitmap blob header does not match payload"
            ))
        ));
    }

    #[test]
    fn decode_rejects_header_with_wrong_count() {
        let mut encoded = encode_bitmap_blob(&sample_blob()).unwrap().to_vec();
        // Corrupt `count` (bytes 9..13).
        encoded[9..13].copy_from_slice(&99u32.to_be_bytes());
        assert!(matches!(
            decode_bitmap_blob(&encoded),
            Err(MonadChainDataError::Decode(
                "bitmap blob header does not match payload"
            ))
        ));
    }

    #[test]
    fn bitmap_page_counts_round_trips_and_sorts_dropping_empty_pages() {
        // Out-of-order input with a zero-count page that must be dropped.
        let counts = BitmapPageCounts::from_pairs([
            (2 * STREAM_PAGE_LOCAL_ID_SPAN, 5),
            (0, 9),
            (STREAM_PAGE_LOCAL_ID_SPAN, 0),
        ]);
        assert_eq!(
            counts.pages,
            vec![(0, 9), (2 * STREAM_PAGE_LOCAL_ID_SPAN, 5)]
        );

        let encoded = counts.encode();
        let decoded = BitmapPageCounts::decode(encoded.as_ref()).unwrap();
        assert_eq!(decoded, counts);

        // Lookups: present pages return their count, the dropped/zero page and
        // an untouched page both return `None`.
        assert_eq!(decoded.count_for_page(0), Some(9));
        assert_eq!(
            decoded.count_for_page(2 * STREAM_PAGE_LOCAL_ID_SPAN),
            Some(5)
        );
        assert_eq!(decoded.count_for_page(STREAM_PAGE_LOCAL_ID_SPAN), None);
    }

    #[test]
    fn bitmap_page_counts_decode_rejects_bad_version_and_length() {
        let encoded = BitmapPageCounts::from_pairs([(0, 1)]).encode().to_vec();

        let mut bad_version = encoded.clone();
        bad_version[0] = 0xff;
        assert!(matches!(
            BitmapPageCounts::decode(&bad_version),
            Err(MonadChainDataError::Decode(
                "unsupported bitmap page counts version"
            ))
        ));

        // Truncated body: header claims one page but no pair bytes follow.
        let truncated = &encoded[..5];
        assert!(matches!(
            BitmapPageCounts::decode(truncated),
            Err(MonadChainDataError::Decode(
                "bitmap page counts length mismatch"
            ))
        ));
    }

    #[test]
    fn bitmap_page_artifact_roundtrips_and_legacy_blob_is_not_wrapped() {
        let bitmap_blob = BitmapBlob {
            min_local: 7,
            max_local: 19,
            count: 2,
            bitmap: RoaringBitmap::from_iter([7, 19]),
        };
        let encoded_blob = encode_bitmap_blob(&bitmap_blob).unwrap();
        assert!(decode_bitmap_page_artifact(encoded_blob.as_ref())
            .unwrap()
            .is_none());

        let artifact = BitmapPageArtifact {
            meta: BitmapPageMeta {
                min_local: 7,
                max_local: 19,
                count: 2,
            },
            bitmap_blob: encoded_blob.clone(),
        };
        let encoded_artifact = encode_bitmap_page_artifact(&artifact);
        let decoded = decode_bitmap_page_artifact(encoded_artifact.as_ref())
            .unwrap()
            .unwrap();
        assert_eq!(decoded, artifact);
        assert_eq!(
            decode_bitmap_blob(decoded.bitmap_blob.as_ref())
                .unwrap()
                .bitmap,
            bitmap_blob.bitmap
        );
    }
}
