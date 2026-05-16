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

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::{
    engine::ingest::ReadPlanningTimings,
    error::{MonadChainDataError, Result},
    primitives::state::PrimaryId,
    store::{
        blob::BlobStore, CachedKvTable, CachedScannableTable, MetaStore, MetaWriteOp,
        ScannableTableId, WriteSession,
    },
};

pub const LOCAL_ID_BITS: u32 = PrimaryId::LOCAL_ID_BITS;
pub const STREAM_PAGE_LOCAL_ID_SPAN: u32 = 64 * 1024;
const BITMAP_BLOB_VERSION: u8 = 2;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, alloy_rlp::RlpEncodable, alloy_rlp::RlpDecodable)]
pub struct BitmapPageMeta {
    pub min_local: u32,
    pub max_local: u32,
    pub count: u32,
}

impl BitmapPageMeta {
    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid bitmap page meta rlp"))
    }
}

pub struct BitmapTables<M: MetaStore> {
    fragments: CachedScannableTable<M>,
    page_meta: CachedKvTable<M>,
    page_blobs: CachedKvTable<M>,
    open_streams: CachedScannableTable<M>,
}

impl<M: MetaStore> BitmapTables<M> {
    pub fn new(
        fragments: CachedScannableTable<M>,
        page_meta: CachedKvTable<M>,
        page_blobs: CachedKvTable<M>,
        open_streams: CachedScannableTable<M>,
    ) -> Self {
        Self {
            fragments,
            page_meta,
            page_blobs,
            open_streams,
        }
    }

    pub fn fragments_table(&self) -> ScannableTableId {
        self.fragments.table_id()
    }

    pub(crate) fn fragments_cache(&self) -> &CachedScannableTable<M> {
        &self.fragments
    }

    pub(crate) fn page_meta_cache(&self) -> &CachedKvTable<M> {
        &self.page_meta
    }

    pub(crate) fn page_blobs_cache(&self) -> &CachedKvTable<M> {
        &self.page_blobs
    }

    pub(crate) fn open_streams_cache(&self) -> &CachedScannableTable<M> {
        &self.open_streams
    }

    /// Stores one bitmap fragment for a block within the stream page it covers.
    pub async fn store_fragment(
        &self,
        fragment: &BitmapFragmentWrite,
        block_number: u64,
    ) -> Result<()> {
        let partition = stream_page_key(&fragment.stream_id, fragment.page_start_local);
        let clustering = block_number_key(block_number);
        self.fragments
            .put(&partition, &clustering, fragment.bitmap_blob.clone())
            .await?;
        Ok(())
    }

    pub fn stage_fragment<B: BlobStore>(
        &self,
        w: &WriteSession<'_, M, B>,
        fragment: &BitmapFragmentWrite,
        block_number: u64,
    ) {
        let partition = stream_page_key(&fragment.stream_id, fragment.page_start_local);
        let clustering = block_number_key(block_number);
        w.scan_put_uncached(
            &self.fragments,
            &partition,
            &clustering,
            fragment.bitmap_blob.clone(),
        );
    }

    pub fn stage_fragments_for_global_page<B: BlobStore>(
        &self,
        w: &WriteSession<'_, M, B>,
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
    pub async fn load_fragments(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Vec<Bytes>> {
        self.load_fragments_with_timings(stream_id, page_start_local, None)
            .await
    }

    pub(crate) async fn load_fragments_with_timings(
        &self,
        stream_id: &str,
        page_start_local: u32,
        mut timings: Option<&mut ReadPlanningTimings>,
    ) -> Result<Vec<Bytes>> {
        let partition = stream_page_key(stream_id, page_start_local);
        // Keep scan-keys + point gets explicit: point-table caches often make
        // this faster than value-bearing scans, and the store API can stay
        // simple until a backend proves it needs batched value reads.
        let list_start = Instant::now();
        let page = self
            .fragments
            .list_prefix(&partition, &[], None, usize::MAX)
            .await?;
        if let Some(t) = timings.as_deref_mut() {
            t.bitmap_list_ms = t
                .bitmap_list_ms
                .saturating_add(list_start.elapsed().as_millis() as u64);
            t.bitmap_list_count = t.bitmap_list_count.saturating_add(1);
        }
        let mut fragments = Vec::with_capacity(page.keys.len());

        for clustering in page.keys {
            let get_start = Instant::now();
            let bytes = self.fragments.get(&partition, &clustering).await?.ok_or(
                MonadChainDataError::MissingData("missing log bitmap fragment"),
            )?;
            if let Some(t) = timings.as_deref_mut() {
                t.bitmap_get_ms = t
                    .bitmap_get_ms
                    .saturating_add(get_start.elapsed().as_millis() as u64);
                t.bitmap_get_count = t.bitmap_get_count.saturating_add(1);
            }
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

    /// Loads the compacted page metadata for one sealed stream page.
    pub async fn load_page_meta(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Option<BitmapPageMeta>> {
        let key = stream_page_key(stream_id, page_start_local);
        let Some(bytes) = self.page_meta.get(&key).await? else {
            return Ok(None);
        };

        Ok(Some(BitmapPageMeta::decode(&bytes)?))
    }

    pub async fn store_page_meta(
        &self,
        stream_id: &str,
        page_start_local: u32,
        meta: &BitmapPageMeta,
    ) -> Result<()> {
        let key = stream_page_key(stream_id, page_start_local);
        self.page_meta.put(&key, Bytes::from(meta.encode())).await?;
        Ok(())
    }

    pub fn stage_page_meta<B: BlobStore>(
        &self,
        w: &WriteSession<'_, M, B>,
        stream_id: &str,
        page_start_local: u32,
        page_meta: &BitmapPageMeta,
    ) {
        let key = stream_page_key(stream_id, page_start_local);
        w.put(&self.page_meta, &key, Bytes::from(page_meta.encode()));
    }

    /// Loads a compacted bitmap page, supporting both the current combined
    /// artifact row and the legacy split page-meta/page-blob representation.
    pub async fn load_page_artifact(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Option<BitmapPageArtifact>> {
        let key = stream_page_key(stream_id, page_start_local);
        let Some(bytes) = self.page_blobs.get(&key).await? else {
            if self.page_meta.get(&key).await?.is_some() {
                return Err(MonadChainDataError::MissingData("missing bitmap page blob"));
            }
            return Ok(None);
        };

        if let Some(artifact) = decode_bitmap_page_artifact(bytes.as_ref())? {
            return Ok(Some(artifact));
        }

        let meta = self
            .load_page_meta(stream_id, page_start_local)
            .await?
            .ok_or(MonadChainDataError::MissingData("missing bitmap page meta"))?;
        Ok(Some(BitmapPageArtifact {
            meta,
            bitmap_blob: bytes,
        }))
    }

    pub async fn store_page_artifact(
        &self,
        stream_id: &str,
        page_start_local: u32,
        artifact: &BitmapPageArtifact,
    ) -> Result<()> {
        let key = stream_page_key(stream_id, page_start_local);
        self.page_blobs
            .put(&key, encode_bitmap_page_artifact(artifact))
            .await?;
        Ok(())
    }

    pub fn stage_page_artifact<B: BlobStore>(
        &self,
        w: &WriteSession<'_, M, B>,
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

    /// Loads the compacted bitmap blob for one sealed stream page.
    pub async fn load_page_blob(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Option<Bytes>> {
        let key = stream_page_key(stream_id, page_start_local);
        let Some(bytes) = self.page_blobs.get(&key).await? else {
            return Ok(None);
        };
        if let Some(artifact) = decode_bitmap_page_artifact(bytes.as_ref())? {
            return Ok(Some(artifact.bitmap_blob));
        }
        Ok(Some(bytes))
    }

    pub async fn store_page_blob(
        &self,
        stream_id: &str,
        page_start_local: u32,
        bitmap_blob: Bytes,
    ) -> Result<()> {
        let key = stream_page_key(stream_id, page_start_local);
        self.page_blobs.put(&key, bitmap_blob).await?;
        Ok(())
    }

    pub fn stage_page_blob<B: BlobStore>(
        &self,
        w: &WriteSession<'_, M, B>,
        stream_id: &str,
        page_start_local: u32,
        bitmap_blob: Bytes,
    ) {
        let key = stream_page_key(stream_id, page_start_local);
        w.put(&self.page_blobs, &key, bitmap_blob);
    }

    /// Loads the open stream inventory for one frontier page.
    pub async fn load_open_streams(&self, global_page_start: u64) -> Result<Vec<String>> {
        self.load_open_streams_with_timings(global_page_start, None)
            .await
    }

    pub(crate) async fn load_open_streams_with_timings(
        &self,
        global_page_start: u64,
        mut timings: Option<&mut ReadPlanningTimings>,
    ) -> Result<Vec<String>> {
        let partition = global_page_start.to_be_bytes();
        let list_start = Instant::now();
        let page = self
            .open_streams
            .list_prefix(&partition, &[], None, usize::MAX)
            .await?;
        if let Some(t) = timings.as_deref_mut() {
            t.bitmap_open_streams_ms = t
                .bitmap_open_streams_ms
                .saturating_add(list_start.elapsed().as_millis() as u64);
            t.bitmap_open_streams_count = t.bitmap_open_streams_count.saturating_add(1);
        }
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
        w: &WriteSession<'_, M, B>,
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
    Ok((shard << PrimaryId::LOCAL_ID_BITS) + u64::from(page_start_local))
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
