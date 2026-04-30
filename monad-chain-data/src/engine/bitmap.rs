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
    store::{KvTable, MetaStore, ScannableKvTable, ScannableTableId},
};

pub const LOCAL_ID_BITS: u32 = PrimaryId::LOCAL_ID_BITS;
pub const STREAM_PAGE_LOCAL_ID_SPAN: u32 = 64 * 1024;
const BITMAP_BLOB_VERSION: u8 = 2;
const BITMAP_BLOB_HEADER_LEN: usize = 1 + 4 * 3;

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
    fragments: ScannableKvTable<M>,
    page_meta: KvTable<M>,
    page_blobs: KvTable<M>,
    open_streams: ScannableKvTable<M>,
}

impl<M: MetaStore> BitmapTables<M> {
    pub fn new(
        fragments: ScannableKvTable<M>,
        page_meta: KvTable<M>,
        page_blobs: KvTable<M>,
        open_streams: ScannableKvTable<M>,
    ) -> Self {
        Self {
            fragments,
            page_meta,
            page_blobs,
            open_streams,
        }
    }

    pub fn fragments_table(&self) -> ScannableTableId {
        self.fragments.table
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

    /// Loads all retained fragments for one stream page.
    pub async fn load_fragments(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Vec<Bytes>> {
        let partition = stream_page_key(stream_id, page_start_local);
        // Keep scan-keys + point gets explicit: point-table caches often make
        // this faster than value-bearing scans, and the store API can stay
        // simple until a backend proves it needs batched value reads.
        let page = self
            .fragments
            .list_prefix(&partition, &[], None, usize::MAX)
            .await?;
        let mut fragments = Vec::with_capacity(page.keys.len());

        for clustering in page.keys {
            let record = self.fragments.get(&partition, &clustering).await?.ok_or(
                MonadChainDataError::MissingData("missing log bitmap fragment"),
            )?;
            fragments.push(record.value);
        }

        Ok(fragments)
    }

    /// Loads the compacted page metadata for one sealed stream page.
    pub async fn load_page_meta(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Option<BitmapPageMeta>> {
        let key = stream_page_key(stream_id, page_start_local);
        let Some(record) = self.page_meta.get(&key).await? else {
            return Ok(None);
        };

        Ok(Some(BitmapPageMeta::decode(&record.value)?))
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

    /// Loads the compacted bitmap blob for one sealed stream page.
    pub async fn load_page_blob(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Option<Bytes>> {
        let key = stream_page_key(stream_id, page_start_local);
        Ok(self.page_blobs.get(&key).await?.map(|record| record.value))
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

pub(crate) fn local_page_start(global_page_start: u64) -> u32 {
    (global_page_start % (1u64 << PrimaryId::LOCAL_ID_BITS)) as u32
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
