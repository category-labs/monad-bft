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

use std::collections::BTreeMap;

use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::{
    error::{MonadChainDataError, Result},
    primitives::state::LogId,
    store::{MetaStore, ScannableKvTable},
};

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

pub struct BitmapTables<M: MetaStore> {
    fragments: ScannableKvTable<M>,
}

impl<M: MetaStore> BitmapTables<M> {
    pub fn new(fragments: ScannableKvTable<M>) -> Self {
        Self { fragments }
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

pub fn page_start_local(local_id: u32) -> u32 {
    (local_id / STREAM_PAGE_LOCAL_ID_SPAN) * STREAM_PAGE_LOCAL_ID_SPAN
}

pub fn sharded_stream_id(index_kind: &str, value: &[u8], shard: u64) -> String {
    let shard_hex_width = ((64 - LogId::LOCAL_ID_BITS) as usize).div_ceil(4);
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

/// Expands one log into the indexed stream entries written at ingest time.
pub fn stream_entries_for_log(
    address: &[u8],
    topics: &[alloy_primitives::B256],
    global_log_id: LogId,
) -> Vec<(String, u32)> {
    let shard = global_log_id.shard();
    let local = global_log_id.local();

    let mut entries = Vec::with_capacity(5);
    entries.push((sharded_stream_id("addr", address, shard), local));

    let topic_kinds = ["topic0", "topic1", "topic2", "topic3"];
    for (topic, kind) in topics.iter().zip(topic_kinds) {
        entries.push((sharded_stream_id(kind, topic.as_slice(), shard), local));
    }

    entries
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
