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

use std::{collections::BTreeSet, sync::Arc};

use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::{
    engine::tables::scan_get_all,
    error::{MonadChainDataError, Result},
    store::{
        blob::BlobStore, CachedKvTable, CachedScannableKvTable, MetaStore, ScannableTableId,
        WriteSession,
    },
};

/// Ids per bitmap page: the seal granule. A page covers the aligned id range
/// `[page_start, page_start + STREAM_PAGE_ID_SPAN)`; bitmap bits are stored
/// PAGE-RELATIVE (bit `v` in page `P` is id `P + v`, `v` in `[0, span)`).
pub const STREAM_PAGE_ID_SPAN: u32 = 64 * 1024;
/// Ids per page group (256 pages): purely the manifest's keying/completeness
/// window — a stream's page-count manifest row covers exactly one group, and
/// is written once the family frontier leaves that group.
pub const PAGE_GROUP_ID_SPAN: u64 = 1 << 24;
const BITMAP_BLOB_VERSION: u8 = 1;
const BITMAP_PAGE_COUNTS_VERSION: u8 = 1;
const BITMAP_BLOB_HEADER_LEN: usize = 1 + 4 * 3;
// Must differ from `BITMAP_BLOB_VERSION`: a page artifact wraps a blob, and
// `decode_bitmap_page_artifact` discriminates the two formats by leading byte.
const BITMAP_PAGE_ARTIFACT_VERSION: u8 = 2;
const BITMAP_PAGE_ARTIFACT_HEADER_LEN: usize = 1 + 4 * 3;

/// A frontier-page bitmap fragment decoded from its stored bytes: the
/// roaring bitmap plus its min/max/count header (see [`encode_bitmap_blob`]).
/// Offsets are page-relative.
#[derive(Debug, Clone)]
pub struct DecodedBitmapFragment {
    pub min_offset: u32,
    pub max_offset: u32,
    pub count: u32,
    pub bitmap: RoaringBitmap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmapPageArtifact {
    pub meta: BitmapPageMeta,
    pub bitmap_blob: Bytes,
}

/// Page-relative min/max/count of one sealed page's bitmap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitmapPageMeta {
    pub min_offset: u32,
    pub max_offset: u32,
    pub count: u32,
}

/// A sealed bitmap page decoded for the read cache, `Arc`-shared so a hit
/// clones a refcount instead of re-deserializing. The on-disk
/// [`BitmapPageArtifact`] remains the write-path type.
#[derive(Debug, Clone)]
pub struct DecodedBitmapPage {
    pub meta: BitmapPageMeta,
    pub bitmap: RoaringBitmap,
}

/// Cache decoder for a stream's per-group page-count manifest.
pub(crate) fn decode_page_counts(bytes: Bytes) -> Result<BitmapPageCounts> {
    BitmapPageCounts::decode(&bytes)
}

/// Cache decoder for one stored bitmap fragment (frontier-page delta).
pub(crate) fn decode_fragment_blob(bytes: Bytes) -> Result<Arc<DecodedBitmapFragment>> {
    Ok(Arc::new(decode_bitmap_blob(bytes.as_ref())?))
}

/// Cache decoder for one sealed bitmap page artifact; a cache hit skips both
/// the framing parse and the roaring deserialize.
pub(crate) fn decode_page(bytes: Bytes) -> Result<Arc<DecodedBitmapPage>> {
    let artifact = decode_bitmap_page_artifact(bytes.as_ref())?
        .ok_or(MonadChainDataError::Decode("invalid bitmap page artifact"))?;
    let blob = decode_bitmap_blob(artifact.bitmap_blob.as_ref())?;
    // The writer duplicates the inner blob header into the outer artifact
    // header, and the query-time page skip reads the OUTER copy (see
    // `engine::query::bitmap::overlaps`). `decode_bitmap_blob` validated the
    // inner header against the payload; reject a divergent outer copy so a
    // corrupt too-narrow header fails loud instead of silently dropping the
    // page from results.
    let expected = BitmapPageMeta {
        min_offset: blob.min_offset,
        max_offset: blob.max_offset,
        count: blob.count,
    };
    if artifact.meta != expected {
        return Err(MonadChainDataError::Decode(
            "bitmap page artifact header does not match blob header",
        ));
    }
    // Built from the payload-validated inner triple, not the outer copy, so
    // downstream readers cannot depend on the unvalidated bytes.
    Ok(Arc::new(DecodedBitmapPage {
        meta: expected,
        bitmap: blob.bitmap,
    }))
}

/// Cache decoder for one open-stream inventory delta row (a chunk of stream
/// ids: one flush's first-seen set, or a sealed page's complete inventory).
pub(crate) fn decode_open_streams_chunk(bytes: Bytes) -> Result<Arc<Vec<String>>> {
    decode_open_streams_delta(&bytes).map(Arc::new)
}

#[derive(Clone)]
pub struct BitmapTables<M: MetaStore> {
    fragments: CachedScannableKvTable<M, Arc<DecodedBitmapFragment>>,
    page_blobs: CachedKvTable<M, Arc<DecodedBitmapPage>>,
    page_counts: CachedKvTable<M, BitmapPageCounts>,
    open_streams: CachedScannableKvTable<M, Arc<Vec<String>>>,
}

impl<M: MetaStore> BitmapTables<M> {
    pub fn new(
        fragments: CachedScannableKvTable<M, Arc<DecodedBitmapFragment>>,
        page_blobs: CachedKvTable<M, Arc<DecodedBitmapPage>>,
        page_counts: CachedKvTable<M, BitmapPageCounts>,
        open_streams: CachedScannableKvTable<M, Arc<Vec<String>>>,
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

    pub(crate) fn fragments_cache(&self) -> &CachedScannableKvTable<M, Arc<DecodedBitmapFragment>> {
        &self.fragments
    }

    pub(crate) fn page_blobs_cache(&self) -> &CachedKvTable<M, Arc<DecodedBitmapPage>> {
        &self.page_blobs
    }

    pub(crate) fn page_counts_cache(&self) -> &CachedKvTable<M, BitmapPageCounts> {
        &self.page_counts
    }

    pub(crate) fn open_streams_cache(&self) -> &CachedScannableKvTable<M, Arc<Vec<String>>> {
        &self.open_streams
    }

    /// Loads all retained fragments for one stream page: a keys-only scan plus
    /// concurrent point gets, which point-table caches make faster than a
    /// value-bearing scan.
    pub async fn load_fragments(
        &self,
        stream_id: &str,
        page_start: u64,
    ) -> Result<Vec<Arc<DecodedBitmapFragment>>> {
        let partition = stream_page_key(stream_id, page_start);
        scan_get_all(&self.fragments, &partition, "missing bitmap fragment").await
    }

    /// Loads a compacted bitmap page, decoded and served from the read cache.
    pub async fn load_page_artifact(
        &self,
        stream_id: &str,
        page_start: u64,
    ) -> Result<Option<Arc<DecodedBitmapPage>>> {
        let key = stream_page_key(stream_id, page_start);
        self.page_blobs.get(&key).await
    }

    pub fn stage_page_artifact<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        stream_id: &str,
        page_start: u64,
        artifact: &BitmapPageArtifact,
    ) {
        let key = stream_page_key(stream_id, page_start);
        w.put(
            &self.page_blobs,
            &key,
            encode_bitmap_page_artifact(artifact),
        );
    }

    /// Stages one encoded delta fragment for a stream's open (unsealed) page,
    /// keyed by `flush_block` (the batch's block number).
    pub fn stage_page_fragment<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        stream_id: &str,
        page_start: u64,
        flush_block: u64,
        blob: Bytes,
    ) {
        w.scan_put(
            &self.fragments,
            &stream_page_key(stream_id, page_start),
            &flush_block.to_be_bytes(),
            blob,
        );
    }

    /// Loads a stream's page-count manifest for one sealed page group.
    pub async fn load_page_counts(
        &self,
        stream_id: &str,
        group_start: u64,
    ) -> Result<Option<BitmapPageCounts>> {
        self.page_counts
            .get(&stream_group_key(stream_id, group_start))
            .await
    }

    pub fn stage_page_counts<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        stream_id: &str,
        group_start: u64,
        counts: &BitmapPageCounts,
    ) {
        w.put(
            &self.page_counts,
            &stream_group_key(stream_id, group_start),
            counts.encode(),
        );
    }

    /// Loads the stream inventory for one page by unioning its delta rows
    /// (clustering key `(marker_block, chunk_idx)`). A page's partition holds
    /// two row flavors with the same union semantics: per-flush first-seen
    /// rows while the page is open, plus its complete inventory staged at
    /// seal (see `ArtifactWrite::OpenStreams`) — recovery reads sealed pages'
    /// partitions when rebuilding the open group's page counts.
    ///
    /// Deliberately not filtered by `published_head`: a row ahead of the head
    /// only adds stream ids whose own fragments are head-filtered to empty —
    /// wasteful, never wrong.
    pub async fn load_open_streams(&self, page_start: u64) -> Result<Vec<String>> {
        let partition = page_start.to_be_bytes();
        let chunks = scan_get_all(
            &self.open_streams,
            &partition,
            "missing open_streams delta row",
        )
        .await?;

        let union: BTreeSet<String> = chunks.iter().flat_map(|c| c.iter().cloned()).collect();
        Ok(union.into_iter().collect())
    }

    /// Stages one open-streams delta row (a flush's first-seen stream ids, or
    /// a chunk of a sealed page's complete inventory), keyed
    /// `(marker_block, chunk_idx)` — the flush block for flush rows, the seal
    /// block for seal rows; the caller chunks the set to bound row size.
    pub fn stage_open_streams_delta<B: BlobStore>(
        &self,
        w: &mut WriteSession<'_, M, B>,
        page_start: u64,
        marker_block: u64,
        chunk_idx: u32,
        blob: Bytes,
    ) {
        w.scan_put(
            &self.open_streams,
            &page_start.to_be_bytes(),
            &open_streams_delta_key(marker_block, chunk_idx),
            blob,
        );
    }
}

/// Encodes one bitmap blob into the stored fragment/page format.
pub fn encode_bitmap_blob(blob: &DecodedBitmapFragment) -> Result<Bytes> {
    let mut payload = Vec::new();
    blob.bitmap
        .serialize_into(&mut payload)
        .map_err(|e| MonadChainDataError::Backend(format!("serialize bitmap blob: {e}")))?;

    let mut out = Vec::with_capacity(BITMAP_BLOB_HEADER_LEN + payload.len());
    out.push(BITMAP_BLOB_VERSION);
    out.extend_from_slice(&blob.min_offset.to_be_bytes());
    out.extend_from_slice(&blob.max_offset.to_be_bytes());
    out.extend_from_slice(&blob.count.to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(Bytes::from(out))
}

/// Reads a big-endian `u32` from `b[off..off+4]`. The caller has length-checked
/// the slice, so the `try_into` is infallible.
fn be_u32(b: &[u8], off: usize) -> u32 {
    u32::from_be_bytes(b[off..off + 4].try_into().expect("4 bytes"))
}

/// Parses the shared `[version u8][min u32][max u32][count u32]` framing header
/// (used by both the fragment-blob and page-artifact formats), returning the
/// `(min_offset, max_offset, count)` triple after checking the version byte.
fn decode_bitmap_meta_header(
    bytes: &[u8],
    version: u8,
    header_len: usize,
    too_short: &'static str,
    bad_version: &'static str,
) -> Result<(u32, u32, u32)> {
    let header = bytes
        .get(..header_len)
        .ok_or(MonadChainDataError::Decode(too_short))?;
    if header[0] != version {
        return Err(MonadChainDataError::Decode(bad_version));
    }
    Ok((be_u32(header, 1), be_u32(header, 5), be_u32(header, 9)))
}

/// Decodes one stored bitmap blob and validates its framing header.
pub fn decode_bitmap_blob(bytes: &[u8]) -> Result<DecodedBitmapFragment> {
    let (min_offset, max_offset, count) = decode_bitmap_meta_header(
        bytes,
        BITMAP_BLOB_VERSION,
        BITMAP_BLOB_HEADER_LEN,
        "bitmap blob too short",
        "unsupported bitmap blob version",
    )?;

    let bitmap = RoaringBitmap::deserialize_from(&bytes[BITMAP_BLOB_HEADER_LEN..])
        .map_err(|e| MonadChainDataError::Backend(format!("deserialize bitmap blob: {e}")))?;

    // Query-time skip decisions trust `min_offset`/`max_offset` (see
    // `engine::query::bitmap::overlaps`); a corrupt too-narrow header would
    // silently drop a page, so validate it against the decoded payload.
    // (`decode_page` extends this defense to the outer artifact header by
    // requiring it to match the triple validated here.)
    let header_matches_payload = match bitmap.min().zip(bitmap.max()) {
        Some((actual_min, actual_max)) => {
            u64::from(count) == bitmap.len() && min_offset == actual_min && max_offset == actual_max
        }
        None => count == 0,
    };
    if !header_matches_payload {
        return Err(MonadChainDataError::Decode(
            "bitmap blob header does not match payload",
        ));
    }

    Ok(DecodedBitmapFragment {
        min_offset,
        max_offset,
        count,
        bitmap,
    })
}

/// Encodes one compacted bitmap page into a single KV value: the version byte
/// prefixes the query metadata to the unchanged page-blob format.
pub fn encode_bitmap_page_artifact(artifact: &BitmapPageArtifact) -> Bytes {
    let mut out = Vec::with_capacity(BITMAP_PAGE_ARTIFACT_HEADER_LEN + artifact.bitmap_blob.len());
    out.push(BITMAP_PAGE_ARTIFACT_VERSION);
    out.extend_from_slice(&artifact.meta.min_offset.to_be_bytes());
    out.extend_from_slice(&artifact.meta.max_offset.to_be_bytes());
    out.extend_from_slice(&artifact.meta.count.to_be_bytes());
    out.extend_from_slice(&artifact.bitmap_blob);
    Bytes::from(out)
}

pub fn decode_bitmap_page_artifact(bytes: &[u8]) -> Result<Option<BitmapPageArtifact>> {
    // A leading byte that is not this version is not an artifact (`None`); the
    // cache decoder turns that into a loud decode error rather than guessing.
    if bytes.first().copied() != Some(BITMAP_PAGE_ARTIFACT_VERSION) {
        return Ok(None);
    }

    let (min_offset, max_offset, count) = decode_bitmap_meta_header(
        bytes,
        BITMAP_PAGE_ARTIFACT_VERSION,
        BITMAP_PAGE_ARTIFACT_HEADER_LEN,
        "bitmap page artifact too short",
        "unsupported bitmap page artifact version",
    )?;

    Ok(Some(BitmapPageArtifact {
        meta: BitmapPageMeta {
            min_offset,
            max_offset,
            count,
        },
        bitmap_blob: Bytes::copy_from_slice(&bytes[BITMAP_PAGE_ARTIFACT_HEADER_LEN..]),
    }))
}

/// Sparse per-stream roll-up of compacted per-page `count`s for one sealed
/// page group; immutable once built. Lets query time answer page emptiness and
/// clause selectivity without fetching bitmaps. Only non-empty pages are
/// listed (at most the 256 pages of a group), sorted by page key.
///
/// Page keys are GROUP-RELATIVE page starts: `page_start - group_start`
/// (equivalently the low 24 bits of the global page start), multiples of
/// [`STREAM_PAGE_ID_SPAN`] in `[0, PAGE_GROUP_ID_SPAN)`. See
/// [`page_start_in_group`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmapPageCounts {
    /// `(page_start_in_group, count)` pairs for the stream's non-empty pages,
    /// sorted ascending by page.
    pub pages: Vec<(u32, u32)>,
}

impl BitmapPageCounts {
    /// Builds a manifest from `(page_start_in_group, count)` pairs, dropping
    /// zero-count pages and sorting by page; on a duplicate page the first
    /// (in input order) wins.
    pub fn from_pairs(pairs: impl IntoIterator<Item = (u32, u32)>) -> Self {
        let mut pages: Vec<(u32, u32)> =
            pairs.into_iter().filter(|(_, count)| *count != 0).collect();
        pages.sort_by_key(|(page_start_in_group, _)| *page_start_in_group);
        pages.dedup_by_key(|(page_start_in_group, _)| *page_start_in_group);
        Self { pages }
    }

    /// Per-page count for `page_start_in_group`. `Some(0)` is never returned;
    /// `None` means the stream contributes nothing in that page.
    pub fn count_for_page(&self, page_start_in_group: u32) -> Option<u32> {
        self.pages
            .binary_search_by_key(&page_start_in_group, |(page, _)| *page)
            .ok()
            .map(|idx| self.pages[idx].1)
    }

    /// Layout: `[version u8][len u32]( [page_start_in_group u32][count u32] )*`,
    /// big-endian.
    pub fn encode(&self) -> Bytes {
        let mut out = Vec::with_capacity(1 + 4 + self.pages.len() * 8);
        out.push(BITMAP_PAGE_COUNTS_VERSION);
        out.extend_from_slice(&(self.pages.len() as u32).to_be_bytes());
        for (page_start_in_group, count) in &self.pages {
            out.extend_from_slice(&page_start_in_group.to_be_bytes());
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
        let len = be_u32(len_bytes, 0) as usize;
        let body = &bytes[5..];
        if body.len() != len * 8 {
            return Err(MonadChainDataError::Decode(
                "bitmap page counts length mismatch",
            ));
        }
        let mut pages = Vec::with_capacity(len);
        for chunk in body.chunks_exact(8) {
            pages.push((be_u32(chunk, 0), be_u32(chunk, 4)));
        }
        Ok(Self { pages })
    }
}

/// Start of the page containing `id`: the id with its low 16 bits cleared.
pub fn page_start(id: u64) -> u64 {
    id & !u64::from(STREAM_PAGE_ID_SPAN - 1)
}

/// Page-relative offset of `id` within its page: `id - page_start(id)`.
pub fn page_offset(id: u64) -> u32 {
    (id & u64::from(STREAM_PAGE_ID_SPAN - 1)) as u32
}

/// Start of the page group containing `id`: the id with its low 24 bits
/// cleared.
pub fn page_group_start(id: u64) -> u64 {
    id & !(PAGE_GROUP_ID_SPAN - 1)
}

/// Group-relative page start (the manifest's page key):
/// `page_start - page_group_start(page_start)`, i.e. the low 24 bits of the
/// global page start.
pub fn page_start_in_group(page_start: u64) -> u32 {
    (page_start & (PAGE_GROUP_ID_SPAN - 1)) as u32
}

/// Renders the canonical stream id for an indexed `(kind, value)` pair. One
/// logical stream spans the whole id space; pages and manifests key further
/// segments under it.
pub fn render_stream_id(index_kind: &str, value: &[u8]) -> String {
    format!("{index_kind}/{}", alloy_primitives::hex::encode(value))
}

/// The index kinds that name bitmap streams (the first segment of a
/// [`render_stream_id`]), across all families. This enum is the join key
/// between ingest writes (`stream_entries_for_*`) and query reads
/// (`IndexedClause::kind`); both sides must spell a kind through it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexKind {
    Addr,
    Topic0,
    Topic1,
    Topic2,
    Topic3,
    From,
    To,
    Selector,
    TopLevel,
    HasTransfer,
}

impl IndexKind {
    const ALL: [IndexKind; 10] = [
        IndexKind::Addr,
        IndexKind::Topic0,
        IndexKind::Topic1,
        IndexKind::Topic2,
        IndexKind::Topic3,
        IndexKind::From,
        IndexKind::To,
        IndexKind::Selector,
        IndexKind::TopLevel,
        IndexKind::HasTransfer,
    ];

    /// Topic kinds by topic position: index `i` holds `topic{i}`.
    pub(crate) const TOPICS: [IndexKind; 4] = [
        IndexKind::Topic0,
        IndexKind::Topic1,
        IndexKind::Topic2,
        IndexKind::Topic3,
    ];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            IndexKind::Addr => "addr",
            IndexKind::Topic0 => "topic0",
            IndexKind::Topic1 => "topic1",
            IndexKind::Topic2 => "topic2",
            IndexKind::Topic3 => "topic3",
            IndexKind::From => "from",
            IndexKind::To => "to",
            IndexKind::Selector => "selector",
            IndexKind::TopLevel => "top_level",
            IndexKind::HasTransfer => "has_transfer",
        }
    }

    /// The canonical stream-value byte length for this kind: 20 (address-like),
    /// 32 (topic), 4 (selector), or 0 (flag streams).
    fn expected_value_len(self) -> usize {
        match self {
            IndexKind::Addr | IndexKind::From | IndexKind::To => 20,
            IndexKind::Topic0 | IndexKind::Topic1 | IndexKind::Topic2 | IndexKind::Topic3 => 32,
            IndexKind::Selector => 4,
            IndexKind::TopLevel | IndexKind::HasTransfer => 0,
        }
    }
}

/// Compact `Copy` in-memory identity of a bitmap stream, used as the hot-path
/// map key on the ingest index track. The canonical string form
/// ([`Self::render`], byte-identical to [`render_stream_id`]) is produced
/// exactly once per persisted artifact, so every stored byte is unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct StreamKey {
    kind: IndexKind,
    /// Stream-value length: 0 (flag streams), 4 (selector), 20 (address) or
    /// 32 (topic); the value occupies `value[..len]`.
    len: u8,
    value: [u8; 32],
}

impl StreamKey {
    pub(crate) fn new(kind: IndexKind, value: &[u8]) -> Self {
        debug_assert_eq!(
            value.len(),
            kind.expected_value_len(),
            "unexpected stream value length for {kind:?}"
        );
        let mut buf = [0u8; 32];
        buf[..value.len()].copy_from_slice(value);
        Self {
            kind,
            len: value.len() as u8,
            value: buf,
        }
    }

    /// Render the canonical stream id — byte-identical to what
    /// [`render_stream_id`] produces for the same kind/value.
    pub(crate) fn render(&self) -> String {
        render_stream_id(self.kind.as_str(), &self.value[..self.len as usize])
    }

    /// Parse a stored canonical stream id back into its compact key (recovery
    /// and snapshot decode read the rendered strings). Inverse of
    /// [`Self::render`] for every id this engine writes.
    pub(crate) fn parse(stream_id: &str) -> Result<Self> {
        let err = || MonadChainDataError::Decode("malformed stream id");
        let (kind_str, value_hex) = stream_id.split_once('/').ok_or_else(err)?;
        let kind = *IndexKind::ALL
            .iter()
            .find(|k| k.as_str() == kind_str)
            .ok_or_else(err)?;
        // Strict inverse of `render`'s value segment: lowercase hex only
        // (`hex::decode` would otherwise accept uppercase and `0x` prefixes).
        if !value_hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(err());
        }
        let value = alloy_primitives::hex::decode(value_hex).map_err(|_| err())?;
        if value.len() != kind.expected_value_len() {
            return Err(err());
        }
        Ok(Self::new(kind, &value))
    }
}

/// Row key for one stream page (fragments partition / page artifact): the
/// stream id, a `/` separator, then the big-endian global page start.
pub fn stream_page_key(stream_id: &str, page_start: u64) -> Vec<u8> {
    stream_scoped_key(stream_id, page_start)
}

/// Row key for one stream's page-count manifest row: the stream id, a `/`
/// separator, then the big-endian group start. Same shape as
/// [`stream_page_key`]; the two live in different tables.
fn stream_group_key(stream_id: &str, group_start: u64) -> Vec<u8> {
    stream_scoped_key(stream_id, group_start)
}

fn stream_scoped_key(stream_id: &str, start: u64) -> Vec<u8> {
    let mut key = format!("{stream_id}/").into_bytes();
    key.extend_from_slice(&start.to_be_bytes());
    key
}

/// Target encoded size of one open-streams delta row; first-seen sets are
/// chunked under this to stay well below the backend object limit.
pub const OPEN_STREAMS_DELTA_TARGET_BYTES: usize = 4 * 1024 * 1024;
const OPEN_STREAMS_DELTA_VERSION: u8 = 1;

/// Clustering key for an open-streams delta row: `marker_block` (the flush
/// block for flush rows, the seal block for a sealed page's inventory rows)
/// then the within-burst chunk index, both big-endian.
fn open_streams_delta_key(marker_block: u64, chunk_idx: u32) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[..8].copy_from_slice(&marker_block.to_be_bytes());
    key[8..].copy_from_slice(&chunk_idx.to_be_bytes());
    key
}

/// Encodes a chunk of open-stream ids: `[version][count u32]( [len u32][utf8] )*`.
pub fn encode_open_streams_delta(streams: &[String]) -> Bytes {
    let mut out = Vec::with_capacity(5 + streams.iter().map(|s| s.len() + 4).sum::<usize>());
    out.push(OPEN_STREAMS_DELTA_VERSION);
    out.extend_from_slice(&(streams.len() as u32).to_be_bytes());
    for s in streams {
        out.extend_from_slice(&(s.len() as u32).to_be_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    Bytes::from(out)
}

/// Decodes a row written by [`encode_open_streams_delta`].
pub fn decode_open_streams_delta(bytes: &[u8]) -> Result<Vec<String>> {
    let mut cur = bytes;
    let version = *cur
        .first()
        .ok_or(MonadChainDataError::Decode("open_streams delta empty"))?;
    if version != OPEN_STREAMS_DELTA_VERSION {
        return Err(MonadChainDataError::Decode("open_streams delta version"));
    }
    cur = &cur[1..];
    let count = take_u32(&mut cur)?;
    let mut out = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let len = take_u32(&mut cur)? as usize;
        let raw = cur
            .get(..len)
            .ok_or(MonadChainDataError::Decode("open_streams delta truncated"))?;
        out.push(
            String::from_utf8(raw.to_vec())
                .map_err(|_| MonadChainDataError::Decode("open_streams delta utf8"))?,
        );
        cur = &cur[len..];
    }
    Ok(out)
}

fn take_u32(cur: &mut &[u8]) -> Result<u32> {
    let bytes = cur
        .get(..4)
        .ok_or(MonadChainDataError::Decode("open_streams delta truncated"))?;
    let value = be_u32(bytes, 0);
    *cur = &cur[4..];
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_blob() -> DecodedBitmapFragment {
        let mut bitmap = RoaringBitmap::new();
        for v in [3u32, 7, 42, 1000] {
            bitmap.insert(v);
        }
        DecodedBitmapFragment {
            min_offset: 3,
            max_offset: 1000,
            count: 4,
            bitmap,
        }
    }

    #[test]
    fn page_math_helpers_agree() {
        let span = u64::from(STREAM_PAGE_ID_SPAN);
        for id in [0u64, 1, span - 1, span, span + 7, PAGE_GROUP_ID_SPAN + 3] {
            assert_eq!(page_start(id), (id / span) * span);
            assert_eq!(u64::from(page_offset(id)), id - page_start(id));
            assert_eq!(
                page_group_start(id),
                (id / PAGE_GROUP_ID_SPAN) * PAGE_GROUP_ID_SPAN
            );
        }
        let page = page_start(PAGE_GROUP_ID_SPAN + 3 * span);
        assert_eq!(
            u64::from(page_start_in_group(page)),
            page - page_group_start(page)
        );
    }

    #[test]
    fn stream_key_render_matches_render_stream_id_and_parses_back() {
        // Every kind at its real value size.
        let cases: &[(IndexKind, &[u8])] = &[
            (IndexKind::Addr, &[0xab; 20]),
            (IndexKind::Topic0, &[0x00; 32]),
            (IndexKind::Topic1, &[0x11; 32]),
            (IndexKind::Topic2, &[0xfe; 32]),
            (IndexKind::Topic3, &[0x7f; 32]),
            (IndexKind::From, &[0x01; 20]),
            (IndexKind::To, &[0xee; 20]),
            (IndexKind::Selector, &[0xde, 0xad, 0xbe, 0xef]),
            (IndexKind::TopLevel, &[]),
            (IndexKind::HasTransfer, &[]),
        ];
        for &(kind, value) in cases {
            let key = StreamKey::new(kind, value);
            let rendered = key.render();
            assert_eq!(rendered, render_stream_id(kind.as_str(), value));
            assert_eq!(StreamKey::parse(&rendered).unwrap(), key);
        }
    }

    #[test]
    fn stream_key_parse_rejects_malformed_ids() {
        let addr20 = "00112233445566778899aabbccddeeff00112233";
        let topic32 = "ab".repeat(32);
        for bad in [
            "",
            "addr",
            &format!("nope/{addr20}"), // unknown kind
            "addr/zz",                 // bad hex value
            "addr/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            // value length must match the kind: a topic-sized value under `addr`.
            &format!("addr/{topic32}"),
            // ...and an address-sized value under a topic kind.
            &format!("topic0/{addr20}"),
            // a retired shard-era id (trailing shard segment),
            &format!("addr/{addr20}/0000000000"),
            // and uppercase hex (not a strict inverse of render).
            &format!("addr/{}", addr20.to_uppercase()),
        ] {
            assert!(StreamKey::parse(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn open_streams_delta_round_trips() {
        let streams = vec![
            "addr/00112233445566778899aabbccddeeff00112233".to_string(),
            "topic0/".to_string() + &"ab".repeat(32),
            String::new(), // empty id survives the round trip
        ];
        let encoded = encode_open_streams_delta(&streams);
        assert_eq!(decode_open_streams_delta(&encoded).unwrap(), streams);
    }

    #[test]
    fn open_streams_delta_empty_round_trips() {
        let encoded = encode_open_streams_delta(&[]);
        assert!(decode_open_streams_delta(&encoded).unwrap().is_empty());
    }

    #[test]
    fn open_streams_delta_rejects_truncation_and_bad_version() {
        let encoded = encode_open_streams_delta(&["x".to_string()]);
        assert!(decode_open_streams_delta(&encoded[..encoded.len() - 1]).is_err());
        let mut bad = encoded.to_vec();
        bad[0] = 0xff;
        assert!(decode_open_streams_delta(&bad).is_err());
    }

    #[test]
    fn open_streams_delta_key_orders_by_marker_then_chunk() {
        assert!(open_streams_delta_key(1, 9) < open_streams_delta_key(2, 0));
        assert!(open_streams_delta_key(2, 0) < open_streams_delta_key(2, 1));
    }

    #[test]
    fn bitmap_blob_round_trips() {
        let encoded = encode_bitmap_blob(&sample_blob()).unwrap();
        let decoded = decode_bitmap_blob(encoded.as_ref()).unwrap();
        assert_eq!(decoded.min_offset, 3);
        assert_eq!(decoded.max_offset, 1000);
        assert_eq!(decoded.count, 4);
        assert_eq!(decoded.bitmap, sample_blob().bitmap);
    }

    #[test]
    fn decode_rejects_header_with_wrong_max_offset() {
        let mut encoded = encode_bitmap_blob(&sample_blob()).unwrap().to_vec();
        // Corrupt `max_offset` to a too-narrow value.
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
        encoded[9..13].copy_from_slice(&99u32.to_be_bytes());
        assert!(matches!(
            decode_bitmap_blob(&encoded),
            Err(MonadChainDataError::Decode(
                "bitmap blob header does not match payload"
            ))
        ));
    }

    #[test]
    fn decode_rejects_prior_format_version() {
        // A pre-page-group blob (version byte 2) must fail loudly, not decode:
        // its bits were shard-local, not page-relative.
        let mut encoded = encode_bitmap_blob(&sample_blob()).unwrap().to_vec();
        encoded[0] = 2;
        assert!(matches!(
            decode_bitmap_blob(&encoded),
            Err(MonadChainDataError::Decode(
                "unsupported bitmap blob version"
            ))
        ));
    }

    #[test]
    fn bitmap_page_counts_round_trips_and_sorts_dropping_empty_pages() {
        let counts = BitmapPageCounts::from_pairs([
            (2 * STREAM_PAGE_ID_SPAN, 5),
            (0, 9),
            (STREAM_PAGE_ID_SPAN, 0),
        ]);
        assert_eq!(counts.pages, vec![(0, 9), (2 * STREAM_PAGE_ID_SPAN, 5)]);

        let encoded = counts.encode();
        let decoded = BitmapPageCounts::decode(encoded.as_ref()).unwrap();
        assert_eq!(decoded, counts);

        assert_eq!(decoded.count_for_page(0), Some(9));
        assert_eq!(decoded.count_for_page(2 * STREAM_PAGE_ID_SPAN), Some(5));
        assert_eq!(decoded.count_for_page(STREAM_PAGE_ID_SPAN), None);
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
    fn bitmap_page_artifact_roundtrips_and_plain_blob_is_not_wrapped() {
        let bitmap_blob = DecodedBitmapFragment {
            min_offset: 7,
            max_offset: 19,
            count: 2,
            bitmap: RoaringBitmap::from_iter([7, 19]),
        };
        let encoded_blob = encode_bitmap_blob(&bitmap_blob).unwrap();
        assert!(decode_bitmap_page_artifact(encoded_blob.as_ref())
            .unwrap()
            .is_none());

        let artifact = BitmapPageArtifact {
            meta: BitmapPageMeta {
                min_offset: 7,
                max_offset: 19,
                count: 2,
            },
            bitmap_blob: encoded_blob,
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

    #[test]
    fn decode_page_rejects_outer_header_diverging_from_blob_header() {
        let blob = sample_blob();
        let artifact = BitmapPageArtifact {
            meta: BitmapPageMeta {
                min_offset: blob.min_offset,
                max_offset: blob.max_offset,
                count: blob.count,
            },
            bitmap_blob: encode_bitmap_blob(&blob).unwrap(),
        };
        // The pristine artifact decodes with the (matching) outer meta.
        let decoded = decode_page(encode_bitmap_page_artifact(&artifact)).unwrap();
        assert_eq!(decoded.meta, artifact.meta);

        // Corrupt the OUTER `max_offset` to a too-narrow value; the inner blob
        // header still matches its payload, so only the cross-check catches
        // the divergence (the query page skip reads the outer copy). The
        // artifact header is `[version u8][min u32][max u32][count u32]`.
        const OUTER_MAX_OFFSET: std::ops::Range<usize> = 1 + 4..1 + 4 + 4;
        let mut corrupt = encode_bitmap_page_artifact(&artifact).to_vec();
        corrupt[OUTER_MAX_OFFSET].copy_from_slice(&10u32.to_be_bytes());
        assert!(matches!(
            decode_page(Bytes::from(corrupt)),
            Err(MonadChainDataError::Decode(
                "bitmap page artifact header does not match blob header"
            ))
        ));
    }
}
