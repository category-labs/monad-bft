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

//! Recovery snapshots: the blob-centric checkpoint store, its versioned binary
//! codec, and the persist/restore helpers around the open working set.
//!
//! Vocabulary: a *checkpoint* is the act/rendezvous of capturing the open
//! working set at a block; a *snapshot* is the persisted artifact it produces.

use std::collections::{BTreeMap, HashMap, VecDeque};

use bytes::Bytes;
use roaring::RoaringBitmap;
use tracing::warn;

use super::{
    index::{OpenState, OpenTail, SealedPageCounts},
    FamilyFrontier,
};
use crate::{
    engine::{bitmap::StreamKey, digest::ChainDigest},
    error::{MonadChainDataError, Result},
    store::{BlobStore, BlobTable, BlobTableId, MetaStore, TableId},
};

/// Blob-centric checkpoint persistence (v2). The snapshot payload — which
/// scales with the open working set and can exceed any meta-row size limit —
/// is written as ONE object to the blob store, keyed by a monotonically
/// increasing generation; then a small fixed-size manifest row is committed to
/// the meta store with a single put. The manifest is the only thing the loader
/// trusts, so a crash between the blob write and the manifest put leaves the
/// old manifest pointing at the old, complete generation — never a torn
/// snapshot.
///
/// The generation is the checkpoint block number: checkpoints persist the index
/// track's `last_block`, recovery resumes from `max(checkpoint, head)`, and the
/// engine only moves forward from the resume block, so successive checkpoints
/// carry non-decreasing block numbers. An equal-generation re-checkpoint (a
/// warm restart that ends before ingesting a new block) is skipped: the
/// committed snapshot already covers that block, and rewriting its blob in
/// place would open a digest/blob mismatch window (the blob+manifest pair is
/// not atomic).
///
/// Single-writer (the index track at `checkpoint`) and single-reader (`recover`
/// at startup), so plain `get`/`put` — no CAS.
#[derive(Clone)]
pub struct SnapshotStore<M: MetaStore, B: BlobStore> {
    meta: M,
    blobs: BlobTable<B>,
    /// `false` for a blob-less store: checkpoint payloads can be neither
    /// written nor read. A manifest row left behind by a previous blob-backed
    /// run is then ignored when the published head covers its generation, and
    /// a hard, actionable error otherwise — never a blind blob read that
    /// surfaces as an opaque backend failure.
    payloads_enabled: bool,
}

/// The committed pointer to the latest snapshot generation. Loading verifies
/// the payload against `payload_len`/`digest`; `previous` drives blob GC
/// (keep current + previous, best-effort delete previous-previous).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SnapshotManifest {
    generation: u64,
    payload_len: u64,
    digest: [u8; 32],
    previous: Option<u64>,
}

const MANIFEST_MAGIC: [u8; 4] = *b"CDSM";
const MANIFEST_VERSION: u8 = 1;
/// magic ∥ version ∥ generation ∥ payload_len ∥ digest ∥ prev flag ∥ prev gen.
const MANIFEST_LEN: usize = 4 + 1 + 8 + 8 + 32 + 1 + 8;

impl SnapshotManifest {
    fn encode(&self) -> Bytes {
        let mut out = Vec::with_capacity(MANIFEST_LEN);
        out.extend_from_slice(&MANIFEST_MAGIC);
        out.push(MANIFEST_VERSION);
        put_u64(&mut out, self.generation);
        put_u64(&mut out, self.payload_len);
        out.extend_from_slice(&self.digest);
        out.push(self.previous.is_some() as u8);
        put_u64(&mut out, self.previous.unwrap_or(0));
        Bytes::from(out)
    }

    /// `None` = not a manifest (wrong magic, version, or length).
    fn decode(bytes: &[u8]) -> Option<Self> {
        // Exact length: the cursor below then consumes every byte, so reads in
        // encode order can neither truncate nor leave a trailing remainder.
        if bytes.len() != MANIFEST_LEN {
            return None;
        }
        let cur = &mut &bytes[..];
        if take(cur, 4).ok()? != MANIFEST_MAGIC || take(cur, 1).ok()?[0] != MANIFEST_VERSION {
            return None;
        }
        let generation = take_u64(cur).ok()?;
        let payload_len = take_u64(cur).ok()?;
        let digest: [u8; 32] = take(cur, 32).ok()?.try_into().expect("32 bytes");
        let has_previous = match take(cur, 1).ok()?[0] {
            0 => false,
            1 => true,
            _ => return None,
        };
        let previous = take_u64(cur).ok()?;
        Some(Self {
            generation,
            payload_len,
            digest,
            previous: has_previous.then_some(previous),
        })
    }
}

/// Blob key for a snapshot generation: fixed-width big-endian, so byte order
/// equals generation order.
fn generation_key(generation: u64) -> [u8; 8] {
    generation.to_be_bytes()
}

impl<M: MetaStore, B: BlobStore> SnapshotStore<M, B> {
    /// Meta row holding the manifest.
    pub(crate) const MANIFEST_TABLE: TableId = TableId::new("ingest_snapshot");
    pub(crate) const MANIFEST_KEY: &'static [u8] = b"latest";
    /// Blob table holding the payload objects. Disjoint from the meta
    /// `ingest_snapshot` table: blob and meta tables live in separate
    /// backend namespaces (S3 key prefixes / the dynamo `blob` pk kind).
    const BLOB_TABLE: BlobTableId = BlobTableId::new("ingest_snapshot");

    pub fn new(meta: M, blob: B) -> Self {
        let blobs = blob.table(Self::BLOB_TABLE);
        Self {
            meta,
            blobs,
            payloads_enabled: true,
        }
    }

    /// Snapshot store for a blob-less run (`[store.blob]` omitted): the
    /// producer never emits checkpoints, and recovery handles a leftover
    /// manifest from a blob-backed past without touching the blob store.
    pub fn without_payloads(meta: M, blob: B) -> Self {
        let blobs = blob.table(Self::BLOB_TABLE);
        Self {
            meta,
            blobs,
            payloads_enabled: false,
        }
    }

    /// True when a snapshot row exists. Used by the `unsafe_seed_begin` guard,
    /// which must treat an already-seeded store as initialized.
    // Only reached via the configured (dynamo-backed) ingest entry point.
    #[cfg(feature = "dynamo")]
    pub(crate) async fn is_initialized(&self) -> Result<bool> {
        Ok(self
            .meta
            .get(Self::MANIFEST_TABLE, Self::MANIFEST_KEY)
            .await?
            .is_some())
    }

    /// Manifest row for RECOVERY decisions: a present row that fails to decode
    /// is a HARD error (corruption deserves a loud failure, not a silent
    /// rebuild); absence is a fresh/never-checkpointed store.
    async fn load_manifest_strict(&self) -> Result<Option<SnapshotManifest>> {
        match self
            .meta
            .get(Self::MANIFEST_TABLE, Self::MANIFEST_KEY)
            .await?
        {
            None => Ok(None),
            Some(row) => {
                SnapshotManifest::decode(&row)
                    .map(Some)
                    .ok_or(MonadChainDataError::Decode(
                        "ingest snapshot manifest row failed to decode",
                    ))
            }
        }
    }

    // Non-manifest bytes decode to `None`; `store` then overwrites the row with
    // a fresh manifest. Unlike `load`, this is not an error here — `store` does
    // not depend on the prior row being intact.
    async fn load_manifest(&self) -> Result<Option<SnapshotManifest>> {
        Ok(self
            .meta
            .get(Self::MANIFEST_TABLE, Self::MANIFEST_KEY)
            .await?
            .and_then(|bytes| SnapshotManifest::decode(&bytes)))
    }

    /// Loads the latest snapshot payload via the manifest. No row ⇒ `None`
    /// (fresh store; recovery rebuilds from fragments). A present row that fails
    /// to decode as a manifest, or a parseable manifest whose blob is missing or
    /// fails length/digest verification, is a HARD error: a corrupt snapshot
    /// store deserves a loud failure, not a silent rebuild.
    pub(crate) async fn load(&self) -> Result<Option<Bytes>> {
        let Some(manifest) = self.load_manifest_strict().await? else {
            return Ok(None);
        };
        let key = generation_key(manifest.generation);
        let Some(payload) = self.blobs.get(&key).await? else {
            return Err(MonadChainDataError::Backend(format!(
                "ingest snapshot manifest points at generation {} but its payload blob is missing",
                manifest.generation
            )));
        };
        if payload.len() as u64 != manifest.payload_len {
            return Err(MonadChainDataError::Backend(format!(
                "ingest snapshot generation {} payload length mismatch: manifest says {}, blob \
                 has {}",
                manifest.generation,
                manifest.payload_len,
                payload.len()
            )));
        }
        if *blake3::hash(&payload).as_bytes() != manifest.digest {
            return Err(MonadChainDataError::Backend(format!(
                "ingest snapshot generation {} payload digest mismatch",
                manifest.generation
            )));
        }
        Ok(Some(payload))
    }

    /// Persists one snapshot generation: blob write, then the manifest put as
    /// the commit point, then best-effort GC of the previous-previous
    /// generation's blob (keep current + previous).
    pub(crate) async fn store(&self, generation: u64, payload: Bytes) -> Result<()> {
        let prev = self.load_manifest().await?;
        if let Some(prev) = &prev {
            if generation <= prev.generation {
                // Equal: warm restart re-checkpointing its resume block — the
                // committed snapshot already covers it (see type docs). Lower
                // would mean the generation invariant broke; never regress the
                // manifest.
                if generation < prev.generation {
                    warn!(
                        generation,
                        committed = prev.generation,
                        "ingest snapshot generation regressed; keeping the committed snapshot"
                    );
                }
                return Ok(());
            }
        }
        let manifest = SnapshotManifest {
            generation,
            payload_len: payload.len() as u64,
            digest: *blake3::hash(&payload).as_bytes(),
            previous: prev.map(|m| m.generation),
        };
        // Delete the target key before writing. A crash-orphan blob left at
        // THIS generation (the blob→manifest window, then a restart that
        // re-checkpoints the same resume block) makes the put an overwrite, and
        // a shorter multi-chunk payload would otherwise leave stale tail chunks
        // that `DynamoBlobStore::get_blob` concatenates onto the new payload →
        // permanent length/digest mismatch on every `load`. Delete-before-put
        // is crash-safe (a crash between delete and put leaves the manifest
        // still pointing at the old, intact generation) and a no-op on a fresh
        // key.
        self.blobs.delete(&generation_key(generation)).await?;
        self.blobs.put(&generation_key(generation), payload).await?;
        // Commit point: one small put, atomic at the row level.
        self.meta
            .put(Self::MANIFEST_TABLE, Self::MANIFEST_KEY, manifest.encode())
            .await?;
        // `generation > prev.generation > prev.previous`, so the stale blob can
        // never be one of the two we keep. Best-effort only: a crash between the
        // manifest put and this delete, or an orphan blob at a generation the
        // restart never re-checkpoints (cadence/config changed), leaks that blob
        // off the manifest chain forever. Acceptable under the spec's
        // best-effort GC contract; the big-endian generation keying would
        // support a range sweep below `previous` if the blob store ever grows a
        // list/scan op.
        if let Some(stale) = prev.and_then(|m| m.previous) {
            if let Err(error) = self.blobs.delete(&generation_key(stale)).await {
                warn!(
                    %error,
                    generation = stale,
                    "best-effort GC of a stale ingest snapshot blob failed"
                );
            }
        }
        Ok(())
    }
}

/// Persist the full open working set (`OpenState` AND `OpenTail`) plus the
/// resume block and id frontier. Snapshotting the tail too is what lets a
/// checkpoint NOT flush fragments or advance the head.
pub(crate) async fn persist_snapshot<M: MetaStore, B: BlobStore>(
    snapshots: &SnapshotStore<M, B>,
    state: &OpenState,
    tail: &OpenTail,
    block: u64,
    frontier: FamilyFrontier,
) -> Result<()> {
    snapshots
        .store(block, encode_snapshot(state, tail, block, frontier))
        .await
}

/// Seed a fresh store's recovery snapshot (empty working set, resume
/// `block = begin - 1`) so the next run warm-resumes at `begin` instead of
/// genesis. UNSAFE: the store has no data below `begin`, violating the
/// gap-free-up-to-head invariant — tests/fixtures only. Callers must reject
/// `begin == 0` and stores that already have a snapshot.
// Only reached via the configured (dynamo-backed) ingest entry point.
#[cfg(feature = "dynamo")]
pub(crate) async fn seed_snapshot_at<M: MetaStore, B: BlobStore>(
    snapshots: &SnapshotStore<M, B>,
    begin: u64,
) -> Result<()> {
    debug_assert!(
        begin >= 1,
        "seed begin must be >= 1 (0 is the normal cold start)"
    );
    persist_snapshot(
        snapshots,
        &OpenState::default(),
        &OpenTail::default(),
        begin - 1,
        FamilyFrontier::default(),
    )
    .await
}

/// Restore the open working set from the latest checkpoint snapshot, or `None`
/// when no snapshot exists. `last_block` is the highest block it captured.
///
/// A wrong version byte or any other decode failure is a HARD error: the
/// manifest already verified the payload's digest, so a snapshot the current
/// codec cannot read is corruption or a bug, never a tolerated rebuild path.
///
/// Blob-less stores cannot read payloads, so a manifest left behind by a
/// blob-backed past is ignored when `head` already covers its generation
/// (recovery would rebuild from fragments at `head` regardless) and is an
/// actionable error otherwise — dropping `[store.blob]` from a config must
/// not wedge recovery behind an opaque blob failure.
pub(crate) async fn recover_checkpoint<M: MetaStore, B: BlobStore>(
    snapshots: &SnapshotStore<M, B>,
    head: u64,
) -> Result<Option<(OpenState, OpenTail, FamilyFrontier, u64)>> {
    if !snapshots.payloads_enabled {
        return match snapshots.load_manifest_strict().await? {
            None => Ok(None),
            Some(manifest) if manifest.generation <= head => {
                tracing::info!(
                    generation = manifest.generation,
                    head,
                    "ignoring checkpoint from a previous blob-backed run; the published head \
                     covers it"
                );
                Ok(None)
            }
            Some(manifest) => Err(MonadChainDataError::Backend(format!(
                "checkpoint generation {} is ahead of the published head {head} but its payload \
                 lives in a blob store this config no longer has; restore [store.blob] for one \
                 run (or reset the store) before going blob-less",
                manifest.generation
            ))),
        };
    }
    let Some(payload) = snapshots.load().await? else {
        return Ok(None);
    };
    decode_snapshot(&payload).map(Some)
}

// Snapshot codec: compact versioned binary. `open_streams_seen` is
// intentionally NOT persisted — it restores empty, so the first flush re-emits
// the open page's streams, which is idempotent (the reader unions open_streams
// delta rows; at most a duplicate row, never a lost one).

const SNAPSHOT_VERSION: u8 = 1;

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}

fn put_bitmap(out: &mut Vec<u8>, bm: &RoaringBitmap) {
    let mut buf = Vec::with_capacity(bm.serialized_size());
    bm.serialize_into(&mut buf)
        .expect("RoaringBitmap::serialize_into a Vec is infallible");
    put_u32(out, buf.len() as u32);
    out.extend_from_slice(&buf);
}

fn put_pages(out: &mut Vec<u8>, pages: &HashMap<(StreamKey, u64), RoaringBitmap>) {
    put_u32(out, pages.len() as u32);
    for ((stream, page_start), bm) in pages {
        // Rendered exactly as `render_stream_id` writes it, so the persisted
        // format is unchanged by the compact in-memory key.
        put_str(out, &stream.render());
        put_u64(out, *page_start);
        put_bitmap(out, bm);
    }
}

fn put_dir<'a>(out: &mut Vec<u8>, dir: impl ExactSizeIterator<Item = &'a (u64, u64, u64)>) {
    put_u32(out, dir.len() as u32);
    for &(block, first, end) in dir {
        put_u64(out, block);
        put_u64(out, first);
        put_u64(out, end);
    }
}

fn put_page_counts(out: &mut Vec<u8>, counts: &SealedPageCounts) {
    put_u32(out, counts.len() as u32);
    for ((stream, group_start), pairs) in counts {
        put_str(out, &stream.render());
        put_u64(out, *group_start);
        put_u32(out, pairs.len() as u32);
        for &(page_start_in_group, count) in pairs {
            put_u32(out, page_start_in_group);
            put_u32(out, count);
        }
    }
}

fn encode_snapshot(
    state: &OpenState,
    tail: &OpenTail,
    block: u64,
    frontier: FamilyFrontier,
) -> Bytes {
    let mut out = Vec::new();
    out.push(SNAPSHOT_VERSION);
    put_u64(&mut out, block);
    put_u64(&mut out, frontier.log);
    put_u64(&mut out, frontier.tx);
    put_u64(&mut out, frontier.trace);
    // `PerFamily::iter` is fixed log, tx, trace order — the codec's field order.
    for (_, fs) in state.iter() {
        put_u64(&mut out, fs.sealed_id);
        out.extend_from_slice(fs.seal_chain.as_slice());
        put_pages(&mut out, &fs.pages);
        put_dir(&mut out, fs.dir.iter());
        put_page_counts(&mut out, &fs.sealed_page_counts);
    }
    for (_, ft) in tail.iter() {
        put_pages(&mut out, &ft.pages);
        put_dir(&mut out, ft.dir.iter());
    }
    Bytes::from(out)
}

fn take<'a>(cur: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    let bytes = cur
        .get(..n)
        .ok_or(MonadChainDataError::Decode("snapshot truncated"))?;
    *cur = &cur[n..];
    Ok(bytes)
}

fn take_u32(cur: &mut &[u8]) -> Result<u32> {
    Ok(u32::from_be_bytes(
        take(cur, 4)?.try_into().expect("4 bytes"),
    ))
}

fn take_u64(cur: &mut &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(
        take(cur, 8)?.try_into().expect("8 bytes"),
    ))
}

fn take_digest(cur: &mut &[u8]) -> Result<ChainDigest> {
    let raw: [u8; 32] = take(cur, 32)?.try_into().expect("32 bytes");
    Ok(ChainDigest::from(raw))
}

fn take_str(cur: &mut &[u8]) -> Result<String> {
    let len = take_u32(cur)? as usize;
    let bytes = take(cur, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| MonadChainDataError::Decode("snapshot utf8"))
}

fn take_bitmap(cur: &mut &[u8]) -> Result<RoaringBitmap> {
    let len = take_u32(cur)? as usize;
    let bytes = take(cur, len)?;
    RoaringBitmap::deserialize_from(bytes)
        .map_err(|_| MonadChainDataError::Decode("snapshot bitmap"))
}

fn take_pages(cur: &mut &[u8]) -> Result<HashMap<(StreamKey, u64), RoaringBitmap>> {
    let count = take_u32(cur)? as usize;
    let mut pages = HashMap::with_capacity(count);
    for _ in 0..count {
        let stream = StreamKey::parse(&take_str(cur)?)?;
        let page_start = take_u64(cur)?;
        let bitmap = take_bitmap(cur)?;
        pages.insert((stream, page_start), bitmap);
    }
    Ok(pages)
}

fn take_dir(cur: &mut &[u8]) -> Result<Vec<(u64, u64, u64)>> {
    let count = take_u32(cur)? as usize;
    let mut dir = Vec::with_capacity(count);
    for _ in 0..count {
        dir.push((take_u64(cur)?, take_u64(cur)?, take_u64(cur)?));
    }
    Ok(dir)
}

fn take_page_counts(cur: &mut &[u8]) -> Result<SealedPageCounts> {
    let entries = take_u32(cur)? as usize;
    let mut counts = BTreeMap::new();
    for _ in 0..entries {
        let stream = StreamKey::parse(&take_str(cur)?)?;
        let group_start = take_u64(cur)?;
        let len = take_u32(cur)? as usize;
        let mut pairs = Vec::with_capacity(len);
        for _ in 0..len {
            pairs.push((take_u32(cur)?, take_u32(cur)?));
        }
        counts.insert((stream, group_start), pairs);
    }
    Ok(counts)
}

fn decode_snapshot(bytes: &[u8]) -> Result<(OpenState, OpenTail, FamilyFrontier, u64)> {
    let mut cur = bytes;
    if take(&mut cur, 1)?[0] != SNAPSHOT_VERSION {
        return Err(MonadChainDataError::Decode("snapshot version"));
    }
    let block = take_u64(&mut cur)?;
    let frontier = FamilyFrontier {
        log: take_u64(&mut cur)?,
        tx: take_u64(&mut cur)?,
        trace: take_u64(&mut cur)?,
    };
    let mut state = OpenState::default();
    for (_, fs) in state.iter_mut() {
        fs.sealed_id = take_u64(&mut cur)?;
        fs.seal_chain = take_digest(&mut cur)?;
        fs.pages = take_pages(&mut cur)?;
        fs.dir = VecDeque::from(take_dir(&mut cur)?);
        fs.sealed_page_counts = take_page_counts(&mut cur)?;
        // open_streams_seen stays empty (default) — see codec note above.
    }
    let mut tail = OpenTail::default();
    for (_, ft) in tail.iter_mut() {
        ft.pages = take_pages(&mut cur)?;
        ft.dir = take_dir(&mut cur)?;
    }
    Ok((state, tail, frontier, block))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{InMemoryBlobStore, InMemoryMetaStore};

    type TestSnapshots = SnapshotStore<InMemoryMetaStore, InMemoryBlobStore>;

    fn fixture() -> (InMemoryMetaStore, InMemoryBlobStore, TestSnapshots) {
        let meta = InMemoryMetaStore::default();
        let blob = InMemoryBlobStore::default();
        let snapshots = SnapshotStore::new(meta.clone(), blob.clone());
        (meta, blob, snapshots)
    }

    fn payload(byte: u8, len: usize) -> Bytes {
        Bytes::from(vec![byte; len])
    }

    fn blob_keys(blob: &InMemoryBlobStore) -> Vec<Vec<u8>> {
        blob.blob_snapshot()
            .into_keys()
            .filter(|(table, _)| *table == TestSnapshots::BLOB_TABLE)
            .map(|(_, key)| key)
            .collect()
    }

    #[test]
    fn manifest_codec_round_trips() {
        for previous in [None, Some(41)] {
            let manifest = SnapshotManifest {
                generation: 42,
                payload_len: 7,
                digest: [0xd1; 32],
                previous,
            };
            let bytes = manifest.encode();
            assert_eq!(bytes.len(), MANIFEST_LEN);
            assert_eq!(SnapshotManifest::decode(&bytes), Some(manifest));
        }
        // Non-manifest bytes (wrong magic/length) decode to None.
        assert_eq!(SnapshotManifest::decode(&[SNAPSHOT_VERSION; 100]), None);
        assert_eq!(SnapshotManifest::decode(b""), None);
    }

    #[tokio::test]
    async fn blob_manifest_round_trip() {
        let (_, blob, snapshots) = fixture();
        assert_eq!(snapshots.load().await.unwrap(), None);
        assert!(!snapshots.is_initialized().await.unwrap());

        let bytes = encode_snapshot(
            &OpenState::default(),
            &OpenTail::default(),
            7,
            FamilyFrontier::default(),
        );
        snapshots.store(7, bytes.clone()).await.unwrap();

        assert!(snapshots.is_initialized().await.unwrap());
        assert_eq!(snapshots.load().await.unwrap(), Some(bytes));
        let (_, _, _, block) = recover_checkpoint(&snapshots, 0)
            .await
            .unwrap()
            .expect("snapshot present");
        assert_eq!(block, 7);
        assert_eq!(blob_keys(&blob), vec![generation_key(7).to_vec()]);
    }

    #[tokio::test]
    async fn crash_between_blob_and_manifest_keeps_prior_generation() {
        let (_, blob, snapshots) = fixture();
        let committed = payload(0xaa, 64);
        snapshots.store(1, committed.clone()).await.unwrap();

        // Simulate a crash AFTER the generation-2 blob write but BEFORE the
        // manifest put: the orphan blob exists, the manifest still points at
        // generation 1, and the loader must serve generation 1.
        blob.put_blob(
            TestSnapshots::BLOB_TABLE,
            &generation_key(2),
            payload(0xbb, 64),
        )
        .await
        .unwrap();
        assert_eq!(snapshots.load().await.unwrap(), Some(committed));
    }

    #[tokio::test]
    async fn corrupt_payload_is_a_loud_error_not_a_rebuild() {
        let (_, blob, snapshots) = fixture();
        snapshots.store(3, payload(0xcc, 64)).await.unwrap();

        // Same length, different content => digest mismatch.
        blob.put_blob(
            TestSnapshots::BLOB_TABLE,
            &generation_key(3),
            payload(0xdd, 64),
        )
        .await
        .unwrap();
        assert!(snapshots.load().await.is_err());

        // Different length => length mismatch.
        blob.put_blob(
            TestSnapshots::BLOB_TABLE,
            &generation_key(3),
            payload(0xcc, 8),
        )
        .await
        .unwrap();
        assert!(snapshots.load().await.is_err());

        // Manifest pointing at a missing blob is equally loud.
        blob.delete_blob(TestSnapshots::BLOB_TABLE, &generation_key(3))
            .await
            .unwrap();
        assert!(snapshots.load().await.is_err());
    }

    #[tokio::test]
    async fn gc_keeps_exactly_current_and_previous() {
        let (_, blob, snapshots) = fixture();
        for generation in 1..=4 {
            snapshots
                .store(generation, payload(generation as u8, 32))
                .await
                .unwrap();
            let mut keys = blob_keys(&blob);
            keys.sort();
            let expected: Vec<Vec<u8>> = (generation.saturating_sub(1).max(1)..=generation)
                .map(|g| generation_key(g).to_vec())
                .collect();
            assert_eq!(keys, expected, "after generation {generation}");
        }
        // The latest generation is still loadable after GC.
        assert_eq!(snapshots.load().await.unwrap(), Some(payload(4, 32)));
    }

    #[tokio::test]
    async fn non_manifest_row_is_a_load_error_but_still_initialized() {
        let (meta, _, snapshots) = fixture();
        // A present meta row that is not a manifest is corruption, not a fresh
        // store: `load` must fail loudly rather than silently rebuild.
        let garbage = encode_snapshot(
            &OpenState::default(),
            &OpenTail::default(),
            9,
            FamilyFrontier::default(),
        );
        meta.put(
            TestSnapshots::MANIFEST_TABLE,
            TestSnapshots::MANIFEST_KEY,
            garbage,
        )
        .await
        .unwrap();

        assert!(snapshots.load().await.is_err());
        assert!(recover_checkpoint(&snapshots, 0).await.is_err());
        // The unsafe_seed_begin guard treats any present row as initialized.
        assert!(snapshots.is_initialized().await.unwrap());
    }

    #[tokio::test]
    async fn wrong_codec_version_payload_is_a_hard_error() {
        let (_, _, snapshots) = fixture();
        // An intact blob/manifest pair whose payload the current codec cannot
        // read (wrong version byte) is corruption or a bug: hard error, never a
        // tolerated rebuild.
        let mut wrong = encode_snapshot(
            &OpenState::default(),
            &OpenTail::default(),
            9,
            FamilyFrontier::default(),
        )
        .to_vec();
        wrong[0] = SNAPSHOT_VERSION.wrapping_add(1);
        snapshots.store(9, Bytes::from(wrong)).await.unwrap();

        assert!(recover_checkpoint(&snapshots, 0).await.is_err());
        // Decode within the CURRENT version stays strict.
        let current = encode_snapshot(
            &OpenState::default(),
            &OpenTail::default(),
            9,
            FamilyFrontier::default(),
        );
        assert!(decode_snapshot(&current[..current.len() - 1]).is_err());
    }

    #[tokio::test]
    async fn equal_or_lower_generation_keeps_committed_snapshot() {
        let (_, blob, snapshots) = fixture();
        let committed = payload(0x11, 16);
        snapshots.store(5, committed.clone()).await.unwrap();

        // A warm restart that re-checkpoints its resume block must not rewrite
        // the committed blob in place (no atomic blob+manifest pair).
        snapshots.store(5, payload(0x22, 16)).await.unwrap();
        assert_eq!(snapshots.load().await.unwrap(), Some(committed.clone()));

        // A regressed generation never moves the manifest backwards.
        snapshots.store(3, payload(0x33, 16)).await.unwrap();
        assert_eq!(snapshots.load().await.unwrap(), Some(committed));
        assert_eq!(blob_keys(&blob), vec![generation_key(5).to_vec()]);
    }

    /// Dropping `[store.blob]` from a previously blob-backed store leaves the
    /// checkpoint manifest behind in meta. Recovery must not blindly chase its
    /// payload into the (now absent) blob store: a head at/past the generation
    /// supersedes it; a head behind it is an actionable error.
    #[tokio::test]
    async fn blobless_recovery_handles_leftover_manifest() {
        let (meta, _, snapshots) = fixture();
        snapshots.store(5, payload(0x11, 16)).await.unwrap();

        let migrated =
            SnapshotStore::without_payloads(meta.clone(), crate::store::NullBlobStore::default());
        // Head covers the generation: ignored, fragments rebuild takes over.
        assert!(recover_checkpoint(&migrated, 5).await.unwrap().is_none());
        assert!(recover_checkpoint(&migrated, 9).await.unwrap().is_none());
        // Head behind the generation: the payload would be required, and the
        // error must say how to get unstuck rather than surface a blob miss.
        let Err(err) = recover_checkpoint(&migrated, 3).await else {
            panic!("head behind the leftover generation must error");
        };
        assert!(err.to_string().contains("[store.blob]"), "{err}");

        // A fresh blob-less store (no manifest) recovers as before.
        let fresh = SnapshotStore::without_payloads(
            InMemoryMetaStore::default(),
            crate::store::NullBlobStore::default(),
        );
        assert!(recover_checkpoint(&fresh, 0).await.unwrap().is_none());
    }

    #[test]
    fn snapshot_round_trips() {
        let addr_key =
            StreamKey::parse(&format!("addr/{}", "aa".repeat(20))).expect("addr stream key");
        let from_key =
            StreamKey::parse(&format!("from/{}", "bb".repeat(20))).expect("from stream key");

        let mut state = OpenState::default();
        state.log.sealed_id = 65_536;
        state.log.seal_chain = ChainDigest::repeat_byte(0x5c);
        let mut bm = RoaringBitmap::new();
        bm.insert(5);
        bm.insert(100);
        bm.insert(40_000);
        state.log.pages.insert((addr_key, 0), bm.clone());
        state.log.dir.push_back((10, 0, 50));
        // The open group's sealed-page-count accumulator rides the snapshot.
        state
            .log
            .sealed_page_counts
            .insert((addr_key, 0), vec![(0, 3), (65_536, 9)]);
        // A non-empty seen set must NOT survive (intentionally not persisted).
        state.log.open_streams_seen.insert(addr_key);
        state.tx.dir.push_back((10, 0, 3));

        let mut tail = OpenTail::default();
        let mut bm2 = RoaringBitmap::new();
        bm2.insert(1);
        bm2.insert(2);
        tail.trace.pages.insert((from_key, 65_536), bm2.clone());
        tail.trace.dir.push((11, 50, 60));

        let frontier = FamilyFrontier {
            log: 100,
            tx: 3,
            trace: 60,
        };

        let bytes = encode_snapshot(&state, &tail, 11, frontier);
        let (s2, t2, f2, block) = decode_snapshot(&bytes).expect("decode");

        assert_eq!(block, 11);
        assert_eq!((f2.log, f2.tx, f2.trace), (100, 3, 60));
        assert_eq!(s2.log.sealed_id, 65_536);
        assert_eq!(s2.log.seal_chain, ChainDigest::repeat_byte(0x5c));
        assert_eq!(s2.tx.seal_chain, ChainDigest::ZERO);
        assert_eq!(s2.log.pages.get(&(addr_key, 0)), Some(&bm));
        assert_eq!(Vec::from(s2.log.dir.clone()), vec![(10, 0, 50)]);
        assert_eq!(
            s2.log.sealed_page_counts.get(&(addr_key, 0)),
            Some(&vec![(0, 3), (65_536, 9)])
        );
        assert!(s2.tx.sealed_page_counts.is_empty());
        assert_eq!(Vec::from(s2.tx.dir.clone()), vec![(10, 0, 3)]);
        assert_eq!(t2.trace.pages.get(&(from_key, 65_536)), Some(&bm2));
        assert_eq!(t2.trace.dir, vec![(11, 50, 60)]);
        // open_streams_seen is recovered empty (idempotent re-emit on resume).
        assert!(s2.log.open_streams_seen.is_empty());
    }

    #[test]
    fn snapshot_empty_round_trips() {
        let bytes = encode_snapshot(
            &OpenState::default(),
            &OpenTail::default(),
            0,
            FamilyFrontier::default(),
        );
        let (s, t, f, block) = decode_snapshot(&bytes).expect("decode");
        assert_eq!(block, 0);
        assert_eq!((f.log, f.tx, f.trace), (0, 0, 0));
        assert!(s.log.pages.is_empty() && s.log.dir.is_empty() && s.log.sealed_id == 0);
        assert!(t.trace.pages.is_empty() && t.trace.dir.is_empty());
    }

    #[test]
    fn snapshot_rejects_truncation_and_bad_version() {
        let bytes = encode_snapshot(
            &OpenState::default(),
            &OpenTail::default(),
            7,
            FamilyFrontier::default(),
        );
        assert!(decode_snapshot(&bytes[..bytes.len() - 1]).is_err());
        let mut bad = bytes.to_vec();
        bad[0] = 0xff;
        assert!(decode_snapshot(&bad).is_err());
    }
}
