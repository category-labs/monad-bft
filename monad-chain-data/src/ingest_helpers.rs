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

//! Leaf helpers for the ingest engine — the bottom-of-stack functions split out
//! of [`crate::ingest_core`] so the orchestration there stays readable. These do
//! store I/O, row/blob encoding, snapshot encode/decode, and recovery.

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::{
    engine::{
        bitmap::{encode_open_streams_delta, OPEN_STREAMS_DELTA_TARGET_BYTES},
        digest::EMPTY_CHECKSUM,
        tables::Tables,
    },
    error::{MonadChainDataError, Result},
    ingest_core::{
        ArtifactWrite, AssignedBlock, Codecs, FamilyFrontier, OpenState, OpenTail, PackEntry,
        SnapshotStore,
    },
    logs::LogIngestPlan,
    primitives::state::{BlockRecord, FamilyWindowRecord, PrimaryId},
    store::{BlobStore, MetaStore, WriteSession},
    traces::encode_block_traces,
    txs::TxIngestPlan,
};

// ---------------------------------------------------------------------------
// Artifact / snapshot / publication I/O
// ---------------------------------------------------------------------------

/// Stage one artifact (page artifact, dir bucket, fragment, or open-streams
/// inventory) into the write session, reusing the engine's own `stage_*` methods
/// so the on-disk key derivation and encoding match the reader exactly. The
/// caller (`IndexEngine::write_all`) batches a burst into one `with_writes`.
pub(crate) fn stage_artifact_write<M, B>(
    w: &mut WriteSession<'_, M, B>,
    write: ArtifactWrite,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
{
    let tables = w.tables();
    match write {
        ArtifactWrite::Page {
            family,
            stream_id,
            page_start_local,
            artifact,
        } => tables.family(family).bitmap().stage_page_artifact(
            w,
            &stream_id,
            page_start_local,
            &artifact,
        ),
        ArtifactWrite::Bucket {
            family,
            bucket_start,
            bucket,
        } => tables
            .family(family)
            .dir()
            .stage_bucket(w, bucket_start, &bucket),
        ArtifactWrite::PageFragment {
            family,
            stream_id,
            page_start_local,
            marker,
            blob,
        } => tables.family(family).bitmap().stage_page_fragment(
            w,
            &stream_id,
            page_start_local,
            marker,
            blob,
        ),
        ArtifactWrite::DirFragment { family, fragment } => {
            let count =
                u32::try_from(fragment.end_primary_id_exclusive - fragment.first_primary_id)
                    .map_err(|_| {
                        MonadChainDataError::Decode("primary directory fragment count exceeds u32")
                    })?;
            tables.family(family).dir().stage_block_fragment(
                w,
                fragment.block_number,
                fragment.first_primary_id,
                count,
            )
        }
        ArtifactWrite::OpenStreams {
            family,
            global_page_start,
            marker,
            streams,
        } => {
            // Split the batch's first-seen streams into chunks that stay within
            // the row-size target, one delta row per chunk keyed by (marker,
            // chunk). We check *before* pushing (and count the 5-byte
            // version+count header) so a flushed chunk never exceeds the target —
            // except a single id larger than the whole target, which can only go
            // in a row of its own.
            const HEADER_BYTES: usize = 5; // version (1) + count (4)
            let bitmap = tables.family(family).bitmap();
            let mut chunk: Vec<String> = Vec::new();
            let mut chunk_bytes = HEADER_BYTES;
            let mut chunk_idx = 0u32;
            for stream_id in streams {
                let entry_bytes = stream_id.len() + 4; // len prefix + utf8
                if !chunk.is_empty() && chunk_bytes + entry_bytes > OPEN_STREAMS_DELTA_TARGET_BYTES
                {
                    bitmap.stage_open_streams_delta(
                        w,
                        global_page_start,
                        marker,
                        chunk_idx,
                        encode_open_streams_delta(&chunk),
                    );
                    chunk.clear();
                    chunk_bytes = HEADER_BYTES;
                    chunk_idx += 1;
                }
                chunk_bytes += entry_bytes;
                chunk.push(stream_id);
            }
            if !chunk.is_empty() {
                bitmap.stage_open_streams_delta(
                    w,
                    global_page_start,
                    marker,
                    chunk_idx,
                    encode_open_streams_delta(&chunk),
                );
            }
        }
    }
    Ok(())
}

/// Persist the full open working set — `OpenState` (carry-over) AND `OpenTail`
/// (un-flushed delta) — plus the resume block and id frontier, as a recovery
/// checkpoint. Snapshotting the tail too is what lets a checkpoint NOT flush
/// fragments or advance the head: resume restores both exactly.
pub(crate) async fn persist_snapshot<M: MetaStore>(
    snapshots: &SnapshotStore<M>,
    state: &OpenState,
    tail: &OpenTail,
    block: u64,
    frontier: FamilyFrontier,
) -> Result<()> {
    snapshots
        .store(encode_snapshot(state, tail, block, frontier))
        .await
}

/// Seed a *fresh* store's recovery snapshot at an explicit begin block so the
/// next ingest run starts at `begin` instead of genesis. Writes an empty working
/// set (`OpenState`/`OpenTail`/`FamilyFrontier` defaults) with resume
/// `block = begin - 1`, so the controller's recovery path treats it as an
/// ordinary warm resume and starts at `begin`.
///
/// UNSAFE: the resulting store has no data for blocks below `begin`, violating
/// the gap-free-up-to-head invariant the query layer assumes — for tests and
/// fixtures only. Callers must reject `begin == 0` and refuse to seed a store
/// that already has a snapshot (see `run_engine_with_store`).
pub(crate) async fn seed_snapshot_at<M: MetaStore>(
    snapshots: &SnapshotStore<M>,
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

/// Restore the open working set from the latest checkpoint snapshot, returning
/// `Some((OpenState, OpenTail, frontier, last_block))` or `None` when no snapshot
/// exists. `last_block` is the highest block the snapshot captured. This is the
/// checkpoint half of recovery; [`crate::ingest_recover`] decides between it and
/// a fragment-based rebuild.
pub(crate) async fn recover_checkpoint<M: MetaStore>(
    snapshots: &SnapshotStore<M>,
) -> Result<Option<(OpenState, OpenTail, FamilyFrontier, u64)>> {
    match snapshots.load().await? {
        Some(bytes) => Ok(Some(decode_snapshot(&bytes)?)),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Snapshot codec
// ---------------------------------------------------------------------------
//
// A compact versioned binary encoding. `open_streams_seen` is intentionally NOT
// persisted: on resume it restores empty, so the first flush re-emits the open
// page's streams under a fresh marker — idempotent, since the reader unions
// open_streams delta rows (at most a duplicate row, never a lost one).

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

fn put_pages(out: &mut Vec<u8>, pages: &HashMap<(String, u64), RoaringBitmap>) {
    put_u32(out, pages.len() as u32);
    for ((stream, page_global), bm) in pages {
        put_str(out, stream);
        put_u64(out, *page_global);
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
    for fs in [&state.log, &state.tx, &state.trace] {
        put_u64(&mut out, fs.sealed_id);
        put_pages(&mut out, &fs.pages);
        put_dir(&mut out, fs.dir.iter());
    }
    for ft in [&tail.log, &tail.tx, &tail.trace] {
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

fn take_pages(cur: &mut &[u8]) -> Result<HashMap<(String, u64), RoaringBitmap>> {
    let count = take_u32(cur)? as usize;
    let mut pages = HashMap::with_capacity(count);
    for _ in 0..count {
        let stream = take_str(cur)?;
        let page_global = take_u64(cur)?;
        let bitmap = take_bitmap(cur)?;
        pages.insert((stream, page_global), bitmap);
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
    for fs in [&mut state.log, &mut state.tx, &mut state.trace] {
        fs.sealed_id = take_u64(&mut cur)?;
        fs.pages = take_pages(&mut cur)?;
        fs.dir = VecDeque::from(take_dir(&mut cur)?);
        // open_streams_seen stays empty (default) — see codec note above.
    }
    let mut tail = OpenTail::default();
    for ft in [&mut tail.log, &mut tail.tx, &mut tail.trace] {
        ft.pages = take_pages(&mut cur)?;
        ft.dir = take_dir(&mut cur)?;
    }
    Ok((state, tail, frontier, block))
}

// ---------------------------------------------------------------------------
// Row/blob encoding + write (data track)
// ---------------------------------------------------------------------------

/// Compress one block's rows into its combined blob + per-family headers. Reuses
/// the per-family `encode_block_*` framing (shared with the legacy path); the
/// data track owns *only* row materialization, so the bitmap/dir indexes those
/// builders also produce are derived separately by the index track.
///
/// The family blobs are concatenated `log ++ tx ++ trace` into one per-block
/// blob; each header's `base_offset` is its region's start within it. The
/// `physical_key` / `physical_base_offset` are left empty here — the store's
/// block-blob coalescer stamps them at flush. Family windows come straight from
/// the producer's assigned id ranges. The block's `artifact_checksum` is left
/// `EMPTY_CHECKSUM`: the real value chains the data `rows_digest` AND the index
/// `bitmap_fragments` (a cross-track fold), so the digest chain is deferred with
/// the rest of the checksum work.
pub(crate) fn encode_pack_entry(b: &AssignedBlock, codecs: &Codecs) -> Result<PackEntry> {
    let logs = LogIngestPlan::flatten_logs(&b.block)?;
    let (mut log_header, log_blob, _log_digest) =
        LogIngestPlan::encode_block_logs(&logs, &codecs.log)?;
    let (mut tx_header, tx_blob, _tx_digest) =
        TxIngestPlan::encode_block_txs(&b.block.txs, &codecs.tx)?;
    let (mut trace_header, trace_blob, _trace_digest) =
        encode_block_traces(&b.block.traces, &codecs.trace)?;

    let log_len = u32::try_from(log_blob.len())
        .map_err(|_| MonadChainDataError::Decode("block log blob too large"))?;
    let tx_len = u32::try_from(tx_blob.len())
        .map_err(|_| MonadChainDataError::Decode("block tx blob too large"))?;
    let trace_base = log_len
        .checked_add(tx_len)
        .ok_or(MonadChainDataError::Decode(
            "combined block blob base offset overflow",
        ))?;
    log_header.base_offset = 0;
    tx_header.base_offset = log_len;
    trace_header.base_offset = trace_base;

    let mut combined_blob = log_blob;
    combined_blob.extend_from_slice(&tx_blob);
    combined_blob.extend_from_slice(&trace_blob);

    let record = BlockRecord {
        block_number: b.number,
        block_hash: b.block.block_hash(),
        parent_hash: b.block.parent_hash(),
        logs: FamilyWindowRecord {
            first_primary_id: PrimaryId::from(b.ranges.log_first),
            count: b.ranges.log_count,
        },
        txs: FamilyWindowRecord {
            first_primary_id: PrimaryId::from(b.ranges.tx_first),
            count: b.ranges.tx_count,
        },
        traces: FamilyWindowRecord {
            first_primary_id: PrimaryId::from(b.ranges.trace_first),
            count: b.ranges.trace_count,
        },
        artifact_checksum: EMPTY_CHECKSUM,
    };

    Ok(PackEntry {
        combined_blob,
        log_header,
        tx_header,
        trace_header,
        record,
        evm_header: b.block.header.clone(),
        hash_locations: TxIngestPlan::collect_hash_locations(&b.block)?,
    })
}

/// Stage a flushed pack's per-block persistence artifacts in one write session:
/// the combined row blob, block metadata (record + offset headers + EVM header),
/// the block-hash→number index, and the tx-hash→location index. The store's
/// block-blob coalescer derives the physical object and stamps each header's
/// `physical_key`/`physical_base_offset` at flush; the hash indexes are logical
/// `(block, idx)` pointers needing no stamping. Durability of this session is
/// what lets `flush_pack` advance `data_durable`.
pub(crate) async fn stage_pack<M, B>(tables: &Tables<M, B>, entries: Vec<PackEntry>) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
{
    if entries.is_empty() {
        return Ok(());
    }
    tables
        .with_writes(|w| {
            Box::pin(async move {
                let tables = w.tables();
                let blocks = tables.blocks();
                for entry in entries {
                    let number = entry.record.block_number;
                    blocks.stage_metadata(
                        w,
                        number,
                        &entry.record,
                        &entry.evm_header,
                        Bytes::from(entry.log_header.encode()),
                        Bytes::from(entry.tx_header.encode()),
                        Bytes::from(entry.trace_header.encode()),
                    );
                    tables.stage_block_blob(w, number, entry.combined_blob);
                    blocks.stage_hash_index(w, &entry.record.block_hash, number);
                    for (tx_hash, location) in &entry.hash_locations {
                        tables.tx_hash_index().stage_put(w, tx_hash, *location);
                    }
                }
                Ok(())
            })
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_round_trips() {
        let mut state = OpenState::default();
        state.log.sealed_id = 65_536;
        let mut bm = RoaringBitmap::new();
        bm.insert(5);
        bm.insert(100);
        bm.insert(40_000);
        state
            .log
            .pages
            .insert(("addr/aa/0000000000".to_string(), 0), bm.clone());
        state.log.dir.push_back((10, 0, 50));
        // A non-empty seen set must NOT survive (intentionally not persisted).
        state
            .log
            .open_streams_seen
            .insert("addr/aa/0000000000".to_string());
        state.tx.dir.push_back((10, 0, 3));

        let mut tail = OpenTail::default();
        let mut bm2 = RoaringBitmap::new();
        bm2.insert(1);
        bm2.insert(2);
        tail.trace
            .pages
            .insert(("from/bb/0000000001".to_string(), 65_536), bm2.clone());
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
        assert_eq!(
            s2.log.pages.get(&("addr/aa/0000000000".to_string(), 0)),
            Some(&bm)
        );
        assert_eq!(Vec::from(s2.log.dir.clone()), vec![(10, 0, 50)]);
        assert_eq!(Vec::from(s2.tx.dir.clone()), vec![(10, 0, 3)]);
        assert_eq!(
            t2.trace
                .pages
                .get(&("from/bb/0000000001".to_string(), 65_536)),
            Some(&bm2)
        );
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
