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

//! Crash recovery: resume from `max(checkpoint_block, published_head)`.
//!
//! During backfill flushes are rare, so the checkpoint (which captures both
//! `OpenState` and `OpenTail`) runs ahead of the head and wins. Live at the tip,
//! flushes are frequent and checkpoints rare, so the durable fragments at the
//! head encode the full `OpenState` and win. The rebuild only reads existing
//! artifacts, so it cannot corrupt already-published data.

use std::collections::{HashMap, HashSet, VecDeque};

use futures::{stream, StreamExt};
use roaring::RoaringBitmap;

use crate::{
    engine::{
        bitmap::{page_group_start, page_start_in_group, StreamKey},
        digest::EMPTY_DIGEST,
        family::Family,
        tables::{FamilyTables, Tables},
    },
    error::{MonadChainDataError, Result},
    ingest::{
        index::{
            last_sealed_span, seal_boundary, FamilyState, OpenState, OpenTail, SealedPageCounts,
            PAGE_SPAN,
        },
        snapshot::{recover_checkpoint, SnapshotStore},
        FamilyFrontier,
    },
    primitives::records::FamilyWindowRecord,
    store::{BlobStore, MetaStore},
};

/// Max concurrent per-stream fragment unions while rebuilding one family's
/// open page.
const STREAM_REBUILD_CONCURRENCY: usize = 16;

/// The recovered starting point for `run_ingest`. `Cold` is a fresh store (no
/// checkpoint, nothing published); `Warm` carries the restored open working set
/// plus the resume block.
pub(crate) enum Recovered {
    Cold,
    Warm {
        state: OpenState,
        tail: OpenTail,
        frontier: FamilyFrontier,
        /// Highest block whose ingest is complete (rows durable, so it also
        /// seeds `data_durable`); the engine continues at `resume + 1`.
        resume: u64,
    },
}

/// Decide recovery from `max(checkpoint_block, published_head)`: restore the
/// checkpoint when it is at/ahead of `head` (the authoritative published head),
/// else rebuild from fragments at the head.
///
/// The rebuild path requires `head` to be an index flush boundary, and the
/// publisher guarantees it: `Progress::publishable_head` only ever returns
/// recorded `batch_flush` horizons (or its seed — the prior published head,
/// itself such a boundary), never the data track's raw pack boundary or the
/// checkpoint block. At a flush boundary `H` the rebuild is complete: seals
/// run inline per block, so every sealed span at/below the frontier@`H` has
/// durable artifacts before `H` was recorded, and the open granule's bits and
/// dir entries below frontier@`H` are fully covered by the fragments flushed
/// at `H` — a mid-batch head would instead lose the tail bits/entries that
/// `seal_family` consumed without ever fragmenting. Deterministic replay from
/// `H + 1` then re-seals identical artifacts. The rebuilt tail is empty for
/// the same reason: nothing below `H` is still unflushed.
pub(crate) async fn recover<M, B>(
    tables: &Tables<M, B>,
    snapshots: &SnapshotStore<M, B>,
    head: u64,
) -> Result<Recovered>
where
    M: MetaStore,
    B: BlobStore,
{
    match recover_checkpoint(snapshots, head).await? {
        None if head == 0 => Ok(Recovered::Cold),
        Some((state, tail, frontier, checkpoint)) if checkpoint >= head => Ok(Recovered::Warm {
            state,
            tail,
            frontier,
            resume: checkpoint,
        }),
        // head > checkpoint, or no checkpoint but blocks are published.
        _ => {
            // Trusts the flush-boundary guarantee above; a head published by a
            // build predating it may not satisfy the premise (see the ops
            // runbook's upgrade note).
            tracing::info!(head, "rebuilding open index state from fragments");
            let (state, frontier) = rebuild_from_fragments(tables, head).await?;
            Ok(Recovered::Warm {
                state,
                tail: OpenTail::default(),
                frontier,
                resume: head,
            })
        }
    }
}

/// Rebuild `OpenState` and the id frontier from the durable artifacts at the
/// published `head`, clamped to the head's id frontier (the index track can
/// flush fragments for blocks above the published head, since the data
/// track's durability can lag the index track's flush boundaries).
async fn rebuild_from_fragments<M, B>(
    tables: &Tables<M, B>,
    head: u64,
) -> Result<(OpenState, FamilyFrontier)>
where
    M: MetaStore,
    B: BlobStore,
{
    let record =
        tables
            .blocks()
            .load_record(head)
            .await?
            .ok_or(MonadChainDataError::MissingData(
                "rebuild recovery: missing head block record",
            ))?;

    // The three families touch disjoint tables — rebuild them concurrently.
    let ((log, log_next), (tx, tx_next), (trace, trace_next)) = futures::try_join!(
        rebuild_family(tables.family(Family::Log), record.logs),
        rebuild_family(tables.family(Family::Tx), record.txs),
        rebuild_family(tables.family(Family::Trace), record.traces),
    )?;

    Ok((
        OpenState { log, tx, trace },
        FamilyFrontier {
            log: log_next,
            tx: tx_next,
            trace: trace_next,
        },
    ))
}

/// Rebuild one family's `FamilyState` from its open page + bucket fragments,
/// returning the state and the family's next-id frontier. `bound` (= the
/// frontier) is the exclusive id cutoff: anything at/above it belongs to a
/// block past the published head and is dropped.
async fn rebuild_family<M>(
    fam: &FamilyTables<M>,
    window: FamilyWindowRecord,
) -> Result<(FamilyState, u64)>
where
    M: MetaStore,
{
    let bound = window.next_primary_id_exclusive()?.as_u64();
    // Open-granule start; same formula as the engine's seal path so the open
    // granule can never be computed differently.
    let sealed_id = seal_boundary(bound);

    // Standby seal chain: the persisted row for the last sealed span carries
    // the running value (written atomically with that span's seal batch).
    // Nothing sealed yet ⇒ the seed value; a sealed span with no row is
    // corruption (the row is written with the seal), so fail loudly.
    let seal_chain = match last_sealed_span(sealed_id) {
        Some(span) => fam.load_seal_chain(span).await?.ok_or_else(|| {
            MonadChainDataError::Backend(format!(
                "seal-chain row missing for sealed span {span}; the store is corrupt"
            ))
        })?,
        None => EMPTY_DIGEST,
    };

    // Sealed-page counts of the OPEN page group, re-derived from durable
    // artifacts so the manifest emitted at group completion is identical to
    // what an uninterrupted run (or a checkpoint-carried accumulator) would
    // write: every sealed page's stream inventory was staged with its seal,
    // and each count is read back from the sealed artifact itself.
    let sealed_page_counts = rebuild_sealed_page_counts(fam, sealed_id).await?;

    let mut pages: HashMap<(StreamKey, u64), RoaringBitmap> = HashMap::new();
    let mut open_streams_seen: HashSet<StreamKey> = HashSet::new();
    let mut dir: VecDeque<(u64, u64, u64)> = VecDeque::new();

    // `bound == sealed_id` means the open granule is empty — nothing to rebuild.
    if bound > sealed_id {
        // Bits are stored PAGE-relative and the open granule IS one page
        // (`sealed_id` is its start), so the exclusive below-head cutoff is
        // `bound`'s page offset. `bound - sealed_id` is in (0, SEAL_SPAN), so
        // the cast is lossless.
        let offset_cutoff = (bound - sealed_id) as u32;

        // Enumerate open streams from the durable inventory, union each
        // stream's fragments (one store round-trip per stream, bounded
        // concurrency; union order is irrelevant), clamp to the head frontier.
        let streams = fam.load_open_bitmap_streams(sealed_id).await?;
        let mut unions = stream::iter(streams)
            .map(|stream_id| async move {
                let mut bm = RoaringBitmap::new();
                for fragment in fam.load_bitmap_fragments(&stream_id, sealed_id).await? {
                    bm |= &fragment.bitmap;
                }
                Ok::<_, MonadChainDataError>((StreamKey::parse(&stream_id)?, bm))
            })
            .buffer_unordered(STREAM_REBUILD_CONCURRENCY);
        while let Some(unioned) = unions.next().await {
            let (stream_key, mut bm) = unioned?;
            bm.remove_range(offset_cutoff..);
            // Record even when the below-head bits are empty, so the first
            // post-recovery flush does not re-emit an inventory row.
            open_streams_seen.insert(stream_key);
            if !bm.is_empty() {
                pages.insert((stream_key, sealed_id), bm);
            }
        }

        // Open directory bucket (block-ordered): drop entries for blocks past
        // the head; clamp a straddler's end to the frontier.
        for f in fam.load_bucket_fragments(sealed_id).await? {
            if f.first_primary_id >= bound {
                continue;
            }
            dir.push_back((
                f.block_number,
                f.first_primary_id,
                f.end_primary_id_exclusive.min(bound),
            ));
        }
    }

    Ok((
        FamilyState {
            pages,
            dir,
            sealed_id,
            open_streams_seen,
            seal_chain,
            sealed_page_counts,
        },
        bound,
    ))
}

/// Re-derive one family's open-group sealed-page-count accumulator
/// (`FamilyState::sealed_page_counts`) from durable artifacts, bounded to the
/// open page group: for each page already sealed in it, enumerate the page's
/// streams from the open-streams inventory (complete — every seal stages the
/// sealed page's full inventory in its own batch) and read each count from
/// the sealed page artifact. Deterministic: the inputs are sealed, final
/// content below the published head.
///
/// PRECONDITION: the open group's sealed pages were sealed by an engine that
/// stages seal-time inventory rows. A store whose open group carries pages
/// sealed BEFORE that feature has only the flush-time first-seen rows, which
/// can miss streams whose only bits arrived after the page's last flush — the
/// rebuilt accumulator would silently omit those `(stream, page)` counts and
/// the manifest emitted at group completion would falsely prove real matches
/// empty. Such a store must resume from a current-version checkpoint or
/// re-backfill instead of taking this rebuild path.
async fn rebuild_sealed_page_counts<M>(
    fam: &FamilyTables<M>,
    sealed_id: u64,
) -> Result<SealedPageCounts>
where
    M: MetaStore,
{
    let group_start = page_group_start(sealed_id);
    let mut counts = SealedPageCounts::new();
    for page in (group_start..sealed_id).step_by(PAGE_SPAN as usize) {
        let mut loads = stream::iter(fam.load_open_bitmap_streams(page).await?)
            .map(|stream_id| async move {
                let count = fam
                    .load_bitmap_page_artifact(&stream_id, page)
                    .await?
                    .ok_or_else(|| MonadChainDataError::SealedPageMissingArtifact {
                        stream_id: stream_id.clone(),
                        page_start: page,
                    })?
                    .meta
                    .count;
                Ok::<_, MonadChainDataError>((StreamKey::parse(&stream_id)?, count))
            })
            .buffer_unordered(STREAM_REBUILD_CONCURRENCY);
        while let Some(loaded) = loads.next().await {
            let (stream_key, count) = loaded?;
            counts
                .entry((stream_key, group_start))
                .or_default()
                .push((page_start_in_group(page), count));
        }
    }
    // Per-stream pairs ascend by page (`buffer_unordered` only scrambles
    // streams WITHIN a page; the outer loop walks pages in order), matching
    // the engine's accumulation order exactly.
    Ok(counts)
}
