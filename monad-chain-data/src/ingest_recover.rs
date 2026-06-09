// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

//! Crash recovery for the branchless ingest engine: decide where to resume and
//! reconstruct the open working set, choosing between the durable checkpoint
//! snapshot and a rebuild from persisted fragments.
//!
//! The engine stresses two recovery regimes (see the recovery-regimes notes):
//!
//! * **backfill** ≈ one giant batch. `BatchFlush`es are rare, so few fragments
//!   exist and `OpenState` is usually empty; the accumulating bits live in
//!   `OpenTail`. The checkpoint (which captures BOTH `OpenState` and `OpenTail`)
//!   runs *ahead* of the published head, so it is the most-advanced state.
//! * **live** ≈ frequent batches. Flushes are frequent, so per-flush fragments +
//!   the open-stream delta inventory together encode the FULL `OpenState` up to
//!   the published head, while checkpoints are rare and lag far behind. Here we
//!   rebuild `OpenState` from those fragments and need no recent checkpoint.
//!
//! Recovery therefore resumes from `max(checkpoint_block, published_head)`:
//! restore the checkpoint when it is at/ahead of the head, otherwise rebuild from
//! fragments at the head. The rebuild reads existing artifacts (never re-writing
//! them), so it cannot corrupt already-published data and resumes at `head + 1`.

use std::collections::{HashMap, HashSet, VecDeque};

use roaring::RoaringBitmap;

use crate::{
    engine::{
        bitmap::local_page_start,
        family::Family,
        tables::{FamilyTables, Tables},
    },
    error::{MonadChainDataError, Result},
    ingest_core::{seal_boundary, FamilyFrontier, FamilyState, OpenState, OpenTail, SnapshotStore},
    ingest_helpers::recover_checkpoint,
    primitives::state::FamilyWindowRecord,
    store::{BlobStore, MetaStore},
};

/// The recovered starting point for `run_ingest`. `Cold` is a fresh store (no
/// checkpoint, nothing published); `Warm` carries the restored open working set
/// plus the resume block and the data-durable floor.
pub(crate) enum Recovered {
    Cold,
    Warm {
        state: OpenState,
        tail: OpenTail,
        frontier: FamilyFrontier,
        /// Highest block whose ingest is complete; the engine continues at
        /// `resume + 1`.
        resume: u64,
        /// Highest block whose row data is durable (seeds `data_durable`).
        durable_floor: u64,
    },
}

/// Decide recovery from `max(checkpoint_block, published_head)`.
///
/// `head` is the authoritative published head read after acquiring the lease.
/// When the checkpoint is at/ahead of the head we restore it directly (backfill /
/// a fresh checkpoint); otherwise we rebuild the open state from the durable
/// fragments at the head (live / a stale-or-absent checkpoint). The rebuilt tail
/// is empty because the head is always a flush boundary.
pub(crate) async fn recover<M, B>(
    tables: &Tables<M, B>,
    snapshots: &SnapshotStore<M>,
    head: u64,
) -> Result<Recovered>
where
    M: MetaStore,
    B: BlobStore,
{
    match recover_checkpoint(snapshots).await? {
        None if head == 0 => Ok(Recovered::Cold),
        Some((state, tail, frontier, checkpoint)) if checkpoint >= head => Ok(Recovered::Warm {
            state,
            tail,
            frontier,
            resume: checkpoint,
            durable_floor: checkpoint,
        }),
        // head > checkpoint, or no checkpoint but blocks are published: rebuild
        // the open state from fragments at the published head.
        _ => {
            let (state, frontier) = rebuild_from_fragments(tables, head).await?;
            Ok(Recovered::Warm {
                state,
                tail: OpenTail::default(),
                frontier,
                resume: head,
                durable_floor: head,
            })
        }
    }
}

/// Rebuild the full `OpenState` and id frontier from the durable artifacts at the
/// published `head`. Per family: the id frontier comes from the head block's
/// record; the open page's bitmaps come from unioning its fragments; the open
/// directory bucket comes from its dir fragments. Everything is clamped to the
/// head's id frontier (Q4: the index track can flush fragments for blocks above
/// the published head, since `data_durable`/`index_visible` advance independently).
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

    let (log, log_next) = rebuild_family(tables.family(Family::Log), record.logs).await?;
    let (tx, tx_next) = rebuild_family(tables.family(Family::Tx), record.txs).await?;
    let (trace, trace_next) = rebuild_family(tables.family(Family::Trace), record.traces).await?;

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
/// returning the state and the family's next-id frontier (`window`'s exclusive
/// end). `bound` (= the frontier) is the exclusive id cutoff: any bit or dir
/// entry at/above it belongs to a block past the published head and is dropped.
async fn rebuild_family<M>(
    fam: &FamilyTables<M>,
    window: FamilyWindowRecord,
) -> Result<(FamilyState, u64)>
where
    M: MetaStore,
{
    let bound = window.next_primary_id_exclusive()?.as_u64();
    // The open granule starts here (a multiple of the seal span); everything
    // below is already sealed into page/bucket artifacts. Identical formula to the
    // engine's seal path so the open page can never be computed differently.
    let sealed_id = seal_boundary(bound);

    let mut pages: HashMap<(String, u64), RoaringBitmap> = HashMap::new();
    let mut open_streams_seen: HashSet<String> = HashSet::new();
    let mut dir: VecDeque<(u64, u64, u64)> = VecDeque::new();

    // `bound == sealed_id` means the last assigned id fell exactly on a seal
    // boundary, so the open granule is empty — nothing to rebuild.
    if bound > sealed_id {
        // Bits are stored SHARD-local (`PrimaryId::local()`), not granule-local,
        // so the open page's bits sit at shard-local `page_start_local + offset`.
        // The exclusive "below the head frontier" cutoff is therefore `bound`'s
        // shard-local value: `page_start_local + (bound - sealed_id)`. Anything
        // at/above it belongs to a block past the published head (Q4 clamp).
        // `bound - sealed_id` is in (0, SEAL_SPAN), so the cast is lossless.
        let page_start_local = local_page_start(sealed_id);
        let local_cutoff = page_start_local + (bound - sealed_id) as u32;

        // Open bitmap pages: enumerate the open streams from the durable inventory
        // (the only consumer of the open-stream delta rows), union each stream's
        // fragments, then clamp to the head frontier.
        let streams = fam.load_open_bitmap_streams(sealed_id).await?;
        for stream_id in streams {
            let mut bm = RoaringBitmap::new();
            for fragment in fam
                .load_bitmap_fragments(&stream_id, page_start_local)
                .await?
            {
                bm |= &fragment.bitmap;
            }
            bm.remove_range(local_cutoff..);
            // The stream is open in this page regardless of whether its
            // below-head bits are empty; record it so the first post-recovery
            // flush does not re-emit an inventory row for it.
            open_streams_seen.insert(stream_id.clone());
            if !bm.is_empty() {
                pages.insert((stream_id, sealed_id), bm);
            }
        }

        // Open directory bucket: the fragments filed in this bucket are exactly
        // the open dir entries (a block straddling the seal boundary is re-filed
        // into the open bucket each flush). Returned block-ordered. Drop entries
        // for blocks past the head; clamp a straddler's end to the frontier.
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
        },
        bound,
    ))
}
