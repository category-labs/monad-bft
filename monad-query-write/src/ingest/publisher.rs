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

//! Durable-progress tracking shared by the tracks, and the publisher task
//! that advances the published head to the newest index flush boundary the
//! data track has caught up to, carrying the head block's row chain into
//! `PublicationState`.

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use tokio::sync::{mpsc, Notify};

use crate::{
    engine::tables::{PublicationTables, Tables},
    error::{MonadChainDataError, Result},
    store::{BlobStore, MetaStore},
};

/// Publishable-head bookkeeping. Every candidate head must be safe to recover
/// from AND complete for readers: an index flush boundary (recorded by
/// `batch_flush`), or the resume seed — the prior published head, itself such
/// a boundary. Raw `data_durable` is NOT safe: it advances at arbitrary
/// size-based pack boundaries, and a mid-flush-batch head would strand the
/// tail bits/dir entries that `seal` consumed without ever fragmenting (see
/// [`super::recover`]). The checkpoint block is not safe either: its tail
/// index lives only in the snapshot (`checkpoint` writes no fragments), so a
/// head there would hide those blocks from indexed queries until the next
/// flush.
struct Heads {
    /// Flush boundaries not yet covered by `data_durable`, ascending. The data
    /// track can lag several flush intervals during catch-up, so this is a
    /// queue, not a last-value register; it is short and pushed once per
    /// `batch_flush`, so a mutex is fine.
    pending: VecDeque<u64>,
    /// Largest recorded boundary at/below `data_durable` — the publishable
    /// head. Monotonic; seeded with the prior published head on a warm start.
    head: u64,
}

pub(crate) struct Progress {
    data_durable: AtomicU64,
    heads: Mutex<Heads>,
    /// Pulsed when a durable frontier advances. `notify_one` stores a permit
    /// when no waiter is parked, so a pulse racing the publisher's
    /// read-publish-rewait cycle is never lost; bursts coalesce because the
    /// publisher re-reads `publishable_head()` on wake.
    advance: Notify,
}

impl Progress {
    /// `data_floor` seeds the data frontier at the resume block (rows are
    /// durable through it), so the first recorded flush boundary at/below it
    /// promotes immediately. `head_floor` re-seeds the prior published head —
    /// never the checkpoint block, whose tail index has no fragments — so the
    /// boot publish is a no-op and every head a reader ever sees past the
    /// prior one is a recorded flush boundary.
    pub(crate) fn new(data_floor: u64, head_floor: u64) -> Self {
        // Recovery resumes from max(checkpoint, published head), so the prior
        // head can never sit above the durable floor.
        debug_assert!(head_floor <= data_floor, "published head above resume");
        Self {
            data_durable: AtomicU64::new(data_floor),
            heads: Mutex::new(Heads {
                pending: VecDeque::new(),
                head: head_floor,
            }),
            advance: Notify::new(),
        }
    }
    pub(crate) fn set_data_durable(&self, block: u64) {
        self.data_durable.fetch_max(block, Ordering::Relaxed);
        self.advance.notify_one();
    }
    /// Records `block` (the index track's `batch_flush` horizon, durable on
    /// call) as a publishable-head candidate. Boundaries arrive ascending; a
    /// repeat (e.g. the terminal flush of an idle resume) is dropped.
    pub(crate) fn record_flush_boundary(&self, block: u64) {
        let mut heads = self.heads.lock().expect("progress mutex poisoned");
        if block > *heads.pending.back().unwrap_or(&heads.head) {
            heads.pending.push_back(block);
        }
        drop(heads);
        self.advance.notify_one();
    }
    pub(crate) fn data_durable(&self) -> u64 {
        self.data_durable.load(Ordering::Relaxed)
    }
    /// The newest recorded flush boundary at/below `data_durable` (never raw
    /// `data_durable` itself). Promotes caught-up boundaries and prunes them
    /// from the queue; a `data_durable` advance racing the read just pulses
    /// `advance` again, so the publisher re-reads it.
    pub(crate) fn publishable_head(&self) -> u64 {
        let durable = self.data_durable.load(Ordering::Relaxed);
        let mut heads = self.heads.lock().expect("progress mutex poisoned");
        while heads.pending.front().is_some_and(|&b| b <= durable) {
            heads.head = heads.pending.pop_front().expect("front checked");
        }
        heads.head
    }
    /// Resolves the next time a durable frontier advances (or immediately if a
    /// pulse was stored since the last wait).
    pub(crate) async fn changed(&self) {
        self.advance.notified().await;
    }
}

/// Publish `progress.publishable_head()` if it advanced past `published`,
/// updating `published` to match. No-op when the head hasn't moved. The head
/// is a flush boundary at/below `data_durable`, so its `BlockRecord` is
/// durable by construction — a missing record is an invariant violation, not
/// a fallback.
async fn publish_if_ahead<M, B>(
    progress: &Progress,
    tables: &Tables<M, B>,
    publisher: &PublicationTables<M>,
    published: &mut u64,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
{
    let head = progress.publishable_head();
    if head > *published {
        let record =
            tables
                .blocks()
                .load_record(head)
                .await?
                .ok_or(MonadChainDataError::MissingData(
                    "missing block record for published head",
                ))?;
        publisher.publish(head, record.row_chain).await?;
        *published = head;
    }
    Ok(())
}

pub(crate) async fn run_publisher<M, B>(
    progress: Arc<Progress>,
    tables: Arc<Tables<M, B>>,
    publisher: Arc<PublicationTables<M>>,
    mut published: u64,
    mut shutdown: mpsc::Receiver<()>,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
{
    // Publish any head already publishable before parking — e.g. a boundary
    // recorded before this task registered a waiter (its pulse is also held
    // as a stored permit, but boot shouldn't depend on that).
    publish_if_ahead(&progress, &tables, &publisher, &mut published).await?;

    loop {
        tokio::select! {
            // A track advanced a durable frontier; bursts during an in-flight
            // write coalesce into one publish of the freshest head.
            _ = progress.changed() => {
                publish_if_ahead(&progress, &tables, &publisher, &mut published).await?;
            }
            _ = shutdown.recv() => {
                publish_if_ahead(&progress, &tables, &publisher, &mut published).await?;
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Regression for the mid-pack published head: with flushes recorded at 92
    // and 99 and a pack durable at 96, `min(data_durable, latest_flush)`
    // would publish 96 — a mid-flush-batch block whose seal-consumed tail
    // bits have no fragments, so the fragment rebuild at 96 would lose them.
    // The head must hold at the last boundary the data track covered.
    #[test]
    fn head_holds_at_last_covered_flush_boundary() {
        let p = Progress::new(0, 0);
        p.record_flush_boundary(92);
        p.record_flush_boundary(99);
        p.set_data_durable(96);
        assert_eq!(p.publishable_head(), 92);
        p.set_data_durable(99);
        assert_eq!(p.publishable_head(), 99);
    }

    #[test]
    fn data_durable_alone_never_advances_the_head() {
        let p = Progress::new(0, 0);
        p.set_data_durable(1_000);
        assert_eq!(p.publishable_head(), 0);
        p.record_flush_boundary(10);
        assert_eq!(p.publishable_head(), 10);
    }

    #[test]
    fn several_pending_boundaries_promote_to_the_newest_covered() {
        let p = Progress::new(0, 0);
        for boundary in [10, 20, 30, 40] {
            p.record_flush_boundary(boundary);
        }
        p.set_data_durable(25);
        assert_eq!(p.publishable_head(), 20);
        p.set_data_durable(40);
        assert_eq!(p.publishable_head(), 40);
    }

    #[test]
    fn boundary_equal_to_data_durable_is_publishable() {
        let p = Progress::new(0, 0);
        p.set_data_durable(50);
        p.record_flush_boundary(50);
        assert_eq!(p.publishable_head(), 50);
    }

    // Warm-resume seeding (rebuild arm: resume == prior head): the seed is
    // immediately publishable, the terminal flush of an idle resume (a repeat
    // of the seed) is a no-op, and the head never moves between recorded
    // boundaries.
    #[test]
    fn seed_is_immediately_publishable_and_never_regresses() {
        let p = Progress::new(7, 7);
        assert_eq!(p.publishable_head(), 7);
        assert_eq!(p.data_durable(), 7);
        p.record_flush_boundary(7);
        assert_eq!(p.publishable_head(), 7);
        p.record_flush_boundary(9);
        assert_eq!(p.publishable_head(), 7); // data still at the seed
        p.set_data_durable(12);
        assert_eq!(p.publishable_head(), 9);
    }

    // Regression for the checkpoint-restore seed: blocks between the last
    // flush boundary and the checkpoint have their open-granule index only in
    // the snapshot (no fragments), so the boot must keep the prior published
    // head — seeding the head at the checkpoint block would let indexed
    // queries silently miss those blocks until the first post-resume flush.
    #[test]
    fn checkpoint_floor_is_not_published_before_a_flush_boundary() {
        let p = Progress::new(5, 2);
        assert_eq!(p.publishable_head(), 2);
        assert_eq!(p.data_durable(), 5);
        // The terminal/first flush at the resume block is already covered by
        // the seeded data frontier and promotes immediately.
        p.record_flush_boundary(5);
        assert_eq!(p.publishable_head(), 5);
    }
}
