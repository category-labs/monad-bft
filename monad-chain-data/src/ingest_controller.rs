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

//! Unified ingest controller.
//!
//! There is a single ingest behaviour — **always follow the tip**. A run that
//! starts far behind catches up in parallel and then settles into tip-following;
//! "backfill" is just that catch-up phase. There is no backfill/live split: the
//! behavioural knobs are the adaptive [`SignalPolicy`] flush cadences (tight near
//! the tip, loose while catching up) and the `checkpoint_every_blocks` cadence. An
//! optional `stop_at`/`count` bounds the run for tests and "ingest up to block X
//! then stop"; it is a run bound, not a mode.
//!
//! All ingest logic lives in `ingest_core`; this file only assembles the config.

use std::sync::Arc;

use crate::{
    engine::{authority::HeadPublisher, tables::Tables},
    error::Result,
    ingest_core::{run_ingest, CodecResolver, PackCfg, Prefetch, SignalPolicy, SnapshotStore},
    ingest_source::ChainDataIngestSource,
    store::{BlobStore, MetaStore},
};

#[derive(Debug, Clone, Copy)]
pub struct IngestRunConfig {
    /// Cold-start floor: the begin block when the store is empty. Production passes
    /// 0 (genesis); a warm resume always overrides it with the snapshot's
    /// `last_block + 1`. Tests use it to seed fixtures at a chosen block.
    pub start: u64,
    /// Inclusive absolute end ceiling. `None` (with `count` also `None`) follows
    /// the tip forever. At most one of `stop_at`/`count` is set.
    pub stop_at: Option<u64>,
    /// Block count measured from the begin (resume) block; resolved to an absolute
    /// `stop_at` once the begin block is known. At most one of `stop_at`/`count`.
    pub count: Option<u64>,
    pub pack_target_bytes: usize,
    pub pack_max_blocks: usize,
    /// Adaptive `BatchFlush` interval (see [`SignalPolicy`]):
    /// `max(distance_to_tip / tip_lag_divisor, 1)`.
    pub tip_lag_divisor: u64,
    /// Snapshot `OpenState` every this many blocks (recovery resume point).
    pub checkpoint_every_blocks: u64,
    pub track_buffer: usize,
    /// Tip poll interval (ms) once caught up to the uploaded tip.
    pub poll_ms: u64,
}

pub async fn run_ingest_controller<S, M, B, R>(
    source: S,
    tables: Arc<Tables<M, B>>,
    authority: Arc<HeadPublisher<M>>,
    snapshots: SnapshotStore<M>,
    resolver: R,
    config: IngestRunConfig,
    prefetch: Prefetch,
) -> Result<()>
where
    S: ChainDataIngestSource,
    M: MetaStore,
    B: BlobStore,
    R: CodecResolver,
{
    let policy = SignalPolicy {
        tip_lag_divisor: config.tip_lag_divisor,
        checkpoint_every_blocks: config.checkpoint_every_blocks,
    };
    let pack_cfg = PackCfg {
        target_bytes: config.pack_target_bytes,
        max_blocks: config.pack_max_blocks,
    };

    // Recovery (the begin block) and the count→stop_at resolution happen inside
    // `run_ingest`, after the lease is acquired. `config.start` is only the
    // cold-start floor.
    run_ingest(
        source,
        tables,
        authority,
        snapshots,
        resolver,
        config.stop_at,
        config.count,
        config.poll_ms,
        config.start,
        policy,
        pack_cfg,
        config.track_buffer,
        prefetch,
    )
    .await
}
