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
//! that advances the published head to `min(data_durable, index_visible)`,
//! carrying the head block's row chain into `PublicationState`.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use tokio::sync::{mpsc, Notify};

use crate::{
    engine::tables::{PublicationTables, Tables},
    error::{MonadChainDataError, Result},
    store::{BlobStore, MetaStore},
};

#[derive(Default)]
pub(crate) struct Progress {
    data_durable: AtomicU64,
    /// Highest block whose index is durable AND reader-visible (set at
    /// `batch_flush`, after the inline artifact writes return durable).
    index_visible: AtomicU64,
    /// Pulsed when a durable frontier advances. `notify_one` stores a permit
    /// when no waiter is parked, so a pulse racing the publisher's
    /// read-publish-rewait cycle is never lost; bursts coalesce because the
    /// publisher re-reads `publishable_head()` on wake.
    advance: Notify,
}

impl Progress {
    pub(crate) fn set_data_durable(&self, block: u64) {
        self.data_durable.fetch_max(block, Ordering::Relaxed);
        self.advance.notify_one();
    }
    pub(crate) fn set_index_visible(&self, block: u64) {
        self.index_visible.fetch_max(block, Ordering::Relaxed);
        self.advance.notify_one();
    }
    pub(crate) fn data_durable(&self) -> u64 {
        self.data_durable.load(Ordering::Relaxed)
    }
    pub(crate) fn publishable_head(&self) -> u64 {
        self.data_durable
            .load(Ordering::Relaxed)
            .min(self.index_visible.load(Ordering::Relaxed))
    }
    /// Resolves the next time a durable frontier advances (or immediately if a
    /// pulse was stored since the last wait).
    pub(crate) async fn changed(&self) {
        self.advance.notified().await;
    }
}

/// Publish `progress.publishable_head()` if it advanced past `published`,
/// updating `published` to match. No-op when the head hasn't moved. The head
/// is `min(data_durable, index_visible)`, so its `BlockRecord` is durable by
/// construction — a missing record is an invariant violation, not a fallback.
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
    // Publish any head already durable before parking — e.g. a warm resume
    // seeds `data_durable` before this task registered a waiter.
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
