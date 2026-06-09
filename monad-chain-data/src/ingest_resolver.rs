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

//! Production [`CodecResolver`] backed by [`Tables`].
//!
//! Resolving a codec ensures (training if necessary) the epoch's dictionaries,
//! which can read back the prior epoch's blocks. That logic lives on
//! [`Tables::ensure_epoch_dict`], so the resolver wraps the *same*
//! `Arc<Tables>` the ingest engine writes through — no separate service, no
//! second `Tables`/cache, no `Arc<Tables>` vs service-owned ownership mismatch.

use std::{future::Future, sync::Arc};

use crate::{
    engine::{family::Family, tables::Tables},
    error::{MonadChainDataError, Result},
    ingest_core::{CodecResolver, Codecs},
    store::{BlobStore, MetaStore},
};

/// A [`CodecResolver`] that resolves per-epoch codecs through [`Tables`]:
///
/// * `resolve` ensures the epoch's dictionaries are durably published (training
///   from the prior epoch's blocks on a miss) and then snapshots the write-side
///   codecs. Because `ensure_epoch_dict` is single-flight, a `resolve` that
///   races a [`Self::prewarm`] already in flight coalesces onto it.
/// * `prewarm` spawns that same ensure in the background so the boundary
///   `resolve` becomes a fast path instead of a hot-path training stall.
#[derive(Clone)]
pub struct TablesCodecResolver<M: MetaStore, B: BlobStore> {
    tables: Arc<Tables<M, B>>,
}

impl<M: MetaStore, B: BlobStore> TablesCodecResolver<M, B> {
    pub fn new(tables: Arc<Tables<M, B>>) -> Self {
        Self { tables }
    }

    /// Snapshot the installed write-side codecs for `version`. Call only after
    /// `ensure_epoch_dicts(version)` has succeeded, so the codecs are present.
    fn installed_codecs(&self, version: u32) -> Result<Codecs> {
        let dicts = self.tables.dicts();
        let codec = |family| {
            dicts
                .write_codec(family, version)
                .ok_or(MonadChainDataError::MissingData(
                    "epoch codec not installed after ensure_epoch_dicts",
                ))
        };
        Ok(Codecs::new(
            codec(Family::Log)?,
            codec(Family::Tx)?,
            codec(Family::Trace)?,
        ))
    }
}

impl<M: MetaStore, B: BlobStore> CodecResolver for TablesCodecResolver<M, B> {
    fn resolve(&self, version: u32) -> impl Future<Output = Result<Codecs>> + Send {
        let this = self.clone();
        async move {
            this.tables.ensure_epoch_dicts(version).await?;
            this.installed_codecs(version)
        }
    }

    fn prewarm(&self, version: u32) {
        let tables = self.tables.clone();
        tokio::spawn(async move {
            if let Err(e) = tables.ensure_epoch_dicts(version).await {
                tracing::warn!(error = %e, version, "background dict pre-train failed");
            }
        });
    }
}
