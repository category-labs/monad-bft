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

//! Production [`CodecResolver`] backed by [`Tables`] — the *same* `Arc<Tables>`
//! the ingest engine writes through, so dict training (which reads back the
//! prior epoch's blocks) needs no separate service or second cache.

use std::{future::Future, sync::Arc};

use monad_query_engine::{family::Family, tables::Tables};
use monad_query_errors::{QueryError, Result};
use monad_query_store::{BlobStore, MetaStore};

use crate::ingest::{CodecResolver, Codecs};

/// Resolves per-epoch codecs through [`Tables::ensure_epoch_dicts`], which is
/// single-flight: a `resolve` racing an in-flight `prewarm` coalesces onto it,
/// so pre-training turns the epoch-boundary resolve into a fast path.
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
                .ok_or(QueryError::MissingData(
                    "epoch codec not installed after ensure_epoch_dicts",
                ))
        };
        Ok(Codecs {
            log: codec(Family::Log)?,
            tx: codec(Family::Tx)?,
            trace: codec(Family::Trace)?,
        })
    }
}

impl<M: MetaStore, B: BlobStore> CodecResolver for TablesCodecResolver<M, B> {
    fn ensure(&self, version: u32) -> impl Future<Output = Result<Codecs>> + Send {
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
