// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

//! The block source the ingest engine pulls from.
//!
//! A `ChainDataIngestSource` is the engine's only inbound dependency: it yields
//! the latest finalized tip (the lease clock + follow signal) and fetches one
//! finalized block at a time. The implementor owns transport concerns —
//! including fetch retry/backoff (see `TODO(fetch-retry)` in `ingest_core`), so
//! the engine itself stays transport-agnostic.

use std::future::Future;

use eyre::Result;

use crate::family::FinalizedBlock;

pub trait ChainDataIngestSource: Clone + Send + Sync + 'static {
    fn get_latest_uploaded(&self) -> impl Future<Output = Result<Option<u64>>> + Send;

    fn fetch_finalized_block(
        &self,
        block_number: u64,
    ) -> impl Future<Output = Result<FinalizedBlock>> + Send;
}
