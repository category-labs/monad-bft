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

//! The block source the ingest engine pulls from — its only inbound dependency.
//! The implementor owns transport concerns, including fetch retry/backoff (see
//! `TODO(fetch-retry)` in the `producer` module).

use std::future::Future;

use eyre::Result;

use crate::ingest_types::FinalizedBlock;

pub trait ChainDataIngestSource: Clone + Send + Sync + 'static {
    fn get_latest_uploaded(&self) -> impl Future<Output = Result<Option<u64>>> + Send;

    fn fetch_finalized_block(
        &self,
        block_number: u64,
    ) -> impl Future<Output = Result<FinalizedBlock>> + Send;
}
