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

use crate::{
    error::Result,
    kernel::tables::Tables,
    logs::{QueryLogsRequest, QueryLogsResponse},
    primitives::range::ResolvedBlockWindow,
    store::{BlobStore, MetaStore},
};

pub async fn execute_block_scan_query<M: MetaStore, B: BlobStore>(
    tables: &Tables<M, B>,
    request: &QueryLogsRequest,
    window: ResolvedBlockWindow,
) -> Result<QueryLogsResponse> {
    let _ = (tables, request, window);
    todo!("execute the fallback block-scan query path")
}
