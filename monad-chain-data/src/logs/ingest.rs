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

use crate::{error::Result, family::FinalizedBlock, primitives::state::BlockRecord};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogIngestPlan {
    pub block_record: BlockRecord,
    pub written_logs: usize,
}

pub fn plan_log_ingest(_block: &FinalizedBlock) -> Result<LogIngestPlan> {
    todo!("implement logs-family ingest planning")
}
