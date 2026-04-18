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

use std::collections::BTreeSet;

use crate::{
    error::{MonadChainDataError, Result},
    family::Hash32,
    kernel::tables::BlockTables,
    logs::LogEntry,
    primitives::EvmBlockHeader,
    store::MetaStore,
};

/// A block as returned by queries: the full header payload plus the block
/// hash, which the header type itself does not carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub hash: Hash32,
    pub header: EvmBlockHeader,
}

/// Loads the blocks referenced by `logs` in ascending block-number order,
/// deduped. Used to fulfill the `blocks` relation on a logs query when the
/// caller opts in.
pub async fn load_blocks_for_logs<M: MetaStore>(
    blocks: &BlockTables<M>,
    logs: &[LogEntry],
) -> Result<Vec<Block>> {
    let distinct: BTreeSet<u64> = logs.iter().map(|l| l.block_number).collect();
    let mut out = Vec::with_capacity(distinct.len());
    for number in distinct {
        out.push(load_block(blocks, number).await?);
    }
    Ok(out)
}

pub async fn load_block<M: MetaStore>(blocks: &BlockTables<M>, number: u64) -> Result<Block> {
    let header = blocks
        .load_header(number)
        .await?
        .ok_or(MonadChainDataError::MissingData("missing block header"))?;
    let record = blocks
        .load_record(number)
        .await?
        .ok_or(MonadChainDataError::MissingData("missing block record"))?;
    Ok(Block {
        hash: record.block_hash,
        header,
    })
}
