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

//! Staged-write and bitmap-seeding helpers for engine-level tests and
//! downstream dev-dependents.

use alloy_primitives::B256;
use monad_query_primitives::{
    records::{BlockRecord, FamilyWindowRecord, PrimaryId},
    EvmBlockHeader,
};
use monad_query_store::{BlobStore, CacheConfig, MetaStore};

use crate::{
    bitmap::{BitmapPageArtifact, BitmapPageCounts},
    family::Family,
    tables::Tables,
    WriteSession,
};

pub fn test_header(number: u64, parent_hash: B256) -> EvmBlockHeader {
    EvmBlockHeader {
        number,
        parent_hash,
        ..EvmBlockHeader::default()
    }
}

/// Empty-window block record for header-staging tests.
pub fn block_record(number: u64) -> BlockRecord {
    let window = FamilyWindowRecord {
        first_primary_id: PrimaryId::ZERO,
        count: 0,
    };
    BlockRecord {
        block_number: number,
        block_hash: Default::default(),
        parent_hash: Default::default(),
        logs: window,
        txs: window,
        traces: window,
        row_chain: Default::default(),
    }
}

/// Cache config for the table-level suites: defaults with the decoded-row
/// cache off, so backend read counters see every materialization.
pub fn test_cache_config() -> CacheConfig {
    CacheConfig {
        row_cache_bytes: 0,
        ..CacheConfig::default()
    }
}

/// Stages block `number`'s header metadata (empty family payloads) inside an
/// open write session.
pub fn stage_block_header<M, B>(tables: &Tables<M, B>, w: &mut WriteSession<'_, M, B>, number: u64)
where
    M: MetaStore,
    B: BlobStore,
{
    let header = EvmBlockHeader {
        number,
        ..Default::default()
    };
    tables.blocks().stage_metadata(
        w,
        number,
        &block_record(number),
        &header,
        bytes::Bytes::new(),
        bytes::Bytes::new(),
        bytes::Bytes::new(),
    );
}

/// Stages and commits block `number`'s header metadata via the production
/// staged-write path.
pub async fn stage_block<M, B>(tables: &Tables<M, B>, number: u64)
where
    M: MetaStore,
    B: BlobStore,
{
    tables
        .with_writes(|w| {
            Box::pin(async move {
                stage_block_header(tables, w, number);
                Ok(())
            })
        })
        .await
        .expect("stage block header");
}

/// Durably writes one bitmap page artifact via the staged (production) write path.
pub async fn seed_bitmap_page_artifact<M, B>(
    tables: &Tables<M, B>,
    family: Family,
    stream_id: &str,
    page_start: u64,
    artifact: &BitmapPageArtifact,
) where
    M: MetaStore,
    B: BlobStore,
{
    tables
        .with_writes(|w| {
            Box::pin(async move {
                tables
                    .family(family)
                    .bitmap()
                    .stage_page_artifact(w, stream_id, page_start, artifact);
                Ok(())
            })
        })
        .await
        .expect("seed bitmap page artifact");
}

/// Durably writes one open-page bitmap fragment via the staged write path.
pub async fn seed_bitmap_page_fragment<M, B>(
    tables: &Tables<M, B>,
    family: Family,
    stream_id: &str,
    page_start: u64,
    flush_block: u64,
    blob: bytes::Bytes,
) where
    M: MetaStore,
    B: BlobStore,
{
    tables
        .with_writes(|w| {
            Box::pin(async move {
                tables.family(family).bitmap().stage_page_fragment(
                    w,
                    stream_id,
                    page_start,
                    flush_block,
                    blob,
                );
                Ok(())
            })
        })
        .await
        .expect("seed bitmap page fragment");
}

pub async fn seed_bitmap_page_counts<M, B>(
    tables: &Tables<M, B>,
    family: Family,
    stream_id: &str,
    group_start: u64,
    counts: &BitmapPageCounts,
) where
    M: MetaStore,
    B: BlobStore,
{
    tables
        .with_writes(|w| {
            Box::pin(async move {
                tables
                    .family(family)
                    .bitmap()
                    .stage_page_counts(w, stream_id, group_start, counts);
                Ok(())
            })
        })
        .await
        .expect("seed bitmap page counts");
}
