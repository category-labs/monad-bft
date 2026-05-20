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

use futures::{stream, StreamExt};
use roaring::RoaringBitmap;
use tracing::trace;

use crate::{
    engine::{
        clause::IndexedFilter,
        family::Family,
        query::{
            directory_resolver::{PrimaryIdResolver, ResolvedPrimaryIdLocation},
            window::resolve_primary_id_window,
        },
        tables::Tables,
    },
    error::{MonadChainDataError, Result},
    primitives::{
        page::QueryOrder,
        range::ResolvedBlockWindow,
        refs::{BlockRef, BlockSpan},
        state::PrimaryId,
    },
    store::{BlobStore, MetaStore},
};

const CANDIDATE_RESOLUTION_BATCH_SIZE: usize = 1_024;
const RECORD_LOAD_CONCURRENCY: usize = 64;

/// Outcome returned by the shared family query runners. Families wrap this
/// into their own response types.
pub(crate) struct IndexedQueryOutcome<T> {
    pub records: Vec<T>,
    pub span: BlockSpan,
    pub stats: QueryExecutionStats,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct QueryExecutionStats {
    pub shards_covered: u64,
    pub shards_visited: u64,
    pub clauses: u64,
    pub clause_streams: u64,
    pub bitmap_page_probes: u64,
    pub compacted_bitmap_pages_read: u64,
    pub open_bitmap_pages_read: u64,
    pub bitmap_fragments_read: u64,
    pub candidate_primary_ids: u64,
    pub directory_hits: u64,
    pub records_loaded: u64,
    pub records_returned: u64,
}

impl QueryExecutionStats {
    pub fn bitmap_pages_read(&self) -> u64 {
        self.compacted_bitmap_pages_read
            .saturating_add(self.open_bitmap_pages_read)
    }

    pub fn merge(&mut self, other: &Self) {
        self.shards_covered = self.shards_covered.saturating_add(other.shards_covered);
        self.shards_visited = self.shards_visited.saturating_add(other.shards_visited);
        self.clauses = self.clauses.saturating_add(other.clauses);
        self.clause_streams = self.clause_streams.saturating_add(other.clause_streams);
        self.bitmap_page_probes = self
            .bitmap_page_probes
            .saturating_add(other.bitmap_page_probes);
        self.compacted_bitmap_pages_read = self
            .compacted_bitmap_pages_read
            .saturating_add(other.compacted_bitmap_pages_read);
        self.open_bitmap_pages_read = self
            .open_bitmap_pages_read
            .saturating_add(other.open_bitmap_pages_read);
        self.bitmap_fragments_read = self
            .bitmap_fragments_read
            .saturating_add(other.bitmap_fragments_read);
        self.candidate_primary_ids = self
            .candidate_primary_ids
            .saturating_add(other.candidate_primary_ids);
        self.directory_hits = self.directory_hits.saturating_add(other.directory_hits);
        self.records_loaded = self.records_loaded.saturating_add(other.records_loaded);
        self.records_returned = self.records_returned.saturating_add(other.records_returned);
    }
}

/// Per-family interface consumed by the shared indexed and block-scan
/// runners. Implemented by each family's materializer.
pub(crate) trait IndexedFamilyQuery<M: MetaStore, B: BlobStore> {
    type Filter: IndexedFilter<Record = Self::Record>;
    type Record;

    fn family() -> Family;

    async fn load_record_at(&self, block_number: u64, idx_in_block: usize) -> Result<Self::Record>;

    async fn load_block_ref(&self, block_number: u64) -> Result<BlockRef>;

    async fn load_filtered_block_records(
        &self,
        block_number: u64,
        order: QueryOrder,
        filter: &Self::Filter,
    ) -> Result<(BlockRef, Vec<Self::Record>)>;
}

/// Shared indexed query runner. Walks the primary-id window for the
/// runner's family, intersects bitmaps per shard, resolves candidates
/// through the shared directory, and materializes each match through the
/// runner. Completes the current block when `limit` is reached.
pub(crate) async fn execute_indexed_family_query<M, B, R>(
    tables: &Tables<M, B>,
    runner: &R,
    filter: &R::Filter,
    block_window: ResolvedBlockWindow,
    order: QueryOrder,
    limit: usize,
) -> Result<IndexedQueryOutcome<R::Record>>
where
    M: MetaStore,
    B: BlobStore,
    R: IndexedFamilyQuery<M, B>,
{
    let (from_block, to_block) = block_window.request_endpoints(order);
    let family = R::family();

    let Some(window) = resolve_primary_id_window(tables.blocks(), family, &block_window).await?
    else {
        return Ok(IndexedQueryOutcome {
            records: Vec::new(),
            span: BlockSpan {
                from_block,
                to_block,
                cursor_block: to_block,
            },
            stats: QueryExecutionStats::default(),
        });
    };

    let clauses = filter.indexed_clauses();
    if clauses.is_empty() {
        return Err(MonadChainDataError::InvalidRequest(
            "indexed query requires at least one indexed clause",
        ));
    }

    let mut resolver = PrimaryIdResolver::new(tables.family(family));
    let mut records = Vec::new();
    let mut stop_after_block = None;
    let shards = window.shard_iter(order);
    let mut stats = QueryExecutionStats {
        shards_covered: shards.len() as u64,
        clauses: clauses.len() as u64,
        ..QueryExecutionStats::default()
    };

    for shard in shards {
        stats.shards_visited = stats.shards_visited.saturating_add(1);
        let (local_from, local_to) = window.local_range_for_shard(shard);

        let Some(candidate_bitmap) = tables
            .family(family)
            .load_intersection_bitmap(&clauses, shard, local_from, local_to)
            .await?
        else {
            continue;
        };
        stats.merge(&candidate_bitmap.stats);

        let locals = locals_in_query_order(candidate_bitmap.bitmap, order);

        for local_chunk in locals.chunks(CANDIDATE_RESOLUTION_BATCH_SIZE) {
            let ids: Vec<_> = local_chunk
                .iter()
                .map(|local| PrimaryId::from_parts(shard, *local))
                .collect();
            stats.candidate_primary_ids =
                stats.candidate_primary_ids.saturating_add(ids.len() as u64);
            let resolved = resolver.resolve_many_ordered(&ids).await?;
            let locations: Vec<_> = resolved.into_iter().flatten().collect();
            stats.directory_hits = stats.directory_hits.saturating_add(locations.len() as u64);

            let materialized = materialize_locations_ordered(runner, locations).await;

            for materialized in materialized {
                if let Some(stop_block) = stop_after_block {
                    if materialized.location.block_number != stop_block {
                        let cursor_block = runner.load_block_ref(stop_block).await?;
                        stats.records_returned = records.len() as u64;
                        trace_query_execution_stats(family, "indexed", &stats);
                        return Ok(IndexedQueryOutcome {
                            records,
                            span: BlockSpan {
                                from_block,
                                to_block,
                                cursor_block,
                            },
                            stats,
                        });
                    }
                }

                let record = materialized.record?;
                stats.records_loaded = stats.records_loaded.saturating_add(1);

                // Most families guarantee `matches` for any candidate the bitmap
                // intersection produces. The trace family violates that
                // guarantee in one case — `is_top_level: Some(false)` combined
                // with other indexed clauses can pick up top-level frames that
                // the runner must drop here. Treat the post-filter as
                // authoritative rather than asserting.
                if !filter.matches(&record) {
                    continue;
                }

                records.push(record);

                if stop_after_block.is_none() && records.len() >= limit {
                    stop_after_block = Some(materialized.location.block_number);
                }
            }
        }
    }

    let stats = QueryExecutionStats {
        records_returned: records.len() as u64,
        ..stats
    };
    trace_query_execution_stats(family, "indexed", &stats);
    Ok(IndexedQueryOutcome {
        stats,
        records,
        span: BlockSpan {
            from_block,
            to_block,
            cursor_block: to_block,
        },
    })
}

struct MaterializedRecord<T> {
    location: ResolvedPrimaryIdLocation,
    record: Result<T>,
}

async fn materialize_locations_ordered<M, B, R>(
    runner: &R,
    locations: Vec<ResolvedPrimaryIdLocation>,
) -> Vec<MaterializedRecord<R::Record>>
where
    M: MetaStore,
    B: BlobStore,
    R: IndexedFamilyQuery<M, B>,
{
    stream::iter(locations)
        .map(|location| async move {
            let record = runner
                .load_record_at(location.block_number, location.idx_in_block)
                .await;
            MaterializedRecord { location, record }
        })
        .buffered(RECORD_LOAD_CONCURRENCY)
        .collect()
        .await
}

fn trace_query_execution_stats(family: Family, path: &'static str, stats: &QueryExecutionStats) {
    trace!(
        ?family,
        path,
        shards_covered = stats.shards_covered,
        shards_visited = stats.shards_visited,
        clauses = stats.clauses,
        clause_streams = stats.clause_streams,
        bitmap_page_probes = stats.bitmap_page_probes,
        bitmap_pages_read = stats.bitmap_pages_read(),
        compacted_bitmap_pages_read = stats.compacted_bitmap_pages_read,
        open_bitmap_pages_read = stats.open_bitmap_pages_read,
        bitmap_fragments_read = stats.bitmap_fragments_read,
        candidate_primary_ids = stats.candidate_primary_ids,
        directory_hits = stats.directory_hits,
        records_loaded = stats.records_loaded,
        records_returned = stats.records_returned,
        "chain data query execution stats"
    );
}

/// Shared block-scan runner used when the query filter carries no indexed
/// clause. Walks blocks in query order, lets the family filter each
/// block's records, and stops once `limit` is reached.
pub(crate) async fn execute_block_scan_family_query<M, B, R>(
    runner: &R,
    filter: &R::Filter,
    block_window: ResolvedBlockWindow,
    order: QueryOrder,
    limit: usize,
) -> Result<IndexedQueryOutcome<R::Record>>
where
    M: MetaStore,
    B: BlobStore,
    R: IndexedFamilyQuery<M, B>,
{
    let (from_block, to_block) = block_window.request_endpoints(order);
    let mut records = Vec::new();
    let mut cursor_block = from_block;
    let mut stats = QueryExecutionStats::default();

    for block_number in block_window.iter(order) {
        let (block_ref, block_records) = runner
            .load_filtered_block_records(block_number, order, filter)
            .await?;
        stats.records_loaded = stats
            .records_loaded
            .saturating_add(block_records.len() as u64);
        records.extend(block_records);
        cursor_block = block_ref;
        if records.len() >= limit {
            break;
        }
    }

    stats.records_returned = records.len() as u64;
    trace_query_execution_stats(R::family(), "block_scan", &stats);

    Ok(IndexedQueryOutcome {
        stats,
        records,
        span: BlockSpan {
            from_block,
            to_block,
            cursor_block,
        },
    })
}

fn locals_in_query_order(bitmap: RoaringBitmap, order: QueryOrder) -> Vec<u32> {
    let mut locals: Vec<u32> = bitmap.into_iter().collect();
    if order == QueryOrder::Descending {
        locals.reverse();
    }
    locals
}
