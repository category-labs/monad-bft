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

use std::time::Instant;

use futures::{stream, StreamExt};
use roaring::RoaringBitmap;
use tracing::trace;

use crate::{
    engine::{
        bitmap::page_start_local,
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
    store::{BlobStore, CacheSnapshot, MetaStore},
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
    pub bitmap_fragment_scans_skipped: u64,
    pub bitmap_page_groups_planned: u64,
    pub bitmap_page_groups_short_circuited: u64,
    pub bitmap_page_jobs_baseline: u64,
    pub bitmap_page_jobs_pruned: u64,
    pub candidate_primary_ids: u64,
    pub directory_hits: u64,
    pub records_loaded: u64,
    pub records_returned: u64,
    pub primary_window_resolve_us: u64,
    pub bitmap_io_us: u64,
    pub bitmap_cpu_us: u64,
    pub directory_bucket_load_us: u64,
    pub directory_resolve_us: u64,
    pub record_load_wall_us: u64,
    pub record_load_task_us: u64,
    pub record_filter_us: u64,
    pub block_scan_load_us: u64,
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
        self.bitmap_fragment_scans_skipped = self
            .bitmap_fragment_scans_skipped
            .saturating_add(other.bitmap_fragment_scans_skipped);
        self.bitmap_page_groups_planned = self
            .bitmap_page_groups_planned
            .saturating_add(other.bitmap_page_groups_planned);
        self.bitmap_page_groups_short_circuited = self
            .bitmap_page_groups_short_circuited
            .saturating_add(other.bitmap_page_groups_short_circuited);
        self.bitmap_page_jobs_baseline = self
            .bitmap_page_jobs_baseline
            .saturating_add(other.bitmap_page_jobs_baseline);
        self.bitmap_page_jobs_pruned = self
            .bitmap_page_jobs_pruned
            .saturating_add(other.bitmap_page_jobs_pruned);
        self.candidate_primary_ids = self
            .candidate_primary_ids
            .saturating_add(other.candidate_primary_ids);
        self.directory_hits = self.directory_hits.saturating_add(other.directory_hits);
        self.records_loaded = self.records_loaded.saturating_add(other.records_loaded);
        self.records_returned = self.records_returned.saturating_add(other.records_returned);
        self.primary_window_resolve_us = self
            .primary_window_resolve_us
            .saturating_add(other.primary_window_resolve_us);
        self.bitmap_io_us = self.bitmap_io_us.saturating_add(other.bitmap_io_us);
        self.bitmap_cpu_us = self.bitmap_cpu_us.saturating_add(other.bitmap_cpu_us);
        self.directory_bucket_load_us = self
            .directory_bucket_load_us
            .saturating_add(other.directory_bucket_load_us);
        self.directory_resolve_us = self
            .directory_resolve_us
            .saturating_add(other.directory_resolve_us);
        self.record_load_wall_us = self
            .record_load_wall_us
            .saturating_add(other.record_load_wall_us);
        self.record_load_task_us = self
            .record_load_task_us
            .saturating_add(other.record_load_task_us);
        self.record_filter_us = self.record_filter_us.saturating_add(other.record_filter_us);
        self.block_scan_load_us = self
            .block_scan_load_us
            .saturating_add(other.block_scan_load_us);
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
    let cache_before = tables.cache_snapshot();

    let primary_window_start = Instant::now();
    let Some(window) = resolve_primary_id_window(tables.blocks(), family, &block_window).await?
    else {
        let stats = QueryExecutionStats {
            primary_window_resolve_us: elapsed_us(primary_window_start),
            ..QueryExecutionStats::default()
        };
        trace_query_execution_stats(family, "indexed", &stats);
        trace_query_cache_stats(family, "indexed", &tables.cache_delta_since(&cache_before));
        return Ok(IndexedQueryOutcome {
            records: Vec::new(),
            span: BlockSpan {
                from_block,
                to_block,
                cursor_block: to_block,
            },
            stats,
        });
    };
    let primary_window_resolve_us = elapsed_us(primary_window_start);

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
        primary_window_resolve_us,
        ..QueryExecutionStats::default()
    };

    for shard in shards {
        stats.shards_visited = stats.shards_visited.saturating_add(1);
        let (local_from, local_to) = window.local_range_for_shard(shard);
        let open_fragment_page_start = (shard == window.end_inclusive.shard())
            .then_some(page_start_local(window.end_inclusive.local()));

        let Some(candidate_bitmap) = tables
            .family(family)
            .load_intersection_bitmap(
                &clauses,
                shard,
                local_from,
                local_to,
                open_fragment_page_start,
            )
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
            stats.directory_bucket_load_us = stats
                .directory_bucket_load_us
                .saturating_add(resolved.bucket_load_us);
            stats.directory_resolve_us = stats
                .directory_resolve_us
                .saturating_add(resolved.resolve_us);
            let locations: Vec<_> = resolved.locations.into_iter().flatten().collect();
            stats.directory_hits = stats.directory_hits.saturating_add(locations.len() as u64);

            let record_load_start = Instant::now();
            let materialized = materialize_locations_ordered(runner, locations).await;
            stats.record_load_wall_us = stats
                .record_load_wall_us
                .saturating_add(elapsed_us(record_load_start));

            for materialized in materialized {
                if let Some(stop_block) = stop_after_block {
                    if materialized.location.block_number != stop_block {
                        let cursor_block = runner.load_block_ref(stop_block).await?;
                        stats.records_returned = records.len() as u64;
                        trace_query_execution_stats(family, "indexed", &stats);
                        trace_query_cache_stats(
                            family,
                            "indexed",
                            &tables.cache_delta_since(&cache_before),
                        );
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
                stats.record_load_task_us = stats
                    .record_load_task_us
                    .saturating_add(materialized.load_us);

                // Most families guarantee `matches` for any candidate the bitmap
                // intersection produces. The trace family violates that
                // guarantee in one case — `is_top_level: Some(false)` combined
                // with other indexed clauses can pick up top-level frames that
                // the runner must drop here. Treat the post-filter as
                // authoritative rather than asserting.
                let filter_start = Instant::now();
                let matches = filter.matches(&record);
                stats.record_filter_us = stats
                    .record_filter_us
                    .saturating_add(elapsed_us(filter_start));
                if !matches {
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
    trace_query_cache_stats(family, "indexed", &tables.cache_delta_since(&cache_before));
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
    load_us: u64,
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
            let load_start = Instant::now();
            let record = runner
                .load_record_at(location.block_number, location.idx_in_block)
                .await;
            MaterializedRecord {
                location,
                record,
                load_us: elapsed_us(load_start),
            }
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
        bitmap_fragment_scans_skipped = stats.bitmap_fragment_scans_skipped,
        bitmap_page_groups_planned = stats.bitmap_page_groups_planned,
        bitmap_page_groups_short_circuited = stats.bitmap_page_groups_short_circuited,
        bitmap_page_jobs_baseline = stats.bitmap_page_jobs_baseline,
        bitmap_page_jobs_pruned = stats.bitmap_page_jobs_pruned,
        candidate_primary_ids = stats.candidate_primary_ids,
        directory_hits = stats.directory_hits,
        records_loaded = stats.records_loaded,
        records_returned = stats.records_returned,
        primary_window_resolve_us = stats.primary_window_resolve_us,
        bitmap_io_us = stats.bitmap_io_us,
        bitmap_cpu_us = stats.bitmap_cpu_us,
        directory_bucket_load_us = stats.directory_bucket_load_us,
        directory_resolve_us = stats.directory_resolve_us,
        record_load_wall_us = stats.record_load_wall_us,
        record_load_task_us = stats.record_load_task_us,
        record_filter_us = stats.record_filter_us,
        block_scan_load_us = stats.block_scan_load_us,
        io_wait_us = stats
            .primary_window_resolve_us
            .saturating_add(stats.bitmap_io_us)
            .saturating_add(stats.directory_bucket_load_us)
            .saturating_add(stats.record_load_wall_us)
            .saturating_add(stats.block_scan_load_us),
        cpu_work_us = stats
            .bitmap_cpu_us
            .saturating_add(stats.directory_resolve_us)
            .saturating_add(stats.record_filter_us),
        "chain data query execution stats"
    );
}

fn trace_query_cache_stats(family: Family, path: &'static str, cache_stats: &[CacheSnapshot]) {
    for cache in cache_stats {
        let total_lookups = cache.total_lookups();
        let hit_ratio = if total_lookups == 0 {
            0.0
        } else {
            cache.total_hits() as f64 / total_lookups as f64
        };
        trace!(
            ?family,
            path,
            table = cache.table,
            value_hits = cache.value_hits,
            none_hits = cache.none_hits,
            misses = cache.misses,
            hit_ratio,
            insertions = cache.insertions,
            evictions = cache.evictions,
            lookup_us = cache.lookup_us,
            lookup_lock_wait_us = cache.lookup_lock_wait_us,
            miss_load_us = cache.miss_load_us,
            populate_us = cache.populate_us,
            hit_bytes = cache.hit_bytes,
            miss_bytes = cache.miss_bytes,
            uncached_ops = cache.uncached_ops,
            uncached_us = cache.uncached_us,
            uncached_bytes = cache.uncached_bytes,
            entries = cache.entries,
            capacity = cache.capacity,
            "chain data query cache stats"
        );
    }
}

/// Shared block-scan runner used when the query filter carries no indexed
/// clause. Walks blocks in query order, lets the family filter each
/// block's records, and stops once `limit` is reached.
pub(crate) async fn execute_block_scan_family_query<M, B, R>(
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
    let cache_before = tables.cache_snapshot();
    let mut records = Vec::new();
    let mut cursor_block = from_block;
    let mut stats = QueryExecutionStats::default();

    for block_number in block_window.iter(order) {
        let block_load_start = Instant::now();
        let (block_ref, block_records) = runner
            .load_filtered_block_records(block_number, order, filter)
            .await?;
        stats.block_scan_load_us = stats
            .block_scan_load_us
            .saturating_add(elapsed_us(block_load_start));
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
    trace_query_cache_stats(
        R::family(),
        "block_scan",
        &tables.cache_delta_since(&cache_before),
    );

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

pub(crate) fn elapsed_us(start: Instant) -> u64 {
    start.elapsed().as_micros() as u64
}
