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

pub(crate) mod bitmap_compaction;
pub(crate) mod directory_compaction;
pub(crate) mod persist;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReadPlanningTimings {
    pub dir_list_ms: u64,
    pub dir_get_ms: u64,
    pub dir_list_count: u64,
    pub dir_get_count: u64,
    pub bitmap_open_streams_ms: u64,
    pub bitmap_open_streams_us: u64,
    pub bitmap_open_streams_count: u64,
    pub bitmap_list_ms: u64,
    pub bitmap_get_ms: u64,
    pub bitmap_shape_ms: u64,
    pub bitmap_shape_us: u64,
    pub bitmap_union_us: u64,
    pub bitmap_frontier_us: u64,
    pub bitmap_open_write_us: u64,
    pub bitmap_index_ms: u64,
    pub bitmap_index_us: u64,
    pub bitmap_compact_ms: u64,
    pub bitmap_compact_us: u64,
    pub bitmap_compact_wall_us: u64,
    pub bitmap_list_count: u64,
    pub bitmap_get_count: u64,
    pub bitmap_fragment_count: u64,
    pub bitmap_fragment_bytes: u64,
    pub bitmap_compact_count: u64,
    pub bitmap_frontier_stream_count: u64,
    pub bitmap_union_page_count: u64,
    pub bitmap_union_stream_count: u64,
    pub bitmap_touched_page_count: u64,
    pub bitmap_touched_stream_count: u64,
    pub bitmap_final_open_stream_count: u64,
    pub dir_index_ms: u64,
    pub dir_index_us: u64,
    pub dir_decode_ms: u64,
    pub dir_decode_us: u64,
    pub dir_fragment_count: u64,
}

impl ReadPlanningTimings {
    pub(crate) fn merge(&mut self, other: Self) {
        self.dir_list_ms = self.dir_list_ms.saturating_add(other.dir_list_ms);
        self.dir_get_ms = self.dir_get_ms.saturating_add(other.dir_get_ms);
        self.dir_list_count = self.dir_list_count.saturating_add(other.dir_list_count);
        self.dir_get_count = self.dir_get_count.saturating_add(other.dir_get_count);
        self.bitmap_open_streams_ms = self
            .bitmap_open_streams_ms
            .saturating_add(other.bitmap_open_streams_ms);
        self.bitmap_open_streams_us = self
            .bitmap_open_streams_us
            .saturating_add(other.bitmap_open_streams_us);
        self.bitmap_open_streams_count = self
            .bitmap_open_streams_count
            .saturating_add(other.bitmap_open_streams_count);
        self.bitmap_list_ms = self.bitmap_list_ms.saturating_add(other.bitmap_list_ms);
        self.bitmap_get_ms = self.bitmap_get_ms.saturating_add(other.bitmap_get_ms);
        self.bitmap_shape_ms = self.bitmap_shape_ms.saturating_add(other.bitmap_shape_ms);
        self.bitmap_shape_us = self.bitmap_shape_us.saturating_add(other.bitmap_shape_us);
        self.bitmap_union_us = self.bitmap_union_us.saturating_add(other.bitmap_union_us);
        self.bitmap_frontier_us = self
            .bitmap_frontier_us
            .saturating_add(other.bitmap_frontier_us);
        self.bitmap_open_write_us = self
            .bitmap_open_write_us
            .saturating_add(other.bitmap_open_write_us);
        self.bitmap_index_ms = self.bitmap_index_ms.saturating_add(other.bitmap_index_ms);
        self.bitmap_index_us = self.bitmap_index_us.saturating_add(other.bitmap_index_us);
        self.bitmap_compact_ms = self
            .bitmap_compact_ms
            .saturating_add(other.bitmap_compact_ms);
        self.bitmap_compact_us = self
            .bitmap_compact_us
            .saturating_add(other.bitmap_compact_us);
        self.bitmap_compact_wall_us = self
            .bitmap_compact_wall_us
            .saturating_add(other.bitmap_compact_wall_us);
        self.bitmap_list_count = self
            .bitmap_list_count
            .saturating_add(other.bitmap_list_count);
        self.bitmap_get_count = self.bitmap_get_count.saturating_add(other.bitmap_get_count);
        self.bitmap_fragment_count = self
            .bitmap_fragment_count
            .saturating_add(other.bitmap_fragment_count);
        self.bitmap_fragment_bytes = self
            .bitmap_fragment_bytes
            .saturating_add(other.bitmap_fragment_bytes);
        self.bitmap_compact_count = self
            .bitmap_compact_count
            .saturating_add(other.bitmap_compact_count);
        self.bitmap_frontier_stream_count = self
            .bitmap_frontier_stream_count
            .saturating_add(other.bitmap_frontier_stream_count);
        self.bitmap_union_page_count = self
            .bitmap_union_page_count
            .saturating_add(other.bitmap_union_page_count);
        self.bitmap_union_stream_count = self
            .bitmap_union_stream_count
            .saturating_add(other.bitmap_union_stream_count);
        self.bitmap_touched_page_count = self
            .bitmap_touched_page_count
            .saturating_add(other.bitmap_touched_page_count);
        self.bitmap_touched_stream_count = self
            .bitmap_touched_stream_count
            .saturating_add(other.bitmap_touched_stream_count);
        self.bitmap_final_open_stream_count = self
            .bitmap_final_open_stream_count
            .saturating_add(other.bitmap_final_open_stream_count);
        self.dir_index_ms = self.dir_index_ms.saturating_add(other.dir_index_ms);
        self.dir_index_us = self.dir_index_us.saturating_add(other.dir_index_us);
        self.dir_decode_ms = self.dir_decode_ms.saturating_add(other.dir_decode_ms);
        self.dir_decode_us = self.dir_decode_us.saturating_add(other.dir_decode_us);
        self.dir_fragment_count = self
            .dir_fragment_count
            .saturating_add(other.dir_fragment_count);
    }
}
