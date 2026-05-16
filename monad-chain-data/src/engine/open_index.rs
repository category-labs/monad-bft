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

use std::{
    collections::{BTreeSet, HashMap},
    hash::Hash,
    sync::Mutex,
};

use crate::engine::family::Family;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct DirectoryIndexKey {
    pub family: Family,
    pub bucket_start: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BitmapIndexKey {
    pub family: Family,
    pub stream_id: String,
    pub page_start_local: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BitmapOpenStreamsKey {
    pub family: Family,
    pub page_global_start: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct OpenIndexesDelta {
    pub directory_blocks: Vec<(Family, u64, u64)>,
    pub bitmap_blocks: Vec<(Family, String, u32, u64)>,
    pub bitmap_open_streams: Vec<(Family, u64, String)>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct OpenIndexesEviction {
    pub directory_buckets: Vec<(Family, u64)>,
    pub bitmap_pages: Vec<(Family, String, u32)>,
    pub bitmap_open_pages: Vec<(Family, u64)>,
}

impl OpenIndexesEviction {
    pub(crate) fn merge(&mut self, other: Self) {
        self.directory_buckets.extend(other.directory_buckets);
        self.bitmap_pages.extend(other.bitmap_pages);
        self.bitmap_open_pages.extend(other.bitmap_open_pages);
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct OpenIndexStats {
    pub directory_keys: u64,
    pub directory_blocks: u64,
    pub bitmap_stream_pages: u64,
    pub bitmap_blocks: u64,
    pub bitmap_open_pages: u64,
    pub bitmap_open_streams: u64,
    pub approx_bytes: u64,
}

#[derive(Debug)]
struct OpenFragmentIndex<K> {
    blocks_by_key: HashMap<K, BTreeSet<u64>>,
}

impl<K> Default for OpenFragmentIndex<K> {
    fn default() -> Self {
        Self {
            blocks_by_key: HashMap::new(),
        }
    }
}

impl<K> OpenFragmentIndex<K>
where
    K: Eq + Hash,
{
    fn insert(&mut self, key: K, block_number: u64) {
        self.blocks_by_key
            .entry(key)
            .or_default()
            .insert(block_number);
    }

    fn blocks(&self, key: &K) -> Vec<u64> {
        self.blocks_by_key
            .get(key)
            .map(|blocks| blocks.iter().copied().collect())
            .unwrap_or_default()
    }

    fn remove(&mut self, key: &K) {
        self.blocks_by_key.remove(key);
    }

    fn key_count(&self) -> u64 {
        self.blocks_by_key.len() as u64
    }

    fn block_count(&self) -> u64 {
        self.blocks_by_key
            .values()
            .map(|blocks| blocks.len() as u64)
            .sum()
    }
}

#[derive(Debug, Default)]
struct OpenIndexesInner {
    directory: OpenFragmentIndex<DirectoryIndexKey>,
    bitmap: OpenFragmentIndex<BitmapIndexKey>,
    bitmap_open_streams: HashMap<BitmapOpenStreamsKey, BTreeSet<String>>,
    rebuilt_for_head: Option<Option<u64>>,
}

#[derive(Debug, Default)]
pub(crate) struct OpenIndexes {
    inner: Mutex<OpenIndexesInner>,
}

impl OpenIndexes {
    pub(crate) fn rebuilt_for_head(&self) -> Option<Option<u64>> {
        self.inner
            .lock()
            .expect("open index poisoned")
            .rebuilt_for_head
    }

    pub(crate) fn replace_rebuilt(&self, head: Option<u64>, delta: OpenIndexesDelta) {
        let mut inner = self.inner.lock().expect("open index poisoned");
        inner.directory.blocks_by_key.clear();
        inner.bitmap.blocks_by_key.clear();
        inner.bitmap_open_streams.clear();
        apply_delta_locked(&mut inner, delta);
        inner.rebuilt_for_head = Some(head);
    }

    pub(crate) fn mark_rebuilt_for_head(&self, head: Option<u64>) {
        self.inner
            .lock()
            .expect("open index poisoned")
            .rebuilt_for_head = Some(head);
    }

    pub(crate) fn apply_delta(&self, delta: OpenIndexesDelta) {
        let mut inner = self.inner.lock().expect("open index poisoned");
        apply_delta_locked(&mut inner, delta);
    }

    pub(crate) fn apply_eviction(&self, eviction: OpenIndexesEviction) {
        let mut inner = self.inner.lock().expect("open index poisoned");
        for (family, bucket_start) in eviction.directory_buckets {
            inner.directory.remove(&DirectoryIndexKey {
                family,
                bucket_start,
            });
        }
        for (family, stream_id, page_start_local) in eviction.bitmap_pages {
            inner.bitmap.remove(&BitmapIndexKey {
                family,
                stream_id,
                page_start_local,
            });
        }
        for (family, page_global_start) in eviction.bitmap_open_pages {
            inner.bitmap_open_streams.remove(&BitmapOpenStreamsKey {
                family,
                page_global_start,
            });
        }
    }

    pub(crate) fn directory_blocks(&self, family: Family, bucket_start: u64) -> Vec<u64> {
        self.inner
            .lock()
            .expect("open index poisoned")
            .directory
            .blocks(&DirectoryIndexKey {
                family,
                bucket_start,
            })
    }

    pub(crate) fn bitmap_blocks(
        &self,
        family: Family,
        stream_id: &str,
        page_start_local: u32,
    ) -> Vec<u64> {
        self.inner
            .lock()
            .expect("open index poisoned")
            .bitmap
            .blocks(&BitmapIndexKey {
                family,
                stream_id: stream_id.to_owned(),
                page_start_local,
            })
    }

    pub(crate) fn bitmap_open_streams(
        &self,
        family: Family,
        page_global_start: u64,
    ) -> BTreeSet<String> {
        self.inner
            .lock()
            .expect("open index poisoned")
            .bitmap_open_streams
            .get(&BitmapOpenStreamsKey {
                family,
                page_global_start,
            })
            .cloned()
            .unwrap_or_default()
    }

    pub fn stats(&self) -> OpenIndexStats {
        let inner = self.inner.lock().expect("open index poisoned");
        let directory_keys = inner.directory.key_count();
        let directory_blocks = inner.directory.block_count();
        let bitmap_stream_pages = inner.bitmap.key_count();
        let bitmap_blocks = inner.bitmap.block_count();
        let bitmap_open_pages = inner.bitmap_open_streams.len() as u64;
        let bitmap_open_streams: u64 = inner
            .bitmap_open_streams
            .values()
            .map(|streams| streams.len() as u64)
            .sum();
        let approx_bytes = directory_keys
            .saturating_mul(24)
            .saturating_add(directory_blocks.saturating_mul(8))
            .saturating_add(bitmap_stream_pages.saturating_mul(80))
            .saturating_add(bitmap_blocks.saturating_mul(8))
            .saturating_add(bitmap_open_pages.saturating_mul(24))
            .saturating_add(bitmap_open_streams.saturating_mul(64));
        OpenIndexStats {
            directory_keys,
            directory_blocks,
            bitmap_stream_pages,
            bitmap_blocks,
            bitmap_open_pages,
            bitmap_open_streams,
            approx_bytes,
        }
    }
}

fn apply_delta_locked(inner: &mut OpenIndexesInner, delta: OpenIndexesDelta) {
    for (family, bucket_start, block_number) in delta.directory_blocks {
        inner.directory.insert(
            DirectoryIndexKey {
                family,
                bucket_start,
            },
            block_number,
        );
    }
    for (family, stream_id, page_start_local, block_number) in delta.bitmap_blocks {
        inner.bitmap.insert(
            BitmapIndexKey {
                family,
                stream_id,
                page_start_local,
            },
            block_number,
        );
    }
    for (family, page_global_start, stream_id) in delta.bitmap_open_streams {
        inner
            .bitmap_open_streams
            .entry(BitmapOpenStreamsKey {
                family,
                page_global_start,
            })
            .or_default()
            .insert(stream_id);
    }
}

pub(crate) fn insert_directory_set(
    delta: &mut OpenIndexesDelta,
    family: Family,
    bucket_start: u64,
    blocks: BTreeSet<u64>,
) {
    for block in blocks {
        delta.directory_blocks.push((family, bucket_start, block));
    }
}

pub(crate) fn insert_bitmap_set(
    delta: &mut OpenIndexesDelta,
    family: Family,
    stream_id: String,
    page_start_local: u32,
    blocks: BTreeSet<u64>,
) {
    for block in blocks {
        delta
            .bitmap_blocks
            .push((family, stream_id.clone(), page_start_local, block));
    }
}

#[cfg(test)]
mod tests {
    use super::{OpenIndexes, OpenIndexesDelta};
    use crate::engine::family::Family;

    #[test]
    fn apply_delta_twice_is_idempotent() {
        let indexes = OpenIndexes::default();
        let mut delta = OpenIndexesDelta::default();
        delta.directory_blocks.push((Family::Log, 0, 7));
        delta
            .bitmap_blocks
            .push((Family::Log, "topic/abcd/0".to_owned(), 0, 7));
        delta
            .bitmap_open_streams
            .push((Family::Log, 0, "topic/abcd/0".to_owned()));

        indexes.apply_delta(delta.clone());
        indexes.apply_delta(delta);

        assert_eq!(indexes.directory_blocks(Family::Log, 0), vec![7]);
        assert_eq!(
            indexes.bitmap_blocks(Family::Log, "topic/abcd/0", 0),
            vec![7]
        );
        assert_eq!(indexes.bitmap_open_streams(Family::Log, 0).len(), 1);
        let stats = indexes.stats();
        assert_eq!(stats.directory_blocks, 1);
        assert_eq!(stats.bitmap_blocks, 1);
        assert_eq!(stats.bitmap_open_streams, 1);
    }
}
