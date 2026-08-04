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

use std::sync::Arc;

use monad_query_engine::{
    family::Family,
    tables::{DictConfig, QueryRuntimeConfig, Tables},
    test_util::{stage_block, stage_block_header, test_cache_config},
};
use monad_query_store::{
    test_util::{ObservedMetaStore, OpCounters},
    CacheConfig, InMemoryBlobStore,
};

fn make_tables(
    cache: CacheConfig,
) -> (
    Tables<ObservedMetaStore, InMemoryBlobStore>,
    Arc<OpCounters>,
) {
    let meta = ObservedMetaStore::counting();
    let blob = InMemoryBlobStore::default();
    let counters = meta.counters.clone();
    let tables = Tables::with_all_configs(
        meta,
        blob,
        cache,
        DictConfig::default(),
        QueryRuntimeConfig::default(),
    );
    (tables, counters)
}

#[tokio::test(flavor = "current_thread")]
async fn first_read_misses_then_serves_from_cache() {
    let (tables, counters) = make_tables(test_cache_config());
    stage_block(&tables, 1).await;

    // Caches are read-populated only: writes never seed them.
    let before = counters.snapshot();
    let v = tables.blocks().load_header(1).await.expect("load_header");
    assert!(v.is_some());
    let after_first = counters.snapshot();
    assert_eq!(
        after_first.get - before.get,
        1,
        "first read after write must miss to the backend"
    );

    let v = tables.blocks().load_header(1).await.expect("load_header");
    assert!(v.is_some());
    let after_second = counters.snapshot();
    assert_eq!(
        after_second.get, after_first.get,
        "second read must serve from cache without a backend get"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn scan_put_then_scan_get_serves_from_cache() {
    let (tables, counters) = make_tables(test_cache_config());
    let tables_ref = &tables;
    tables
        .with_writes(|w| {
            Box::pin(async move {
                tables_ref
                    .family(Family::Log)
                    .dir()
                    .stage_block_fragment(w, 1, 0, 1);
                Ok(())
            })
        })
        .await
        .expect("with_writes");

    let fam = tables.family(Family::Log);
    // load_bucket_fragments = one scan_keys + one cached scan_get per clustering.
    let before = counters.snapshot();
    let fragments = fam.load_bucket_fragments(0).await.expect("load");
    assert_eq!(fragments.len(), 1);
    let after_first = counters.snapshot();
    assert_eq!(
        after_first.scan_get - before.scan_get,
        1,
        "first load misses one scan_get to the backend"
    );
    assert_eq!(
        after_first.scan_keys - before.scan_keys,
        1,
        "scan_keys is uncached and runs exactly once"
    );

    let fragments = fam.load_bucket_fragments(0).await.expect("load");
    assert_eq!(fragments.len(), 1);
    let after_second = counters.snapshot();
    assert_eq!(
        after_second.scan_get, after_first.scan_get,
        "second load serves scan_gets from cache"
    );
    assert_eq!(
        after_second.scan_keys - after_first.scan_keys,
        1,
        "scan_keys runs again (uncached)"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn staged_write_not_visible_until_committed() {
    // Staged writes never seed the cache (production parity: ingest runs with caches disabled).
    let (tables, _counters) = make_tables(test_cache_config());
    let tables_ref = &tables;

    tables
        .with_writes(|w| {
            Box::pin(async move {
                stage_block_header(tables_ref, w, 1);
                let read = tables_ref.blocks().load_header(1).await?;
                assert!(
                    read.is_none(),
                    "staged-but-uncommitted write must not be visible in-closure"
                );
                Ok(())
            })
        })
        .await
        .expect("with_writes");

    // The in-closure miss must not be cached (absence never is), so the
    // committed value is visible to the very next read.
    let read = tables.blocks().load_header(1).await.expect("load_header");
    assert!(
        read.is_some(),
        "committed write must be visible despite the pre-commit miss"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cache_eviction_is_not_correctness_bug() {
    // 256 bytes holds one or two decoded records, forcing evictions.
    let cache = CacheConfig {
        block_header_cache_bytes: 256,
        ..test_cache_config()
    };
    let (tables, _counters) = make_tables(cache);

    for i in 1..=5u64 {
        stage_block(&tables, i).await;
    }

    for i in 1..=5u64 {
        let v = tables.blocks().load_header(i).await.expect("load");
        assert!(v.is_some(), "value at block {i} must still be reachable");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn zero_size_cache_skips_lru() {
    let cache = CacheConfig {
        block_header_cache_bytes: 0,
        ..test_cache_config()
    };
    let (tables, counters) = make_tables(cache);
    stage_block(&tables, 1).await;

    let before = counters.snapshot();
    let _ = tables.blocks().load_header(1).await.expect("load");
    let after_first = counters.snapshot();
    assert_eq!(
        after_first.get - before.get,
        1,
        "zero-size cache disables caching: read must hit backend"
    );

    let _ = tables.blocks().load_header(1).await.expect("load");
    let after_second = counters.snapshot();
    assert_eq!(
        after_second.get - after_first.get,
        1,
        "zero-size cache must not serve the second read"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn populate_keys_match_scan_get_keys() {
    let (tables, counters) = make_tables(test_cache_config());
    let tables_ref = &tables;

    tables
        .with_writes(|w| {
            Box::pin(async move {
                let dir = tables_ref.family(Family::Log).dir();
                dir.stage_block_fragment(w, 1, 0, 5);
                dir.stage_block_fragment(w, 2, 5, 7);
                Ok(())
            })
        })
        .await
        .expect("stage");

    let fam = tables.family(Family::Log);
    // Prime via read (writes never seed caches); the second load must be fully cached.
    let fragments = fam.load_bucket_fragments(0).await.expect("load");
    assert_eq!(fragments.len(), 2);

    let before = counters.snapshot();
    let fragments = fam.load_bucket_fragments(0).await.expect("load");
    assert_eq!(fragments.len(), 2);
    let after = counters.snapshot();
    assert_eq!(
        after.scan_get, before.scan_get,
        "per-clustering scan_gets must observe ZERO backend calls once cached"
    );
}
