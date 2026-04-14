# Plan: per-table read-through cache (RAM tier)

Port the `roaring-monad` caching pattern to `monad-chain-data`. RAM-only
for this commit. NVMe / cascade deferred.

Landed as a stack of commits. The first three are prerequisite
refactors; the last two are the cache work itself. Prerequisite
refactors (1) and (2) are **history rewrites** applied back to the
commit that introduced the dead surface — `monad-chain-data` is
pre-production, so we keep the history clean rather than layering
migrations.

## Stack

1. **Remove `delete` from store traits.** History rewrite at
   `a832d77f8` (initial commit). Cherry-pick the rest of the stack on
   top; the only intermediate collision is `19f8f4296`, which also
   touched `DelCond`.
2. **Migrate `bitmap_page_blob` → metadata kv table.** History rewrite
   at `19f8f4296` ("Compact sealed log bitmap pages"), replay
   `6a230f7ce` ("Introduce typed engine substrate") on top. No other
   intermediate commit references the symbol.
3. **Relax `BlobStore::read_range` to clamp on overshoot.** New commit
   on top of the rewritten stack. Callers are unaffected (all current
   `end` values are derived from header offsets and sit within the
   blob); only the cache-layer range walker benefits.
4. **Cache mechanism.** Generic primitive + wrappers under `store/`.
   Unused on its own — lets the retrofit stay focused in the next
   commit.
5. **Cache policy + retrofit + service API + smoke test.** Engine
   wiring, per-table `CacheConfig`, `MonadChainDataService` constructor
   and `cache_metrics` accessor, `tests/cache_smoke.rs`.

---

## Prerequisite 1: remove `delete` from store traits

Zero production callers in `src/`. One test touchpoint,
`tests/log_bitmap_pages.rs:188`, which calls `delete_blob` directly on
`InMemoryBlobStore` to simulate missing-data corruption.

### Rewrite at `a832d77f8`

- `BlobStore`: drop `async fn delete_blob(...)`.
- `MetaStore`: drop `async fn delete(...)` and `async fn scan_delete(...)`.
- Drop `pub enum DelCond` entirely.
- Drop `BlobTable::delete`, `KvTable::delete`, `ScannableKvTable::delete`.
- `InMemoryMetaStore` / `InMemoryBlobStore`: drop the impl methods.
- Initial-commit tests that exercised delete (if any): remove.

### Collision at `19f8f4296`

This commit added `DelCond::IfVersion(u64)` alongside bitmap-page work.
The bitmap migration for prerequisite 2 lands here too. Amend once:
drop the `DelCond` change, apply the bitmap migration.

### Test fix

`tests/log_bitmap_pages.rs:188` was simulating "what happens if the
compacted page blob disappears?" Post-rewrite, `bitmap_page_blob` lives
in the meta store (prerequisite 2), and delete is gone. Replace the
`blob_store.delete_blob(...)` call with a test-only helper on
`InMemoryMetaStore`:

```rust
impl InMemoryMetaStore {
    pub fn clear_key(&self, table: TableId, key: &[u8]);            // remove a kv row
    pub fn clear_scan_key(&self, table: ScannableTableId,
                          partition: &[u8], clustering: &[u8]);      // remove a scan row
}
```

These are concrete methods on the in-memory fixture, not trait
methods. The real backends have no equivalent (append-only).

---

## Prerequisite 2: migrate `bitmap_page_blob` → metadata kv

`bitmap_page_blob` is bounded at roughly 8 KiB + a 13-byte framing
header + roaring's own small header. Only consumed whole via `get(key)`
— never `read_range`, never `list_prefix`. It belongs in the meta
store.

### Rewrite at `19f8f4296`

- `BitmapTables::page_blobs: BlobTable<B>` → `KvTable<M>`.
- `BitmapTables::new` no longer takes a `BlobTable` parameter.
- `FamilyTables::new` construction: swap
  `blob_store.table(BlobTableId::leaked(format!("{}_bitmap_page_blob", prefix)))`
  for `meta_store.table(TableId::leaked(format!("{}_bitmap_page_blob", prefix)))`.
- `BitmapTables::page_blob_table() -> BlobTableId` → remove. This
  accessor is only used by tests and is no longer meaningful.
- `BitmapTables::load_page_blob` / `store_page_blob` return / accept
  `Bytes` — same shape, just via `KvTable`.
- `FamilyTables::bitmap_page_blob_table()` → remove (see above).

### Replay `6a230f7ce`

"Introduce typed engine substrate" moved table wiring around but
didn't change `page_blobs`'s semantic layer. Replay with the updated
type in hand. Likely trivial conflict or none.

### Downstream impact on the stack

No intermediate commit (`19f8f4296 .. HEAD`) references `page_blobs`,
`load_page_blob`, `store_page_blob`, or `bitmap_page_blob_table`
besides `19f8f4296` and `6a230f7ce`. The rest of the stack cherry-picks
cleanly.

---

## Prerequisite 3: relax `BlobStore::read_range` on overshoot

Today: `read_range(table, key, start, end_exclusive)` returns
`MonadChainDataError::Decode("invalid blob range")` when `end_exclusive
> blob.len()`.

New contract: `read_range` clamps `end_exclusive` to `blob.len()` and
returns the short tail. Still errors on `start > end_exclusive` or
`start > blob.len()` (those are caller bugs).

- `InMemoryBlobStore::read_range`: replace the bounds error with a
  clamp.
- The `BlobStore` trait default impl: same.
- No existing caller overshoots — header-derived offsets are all inside
  the blob — so behavior is unchanged for them.

This commit stands alone so the change is reviewable without cache
noise.

---

## Cache design (prerequisite-free after the three above)

### Module layout (mechanism in `store/`, policy in `engine/`)

```
src/store/cache.rs                # NEW — generic mechanism
  TableCacheConfig { max_bytes }
  BlobTableCacheConfig { max_bytes, chunk_size }
  TableCacheMetrics
  HashMapTableCache<V, W>                  # generic primitive
  BytesWeigher, RecordWeigher

src/store/meta/mod.rs             # EXTEND
  CachedKvTable<M>                         # wraps KvTable<M>
  CachedScannableKvTable<M>                # wraps ScannableKvTable<M>

src/store/blob/mod.rs             # EXTEND
  CachedBlobTable<B>                       # wraps BlobTable<B>

src/engine/cache.rs               # NEW — policy
  CacheConfig, FamilyCacheConfig           # per-table field layout
  CacheMetrics                             # mirrors CacheConfig
```

`store/` wrappers are generic byte-in / byte-out decorators with no
knowledge of specific tables. `engine/` owns the named table inventory
and default sizing.

### KV / scannable cache: full `Record` stored

`MetaStore` is fully versioned. Cache stores `Record { version, value }`
so callers see the real version on a hit.

- `HashMapTableCache<V, W>` is generic. Type aliases:
  `type KvCache = HashMapTableCache<Record, RecordWeigher>;`
  `type BytesCache = HashMapTableCache<Bytes, BytesWeigher>;`
- `CachedKvTable::get(key) -> Option<Record>` — read-through.
  Returns the cached `Record` on hit; fetches, populates, returns on
  miss.
- `CachedKvTable::put(key, bytes) -> PutResult` — write-through.
  On `applied == true`, insert `Record { version: result.version.expect("applied"), value: bytes }`. On `!applied`,
  do not touch the cache.
- No `delete` (trait doesn't expose one).

`CachedScannableKvTable` has the same contract keyed by
length-prefixed `u32::to_be_bytes(partition.len()) || partition ||
clustering`. `list_prefix` is **passthrough** — never reads from or
writes to the cache. Rationale: list results are paged and typically
scan fresh territory; populating from them inflates entry count without
a proportional hit benefit, and the cache stays a point-read structure.

### Blob cache: chunked, `get_uncached`, no size needed

Cache key: `blob_key || chunk_idx.to_be_bytes()` (u32 BE). Cache value
is the chunk's `Bytes`.

Chunk size is per-table config (`BlobTableCacheConfig::chunk_size`).
Default **512 KiB** at the `store/cache.rs` layer; the engine is
unopinionated and just threads the value through. Constraint:
positive power of two (enforced at config build time).

#### `read_range(key, start, end) -> Option<Bytes>`

```text
chunk_start = start / C
chunk_end   = div_ceil(end, C)                       # exclusive

probe each chunk idx in [chunk_start..chunk_end) against the cache.
collect contiguous runs of misses as (run_start_idx, run_end_idx).

for each miss run:
    byte_start = run_start_idx * C
    byte_end   = run_end_idx   * C                   # relaxed store clamps the tail
    let fetched = store.read_range(key, byte_start, byte_end).await?;
    match fetched {
        None => return Ok(None),                     # blob absent
        Some(b) => split b into per-chunk slices sized C (last may be short),
                   insert each into cache,
    }

walk [chunk_start..chunk_end), get cached bytes for each,
splice into an output buffer, slice to [start - chunk_start*C, end - chunk_start*C).
return Ok(Some(out)).
```

Key properties:
- **Coalesced miss fetch.** One `store.read_range` per contiguous miss
  run, not per chunk. Cold range covering 3 chunks = 1 store call.
- **No size required.** Relies on the relaxed `read_range` contract
  (prerequisite 3) — the last chunk of a blob may come back short; we
  cache it short.
- **No splicing across chunks when the value is in one chunk.** With
  the 512 KiB default, virtually every single-tx read sits inside one
  chunk; the splice path is a bounded fallback for values straddling a
  chunk boundary.

#### `get_uncached(key) -> Option<Bytes>`

Passthrough. No cache read, no cache write. Replaces the current
`CachedBlobTable::get(key)` shape — explicit naming makes the
bypass-cache intent obvious at call sites. Used by full-blob decoders
(unfiltered block scans), where caching a multi-MB value to serve one
query would pollute the working set for the point-read workload.

#### `put(key, bytes)`

Write-through. After `store.put` succeeds, chunk `bytes` into segments
of `C` (last short) and insert each under `(blob_key, idx)`. Fresh
writes are likely to be read soon; warming the chunks avoids a cold
first read.

#### No `delete`, no `list_prefix` cache interaction

`delete` is gone from the trait (prerequisite 1). `list_prefix`
passthrough (never populated from, never read from). Same rationale as
the scannable case.

### Cacheable tables (17 total — down from 18 after the bitmap move)

#### Global (4)

| Table              | Type | Owner              |
| ------------------ | ---- | ------------------ |
| `block_records`    | kv   | `BlockTables`      |
| `block_header`     | kv   | `BlockTables`      |
| `block_hash_index` | kv   | `BlockTables`      |
| `tx_hash_index`    | kv   | `TxHashIndexTable` |

#### Per-family, × 2 families (Log, Tx) (6.5 each = 13 after the move)

| Table                | Type      | Owner                 |
| -------------------- | --------- | --------------------- |
| `block_header`       | kv        | `FamilyTables`        |
| `block_blob`         | blob      | `FamilyTables`        |
| `dir_bucket`         | kv        | `PrimaryDirTables`    |
| `dir_fragments`      | scannable | `PrimaryDirTables`    |
| `bitmap_page_meta`   | kv        | `BitmapTables`        |
| `bitmap_page_blob`   | kv        | `BitmapTables` (moved)|
| `bitmap_fragments`   | scannable | `BitmapTables`        |

#### Skipped

| Table                 | Reason                                        |
| --------------------- | --------------------------------------------- |
| `publication_state`   | Single key; no benefit.                       |
| `open_bitmap_streams` | Pure `list_prefix` workload; no point `get`.  |

So the only blob table in the cache graph is `block_blob`, in both
Log and Tx families. Every other cache slot is kv/scannable-kv.

### Config shape

```rust
// src/store/cache.rs
pub struct TableCacheConfig {
    pub max_bytes: u64,                  // 0 disables
}
impl TableCacheConfig { pub const fn disabled() -> Self { ... } }

pub struct BlobTableCacheConfig {
    pub max_bytes: u64,                  // 0 disables
    pub chunk_size: u32,                 // positive power of two; default 512 KiB
}
impl BlobTableCacheConfig {
    pub const DEFAULT_CHUNK_SIZE: u32 = 512 * 1024;
    pub const fn disabled() -> Self { Self { max_bytes: 0, chunk_size: Self::DEFAULT_CHUNK_SIZE } }
}
```

```rust
// src/engine/cache.rs
pub struct CacheConfig {
    pub block_records: TableCacheConfig,
    pub block_header: TableCacheConfig,
    pub block_hash_index: TableCacheConfig,
    pub tx_hash_index: TableCacheConfig,
    pub logs: FamilyCacheConfig,
    pub txs: FamilyCacheConfig,
}
impl CacheConfig { pub const fn disabled() -> Self { ... } }

pub struct FamilyCacheConfig {
    pub block_header: TableCacheConfig,
    pub block_blob: BlobTableCacheConfig,
    pub dir_bucket: TableCacheConfig,
    pub dir_fragments: TableCacheConfig,
    pub bitmap_page_meta: TableCacheConfig,
    pub bitmap_page_blob: TableCacheConfig,    // kv after migration
    pub bitmap_fragments: TableCacheConfig,
}
impl FamilyCacheConfig { pub const fn disabled() -> Self { ... } }
```

Default = disabled. External override arrives via
`MonadChainDataService::new_with_cache_config`.

### Retrofit

Constructors thread their cache config through. Internal field types
flip from `KvTable` / `BlobTable` / `ScannableKvTable` to the
corresponding `Cached*` wrapper. Accessor signatures don't change —
consumers call `.get(key)` on the accessor as before.

1. `TxHashIndexTable::new(meta, config)`.
2. `PrimaryDirTables::new(frag, buckets, family_config)`.
3. `BitmapTables::new(frag, page_meta, page_blob, open, family_config)`.
4. `FamilyTables::new(meta, blob, family, family_config)`.
5. `BlockTables::new(meta, config)`.
6. `Tables::new(meta, blob, cache_config)`.

### Service API

```rust
impl<M: MetaStore, B: BlobStore> MonadChainDataService<M, B> {
    pub fn new(meta: M, blob: B, limits: QueryLimits) -> Self {
        Self::new_with_cache_config(meta, blob, limits, CacheConfig::disabled())
    }

    pub fn new_with_cache_config(
        meta: M,
        blob: B,
        limits: QueryLimits,
        cache: CacheConfig,
    ) -> Self { ... }

    pub fn cache_metrics(&self) -> CacheMetrics { ... }
}
```

Existing `new(...)` stays byte-for-byte identical in behavior — zero
churn to existing test construction sites.

### Metrics

`TableCacheMetrics { hits, misses, inserts, evictions, bytes_used }`.
`hits` / `misses` / `bytes_used` read from
`quick_cache::sync::Cache::{hits, misses, weight}`. `inserts` /
`evictions` come from a `MetricsLifecycle` we own. `CacheMetrics`
mirrors `CacheConfig`'s field layout (including the `logs` / `txs`
split) so consumers navigate config and metrics the same way.

### Testing

Existing tests must pass untouched under the disabled default.

Add `tests/cache_smoke.rs`:

1. Enable `tx_hash_index` (1 MiB) and `logs.block_blob` (1 MiB,
   `chunk_size = 4 KiB` so the test can land on chunk boundaries
   without ingesting MBs).
2. Ingest two small blocks with a handful of logs and txs.
3. `get_transaction(hash)` twice on the same hash →
   `tx_hash_index.hits == 1` after the second call.
4. A logs query that triggers `block_blob` ranges, repeated →
   `logs.block_blob.hits > 0` on the second call.
5. Disabled-by-default table (e.g. `block_records`) stays at zero
   across both calls.
6. Chunk-boundary case: a range spanning two chunks → two chunk entries
   present, `bytes_used ≈ chunk_size + tail_size`. Asserts that
   coalesced fetch produced the expected number of cache inserts (= 2,
   not 1 concatenated blob).

No white-box unit tests of the cache primitive — service-level
assertions pin the wrapper contract.

### Invariants we rely on, not check

- **Append-only / single-writer ingest.** No two writers compete to
  update the same key with different values; the
  `cache.get → store.get → cache.put` sequence can't race itself into
  an old value because there is no "old value" being concurrently
  written.
- **Re-put with a different blob length does not happen.** The cache
  does not track per-blob length, so stale chunks from a replaced blob
  would poison reads. Sealed, immutable ingest means this doesn't occur.
  Documented in the module rustdoc, not enforced in code.

---

## Verification per commit

Every commit in the stack, before the next lands:
- `cargo test -p monad-chain-data`
- nightly `fmt`
- project clippy invocation

## Open items

- **History-rewrite mechanics without `-i`.** CLAUDE.md forbids `git
  rebase -i`. Use checkout-branch-at-parent + cherry-pick-with-amend +
  cherry-pick-remainder for each rewrite point.
- **`quick_cache` workspace dep** is already staged in the working
  tree from earlier iteration; folds into the cache-mechanism commit
  (step 4).
