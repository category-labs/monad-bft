# BFT Index Migration — Code Change Breakdown

Companion to `bft-index-migration-plan.md`. Describes the code that was
landed for the migration, organized by area. All sections describe shipped
code unless explicitly marked otherwise.

## 1. redb-backed `KVStore` impl

**File:** `monad-archive/src/kvstore/redb.rs`

- One `redb::Database` per instance, held behind `Arc`, opened from a path
  passed via the `redb` variant of `ArchiveArgs`.
- One `TableDefinition<&str, &[u8]>` table — KVStore semantics are flat.
- Reads: short read transactions per `get`/`exists`/`scan_prefix_after_*`.
  redb read-txns are cheap and parallel.
- Writes: redb is single-writer. Wrapped with a tokio mutex around the
  write txn; the lock is released only after `commit()`.
- Async glue: redb is sync, so `tokio::task::spawn_blocking` for txn
  bodies. A multi-thread test (16 tasks × 50 puts) pins the
  write-lock + `spawn_blocking` contract.
- `WritePolicy::NoClobber`: existence check inside the same write txn.
- Registered in the `enum_dispatch` macro list in `kvstore/mod.rs` so it
  joins `KVStoreErased`.

## 2. Key schema centralization

**File:** `monad-archive/src/model/bft_paths.rs`

- Helpers, all returning `String` or `&'static str`:
  - `header_path(id)` → `bft/ledger/headers/<4hex>/<hex>`
  - `body_path(body_id)` → `bft/ledger/bodies/<4hex>/<hex>`
  - `index_page_path(seq_num)` / `index_page_path_for_page(page)`
    → `bft/index/<page>`
  - `index_page_for(seq_num)` / `index_page_offset(seq_num)`
  - `marker_path(head_num)` → `bft/index_markers/<head_num>`
  - `legacy_header_path(id)` / `legacy_body_path(body_id)`
  - `legacy_header_suffix()` for filtering listed keys
  - `pull_cursor_path(shard)` and `body_copy_cursor_path()` for
    inventory-free resume
- Inverse parsers: `parse_header_path`, `parse_body_path`,
  `parse_legacy_header_path`, `parse_legacy_body_path`,
  `parse_index_per_key_path`, `parse_marker_path`. Both sharded parsers
  validate that the leading hex chars match the shard segment.
- Constants: `HEX_SHARD_CHARS = 4`, `INDEX_PAGE_SIZE = 1000`,
  `INDEX_ENTRY_BYTES = 32`. Defined here, used nowhere else.
- Round-trip tests for every helper/parser pair.

## 3. Paged-index abstraction

**File:** `monad-archive/src/model/index_backend.rs`

- Trait `IndexBackend`:
  - `get_id(seq_num) -> Option<BlockId>`
  - `put_id(seq_num, id)` — read-modify-write on paged backends
  - `put_ids_batch(entries)` — fast path for bulk uploads. Backend-defined
    semantics: on `KvIndexBackend` it's a per-key bulk put; on
    `PagedIndexBackend` it's an **unconditional full-page write**. Caller
    asserts the entries are the intended full state of each touched page.
    No GET, no shared mutex — pages run independently for parallel
    upload. Documented + asserted by `paged_batch_clobbers_prior_entries_on_same_page`.
  - `iter_range(min, max_inclusive)` — page-aware streaming, used by
    reconcile. `PagedIndexBackend::iter_range` uses `.buffered(32)` so
    S3 GET latency overlaps across pages while preserving ascending order.
- All-zero `BlockId` is the unindexed sentinel — symmetric across both
  backends so intra-page gaps from canary/partial runs read as `None`
  rather than a valid-looking zero id.
- Two impls under `IndexBackendErased` (enum_dispatch):
  - `KvIndexBackend` — wraps a `KVStoreErased`, stores one key per
    seq_num at `bft/index_legacy_perkey/<n>`. Used as the **local**
    backend during indexing.
  - `PagedIndexBackend` — wraps a `KVStoreErased` (typically S3),
    `put_id` is RMW under an internal `write_lock`; `put_ids_batch`
    is unconditional. Used by the live archiver and by the upload tool.
- `BftBlockModel` gained an `index: IndexBackendErased` field separate
  from its `KVStoreErased`. `BftBlockModel::with_index` lets a caller
  (e.g. `monad-archiver`) pick the paged backend explicitly while the
  migrator stays on `KvIndexBackend`.

## 4. Indexer hardening (`migrate_bft_archive.rs`)

- **Missing-data path** errors out (`return Err(...)`) and increments
  `BFT_MIGRATION_SUBCHAIN_STALLED`; the marker is preserved across runs
  for operator investigation.
- **Genesis termination**: `IndexerConfig::genesis_seq_num` (CLI
  `--genesis-seq-num`, default 0). The chain-walk indexes the genesis
  block, then returns `BlockStepResult::Genesis` instead of recursing.
- **Canary mode**: `IndexerConfig::max_blocks` (CLI `--max-blocks`).
  Stops cleanly after the global block counter hits the cap. Markers
  are deleted on Ok exit, so canary runs are intentionally non-resumable.
- **Discovery from a seed list**: `IndexerConfig::seed_tips` (CLI
  `--seed-tips-file`). When provided, replaces the legacy
  `scan_prefix_with_max_keys` sampling. Each seed runs through
  `find_committable_head` exactly as before.
- **`BlockStepResult::{Advance, Genesis, Canary}`** replaces the prior
  `Option<(BlockId, u64)>` return so the orchestrator's log message
  matches the actual stop reason when both conditions could fire on the
  same block.

## 5. Reconciliation tool

**File:** `monad-archive/examples/reconcile_bft_index.rs`

- Inputs:
  - `--source` (`ArchiveArgs`): typically the local redb header cache.
  - `--sink` (`ArchiveArgs`): the index — local redb during the verify
    phase, S3 after upload (`--paged-sink` toggles the layout).
  - `--min-seq-num` / `--max-seq-num`: the range to verify.
- Windowed: `--window-size` (default 100K) bounds peak memory regardless
  of total range. Across windows we carry only the last slot's BlockId
  for the chain-contiguity check.
- Passes per window:
  1. Sink index lookup + source header GET, parallelized via
     `buffer_unordered(--concurrency)`.
  2. Hash-chain contiguity within the window plus the boundary check
     against the previous window's tail.
  3. Body presence (`--skip-body-check` to skip when bodies are still
     on the legacy path mid-migration).
  4. After the loop: markers postcondition
     (`scan_prefix(bft/index_markers/)` is empty).
- Output: structured JSON via `--json-out` plus a console summary.
  Exit non-zero on any failure. Sample lists are bounded by
  `--max-failures-per-kind`.
- **Not implemented:** `--reindex-gaps`. Operators re-run the indexer
  over the affected range; `tail_already_indexed` keeps the operation
  idempotent.

## 6. Bulk-pull tool

**File:** `monad-archive/examples/pull_legacy_headers.rs`

- **No inventory file.** Drives `KVReader::scan_prefix_after_with_max_keys`
  in a loop against the source bucket — pages of up to 1000 keys at a
  time, fed straight into a parallel GET+PUT pipeline.
- Sharded by leading hex chars (`--shard-hex-chars`, default 1 → 16
  shards). Each shard runs an independent LIST loop in parallel; concurrent
  shards capped at 64 to bound `shards × fetch_concurrency` in flight.
- **Resume cursor** stored in the sink at
  `bft/migration/pull_cursor/<shard>`. In-process the cursor advances on
  every page (so a transient failure on one key doesn't block forward
  progress); the persisted cursor only advances after a fully-clean page,
  so a process restart re-attempts the failed keys. NoClobber + the
  `skip_existing` cheap-redb-lookup path make replay safe and fast.
- `--fail-fast` aborts new dispatches on the first error.
- `WritePolicy::NoClobber` for sink PUTs — content-addressed bytes mean
  racing live-archiver writes are inherently safe.

## 7. Body migration tool

**File:** `monad-archive/examples/migrate_bft_bodies.rs`

- **No inventory file.** Iterates the local redb header cache (`--header-cache`):
  paginated `scan_prefix_after_with_max_keys` across `bft_block/`, decode
  each cached header, derive `block_body_id`, build the legacy and
  destination body paths.
- Server-side `Bucket::copy_from` (added on `Bucket` so callers don't
  reach under the abstraction). Same-region, free egress.
- HEAD-based idempotency: simple objects compare ETags; multipart
  objects (ETag has `<md5>-<parts>` form) fall back to content-length
  match because CopyObject can re-layout multipart parts. Same-region
  copies preserve bytes (and thus length), so length-match on a present
  destination is a sound idempotency signal.
- `--delete-source` is opt-in; default off until end-to-end verification
  is complete.
- Resume cursor at `bft/migration/body_copy_cursor` in the same redb;
  same advance-on-clean-page semantics as the bulk-pull tool.

## 8. Index-page upload tool

**File:** `monad-archive/examples/upload_index_pages.rs`

- Reads the per-key local index via `KvIndexBackend`, writes paged blobs
  to S3 via `PagedIndexBackend::put_ids_batch`.
- Pages run under `buffer_unordered(--concurrency)`; each page is
  independently idempotent because `put_ids_batch` is unconditional and
  deterministic for a fixed input range.
- Boundary pages (where `--min-seq-num` / `--max-seq-num` falls
  mid-page) are safe under the unconditional-write semantics because the
  caller passes only the entries it knows about — which by definition
  is the full intended state for those slots.

## 9. CLI plumbing

- `ArchiveArgs` (`monad-archive/src/cli.rs`) gained the `redb <path>`
  variant alongside `fs <path>`/`aws <bucket>`/etc.
- `monad-indexer` flags: `--seed-tips-file`, `--genesis-seq-num`,
  `--max-blocks`. The IndexBackend choice is implicit
  (`BftBlockModel::with_index`): the migrator uses `KvIndexBackend`,
  the live archiver uses `PagedIndexBackend`.
- Reconciliation, bulk-pull, body migration, and paged upload all ship
  as **examples**, not subcommands of `monad-indexer`. Migration tooling
  is one-shot — folding it into the live binary would mean every prod
  build carries dead code post-migration. `cargo run --release --example <name>`
  is the operator UX.

## 10. KVStore trait extension

**File:** `monad-archive/src/kvstore/mod.rs`

`KVReader` gained one new required method:

```rust
async fn scan_prefix_after_with_max_keys(
    &self,
    prefix: &str,
    after: &str,
    max_keys: usize,
) -> Result<Vec<String>>;
```

Returns up to `max_keys` keys with the given `prefix`, in lex order,
strictly greater than `after`. The existing `scan_prefix_with_max_keys`
becomes a default that calls the new method with `after = ""`.

- **S3** uses native `start-after` for the first request, then
  continuation tokens.
- **redb** does a B-tree range scan with `lower = max(prefix, after)`,
  filtering `k > after` after the iterator yields.
- **memory / fs / mongo** filter + sort to honor lex order.
- **dynamodb / cloud_proxy** keep their `unimplemented!()` bail-out
  (these backends never see prefix scans in production).

This is what makes the inventory-free design possible: every paginated
streaming scan in `pull_legacy_headers`, `migrate_bft_bodies`, and the
resume-cursor reads goes through this one trait method.

## 11. Tests / canary mode

- `cargo test -p monad-archive` exercises trait-conformance for
  `MemoryStorage` and `RedbStorage` (tempdir).
- redb has a dedicated test for the resume-cursor contract
  (`scan_prefix_after_resumes_strictly_greater`).
- Round-trip tests for every `bft_paths` helper/parser.
- `IndexBackend` round-trip tests for both impls — including the
  paged backend's intra-page zero-gap and unconditional-batch contracts.
- Canary mode (`--max-blocks`) on the indexer for cheap end-to-end
  validation against a 1% slice before the full 120M run.

## Order of work (as landed)

1. Helpers + sharded paths (#2).
2. redb backend (#1).
3. `IndexBackend` abstraction + per-key impl (#3).
4. Indexer hardening (#4).
5. `PagedIndexBackend` (#3, second commit).
6. Reconciliation (#5).
7. Bulk pull (#6) — initial inventory-driven shape.
8. Body migration (#7) — initial inventory-driven shape.
9. Paged upload (#8).
10. Canary + cleanups (#11, polish).
11. **Trait extension `scan_prefix_after_with_max_keys` (#10) plus
    rewrite of `pull_legacy_headers` and `migrate_bft_bodies` to drop
    inventory files in favor of streaming LIST and redb-driven
    iteration.** This is the most recent change and what the as-shipped
    plan reflects.
