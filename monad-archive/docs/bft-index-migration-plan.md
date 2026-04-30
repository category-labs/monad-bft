# BFT Block Index Migration Plan

One-shot migration of ~120M legacy BFT blocks (`bft_block/<hash>.{header,body}`) into the new ledger format. The migration must be complete (no gaps), correct (right bytes), and resumable.

## Storage layout (new schema)

- **Headers**: `bft/ledger/headers/<shard>/<id>` — shard = first 4 hex chars of `<id>`. ~65K shards × ~1,830 entries.
- **Bodies**: `bft/ledger/bodies/<shard>/<id>` — same scheme, keyed by `body_id`.
- **Index**: page-based at `bft/index/<page>` — page covers seq_nums `[page*1_000, (page+1)*1_000)`. Each page is a contiguous blob of fixed-stride 32-byte block_id entries (~32 KB full). Random reads use HTTP byte-range; sequential scans get 1K entries per round-trip. Page size chosen small enough that the live archiver can rewrite the full current page on every committed block (or every few blocks) without burning bandwidth.
- All key construction goes through helpers (`header_path()`, `body_path()`, `index_page_for_seq()`, etc.) so the schema is one-file to change.

## Local infrastructure

- Format `nvme1n1` as XFS, mount `/mnt/staging`.
- Wrap **redb** behind a new `KVStore`/`KVReader` impl. (rocksdb is a fine alternative; redb preferred for being pure-Rust.)
- Two databases on EBS:
  - **Legacy header cache** (~60 GB): every legacy header pulled once.
  - **New index** (~10 GB): seq_num → block_id, written as the indexer runs.
- Bodies are **not** pulled locally. They migrate via S3 server-side `CopyObject` + `DeleteObject`.

## Phase 1 — Bulk header pull

- `examples/pull_legacy_headers.rs` drives a streaming `ListObjectsV2` against
  the legacy bucket and pipes each page directly into a parallel GET+PUT
  pipeline writing into the local redb on `/mnt/staging`. No inventory file,
  no S3-Inventory daily wait — LIST is paginated via the new
  `KVReader::scan_prefix_after_with_max_keys` trait method (S3 maps it onto
  `start-after` + continuation tokens; redb maps it onto a B-tree range scan).
- Sharded by leading hex char (`bft_block/0`..`bft_block/f` by default,
  16-way) so LISTs run in parallel; each shard maintains its own resume
  cursor at `bft/migration/pull_cursor/<shard>` inside the local redb. A
  crashed run picks up where the last clean page left off without
  re-LISTing.
- Bodies are skipped — they migrate via server-side `CopyObject` after
  indexing, driven off the same local redb.
- Saturates EC2 ↔ S3 throughput (m6a.8xlarge, 12.5 Gbps). Expect ~1–2 hours.

## Phase 2 — Indexing (local only)

- Run the indexer with `--source` pointing at the legacy redb, `--sink` at
  the new index redb.
- Discovery seeds sub-chain heads from `--seed-tips-file` (operator-provided
  list of committable head BlockIds) when available; otherwise falls back to
  `scan_prefix_with_max_keys` sampling against the local redb.
- Sub-chain walks read from local redb → microsecond reads. The chain-walk
  concurrency limit (one tokio task per sub-chain) is fine because there's
  no network round-trip per block.
- DB commit batches align with the existing `max_num_per_batch` marker
  boundary so resume semantics are clean.
- When `--copy-data` is set, the indexer dual-writes each indexed header to
  the new sharded path `bft/ledger/headers/<shard>/<hex>` as it goes.
  Headers therefore land in their final layout at index time — there is no
  separate "Phase 5 header copy" step in this plan.

## Phase 3 — Verify & reconcile (local only)

Two distinct passes; do not conflate.

### a. Completeness pass — `examples/reconcile_bft_index.rs`

- Walks the sink index across `[min..=max]` in fixed `--window-size`
  chunks (default 100K) so peak RAM is bounded regardless of the total
  range. For every indexed seq_num it verifies:
  1. the entry's header exists in the source (the local redb header cache)
     and decodes,
  2. the header's `seq_num` matches the index slot,
  3. the parent_id forms a contiguous hash chain with seq_num − 1
     (cross-window boundary carries one BlockId),
  4. the header's body is reachable in the source (skippable via
     `--skip-body-check`).
- Postcondition: `bft/index_markers/` is empty (any leftover marker means
  at least one sub-chain stopped without joining).
- Output: structured JSON report + console summary, exit non-zero on any
  failure. Fixing gaps means re-running the indexer over the affected
  range — `tail_already_indexed` keeps the operation idempotent.

### b. Correctness pass — `examples/verify_bft_copy_correctness.rs`

- Sample-based byte equality between source and sink. Catches drift; does
  not catch gaps.

## Phase 4 — Upload to S3

After both verification passes are clean:

1. **Index pages → S3** (`examples/upload_index_pages.rs`): reads the
   per-key local index, writes one paged blob per 1K-block range to S3 via
   `PagedIndexBackend::put_ids_batch`. Pages run under
   `buffer_unordered`; each page is independently idempotent. ~120K pages,
   ~$0.60.
2. **Bodies → S3** (`examples/migrate_bft_bodies.rs`): server-side
   `CopyObject` from `bft_block/<hash>.body` to
   `bft/ledger/bodies/<shard>/<hash>`. Driven off the local redb header
   cache — each cached header carries `block_body_id`, which names the
   legacy body key — so no body LIST or inventory file is needed. Resume
   cursor at `bft/migration/body_copy_cursor` in the same redb. Same-region
   copies, free egress, ~$600 in CopyObject charges.
3. **Cleanup**: `DeleteObject` on legacy paths once destinations are
   confirmed. Headers were already written to the new sharded path during
   Phase 2 indexing (see `--copy-data`); only legacy `bft_block/*` keys
   need cleanup. Opt-in via `--delete-source` on `migrate_bft_bodies` and
   a similar flag on the legacy-header sweep.

## Code hardening (already landed; see git log on branch)

- Key construction centralized in `model/bft_paths.rs`.
- `index_sub_chain` missing-data path now errors and preserves the marker for
  operator investigation; `BlockStepResult::{Advance, Genesis, Canary}`
  distinguishes the three terminal outcomes.
- `--genesis-seq-num` CLI flag short-circuits cleanly at genesis instead of
  spuriously failing on legitimate chain ends.
- Reconciliation tool ships as `examples/reconcile_bft_index.rs`.
- redb-backed `KVStore`/`KVReader` impl (single-writer / multi-reader, write
  txns wrapped with a tokio mutex, all txn bodies on `spawn_blocking`).
- `KVReader::scan_prefix_after_with_max_keys(prefix, after, max_keys)`
  trait extension — paginated, lex-ordered, strictly-greater-than `after`
  semantics. S3 uses `start-after` + continuation tokens; redb does a
  range scan with `lower = max(prefix, after)`. Drives all
  inventory-free streaming in Phases 1 and 4.

## Live-archiver index page behavior

The page format is also what the live archiver writes going forward. Because pages are small (~32 KB), the simplest correct behavior is:

- The **current** page (the one whose seq_num range the live tip is inside) is rewritten in full on every committed block, or every few blocks if write rate becomes a concern.
- Sealing happens implicitly: when the live tip crosses a 1K boundary, the previous page has its final shape and is never rewritten again.
- Readers always look up `bft/index/<page>` regardless of whether the page is sealed or current. A partial page returns however many entries have been committed so far; the byte-range offset for any seq_num is still well-defined and stable.

This avoids any "tail vs sealed" distinction in the schema. The full-rewrite cost per block is bounded: 32 KB PUT (vs 320 KB at 10K page size) is what motivated the smaller page choice.

## Coordination with the live archiver

- Live archiver runs in **dual-write** mode (legacy + new format) for the duration of migration.
- Migration uses `WritePolicy::NoClobber` for headers/bodies — content-addressed, so racing writers are safe.
- Index entries write `AllowOverwrite` but are deterministic on the canonical chain → also safe. The full-page rewrite means historical pages are immutable once their range is past the tip.
- After migration completes, re-run the migration over the dual-write window to pick up any blocks the live archiver wrote during the transition.
- Live archiver then cuts over to new-format-only.

## Cost summary (one-shot)

| Step | Requests | Cost |
|---|---|---|
| LIST calls (Phase 1, ~120K @ 1000/page) | 120K | ~$0.60 |
| Header GETs (Phase 1) | 120M | ~$48 |
| Index page PUTs (Phase 4) | 120K | ~$0.60 |
| Header sharded-path PUTs (Phase 2 dual-write) | 120M | ~$600 |
| Body server-side copies (Phase 4) | 120M | ~$600 |
| EBS hours (~24h, 2× 2 TB gp3) | — | ~$20 |
| **Total** | | **~$1,300** |

## Risk register

| Risk | Mitigation |
|---|---|
| Discovery misses some legacy keys | Phase 1 LIST is paginated and exhaustive per shard; cursor only advances after a clean page so transient errors trigger replay. The local redb after Phase 1 is itself the authoritative ground truth used by every later phase |
| LIST returns stale view because live archiver is writing | Live archiver dual-writes during the migration window; post-migration re-run sweeps the dual-write window. `NoClobber` PUTs into redb make a re-listing safe |
| Silent stop on missing source data | Error-on-missing patch + completeness pass |
| Sub-chains never stitch | Completeness pass + empty-markers postcondition |
| Live-archiver race | `NoClobber` + post-migration re-run window |
| Local DB corruption mid-run | Marker-aligned commit batches; resume from last marker; per-shard pull cursors only advance after a clean page |
| EBS performance shortfall | Both volumes are gp3; throughput is far above what S3 latency-bound GETs deliver |

## Out of scope

- Rewrites of legacy archived data outside the BFT block scope.
- Reader-side changes in `monad-rpc` / archive checker beyond what the new schema requires.
- Retention policy for legacy paths after migration.
