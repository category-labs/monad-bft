# monad-chain-data — docs & test-suite comprehension notes

Crate: `/home/jhow/monad-bft/monad-chain-data` (branch `jhow/chain-data/feature`). Git history for the dir is squashed to 3 commits (`e2d8bba7` rebuild, `c14b2d0a` remove fjall, `5c9b91fe` initial ingest) — no fjall/shard/lease code remains anywhere in `src/` or `tests/` (only two historical comments in `src/engine/bitmap.rs:778,857` about retired shard-era ids).

---

## 1. Existing docs inventory

Only two docs exist: `docs/onboarding.md` and `docs/ops-runbook.md`. The `queryX` spec is at the **crate root** (`/home/jhow/monad-bft/monad-chain-data/queryX`), not in `docs/`. The five deep-dive docs linked from onboarding.md **do not exist**.

### docs/onboarding.md — section-by-section verdict

| Section | Verdict | Notes |
|---|---|---|
| "What this crate is" | accurate | Service API in `src/api.rs`, transport elsewhere — correct. |
| "Families" | accurate | `Family` enum in `src/engine/family.rs` ✓; transfers-as-trace-view ✓ (`has_transfer` bit, `IndexKind::HasTransfer` in `src/engine/bitmap.rs:512`). |
| "Primary IDs — the spine" | accurate | `STREAM_PAGE_ID_SPAN = 64*1024` (`src/engine/bitmap.rs:33`), `PAGE_GROUP_ID_SPAN = 1<<24` (`:37`), page-relative bits, version-byte format note — all verified. |
| "Blocks tie ids back to chain position" | **stale in 2 details** | Struct sketch names a field `artifact_checksum`; the real field is `row_chain` (`src/primitives/records.rs:177`). "Compare `RawLogEntry` (stored) vs `LogEntry`" — the stored type is **`StoredLog`** (`src/logs/types.rs:38`); `RawLogEntry` doesn't exist. |
| "How a record is physically stored" | **partially stale** | Says "Per family, per block, there's a blob and a header". Reality: **one shared blob object per block** (`BLOCK_BLOB_TABLE`, key = block number) holding contiguous per-family regions (logs → txs → traces), each family having its own header with `base_offset` + relative `offsets` (pinned by `tests/shared_block_blob.rs`). Row encoding (RLP → per-row zstd frame) and dict_version are accurate. |
| "The two indexes" | **stale in 1 detail** | "bucketed in ranges of 10,000 ids" — wrong: `DIRECTORY_BUCKET_SIZE = STREAM_PAGE_ID_SPAN = 65,536` (`src/engine/primary_dir.rs:27`). Sealed-summary vs open-fragment routing and `render_stream_id` are accurate. |
| "The published head" | mostly accurate, **CAS language stale** | `PublicationState { indexed_finalized_head, head_row_chain }` ✓. But "guarded by compare-and-swap" / "the head moves in one CAS" — there is **no CAS anywhere**; the publisher does a plain single-row put (`src/ingest/publisher.rs`). Atomicity comes from it being one row; single-writer is an *operational* rule (ops-runbook), not enforced. |
| "The query path" (window, indexed vs scan, pipeline, mem_scan) | accurate, line refs stale | All cited modules/functions exist: `ResolvedBlockWindow::resolve*` (`src/primitives/range.rs:34`), `has_indexed_clause` (`src/engine/clause.rs:81`), `execute_indexed_family_query` (`src/engine/query/family_runner.rs:543`, doc says 97), `execute_block_scan_family_query` (`:935`, doc says 309), `intersect_group_page` (`src/engine/query/bitmap.rs:196`). "read `query_logs` at `src/api.rs:1791`" — **api.rs is 384 lines**; `query_logs` is at `src/api.rs:181`. Distributive-law explanation, block-aligned limit, `get_transaction` hash index, `src/mem_scan.rs` parity — all accurate. |
| "The ingest path / four tasks / accumulators / publication" | conceptually accurate, **paths stale** | Cites `src/ingest_core.rs`, `src/ingest_helpers.rs`, `src/backfill.rs`, `src/live.rs`, `src/ingest_config.rs`, `src/ingest_recover.rs`, `src/ingest_source.rs`, `src/family.rs` — **none exist**. Actual layout: `src/ingest/{mod,producer,data_track,index,publisher,recover,snapshot,source,resolver,probe,rtt}.rs`, `src/config/{mod,ingest,reader}.rs`, `src/ingest_types.rs` (`FinalizedBlock` here, not `src/family.rs`). Also `FetchPlan` doesn't exist (only `SignalPolicy` + `Prefetch`). `src/ingest/mod.rs:32` states flatly: "There is no backfill/live mode" — onboarding's backfill-vs-live framing is softer than the code's. |
| "Write authority — who's allowed to publish" | **entirely stale** | `src/engine/authority.rs` does not exist. No lease, no `acquire_or_wait`, no fencing, no lease field in `PublicationState`. The lease layer was removed. Single-writer is now the ops rule "exactly one ingest writer per store pair". |
| "Recovery" | accurate except lease ref | `max(checkpoint_block, published_head)` ✓ (`src/ingest/recover.rs:16,68`); checkpoint-vs-fragment-rebuild regimes ✓; above-head clamp ✓. "happens *after* lease acquire" — stale (no lease). |
| "Standby verification" | accurate | `row_chain`, `head_row_chain`, per-family seal chains, `MonadChainDataService::standby_digests` (`src/api.rs:349`), `src/engine/digest.rs` — all verified. |
| "Map of the crate" table | **several rows stale** | Rows for ingest engine (`ingest_core.rs`/`ingest_helpers.rs`), controllers (`backfill.rs`/`live.rs`/`ingest_config.rs`), recovery (`ingest_recover.rs`), write authority (`engine/authority.rs`), and "meta store (CAS)" are wrong per above. Other rows correct. |
| "Deep dives" | **all 5 links dead** | `deep-dive-frontier-model.md`, `deep-dive-primary-standby.md` (also describes removed lease machinery), `deep-dive-ingest-batching.md`, `deep-dive-recovery.md`, `deep-dive-bitmap-index.md` — none exist in `docs/`. |
| "The one paragraph to remember" | accurate except "one compare-and-swap" | Same CAS caveat. |

### docs/ops-runbook.md — verdict: **current and accurate** (pasted from category-infra 2026-06-11)

Spot-checked claims, all verified against code:
- `ingest::timing` 5s throughput probe ✓ (`src/ingest/probe.rs:31`, `blocks_per_s` at `:202`).
- Defaults: `fetch_concurrency = 2000`, `fetch_buffer = 5000`, `pack_max_blocks = 10_000`, `checkpoint_every_blocks = 10_000`, `tip_lag_divisor = 10`, flush cadence `max(distance_to_tip / tip_lag_divisor, 1)` ✓ (`src/config/ingest.rs:71–113`).
- "no start-block knob", "resume from max(checkpoint, published_head)", "backfill vs live is not a mode switch" ✓.

Sections: header/canonical-pointer; one-writer rule; prerequisites; §1 credentials (incl. a thorough Scylla Alternator authorization-gap explanation); §2 TOML config; §3 connectivity smoke tests; §4 systemd enable; §5 verify/log expectations; Operations. All **accurate**; coverage is ops-only (acceptable scope).

---

## 2. queryX spec summary (`/home/jhow/monad-bft/monad-chain-data/queryX`)

Draft MIP introducing five JSON-RPC methods: `eth_queryBlocks`, `eth_queryTransactions`, `eth_queryLogs`, `eth_queryTraces`, `eth_queryTransfers`. Motivation: filtering beyond logs, joins (relations), field selection, sane pagination.

**Common request**: `filter` (method-specific), `fields` (per-schema field selection, `string[] | true`), `order` (`asc` default / `desc`), `fromBlock`/`toBlock` (hex quantity or tag `latest|earliest|safe|finalized`, resolved server-side at execution; inclusive; **role swaps with order** — in `desc`, `fromBlock` is the *upper* bound), `limit` (target count of primary objects, default 100).

**Common response**: `data` keyed by object type; three block refs `{number, hash, parentHash}`: `fromBlock`/`toBlock` (resolved endpoints at execution time) and `cursorBlock` (last block scanned, inclusive).

**Key semantics the engine must honor** (the contract the tests pin):
- **Block-aligned responses**: the server always *completes the current block* before stopping — limit is a target, may be exceeded, never split a block across pages.
- **Pagination**: next page starts at `cursorBlock ± 1`; done when `cursorBlock == toBlock`.
- **Limit applies only to primary objects**; relations don't count.
- **Only many-to-one joins** (log→tx, log→block, etc.), normalized + deduplicated in separate arrays.
- **Errors**: `-32602` invalid params; `-32005` limit exceeded with `data: { maxLimit, maxBlockRange }`.
- Per-method filters: txs `{from, to, selector}` (selector skips input < 4 bytes); logs `{address, topics}` (eth_getLogs positional semantics, ≤4 entries, null wildcard); traces `{from, to, selector, isTopLevel}`; transfers `{from, to, isTopLevel}`; blocks: no filter.
- TODOs in spec: reorg-detection treatment, backwards compatibility, security.

**Spec ↔ crate split** (important for docs): the engine implements numeric `Option<u64>` endpoints, boolean relations, and full-row entries. Tag resolution (`latest`…), `fields` projection, and JSON-RPC error-code mapping live in the transport (monad-rpc handlers/chaindata wire.rs). Engine-side error model: `MonadChainDataError::LimitExceeded { kind: Limit|BlockRange, max_limit, max_block_range }` (→ -32005) and `InvalidRequest(&str)` (→ -32602).

---

## 3. Behavior catalog from tests

28 integration test files (~110 tests) + 124 unit tests in `src/`. All integration query/ingest tests populate stores **through the real engine** (`testkit::populate_via_engine`), so they pin end-to-end ingest→publish→query behavior.

### Query — envelope/range resolution (`tests/query_logs_range_resolution.rs`)
- `from_block` above published head (asc) → `InvalidRequest("block range starts above the published head")` — *never silently collapses*.
- `to_block` above head (asc) → **clamped** to head. Desc is mirrored: `to_block` (lower bound) above head → error; `from_block` (upper bound) above head → clamp.
- Inverted ranges error in both orders (in desc, `from < to` is the inverted case).
- `from_block = 0` floors to `EARLIEST_QUERYABLE_BLOCK = 1` — block numbering starts at 1.
- `None` endpoints default to full chain.
- **Published head 0 = "no published blocks"** → `MissingData("no published blocks")`; head 0 is a pre-first-publish sentinel (`src/engine/tables.rs:942–949`).

### Query — limits (`tests/query_logs_limits.rs`)
- `limit > max_limit` → `LimitExceeded { kind: Limit, … }` carrying both maxima.
- range > `max_block_range` → `LimitExceeded { kind: BlockRange }`; range == max succeeds (inclusive bound).
- **Defaulted (None) endpoints are still bounded** — a defaulted full-chain range exceeding `max_block_range` errors rather than silently clamping. (Surprise for new contributors.)

### Query — indexed pipeline (`tests/query_logs_indexed.rs`) — *the best worked-example file*
- AND-of-OR filter semantics: `address ∈ {2,3} AND topic0=9 AND topic1=10` selects exactly the one matching log.
- Descending returns newest-first **within a block too** (log_index 1 before 0).
- **Block-aligned limit**: `one_five_one_log_chain` (blocks with 1/5/1 matches, limit 2) returns **6** logs and `cursor_block = 2` — limit reached mid-block ⇒ finish the block; symmetric descending. If limit met exactly at a block boundary, the cursor stops there.
- Pagination resumes from `cursor_block + 1` (asc).
- Crossing directory-bucket and bitmap-page boundaries returns complete results (fixtures sized `PAGE_SPAN - 2` then `BUCKET + 4`).
- `historical_indexed_query_resolves_through_the_sealed_summary`: filling past `DIRECTORY_BUCKET_SIZE` seals bucket 0; queries resolve through the summary and through mixed sealed/open ranges.
- `indexed_query_pipeline_preserves_global_order_across_blocks`: 25 blocks × 3 logs, both orders — concurrent stages must emit strict query order **with no sort**.
- `indexed_query_cursor_completes_block_spanning_page_boundary`: a single block of `PAGE_SPAN + 8` logs spans bitmap pages 0 and 1; limit 5 still returns all `PAGE_SPAN+8` rows of the block (block alignment beats page boundaries).

### Query — block scan path (`tests/query_logs_scan.rs`)
- No indexed clause (`LogFilter::default()`) → block scan; same block-aligned-limit + cursor + pagination semantics as indexed.
- Descending scan returns newest-first within block.
- `block_scan_orders_blocks_and_lands_cursor_under_concurrency`: concurrent block reads (`buffered`) must preserve input order; limited scans land cursor on the last *emitted* block in both orders.

### Query — relations (`tests/query_logs_blocks_relation.rs`, `tests/query_logs_transactions_relation.rs`, `tests/query_traces.rs`)
- Relations are opt-in (`false` → `None` in response).
- Blocks relation: deduped, **only blocks that matched**, hashes + full header fields round-trip, and — surprise — **always returned ascending even for descending queries**.
- Transactions relation: deduped (two logs from one tx → one tx), sorted by `(block_number, tx_index)`, only txs referenced by matched logs; empty result ⇒ `Some([])` not `None`.

### Query — per-family filters
- **Txs** (`tests/query_transactions.rs`): `from` across blocks; `to` filter **excludes contract creation** (`to = None`); `selector` matches first 4 input bytes and **skips txs with input < 4 bytes**; no filter → scan in block order. `TxEntry.sender` is the stored sender field, not the recovered signer (`tests/common/mod.rs:273` comment).
- **Traces** (`tests/query_traces.rs`): `from`, `selector`, `is_top_level: true` keep roots; `is_top_level: false` keeps non-roots; **`is_top_level=false` combined with indexed clauses is a post-filter on indexed candidates**. Also `compute_trace_addresses` unit-pins depth-list → trace-address derivation.
- **Transfers** (`tests/query_transfers.rs`): qualification rules pinned exactly — value > 0, `CallKind::Call`/`SelfDestruct` qualify, **DelegateCall never**, zero value never, reverted tx (`tx_status: false`) never, failed frame (`status != 0`) never; nested calls qualify. `is_top_level` and `from` filters work over the projection.
- **Blocks** (`tests/query_blocks.rs`): asc/desc full range, limit+cursor, paginate from cursor±1, header field round-trip.

### Query — bitmap internals (`tests/query_bitmap_page_intersection.rs`, `tests/query_bitmap_page_counts_manifest.rs`)
- Clauses matching in **disjoint pages** intersect to `None` (the distributive per-page law).
- Per-page intersection keeps only ids present in every clause on that page.
- **Per-page short-circuit**: with caching off and a counting store, sparse-driver-first ordering does 3+1=4 page-blob fetches vs naive 6; reversed ordering does 3+2=5 — the asymmetry pins the win to short-circuiting, not caching. (Great doc example.)
- **Manifest skipping is seal-state-gated, not data-gated**: identical seeded data, frontier in group 1 ⇒ sealed group 0's zero-count pages skipped with **0 fetches**; frontier inside group 0 ⇒ ≥3 fetches ("absent manifest ⇒ unknown, never a skip").
- `query_crossing_group_boundary_returns_exact_ids_in_both_orders`: sealed artifact + manifest in group 0, open fragment in frontier group 1, manifest-proven-empty page in range — exact ids both orders, exactly 2 fetches.

### Ingest (`tests/log_ingest.rs`, `tests/tx_ingest.rs`, `tests/trace_ingest.rs`, `tests/get_transaction.rs`)
- **Gapless id windows**: blocks 1(2 logs)/2(0)/3(1) ⇒ windows (0,2),(2,0),(2,1) — an empty block's `first_primary_id` equals the next block's. Same for txs and traces.
- Per-block family headers report `row_count`; the family region length equals the last relative offset; trace region starts at the family's `base_offset`.
- **Undecodable signed tx bytes abort ingest** with `Decode("invalid signed tx envelope")` — fail-fast (`tx_ingest.rs::ingest_rejects_invalid_signed_tx_bytes`).
- Empty traces fine (count 0).

### Storage layout & artifacts (`tests/shared_block_blob.rs`, `tests/block_header_artifact.rs`, `tests/block_hash_index.rs`)
- **One blob object per block** in `BLOCK_BLOB_TABLE` (key = BE block number), regions contiguous: `log.base_offset = 0`, `tx.base_offset = end(log)`, `trace.base_offset = end(tx)`; total object length = end(trace). Indexed and scan queries for all three families read through this shared object.
- **No legacy tables written**: asserts absence of `block_record`, `block_header`, `log_block_header`, `tx_block_header`, `trace_block_header` rows; metadata is in `block_metadata`.
- `EvmBlockHeader` round-trips with all optional fields populated incl. Monad-specific `block_access_list_hash`, `slot_number`.
- `load_header` of a missing block → `Ok(None)`.
- Block-hash → number index: distinct per block, unknown hash → `None`.

### get_transaction (`tests/get_transaction.rs`)
- Returns `(TxEntry, EvmBlockHeader)` pair; resolves by hash across blocks; unknown hash → `None`; contract-creation tx decodes with `to() == None`.
- **Publication gating on point lookups**: with tx artifacts present but the publication row removed, a hash-index hit must NOT resolve (`get_transaction_ignores_index_hits_without_published_head`).

### Write session (`tests/session.rs`)
- `with_writes` closure: error ⇒ nothing flushed; staged writes **invisible mid-session** (no read-your-own-writes) but visible to the very next read after commit (absence is never cached); error/panic evicts any cache entries the closure populated; partial `apply_writes` failure evicts cache; retry after failure is idempotent; meta and blob `apply_writes` fire **in parallel** (timed-overlap assertion).

### Caching (`tests/cache.rs`, `tests/row_cache.rs`)
- Caches are **read-populated only** — writes never seed them (production parity: ingest runs caches-off).
- Zero-size budget disables a cache; eviction never a correctness bug; `scan_keys` uncached while per-clustering `scan_get`s are cached; window stats reset on `take_cache_window_stats`.
- Row cache: second `get_transaction`/indexed query does **zero blob reads**; only materialized rows are cached; **materialize read coalescing** merges near rows into one ranged read, splitting when the gap exceeds `materialize_span_max_gap_bytes` (`near_rows_share_one_read_far_rows_split`); transfers hit rows cached by a prior trace query.

### Row codec / dictionaries (`tests/row_codec_dict.rs`)
- `dict_version` stamped on every block header == `block / epoch_blocks`; round-trips across epoch boundaries on both paths.
- `ensure_epoch_dict` is idempotent (same bytes both times).
- A family with too few training samples publishes an **empty sentinel dict** and still round-trips (plain frames).

### Store backends (`tests/blob_store.rs`, `tests/dynamo_blobstore.rs`, `tests/dynamo_metastore.rs`, `tests/s3_blobstore.rs`)
- `read_range` trait contract pinned identically across in-memory, dynamo, s3: end overshoot clamps; `start == len` returns empty; `start > len` or reversed range → `"decode error: invalid blob range"`; missing key → `None`. `delete_blob` idempotent everywhere.
- DynamoBlobStore chunking: tiny chunk sizes force multi-chunk paths — >25-item batching, ranged reads across chunk boundaries, delete removes *every* chunk. **`overwrite_with_shorter_multi_chunk_payload_has_no_stale_tail` documents a real hazard**: bare overwrite with fewer chunks leaves a stale tail (asserted!); delete-before-put is the fix, which `SnapshotStore::store` performs. Excellent doc material.
- DynamoMetaStore: round trips; `scan_keys` drains a whole partition in clustering order (400 × 4KB rows forces paging); `apply_writes` crosses the 25-item batch limit; empty apply is a no-op.
- S3BlobStore: ranged GET semantics incl. errors; multi-table `apply_writes` with binary keys.

### RPC conversion (`tests/tx_rpc_conversion.rs`, feature `alloy-rpc-types-eth`)
- `TxEntry::to_rpc_transaction` carries block context, signer, tx type; `effective_gas_price` is `None`; `to()`/`selector()` decode from the stored 2718 envelope.

### Unit tests in `src/` (124 total, run in 0.5s)
Heaviest: `src/ingest/index.rs` (16 — seal/flush accumulator logic), `src/engine/bitmap.rs` (14 — codec, version byte, page math), `src/ingest/snapshot.rs` (11), `src/store/cache/mod.rs` (10), `src/primitives/range.rs` (10 — window resolution table), `src/engine/query/directory_resolver.rs` (6), `src/config/mod.rs` (6 — TOML wire shapes incl. Redacted), plus digest, row_codec, records, mem_scan, traces/ingest, dynamo/s3 helpers. Recovery logic is exercised via `src/ingest/rtt.rs` + unit tests — there is **no integration test in `tests/` for crash recovery, the publisher frontier (`min(data_durable, index_visible)`), or standby digest comparison**.

---

## 4. Test infrastructure

- **`src/testkit.rs`** (shipped module): `VecSource`, `populate_via_engine(blocks) -> PopulatedStore` drives the **real branchless engine** end-to-end over fresh in-memory stores (backfill cadence, `tip_lag_divisor: 1`, terminal flush ⇒ `head == last_block`), `try_populate_via_engine` for negative tests, `populate_via_engine_with_dict` for small-epoch dict tests, `PopulatedStore::reader[_with_limits]`. In-memory stores clone-share `Arc<RwLock>` backing. `TEST_PREFETCH` (concurrency 4) exercises the ordered-prefetch path.
- **`tests/common/mod.rs`**: fixture builders — headers (`test_header`/`chain_header` linked by `hash_slow`), envelopes, logs/filters/requests, `chain_of_blocks`, `IngestTx` builder (real signed `TxLegacy` 2718 envelope; `to=None` = creation), `base_trace`/`top_level_call`/`nested_call`, direct artifact seeders (`seed_bitmap_page_artifact/fragment/counts`) writing through the production staged-write path, `stage_block*` helpers, `test_cache_config` (row cache off).
- **`tests/common/observed_store.rs`**: `ObservedMetaStore`/`ObservedBlobStore` proxies — read counters (global + **per-table `get` tallies** via `start_counting`/`get_calls`, for exact-fetch-count assertions), one-shot `apply_writes` failure injection, apply delays + timing windows (for the parallel-flush overlap test).
- **Running**: `cargo test -p monad-chain-data` — whole suite (124 unit + ~110 integration) finishes in **<2 s**, no external services, all `current_thread` tokio. Gated suites:
  - `tests/dynamo_metastore.rs` / `tests/dynamo_blobstore.rs`: `#[ignore]`, need `CHAIN_DATA_DYNAMO_TEST_ENDPOINT` pointing at DynamoDB Local or Alternator (`docker run -p 8000:8000 amazon/dynamodb-local`), run with `--features dynamo -- --ignored`. Silent skip if env unset.
  - `tests/s3_blobstore.rs`: `#[ignore]`, but **self-contained** — spins an in-process `s3s`+`s3s-fs` server (dev-deps pinned `=0.12.0` for MSRV). `--features s3 -- --ignored`. No docker needed.
  - `tests/tx_rpc_conversion.rs`: compiled empty unless `--features alloy-rpc-types-eth`.
  - Default features are `["s3", "dynamo"]`, so gated tests compile by default; only the `--ignored` flag + env gate them.

---

## 5. Gap analysis for new docs (prioritized)

1. **Fix onboarding.md in place — don't rewrite.** The architecture narrative is strong and matches the code's concepts; it needs a mechanical pass: (a) delete/rewrite the "Write authority" section (lease removed), (b) fix all `src/ingest_*`/`backfill`/`live`/`authority` paths to the `src/ingest/` + `src/config/` layout, (c) CAS → single-row put + operational one-writer rule, (d) the six specific stale names in §6, (e) drop or write the deep-dive links.
2. **The five deep-dives are the biggest hole** — already linked and scoped (frontier model; ingest batching; recovery; bitmap index; primary/standby — the last needs re-scoping to "standby verification" only). `src/ingest/mod.rs`, `src/ingest/recover.rs`, and `src/ingest/publisher.rs` module docs are excellent seed material.
3. **On-disk format spec**: tables and key encodings, RLP layouts, version bytes, and the documented hard breaks (traces window, `row_chain`, de-shard — `records.rs:171–177`). Nothing covers this today; `tests/shared_block_blob.rs` is the only de-facto spec.
4. **Query execution walkthrough** referencing the instructive tests: `per_page_short_circuit_skips_later_clause_fetches`, `query_crossing_group_boundary_returns_exact_ids_in_both_orders`, `one_five_one_log_chain`, `indexed_query_cursor_completes_block_spanning_page_boundary`.
5. **queryX ↔ crate mapping doc**: which spec features the engine implements vs the transport (tags, `fields` projection, JSON-RPC error codes, default limit 100) — currently only discoverable by reading monad-rpc.
6. **Testing guide**: gated suites, the testkit/populate pattern, the observed-store fetch-count technique, and the coverage-gap note (no integration tests for crash recovery / publisher frontier / standby comparison — flag as to-add or unit-covered-by-design).
7. **Glossary**: family, primary id, page (64K), page group (256 pages), directory bucket (= page span, *not* 10k), fragment vs artifact vs manifest, seal vs flush, frontier, OpenTail/OpenState, row chain / seal chain, published head, head-0 sentinel.
8. ops-runbook.md needs nothing — it's current; keep the canonical-copy pointer honest.

---

## 6. Stale/wrong claims found (exact quotes → current state)

1. `onboarding.md:18` — "a single compare-and-swap'd row" / `:177` "guarded by compare-and-swap" / `:445` "one compare-and-swap on the published head" → **No CAS exists.** `PublicationTables::store_state` is a plain put of the single `publication_state` row. Atomicity = single row; exclusivity = operational rule.
2. `onboarding.md:354–363` — "Write authority … `src/engine/authority.rs`. Exactly one process may advance the head at a time, via a **lease** recorded in that same `PublicationState` row … Standbys block passively in `acquire_or_wait` … A fenced or lost writer steps down" → **File and entire mechanism do not exist.** `PublicationState` has only `indexed_finalized_head` + `head_row_chain` (`src/primitives/records.rs:194–199`).
3. `onboarding.md:377` — "Recovery … happens *after* lease acquire" → no lease; recovery just reads the published head (`src/ingest/recover.rs:68–89`).
4. `onboarding.md:138` — "It's bucketed in ranges of 10,000 ids" → `DIRECTORY_BUCKET_SIZE = STREAM_PAGE_ID_SPAN` = **65,536** (`src/engine/primary_dir.rs:27`).
5. `onboarding.md:87` — struct sketch field "`artifact_checksum, // chained digest`" → field is **`row_chain: Hash32`** (`src/primitives/records.rs:177`).
6. `onboarding.md:104` — "Compare `RawLogEntry` (stored) vs `LogEntry` (materialized)" → stored type is **`StoredLog`** (`src/logs/types.rs:38`).
7. `onboarding.md:108–109` — "Per family, per block, there's a **blob** and a **header**" → per block there is **one shared blob object** (`BLOCK_BLOB_TABLE`) with contiguous per-family regions located by each family header's `base_offset` (pinned by `tests/shared_block_blob.rs`). Headers are per-family; the blob object is not.
8. `onboarding.md:186` — "read `query_logs` at `src/api.rs:1791`" → `src/api.rs` is 384 lines; `query_logs` is at **`src/api.rs:181`**.
9. `onboarding.md:215–221` — "`src/engine/query/family_runner.rs:309`" (block scan) and "same file, line 97" (indexed) → actual lines **935** and **543**; function names still correct.
10. `onboarding.md:293–300` — "`src/ingest_core.rs` + `src/ingest_helpers.rs`, with thin `src/backfill.rs` / `src/live.rs` controllers … `ChainDataIngestSource` trait (`src/ingest_source.rs`) … `run_configured_chain_data_engine_ingest` (`src/ingest_config.rs`) … `FinalizedBlock`s (`src/family.rs`)" → actual: `src/ingest/{producer,data_track,index,publisher,recover,snapshot,source,resolver,probe,rtt}.rs`; source trait in **`src/ingest/source.rs`**; config entry in **`src/config/`**; `FinalizedBlock` in **`src/ingest_types.rs`**. Same for the crate-map table rows and "meta store (CAS)".
11. `onboarding.md:342–343` — "the signal cadence (`SignalPolicy`) and the fetch plan (`FetchPlan`)" → **`FetchPlan` does not exist**; the knobs are `SignalPolicy` and `Prefetch`. Note `src/ingest/mod.rs:32` goes further: "There is no backfill/live mode".
12. `onboarding.md:424–437` — links to `deep-dive-frontier-model.md`, `deep-dive-primary-standby.md`, `deep-dive-ingest-batching.md`, `deep-dive-recovery.md`, `deep-dive-bitmap-index.md` → **none exist** in `docs/`.

No references to fjall or the shard layer remain in either doc; the "Format note" at `onboarding.md:72–75` correctly describes the unsharded id space.
