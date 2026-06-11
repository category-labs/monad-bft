# monad-chain-data — Query / Read Path: Source Notes

All paths relative to `/home/jhow/monad-bft/monad-chain-data/`. "f:r" line refs are from the current `jhow/chain-data/feature` checkout (e2d8bba7).

---

## 1. Purpose & responsibilities per file

- **`src/api.rs`** — The public service facade. `MonadChainDataService<M: MetaStore, B: BlobStore>` wraps `Tables` (data tables + caches + runtime config), `PublicationTables` (head watermark), and `QueryLimits`. Owns the shared query prelude (limit check → published head → block-window resolution), the per-family `query_*` entry points, point lookups (`get_transaction`), relation joins, and the standby-verification probe (`standby_digests`). Strictly read-only: "reads the published head observationally and never writes" (api.rs:69–76).

- **`src/engine/query/mod.rs`** — Pure module manifest: `bitmap`, `directory_resolver`, `family_runner`, `row_cache`, `window`, all `pub(crate)` (mod.rs:16–20). The whole query engine is crate-internal; only `api.rs` types are public.

- **`src/engine/query/family_runner.rs`** — The heart of query execution. Defines the `IndexedFamilyQuery` trait (the per-family hook surface: decode/stamp/cache selection) plus three shared runners: the indexed 3-stage pipeline (`execute_indexed_family_query`, :543), the block-scan runner (`execute_block_scan_family_query`, :935), and the dispatch (`run_family_query`, :520). Also owns batched row materialization with span coalescing (`load_records_in_block`, :171), the whole-block scan materializer (`load_filtered_block_records`, :328), the byte-weighted decode budget, cursor semantics, and `IndexedQueryStats`.

- **`src/engine/query/window.rs`** — Maps a resolved *block* window to a *primary-id* window for one family (`resolve_primary_id_window`, :74) and provides the id-space geometry helpers on `ResolvedPrimaryIdWindow`: which page groups the window touches (`group_iter`, :37), page bounds within a group (:48), and page-relative offset clipping (:59).

- **`src/engine/query/bitmap.rs`** — Read-side bitmap algebra. `PageGroupPlan` (per-group clause streams + page-count manifests, :62), plan construction (`build_page_group_plan`, :86), per-page AND intersection with most-selective-first short-circuit (`intersect_group_page`, :196), OR-fold of a clause's value streams (`load_clause_page_bitmap`, :308), page artifact vs fragment loading (`load_bitmap_page`, :337), the manifest emptiness rule (`manifest_proves_empty`, :406), and the serial reference implementation `load_intersection_ids` (:139, off the production path — kept behavior-equivalent for tests).

- **`src/engine/query/directory_resolver.rs`** — `PrimaryIdResolver`: primary id → `(block_number, idx_in_block)`. Routes analytically on the seal frontier: sealed buckets resolve from a compacted summary (single get, binary search), the one open bucket scans fragments; per-bucket sources memoized behind a non-await-holding `Mutex<HashMap>` (:41–98). Missing data is always a hard commit-contract error, never a fallback.

- **`src/engine/query/row_cache.rs`** — Per-family decoded-row caches (`RowCache<T>`, `RowCaches`): query-invariant stored rows keyed `(block_number, idx_in_block)`, weighed by decompressed frame length, write-once so no invalidation (:16–22). Transfers share the trace cache (same `StoredTrace` frames, :75–80).

- **`src/mem_scan.rs`** — Pure functions scanning *unfinalized, in-memory* blocks (above the published head) with the exact row shapes and filter semantics the indexed path produces, so a transport can merge tip results with store results. Only logs (`scan_block_logs`, :49) and txs (`scan_block_txs`, :78) exist. No store access at all.

- **`src/blocks.rs`** — `eth_queryBlocks` execution (`execute_query_blocks`, :62): no filter, no relations, page = `min(limit, window_len)` blocks prefetched concurrently. Plus `load_blocks_by_numbers` (:100) which fulfills the `blocks` relation for the family queries (dedupe + ascending via `BTreeSet`).

- **`src/txs/hash_index.rs`** — `TxHashIndexTable`: tx_hash → fixed 12-byte `TxLocation { block_number u64 BE, tx_index u32 BE }` (txs/types.rs:76–103), a `CachedKvTable` point lookup (:46–48) written at ingest, read by `get_transaction`.

- **`queryX`** — The spec: five `eth_query*` JSON-RPC methods sharing one envelope (`fromBlock`/`toBlock`/`order`/`limit`), filters (AND of fields, OR within a field), many-to-one relations, field selection, and the `fromBlock`/`toBlock`/`cursorBlock` response triple for pagination/reorg detection.

- **`src/lib.rs`** — Root re-exports define the external surface (lib.rs:32–78): service + configured reader constructors, every request/response/filter type, the mem-scan helpers, span/ref primitives. Engine query internals deliberately not re-exported.

---

## 2. Public query API surface (api.rs)

Construction: `MonadChainDataService::new(meta, blob, limits)` (api.rs:81) or `with_all_configs(meta, blob, limits, CacheConfig, DictConfig, QueryRuntimeConfig)` (api.rs:95). Accessors: `tables()`, `publication()`, `limits()`. A backend-erased wrapper `ConfiguredChainDataReader` (config/reader.rs:43) monomorphizes InMemory / Dynamo+S3 / Dynamo+Dynamo behind an enum and forwards every method (used by monad-rpc).

| queryX method | Entry point | Request | Response | Notes |
|---|---|---|---|---|
| `eth_queryLogs` | `query_logs` (api.rs:181) | `QueryLogsRequest { envelope, filter: LogFilter, relations: LogsRelations }` | `QueryLogsResponse { logs: Vec<LogEntry>, blocks: Option<Vec<Block>>, transactions: Option<Vec<TxEntry>>, span: BlockSpan }` | relations: blocks + transactions |
| `eth_queryTransactions` | `query_transactions` (api.rs:208) | `QueryTransactionsRequest { envelope, filter: TxFilter, relations: TxsRelations }` | `QueryTransactionsResponse { txs, blocks, span }` | no `transactions` relation — records *are* txs (api.rs:220) |
| `eth_queryTraces` | `query_traces` (api.rs:280) | `TraceFilter { from, to, selector, is_top_level }` | `QueryTracesResponse { traces, blocks, transactions, span }` | |
| `eth_queryTransfers` | `query_transfers` (api.rs:308) | `TransferFilter { from, to, is_top_level }` | `QueryTransfersResponse { transfers, blocks, transactions, span }` | a *view over the trace family* gated by the `has_transfer` indexed column; always indexed (api.rs:305–312) |
| `eth_queryBlocks` | `query_blocks` (api.rs:339) | `QueryBlocksRequest { envelope }` — no filter | `QueryBlocksResponse { blocks: Vec<Block>, span }` | no filter, no relations (per spec) |
| (eth_getTransactionByHash backing) | `get_transaction(tx_hash)` (api.rs:245) | `Hash32` | `Result<Option<(TxEntry, EvmBlockHeader)>>` | header supplies timestamp + base fee context |
| (standby verification) | `standby_digests(block_number)` (api.rs:349) | `u64` | `Option<StandbyDigests { block_number, row_chain, seal_chains: PerFamily<Option<SealChainPoint>> }>` | not a queryX op |

**Common envelope** (`QueryEnvelope`, primitives/limits.rs:26–34): `from_block: Option<u64>`, `to_block: Option<u64>`, `order: QueryOrder {Ascending(default), Descending}`, `limit: usize` (default `DEFAULT_QUERY_LIMIT = 100`, limits.rs:20). Tag resolution (`latest`/`safe`/`finalized` etc.) is **not** in the engine — the transport (monad-rpc `handlers/chaindata/wire.rs`) maps tags to numbers; the engine sees `Option<u64>` only.

**Span**: every response carries `BlockSpan { from_block, to_block, cursor_block: BlockRef }` (primitives/refs.rs:25–30), `BlockRef { number, hash, parent_hash }` — exactly the spec's three block references.

**Error modes** (`MonadChainDataError`, error.rs:22–68):
- `InvalidRequest` — limit 0 (limits.rs:74), inverted range, lower bound above head, block `[0,0]` (range.rs:104–145), indexed query with zero clauses (family_runner.rs:574), transfers reaching block-scan (transfers/materialize.rs:187–195). → spec `-32602`.
- `LimitExceeded { kind: Limit | BlockRange, max_limit, max_block_range }` — `limit > max_limit` (limits.rs:79) or resolved span > `max_block_range` (range.rs:57). → spec `-32005` (the maxima are in the error for the spec's error `data`).
- `MissingData` — "no published blocks" when nothing is queryable (api.rs:376–383); missing block record/header inside the published range (hard data error). Note: an index hit for `get_transaction` that fails to materialize inside the published range is `MissingData`, *not* `None` (api.rs:243–244).
- `SealedDirectoryBucketMissingSummary`, `PrimaryDirectoryMissingId`, `SealedPageMissingArtifact` — broken ingestion/compaction commit contract, deliberately loud (error.rs:34–61).
- `Decode`, `Backend` — corruption / store failures.

**`QueryLimits`** (limits.rs:53–88): `max_limit` bounds `request.limit`; `max_block_range` bounds the resolved span (worst-case non-indexed scan). Neither caps per-block results — the spec requires completing the current block, so a hot block can exceed `max_limit`.

---

## 3. Query execution data flow (indexed family query, end to end)

Using `query_logs` as the example; txs/traces/transfers are identical modulo the materializer.

1. **Prelude** — `resolve_head_and_window` (api.rs:124–134): `limits.check_limit(envelope.limit)`; `require_published_head()` reads `PublicationTables::queryable_head()` (tables.rs:947 — head 0 is the pre-first-publish sentinel and maps to `None`; range queries error rather than return empty).
2. **Block-range resolution** — `ResolvedBlockWindow::resolve` (primitives/range.rs:43): maps order-dependent `(from,to)` to internal `(low,high)` with `low<=high` (range.rs:96–146). Defaults: asc `(1, head)`, desc `(1, head)` reading from the head down. Upper bound silently clamps to head; lower bound above head errors; block 0 clamps to 1 (genesis-clamp like eth_getLogs) unless `[0,0]`. Span checked against `max_block_range` *before* loading the two bound `BlockRecord`s (range.rs:56–72), which become `low`/`high` `BlockRef`s.
3. **Dispatch** — `run_query` (api.rs:139) → `run_family_query` (family_runner.rs:520): if `filter.has_indexed_clause()` (clause.rs:81, true iff `indexed_clauses()` non-empty) → indexed runner, else block-scan runner. Which filter fields emit clauses: logs `address→Addr`, `topics[i]→Topic{i}` (logs/materialize.rs:78–92); txs `from/to/selector` (txs/materialize.rs:76–90); traces `from/to/selector` + `is_top_level: Some(true)→TopLevel` marker — `Some(false)` emits *no* clause (traces/materialize.rs:75–96); transfers `from/to/top_level(true)` plus an unconditional `HasTransfer` marker, so transfers are always indexed (transfers/materialize.rs:69–83).
4. **Primary-id window** — `resolve_primary_id_window` (window.rs:74): two concurrent walks over block records — first block in range with `family.window_in(record).count > 0` gives `start = first_primary_id` (window.rs:99); last such block gives `end_exclusive = next_primary_id_exclusive()` (window.rs:118). `None` (empty window) short-circuits to an empty result with `cursor_block = to_block` (family_runner.rs:561–571). Each `BlockRecord` carries all three family windows `FamilyWindowRecord { first_primary_id, count }` (primitives/records.rs:152–178; family.rs:79–86).
5. **Frontier / seal classification** — `family_frontier_id` (family_runner.rs:990): the family's *global* id frontier = the published-head block's window end (zero-count windows still carry it). Crucially derived from the publication head, not the query's high block — "deriving it from the query range would misclassify a sealed bucket above the range as open" (family_runner.rs:580–589). From it: `sealed_below = bucket_start(frontier)` (directory routing) and `frontier_group = page_group_start(frontier)` (manifest authority).
6. **Stage 0 — group planning** (family_runner.rs:596–636): for each page group the window touches (`window.group_iter(order)`, window.rs:37), `build_page_group_plan` (engine/query/bitmap.rs:86) expands each clause into stream ids (`IndexedClause::stream_ids` → `render_stream_id(kind, value)` = `"{kind}/{hex}"`, engine/bitmap.rs:493) and loads per-clause page-count manifests. A clause with an empty stream set ⇒ `None` plan ⇒ group contributes nothing. Plans build with `buffered(GROUP_PLAN_LOOKAHEAD=4)` and flatten lazily into the page work-list (`PageWorkItem { page_start, from_offset, to_offset, plan: Arc<PageGroupPlan> }`, family_runner.rs:925), dropping manifest-proven-empty pages via `plan.candidate_pages` (bitmap.rs:384).
7. **Stage 1 — page intersection + resolve** (family_runner.rs:641–694): per page, `intersect_group_page` (bitmap.rs:196) ANDs all clauses most-selective-first (selectivity from manifest counts; unknowns last, bitmap.rs:436), short-circuiting when the running set empties. Each clause's page bitmap is the OR over its value streams' pages, fetched concurrently (bitmap.rs:308–335); each stream page is the compacted page artifact if sealed (`load_bitmap_page_artifact`) else the union of frontier fragments (`load_bitmap_fragments`), with min/max-offset overlap pre-filtering (bitmap.rs:337–375). The result is clipped to the window's page-relative offset range only when boundary bits exist (bitmap.rs:244–257). Surviving page-relative offsets become `PrimaryId(page_start + offset)` (descending uses roaring's double-ended iterator in place, family_runner.rs:666–671), and each id resolves through the shared `PrimaryIdResolver` (directory_resolver.rs:58) into `ResolvedPrimaryIdLocation { block_number, idx_in_block }`. Concurrency `buffered(page_intersect_concurrency=32)`; `buffered` (not unordered) preserves global order at every stage.
8. **Stage 2 — count-gated materialization** (family_runner.rs:696–774): the ordered location stream is grouped per block by `next_block_group` (family_runner.rs:814) with the boundary-straddling location stashed in `carry`. The batch loop pulls groups until candidate count ≥ records still needed, then materializes them concurrently (`buffered(materialize_concurrency=8)`) via `load_records_in_block` (see §4). Records pass the post-filter `filter.matches` (an invariant check for most families; a genuine dropper only for `is_top_level: Some(false)`), and the limit is checked per record with the stop block remembered.
9. **Row materialization** — `load_records_in_block` (family_runner.rs:171–323): probe the row cache for every requested index; for misses load the family's `BlockBlobHeader` (offsets + dict version + physical key, records.rs:90–144), coalesce missed frames into read spans (`coalesce_frame_ranges`, family_runner.rs:464: merge if gap ≤ `materialize_span_max_gap_bytes`=16 KiB and merged ≤ `materialize_span_max_bytes`=512 KiB), or collapse to one whole-region read if > 8 spans and region ≤ 8 MiB (family_runner.rs:236–247). Acquire byte-weighted permits from the process-global decode semaphore (`materialize_permits_for_bytes`, family_runner.rs:867: `clamp(bytes/32KiB, 1, 1024)`) — only on a cache miss. Range-read the blob (`read_block_blob_header_range`, tables.rs:424, resolved through `physical_key_or` for coalesced shared objects), zstd-decompress each frame under the block's dictionary version (`block_decoder`, tables.rs:481; version 0 / empty sentinel = plain), `decode_stored` (RLP), insert into the row cache, then stamp per-query context (`into_record` with the `BlockRecord`'s number/hash and `idx_in_block`).
10. **Cursor + result** (family_runner.rs:781–807): see §4. Response assembled with `BlockSpan { from_block, to_block, cursor_block }`.
11. **Relation joins** — `join_relations` (api.rs:151–177): `blocks` → `load_blocks_by_numbers` (dedupe asc, `buffered(8)`, blocks.rs:100); `transactions` → `load_txs_by_positions` (dedupe asc by `(block, idx)`, one batched `load_records_in_block` per block via `TxMaterializer`, `buffered(RELATION_TX_CONCURRENCY=8)`, txs/materialize.rs:167–200). Joins run after the page completes; related objects don't count toward the limit (spec-conformant).

**Block-scan path** (no indexed clause; family_runner.rs:935–984): walk blocks in query order with a `FuturesOrdered` read-ahead that doubles from 2 up to 16 per consumed block (wasted whole-region reads are expensive if `limit=1` fills immediately; constants at :60–70). Each block: `load_filtered_block_records` (family_runner.rs:328) reads the *whole* family region, decodes every row in query order via `decode_scan_record`, applies `filter.matches`. Stops block-aligned once `records.len() >= limit`; dropping the `FuturesOrdered` cancels in-flight read-ahead. Cursor = the last consumed block's ref.

**`query_blocks`** (blocks.rs:62–96): window resolution then exactly `min(limit, window_len)` block loads, `buffered(8)` preserving window order; each is `try_join!(load_record, load_header)` (blocks.rs:119–133); cursor = last loaded block. Every block in the window must exist (missing = `MissingData`).

**`get_transaction`** (api.rs:245–276): hash-index get → `None` if absent; `None` if no queryable head or `location.block_number > head` (index rows can land before the head advances); else point-read via `TxMaterializer::load_record_at` (single-index batched path, so row cache + coalescing + decode budget all apply, family_runner.rs:157–169) + `load_header`; missing header ⇒ `MissingData`.

---

## 4. family_runner in depth

**Trait surface** (`IndexedFamilyQuery`, family_runner.rs:89–393). Per family hooks: `family()`, `tables()`, `row_cache()`, `decode_stored` (frame → query-invariant `StoredRow`), `into_record_owned` / `into_record` (stamping; borrow variant lets a cached row be stamped without surrendering the cached copy, :114–128), `decode_scan_record` (scan-path decode; `Ok(None)` skips a frame — default 1:1; transfers override to hard-error). The point, batched, and block-scan read paths are implemented once in the trait's default methods. Implementors: `LogMaterializer` (logs/materialize.rs:119), `TxMaterializer` (txs/materialize.rs:126), `TraceMaterializer` (traces/materialize.rs:129), `TransferMaterializer` (transfers/materialize.rs:149 — family `Trace`, stored row `StoredTrace`, projects to `TransferEntry` via `trace_into_transfer`, :110).

**Phases of the indexed pipeline** (all stages are lazy streams; nothing runs ahead of demand beyond the buffer widths):

- *Plan*: id window + clauses + frontier (+ stats clock). Empty window → early return with `cursor = to_block`.
- *Stage 0* (group plans, look-ahead 4): manifest loads per group; flattens to the page work-list in query order. Ordering argument (family_runner.rs:596–603): `buffered` preserves group order, plans flatten to pages in order, and **page order == id order == block order**, so the pipeline stays globally ordered with no sort anywhere.
- *Stage 1* (page futures, width `page_intersect_concurrency`): intersection + id→location resolution inside the same future (the resolver memo makes per-page resolution mostly in-memory). Emits `Vec<ResolvedPrimaryIdLocation>` per page, flattened.
- *Stage 2* (block materialization, width `materialize_concurrency`): the **count-gated batch loop** (family_runner.rs:712–774). `need = limit - records.len()`; pull block groups until `batch_candidates >= need`. Because the loop stops at the *first* group crossing the threshold, every earlier group has fewer candidates than needed — so the limit can only fill on the batch's final block (`debug_assert` at :756). Families whose candidates all materialize into emitted records (logs/txs/transfers) therefore never decode past the limit block; only the trace post-filter (`is_top_level: Some(false)`) can drop candidates and force the outer `while` to refill.

**Partial pages / open tail**: there is no special open-tail read mode in the runner — the distinction is pushed down: (a) bitmap reads of pages at/above the frontier fall back from artifact to fragment union (bitmap.rs:337–375); (b) the frontier *group* has no page-count manifest, so the previous group's manifest is borrowed purely as a fetch-*ordering* hint and never skips a page ("absent ⇒ unknown, never a skip", family_runner.rs:585–589, bitmap.rs:99–110); (c) directory buckets at/above `sealed_below` resolve from fragments instead of summaries (directory_resolver.rs:67–80). The window's boundary pages get clipped offsets (`offsets_in_page`, window.rs:59); the manifest skip is re-checked inside `intersect_group_page` so serial and concurrent callers agree (bitmap.rs:209–212).

**Limit semantics**: limit is a *target*; the current block always completes (records from the stop block keep appending after `stop_block` is set, family_runner.rs:760–772). Tests pin this (tests/query_logs_indexed.rs:222, :240, :258).

**Cursor / continuation** (family_runner.rs:776–789): pagination is stateless, block-cursor-based per the spec — no opaque tokens. Indexed path: `cursor_block = stop_block` **only if** a later block with candidates exists, proved by the `carry` slot (the location that straddled the last block boundary — already paid for, no extra resolve); otherwise the page is terminal and `cursor_block = to_block` even though the limit filled. Block-scan: cursor is simply the last fully-consumed block. `query_blocks`: last returned block. Clients resume at `cursor+1` (asc) / `cursor−1` (desc); `cursor == to_block` ⇒ done. Empty-result responses keep `cursor = to_block` (family queries) — response docs say "an empty page means no next page; do not advance the cursor" (logs/materialize.rs:62–63).

**Stats**: `IndexedQueryStats` (family_runner.rs:405–443) — relaxed atomics accumulated across all stages (plan/intersect/resolve/materialize µs, page & candidate & span & byte counters, post-filter drops), dumped at `debug!` per query (`log_indexed_query_stats`, :877).

---

## 5. Caching

All read-side caches are **read-populated only; writes never seed them** — published data is write-once, so nothing needs invalidation (store/cache/mod.rs:391–396).

**Generic core** (`CachedInner`, store/cache/mod.rs:195–366): byte-budgeted weighted LRU (unbounded entry count; eviction purely by summed weight; `budget == 0` disables) + **single-flight** (concurrent misses share one fetch future; leader cleans up via drop guard; *every* awaiter populates on `Ok(Some)` so a cancelled leader can't strand an uncached value; errors never cached). **Absence is never cached**: a backend miss always re-probes, so a key written after a miss (an unmined tx hash, a pre-publication block) is visible to the very next read (store/cache/mod.rs:351–358). Values are decoded once on miss and weighed by their *pre-decode* byte length + `CACHE_ENTRY_OVERHEAD = 64` bytes overhead (`Weighted`/`weigh_weighted`, :44–58). No mutex held across `.await` (crate-wide invariant). Hit/miss counters drained per window (`take_window_stats`).

**Cache inventory** (`CacheConfig`, store/cache/mod.rs:98–177; default 2 GiB total split by ratios /1024 — row 512, bitmap_by_block 256, bitmap_page_blob 128, dir_by_block 32, page_counts 24, dir_bucket / open_streams / block_header / hash→number 16 each, tx_hash_index 8):

| Cache | Key | Value | Where |
|---|---|---|---|
| block_metadata (BlockTables) | block# BE8 | decoded `BlockRecord` | tables.rs:1040 |
| evm_header | block# BE8 | raw bytes (identity decode) | tables.rs:1046 |
| block_hash_to_number | hash | `u64` | tables.rs:1051 |
| tx_hash_index | tx hash | `TxLocation` | txs/hash_index.rs:36 |
| per-family block_metadata view | block# BE8 | `Arc<BlockBlobHeader>` (one family's header picked out of the same row) | tables.rs:1186–1197 |
| dir fragments (scannable) | (bucket BE8, block BE8) | `PrimaryDirFragment` | tables.rs:1203 |
| dir buckets | bucket BE8 | `PrimaryDirBucket` | tables.rs:1209 |
| bitmap fragments (scannable) | (stream/page key, flush block) | `Arc<DecodedBitmapFragment>` | tables.rs:1216 |
| bitmap page blobs | `"{stream}/"+page BE8` | `Arc<DecodedBitmapPage>` (decoded roaring — hit skips deserialize) | tables.rs:1221 |
| bitmap page counts | `"{stream}/"+group BE8` | `BitmapPageCounts` | tables.rs:1226 |
| open_bitmap_stream (scannable) | (page BE8, marker+chunk) | `Arc<Vec<String>>` | tables.rs:1231 |
| dict_by_version | version BE4 | bytes — **budget 0, uncached** (DictManager memoizes decoders, so the table is read at most once per (family, version) — a cache could never see a second lookup to hit) | tables.rs:1198–1201 |
| seal_chain | span BE8 | `Hash32` — budget 0, recovery/verification only | tables.rs:1237 |

**Row caches** (`RowCaches`, row_cache.rs:76–108): three weighted LRUs over `Arc<StoredLog>` / `Arc<StoredTxEnvelope>` / `Arc<StoredTrace>`, key `(block_number, idx_in_block as u32)` (wider indices silently bypass, :52–67), weight = decompressed frame length (stamped at insert — unrecoverable later), total budget split evenly /3 so a hot family can't starve the others. Only rows a query *actually materialized* are inserted — never whole blobs. Probe-then-insert pattern (no single-flight here; the batch path probes, fetches misses itself, then seeds, family_runner.rs:185–311). Transfers and traces share one instance.

**Per-query memo**: `PrimaryIdResolver.bucket_cache` (directory_resolver.rs:46) — a per-query-instance `Mutex<HashMap<bucket_start, Arc<CachedBucket>>>`; races duplicate at most an idempotent fetch.

**Never cached**: raw block-blob bytes ("the decoded-row caches above the decode layer absorb repeat reads", tables.rs:255–257); scan results (`scan_keys` always bypasses — unbounded sets would need invalidation, store/cache/mod.rs:494). All blob reads share one process-global IO semaphore (`blob_io_concurrency`=1024, tables.rs:372–379).

**Why caching is safe**: everything below the published head is immutable; reads only occur at/below the head (range resolution clamps); absence is never cached, so a pre-publication probe can't mask the key's later write.

---

## 6. mem_scan and live-data semantics

`mem_scan.rs` provides *engine-faithful pure scanners* over caller-supplied unfinalized blocks; the engine itself never sees unsealed tip data — the stitch is the transport's job.

- `MemLogsBlock { block_number, block_hash, logs_by_tx: &[Vec<Log>] }` (:31) deliberately mirrors ingest's per-tx grouping so **`log_index` is assigned block-globally in transaction order, identically to `logs::ingest`** (:16–19, test :114 pins `(tx,log) = (0,0),(0,1),(1,2)`).
- `MemTx { tx_hash, sender, signed_tx_bytes }` (:40): `sender` is caller-authoritative (not recovered from bytes), matching the ingest contract; `to`/`selector` filters decode `signed_tx_bytes`.
- `scan_block_logs(block, &LogFilter) -> Vec<LogEntry>` and `scan_block_txs(n, hash, &[MemTx], &TxFilter) -> Vec<TxEntry>` reuse the *same* `IndexedFilter::matches` implementations the store path uses as its post-filter — one filter semantics, two data sources. Rows come back in block order (ascending log_index / tx_index); "the caller handles query-order reversal and limits across blocks" (:46–48, :75–77).
- **Consistency at the seam**: the published head is the watermark. The store path serves `[1, head]`; tip blocks are `> head`. Because rows are identical in shape and filter semantics, a transport can run the store query, then append (asc) or prepend (desc) mem-scanned tip blocks with no dedup risk as long as it partitions strictly at the head it observed. There is no snapshot pinning: a head advance between the head read and the tip scan could double-cover a block — the caller must partition on one observed head.
- **Coverage gap**: only logs and txs. No mem-scan for traces, transfers, or blocks.
- **No consumer yet**: grep shows zero references outside the crate (`monad-rpc` handlers call `query_*`/`get_transaction` only). The exported helpers are forward-provisioning for the live/tip transport. (In `monad-rpc/src/data/mod.rs:376` the chain-data reader is only the *last* fallback for finalized tx-by-hash after buffer → triedb → archive.)

---

## 7. blocks.rs and hash_index.rs — point lookups

- **Block by number**: `BlockTables::load_record` (tables.rs:1059, cached `BlockRecord`: number/hash/parent/3 family windows/row_chain) and `load_header` (tables.rs:1089, RLP `EvmBlockHeader` in a *separate* table so hot record reads don't pay ~500 B they never decode, tables.rs:984–986). `Block { hash, header }` pairs them (blocks.rs:36–40, :119–133) — the header type doesn't carry the hash; it comes from the record.
- **Block by hash**: `block_number_by_hash` (tables.rs:1102) over the `block_hash_to_number_index` table (BE8 value). Documented caveat: `Some(n)` does not guarantee block `n` is published — index entries land before the head advances, so follow-ups may see `MissingData` (tables.rs:1099–1101). Exposed through `ConfiguredChainDataReader::block_number_by_hash` (config/reader.rs:101).
- **Tx by hash**: `TxHashIndexTable::get` (hash_index.rs:46) → `TxLocation` (12-byte fixed encoding, txs/types.rs:82–103) → published-head guard → `load_record_at` point read (goes through the full batched machinery so one frame = one coalesced span, cache + decode budget apply uniformly, family_runner.rs:157–169) → header join. `TxEntry` exposes lazy `envelope()/to()/selector()` accessors (each re-parses; decode once for many fields, txs/types.rs:37–54) and an `alloy-rpc-types-eth` conversion that leaves `effective_gas_price` to the RPC layer (needs base fee from the header, txs/types.rs:55–71).

---

## 8. Invariants & assumptions the read path relies on

1. **Id monotonicity**: primary ids are minted contiguously in block order per family; therefore page order == id order == block order, and the ordered-stream pipeline needs no sorting (family_runner.rs:600–603). `BlockRecord` family windows are the authoritative id↔block mapping.
2. **Pages partition the id space**, bits are **page-relative** (`STREAM_PAGE_ID_SPAN = 64Ki`, engine/bitmap.rs:30–33), so AND-intersection distributes over pages (bitmap.rs:128–131).
3. **Unified seal granule**: `PAGE_SPAN == BUCKET_SPAN == SEAL_SPAN = 64Ki` (static assert, ingest/index.rs:56–61; primary_dir.rs:27). One frontier classifies both bitmap pages and directory buckets as sealed/open.
4. **Frontier from the publication head**, never the query range (family_runner.rs:580–582).
5. **Manifest semantics**: page-count manifests exist only for fully-sealed groups (256 pages, `PAGE_GROUP_ID_SPAN = 2^24`); `None` = unknown (never skip), `Some(0)` (incl. absent page in a zero-dropped manifest) = proven empty, skip with zero fetches — *only* in sealed groups (bitmap.rs:406–415). A clause's manifest count is the SUM over its OR streams; any stream missing its row makes the whole clause unknown (bitmap.rs:264–300).
6. **Commit contract**: every bitmap candidate id must exist in the directory (`PrimaryDirectoryMissingId` otherwise); every sealed bucket must have a compacted summary (`SealedDirectoryBucketMissingSummary`) — never a silent fragment fallback (directory_resolver.rs:67–77, :113–130).
7. **Directory bucket shape**: entries strictly increasing in both `first_primary_id` and `block_number`, sentinel `end_primary_id_exclusive` above the last entry; enforced at decode (primary_dir.rs:49–88). Empty blocks are *omitted* from buckets; lookup routes past them (directory_resolver.rs test :209).
8. **Bitmap framing headers are validated against payloads** at decode (min/max/count must match; a corrupt too-narrow header would silently drop a page via the `overlaps` skip, engine/bitmap.rs:329–342).
9. **Blob header shape**: `offsets.len() == row_count + 1` (trailing sentinel = region length, records.rs:89–103); every frame slice is bounds-checked (family_runner.rs:216–218, :293–297, :374–381).
10. **Every block in `[1, published_head]` has a record and per-family headers**; missing ⇒ hard `MissingData`, not skip. Block 0 has no record (ingest starts at block 1; `EARLIEST_QUERYABLE_BLOCK = 1`, range.rs:27–29).
11. **Published data is immutable** (finalized only) — basis for all caching and the absence of reorg handling in the engine.
12. **Dictionary availability**: a block written under dict version V ≥ 1 implies the dict (or empty sentinel) is durably published; missing dict bytes = hard error (tables.rs:481–503).
13. **Descending iteration** relies on `DoubleEndedIterator` everywhere (`QueryOrder::iterate`, order.rs:26–37), including roaring's double-ended page iterator (family_runner.rs:666–668).
14. **Reader/recovery lockstep** (project memory): any data-model change must update both query and recovery paths together — e.g. `seal_boundary`/`last_sealed_span` are shared by sealing, recovery, and `standby_digests` (ingest/index.rs:63–76, api.rs:349–374).

---

## 9. Glossary

- **Family** — indexed record domain: `Log`, `Tx`, `Trace` (family.rs:59). Transfers are a *view* over `Trace`, not a fourth family.
- **Primary id** — `u64` per-family global row id, minted contiguously in block order. Family-scoped newtypes `LogId/TxId/TraceId` (records.rs:81–83).
- **Family window (`FamilyWindowRecord`)** — one block's id slice for a family: `{ first_primary_id, count }`.
- **Block window (`ResolvedBlockWindow`)** — resolved inclusive block range `(low, high)` as `BlockRef`s.
- **Primary-id window (`ResolvedPrimaryIdWindow`)** — the block window translated into a family's `[start, end_inclusive]` id range (window.rs:30).
- **Page** — 64Ki-id-aligned bitmap segment; the seal granule. Bits are page-relative offsets.
- **Page group** — 256 pages (2^24 ids); the page-count manifest's keying/completeness window.
- **Stream** — one bitmap series for an indexed `(kind, value)` pair, id `"{kind}/{hex(value)}"` (e.g. `addr/a0b8…`). Flag streams (`top_level`, `has_transfer`) have empty values.
- **Clause (`IndexedClause`)** — one AND-term of a query: an `IndexKind` plus OR-set of values, expanding to one stream per value (clause.rs:26–59).
- **Marker clause** — presence-only clause (empty value), e.g. `has_transfer`.
- **Plan (`PageGroupPlan`)** — per-group reusable intersection inputs: clause streams + manifests + sealed flag.
- **Manifest (`BitmapPageCounts`)** — sparse per-stream `(page_in_group, count)` roll-up for one sealed group; answers emptiness and selectivity without fetching bitmaps (engine/bitmap.rs:389–403).
- **Frontier** — first not-yet-assigned id of a family at the publication head; everything strictly below its granule is sealed.
- **Sealed vs open** — sealed pages have compacted page artifacts; sealed buckets have compacted summaries; the open (frontier) granule is served from flush-time fragments.
- **Fragment** — one flush's delta for an open page (bitmap) or one block's id range (directory).
- **Bucket / summary (`PrimaryDirBucket`)** — 64Ki-id directory granule / its compacted block-list with sentinel.
- **Resolver (`PrimaryIdResolver`)** — id → `(block_number, idx_in_block)`, sealed/open routed, per-query memoized.
- **Runner / materializer** — an `IndexedFamilyQuery` implementor (`LogMaterializer` etc.); "runner" also refers to the shared executors in family_runner.
- **Stored row** — query-invariant decoded frame (`StoredLog`/`StoredTxEnvelope`/`StoredTrace`); the row-cache unit. **Stamping** = attaching per-query block context to produce the public record.
- **Frame** — one row's compressed byte slice within a block's family region; **region** = a family's whole slice of the per-block blob; **ReadSpan** = several coalesced frames fetched in one range read.
- **Carry** — the block-boundary-straddling location stashed between block groups; doubles as the "does a later block exist?" proof for the cursor.
- **Stop block** — block in which the limit filled; the runner still completes it.
- **Cursor block** — last block fully scanned (spec `cursorBlock`); resume at cursor±1.
- **Span (`BlockSpan`)** — the response triple `from_block`/`to_block`/`cursor_block`.
- **Envelope (`QueryEnvelope`)** — shared request shell: bounds + order + limit.
- **Relation** — opt-in many-to-one join (`blocks`, `transactions`), deduped, never counted against the limit.
- **Publication head / queryable head** — reader-visible finalized watermark; head 0 = nothing queryable.
- **Decode budget** — process-global byte-weighted semaphore bounding concurrent stage-2 read+decode.
- **Dict version / epoch** — `block / epoch_blocks`; zstd dictionary per family per epoch; 0 / empty sentinel = plain frames.

---

## 10. Gotchas & spec divergences to call out in docs

**Design gotchas**
1. **`limit` is a target, not a cap** — the current block always completes; responses can exceed `limit` (and `max_limit`) on a hot block. Spec-conformant but surprises clients.
2. **Terminal-page cursor optimization**: when the limit fills but no later *candidate* block exists, the indexed cursor jumps to `to_block` (family_runner.rs:776–789). Clients must not assume `cursor != to_block` whenever `len == limit`.
3. **Transfers must never block-scan**: `TransferFilter` always emits `has_transfer`, and `decode_scan_record` hard-errors — the scan path would emit reverted-but-value-carrying frames because `TransferFilter::matches` deliberately omits the `status`/`tx_status` re-check that the bitmap pre-filters (transfers/materialize.rs:181–195). Any future change to clause emission must preserve "always ≥ 1 clause".
4. **`is_top_level: Some(false)` (traces & transfers) is the only true post-filter** — inexpressible as a positive bitmap clause, so it drops materialized candidates and is the sole reason the stage-2 batch loop can refill (traces/materialize.rs:75–78, family_runner.rs:760–763). A filter constrained *only* by it routes to the block-scan path.
5. **`matches` is otherwise an invariant check, not a filter** (clause.rs:70–71): a release-mode mismatch means index corruption silently shrinks results rather than erroring (it does bump `post_filter_dropped` stats).
6. **Above-head probes are miss-safe**: writes never seed read caches, but absence is never cached either — `standby_digests`/`block_number_by_hash` style probes for not-yet-published keys simply re-hit the backend on the next read; nothing is stranded. (`block_number_by_hash`'s publish-lag caveat is documented at tables.rs:1099–1101.)
7. **Missing-summary is fatal by design**: a sealed bucket without a summary or a bitmap candidate missing from the directory throws commit-contract errors instead of degrading (error.rs:34–61) — operators should know these errors mean store corruption, not transient failure.
8. **Block 0 clamp**: `[0, N]` silently becomes `[1, N]` (eth_getLogs-style genesis clamp) but `[0, 0]` errors (range.rs:135–144).
9. **`load_intersection_ids` is dead on the production path** — retained only as the serial equivalence reference for the concurrent pipeline (bitmap.rs:136–139); don't document it as an API.
10. **Mem-scan coverage is partial** (logs + txs only) and currently unconsumed anywhere in the repo.
11. **Transfers share the trace row cache** — cache stats for "row:trace" include transfer traffic (row_cache.rs:75–80).
12. **Concurrency knobs matter**: defaults suit fast local backends; S3/Dynamo deployments should raise `materialize_concurrency`/`blob_io_concurrency` (tables.rs:269–322). Two distinct fan-outs (stage-1 pages vs stage-2 blocks) hit different backends.
13. **The engine never coalesces a fully-cached block with the budget**: permits are acquired only on a row-cache miss (family_runner.rs:249–258) — hot repeated queries bypass the semaphore entirely.
14. **`QueryBlocksResponse` invariant**: every block in the window must exist, so the page is exactly `min(limit, window_len)` blocks; a missing record/header inside the range is a hard error, not a short page (blocks.rs:72–75).

**Spec ↔ implementation divergences**
1. **Tags, fields, error-code mapping are out of the engine**: `fromBlock`/`toBlock` tags (`latest`/`safe`/`finalized`/`earliest`), the `fields` projection, normalized JSON shaping, and `-32602`/`-32005` mapping all live in monad-rpc `handlers/chaindata` (wire.rs). The engine returns *full* records; field selection is projection-only at the wire.
2. **"earliest" = block 1**, not 0 — ingest requires the chain to start at block 1 (range.rs:27–29).
3. **No internal time/size constraint**: the spec allows the server to stop early on response-size/time limits; the engine implements only `limit` + the up-front `max_block_range`/`max_limit` guards. A transport wanting time-boxing must add it itself.
4. **Spec's reorg-detection section is TODO**, and the engine sidesteps it: only finalized published blocks are served, so the returned `hash`/`parentHash` triplets are stable by construction.
5. **Desc default for omitted `toBlock`** ("runs to genesis") clamps to block 1, per (2).
6. **Spec limit default 100** matches `DEFAULT_QUERY_LIMIT` (limits.rs:20); `limit: 0` is rejected outright (`InvalidRequest`), which the spec doesn't specify.
7. **Transfers** are defined in the spec as "native token transfers"; the implementation realizes this as trace frames with `value > 0` whose `status`/`tx_status` passed at ingest (the `has_transfer` stream), `to` always resolved (Create*→new contract, SelfDestruct→beneficiary, transfers/materialize.rs:107–124).

---

## 11. Open questions

1. **Tip stitching ownership**: `mem_scan` helpers are exported but nothing consumes them (monad-rpc included). Is the live/tip merge transport still pending, and will it pin a single observed head to avoid double-covering the seam block? Also: are tip traces/transfers/blocks intentionally out of scope for live queries, or future work?
2. **`safe`/`finalized` tag semantics** at the wire layer — both presumably resolve to the published head since the store is finalized-only; not verified (wire.rs out of scope).
3. **Resolved — stale negative-cache entries**: negative caching was removed from the read-cache layer (the LRU stores `V`, not `Option<V>`); above-head probes re-hit the backend on the next read, so no guard is needed (gotcha 6).
4. **`resolve_primary_id_window` cost on sparse families**: the low/high walks scan block records serially from each end until a non-zero window (window.rs:99–135); over a `max_block_range`-sized range of family-empty blocks this is O(range) cached point reads. Presumably bounded acceptable by `max_block_range`; no skip structure exists.
5. **Fetch-retry** is listed as open in project memory for the engine generally; the read path has no retry layer either — backend errors surface directly (single-flight even re-wraps a shared error for followers, store/cache/mod.rs:88–93). Is retry the transport's job?
6. **`QueryLimits.max_limit` vs block-completion**: a single block with more matches than `max_limit` will return them all; is the transport expected to impose a response-size guard for pathological blocks?
