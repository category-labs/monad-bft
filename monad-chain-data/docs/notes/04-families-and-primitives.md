# monad-chain-data — Data Families & Primitives: Source Notes

All paths relative to `/home/jhow/monad-bft/monad-chain-data/src/`. Line refs are exact as of branch `jhow/chain-data/feature` (e2d8bba7).

---

## 1. Purpose & responsibilities per file

- **`logs/mod.rs`** (23 lines) — module wiring. Crate-internal exports for ingest helpers (`encode_block_logs`, `flatten_logs`, `stream_entries_for_log`) and the `LogMaterializer`; public exports for the query surface (`LogFilter`, `LogsRelations`, `QueryLogsRequest/Response`) and record types (`LogEntry`, `StoredLog`) — logs/mod.rs:20-23.
- **`logs/types.rs`** — record schema: public `LogEntry` (logs/types.rs:25-33) and on-disk `StoredLog` (logs/types.rs:38-44) with RLP encode/decode and the `into_log_entry` stamping conversion (logs/types.rs:56-66).
- **`logs/materialize.rs`** — read path: `LogFilter`'s `IndexedFilter` impl (clauses + post-check, logs/materialize.rs:75-107), request/response types, and `LogMaterializer` implementing `IndexedFamilyQuery` (logs/materialize.rs:119-149).
- **`logs/ingest.rs`** — write path: flatten per-tx logs into block order (`flatten_logs`, logs/ingest.rs:31-53), compress into the framed blob (`encode_block_logs`, logs/ingest.rs:56-61), and expand a log into its index stream keys (`stream_entries_for_log`, logs/ingest.rs:65-74).
- **`txs/mod.rs`** — same wiring; also exports `TxHashIndexTable` and `load_txs_by_positions` (the relation loader) — txs/mod.rs:21-28.
- **`txs/types.rs`** — `TxEntry` (txs/types.rs:28-35) with lazy envelope decoding helpers (`envelope()`, `to()`, `selector()`, feature-gated `to_rpc_transaction()` txs/types.rs:59-71), the fixed-width `TxLocation` (txs/types.rs:77-103), and on-disk `StoredTxEnvelope` (txs/types.rs:109-135).
- **`txs/materialize.rs`** — `TxFilter` `IndexedFilter` impl (txs/materialize.rs:73-114), `TxMaterializer` (txs/materialize.rs:126-158), and `load_txs_by_positions` for serving the `transactions` relation of other families (txs/materialize.rs:167-200).
- **`txs/ingest.rs`** — `collect_hash_locations` (tx_hash → location pairs, txs/ingest.rs:33-50), `encode_block_txs` (txs/ingest.rs:53-65), `stream_entries_for_tx` (txs/ingest.rs:69-83).
- **`txs/hash_index.rs`** — the only family-private KV table: `TxHashIndexTable` wrapping a `CachedKvTable<M, TxLocation>` over `TableId "tx_hash_index"` with `get`/`stage_put`/stats (txs/hash_index.rs:25-66).
- **`traces/mod.rs`** — wiring; publicly exports two ingest-side helpers other crates need: `compute_trace_addresses` and `is_transfer_frame` (traces/mod.rs:20).
- **`traces/types.rs`** — `TraceEntry` (traces/types.rs:27-43), shared selector extraction rule `selector_from_input` (traces/types.rs:60-62), `StoredTrace` plus a manual RLP shim `StoredTraceRlp` to encode `Option<Address>` and `CallKind` (traces/types.rs:66-176).
- **`traces/materialize.rs`** — `TraceFilter` with the `is_top_level: Option<bool>` tri-state (traces/materialize.rs:37-44), its `IndexedFilter` impl (traces/materialize.rs:72-117), `TraceMaterializer` (traces/materialize.rs:129-159).
- **`traces/ingest.rs`** — `encode_block_traces` (traces/ingest.rs:32-39), `stream_entries_for_trace` (traces/ingest.rs:44-65), **the canonical `has_transfer` predicate `is_transfer_frame`** (traces/ingest.rs:74-84), and `compute_trace_addresses` (DFS depth list → OpenEthereum trace addresses, traces/ingest.rs:92-134) with unit tests (traces/ingest.rs:136-191).
- **`transfers/mod.rs`** — module doc states the design: transfers are *"a view over the trace family… No separate persistence: the materializer ANDs a `has_transfer` clause into trace queries and projects matches into `TransferEntry`s"* (transfers/mod.rs:16-18). No `ingest.rs` exists for transfers — nothing extra is written.
- **`transfers/types.rs`** — `TransferEntry` only (transfers/types.rs:24-33); no Stored type, no codec.
- **`transfers/materialize.rs`** — `TransferFilter` (always-AND `has_transfer` clause, transfers/materialize.rs:69-83), projection `trace_into_transfer` (transfers/materialize.rs:110-135), `TransferMaterializer` reusing `Family::Trace` storage and the trace row cache (transfers/materialize.rs:149-196), with a hard-error `decode_scan_record` override.
- **`primitives/mod.rs`** — submodule list plus re-export `pub use alloy_consensus::Header as EvmBlockHeader` (primitives/mod.rs:22).
- **`primitives/records.rs`** — durable on-disk record shapes: `PrimaryId` + per-family id newtypes, `BlockBlobHeader`, `FamilyWindowRecord`, `BlockRecord`, `PublicationState`.
- **`primitives/refs.rs`** — lightweight read-side block references: `BlockRef`, `BlockSpan`.
- **`primitives/order.rs`** — `QueryOrder` (Ascending/Descending) and direction-agnostic iteration.
- **`primitives/range.rs`** — `ResolvedBlockWindow`: resolution of queryX `(from_block, to_block, order)` into an internal inclusive `(low, high)` window, clamping/validation rules.
- **`primitives/limits.rs`** — `QueryEnvelope` (the shared request envelope), `QueryLimits` (deployment caps), `LimitExceededKind`. **Note: despite the filename this is NOT `LimitedVec`** (that lives elsewhere in monad-bft); this file is query-shape limits.
- **`error.rs`** — single crate error enum `MonadChainDataError` + `Result` alias.
- **`testkit.rs`** — public test utilities: in-memory ingest-then-read fixtures driven through the *real* engine.

---

## 2. The family pattern

Three **persisted families** — `Family::Log`, `Family::Tx`, `Family::Trace` (engine/family.rs:59-63, canonical order `Family::ALL` engine/family.rs:67) — plus one **projected family** (transfers, a filtered view of Trace with no `Family` variant of its own).

Each persisted family module provides exactly three things:

1. **`types.rs` — two record shapes + codec.** A public `*Entry` (what queries return; carries `block_number`, `block_hash`, etc.) and an on-disk `Stored*` (only per-row fields; block-level fields stripped at write time and re-stamped at read time from the `BlockRecord`). Encoding is RLP (`alloy_rlp`); `decode_exact` so trailing bytes are rejected.
2. **`ingest.rs` — three pure functions** wired into the engine by `ingest/data_track.rs` and `ingest/index.rs`:
   - `encode_block_*(rows, codec) -> (BlockBlobHeader, Vec<u8>, ChainDigest)` — all three delegate to the shared `encode_block_rows` (engine/row_codec.rs:142-175): per row record the byte offset, RLP-encode, feed raw bytes into a `RowDigest`, zstd-compress into its own frame. Called from `ingest/data_track.rs:388-392`.
   - `stream_entries_for_*(record) -> Vec<StreamKey>` — expands one record into the bitmap index streams it belongs to. Consumed by `accumulate_family` (ingest/index.rs:714-739).
   - (txs only) `collect_hash_locations` for the side `tx_hash_index` table (txs/ingest.rs:33, staged via `ingest/data_track.rs:448`).
3. **`materialize.rs` — two trait impls** plugging the family into the generic engine read path:
   - `impl IndexedFilter for *Filter` (engine/clause.rs:71-90): `indexed_clauses()` (filter → AND-of-OR bitmap clauses) and `matches()` (in-memory record check). Per the trait doc, `matches` is *"an invariant check on materialized records, not a release-mode post-filter"* for the indexed path; for the **block-scan path** (no clauses) `matches` does all the filtering.
   - `impl IndexedFamilyQuery for *Materializer` (engine/query/family_runner.rs:89-148): declares `Family`, `Filter`, `Record`, `StoredRow`, gives the engine `tables()`, the family's row cache, `decode_stored()` and `into_record_owned()` (the "stamping" step). Optional override `decode_scan_record` controls block-scan behavior.

**Storage registration is centralized, not per-module.** Families don't register tables themselves; `Family::table_ids()` (engine/family.rs:69-75) mints the full per-family table set via the `family_table_ids!` macro (engine/family.rs:41-54): `{prefix}_dict_by_version`, `{prefix}_dir_by_block`, `{prefix}_dir_bucket`, `{prefix}_bitmap_by_block`, `{prefix}_bitmap_page_blob`, `{prefix}_bitmap_page_counts`, `{prefix}_open_bitmap_stream`, `{prefix}_seal_chain`. `Tables` constructs one `FamilyTables` per `Family` (engine/tables.rs:1163-1240). Row blobs for all families share one `block_blob` blob table (`BLOCK_BLOB_TABLE`, engine/family.rs:23) — the per-family `BlockBlobHeader` carves regions out of it. Per-family decoded-row caches live in `RowCaches { logs, txs, traces }` (engine/query/row_cache.rs:76-92; byte budget split evenly across the three).

**How the four differ:**

| | Logs | Txs | Traces | Transfers |
|---|---|---|---|---|
| `Family` | `Log` | `Tx` | `Trace` | reuses `Trace` (transfers/materialize.rs:156-158) |
| Stored type | `StoredLog` | `StoredTxEnvelope` | `StoredTrace` | `StoredTrace` (shared) |
| Own ingest code | flatten + encode + streams | + `tx_hash_index` side table | + `trace_address` computation, `has_transfer` predicate | none |
| Index streams | addr, topic0-3 | from, to, selector | from, to, selector, top_level, has_transfer | (rides trace streams) |
| Block-scan path | yes | yes | yes | **forbidden** (hard error, transfers/materialize.rs:187-195) |
| Relations offered | blocks, transactions | blocks | blocks, transactions | blocks, transactions |
| Row cache | own | own | own | trace's (family_runner.rs:102-104) |
| `idx_in_block` use in stamping | unused (`log_index`/`tx_index` stored in row) | **used** as `tx_index` (txs/materialize.rs:149-157) | unused (`tx_index` stored in row) | unused |

---

## 3. Record schemas per family

### Logs
- **Public `LogEntry`** (logs/types.rs:25-33): `block_number: u64`, `block_hash: Hash32`, `tx_index: u32`, `log_index: u32`, `address: Address`, `topics: Vec<B256>`, `data: Bytes`. `log_index` is **block-relative** (running position across the block, logs/ingest.rs:40).
- **Stored `StoredLog`** (logs/types.rs:38-44): drops `block_number`/`block_hash` (reconstructed from `BlockRecord` at read time). RLP-derive encoded; one zstd frame per row.
- **Indexed (bitmap clauses)**: `address` → `IndexKind::Addr`; `topics[0..4]` positionally → `IndexKind::Topic0..Topic3` (logs/materialize.rs:78-92; write side logs/ingest.rs:65-74). **Stored-only**: `data`, `tx_index`, `log_index`.
- **Order/primary key**: rows written in block order — outer loop tx_index, inner loop log order (logs/ingest.rs:35-50) — each row gets a sequential `PrimaryId`. Global sort = `(block_number, tx_index, log_index)` ≡ ascending `LogId`.

### Txs
- **Public `TxEntry`** (txs/types.rs:28-35): `block_number`, `block_hash`, `tx_index: u32`, `tx_hash: Hash32`, `sender: Address`, `signed_tx_bytes: Bytes`. Derived accessors re-decode the EIP-2718 envelope each call (`envelope()`/`to()`/`selector()`, txs/types.rs:40-53 — "decode once for many fields"). `to_rpc_transaction()` (feature `alloy-rpc-types-eth`) leaves `effective_gas_price: None`; the RPC layer fills it from the block base fee (txs/types.rs:55-71).
- **Stored `StoredTxEnvelope`** (txs/types.rs:109-113): `tx_hash`, `sender`, `signed_tx_bytes` only; `tx_index` reconstructed from `idx_in_block` at read (txs/materialize.rs:149-157).
- **Side index `TxLocation`** (txs/types.rs:77-103): fixed 12 bytes = 8-byte BE `block_number` ++ 4-byte BE `tx_index`, value of `tx_hash_index` keyed by tx_hash. Last-write-wins; hash caller-authoritative (txs/ingest.rs:32).
- **Indexed**: `sender` → `From` (always); `to` → `To` and 4-byte calldata selector → `Selector` (both skipped for contract creations / short calldata, txs/ingest.rs:69-83). **Stored-only**: everything inside `signed_tx_bytes`.
- **Order**: `(block_number, tx_index)` ≡ ascending `TxId`.

### Traces
- **Public `TraceEntry`** (traces/types.rs:27-43): `block_number`, `block_hash`, `tx_index: u32`, `trace_address: Vec<u32>` (OpenEthereum path-from-root, empty = top-level), `typ: CallKind`, `from`, `to: Option<Address>`, `value: U256`, `gas`, `gas_used: u64`, `input`, `output: Bytes`, `status: u8` (frame-level; `0 == EVMC_SUCCESS`, ingest_types.rs:93-95), `depth: u32`, `tx_status: bool` (receipt status of containing tx, ingest_types.rs:102-104). Helpers: `is_top_level()` (= `trace_address.is_empty()`), `selector()` (traces/types.rs:46-54).
- **Stored `StoredTrace`** (traces/types.rs:67-81) keeps `tx_index` and `trace_address` in-row (unlike txs) and drops only block fields. RLP via manual shim `StoredTraceRlp` (traces/types.rs:85-100): `typ` as one byte (`CallKind::as_u8`, ingest_types.rs:56-66 — enum-order ≠ byte-order: StaticCall=1, DelegateCall=2), `to` as variable `Bytes` (empty = None, 20 = Some, else decode error traces/types.rs:131-139), `tx_status` as 0/1 byte.
- **Indexed**: `from` → `From` (always); `to` → `To` (if present); selector → `Selector` (input ≥ 4 bytes); `TopLevel` marker for root frames; `HasTransfer` marker per `is_transfer_frame` (traces/ingest.rs:44-65). **Stored-only**: `value`, `gas`, `gas_used`, `input` beyond selector, `output`, `status`, `depth`, `trace_address` beyond emptiness, `tx_status`.
- **Order**: DFS pre-order within tx, txs in block order ≡ ascending `TraceId`. Frames arrive pre-flattened with `tx_index`/`trace_address` assigned by the producer (ingest_types.rs:27-29; traces/ingest.rs:30-31).

### Transfers (projection, no schema of its own on disk)
- **Public `TransferEntry`** (transfers/types.rs:24-33): `block_number`, `block_hash`, `tx_index`, `trace_address`, `typ: CallKind`, `from: Address`, `to: Address` (**non-optional** — resolved for qualifying kinds: Create*→new contract, SelfDestruct→beneficiary, transfers/types.rs:20-22), `value: U256`. Drops gas/input/output/status.
- **Indexed**: `from` → `From`, `to` → `To`, `is_top_level: Some(true)` → `TopLevel`, plus **unconditionally** `HasTransfer` (transfers/materialize.rs:69-83). Marker clauses use `IndexedClause::marker` (empty value bytes, engine/clause.rs:43-50).

### Index kinds (shared vocabulary)
`IndexKind` = `{Addr, Topic0..Topic3, From, To, Selector, TopLevel, HasTransfer}` (engine/bitmap.rs:502-513); canonical value lengths 20 / 32 / 4 / 0 (engine/bitmap.rs:552-555). `StreamKey::new` debug-asserts length (engine/bitmap.rs:577-591). Stream ids render via `render_stream_id` (engine/clause.rs:53-58). Clause semantics: AND across clauses, OR within a clause's value set.

`set_allows` (engine/clause.rs:62-67) is the shared post-filter helper: *"Absent filter ⇒ allow; absent value ⇒ reject"* — e.g. a contract-creation tx (no `to`) never matches a `to` filter (txs/materialize.rs:96-110); a log with fewer topics never matches a filter on that position (logs/materialize.rs:99-103).

---

## 4. Materialization (write-side derivation + read-side stamping)

**Source data** is the `FinalizedBlock` (ingest_types.rs:23-30): `header: EvmBlockHeader` (= `alloy_consensus::Header`), `logs_by_tx: Vec<Vec<Log>>`, `txs: Vec<IngestTx>`, `traces: Vec<IngestTrace>` (pre-flattened DFS). `IngestTx.sender` is *caller-authoritative — never recovered from `signed_tx_bytes`* (ingest_types.rs:32-34); ingest validates `txs.len() == logs_by_tx.len()` when txs is non-empty. `CallKind` is a local mirror of monad-archive's enum to avoid the dependency (ingest_types.rs:42-43).

- **Logs**: `flatten_logs` (logs/ingest.rs:31-53) walks `logs_by_tx` per tx index, assigning block-relative `log_index = logs.len()` as it goes; u32 overflow errors as `Decode`. A tx with no logs contributes nothing (indices stay contiguous).
- **Txs**: write side near-copies `IngestTx` into `StoredTxEnvelope` (txs/ingest.rs:57-64). `stream_entries_for_tx` (txs/ingest.rs:69-83) **decodes the 2718 envelope at ingest** (fallible — a malformed signed tx fails the block). `collect_hash_locations` enumerates `(tx_hash, TxLocation)` pairs (txs/ingest.rs:33-50).
- **Traces**: rows stored verbatim from `IngestTrace` (`From<&IngestTrace> for StoredTrace`, traces/types.rs:178-196). Producer-side `compute_trace_addresses` (traces/ingest.rs:92-134) converts a DFS depth sequence into trace addresses: maintains a `cursor` path and per-depth `next_child_slot` counters; depth 0 resets (new tx root); rejects an orphan first depth > 0 and any depth jump > parent+1 as `InvalidBlock` (traces/ingest.rs:111-120). Doc example: tree `root -> A -> {A1, A2}; root -> B` ⇒ `[], [0], [0,0], [0,1], [1]`.
- **Transfers — derivation rule**: **native-value transfers from traces only; not ERC-20, not log-derived.** `is_transfer_frame` (traces/ingest.rs:74-84) is documented as *"THE definition of the `has_transfer` index bit"*: value moves iff (a) `typ ∈ {Call, CallCode, Create, Create2, SelfDestruct}` (DelegateCall/StaticCall never move value), (b) `value > 0`, (c) frame `status == 0`, and (d) `tx_status == true`. Both status checks are required because *"the tracer does not rewrite descendant statuses when a parent reverts"* (traces/ingest.rs:72-73). Read side: `trace_into_transfer` (transfers/materialize.rs:110-135) projects the trace and errors (`Decode`, "tracer invariant violated") if `to` is None on a has_transfer frame. `TransferFilter::matches` re-checks `value > 0` defensively *"so an ingest bug surfaces as a missing row instead of bad output"* (transfers/materialize.rs:85-89) but deliberately does **not** re-check statuses — which is exactly why block-scan is forbidden (`decode_scan_record` override returns `InvalidRequest("transfers cannot be served by the block-scan path")`, transfers/materialize.rs:181-195).

**Read-side stamping** (all families): `into_record_owned(stored, &BlockRecord, idx_in_block)` re-attaches `block_number`/`block_hash` from the `BlockRecord`; only txs additionally use `idx_in_block` (as `tx_index`). The borrowed `into_record` default clones from cache (family_runner.rs:114-120).

---

## 5. primitives/ in depth

### records.rs — durable shapes
- **`PrimaryId`** (records.rs:23-52): u64 newtype, the global per-family row ordinal. `checked_add` errors on overflow; `idx_in_block(first)` converts a global id to an index within a block's window.
- **Family-scoped id newtypes** `LogId`, `TxId`, `TraceId` via `family_id!` macro (records.rs:57-83): zero-cost wrappers so *"a family's signatures can't accidentally accept ids minted for another family"*; bidirectional `From` with `PrimaryId`.
- **`BlockBlobHeader`** (records.rs:89-144): per-family per-block blob index. Fields: `offsets: Vec<u32>` (one per row **plus trailing sentinel** = total length, so `len == row_count + 1`), `dict_version: u32` (`0` = plain frames), `base_offset: u32` (family region start in the shared per-block blob), `physical_key: Vec<u8>` (empty ⇒ block owns its object at the deterministic `block_number` key; set once the coalescer repoints into a shared object, records.rs:97-100), `physical_base_offset: u64`. `abs_range(idx)` / `region_range()` compute absolute byte ranges (records.rs:116-127). All three families *"use this identical on-disk layout"* (records.rs:87-88).
- **`FamilyWindowRecord`** (records.rs:152-162): `{first_primary_id, count: u32}` — a block's contiguous id window in one family; `next_primary_id_exclusive()` is checked.
- **`BlockRecord`** (records.rs:164-188): per-block row: `block_number`, `block_hash`, `parent_hash`, three `FamilyWindowRecord`s (`logs`, `txs`, `traces`), and `row_chain: Hash32` — head of the per-block content-digest chain for standby verification. Doc notes both `traces` and `row_chain` additions were hard on-disk RLP breaks requiring re-ingest.
- **`PublicationState`** (records.rs:191-209): the single publication row: `indexed_finalized_head: u64` + `head_row_chain: Hash32`. *"Field order is the on-disk RLP list order; reordering changes the wire format."*
- Shared helper `decode_rlp` maps any RLP failure to `Decode(static msg)` (records.rs:146-150).

### refs.rs — cross-family/block references
- **`BlockRef`** (refs.rs:18-23): `{number, hash, parent_hash}` — a *value* reference to a block, cheap `Copy`, built `From<&BlockRecord>` (refs.rs:32-40). Hash+parent_hash let consumers verify chain identity/continuity without refetching headers.
- **`BlockSpan`** (refs.rs:25-30): `{from_block, to_block, cursor_block: BlockRef}` — attached to every query response. `from/to` mirror the *resolved* request bounds (post-default/clamp, request orientation via `request_endpoints`); `cursor_block` is the last block scanned. Contract: *"An empty `logs` means no next page; do not advance the cursor"* (logs/materialize.rs:62-63, same on the other families).
- Record-to-record relations (log→tx, trace→tx) are **not** stored refs — they're positional `(block_number, tx_index)` coordinates carried on the records; see §8.

### order.rs
- **`QueryOrder`** (order.rs:16-21): `Ascending` (default) / `Descending`. `iterate()` (order.rs:26-36) walks any `DoubleEndedIterator` forward or reversed. No separate "global ordering scheme" type: global order **is** `PrimaryId` order within a family (= block order, then in-block row order); `QueryOrder` only chooses traversal direction.

### range.rs — window resolution semantics
- **`EARLIEST_QUERYABLE_BLOCK = 1`** (range.rs:29): ingest starts at block 1; block 0 has no record.
- **`ResolvedBlockWindow { low, high }`** (range.rs:34-37): internal inclusive range, `low.number <= high.number`, direction-neutral.
- `resolve()` (range.rs:43-78): maps spec bounds to numbers, enforces `span <= limits.max_block_range` (else `LimitExceeded{BlockRange}`), loads both bound `BlockRecord`s (missing ⇒ `MissingData`) to produce `BlockRef`s.
- `resolve_block_numbers()` (range.rs:96-146), in order:
  1. If both bounds given, inversion w.r.t. order is checked **before defaulting** so the error reports the real cause (range.rs:103-114).
  2. Defaults: ascending `(from ?? 1, to ?? head)`; descending `(to ?? 1, from ?? head)` (range.rs:116-125).
  3. Lower bound above published head ⇒ `InvalidRequest`; upper bound silently clipped to head (range.rs:127-134).
  4. `low` clamps `0 → 1` (eth_getLogs-style genesis clamp) — but `[0,0]` fails the final `low > high` check (range.rs:135-145; tests range.rs:181-195).
- `request_endpoints(order)` maps `(low,high)` back to spec `(from,to)` orientation (range.rs:81-87); `iter(order)` walks the inclusive range in traversal direction (range.rs:89-92).

### limits.rs — query envelope and caps
- **`QueryEnvelope`** (limits.rs:26-34): `from_block: Option<u64>`, `to_block: Option<u64>`, `order`, `limit: usize` (default `DEFAULT_QUERY_LIMIT = 100`, limits.rs:20). `limit` is a *target*: *"The server completes the current block before stopping, so the actual count may exceed this"*.
- **`QueryLimits`** (limits.rs:54-88): per-deployment `{max_limit, max_block_range}`; breaches surface as `LimitExceeded` → queryX error `-32005` (limits.rs:47-52). Neither caps per-block results. `UNLIMITED` const for tests/trusted callers (limits.rs:60-64). `check_limit` rejects `0` as `InvalidRequest` and `> max_limit` as `LimitExceeded{Limit}` (limits.rs:73-87).
- **`LimitExceededKind`** `{Limit, BlockRange}` with `Display` (limits.rs:90-103).

---

## 6. error.rs — taxonomy

Single enum `MonadChainDataError` (error.rs:22-68), `Result<T>` alias (error.rs:20). Variants:

| Variant | Payload | Meaning / class |
|---|---|---|
| `Backend(String)` | dynamic msg | store/IO layer failure — the only environment-dependent (potentially transient/retryable) class; everything else is deterministic |
| `Decode(&'static str)` | static msg | corrupt/unexpected stored bytes, internal overflow, violated data invariants — fatal for the datum |
| `InvalidRequest(&'static str)` | static msg | caller error: bad ranges, zero limit, transfers-via-scan |
| `InvalidBlock(&'static str)` | static msg | malformed ingest input (e.g. trace depth sequence) — block rejected |
| `MissingData(&'static str)` | static msg | expected row absent (block record at a bound, indexed tx that won't materialize) |
| `SealedDirectoryBucketMissingSummary{bucket_start}` (error.rs:34-43) | | broken ingestion/compaction commit contract — *"surfaced loudly rather than masked by a fragment scan"* |
| `SealedPageMissingArtifact{stream_id, page_start}` (error.rs:44-53) | | same contract break for a sealed bitmap page; loud instead of *"silently dropping real matches"* |
| `PrimaryDirectoryMissingId{id, backing}` (error.rs:54-61) | | inventory resolved an id the backing store lacks — same contract break |
| `LimitExceeded{kind, max_limit, max_block_range}` (error.rs:62-67) | | query-shape cap breach; queryX `-32005` |

Pattern: the three "commit contract" variants are deliberately fatal+loud invariant violations (storage corruption indicators), not user errors. `Decode` always carries `&'static str` so error construction is allocation-free; only `Backend` allocates.

---

## 7. testkit.rs — test utilities

Philosophy (module doc, testkit.rs:16-19): *populate a store through the **real branchless ingest engine**, then read it back via `MonadChainDataService`* — no mocked write path. In-memory stores clone-share `Arc<RwLock>` backing, so readers built from clones see engine writes without plumbing.

- **`VecSource`** (testkit.rs:38-68): `Vec`-backed `ChainDataIngestSource` over blocks `[start, start+len)`; `get_latest_uploaded` reports the last block; `fetch_finalized_block` errors out-of-range.
- **`PopulatedStore { meta: InMemoryMetaStore, blob: InMemoryBlobStore }`** (testkit.rs:72-90): hold it to keep backing maps alive; `reader()` → `MonadChainDataService` with `QueryLimits::UNLIMITED`; `reader_with_limits(limits)` for limit tests.
- **`TEST_PREFETCH`** (testkit.rs:94-97): concurrency 4 / buffer 8 — deliberately >1 so fixtures exercise the ordered-prefetch path.
- **`populate_config`** (testkit.rs:101-119): backfill cadence (8 MiB pack target, 4096 max blocks, `tip_lag_divisor: 1`, checkpoint every 4096) so small fixtures see few flushes and the terminal flush guarantees `published head == last_block`.
- **Entry points**: `populate_via_engine(blocks)` (panics on error/empty — testkit.rs:134-139), `try_populate_via_engine` (fallible, for negative tests), `populate_via_engine_with_dict(blocks, DictConfig)` (small `epoch_blocks` to exercise the dict lifecycle — testkit.rs:151-159). Input blocks must be **parent-linked and contiguously numbered**.
- **`run_engine`** (testkit.rs:163-194) shows the canonical wiring: fresh in-memory stores → `SnapshotStore` → `Tables::with_all_configs` → `TablesCodecResolver` → `PublicationTables` → `VecSource` → `run_ingest(...)`.
- Typical test flow: build `FinalizedBlock` fixtures → `populate_via_engine` → `store.reader()` → `reader.query_logs(...)` etc.

---

## 8. Cross-family relations

- **Mechanism**: positional coordinates, not foreign keys. Every record carries `(block_number, tx_index)`; the block side needs only `block_number`. No stored edges — joins recomputed per response page.
- **Opt-in via `*Relations` flags**: logs/traces/transfers offer `{blocks, transactions}`; txs offers `{blocks}` only. Responses carry `Option<Vec<Block>>` / `Option<Vec<TxEntry>>`, `None` unless requested, **deduped and ascending** regardless of query order (logs/materialize.rs:67-71).
- **Join execution** — `MonadChainDataService::join_relations` (api.rs:151-177): `blocks` → `load_blocks_by_numbers` (record + header, concurrency 8, blocks.rs:100); `transactions` → `load_txs_by_positions` (txs/materialize.rs:167-200): dedupe through a `BTreeSet`, group indices per block in a `BTreeMap`, then **one coalesced batched read per block** (`load_records_in_block`, fan-out `RELATION_TX_CONCURRENCY = 8`) hitting the tx row cache.
- **What makes it cheap**: (1) `BlockBlobHeader.offsets` allow point-extraction of row N without decoding siblings; (2) frame range-reads coalesce; (3) decoded rows land in `RowCache`s; (4) `BlockRecord` already holds number/hash/parent for stamping and `BlockRef`s.
- **Point lookup by hash**: `get_transaction(tx_hash)` (api.rs:245-275) → `tx_hash_index` KV → `TxLocation` → guard `location.block_number <= published head` (unpublished ⇒ `None`) → `TxMaterializer::load_record_at` + block header. Index hit that fails to materialize inside the published range = `MissingData`, not `None` (api.rs:240-244).
- **Transfers→transactions** works identically because `TransferEntry` keeps `tx_index`; transfers→traces needs no join (a transfer *is* a trace row).

---

## 9. Glossary

- **Family** — one of the engine's indexed record domains: `Log`, `Tx`, `Trace`. Transfers are a *projected view* of `Trace`, not a fourth family.
- **Entry vs Stored** — `*Entry`: public query result with block context stamped on; `Stored*`: the on-disk row with block-level fields stripped.
- **Stamping** — re-attaching `block_number`/`block_hash` (and for txs, `tx_index`) from the `BlockRecord` at read time (`into_record_owned`).
- **PrimaryId** — global, dense, per-family u64 row ordinal assigned sequentially at ingest; defines canonical record order. `LogId`/`TxId`/`TraceId` are its family-scoped newtypes.
- **Family window** — `FamilyWindowRecord{first_primary_id, count}`: a block's contiguous id slice in one family.
- **Stream / StreamKey / stream id** — one indexed posting list: an `(IndexKind, value-bytes)` pair; per-record expansion is `stream_entries_for_*`.
- **IndexKind** — Addr, Topic0–3, From, To, Selector, TopLevel, HasTransfer.
- **IndexedClause** — one AND-term of a query: an `IndexKind` plus an OR-set of values; **marker clause** = presence-only (TopLevel, HasTransfer).
- **Indexed path vs block-scan path** — bitmap-driven candidate intersection vs decode-every-row-in-window; chosen by `has_indexed_clause()`.
- **`has_transfer` bit** — ingest-time index marker defined solely by `is_transfer_frame` (traces/ingest.rs:74); the transfers view is exactly the frames carrying it.
- **trace_address** — OpenEthereum-style path-from-root within a tx call tree; empty = top-level frame.
- **BlockBlobHeader** — per-(family, block) frame directory: row byte offsets + sentinel, dict version, region/physical placement in the shared `block_blob` object.
- **Row chain / seal chain** — standby-verification digest chains: per-block over raw row bytes (`BlockRecord.row_chain`, surfaced at `PublicationState.head_row_chain`), and per-family over sealed artifacts (`{family}_seal_chain`).
- **Published head** — `PublicationState.indexed_finalized_head`: the reader-visible watermark.
- **BlockRef / BlockSpan** — `{number, hash, parent_hash}` value-reference; span = `{from, to, cursor}` refs on every response.
- **QueryEnvelope** — shared request fields: optional from/to block (order-dependent roles), order, target limit.
- **Relations** — opt-in response joins (`blocks`, `transactions`) computed from positional coordinates.
- **tx_hash_index** — the only non-positional lookup structure: tx_hash → 12-byte `TxLocation`.
- **Materializer** — a family's `IndexedFamilyQuery` impl: tables handle + row cache + decode + stamping driven by the generic query runner.

---

## 10. Gotchas / surprising design choices & open questions

**Gotchas / design choices**
1. **Transfers are native-value only.** No ERC-20/log-derived transfers; "transfer" = value-moving successful trace frame inside a committed tx (traces/ingest.rs:74-84). Docs must not imply token-transfer coverage.
2. **The transfer definition is write-time-frozen.** The bit is computed at ingest with *"no read-side mirror"* (traces/ingest.rs:69-71) — changing `is_transfer_frame` changes the view only for newly ingested blocks; a definition change implies re-backfill.
3. **Transfers hard-fail the block-scan path** (transfers/materialize.rs:187-195) because `TransferFilter::matches` deliberately omits the status checks the bitmap pre-filters; an unindexed transfers query is `InvalidRequest`, not a slow path.
4. **Double status check is load-bearing**: `status == 0 && tx_status` — the tracer doesn't rewrite descendant statuses when a parent reverts (traces/ingest.rs:72-73); a per-frame check alone would surface phantom transfers.
5. **tx `tx_index` is positional, not stored** — reconstructed from `idx_in_block` (txs/materialize.rs:149-157), while logs and traces store their indices in-row.
6. **`log_index` is block-relative**, assigned by `flatten_logs` as a running count across the whole block (logs/ingest.rs:40) — matches Ethereum receipt semantics but worth stating.
7. **Tx envelope is parsed lazily and repeatedly** on the read side (`TxEntry::to()/selector()` each re-decode; txs/types.rs:38-39), but parsed *eagerly and fallibly* at ingest (a tx whose signed bytes don't decode 2718 fails ingest). Filters on `to`/`selector` decode once per candidate tx (txs/materialize.rs:96-110).
8. **`sender` is caller-authoritative** — never ECDSA-recovered (ingest_types.rs:32-34); `to_rpc_transaction` uses `Recovered::new_unchecked` (txs/types.rs:61-64). Garbage-in stays in.
9. **`effective_gas_price` / `block_timestamp` are `None`** in `to_rpc_transaction`; the RPC layer fills them from the header (txs/types.rs:55-58).
10. **`tx_hash_index` is last-write-wins** with no published-head filtering at *write* time; staleness handled at read by the head guard in `get_transaction` (api.rs:252-257).
11. **`CallKind` byte encoding ≠ declaration order** (StaticCall=1, ingest_types.rs:56-66) and is a local mirror of monad-archive's enum — keep in sync manually.
12. **`StoredTrace` needs a manual RLP shim** (derives can't do `Option<Address>`); `to` is length-discriminated `Bytes` (0/20, else decode error) (traces/types.rs:84-139).
13. **`is_top_level: Some(false)` is scan-only as a clause**: negation can't be a positive bitmap clause; a filter constrained only by it routes to block-scan; combined with positive clauses it post-filters indexed results (traces/materialize.rs:75-78).
14. **Block 0 is unqueryable**; `[0, N]` silently clamps to `[1, N]`, but `[0,0]` errors (range.rs:135-145). `EARLIEST_QUERYABLE_BLOCK = 1` because *"the current ingest path requires the chain to start at block 1"* (range.rs:27-29).
15. **`limit` is a soft target** — block-completion semantics mean responses can exceed both `limit` and `max_limit` for a hot block (limits.rs:30-33, 47-52).
16. **Format breaks are wipe-and-re-ingest**: `BlockRecord.traces` and `.row_chain` additions were hard RLP breaks (records.rs:171-177); `PublicationState` field order is wire format (records.rs:192).
17. **Topic filters are positional only** (4 fixed slots); OR within a slot, AND across slots — no cross-position semantics.
18. **`primitives/limits.rs` ≠ `LimitedVec`** — naming trap; it's query caps.
19. **Transfers share the trace row cache and trace dict tables** (family_runner.rs:102-104, engine/family.rs:26-28) — no transfer-specific storage exists at all, including stats.
20. **`matches` is dual-purpose**: invariant check on the indexed path (a false there indicates an index bug for fully-indexed filters), sole filter on the scan path (engine/clause.rs:71-76).

**Open questions** (for doc authors to resolve/flag)
- `LogFilter.topics` is `[Option<HashSet<B256>>; 4]` — no representation of eth_getLogs' nested-null wildcard beyond `None`; confirm transport-layer mapping (wire.rs in monad-rpc) handles spec topic arrays.
- `TxFilter.to`/`selector` never return creations (set_allows rejects absent values) — intended per spec, but worth an explicit doc note.
- `tx_index` overflow checks use error variant `Decode` even on ingest paths (logs/ingest.rs:36-41, txs/ingest.rs:40) where `InvalidBlock` might seem more apt — taxonomy slightly blurred.
- `selector()` on a trace whose input is exactly 4 bytes counts as a selector (traces/types.rs:60-62) — same rule write/read; document the ≥4-byte rule.
- Cursor advance rules are documented on the response types but actual cursor mechanics live in the engine's `family_runner`/`window` — cross-reference needed.
