# monad-chain-data — onboarding

## What this crate is

It's a **storage + query engine for EVM execution history**. You feed it
finalized blocks one batch at a time (the *ingest* path); it lets you ask
"give me the logs/transactions/traces/transfers matching this filter over this
block range" (the *query* path). It is not the transport or the JSON-RPC layer —
those live elsewhere and call into the `MonadChainDataService` API in
`src/api.rs`.

Everything hangs off three ideas:

1. **Families** — the engine knows three kinds of record, and treats them almost
   identically.
2. **Primary IDs** — every record gets a single, gapless, global `u64` id within
   its family. That id *is* the data model. Indexes map *into* it; the directory
   maps *out* of it.
3. **The published head** — a single compare-and-swap'd row is the only thing
   that makes data visible to readers. Below it: immutable, queryable. Above it:
   doesn't exist as far as a query is concerned.

---

## The data model

### Families

A **family** (`src/engine/family.rs`) is an indexed, queryable stream of one kind
of record. There are exactly three, and the `Family` enum is the canonical list:

- **`Log`** — EVM event logs.
- **`Tx`** — transactions (hash, sender, raw signed bytes).
- **`Trace`** — call frames (CALL / CREATE / SELFDESTRUCT / …), flattened per
  block.

There's a fourth thing you can query — **transfers** — but it is *not* a family.
A transfer is a **view over the trace family**: at ingest, any trace frame that
moves value gets a `has_transfer` bit set in the index, and the transfers query
is just a trace query that ANDs in that one bit and projects each matching
`TraceEntry` into a `TransferEntry` (`src/transfers/materialize.rs`). It reuses
the trace family's blobs, directory, and bitmaps wholesale. Keep this in your
head: **transfers ride entirely on trace storage.**

The three families are structurally identical — same set of tables, same
indexing, same query machinery. That's why so much of the engine is generic over
a family (`execute_indexed_family_query`, `PrimaryIdResolver`, the bitmap code).
The per-family modules (`src/logs`, `src/txs`, `src/traces`) mostly just define
the record type, what fields are indexed, and how to encode/decode a row.

### Primary IDs — the spine

Every record in a family has a `PrimaryId` (`src/primitives/state.rs`): a `u64`
that counts up, gaplessly, across the whole family's history. Log #0, log #1,
… log #N regardless of which block they're in. Same for txs, same for traces.
(They're wrapped in `LogId` / `TxId` / `TraceId` newtypes so you can't cross the
streams by accident, but underneath it's one `PrimaryId`.)

The id is split into two parts:

```
   63                    24 23            0
  +------------------------+--------------+
  |        shard           |    local     |
  +------------------------+--------------+
            (40 bits)         (24 bits)
```

- **local** (low 24 bits) → up to ~16.7M records per shard.
- **shard** (the rest) → which 16.7M-record band you're in.

It is split because the bitmap index works in *local* space within a shard,
and the shard is the unit at which the index seals and becomes immutable. More on
that below. Just remember: **a `PrimaryId` is a `(shard, local)` pair**, and
`from_parts` / `.shard()` / `.local()` convert back and forth.

### Blocks tie ids back to chain position

The id-space is continuous and ignores block boundaries. The thing that records
where the boundaries are is the **`BlockRecord`** (`src/primitives/state.rs`). One per block:

```rust
struct BlockRecord {
    block_number, block_hash, parent_hash,
    logs:   FamilyWindowRecord,   // { first_primary_id, count }
    txs:    FamilyWindowRecord,
    traces: FamilyWindowRecord,
    artifact_checksum,            // chained digest — see standby verification
}
```

A `FamilyWindowRecord` is just `{ first_primary_id, count }` — "this block's logs
are ids `[first, first+count)`". Across all blocks these windows **partition the
id-space perfectly: contiguous, no gaps, no overlaps.** That invariant is what
makes everything else work:

- The next block's `first_primary_id` is simply the previous block's
  `first + count`. Ingest computes ids by running this cursor forward.
- Given a block, you know its id range. Given an id, you can find its block
  (that's what the directory index is for).
- A record stores only its *intra-block* fields in the blob; block-level fields
  (`block_number`, `block_hash`) are stripped at ingest and re-attached at read
  from the `BlockRecord`. Compare `RawLogEntry` (stored) vs `LogEntry`
  (materialized) in `src/logs/types.rs`.

### How a record is physically stored

Per family, per block, there's a **blob** and a **header**
(`src/logs/materialize.rs`, `src/logs/ingest.rs` show the log version):

- The **blob** is every row in the block, RLP-encoded then zstd-frame-compressed
  one row at a time, concatenated.
- The **header** is a list of byte `offsets` into that blob (row *i* lives at
  `offsets[i]..offsets[i+1]`) plus the `dict_version` the frames were compressed
  under.

So to read record *i* of a block: load the header, slice `offsets[i..i+2]`, pull
that byte range out of the blob, decompress, decode. Random access by intra-block
index, which is exactly what the query path needs.

(The `dict_version` is an epoch-based compression dictionary — every N blocks a
new dict is trained so common byte patterns compress well. Not important for
understanding the paths; just know the header tells you which dict decodes the
blob, and the checksum digest deliberately ignores compression entirely.)

### The two indexes

Every family carries two indexes, and they answer opposite questions.

**1. The primary directory — "id → where is it?"**
(`src/engine/primary_dir.rs`, `src/engine/query/directory_resolver.rs`)

Maps a `PrimaryId` back to `(block_number, idx_in_block)`. It's bucketed in
ranges of 10,000 ids. A bucket whose whole range sits below the published-head
frontier is **sealed** — it has a compacted summary you binary-search. The single
bucket straddling the frontier is **open** — you scan its per-block fragments.
The resolver routes analytically: it knows `sealed_below`, so it never probes the
summary and falls back to fragments; sealed → one summary read, open → fragment
scan. (A sealed bucket missing its summary is a hard error, not a silent fallback
— the commit contract guarantees it's there.)

**2. The bitmap index — "value → which ids?"**
(`src/engine/bitmap.rs`, `src/engine/query/bitmap.rs`)

This is the inverted index that makes filtered queries fast. The core unit is a
**stream**: `sharded_stream_id(kind, value, shard)`. For a log, ingest emits one
entry per indexed field into the matching stream
(`src/logs/ingest.rs::stream_entries_for_log`):

```
addr=0xABC   shard 3   ->  set local-bit for this log
topic0=0x12  shard 3   ->  set local-bit
topic1=...   shard 3   ->  set local-bit
```

Each stream is a roaring bitmap of *local* ids within its shard. Streams are
chopped into **pages** spanning 64K local ids each. A sealed shard also carries a
**page-count manifest** (how many bits each stream has in each page) so the query
side can skip provably-empty pages without fetching them.

The names `addr` / `topic0..3` (logs), `from` / `to` / `selector` (txs),
`has_transfer` etc. (traces) are the indexed **kinds**. The exact same
`sharded_stream_id(kind, value, shard)` call builds the stream id at ingest and
looks it up at query — that symmetry is the whole trick, so when you read one
side, read the other.

### The published head

One row, `PublicationState` (`src/primitives/state.rs`), guarded by
compare-and-swap. Its `indexed_finalized_head` is **the reader-visible
watermark**: queries only ever see blocks at or below it. Advancing it is the
atomic act of publication — all the artifacts for a block are written *first*,
then the head moves in one CAS. The other fields (`owner_id`, `session_id`,
`lease_valid_through_block`) are write-coordination state the query path never
looks at, plus `head_artifact_checksum` for standby verification.

---

## The query path

Entry points are `query_logs` / `query_transactions` / `query_traces` /
`query_transfers` / `query_blocks` in `src/api.rs`. They all follow the same
shape (read `query_logs` at `src/api.rs:1791` as the canonical one):

```
1. check the request limit
2. load the published head
3. resolve the block window         (clamp to head, enforce max range)
4. dispatch:  indexed  vs  block-scan
5. (optional) join "relations" — block headers, txs — onto the results
```

### Resolving the window

`ResolvedBlockWindow::resolve` (`src/primitives/range.rs`) turns the request's
`from_block`/`to_block` (whose lower/upper roles depend on query `order`) into a
clean low≤high block range, clamped to the published head and bounded by
`QueryLimits::max_block_range`.

For an indexed query that block range then becomes an **id window**
(`src/engine/query/window.rs`): walk up from the low block to the first non-empty
family window → first id; walk down from the high block to the last non-empty
window → last id. (The two walks run concurrently.) Now you have `[start_id,
end_id]` in the family's primary-id space, which is what the bitmap index speaks.

### Indexed vs block-scan

The fork is one question: **does the filter touch an indexed field?**
(`filter.has_indexed_clause()`).

- **No indexed clause** → **block scan** (`execute_block_scan_family_query`,
  `src/engine/query/family_runner.rs:309`). Walk the blocks in order, load each
  block's whole blob, decode and filter every row, stop when the limit is hit.
  O(rows in range). Used for e.g. "all logs in blocks 100–110".

- **At least one indexed clause** → **indexed query**
  (`execute_indexed_family_query`, same file, line 97). The fast path. O(matching
  rows). This is the heart of the engine — described next.

### The indexed pipeline

A filter is a list of **clauses** (`src/engine/clause.rs`). Each clause is one
field; its `values` are OR'd together; clauses are AND'd with each other. E.g.
`address ∈ {A,B} AND topic0 = T` is two clauses.

The runner does, conceptually:

```
for each shard in the id window:
    build a per-shard plan: the clause streams + the page-count manifest
    for each candidate page in this shard's local range:
        intersect the clauses on this page  ->  surviving local ids
        resolve each survivor via the directory  ->  (block, idx)
        materialize  ->  the actual record
stop once `limit` records are collected (block-aligned), record a cursor
```

The key insight that makes per-page work correct is the **distributive law**:

```
⋂_clauses ( ⋃_pages bits )  ==  ⋃_pages ( ⋂_clauses bits_on_that_page )
```

Because pages partition the local-id space, you can intersect clause bitmaps
*one page at a time* and union the results. That's a big win over building each
clause's full bitmap and then ANDing: per page you fetch the most-selective
clause first, AND down, and the **moment the running intersection goes empty you
stop fetching the rest of that page's clauses** (`intersect_shard_page` in
`src/engine/query/bitmap.rs`). On sealed shards the manifest lets you skip pages
that any clause proves empty with *zero* fetches.

The pipeline is staged and concurrent (`family_runner.rs`): page intersections
run `PAGE_CONCURRENCY` at a time, materializations `min(limit, ceiling)` at a
time, all kept in query order so the consumer can apply a clean **block-aligned
limit** — once it has `limit` records it finishes the current block and stops,
emitting a `cursor_block` so the caller can paginate.

**Materialize** is the last step (`src/logs/materialize.rs::load_record_at`):
take a `(block, idx)`, load that block's header + the one row's byte range from
the blob, decompress, decode, re-attach block-level fields. The directory got you
the location; materialize turns the location into the object.

So the full indexed flow for one record:

```
filter clause  --(sharded_stream_id)-->  bitmap streams  --intersect-->
local ids  --(+shard)-->  PrimaryId  --directory-->  (block, idx)
  --blob slice + decode-->  LogEntry/TxEntry/TraceEntry
```

`get_transaction(hash)` is a special case: txs also keep a hash→location index
(`src/txs/hash_index.rs`), so a by-hash lookup skips the bitmap machinery and
goes straight to materialize.

### Querying the unfinalized tip

The indexed path only sees finalized data (≤ published head). To answer queries
that reach `latest` — the proposed, not-yet-indexed blocks — the caller scans
those blocks **in memory** (`src/mem_scan.rs`). The crucial property:
`scan_block_logs` / `scan_block_txs` construct the *exact same* `LogEntry` /
`TxEntry` rows, with the *exact same* `log_index` assignment and the *exact same*
filter matchers, that the indexed path would produce. So the caller can run the
indexed query up to the head, run the mem-scan over the tip blocks, and
concatenate — no semantic divergence between the two halves.

---

## The ingest path

The write path is the **branchless ingest engine** (`src/ingest_core.rs` +
`src/ingest_helpers.rs`, with thin `src/backfill.rs` / `src/live.rs`
controllers). It is *not* part of `MonadChainDataService` — that service is a
read-only query/verify layer. Ingest runs embedded in `monad-archiver`, which
implements the `ChainDataIngestSource` trait (`src/ingest_source.rs`) and calls
`run_configured_chain_data_engine_ingest` (`src/ingest_config.rs`); there is no
standalone ingest binary. Input is a stream of `FinalizedBlock`s
(`src/family.rs`): an EVM header plus `logs_by_tx`, `txs`, `traces`.

### The four tasks

The engine fuses fetch + id-assignment + signalling into a *producer*, fans each
block out to two *tracks* over bounded channels, and closes the loop with a
*publisher*:

```
producer     fetch block, assign sequential per-family primary ids, emit
             BatchFlush / Checkpoint signals on a cadence
data track   frame-compress each row (RLP → zstd) into packed blobs; flush packs
             on a size threshold → advances `data_durable`
index track  accumulate inverted-index bits + directory entries; seal completed
             granules and flush fragments → advances `index_visible`
publisher    CAS the published head forward to min(data_durable, index_visible)
             under the write lease
```

The two tracks are split so row compression runs in parallel with index
building. Everything the index track writes is durable on return (inline writes,
no separate writer task).

### Open accumulators — seal vs flush

The index track keeps two per-family accumulators:

* **OpenTail** — bits added since the last `BatchFlush` (the current delta).
* **OpenState** — bits already written as fragments in prior flushes whose
  page/bucket hasn't sealed yet (the carry-over).

Three operations, identical in backfill and live:

* **accumulate(block)** → insert the block's ids into OpenTail.
* **seal** (continuous, frontier-driven) → once a 64K granule is fully below the
  id frontier, write `OpenState ∪ OpenTail` for it as a compacted page/bucket
  artifact and drop it from both.
* **batch_flush** → write each OpenTail entry as a reader-visible *fragment*
  (keyed by the same sharded stream id the query reads), carry it into OpenState,
  and advance `index_visible` to the tip.

The only difference between backfill and live is the signal cadence
(`SignalPolicy`) and the fetch plan (`FetchPlan`): backfill flushes/checkpoints
rarely (the head lags, amortizing the publish CAS); live flushes on every
tip-drain so the head tracks the tip.

### Publication — the single atomic reveal

Nothing is visible to a query until the publisher's CAS lands. The published head
lives in one `PublicationState` row, and `min(data_durable, index_visible)`
guarantees it never covers a block whose rows or index aren't both durable. A
writer can crash mid-batch and a reader never sees a half-written block: the
artifacts may be on disk, but the head never moved.

### Write authority — who's allowed to publish

`src/engine/authority.rs`. Exactly one process may advance the head at a time, via
a **lease** recorded in that same `PublicationState` row and tracked against an
external clock (the latest observed upstream finalized block). Standbys block
passively in `acquire_or_wait` and only take over once the lease expires.
Coordination flows entirely through CAS on the one row — there's no separate
consensus layer. A fenced or lost writer steps down (its pipeline aborts) rather
than risk split-brain. The query path never reads any of this; it's the outer
ring around publication.

### Recovery — resume from max(checkpoint, published head)

On startup or takeover the engine reconstructs the open (frontier) index state
before writing, picking between two sources (`src/ingest_recover.rs`):

* a **checkpoint** snapshot (`OpenState` + `OpenTail` in one meta row), which
  backfill writes periodically and which runs *ahead* of the published head; and
* a **rebuild from fragments** — re-deriving `OpenState` from the durable
  per-flush bitmap fragments + open-stream inventory at the published head, which
  live recovery uses (its head runs far ahead of the rare checkpoint).

Recovery resumes from `max(checkpoint_block, published_head)` and happens *after*
lease acquire, so the rebuild keys off the authoritative head and never
re-ingests already-published blocks. The two regimes are the subject of the
recovery deep-dive.

### Standby verification (not yet implemented)

The standby-equality mechanism is a per-block **content digest** over the
*logical* (pre-compression) artifacts, folded into a hash chain whose running
value is stored in every `BlockRecord.artifact_checksum`. A standby ingesting the
same finalized blocks re-derives the chain and compares one 32-byte value —
proving it would have written byte-equivalent artifacts without re-reading
storage or taking the lease, and agreeing even if their zstd versions emit
different bytes (the fold is over uncompressed rows). The digest primitives live
in `src/engine/digest.rs`. The engine publishes `EMPTY_CHECKSUM`; computing the
real chain (a cross-track fold of the data `rows_digest` and the index
`bitmap_fragments`) and the standby verify path are not yet built.

---

## A map of the crate

| Area | Where | What |
|---|---|---|
| Public service API | `src/api.rs` | `MonadChainDataService`: read-only query entry points |
| Core types | `src/primitives/state.rs` | `PrimaryId`, `FamilyWindowRecord`, `BlockRecord`, `PublicationState` |
| Family abstraction | `src/engine/family.rs` | the `Family` enum + its tables |
| Indexed query runner | `src/engine/query/family_runner.rs` | the staged indexed pipeline + block-scan |
| Bitmap intersection | `src/engine/query/bitmap.rs`, `src/engine/bitmap.rs` | streams, pages, per-page AND |
| Directory (id→location) | `src/engine/query/directory_resolver.rs`, `src/engine/primary_dir.rs` | sealed summaries vs open fragments |
| Clauses | `src/engine/clause.rs` | how a filter becomes AND-of-OR streams |
| Per-family logic | `src/logs`, `src/txs`, `src/traces` | `types` / `ingest` / `materialize` / `*_query` |
| Transfers (trace view) | `src/transfers` | `has_transfer` projection over traces |
| Ingest engine | `src/ingest_core.rs`, `src/ingest_helpers.rs` | branchless two-track write path |
| Ingest controllers | `src/backfill.rs`, `src/live.rs`, `src/ingest_config.rs` | backfill/live cadence + store wiring |
| Crash recovery | `src/ingest_recover.rs` | max(checkpoint, head): snapshot restore or fragment rebuild |
| Checksums | `src/engine/digest.rs` | content digest + chain (standby verify; not yet built) |
| Write authority | `src/engine/authority.rs` | lease, takeover, fencing |
| Unfinalized tip | `src/mem_scan.rs` | in-memory scan matching indexed semantics |
| Storage backends | `src/store` | meta store (CAS), blob store, cache, write session |

### Deep dives

Once the above makes sense, these go one level deeper on the cross-cutting
subsystems (each anchored to invariants rather than line numbers, so they age
gracefully):

- [`deep-dive-frontier-model.md`](deep-dive-frontier-model.md) — the sealed-vs-open
  split that governs both indexes and the in-memory state. **Read this first** —
  the others lean on it.
- [`deep-dive-primary-standby.md`](deep-dive-primary-standby.md) — multi-node write
  coordination: the lease lifecycle, fencing, and the checksum chain that lets a
  standby verify agreement (not yet built).
- [`deep-dive-ingest-batching.md`](deep-dive-ingest-batching.md) — the branchless
  ingest engine: the four tasks, open accumulators (seal vs flush), the three
  frontiers, backfill vs live, and why the ordering is crash-safe.
- [`deep-dive-recovery.md`](deep-dive-recovery.md) — how a restart rebuilds the
  open state: `max(checkpoint, head)`, the backfill vs live regimes, the
  fragment rebuild, and the mandatory above-head clamp.
- [`deep-dive-bitmap-index.md`](deep-dive-bitmap-index.md) — the inverted-index
  lifecycle from ingest fragment to sealed page + manifest, and the distributive
  law that makes per-page intersection work.

### The one paragraph to remember

Records get gapless global **primary ids**; `BlockRecord` windows partition that
id-space by block. A **bitmap inverted index** turns a filter value into a set of
ids (`value → ids`); the **directory** turns an id back into a block location
(`id → (block, idx)`); **materialize** turns a location into the row. Ingest
builds those two indexes and the blob, then makes it all visible with one
compare-and-swap on the **published head**. Queries read strictly below that head;
the unfinalized tip is scanned in memory with identical semantics and merged.
