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

Driven by `src/bin/chain-data-ingest.rs`, but the logic lives in
`plan_ingest_blocks` → `apply_ingest_plan` in `src/api.rs`. Input is a batch of
`FinalizedBlock`s (`src/family.rs`): an EVM header plus `logs_by_tx`, `txs`,
`traces`. The split into *plan* and *apply* matters — planning is pure CPU work
that can be parallelized and even verified without writing; applying is the I/O
and the publish.

### Plan (`plan_ingest_blocks`, src/api.rs:807)

```
1. begin_write: acquire/renew the write lease, learn the authoritative head,
   and run recovery if ownership just changed
2. validate the batch is contiguous: block N+1, parent_hash chains correctly
3. compute each block's starting ids by running the window cursor forward
4. ensure each epoch's compression dictionary is published before framing
5. PARALLEL (rayon) per block: build the log/tx/trace ingest plans
6. SEQUENTIAL: fold the artifact-checksum chain across blocks
7. stage all artifacts into one write session
```

Step 5 is where the per-family work happens (`LogIngestPlan::build` etc.). For
each family it: flattens the records, assigns intra-block indices, encodes each
row (RLP → frame-compress) into the blob while building the offsets header,
emits the **bitmap fragments** (the inverted-index entries, keyed by the same
`sharded_stream_id` the query reads), and folds a **row digest** over the
*uncompressed* row bytes. Because blocks are independent here, this fans out
across cores; the only sequential part is the checksum chain (step 6), which by
nature depends on the previous block.

### Apply (`apply_ingest_plan`, src/api.rs:1628)

```
1. commit the staged writes: blobs + metadata + index fragments
   (meta and blob stores commit concurrently)
   — plus a compaction step that seals directory buckets and compacts
     bitmap pages as the frontier moves
2. authority_publish: CAS the publication head forward to the new block,
   stamping the new head_artifact_checksum
```

Until step 2's CAS lands, none of the freshly written artifacts are visible to
any query — the published head is still where it was. Publication is the single
atomic reveal. This is why a writer can crash mid-batch and a reader never sees a
half-written block: the artifacts may be on disk, but the head never moved.

### Write authority — who's allowed to publish

`src/engine/authority.rs`. Exactly one process may advance the head at a time. It
holds a **lease**, recorded in that same `PublicationState` row, tracked against
an external clock (the latest observed upstream finalized block). Standbys watch
passively and only take over once the lease expires. Coordination flows entirely
through CAS on the one row — there's no separate consensus layer. When a process
takes over (lease expired, or it restarted with a fresh `session_id`), it runs
**recovery** to rebuild the open (frontier) index state before writing. The query
path never reads any of this; it's purely the outer ring around publication.

### Standby verification — the artifact checksum chain

`src/engine/digest.rs`. Every block produces a **content digest** over its
*logical* artifacts: the uncompressed row payloads, the EVM header, the per-family
windows, dict versions, and the bitmap fragments. These per-block digests are
folded into a **hash chain** — `chain(prev, block)` — whose running value is
stored in every `BlockRecord.artifact_checksum` and mirrored in the published
head.

The point: a **standby** ingesting the same finalized blocks re-derives the same
chain and compares (`verify_artifact_checksums`, `src/api.rs:1700`). If its
computed head matches the published head's checksum, it has *proven it would have
written byte-equivalent artifacts* — without re-reading a single stored byte, and
without taking the write lease. The digest folds rows *before* compression on
purpose, so two nodes agree whenever they agree on logical content even if their
zstd versions emit different bytes. One 32-byte value certifies the whole history.

---

## A map of the crate

| Area | Where | What |
|---|---|---|
| Public service API | `src/api.rs` | `MonadChainDataService`: ingest + query entry points |
| Core types | `src/primitives/state.rs` | `PrimaryId`, `FamilyWindowRecord`, `BlockRecord`, `PublicationState` |
| Family abstraction | `src/engine/family.rs` | the `Family` enum + its tables |
| Indexed query runner | `src/engine/query/family_runner.rs` | the staged indexed pipeline + block-scan |
| Bitmap intersection | `src/engine/query/bitmap.rs`, `src/engine/bitmap.rs` | streams, pages, per-page AND |
| Directory (id→location) | `src/engine/query/directory_resolver.rs`, `src/engine/primary_dir.rs` | sealed summaries vs open fragments |
| Clauses | `src/engine/clause.rs` | how a filter becomes AND-of-OR streams |
| Per-family logic | `src/logs`, `src/txs`, `src/traces` | `types` / `ingest` / `materialize` / `*_query` |
| Transfers (trace view) | `src/transfers` | `has_transfer` projection over traces |
| Ingest binary | `src/bin/chain-data-ingest.rs` | fetch → plan → apply driver |
| Checksums | `src/engine/digest.rs` | content digest + chain for standby verification |
| Write authority | `src/engine/authority.rs` | lease, takeover, recovery |
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
  coordination: the lease lifecycle, recovery, and the checksum chain that lets a
  standby verify agreement.
- [`deep-dive-ingest-batching.md`](deep-dive-ingest-batching.md) — the plan/apply
  split, parallel build vs. serial checksum fold, two-phase commit, and why the
  ordering is crash-safe.
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
