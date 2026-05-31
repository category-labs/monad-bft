# Deep dive: the bitmap inverted-index lifecycle

> Prerequisite: read `docs/onboarding.md` and `docs/deep-dive-frontier-model.md`
> (this index is the canonical instance of the sealed/open split). This dive
> follows one indexed value from ingest-time fragment, through open page, to
> sealed page + manifest, and back out at query time.

## What this is and why it exists

The bitmap index is the engine's main filtered-query accelerator. Every filtered
query — logs by address/topic, txs by from/to/selector, traces by
from/to/selector/top_level/has_transfer — is answered by intersecting roaring
bitmaps that record `field-value → record-local-id` membership. Without it, those
queries would be full block scans.

The whole thing is built on one symmetry: **the same function,
`sharded_stream_id(kind, value, shard)`, names a posting list at ingest and looks
it up at query.** Write and read can't drift apart because they compute the same
string.

## The coordinate system

A record's `PrimaryId` is `(shard, local)` with `local` in the low 24 bits
(`src/primitives/state.rs:31`). Within a shard's `2^24` local ids:

```
STREAM_PAGE_LOCAL_ID_SPAN = 64 * 1024 = 65536        // src/engine/bitmap.rs:35
STREAM_PAGES_PER_SHARD    = 2^24 / 65536 = 256       // src/engine/bitmap.rs:40
page_start_local(local)   = (local / 65536) * 65536  // round down to page boundary
```

So: **a shard is exactly 256 pages of 64K locals.** (Pages nest cleanly in
shards; the 10k directory bucket does not — see the frontier dive.)

## 1. Streams — the posting list

A **stream** is one inverted-index posting list: "which record local-ids *within
one shard* have field `kind` = byte-value `value`?" Its id is a string
(`sharded_stream_id`, `bitmap.rs:687`):

```
"{kind}/{hex(value)}/{shard:010x}"      e.g.  addr/a0b1…/0000000000
```

The shard is zero-padded to a fixed 10 hex digits so the shard is **recoverable
from the stream id** (`parse_stream_shard`, `bitmap.rs:762`). That's why the
page-count manifest can be keyed by `stream_id` alone — the shard is already baked
in.

The indexed **kinds** differ per family (emitted at ingest):

- **Logs** (`src/logs/ingest.rs:169`): `addr` always; `topic0..3` for each present
  topic (absent topics emit nothing).
- **Txs** (`src/txs/ingest.rs:159`): `from` always; `to` only if a recipient
  (skipped for contract-creation); `selector` only if calldata ≥ 4 bytes.
- **Traces** (`src/traces/ingest.rs:139`): `from`; `to` if present; `selector` if
  input ≥ 4 bytes; `top_level` (empty-bytes value) for the root frame only;
  `has_transfer` (empty-bytes value) when the frame is a successful, committed,
  value-moving call. The `has_transfer` stream *is* the entire transfers feature —
  transfers queries just AND in that one bit (see `onboarding.md`).

**Why shard the stream id?** It bounds each posting list to one shard's `2^24`
local space, so a stream's bitmap can never grow unbounded, the index partitions
cleanly by shard, and **shards seal independently**.

## 2. Pages — partitioning the local space

Within a shard, the local space is sub-partitioned into the 256 disjoint 64K
pages. Pages exist for four reasons, and the fourth is the important one:

1. Bounded bitmaps (each ≤ 64K locals).
2. Page-level skip (the sealed-shard manifest can drop a whole page).
3. Page-level concurrency (the query pipeline runs pages in parallel).
4. **The distributive law** that makes per-page intersection + short-circuit
   correct (§6).

A page's bitmap is a `roaring::RoaringBitmap`. The stored blob carries
`(min_local, max_local, count)` in a header *plus* the serialized roaring payload.
Query-time skip decisions trust the header bounds, so `decode_bitmap_blob`
**re-derives the bounds from the payload and fails loud on drift** — a corrupted
too-narrow header would otherwise silently drop a matching page.

## 3. Ingest-side: fragments

A **fragment** is *one block's contribution to one stream's one page* — the local
ids that block added for that field-value, within that 64K page.

At plan time each record expands into `(stream_id, local_id)` pairs
(`stream_entries_for_log` etc.), and `encode_grouped_bitmap_fragments`
(`bitmap.rs:622`) groups them into `stream → page → roaring bitmap`:

```rust
grouped.entry(stream_id).or_default()
       .entry(page_start_local(local_id)).or_default()
       .insert(local_id);
```

A single block whose ids cross a page boundary therefore yields **multiple**
page-fragments for one stream. Each non-empty page bitmap becomes a
`BitmapFragmentWrite { stream_id, page_start_local, bitmap_blob }`.

Two tables hold the open state:

- **`bitmap_by_block`** (the fragments) — partition key
  `stream_page_key(stream_id, page)`, clustering key `block_number`. So all of one
  page's per-block fragments live under one partition, listed by prefix.
- **`open_bitmap_stream`** (the inventory) — per global frontier page, the *set of
  stream ids* that have any fragment in that page. It's **append-only by design**
  so replay can't lose membership. This is the seal-time index telling the
  compactor *which* streams to compact for a sealing page (computed per block as
  `touched_streams_by_page`, `bitmap.rs:712`).

## 4. The seal transition: fragments → page artifact + manifest

While a shard holds the frontier, its pages stay open (many small per-block
fragments, membership in the inventory). The seal trigger is the frontier moving
*past* the unit — driven from Phase B of ingest
(`src/engine/ingest/bitmap_compaction.rs`):

- **Page seals** when the frontier moves to a later page. Every touched stream's
  retained fragments for that page are OR-merged into one **`bitmap_page_blob`**
  artifact (`compact_bitmap_page`, `bitmap.rs:658` — an empty merge is corrupted
  state and fails loud). Reads of a sealed page hit this artifact; reads of the
  still-open page fall through to scanning fragments.
- **Shard seals** when the frontier crosses its last (256th) page. Now the shard's
  per-stream **`bitmap_page_counts`** manifest is written.

### The manifest (`BitmapPageCounts`)

A sparse, per-stream, per-page roll-up of compacted counts for one fully-sealed
shard, immutable once written. Key = the `stream_id` (which already encodes the
shard). Built by walking all 256 page slots of the shard, taking each
(stream, page)'s count from this batch's fresh compacted metas or the durable
artifact's stored count.

Two semantics to internalize (`bitmap.rs:541`):

- **Zero-count pages are dropped** (`from_pairs` filters `count != 0`), and
  `count_for_page` binary-searches and **never returns `Some(0)`** — a missing
  page means "this stream contributes nothing here."
- **A touched-but-missing artifact is a hard error** (`SealedShardPageMissingArtifact`).
  Why so strict: on a *present* manifest an absent page reads as count 0, which on
  a sealed shard would skip the page with zero fetches and silently drop matches.
  So the manifest must be complete or fail.

## 5. Query-side: the read

Per shard, `build_shard_page_plan` (`src/engine/query/bitmap.rs:71`) precomputes a
`ShardPagePlan`:

- `clause_streams` — per clause, its OR-set of value-streams in this shard. (If
  any clause has *no* streams in this shard, the whole shard intersection is empty
  → return `None`, bail before any fetch.)
- `clause_counts` — per clause, its summed manifest (`None` = unknown, never
  empty).
- `shard_sealed = shard < frontier_shard` — the gate that decides whether `Some(0)`
  may skip.

Then per candidate page, `intersect_shard_page` (`query/bitmap.rs:186`):

1. **Sealed-shard zero-fetch skip**: `if shard_sealed && counts.contains(&Some(0))`
   → skip, no fetch. On the frontier shard `shard_sealed` is false, so its
   (borrowed) manifest never skips — it only *orders* fetches.
2. **Order clauses most-selective-first** by manifest count (unknowns last,
   stable).
3. **AND down with short-circuit**: fetch each clause's page bitmap (a sealed page
   reads the artifact; an open page scans fragments), AND into the running set,
   and **the moment it goes empty, `break`** — skipping every remaining clause's
   fetch for this page.
4. **Clip** the surviving bitmap to the query's local range (only the two boundary
   pages can carry out-of-range bits).

A clause's per-page count is the **sum over its OR value-streams** — so a clause
is empty in a page iff all its value-streams are. And if *any* value-stream lacks
a manifest row, the whole clause estimate degrades to `None` ("unknown"), never an
under-count that could wrongly skip.

## 6. The distributive law — the keystone invariant

Documented at `query/bitmap.rs:121`:

```
⋂_clauses ( ⋃_pages bits )  ==  ⋃_pages ( ⋂_clauses bits_on_that_page )
```

It holds because pages **partition** the local space and **every clause bit in a
page comes only from that page's stream rows** — so intersecting whole-clause
bitmaps (each a union over pages) equals unioning the per-page intersections, and
the disjoint pages never double-count on the outer OR.

This is what licenses everything in §5: per-page short-circuit (an empty page
prunes the rest of *that page's* clause fetches — the old clause-outer loop could
only prune a clause empty across the *whole* shard) and per-page concurrency. Two
equivalence tests pin the optimized path against a serial reference:
`tests/query_bitmap_page_intersection.rs` (serial == concurrent, with fetch
counting) and `tests/query_bitmap_page_counts_manifest.rs` (manifest skips fetch
nothing on a sealed shard).

`load_intersection_bitmap` (`query/bitmap.rs:145`) is that serial reference — no
longer on the production path (the pipeline calls `build_shard_page_plan` +
`intersect_shard_page` directly) but kept behavior-equivalent for the tests.

## 7. Clauses → streams (the symmetry, restated)

An `IndexedClause` (`src/engine/clause.rs:23`) is `{ kind, values }`: one field,
its `values` OR'd together. Clauses AND together. `stream_ids_for_shard` maps each
value through the *same* `sharded_stream_id` the ingest side used. So for clauses
`C1..Cn`, page P contributes:

```
⋂_i ( ⋃_{v ∈ Ci.values}  stream(Ci.kind, v, shard).page(P) )
```

fetched most-selective-first, short-circuiting on empty, clipped to the local
range. The `matches()` method on each filter is an *invariant check* on
materialized records, not a release-mode post-filter — with one documented trace
exception (`is_top_level: Some(false)` combined with other clauses) the runner
must drop.

## The lifecycle, end to end

```
INGEST   record --stream_entries--> (stream_id, local_id) pairs
         --encode_grouped_bitmap_fragments--> per (stream,page) fragment
         --> bitmap_by_block (fragment)  +  open_bitmap_stream (inventory)

SEAL     frontier passes page  --compact_bitmap_page--> bitmap_page_blob
(Phase B) frontier passes shard --roll up counts------> bitmap_page_counts (manifest)

QUERY    clause --stream_ids_for_shard--> streams
         --build_shard_page_plan--> candidate pages (manifest skips empties)
         --intersect_shard_page--> AND clauses most-selective-first, short-circuit
         survivors --(+shard)--> PrimaryId --directory--> (block, idx) --materialize-->
```

## The mental model to keep

> An inverted index of `(kind, value, shard) → local-id bitmap`, partitioned into
> 64K pages. Ingest writes small per-block fragments to the open frontier page;
> compaction OR-merges sealed pages into single artifacts and, once a shard fully
> seals, writes a per-stream page-count manifest. Queries AND clause bitmaps
> *per page* (legal by the distributive law), fetching most-selective-first and
> short-circuiting on empty, with the sealed-shard manifest skipping provably-empty
> pages for free. The same `sharded_stream_id` builds and reads the index, so they
> never drift.

## Start-here pointers

- Stream id + arithmetic: `src/engine/bitmap.rs:35,687`
- Ingest expansion: `src/logs/ingest.rs:169`, `txs/ingest.rs:159`,
  `traces/ingest.rs:139`
- Fragment grouping: `src/engine/bitmap.rs:622`; tables in `family.rs:21`
- Seal/compaction + manifest: `src/engine/ingest/bitmap_compaction.rs`;
  `compact_bitmap_page` `bitmap.rs:658`; manifest `bitmap.rs:530`
- Query plan + intersect: `src/engine/query/bitmap.rs:71,186`; distributive law `:121`
- Clauses: `src/engine/clause.rs:23`
- Equivalence tests: `tests/query_bitmap_page_intersection.rs`,
  `tests/query_bitmap_page_counts_manifest.rs`
