# Deep dive: the frontier / sealed-vs-open model

> Prerequisite: read `docs/onboarding.md` first. This dive expands the one idea
> that quietly governs the whole engine — how index state transitions from
> *mutable* to *immutable* — and why three different structures all implement it
> at three different granularities.

## The one idea

Every family's records occupy a gapless, monotonically growing `PrimaryId`
space. At the published head there is exactly one **frontier id**: the first id
the family has *not yet* assigned. That single number splits the id-space in two:

- **Below the frontier → sealed.** Immutable. Compacted into summary artifacts.
  A reader may trust and even *skip* sealed regions because the commit contract
  guarantees the artifact is present.
- **The region straddling the frontier → open.** Mutable. Represented as a pile
  of small per-block fragments that haven't been compacted yet.

Three structures implement this split — the **primary directory**, the **bitmap
index**, and the **in-memory open-index state** — and once you see it's the same
split each time, a large amount of the codebase collapses into one pattern.

The deep reason it's safe is an **asymmetry**: getting the boundary slightly
wrong toward "open" only costs extra fetches; getting it wrong toward "sealed"
would silently drop data. So every ambiguity resolves toward open, and every
genuinely-sealed-but-missing artifact is a *loud* error, never a quiet fallback.

## Where the frontier comes from

`family_frontier_id` (`src/engine/query/family_runner.rs:362`) loads the
`BlockRecord` at the **published head** and returns that family's
`next_primary_id_exclusive()` — `first_primary_id + count`
(`src/primitives/state.rs:251`). Two subtleties worth internalizing:

- It's the window's **exclusive end**, the running id cursor — so even a block
  that emitted *zero* records of a family still carries a meaningful frontier
  (the cursor is defined, just unchanged).
- It's derived from the **published head, not the query's block range**
  (`family_runner.rs:133-138`). If you computed it from the query range, a bucket
  that's globally sealed but happens to sit *above* your range would get
  misclassified as the open bucket. The sealed/open classification has to be a
  global property so every query agrees on it. No published head (nothing
  published) → frontier is `PrimaryId::ZERO`, so *everything* routes to the open
  path.

From that one id, each structure derives its own boundary:

```
frontier_id
   ├─ bucket boundary :  sealed_below   = bucket_start(frontier_id)   // directory
   └─ shard           :  frontier_shard = frontier_id.shard()         // bitmap
```

## The PrimaryId coordinate system

A `PrimaryId` is a `u64` split into `(shard, local)` with `LOCAL_ID_BITS = 24`
(`src/primitives/state.rs:31`):

```
   63                    24 23            0
  +------------------------+--------------+
  |        shard           |    local     |     local() = id & (2^24 - 1)
  +------------------------+--------------+     shard() = id >> 24
```

So a shard spans `2^24 = 16,777,216` local ids. Everything below keys off this.

## Split #1 — the primary directory (granularity: 10k buckets)

The directory answers `id → (block_number, idx_in_block)`. It's chopped into
**10,000-id buckets** (`DIRECTORY_BUCKET_SIZE`, `src/engine/primary_dir.rs:28`);
`bucket_start(id) = id - (id % 10_000)` (`primary_dir.rs:305`).

`PrimaryIdResolver::resolve` (`src/engine/query/directory_resolver.rs:71`) routes
**analytically** on `sealed_below = bucket_start(frontier_id)`:

- `bucket < sealed_below` → **sealed**: one `get` of a compacted `PrimaryDirBucket`
  summary, then binary-search it (`containing_bucket_entry`,
  `directory_resolver.rs:165`).
- otherwise → the single **open** bucket straddling the frontier: scan its
  retained per-block fragments.

It never probes the summary and falls back to fragments on a miss — it knows
which world the bucket is in before it reads. The resolver is shared across the
query's concurrent page futures; its memo is a `Mutex<HashMap>` held only for the
in-memory check/insert and **never across a fetch await**
(`directory_resolver.rs:44-60`), so racing tasks just both fetch and last-write
wins harmlessly.

**The fail-loud invariant.** A bucket classified sealed whose summary is missing
is *not* a cache miss to recover from — it's a broken commit contract. The
resolver returns `SealedDirectoryBucketMissingSummary` rather than scanning
fragments (`directory_resolver.rs:84-94`). There's a test that seeds fragments
that *would* resolve and asserts the resolver refuses to use them
(`directory_resolver.rs:293`). Why so strict? Because the summary was flushed in
the *same* `WriteSession` as the data, *before* the publication CAS — see the
commit-ordering argument below.

## Split #2 — the bitmap index (granularity: 64k pages, sealed per-shard)

The bitmap inverted index stores, per `(stream, page)`, a roaring bitmap of local
ids. A stream's local space is partitioned into **64K-wide pages**
(`STREAM_PAGE_LOCAL_ID_SPAN = 64*1024`, `src/engine/bitmap.rs:35`), exactly **256
pages per shard** (`STREAM_PAGES_PER_SHARD`, `bitmap.rs:40`).

But the seal boundary here is the **shard**, not the page:

```rust
shard_sealed = shard < frontier_shard      // src/engine/query/bitmap.rs:92
```

A sealed shard carries an immutable per-stream **page-count manifest**
(`BitmapPageCounts`) — how many bits each stream has in each page. That manifest
lets a query skip a provably-empty page with **zero fetches**: in
`intersect_shard_page`, `if plan.shard_sealed && page_counts.contains(&Some(0))`
→ skip (`query/bitmap.rs:200`).

**The frontier shard's manifest is a hint, never a skip.** The frontier shard
hasn't sealed — its pages are still taking fragments — so its manifest is absent.
`build_shard_page_plan` *borrows* the last sealed shard's manifest (`shard - 1`)
purely to order clause fetches most-selective-first, and the skip is hard-gated
on `shard_sealed` so a stale borrowed count can only *mis-order* fetches, never
*drop* a page (`query/bitmap.rs:88-105`). That's the asymmetry again: a wrong
order wastes time; a wrong skip loses data, so skipping is only ever allowed
where the artifact is authoritative.

(The bitmap-index lifecycle — fragments, pages, manifests, sealing — gets its own
deep dive. Here it's just the second instance of the split.)

## Split #3 — the in-memory open-index state

`OpenIndexes` (`src/engine/open_index.rs:147`) is the in-memory mirror of exactly
the *open* (uncompacted) region: the current open directory bucket's fragments
and the current open bitmap page's fragments + which streams touched it. It's an
`RwLock` over three maps keyed by `(family, bucket)` / `(family, stream, page)` /
`(family, page)`, each holding fragments ordered by block number.

Three operations are worth knowing because they thread through ingest and
recovery:

- **`apply_delta`** advances the open state in place after staging a batch
  (`OpenIndexesDelta` = the new fragments this batch added, `open_index.rs:51`).
- **`apply_eviction`** drops keys that just sealed (`OpenIndexesEviction` = the
  buckets/pages compaction sealed, `open_index.rs:58`).
- **`projected_with_delta`** *clones* the inner state and applies a delta to the
  clone (`open_index.rs:181`). This is the trick that lets Phase B compaction
  *plan* against the post-Phase-A open state without committing it yet.

`rebuilt_for_head` (`open_index.rs:144`) stamps which published head this state
was rebuilt for — the idempotence key for recovery (covered in the primary &
standby dive).

## The transition: compaction / sealing

Open fragments become sealed summaries/pages during **Phase B** of ingest. The
*trigger* in both structures is "the frontier fully crossed the unit":

- **Directory:** `sealed_ranges` yields every 10k bucket whose
  `bucket_end <= next_primary_id` (`src/engine/ingest/directory_compaction.rs`),
  and `compact_bucket_from_fragments` folds that bucket's fragments into one
  `PrimaryDirBucket` summary (validating contiguous blocks + id chaining — fail
  loud on a gap).
- **Bitmap:** pages the frontier moved past get OR-merged into one page artifact;
  when the frontier crosses a shard's last page, that shard's per-stream manifest
  is written (`newly_sealed_shards`, `src/engine/ingest/bitmap_compaction.rs`).
  If the open-stream inventory says a page was touched but no compacted artifact
  exists, it fails loud with `SealedShardPageMissingArtifact` — because an absent
  page in a *present* manifest reads as count 0 and would be silently skipped on
  the now-sealed shard.

### Why a sealed artifact is safe to assume present

This is the load-bearing guarantee, and it's pure ordering
(`src/api.rs:764-772`). During ingest:

```
1. write ALL artifacts (fragments, bucket summaries, page blobs, manifests)
2. ... THEN advance the published head with a single CAS        <-- the reveal
```

The published head **is** the commit boundary. So if a reader sees an id below
the frontier (i.e. the head moved past it), every sealed artifact for that id —
its bucket summary, its shard's page blobs and manifest — was durably committed
*first*. Therefore a sealed-but-missing artifact can't happen under correct
operation, which is exactly why the code treats it as a loud error rather than
falling back. (Anything written *above* the head is speculative and gets ignored
or overwritten on the next attempt — the recovery story, in the primary & standby
dive.)

## The granularities do NOT nest — and that's intentional

This is the single most counterintuitive fact, so verify the arithmetic
yourself:

| Split | Unit | Local-id span | Sealed test |
|---|---|---|---|
| Directory | bucket | 10,000 | `bucket < bucket_start(frontier)` |
| Bitmap page | page | 65,536 | (seals with its shard) |
| Bitmap shard | shard | 16,777,216 | `shard < frontier_shard` |

- **Pages nest cleanly in shards:** `2^24 / 65536 = 256`, exact. A shard *is* 256
  pages.
- **Buckets nest in nothing:** `2^24 / 10_000 = 1677.72…` and
  `65536 / 10_000 = 6.55…` — neither divides. A single 10k directory bucket can
  straddle a 64K bitmap-page boundary. The code never assumes alignment between
  the two grids; each computes its own boundary from the raw frontier id. (This
  is also why one block's fragment is written into *every* 10k bucket its id
  window overlaps — `fragment_bucket_starts`, `primary_dir.rs:309`.)

The practical consequence: at any instant the directory may have **many** sealed
buckets while the bitmap's **whole current shard** is still open (no manifest
yet). The directory seals ~1677× more often than a bitmap shard manifest is
produced. So the bitmap leans on per-page short-circuit fetching *within* the
open shard, while the directory already resolves most ids from sealed summaries.
These are deliberately different precisions: small frequently-sealed buckets vs.
a coarse once-per-16.7M-ids manifest that amortizes 256 page probes.

## The mental model to keep

> One frontier id, taken from the published head, splits each family's id-space
> into sealed (immutable, summarized, skippable, must-be-present) and open
> (mutable, fragmented, scanned, hint-only). Three structures implement that
> split at three non-aligned granularities. Skips and must-be-present assertions
> only ever apply on the sealed side; the open side is always demoted to a hint
> or a scan. The whole thing is made safe by writing artifacts before the
> single-CAS head advance.

## Start-here pointers

- Frontier derivation: `src/engine/query/family_runner.rs:362` +
  `src/primitives/state.rs:251`
- Directory routing & fail-loud: `src/engine/query/directory_resolver.rs:71,84`
- Bitmap sealed test & manifest-skip rule: `src/engine/query/bitmap.rs:71,200`
- In-memory open state: `src/engine/open_index.rs:147` (`projected_with_delta:181`)
- Sealing: `src/engine/ingest/directory_compaction.rs`,
  `src/engine/ingest/bitmap_compaction.rs`
- Commit-ordering boundary: `src/api.rs:764`
