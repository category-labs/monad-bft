# Deep dive: primary & standbys — write coordination, leases, recovery, verification

> Prerequisite: read `docs/onboarding.md`, and ideally
> `docs/deep-dive-frontier-model.md` (the "published head = commit boundary"
> idea recurs here). This dive covers how multiple processes coordinate who may
> write, and how a standby proves it agrees with the primary.

## The shape of the problem

Exactly one process — the **primary** — may advance the published head. Every
other process is a **standby**: it observes and verifies but never writes
anything a reader will see. There is **no consensus protocol**. Coordination
flows through just two things:

1. **One CAS-guarded row** — `PublicationState` (`src/primitives/state.rs:301`),
   written only through compare-and-swap.
2. **One external clock** — the latest observed upstream finalized block number,
   threaded in everywhere as `observed_upstream_finalized_block: Option<u64>`.

That's the whole coordination substrate. Everything below is how those two
pieces produce safe single-writer behavior with automatic takeover.

## The one row, doing two jobs

```rust
struct PublicationState {        // src/primitives/state.rs:301
    indexed_finalized_head: u64,     // JOB 1: reader-visible watermark
    head_artifact_checksum: Hash32,  // JOB 1: chained checksum at that head
    owner_id: u64,                   // JOB 2: write-coordination (the "outer ring")
    session_id: SessionId,           // JOB 2
    lease_valid_through_block: u64,  // JOB 2
}
```

Keep these two jobs separate in your head, because the code does:

- **The reader/standby half** — `indexed_finalized_head` + `head_artifact_checksum`.
  The query path reads *only* `indexed_finalized_head` (via
  `load_published_head`) and nothing else. It never looks at the lease fields.
- **The outer coordination ring** — `owner_id` / `session_id` /
  `lease_valid_through_block`. Only `authority.rs` touches these.

### owner_id vs session_id (and why a restart forces takeover)

- `owner_id` is a **stable per-node** identity — survives restarts.
- `session_id` is a **fresh per-process** identity, regenerated on every startup
  (`new_session_id` mixes pid + a startup nonce + wall-clock).

The "is this still my live session?" check requires **both** to match
(`authority.rs:334`). So a restarted primary reuses its `owner_id` but presents a
*new* `session_id`, fails the same-session test against the row its previous
incarnation wrote, and is routed down the **takeover** path → `Reacquired` →
**recovery runs**. This is deliberate: a crashed process may have left
speculative artifacts above the published head, and only the takeover path cleans
up the in-memory open-index state.

### The head-0 sentinel

`indexed_finalized_head == 0` means "nothing published." Block numbers start at
**1**, so there is never a block-0 record — which is what makes 0 an unambiguous
sentinel. A `Fresh` acquire writes the row at head 0; the plan path maps head 0 →
`None` so it never tries to load a nonexistent block-0 record (`src/api.rs:833`).

## The lease lifecycle

`src/engine/authority.rs`. The cached lease (`PublicationLease`) carries the
`CasVersion` it was last seen at — every renew/publish CASes against that version,
and a conflict is discriminated by *reloading and comparing owner/session*.

### `WriteContinuity` — the output of acquiring

`begin_write` returns one of three states (`authority.rs:47`), and
`requires_recovery()` is true for two of them:

| Continuity | Meaning | Recovery? |
|---|---|---|
| `Fresh` | created the very first publication row | yes |
| `Continuous` | same owner+session, lease still valid | **no** |
| `Reacquired` | took over an expired/foreign lease (or restarted) | yes |

### The acquisition decision tree

`acquire_publication_with_session` (`authority.rs:301`) — runs on a cold cache:

```
no row?                                   -> CAS-insert at head 0      => Fresh
same owner AND same session AND in-window -> renew (or no-op)          => Continuous
foreign owner AND lease still fresh       -> refuse: LeaseStillFresh   (stay passive)
otherwise (own expired, or foreign past bound) -> CAS-takeover         => Reacquired
```

Two things to notice:

- **Takeover preserves the head.** Both the renew and takeover branches copy
  `indexed_finalized_head` and `head_artifact_checksum` from the current row
  unchanged — **only the lease fields change** (`authority.rs:346-352`,
  `:376-383`). A successor continues the predecessor's published history and
  checksum chain; only a *publish* ever advances the head.
- **"Passive" is an error, not a state.** A standby that finds a fresh foreign
  lease gets `LeaseStillFresh` and backs off. There's no separate passive enum
  variant — refusal *is* the error return.

### Lease window math

- `lease_valid_through_block(observed) = observed + lease_blocks - 1`
  (`authority.rs:276`).
- `needs_renewal` is true when `lease_valid_through - observed <= renew_threshold`
  — i.e. renew once the clock enters the last `renew_threshold` blocks of the
  window.
- The clock is **mandatory**: `require_observed_finalized_block` maps `None` →
  `LeaseObservationUnavailable`. A writer never acquires, renews, or publishes
  without a current clock reading. Fail closed.

## The CAS mechanism and the two publish failures

`MetaStoreCas` (`src/store/meta/mod.rs`) is a versioned compare-and-swap:
`cas_put(table, key, expected: Option<CasVersion>, value)` → `Applied { new_version }`
or `Conflict { current_version }`. `expected = None` means insert-if-absent. The
publication head row is the *only* CAS row in the system; everything else is
idempotent `put`.

### "Revalidate inside the publish CAS" (D1 option A)

The lease is **not** held as a guard across the plan→apply gap. Instead,
`publish_current` (`authority.rs:530`) does it all at publish time:

```
1. renew_if_needed(lease, observed)        // extend the window if we're near expiry
2. next_state = lease.as_state();
   next_state.indexed_finalized_head = new_head;
   next_state.head_artifact_checksum = checksum;
3. cas_state(expected = lease.version, next_state)   // head advance + renewal, one CAS
```

The head advance and any lease renewal land in **one version-CAS**. No lock is
held across the (potentially long) plan/apply work.

### LeaseLost vs FencedOut

On a CAS `Conflict`, `publish_current` reloads the row to discriminate
(`authority.rs:550`):

- **`LeaseLost`** — the reloaded owner/session **differ** (a foreign takeover
  happened), or the row is gone. Ownership genuinely moved on.
- **`FencedOut { current_head }`** — **same** owner+session but the version moved.
  A same-owner concurrent publish race (e.g. two in-process writers, or a retried
  publish).

Both are treated as "invalidate the cached lease" (`should_invalidate_cached_lease`),
so on either, the cache is cleared and the **next `begin_write` reacquires from
the store** → continuity becomes `Reacquired` → recovery runs. Neither is retried
as if it were an I/O error — a fenced/lost writer must step down, not hammer the
CAS.

(There's a simpler `SingleWriter` authority for non-clustered deployments: plain
version-CAS, no lease/session, writes zeros into the lease fields. A cold start
reports `Reacquired` so it always rebuilds open indexes; a stale version maps to
`FencedOut`.)

## Recovery — rebuilding the open frontier

Recovery runs **only** on `Fresh`/`Reacquired` (`src/api.rs:846`); a `Continuous`
renewal skips it because the in-memory open indexes are already coherent with the
head this same live session published.

What it rebuilds: the **open-index state** — the in-memory mirror of the
uncompacted fragments for the current open directory bucket and bitmap page (see
the frontier-model dive). After an ownership transition the new writer's in-memory
copy is empty/stale, so `rebuild_family_open_indexes` (`src/api.rs:719`) rescans
durable fragments **as of the published head**:

- It rebuilds **only the current open bucket/page** (derived from the family
  window's `next_primary_id_exclusive` — the frontier). Sealed buckets/pages have
  durable summaries and aren't touched.
- Both scans filter fragments by `block <= published_head`.

That filter is the whole point. A dead predecessor may have written fragments for
blocks *above* the published head (it crashed after writing artifacts but before
the head-advancing CAS). Those are speculative and **never were fenced in** — the
`<= published_head` filter excludes them, and the next ingest simply overwrites
them (same block number → same keys). Idempotence on `rebuilt_for_head ==
Some(head)` keeps repeated `begin_write`s from re-scanning.

This is the standby-safety guarantee restated: **the published head is the commit
boundary; anything above it is speculative and is ignored on takeover**, so a
crash mid-batch can't corrupt a successor.

## The artifact-checksum chain — how a standby verifies agreement

`src/engine/digest.rs`. The goal: a standby ingesting the same finalized blocks
should be able to prove it *would have written byte-equivalent artifacts* —
without taking the lease and without reading a single stored artifact byte.

### What's hashed

- **Per-block content digest** (`block_content_digest`) folds: block identity
  (number, hash, parent hash), the EVM header RLP, and the three per-family
  content digests in fixed order log→tx→trace.
- **Per-family content digest** (`family_content_digest`) folds: the primary-id
  window, the dict version, a `RowDigest` over the **uncompressed** rows, and the
  bitmap fragments.
- **The chain**: `chain(prev, block) = blake3(prev ‖ block)`, seeded with
  `EMPTY_CHECKSUM` (zero). The running value is stored in **every**
  `BlockRecord.artifact_checksum` and mirrored in the head row's
  `head_artifact_checksum`. One 32-byte value certifies the whole history.

### Two determinism rules that matter

- **Uncompressed rows, on purpose** (`digest.rs:31`). The digest folds row bytes
  *before* zstd framing and excludes the compressed-frame offsets. So two nodes
  agree whenever their *logical* content agrees, even if their zstd library
  versions emit different compressed bytes. The checksum is codec-independent by
  construction.
- **Canonical ordering + length-prefixing.** Every variable-length field is
  length-prefixed (so `["ab","c"]` ≠ `["a","bc"]`), and bitmap fragments — which
  have no inherent storage order — are folded sorted by `(stream_id,
  page_start_local)`. Map/iteration order can't perturb the result.

### How verification runs

`verify_artifact_checksums` (`src/api.rs:1700`) takes a contiguous run of
`FinalizedBlock`s and:

1. Seeds the chain from the published **predecessor** record (or the empty seed
   if starting at block 1).
2. For each block: resolves the same epoch codecs, **re-runs the same ingest
   plan builders** to recompute the content digest, folds the chain, then loads
   the published `BlockRecord` and compares.

Outcomes (`VerifyOutcome`):

- **`Match { through_block }`** — every block's recomputed chain value equals the
  published `artifact_checksum`.
- **`Mismatch { block_number, published, computed }`** — first divergence. The
  standby would *not* have written the same artifacts.
- **`Pending { block_number }`** — the primary hasn't published that block yet
  (the standby is ahead). Not an error.

Crucially it never calls `begin_write`, never CASes, and reads only the 32-byte
checksums + window metadata from the published records — it recomputes digests
from the *supplied* blocks. And because the chain is seeded from the published
predecessor and folded one block at a time, it's **batch-grouping-independent**: a
standby can call it with any block grouping regardless of how the primary batched
ingest.

## The concrete primary-side sequence

Putting the two halves of `src/api.rs` together:

```
plan_ingest_blocks:                                    (begin_write side)
  cold cache?
    begin_write(observe_upstream)  -> AuthorityState{ head, continuity }
    head 0 -> None
    continuity.requires_recovery() -> ensure_open_indexes_rebuilt(head)
    load previous head BlockRecord -> cache planning state
  validate contiguity (block 1 first, else head+1 + parent_hash match)
  seed checksum chain from previous.artifact_checksum

apply_ingest_plan:                                     (publish side)
  apply_staged_writes(writes)          // ALL artifacts, with I/O retry/backoff
  authority_publish(new_head, head_checksum, observe_upstream)
      -> publish_current: renew_if_needed, then single CAS of head+checksum
      -> on LeaseLost / FencedOut: clear cached lease (next begin_write reacquires)
```

The ordering — **artifacts first, head-CAS last** — is the same commit boundary
that makes speculative artifacts safely ignorable on takeover, and the checksum
stamped into the head row at publish is exactly what a standby re-derives to prove
agreement.

## The mental model to keep

> One CAS row + one external clock = single-writer-with-takeover, no consensus.
> The row's lease fields decide *who may write* (owner+session+window); its head
> fields decide *what readers see*. Takeover preserves the head and chain;
> only publish advances them. A restart always takes over (fresh session) and
> recovers. The checksum chain — folded over *uncompressed* content — lets a
> standby certify byte-equivalence against the head without the lease or the
> stored bytes.

## Start-here pointers

- The row: `src/primitives/state.rs:301`
- Acquisition decision tree: `src/engine/authority.rs:301`
- Publish + the two failure modes: `src/engine/authority.rs:530`
- Recovery rebuild: `src/api.rs:719` (+ gating at `:846`)
- Digest + chain: `src/engine/digest.rs` (uncompressed rationale at `:31`)
- Standby verification: `src/api.rs:1700`
- The primary-side begin/publish wiring: `src/api.rs:807` and `:1636`
