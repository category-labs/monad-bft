# Deep dive: ingest batching & the two-phase commit

> Prerequisite: read `docs/onboarding.md`. The
> `docs/deep-dive-primary-standby.md` dive covers the lease/publish machinery
> this one leans on; the `docs/deep-dive-frontier-model.md` dive covers the
> sealing that Phase B performs. This dive is the end-to-end ingest pipeline and
> *why its ordering is crash-safe*.

## The two things to nail

If you remember only two ideas from this dive:

1. **Artifacts first, head-CAS last.** Every byte of a batch is written before a
   single compare-and-swap advances the published head. A crash mid-batch leaves
   *invisible orphans*, never a half-visible block.
2. **Everything heavy is parallel; only the checksum chain is serial.** Per-block
   encoding/indexing/digesting fans out across cores. The one unavoidable serial
   dependency is folding the artifact-checksum chain.

Everything else is plumbing around those two.

## The plan / apply split

Ingest is split into a *reads-only* planning half and an *effectful* commit half:

- **`plan_ingest_blocks`** (`src/api.rs:807`) → produces an `Option<IngestPlan>`.
  Pure CPU + reads. Builds all encoded artifacts, index fragments, the checksum
  chain, and the compaction plans, and **stages** writes into in-memory buffers —
  but applies nothing durable except epoch dictionaries.
- **`apply_ingest_plan` / `_with_retry`** (`src/api.rs:1628`, `:1636`) → does the
  durable writes and the publish CAS.

`IngestPlan` (`src/api.rs:347`) carries: per-block `outcomes`, `timings`, the
combined `writes: StagedWrites` (Phase A + Phase B meta+blob ops), and a
`PublicationAdvance` — *just the target head + its checksum + the CAS key*. Note
the published row *bytes* are not in the plan: the lease/owner/session fields are
stamped only inside the publish CAS at apply time.

Why split it:

- **Pipelining.** The ingest binary runs plan and apply on separate tasks over
  bounded channels, so block N+1 plans while block N commits.
- **Plan-without-commit.** The same planning machinery is reused by
  `verify_artifact_checksums` to re-derive checksums with no lease and no writes
  (the standby path).
- **Idempotent retry.** `StagedWrites` is a plain cloneable buffer, so the apply
  half can re-apply the exact same writes on I/O failure.

## Validation & contiguity

A planned batch must be a contiguous, parent-linked extension of the published
head. `plan_ingest_blocks` enforces (`src/api.rs:863`):

- First block is block **1** on a fresh store, else `head + 1`.
- `previous.block_hash == blocks[0].parent_hash()` (chains to the published head),
  and within the batch each block's `parent_hash` matches the prior block's hash.
- If `txs` is non-empty it must align in length with `logs_by_tx` (log-only
  fixtures may pass empty `txs`).

The `planning_state` mutex caches, across calls, the running head and the
predecessor `BlockRecord` — so warm batches skip re-acquiring the lease and
re-reading the head. On a cold cache it calls `begin_write` (learning the head +
continuity) and, if continuity requires it, runs recovery before proceeding.

## Primary-id assignment

Ids are a dense, gap-free global sequence advanced block by block. For each block,
its starting `LogId/TxId/TraceId` is the previous block's window's exclusive end —
`first_primary_id + count` (`next_primary_id_exclusive`). A fresh store starts at
`ZERO`. These starts are computed *sequentially up front* (cheap integer work)
and frozen into a `plan_starts` array (`src/api.rs:917`) so the heavy per-block
build has no data dependency between blocks.

## The epoch-dictionary barrier

Row codecs are per-epoch zstd dictionaries (epoch = `block_number /
epoch_blocks`). The rule: **an epoch's dictionary must be durably published before
any block of that epoch is framed.** A contiguous batch usually touches one epoch.

Before the parallel build, `plan_ingest_blocks` calls `ensure_epoch_dicts` for
every epoch the batch spans (`src/api.rs:968`). `ensure_epoch_dict`
(`src/api.rs:1493`) is the block-until-ready primitive: fast-path if installed;
otherwise single-flight under a per-family train lock (so it trains at most once
even when the binary's background pre-train races it); train from the prior
epoch's leading blocks; and critically — **persist the dict durably *before*
installing the write codec** (`src/api.rs:1540`), so any block written under a
version always has its dict (or empty sentinel) present on the read path.

## Stage A — the parallel build + the serial fold

This is the heart of the parallel/serial boundary.

**Parallel** (`src/api.rs:1003`, rayon `par_iter` over blocks — blocks are
independent): for each block, build the log/tx/trace ingest plans (encode the
per-family blob + offsets, emit bitmap fragments, fold the *uncompressed* row
digest in the same pass that compresses), and compute the standalone
`block_content_digest`. The `BlockRecord.artifact_checksum` is left empty here.

**Serial** (`src/api.rs:1056`) — the one thing the parallel build can't carry:

```
cumulative = seed_checksum                 // = previous head's checksum, or EMPTY
for output in planned (contiguous order):
    cumulative = chain(cumulative, output.block_content_digest)
    output.staged.block_record.artifact_checksum = cumulative
```

`chain(prev, block) = blake3(prev ‖ block)` is a strict left fold: block N's
stored checksum depends on N−1's *chained* value, back to the seed. The per-block
*content* digests are independent and parallelizable; the *running chain* is
inherently sequential, order- and history-sensitive. That single 32-byte chained
value per block is what a standby re-derives to certify equality.

## The two-phase commit

### Phase A — stage all block artifacts into one write session

`stage_writes` (`src/api.rs:1174`) stages, per block, into one `WriteSession`: the
`BlockRecord` + EVM header + per-family headers; the per-family blobs; directory
fragments; bitmap fragments; and the block-hash + tx-hash index entries. Staging
populates the per-table caches as a side effect — which is what makes the Phase B
*reads* hit cache.

**`PhaseAFragmentWriteFilter`** (`src/api.rs:1115`) withholds bitmap fragments for
pages that are *not* the open frontier page. Why: pages the batch sealed get
*compacted* in Phase B (which reads existing page contents + the new fragments and
writes a consolidated page), so writing their raw fragments in Phase A would be
redundant work Phase B immediately supersedes. Only the still-open frontier page
keeps its raw fragments. (Directory fragments are always written in Phase A.)

### Phase B — compaction (needs the just-staged state)

Phase B *planning* runs **concurrently with Phase A staging**:

```rust
let (phase_a_writes, stage_b_plan) = futures::join!(phase_a_stage, stage_b_plan);
//                                                   src/api.rs:1280
```

`stage_b_plan` (`src/api.rs:1251`) is a `try_join!` of six compaction planners
(directory + bitmap × log/tx/trace). They read the **projected** open-index state
(`projected_with_delta` — the post-Phase-A open state, applied to a clone without
committing) and load existing fragments from the store, hitting the caches Phase A
just populated. The resulting bucket-seals and page-compactions are staged into a
*second* write session and merged into the combined writes.

> Precision note: "meta and blob commit concurrently" is true but happens at
> **apply** time, not here. `apply_staged_writes_timed` (`src/engine/tables.rs:541`)
> `try_join!`s the meta-store and blob-store applies. What runs concurrently
> *during planning* is Phase A staging ∥ Phase B read-planning.

## The crash-safety argument

End to end, the ordering is:

```
0. epoch dict durable                                      (before any framing)
1. apply_staged_writes(writes)   -> ALL artifacts to meta + blob stores
                                    (block records, blobs, headers, dir/bitmap
                                     fragments, compacted pages, hash indexes)
2. authority_publish(new_head, checksum)  -> single ownership-validating CAS
                                             <-- THE REVEAL happens here, once
```

Every reader resolves its query window against the published head, and every
artifact for a block lives at keys for block numbers `> published_head` until step
2 moves the head. So:

- **Artifacts are addressable the instant step 1 lands, but logically invisible**
  — no reader or standby looks above the head.
- **The head is monotone and moves in one atomic CAS** from `h` to `new_head`.
  There is no intermediate state where some-but-not-all of the batch is readable.
- **Crash before the CAS** → the written artifacts are orphans for block numbers
  above the un-advanced head. On restart the writer takes over (fresh session →
  `Reacquired` → recovery from the old head) and simply overwrites them on
  re-ingest (same block number → same keys). A standby sees `Pending` for the
  un-advanced block, never a mismatch.

**Invariant: artifacts first, head-CAS last ⇒ a crash yields invisible orphans,
never a torn batch.**

## Retry semantics

`apply_ingest_plan_with_retry` (`src/api.rs:1636`) retries **only the staged-writes
commit**, with exponential backoff (`IoRetryPolicy`: none / bounded / infinite).
It's safe to repeat because the writes are keyed by block number / primary id /
stream-page, so re-applying the identical `StagedWrites` overwrites the same keys
(note the deliberate `writes.clone()` per attempt).

**Publish is *not* in the retry loop.** After the writes commit, `authority_publish`
is called once. Lease/CAS failures (`LeaseLost` / `FencedOut`) are handled
*distinctly* from I/O errors: they clear the cached lease (so the next
`begin_write` reacquires and recovers) and propagate — they are never retried as
I/O, because a fenced/lost lease means another writer owns publication and
re-CASing would be wrong.

## The ingest binary

`src/bin/chain-data-ingest.rs` is a **4-stage pipeline of `tokio` tasks over
bounded channels** (depth `--plan-buffer`), so the stages overlap across batches:
fetch(N+2) ∥ plan(N+1) ∥ apply(N).

1. **Fetch** — ordered `buffered` stream of per-block fetches (block + receipts +
   traces in parallel), with a `Semaphore` capping active concurrency; completed
   fetches coalesce into `--batch-size` consecutive blocks.
2. **Plan** — `plan_ingest_blocks` per batch.
3. **Apply/IO** — `apply_ingest_plan_with_retry`; the only stage that writes +
   publishes, hence serialized (the channel depth is the backpressure on
   publication).
4. **Progress consumer** — accumulates totals/metrics and autotunes.

Two knobs worth knowing: **batch size** (default 1 for live-mode parity;
backfill raises it to 32–256 to amortize the WAL/commit cost), and **adaptive
fetch concurrency** (an AIMD controller resizing the fetch semaphore by observed
retry rate, only under `--autotune`). The binary also **pre-trains the next
epoch's dictionary** in the background once the current epoch passes its sampling
window, safe against the plan-path `ensure` via the single-flight train lock.

## The mental model to keep

> A batch is planned (parallel encode + serial checksum fold) into staged writes,
> then applied: all artifacts committed first, the published head advanced last by
> one CAS. Plan and apply are split so they pipeline, so planning can verify
> without committing, and so the apply can retry idempotently. The single-CAS
> reveal at the end is what makes a crash leave invisible orphans rather than a
> torn batch.

## Start-here pointers

- Plan: `src/api.rs:807` — validation `:863`, id assignment `:917`,
  epoch-dict barrier `:968`, parallel build `:1003`, serial checksum fold `:1056`
- Phase A staging: `src/api.rs:1174`; the A∥B `join!`: `:1280`
- Phase B planners: `src/api.rs:1251`
- Epoch dict persist-before-install: `src/api.rs:1540`
- Apply + publish: `src/api.rs:1636`; concurrent commit: `src/engine/tables.rs:541`
- The binary: `src/bin/chain-data-ingest.rs`
