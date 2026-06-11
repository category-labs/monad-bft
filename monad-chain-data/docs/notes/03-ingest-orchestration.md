# monad-chain-data — Ingest Orchestration Layer (source notes)

All paths relative to `/home/jhow/monad-bft/monad-chain-data/src` unless prefixed. Line refs are from branch `jhow/chain-data/feature` @ `e2d8bba7`.

## 1. Purpose & responsibilities per file

- **`ingest/mod.rs`** — Pipeline assembly and supervision. Defines the cross-track message protocol (`IngestMsg`), id-assignment types (`FamilyFrontier`, `FamilyRanges`, `AssignedBlock`), run knobs (`IngestRunConfig`), and `run_ingest` (mod.rs:204), which performs recovery, seeds the checksum chain, resolves `count`→`end`, spawns the four tasks (producer, data track, index track, publisher), and supervises them (first error aborts all; publisher gets a graceful final-publish shutdown). Module doc (mod.rs:16-34) states the core thesis: **there is no backfill/live mode branch** — the engine always follows the tip; "backfill" is just the catch-up phase, and the `SignalPolicy` cadences are the only knobs.
- **`ingest/producer.rs`** — The single ordered stage: fetch blocks from the source with bounded parallel prefetch, assign primary ids (the one cross-block sequential step), fan each block out to both tracks, and emit `BatchFlush`/`Checkpoint` signals on adaptive cadences. Carries the `TODO(fetch-retry)` (producer.rs:194-197).
- **`ingest/publisher.rs`** — `Progress` (the shared durable-frontier object: `data_durable`, `index_visible`, a `Notify`) and the publisher task that advances the reader-visible head to `min(data_durable, index_visible)`, carrying the head block's `row_chain` into `PublicationState`.
- **`ingest/source.rs`** — The 2-method `ChainDataIngestSource` trait (`get_latest_uploaded`, `fetch_finalized_block`), the engine's **only inbound dependency**. The implementor owns transport concerns including retry/backoff (source.rs:16-18).
- **`ingest/resolver.rs`** — `TablesCodecResolver`: production `CodecResolver` backed by the *same* `Arc<Tables>` the engine writes through, so per-epoch dictionary training (which reads back prior-epoch blocks) needs no second service/cache. `ensure` is single-flight via `Tables::ensure_epoch_dicts`; `prewarm` spawns a background train.
- **`ingest/recover.rs`** — Crash recovery decision + the fragment-rebuild path: resume from `max(checkpoint_block, published_head)`; if the head is ahead of (or there is no) checkpoint, rebuild `OpenState` + id frontier + seal chains + sealed-page-count accumulators from durable artifacts at the head, clamped to the head's frontier.
- **`ingest/snapshot.rs`** — `SnapshotStore`: blob-centric checkpoint persistence (payload blob keyed by generation = checkpoint block; small manifest meta row as the commit point, blake3-verified), plus the versioned binary snapshot codec for (`OpenState`, `OpenTail`, frontier, block). Includes the unsafe `seed_snapshot_at` fixture knob.
- **`ingest/data_track.rs`** — The data track: per-epoch codec resolution (epoch barrier), parallel CPU row encoding on blocking tasks drained in strict block order (where the per-block `row_chain` checksum folds), pack accumulation (`BlobPacker`), and depth-1 pipelined pack flushes that advance `data_durable`.
- **`ingest/index.rs`** — The branchless index engine: accumulate per-block stream bits/dir entries into `OpenTail`, *continuously* seal completed 64Ki-id granules (`OpenState[g] ∪ OpenTail[g]` → page/bucket artifacts + seal-chain rows + page-count manifests), flush the tail as reader-visible fragments on `BatchFlush` (advances `index_visible`), and snapshot the working set on `Checkpoint`.
- **`ingest/probe.rs`** — Zero-cost-when-off timing probe (`RUST_LOG=info,ingest::timing=trace`); cumulative ns counters per stage + a 5s delta reporter. Bottleneck diagnosis is by backpressure: `producer_send_*_blocked` high ⇒ that track is the limiter; `*_recv_blocked` high ⇒ that track is starved.
- **`ingest/rtt.rs`** — `#[cfg(test)]`-only end-to-end round-trip tests over in-memory stores (mod.rs:62-63): publish/snapshot behaviour, both recovery regimes' equivalence, clamping, row-chain/seal-chain cadence-independence, restart continuity, and perturbation sensitivity.
- **`ingest_types.rs`** — Wire-agnostic input model: `FinalizedBlock {header, logs_by_tx, txs, traces}`, `IngestTx` (caller-authoritative `sender` + hash; raw `signed_tx_bytes`), `IngestTrace` (DFS-flattened call frames with `trace_address`, `tx_status`), and a local `CallKind` mirror to avoid a `monad-archive` dependency (ingest_types.rs:42-53).
- **`config/mod.rs`** — Operator-facing store config: meta backend (Dynamo/Alternator), blob backend (S3 or Dynamo), cache budgets, reader query-runtime budgets, secret redaction (`Redacted`), and store builders shared by ingest and reader (including the Alternator batch-write-limit startup probe).
- **`config/ingest.rs`** — `ChainDataEngineConfig` (the production cadence/range/prefetch knobs + defaults), validation, and `run_configured_chain_data_engine_ingest`: backend dispatch → store assembly → `run_ingest`. Deliberately **no `start` knob** — begin is always derived from store state (config/ingest.rs:46-50).
- **`config/reader.rs`** — `ConfiguredChainDataReader`: monomorphized meta×blob backend product behind one enum (`InMemory`/`DynamoS3`/`DynamoDynamo`) with query passthroughs; not part of ingest but built from the same config types.

## 2. Key types

### Pipeline messages & channels
- `IngestMsg<C>` (mod.rs:132-139): `Block(Arc<AssignedBlock>) | BatchFlush | Checkpoint(C)`. The type parameter is the track's half of the checkpoint oneshot rendezvous — `DataMsg = IngestMsg<oneshot::Sender<()>>` (mod.rs:142), `IndexMsg = IngestMsg<oneshot::Receiver<()>>` (mod.rs:144) — so misrouting a half is a **compile error**.
- Two bounded mpsc channels of depth `track_buffer` (mod.rs:297-298), producer → data track and producer → index track. Same `Arc<AssignedBlock>` sent to both (producer.rs:122-132).
- `AssignedBlock {number, ranges: FamilyRanges, block: FinalizedBlock}` (mod.rs:123-127).
- `FamilyFrontier = PerFamily<u64>` — next-primary-id per family; `assign()` (mod.rs:103-121) mints contiguous id ranges per block (logs/txs/traces counted from the block) and advances the frontier. Owned by the producer's `Signaller`; the index track keeps its own copy advanced in `accumulate`.

### Progress / publication
- `Progress` (publisher.rs:33-44): `data_durable: AtomicU64` (highest block whose pack flush is durable), `index_visible: AtomicU64` (highest block whose flushed fragments are durable AND reader-visible), `advance: Notify` (permit-storing pulse, so a notification racing the publisher's read-publish-rewait loop is never lost; bursts coalesce). Owned as `Arc` shared by data track (sets `data_durable`), index track (sets `index_visible`), and publisher (reads min).
- `PublicationTables` (engine/tables.rs:909): single meta row `publication_state/state` holding `PublicationState {indexed_finalized_head, head_row_chain}`; `publish()` at tables.rs:958.

### Index-side state (the "open working set")
- `FamilyTail` (index.rs:141-145): current-batch delta since last `BatchFlush` — `pages: HashMap<(StreamKey, page_start), RoaringBitmap>` (page-relative bits) and `dir: Vec<(block, first_id, end_id)>`.
- `FamilyState` (index.rs:150-167): carry-over — flushed-but-unsealed `pages`, `dir: VecDeque`, `sealed_id` (open-granule start, multiple of `SEAL_SPAN`), `open_streams_seen: HashSet<StreamKey>` (per-page inventory dedup; *not* persisted in snapshots), `seal_chain: ChainDigest` (running per-family standby chain through the last sealed span), `sealed_page_counts: SealedPageCounts` (per-`(stream, group)` accumulator of sealed `(page_in_group, count)` pairs for the OPEN page group).
- `OpenTail = PerFamily<FamilyTail>`, `OpenState = PerFamily<FamilyState>` (index.rs:173-174). Owned exclusively by the `IndexEngine` on the index track (single writer).
- Spans: `PAGE_SPAN = STREAM_PAGE_ID_SPAN = 64Ki ids` (bitmap.rs:33), `BUCKET_SPAN = DIRECTORY_BUCKET_SIZE = 64Ki` (primary_dir.rs:27), compile-asserted equal (index.rs:59) → unified `SEAL_SPAN`; page groups are `2^24` ids (bitmap.rs:37).
- `ArtifactWrite` (index.rs:81-138): the seven index write kinds — `Page` (sealed bitmap artifact), `Bucket` (sealed primary-dir bucket), `PageFragment` (open-page flush delta), `DirFragment`, `OpenStreams` (chunked stream-inventory delta rows, written at flush for first-seen streams AND at seal with the full inventory), `SealChain` (chained digest row staged with its span's seal batch), `PageCounts` (per-(stream,group) manifest, written exactly once when the seal boundary leaves the group).

### Data-track state
- `PackEntry` (data_track.rs:79-91): one block's fully-encoded artifacts (`combined_blob` = log++tx++trace, three `BlockBlobHeader`s, `BlockRecord`, EVM header, `(tx_hash, TxLocation)` pairs, standalone `content_digest`).
- `BlobPacker` (data_track.rs:93-128): accumulates entries until `bytes >= target_bytes || len >= max_blocks`.
- `DataPipeline` (data_track.rs:266-278): `inflight: FuturesOrdered<JoinHandle<PackEntry>>` (parallel encodes drained in block order), `packer`, `chain_head: ChainDigest` (the running row chain), `flushes: FuturesOrdered` (depth-≤1 background pack flush).

### Recovery/snapshot types
- `Recovered::{Cold, Warm{state, tail, frontier, resume}}` (recover.rs:56-66).
- `Boot` (mod.rs:187-199): per-regime seed values — `state`, `tail`, `frontier`, `resume` (last durable block), `durable_floor` (initial `data_durable`), `begin` (first block to fetch), `chain_seed` (row-chain value as of `resume`).
- `SnapshotStore` (snapshot.rs:59) + `SnapshotManifest {generation, payload_len, digest(blake3), previous}` (snapshot.rs:67-73). Generation = checkpoint block number. Single-writer (index track) / single-reader (recover) ⇒ plain get/put, no CAS.

## 3. The pipeline end-to-end

```
producer (fetch, assign ids, emit signals) ─┬─► data track  (encode rows, pack, write blobs)  → data_durable
                                            └─► index track (accumulate, seal, flush, ckpt)   → index_visible
                        publisher: published head = min(data_durable, index_visible)
```

**Setup (`run_ingest`, mod.rs:204-356):**
1. `publisher.published_head()` (mod.rs:230) → `recover(tables, snapshots, published)` (mod.rs:241) → `Boot`. Warm: `chain_seed` = `blocks().load_record(resume).row_chain` (mod.rs:258-263, `unwrap_or(EMPTY_DIGEST)` only for the validated `unsafe_seed_begin` floor); `begin = (resume+1).max(config.start)`. Cold: everything default, `begin = config.start`, `chain_seed = EMPTY_DIGEST`.
2. `progress.set_data_durable(durable_floor)` (mod.rs:287) — seeded so a restart with nothing left to ingest can still publish the durable tail. **`index_visible` is deliberately NOT seeded** (mod.rs:283-287): it must only reflect fragments actually re-written by `batch_flush`.
3. `end` resolution: explicit `end` wins; `count` → `begin + count - 1` (mod.rs:291-295).
4. Spawn: `run_producer` (mod.rs:332), `run_data_track` (mod.rs:340), `run_index_track` (mod.rs:349) in one `JoinSet`; `run_publisher` separately (mod.rs:350) with a shutdown channel so it can always publish the final head.

**Producer (`run_producer`, producer.rs:198-239):** loop: `source.get_latest_uploaded()` → `target = min(tip, end)`; if `target >= next`, `fetch_range(next..=target)` (producer.rs:215) then loop immediately if tip not drained; only sleeps `poll_ms` once caught up to the tip (producer.rs:237 — catch-up streams the backlog without polling). Past `end`: `sig.terminate()` = final `BatchFlush` + `Checkpoint` (producer.rs:180-183, 231-234).
`fetch_range` (producer.rs:247-296): `stream::iter(start..=end).map(spawn fetch task).buffered(window)` — a `Semaphore(prefetch.concurrency)` caps *active* fetch+decode tasks; `window = buffer.max(concurrency)` is the ordered look-ahead so the engine never starves through a flush/checkpoint stall; `.buffered` (not `buffer_unordered`) preserves range order so id-assignment stays sequential. Per block: `sig.feed(block)` (assign ids via `frontier.assign`, send `Arc<AssignedBlock>` to both tracks, producer.rs:115-136) then `sig.maybe_signal()`.
**Signals (`Signaller`, producer.rs:60-184):** `BatchFlush` interval = `max(distance_to_tip / tip_lag_divisor, 1)` (producer.rs:156-161) — every block at the tip, coarser when behind ("backfill" vs "live" is *only* this smooth function). `Checkpoint` every `checkpoint_every_blocks`, wired through a fresh oneshot pair (producer.rs:146-150). Both cadences are independent and may fire together.

**Data track (`run_data_track`, data_track.rs:147-253), per message:**
- `Block`: compute epoch `version = number / epoch_blocks` (data_track.rs:191); on epoch change, `flush_barrier()` then `resolver.ensure(version)` (data_track.rs:199-211 — prior-epoch rows must be durable before dict training reads them back). Spawn `encode_pack_entry` on `spawn_blocking` (data_track.rs:217-224); while `inflight >= encode_concurrency` (= core count), `pop_encode()` and `start_flush()` if `should_flush` (data_track.rs:225-230).
- `pop_encode` (data_track.rs:334-343): awaits the *oldest* encode (block order), folds `chain_head = chain(chain_head, entry.content_digest)`, stamps `entry.record.row_chain`, pushes to packer. **The only place the row chain advances** ⇒ fold order is exact block order regardless of encode parallelism.
- `start_flush` (data_track.rs:293-310): drain any prior flush first (depth-1 pipeline), then spawn `stage_pack(tables, entries)` — one `with_writes` session per pack (data_track.rs:456-490: `stage_metadata`, `stage_block_blob`, `stage_hash_index`, tx-hash index puts), so pack durability == head durability. Completion advances `set_data_durable(last_block)` (data_track.rs:313-319).
- `BatchFlush` → `flush_barrier()` (drain encodes + flush residual pack + drain flushes; data_track.rs:324-328). `Checkpoint(done)` → `flush_barrier()` then `done.send(())` (data_track.rs:239-242).
- `maybe_prewarm` (data_track.rs:358-376): once the leading `sample_span_blocks` of the current epoch are durable, fire `resolver.prewarm(next_epoch)` once.

**Index track (`run_index_track`, index.rs:373-402), per message:**
- `Block`: `accumulate(&b)` (index.rs:267-302 — per family, expand each record via `stream_entries_for_{log,tx,trace}` into page-relative bits + one dir entry per block; advance frontier/last_block) then `seal_ready()` (index.rs:306-318) — runs **per block**, sealing every granule the frontier just completed via `seal_family` (index.rs:418-542): union `state[g] ∪ tail[g]` → `Page` artifacts + `Bucket` + per-span `SealChain` fold (`SealDigest` over the exact persisted artifact bytes, stream-id-sorted; index.rs:478-489) + complete `OpenStreams` inventory keyed by seal block (index.rs:494-504) + once-per-group `PageCounts` manifests when the boundary leaves a page group (index.rs:517-536). Writes go through `write_all` (index.rs:229-248): depth-1 pipelined `with_writes` burst (bursts commit in order; commit overlaps the next block's compute).
- `BatchFlush` → `batch_flush()` (index.rs:322-339): `flush_family` per family (index.rs:563-615 — each touched open page/dir entry written as a delta fragment AND unioned into `FamilyState`; tail drained; first-seen streams emit an `OpenStreams` delta), then `write_all` + `drain_write`, then `set_index_visible(last_block)` — unconditionally, so empty blocks / terminal flush still move the head.
- `Checkpoint(rx)` → `checkpoint()` (index.rs:344-370): `drain_write()`, await the data track's durability oneshot, then `persist_snapshot(snapshots, state, tail, last_block, frontier)`. **Does not flush fragments or advance the head** — snapshotting the tail too is what allows that (snapshot.rs:271-273).

**Publisher (`run_publisher`, publisher.rs:100-128):** publish any already-durable head before parking (warm-resume case, publisher.rs:111-113); then on each `progress.changed()` pulse, `publish_if_ahead` (publisher.rs:74-98): load `BlockRecord` at `min(data_durable, index_visible)` (missing record = invariant violation, hard error) and `publisher.publish(head, record.row_chain)`. On shutdown signal: one final publish, return.

**Supervision (mod.rs:361-391):** first pipeline error/panic captured via `capture_first` → `abort_all`; cancellations from our own aborts are swallowed (`join_to_result`, mod.rs:409-420). Pipeline drained ⇒ send publisher shutdown; outcome = pipeline result AND publisher result.

**Backfill vs live:** same code path. Backfill = `end`/`count` set (or just far behind the tip): flush interval is large (~distance/divisor), checkpoints dominate recovery; bounded runs end with `terminate()` (final flush + checkpoint). Live = caught up: flush every block (interval 1), `poll_ms` polling, checkpoints comparatively rare.

## 4. Recovery in depth

**Persisted state relevant to recovery:**
1. Per-block artifacts (row blobs + `BlockRecord` incl. `row_chain` + per-family windows) — durable through `data_durable`.
2. Reader-visible index fragments + sealed artifacts + per-span `SealChain` rows + seal-time `OpenStreams` inventories — durable through whatever the index track committed.
3. The checkpoint snapshot (full `OpenState` + `OpenTail` + frontier + block), committed via the blob+manifest scheme.
4. `PublicationState` (published head + `head_row_chain`).

**Decision (`recover`, recover.rs:72-100):** resume from `max(checkpoint_block, published_head)`:
- No checkpoint AND `head == 0` → `Cold`.
- `checkpoint >= head` → **checkpoint regime** (`Warm` from the decoded snapshot; resume = checkpoint block). Rationale: during backfill flushes are rare so the checkpoint runs ahead of the head and wins (recover.rs:18-20). Note `recover_prefers_checkpoint_when_ahead_of_head` (rtt.rs:415-430).
- `head > checkpoint` (or no checkpoint but blocks published) → **live-rebuild regime**: `rebuild_from_fragments(tables, head)`, `tail = OpenTail::default()` (**the published head is always a flush boundary**, so the rebuilt tail is empty; recover.rs:71), resume = head. Live at the tip flushes are frequent and checkpoints rare, so the durable fragments at the head encode the full `OpenState`.

**Fragment rebuild (`rebuild_from_fragments` → `rebuild_family`, recover.rs:106-237), per family, three families concurrently:**
- `bound` = head record's `next_primary_id_exclusive` — the **exclusive id cutoff**; anything at/above belongs to blocks past the published head (the index track legally flushes ahead of the head since the two frontiers advance independently; recover.rs:103-105) and is dropped.
- `sealed_id = seal_boundary(bound)` — same formula as the engine's seal path (index.rs:66-68) so the open granule can never be computed differently.
- **Seal chain**: load the persisted row for `last_sealed_span(sealed_id)`; nothing sealed ⇒ `EMPTY_DIGEST`; a sealed span with **no** row is corruption → loud failure (recover.rs:160-167).
- **Sealed-page counts** of the open group (`rebuild_sealed_page_counts`, recover.rs:255-291): for each already-sealed page in the group, enumerate streams from the seal-time inventory and read each count back from the sealed artifact itself — deterministic, identical to a checkpoint-carried accumulator. PRECONDITION (recover.rs:247-254): pages sealed by a pre-inventory engine version can't take this path (must resume from a current checkpoint or re-backfill).
- **Open page**: enumerate streams via `load_open_bitmap_streams(sealed_id)`, union each stream's fragments (`buffer_unordered(16)`, recover.rs:51), clamp page-relative bits with `bm.remove_range(offset_cutoff..)` where `offset_cutoff = bound - sealed_id` (bits are PAGE-relative — an absolute cutoff would wipe pages past page 0; regression test rtt.rs:376-412). Streams recorded in `open_streams_seen` even when their below-head bits are empty so the first post-recovery flush doesn't re-emit an inventory row (recover.rs:204-207).
- **Open dir bucket**: drop entries `first >= bound`, clamp straddlers' `end` to `bound` (recover.rs:216-223).
- Returns `(FamilyState, bound)` — `bound` becomes the family's frontier.

**Chain seeding on resume (mod.rs:252-263):** keyed on the recovery *regime*, not `resume == 0` — a Warm resume at genesis must fold block 0's record (test rtt.rs:684-713). Cold seeds `EMPTY_DIGEST` (loading a stale record would double-count the re-ingested block).

**Crash-at-each-stage analysis:**
- Crash mid-pack-flush: `data_durable` never advanced past the failed pack; on resume the resume block ≤ last durable; re-ingested blocks recompute identical content digests from the same prior chain value, so rewrites are **byte-identical/idempotent** (data_track.rs:140-146).
- Crash between data flush and index flush: head stays at the older `min`; rebuild clamps any index fragments flushed above the head (test `rebuild_clamps_fragments_above_head`, rtt.rs:433-516).
- Crash between snapshot blob write and manifest put: the manifest still points at the old, complete generation — never a torn snapshot (snapshot.rs:38-45).
- Crash between checkpoint and publish: head regressed below checkpoint; warm resume seeds `data_durable = resume` so the terminal flush re-publishes the durable tail even with nothing to ingest (test `warm_resume_past_end_publishes_durable_tail`, rtt.rs:249-283).
- Re-checkpoint of the same generation on a warm restart: skipped — rewriting the blob in place would open a digest/blob mismatch window (snapshot.rs:217-228); a *lower* generation logs a warn and never regresses the manifest.
- Crash-orphan blob at the target generation: handled by **delete-before-put** (snapshot.rs:238-245) — a shorter multi-chunk payload would otherwise concatenate stale tail chunks under `DynamoBlobStore::get_blob` → permanent digest mismatch.

**Checksum verification on resume:** snapshot payloads are blake3-verified against the manifest at load; corrupt manifest row / missing blob / length / digest mismatch / wrong codec version are all **hard errors, never a silent rebuild** (snapshot.rs:165-206, 311-316). Seal chains: recovery reads the persisted span row (corruption-checked); the row chain re-seeds from the resume block's stored record. Both chains are proven cadence/restart-independent and content-sensitive by rtt.rs:611-918.

## 5. Sources

- **Trait** (`source.rs:26-33`): `get_latest_uploaded() -> Option<u64>` (the "uploaded tip" the producer follows) + `fetch_finalized_block(n) -> FinalizedBlock`. `Clone + Send + Sync + 'static`; cloned into every spawned fetch task.
- **Production source** — `ArchiverChainDataSource` in `/home/jhow/monad-bft/monad-archive/src/bin/monad-archiver/chain_data_ingest_worker.rs:47-105`. Wraps the archiver's `BlockDataReaderErased`; one block = 3 concurrent object fetches (block, receipts, traces — traces optional, default empty) via `tokio::try_join!` (chain_data_ingest_worker.rs:55-67). `get_latest_uploaded` = `reader.get_latest(LatestKind::Uploaded)`. The underlying `BlockDataReaderArgs` backends on this branch: **Aws (S3), Triedb, MongoDb, Fs, Scylla** (monad-archive/src/cli.rs:188-194).
- **Bundle source** (compressed-block-bundle backfill: `BlockDataReaderArgs::Bundle`, `PipelinedBundleReader` with read-ahead/parallel-zstd-decode/adaptive fetch concurrency, written by `monad-archive-bundler`) — **not on this branch**; it exists on the checkpoint/backup branches (`backup/jhow-chain-data-feature-before-cleanup`, `6/9-checkpoint`: `monad-archive/src/bundle.rs`, `cli.rs` `Bundle(BundleReaderArgs)` with `fetch_concurrency`/`decode_concurrency`/`autotune` knobs, "backfill source only — the bundle index is read once at open"). The mainnet S3 bundle archive (≈40k bundles, 0..79M) is consumed via that variant on the deployment branch.
- **Test source** — `VecSource` (testkit.rs:38-68).
- **Fetch/prefetch/parallelism:** producer-side semaphore caps active fetch+decode at `Prefetch.concurrency`; `.buffered(buffer.max(concurrency))` keeps a wider in-order look-ahead (producer.rs:257-287). Each fetch is its own `tokio::spawn` so decode parallelism rides the same cap. Defaults sized for high-latency per-object sources: concurrency 2000, buffer 5000 (config/ingest.rs:106-113).
- **Fetch-retry (the open item):** `TODO(fetch-retry)` at producer.rs:194-197 — a source error aborts the entire single-writer pipeline; the desired fix is a retry/backoff policy, ideally inside the source impl so the engine stays transport-agnostic. **Partially addressed in production**: the archiver source implements 8 attempts of exponential backoff (50ms base, 5s cap) with full jitter (chain_data_ingest_worker.rs:38-104). Engine-side, any non-retried error still aborts the run (which is restart-safe, but bounces the process). Note the comment at chain_data_ingest_worker.rs:41 still says "bounce the write lease" — stale since the lease layer was removed.

## 6. Configuration

### `ChainDataEngineConfig` (config/ingest.rs:53-116) — the engine knobs
| Knob | Default | Meaning |
|---|---|---|
| `end` | `None` | Inclusive absolute stop block (bounded backfill). Restart-safe/idempotent. Absent ⇒ follow tip forever. |
| `count` | `None` | Blocks from the resume point; mutually exclusive with `end`; **not restart-idempotent** (counts from the *new* resume point) — prefer `end` (config/ingest.rs:59-61, test rtt.rs:285-301). |
| `unsafe_seed_begin` | `None` | Tests/fixtures only: seed a fresh store's snapshot at `begin-1` so ingest starts above genesis. Rejected if a snapshot exists (config/ingest.rs:240-255); breaks the gap-free invariant. Must be ≥1. |
| `pack_target_bytes` | 8 MiB | Data-track pack flush size threshold. |
| `pack_max_blocks` | 10 000 | Hard cap on blocks per pack. |
| `tip_lag_divisor` | 10 | Flush interval = `max(distance_to_tip/divisor, 1)`. Larger = fresher head; pure reader-freshness knob (OpenTail memory is bounded by seal, not flush). |
| `checkpoint_every_blocks` | 10 000 | Snapshot cadence; bounds fragment-replay on recovery. Must be > 0. |
| `track_buffer` | 2048 | Inter-track channel depth; deep enough that a multi-second index seal burst doesn't convoy the producer/data track (config/ingest.rs:108-109). |
| `poll_ms` | 50 | Tip re-poll interval once caught up (never polls mid-backlog). |
| `fetch_concurrency` | 2000 | Concurrent block fetch+decode tasks; sized for high-latency 3-GETs/block sources; semaphore only fills during catch-up. |
| `fetch_buffer` | 5000 | Ordered look-ahead of decoded blocks; memory ∝ this × block size — shrink for constrained runs. |

Validation (config/ingest.rs:118-149): `end` xor `count`; `count ≥ 1`; `unsafe_seed_begin ≥ 1` and `end ≥ unsafe_seed_begin`; every numeric knob ≥ 1.

### `ChainDataStoreConfig` (config/mod.rs:51-60)
- `meta`: `ChainDataMetaBackendConfig::Dynamo(ChainDataDynamoMetaConfig)` (only variant, behind `dynamo` feature). Key fields (config/mod.rs:304-433): `table`/`table_prefix` + `table_layout` (`single` | `per-logical-table`); `endpoint_url`/`endpoint_urls` (multi-endpoint **round-robins across Alternator nodes**); `region`/`profile`/redacted creds; `create_table`; `max_concurrency` (256) / `table_max_concurrency` (256); **`scylla_profile`** (forces single-table layout and swaps both concurrencies to `scylla_concurrency`, default 1024 — the production Scylla tuning); `batch_write_max_items` (unset ⇒ probe 100 under `scylla_profile` else 25; verified at startup by `discover_batch_write_limit` with fallback to 25 — config/mod.rs:329-333, 599-630).
- `blob`: `S3(ChainDataS3BlobConfig)` (bucket required, `endpoint_urls` for RustFS/MinIO-style endpoints, `prefix`, `force_path_style`, `max_concurrency` 64, `create_bucket`, redacted creds) or `Dynamo(ChainDataDynamoBlobConfig)` (table required, `max_concurrency` 256, `chunk_size`; reuses the meta store's client ring when co-deployed — config/mod.rs:660-690).
- `cache`: `total_mib` (mode default: **ingest = 0**, reader = 2048) + per-table MiB overrides (config/mod.rs:489-536).
- `query`: reader-only runtime budgets — irrelevant to ingest.
- `reader_only`: must be false for ingest (`validate_ingest`, config/mod.rs:155-160).
- Secrets are `Redacted` (Debug prints `[REDACTED]`; `serde(transparent)`) — config/mod.rs:38-49.

### Production knobs that mattered (per deployment context)
`scylla_profile=true` + `scylla_concurrency`, multi-endpoint Alternator round-robin, the probed batch-write limit (Alternator 100 vs DynamoDB 25), S3 `endpoint_urls`/`force_path_style` for RustFS, ingest cache pinned to 0, and `fetch_concurrency`/`fetch_buffer` for the bundle/S3 catch-up phase.

### Archiver embedding (production driver)
`monad-archiver` daemon, TOML `[chain_data_ingest] enabled=true` + `[chain_data_ingest.store]` + `[chain_data_ingest.engine]` (`ArchiverChainDataIngestConfig`, monad-archive cli.rs:155-173, gated on cargo feature `chain-data-ingest`). `main.rs:106-115` spawns `chain_data_ingest_worker(block_data_source.clone(), config)` alongside the archive workers; worker results join via `select_all` and **any worker error (incl. ingest) takes down the process** (main.rs:230-234) — restart is systemd's job. The worker validates both configs then calls `run_configured_chain_data_engine_ingest` (chain_data_ingest_worker.rs:107-124), which builds stores, shares one `Arc<Tables>` between engine and resolver (config/ingest.rs:230-236), and calls `run_ingest` with `start: 0` (warm resume overrides; config/ingest.rs:276).

## 7. Concurrency model

- **Tasks:** producer, data track, index track (one `JoinSet`), publisher (separate handle), plus optional timing reporter (`AbortOnDrop`), per-fetch spawned tasks (producer), per-encode `spawn_blocking` tasks (data track), depth-1 background pack-flush task (data track), depth-1 background write-burst task (index track), fire-and-forget prewarm task (resolver.rs:70-77).
- **Channels:** two bounded mpsc (`track_buffer`) carrying the same `Arc<AssignedBlock>`; one oneshot per checkpoint (data→index durability rendezvous); shutdown mpsc(1) to the publisher; `Notify` pulses to the publisher.
- **Backpressure:** producer blocks on whichever track channel is full (timed separately per channel for diagnosis, producer.rs:99-110); data track blocks on `pop_encode` once `inflight == encode_concurrency`; index track is naturally serialized per message with its store commit pipelined at depth 1; fetch parallelism capped by the semaphore + ordered buffer.
- **Ordering guarantees:** id assignment is the single ordered cross-block step (producer, `feed`); fetch results delivered in range order (`.buffered`); encodes drained in block order (`FuturesOrdered`) so the row chain folds in exact block order; pack flushes complete in submission order (depth-1); index write bursts commit in order (depth-1 drain-before-issue — "the seal chain and recovery invariants assume no gaps", index.rs:224-228); each track consumes its channel FIFO so both see identical Block/Flush/Checkpoint ordering.
- **Failure propagation:** closed downstream channel makes the producer return `Ok(())` and let the failing track's error surface via the JoinSet (producer.rs:112-114); spawned-task panics are normalized to `Backend` errors (mod.rs:148-150, 409-420); a background flush error surfaces at the next `start_flush`/`flush_barrier` — `data_durable` never advances past a failed pack, so nothing incorrect publishes (data_track.rs:289-292).

## 8. Invariants

1. **Published head = `min(data_durable, index_visible)`** — its `BlockRecord` is durable by construction; a missing record is a hard invariant violation (publisher.rs:72-74).
2. **Gap-free up to head**: the store has every block from genesis (or the unsafe seed floor) through the published head; begin is always derived from store state, never operator-set (config/ingest.rs:46-50).
3. **Idempotent re-ingest**: replaying blocks after a crash rewrites byte-identical records/blobs — same logical block, same prior chain value ⇒ same content digest and `row_chain` (data_track.rs:140-146); manifest `PageCounts` replays rewrite identical values (index.rs:128-137); `OpenStreams` re-emits are union-idempotent for readers (snapshot.rs:328-330).
4. **Cadence independence**: `row_chain` and per-family `seal_chain` are deterministic functions of the logical block stream — independent of flush cadence, pack boundaries, checkpoint cadence, and restarts in either regime (rtt.rs:611-918). Seal digests cover final sealed artifact bytes, never fragments.
5. **Checkpoint never outruns durability**: the snapshot persists only after the data track's flush barrier signals (the oneshot rendezvous) and after the index track's own in-flight burst drains (index.rs:344-357).
6. **Snapshot commit atomicity**: blob first, manifest put is the single commit point; load verifies length+blake3; equal/lower generations never move the manifest backwards; GC keeps current+previous.
7. **`batch_flush` advances the head without a sink barrier** because index artifact writes are inline-durable on return (mod.rs:28-30) — the one `drain_write()` before `set_index_visible` (index.rs:336-337) is the guard.
8. **Rebuild safety**: fragment rebuild only *reads* existing artifacts — it cannot corrupt published data (recover.rs:21-22); everything above the head frontier is clamped.
9. **Exactly-once manifest emission**: a `(stream, page-group)` `PageCounts` row is written exactly once, when the seal boundary leaves the group; never merged with a stored row (index.rs:128-137, tests index.rs:919-1003).
10. **Dir contiguity is checked at seal**: id gaps or out-of-order blocks in the directory chain are hard errors (`compact_dir_bucket`, index.rs:658-670); an empty sealed bucket is impossible (index.rs:678-682).

## 9. Glossary

- **Producer** — the fetch/assign/signal task; the only ordered cross-block stage.
- **Data track** — the row-persistence half: encode → pack → blob flush; owns `data_durable`.
- **Index track / index engine** — the query-index half: accumulate → seal → flush → checkpoint; owns `index_visible`.
- **Publisher** — advances the reader-visible head to `min(data_durable, index_visible)` with the head's `row_chain`.
- **Signaller** — producer-side struct holding the id frontier, both senders, and cadence counters.
- **Family** — one of Log / Tx / Trace; everything index-side is `PerFamily`.
- **Primary id / frontier** — per-family monotonically minted record id; frontier = next id.
- **Page** — 64Ki-id span of one stream's bitmap; bits stored page-relative.
- **Page group** — 2^24-id span; unit of the `PageCounts` manifest.
- **Bucket** — 64Ki-id primary-directory span (block ↔ id-range mapping); shares the seal boundary with pages.
- **Granule / `SEAL_SPAN`** — the unified page+bucket seal unit (64Ki ids).
- **Seal** — turning the open union `state[g] ∪ tail[g]` into immutable artifacts once the frontier passes a granule; runs continuously, per block.
- **Fragment / delta** — open-region partial writes emitted at `BatchFlush` (page fragments keyed by flush block; dir fragments; open-streams delta rows) that make the open tail reader-visible before sealing.
- **`OpenTail`** — current-batch delta since the last flush (per family).
- **`OpenState`** — flushed-but-unsealed carry-over plus `sealed_id`, seal chain, and the sealed-page-count accumulator.
- **BatchFlush** — the signal that drains the tail into fragments and advances `index_visible`; cadence adapts to distance-to-tip.
- **Checkpoint** — the cross-track durability rendezvous that snapshots `OpenState`+`OpenTail`; **snapshot** is the persisted artifact it produces (snapshot.rs:19-21).
- **Resume block** — last fully-durable block; engine continues at `resume + 1`.
- **Row chain** — per-block chained blake3 digest over block content; stored in each `BlockRecord`; the standby verification spine.
- **Seal chain** — per-family chained digest over sealed span artifacts, persisted per span.
- **Pack** — a batch of fully-encoded blocks written in one `with_writes` session (`BlobPacker`).
- **Codec / epoch / dict** — per-epoch (1M blocks) zstd row codecs; the dict trains on the prior epoch's leading 900k blocks; `CodecResolver` provides them; **prewarm** is the background pre-train hint.
- **Probe / `rtt`** — the timing probe (`ingest::timing` target); `rtt.rs` = round-trip tests (test-only).
- **`Prefetch`** — producer fetch parallelism (`concurrency` cap + ordered `buffer` window).
- **Uploaded tip** — `get_latest_uploaded()`; the far side of the distance measurement.

## 10. Gotchas, surprising choices, open questions

**Gotchas / surprising design choices:**
- **"Branchless" is literal**: no mode flag anywhere; backfill behaviour emerges purely from `flush_interval(distance)` and `end` being a run bound (mod.rs:32-34). Docs should not describe "backfill mode" as code.
- `index_visible` deliberately unseeded on resume while `data_durable` is seeded (mod.rs:283-287) — asymmetric on purpose; getting this wrong would publish heads covering non-durable fragments.
- The chain seed is keyed on recovery *regime*, not `resume == 0` — the genesis-warm-resume distinction (mod.rs:252-257) is subtle enough to have its own test.
- `open_streams_seen` intentionally **not** in the snapshot; restores empty and the first flush idempotently re-emits inventory rows (snapshot.rs:328-330).
- Checkpoints don't flush or advance the head at all — only because the tail rides the snapshot (snapshot.rs:271-273).
- Delete-before-put on snapshot blobs exists solely for the Dynamo multi-chunk stale-tail-chunk hazard (snapshot.rs:238-245).
- Snapshot corruption is always a loud error, never a fallback rebuild — even though a rebuild path exists (snapshot.rs:165-169). The rebuild path is only taken when *no* snapshot row exists or the head is ahead.
- Pages/buckets sharing one seal boundary is a compile-time assert (index.rs:59); the whole branchless seal path depends on it.
- Sealed-page counts come from sealed page *content* (`meta.count`), never flush cadence; the rebuild's PRECONDITION on seal-time inventory rows (recover.rs:247-254) means **stores sealed by older engine builds can't take the rebuild path** — checkpoint resume or re-backfill only.
- Bits and clamp cutoffs are page-relative; the absolute/relative confusion was a real regression (test rtt.rs:376-412).
- Background flush errors surface up to one pack late by design (data_track.rs:289-292).
- Epoch barrier: the data track fully drains and flushes before resolving a new epoch's codecs, or dict training would be nondeterministic (data_track.rs:199-203).
- The producer suppresses a redundant `maybe_signal` after `fetch_range` with a precise argument why it's a no-op (producer.rs:218-227).
- `count` is not restart-idempotent (config/ingest.rs:59-61) — operator footgun; `end` is the safe knob.
- `mem_scan`/`unsafe_seed_begin`/`is_initialized` are `#[cfg(feature = "dynamo")]`-gated because only the configured entry point reaches them.
- In the archiver, **any** worker error (including a non-retried ingest fetch failure) kills the whole daemon (main.rs:230-234).

**Open questions / known work items:**
- **fetch-retry** (producer.rs:194-197): engine-level policy still absent; mitigated only inside `ArchiverChainDataSource`. Any new source impl must bring its own retry or a blip aborts the run.
- Stale comment in the archiver retry rationale references the removed write lease (chain_data_ingest_worker.rs:42).
- Snapshot blob GC is best-effort: a crash between manifest put and the prev-prev delete, or a cadence change, leaks blobs off the manifest chain forever; a range sweep would need a blob-store list op (snapshot.rs:251-257).
- The bundle source (`PipelinedBundleReader`, bundler binary) lives only on the pre-cleanup/checkpoint branches, not `jhow/chain-data/feature` — docs need to be explicit about which branch carries the production bundle backfill path.
- `Boot.durable_floor` is 0 on Cold but `resume = start - 1`; harmless (publisher compares against `published`), but the asymmetry is undocumented beyond the field comment (mod.rs:271-279).
