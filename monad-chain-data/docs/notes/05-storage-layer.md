# monad-chain-data storage layer — source notes

Scope: `/home/jhow/monad-bft/monad-chain-data/src/store/**` plus the contract surface consumed by `engine/` and `ingest/`. All line numbers from current branch `jhow/chain-data/feature` (HEAD e2d8bba7).

---

## 1. Purpose & responsibilities per file

- **src/store/mod.rs** — module root + re-export surface. Declares `blob`, `cache`, `meta`, `session` (always), `dynamo_common` (cfg `dynamo`), `sdk` (cfg `dynamo` or `s3`). Re-exports the trait/table types, the dynamo/s3 backends behind features, and the cache/session types (mod.rs:16–39). `dynamo_common` and `sdk` are `pub(crate)` — internal plumbing, not API.

- **src/store/sdk.rs** — AWS-SDK plumbing shared by both SDK backends: `StaticCredentials` (debug-redacted), `load_sdk_config` (resolves credential/region chain once), and the lock-free `ReadStats`/`ReadGuard`/`ReadStatsWindow` counters all backends share for read-rate metrics.

- **src/store/dynamo_common.rs** — wire-format and `BatchWriteItem` machinery shared by the dynamo meta and blob backends: attribute names (`pk`/`sk`/`val`), pk encoding (`encode_pk`), batch chunk splitting (count + payload soft limit), the chunk-retry executor with progress tracking and watchdog logging, `ClientRing` (round-robin endpoint clients), `SharedDynamoConnection` (co-deployed meta+blob client sharing), and idempotent pk/sk table provisioning/validation.

- **src/store/blob/mod.rs** — the `BlobStore` trait, `BlobTableId`, `BlobWriteOp`, and the `BlobTable<B>` handle (which optionally carries a process-global read-concurrency semaphore). Provides a default `read_range` (fetch-full-then-slice).

- **src/store/blob/s3.rs** — S3-API-compatible `BlobStore` (AWS S3, MinIO, R2, Ceph RGW, RustFS). Object key = `{root_prefix}/{table}/{hex(key)}`. Server-side `Range` reads. Multi-endpoint client partitioning by key hash.

- **src/store/blob/dynamo.rs** — DynamoDB-API-compatible chunked `BlobStore` (AWS DynamoDB, DynamoDB Local, Scylla Alternator). Splits blobs into ≤350 KiB items in one partition; `read_range` is a single-partition `Query` over the covering chunk range.

- **src/store/blob/in_memory.rs** — test-only `BlobStore` fixture (`BTreeMap` behind `RwLock`), with a `get_blob_calls` counter for cache-coalescing assertions and `blob_snapshot()` for equality assertions. Explicitly "not a deployable backend" (in_memory.rs:31).

- **src/store/meta/mod.rs** — the `MetaStore` trait, `TableId`/`ScannableTableId`, `MetaWriteOp`, and thin `KvTable<M>`/`ScannableKvTable<M>` handles.

- **src/store/meta/dynamo.rs** — DynamoDB-API-compatible `MetaStore`: kv and scannable rows in `(pk Binary HASH, sk Binary RANGE, val Binary)` schema; single-table or per-logical-table layouts; the `BatchWriteItem` limit probe; read-stats snapshots.

- **src/store/meta/in_memory.rs** — test-only `MetaStore` fixture: two `BTreeMap`s (kv, scan) behind `RwLock`s; extra test hooks (`clear_key`, `kv_snapshot`, `scan_snapshot`, `len`).

- **src/store/cache/mod.rs** — read-side caching layer: byte-budgeted LRU (`WeightedLru`) + single-flight (`Shared` futures) + decode-on-miss, wrapped as `CachedKvTable` and `CachedScannableKvTable`; `CacheConfig` ratio-based budget split.

- **src/store/session/mod.rs** — `WriteSession`: a staging buffer of `MetaWriteOp`s + `BlobWriteOp`s collected by a closure and committed by `Tables::with_writes`. Purely accumulation; commit machinery lives in `engine/tables.rs`.

---

## 2. Storage traits

### `BlobStore` (blob/mod.rs:121–174)
"Unversioned object storage with range-read support." Bound: `Clone + Send + Sync + 'static`; impls must be cheaply cloneable (internal `Arc`). All methods return `Send` futures (explicit `impl Future + Send`) because callers drive them from spawned tasks and the cache layer stores `get_blob` in a cross-thread single-flight `Shared` (blob/mod.rs:133–135).

- `table(BlobTableId) -> BlobTable<Self>` — default helper (blob/mod.rs:126).
- `put_blob(table, key, value: Bytes) -> Result<()>` — unconditional overwrite-put. No CAS / conditional semantics in the trait surface.
- `get_blob(table, key) -> Result<Option<Bytes>>` — `Ok(None)` = missing.
- `delete_blob(table, key) -> Result<()>` — **idempotent no-op on missing key** (blob/mod.rs:147).
- `apply_writes(Vec<BlobWriteOp>) -> Result<()>` — batch put. **Not atomic** in any real backend; correctness relies on head publication gating visibility (see §8).
- `read_range(table, key, start, end_exclusive) -> Result<Option<Bytes>>` — default impl fetches the full blob then slices (blob/mod.rs:157–173). Contract: `start > end_exclusive` or `start > blob.len()` ⇒ `Err(Decode("invalid blob range"))`; end past EOF is **clamped**; `None` if blob absent; zero-length range on a present blob ⇒ `Some(Bytes::new())`.

`BlobWriteOp { table: BlobTableId, key: Vec<u8>, value: Bytes }` (blob/mod.rs:31–36).

`BlobTable<B>` (blob/mod.rs:54–119): holds store clone + table id + optional `io_limit: Arc<Semaphore>`. `get`/`read_range` acquire one permit (process-global read cap, blob/mod.rs:60–62); **writes and deletes are never gated** (blob/mod.rs:91–92) — ingest is never throttled by the query-side limiter. The limiter is attached in `Tables::with_all_configs` from `QueryRuntimeConfig::blob_io_concurrency` (engine/tables.rs:371–379, default 1024).

### `MetaStore` (meta/mod.rs:127–184)
"Plain key-value and scannable metadata storage." Doc contract (meta/mod.rs:129–131): **writes are idempotent and content-deterministic at the chain-data layer, so there are intentionally no versioning or compare-and-set semantics.** Same `Clone + Send + Sync + 'static` and `Send`-future requirements as `BlobStore`.

Two key shapes:
- **kv**: `(TableId, key) -> Bytes` via `get` / `put`.
- **scannable**: `(ScannableTableId, partition, clustering) -> Bytes` via `scan_get` / `scan_put`, plus `scan_keys(table, partition) -> Vec<Vec<u8>>` returning **every clustering key in the partition in unsigned-byte order** (meta/mod.rs:173–178) — the whole partition in one call (backends paginate internally).
- `apply_writes(Vec<MetaWriteOp>)` — batch of `Put`/`ScanPut` (meta/mod.rs:30–43). No deletes in `MetaWriteOp` — the meta keyspace is effectively append/overwrite-only (`InMemoryMetaStore::clear_key` is explicitly test-only: "real backends are append-only", meta/in_memory.rs:53–54).

`TableId` / `ScannableTableId` / `BlobTableId` are `&'static str` newtypes; names are opaque and **backends own the namespacing** needed to avoid collisions on shared physical resources (blob/mod.rs:39–41, meta/mod.rs:45–47).

### Error contract (src/error.rs)
Backends only ever produce `MonadChainDataError::Backend(String)` (transport/service failures) and `Decode(&'static str)` (invalid range, malformed attribute). Higher layers add `MissingData`, the commit-contract violations, etc. No typed retryable/permanent distinction at the trait boundary — retries happen *inside* the backends (batch-write executor); errors that escape are terminal for that operation.

---

## 3. Backends

### 3a. `S3BlobStore` (blob/s3.rs)
- **Object key layout (wire contract)**: `{root_prefix}/{table.as_str()}/{lowercase-hex(key)}` (s3.rs:18–22, `object_key` s3.rs:486–498). Prefix normalized by trimming slashes (s3.rs:480–482).
- **Config** (`S3BlobStoreConfig`, s3.rs:60–106): bucket, root_prefix, `endpoint_urls: Vec<String>` (empty = real AWS default resolver), region, profile, `force_path_style` (required by MinIO/Ceph; false for AWS/R2), `max_concurrency` (apply_writes PUT fan-out, clamped ≥1, default 32; config layer default 64 — config/mod.rs:269), `create_bucket` (bootstrap only), `credentials: Option<S3Credentials>`.
- **Multi-endpoint**: one SDK client per endpoint; the client for an object is chosen by **hashing the object key** (`DefaultHasher` mod N, s3.rs:221–233) — stable partitioning, not round-robin (unlike dynamo).
- **Construction** (s3.rs:159–215): `load_sdk_config` → per-endpoint clients → optional `create_bucket_if_needed` (sets `LocationConstraint` only for real AWS and region ≠ us-east-1; treats `BucketAlreadyOwnedByYou` as success, s3.rs:329–352) → **validates bucket access on every client** with concurrent `HeadBucket` (s3.rs:196–203).
- **put_blob** (s3.rs:365–408): single `PutObject` with a slow-send watchdog — warn at 30 s, then at 90 s (the SDK operation deadline fails the send by 120 s); debug log if the put took ≥10 s.
- **get_blob/read_range** (s3.rs:237–283, 457–477): `read_range` pushes the range to the server (`Range: bytes=start-(end-1)`); zero-length ranges and 416'd windows (start ≥ EOF) resolve against a `HeadObject` length to the trait contract — missing ⇒ `Ok(None)`, start ≤ len ⇒ empty read, start > len ⇒ `Decode("invalid blob range")`; end past EOF is clamped server-side. `NoSuchKey` ⇒ `Ok(None)`.
- **delete_blob**: S3 DeleteObject succeeds on missing keys → idempotent for free (s3.rs:414–427).
- **apply_writes** (s3.rs:429–455): concurrent independent PUTs (`buffer_unordered(max_concurrency)`), fail-fast via `try_collect`. **Not atomic** — partial batch leaves orphan objects only (s3.rs:25–28).
- **Read stats** (`S3ReadStatsSnapshot`, s3.rs:125–136): range GETs vs full GETs; bytes; in-flight/max-in-flight. `take_read_stats` drains the window (s3.rs:294–307).
- No size limits or chunking — S3 objects hold whole values.

### 3b. `DynamoBlobStore` (blob/dynamo.rs)
DynamoDB caps items at 400 KiB and has no server-side byte-range read, so each blob is **chunked**:
- **Layout (wire contract)** (dynamo.rs:23–30):
  - `pk = u16-be(len("blob")) ∥ "blob" ∥ u16-be(len(table)) ∥ table ∥ blob-key` (the metastore's `encode_pk` with kind `"blob"` — kinds are disjoint from `kv`/`scan`, so meta and blob can share one physical table).
  - `sk = u32-be(chunk_index)` fixed-width so byte sort = chunk order (dynamo.rs:498–500).
  - `val` = chunk bytes; `len` (Number) = total blob length, **on chunk 0 only** (dynamo.rs:71).
- **Chunk size**: default 64 KiB (`DEFAULT_CHUNK_SIZE`, dynamo.rs:75). Bounded to `1..=MAX_CHUNK_SIZE` = 350 KiB (config validation rejects out-of-range values; the store-internal clamp remains as a backstop). **`chunk_size` is a wire contract** — it sets the byte→chunk-index mapping and must never change on a table holding data (dynamo.rs:95–97). `create_table` records it in a per-table marker item that `validate_table` checks at startup (absent marker = pre-marker table, tolerated).
- **Writes**: `put_blob`/`apply_writes` build one PutRequest per chunk (empty value still writes an empty chunk 0 so the blob reads back present, dynamo.rs:291–294), split into `BatchWriteItem` chunks via shared `split_batch_write_chunks`, run via the shared retry executor. Multi-chunk blobs are independent PutItems → **not atomic; partial write leaves orphan chunks; head publication keeps torn blobs invisible** (dynamo.rs:33–34).
- **get_blob**: strongly-consistent ascending-sk `Query` of the whole partition with `LastEvaluatedKey` pagination, then concatenated (dynamo.rs:393–405).
- **read_range** (dynamo.rs:444–477): strongly-consistent point-read of chunk 0's `len` (projection only) → clamp end → `Query` of inclusive chunk span `[first,last]` → slice. If `len` claims more bytes than the chunks hold ⇒ `Decode("blob range exceeds stored chunks")` — "a torn write that head publication should have kept invisible" (dynamo.rs:469–475).
- **delete_blob** (dynamo.rs:410–442): keys-only `Query` enumerates the chunk set (not derived from `len`, so **orphan tail chunks from a shorter overwrite are removed too**), then batch deletes.
- **Connection sharing**: `with_connection(SharedDynamoConnection, …)` (dynamo.rs:189–204) lets a co-deployed deployment reuse the meta store's `ClientRing` + probed `batch_write_max_items` (live `Arc<AtomicUsize>`). The standalone `new()` path keeps the DynamoDB-safe 25 (dynamo.rs:174–183). In the configured DynamoDynamo path the blob store **always** gets the meta store's connection; its own endpoint/region/profile/credential config fields are silently ignored (warn unless they exactly match, config/mod.rs:660–686).
- No per-read stats on the blob dynamo backend (only meta dynamo and s3 report read stats; `ConfiguredChainDataReader` returns blob read stats only for the DynamoS3 variant — config/reader.rs:176).

### 3c. `InMemoryBlobStore` (blob/in_memory.rs)
`Arc<RwLock<BTreeMap<(BlobTableId, Vec<u8>), Bytes>>>`. Uses trait-default `read_range`. `get_blob_calls` counts object-level reads for cache fetch-collapsing tests. Re-exported at crate root (`lib.rs:67`) for downstream test use.

### 3d. `DynamoMetaStore` (meta/dynamo.rs)
- **Wire contract** (meta/dynamo.rs:19–23): rows are `(pk: Binary, sk: Binary, val: Binary)`.
  - kv row: `pk = encode_pk("kv", table_name, key)`, `sk = [0x00]` sentinel (`SK_SENTINEL`, dynamo.rs:66).
  - scan row: `pk = encode_pk("scan", table_name, partition)`, `sk = clustering` raw bytes.
  - `encode_pk` (dynamo_common.rs:102–112): `u16-be(len(kind)) ∥ kind ∥ u16-be(len(table)) ∥ table ∥ tail` — length prefixes make boundaries unambiguous (tests meta/dynamo.rs:666–693).
  - Binary `sk` sorts by unsigned byte order ⇒ `scan_keys` is a single-partition forward `Query`.
- **Table layouts** (`DynamoTableLayout`, dynamo.rs:69–84): `Single { table_name }` (all rows in one physical table — the Scylla profile forces this) or `PerLogicalTable { prefix }` → physical name `{prefix}-{logical}` with `_`→`-` normalization (`dynamo_safe_name`, dynamo.rs:651–653). `PerLogicalTable` provisioning/validation iterates `ALL_LOGICAL_TABLE_NAMES` (engine/tables.rs:106–137).
- **Reads**: `get`/`scan_get` are single `GetItem` with **`consistent_read(true)`** (dynamo.rs:302–337); `scan_keys` is a consistent, forward, `ProjectionExpression sk`-only `Query` following `LastEvaluatedKey` to exhaustion (dynamo.rs:592–640).
- **Writes**: `put`/`scan_put` = single unconditional `PutItem`. `apply_writes` = build PutRequests with estimated wire bytes → `split_batch_write_chunks` → `run_batch_write_chunks` with global `batch_max_concurrency` and per-physical-table `batch_table_max_concurrency`; warns when the **payload soft limit** forced extra chunks (dynamo.rs:496–590).
- **Batch-write limits** (dynamo_common.rs:47–57): DynamoDB caps 25 requests/call (`BATCH_WRITE_LIMIT`); payload soft limit 12 MiB (`BATCH_WRITE_PAYLOAD_SOFT_LIMIT`, margin under DynamoDB's 16 MiB / Alternator's HTTP body cap, accounting for base64+JSON framing via `estimated_batch_write_item_bytes` with 512 B per-item overhead).
- **Alternator probe** (`discover_batch_write_limit`, dynamo.rs:387–443): Alternator defaults to 100 items/batch; the probe sends `candidate` **deletes of non-existent keys** (`__alternator_batch_probe__#{i}` pks — never exist as rows; idempotent no-op) and falls back to 25 if rejected, because an over-limit batch fails non-retryably. Stored in the `Arc<AtomicUsize>` shared with a co-deployed blob store. Driven at startup by `build_dynamo_meta_store` (config/mod.rs:617–630): candidate = `batch_write_max_items` config, else 100 under `scylla_profile`, else 25.
- **Retry executor** (`run_batch_write_chunks`/`write_batch_chunk`, dynamo_common.rs:340–535): per-chunk loop retrying both send errors and `UnprocessedItems` leftovers (resubmits only leftovers); ≤8 retries, exponential backoff base 50 ms cap 20 s with **deterministic full jitter** (FNV/xorshift over (table, chunk_idx, attempt), dynamo_common.rs:226–243 — reproducible, no RNG); slow-send watchdog warns at 10 s then 60 s; progress tracker logs started/completed/failed/retries/unprocessed/active-by-table on stream failure. Worst-case cumulative delay noted as 12.75 s (dynamo_common.rs:54).
- **ClientRing** (dynamo_common.rs:62–85): one client per endpoint, round-robined per request (atomic cursor) "to spread coordinator load (Alternator nodes are peers)". Contrast with S3's key-hash partitioning.
- **Provision/validate** (dynamo_common.rs:539–641): `create_pk_sk_table` is **tests/dev only** (never called by `new`; config layer calls it only when `create_table=true`): idempotent (treats `ResourceInUseException` as success — matched by metadata code because **Alternator may report it untyped**, dynamo_common.rs:569–570), binary pk HASH + binary sk RANGE, `PayPerRequest` billing, describe-and-sleep poll (60×250 ms) for ACTIVE. `validate_pk_sk_table` is the startup connectivity/schema check (exists, ACTIVE, exact key schema); run for every physical table with bounded concurrency (dynamo.rs:373–385).
- **Read stats** (`DynamoMetaReadStatsSnapshot`, dynamo.rs:150–162): GetItem vs Query, plus item and byte counts.

### 3e. `InMemoryMetaStore` (meta/in_memory.rs)
Two `Arc<RwLock<BTreeMap>>`s. `scan_keys` seeks to `(table, partition, vec![])` and `take_while`s the partition (in_memory.rs:160–167) — clustering keys in byte order, matching the trait. Lock poisoning maps to `Backend("poisoned lock")`. Test hooks: `clear_key`, `kv_snapshot`/`scan_snapshot`, `len`/`is_empty`.

---

## 4. sdk.rs & dynamo_common.rs — clients, auth, endpoints

- **`StaticCredentials`** (sdk.rs:32–54): `{access_key_id, secret_access_key, session_token: Option}`; `Debug` redacts secrets. Aliased as `S3Credentials` (blob/s3.rs:57) and `DynamoCredentials` (meta/dynamo.rs:59). DynamoDB Local / Alternator accept any non-empty pair; MinIO/Ceph **require** an explicit pair.
- **`load_sdk_config`** (sdk.rs:58–81): builds `aws_config::defaults(BehaviorVersion::latest())`, optionally overriding region, profile name, and credentials provider. Resolved **once** per store; per-endpoint overrides applied on derived service clients (s3.rs:310–324, meta/dynamo.rs:211–223, blob/dynamo.rs:164–172).
- **Credential precedence**: explicit `StaticCredentials` (from config TOML) > profile > ambient AWS chain (env vars, shared config, IMDS instance role). `credentials: None` ⇒ ambient chain — the AWS-cloud path.
- **No-static-creds policy intersection**: the crate never takes credentials from CLI args; they arrive via TOML config structs whose `access_key_id`/`secret_access_key`/`session_token` fields are the **`Redacted`** newtype (config/mod.rs:38–49) — `#[serde(transparent)]` `Option<String>` rendering `[REDACTED]` in any Debug/config dump (test config/mod.rs:739–768). Validation requires key-id/secret as a pair or neither (`validate_pair`, config/mod.rs:721–732). The master-branch policy (PRs #3134/#3135) removed *args-based* secrets in monad-archive's `ScyllaCliArgs` (that type lives in monad-archive/src/cli.rs, not this crate); monad-chain-data's config-file-based `Redacted` credentials + ambient-chain default is the aligned pattern, but the feature-branch archive CLI may still need rebase alignment.
- **`ReadStats`/`ReadGuard`** (sdk.rs:86–190): relaxed atomics; `start(kind)` bumps started/kind/in-flight and CAS-maxes `max_in_flight`; `finish(error, items, bytes)` records the outcome; **dropping an unfinished guard counts the read as canceled**. `take()` swaps everything to 0 except `in_flight` (sampled). Two generic `kinds` slots: dynamo = GetItem/Query, s3 = range/full GET.
- **`SharedDynamoConnection`** (dynamo_common.rs:90–100): `{ring, batch_write_max_items: Arc<AtomicUsize>, endpoint_urls}` — the meta store's connection state handed to a co-deployed dynamo blob store.
- **`split_batch_write_chunks`** (dynamo_common.rs:161–215): groups items by physical table, splits each table's run by max-items and the 12 MiB payload soft limit, then **interleaves chunks round-robin across tables** so concurrent execution spreads load (test dynamo_common.rs:726–747).
- **`summarize_names`** (dynamo_common.rs:248–261): `name:count,...` top-N formatter for log fields.

---

## 5. cache/ — what's cached and how

**Architecture**: caches sit *above* the `MetaStore` and hold **decoded values** (`Weighted<V>`), not raw bytes; raw blob bytes are never cached (decoded-row caches above the decode layer absorb repeats — engine/tables.rs:255–256).

- **`Weighted<T>`** (cache/mod.rs:44–51): decoded value + weight stamped from the **pre-decode byte length**. Weigher adds `CACHE_ENTRY_OVERHEAD` = 64 B per entry (cache/mod.rs:52–58). Only present values are stored — **absence is never cached** (a miss always re-probes the backend; a key written after a miss is visible to the next read).
- **`WeightedLru`** (cache/mod.rs:180–196): `lru::LruCache` **unbounded by count** (eviction purely by byte budget) + running `size` under one mutex; evicts LRU until `size <= budget` (replace refunds old weight; loop terminates even for a single oversized value, cache/mod.rs:259–280). **Budget 0 disables the cache entirely**.
- **Single-flight** (`CachedInner::get_or_fetch`, cache/mod.rs:318–369): separate `in_flight: Mutex<HashMap<K, Shared<BoxFuture>>>`; first miss (leader) inserts the shared fetch and owns cleanup via an `InFlightGuard` drop guard; followers clone-and-await. **Every awaiter populates the cache on Ok(Some)** because the leader can be cancelled mid-await (test cache/mod.rs:753–785); misses and errors (`Arc`-wrapped) are **never cached**. Neither mutex is ever held across an `.await` (crate-wide invariant, cache/mod.rs:179–180, 204–205). Coalesced followers still count as misses, so single-flight wins show as fewer backend reads, not hit-ratio.
- **`probe`/`insert`** (cache/mod.rs:243–286): cache-only lookup that **promotes** LRU position + counts hit/miss (batch readers probe, fetch misses themselves, then insert).
- **Wrappers**: `CachedKvTable<M, V=Bytes>` (cache/mod.rs:396–442) and `CachedScannableKvTable<M, V>` (cache/mod.rs:447–508). `decode: fn(Bytes) -> Result<V>` runs once per miss **inside** the single-flight fetch. `scan_keys` always **bypasses** the cache (unbounded result sets would need invalidation on every adjacent write, cache/mod.rs:490–494). `put` on the scannable wrapper writes through, never seeding the cache.
- **Coherence story**: caches are **read-populated only**; writes never seed or invalidate (session/mod.rs:53–55: an abandoned/failed session can never leave a phantom value). Sound because the data model is write-once/content-deterministic — a key's value never changes once visible (the one mutable row, `publication_state`, is read uncached via raw `meta_store.get`, engine/tables.rs:923–932).
- **`CacheConfig`** (cache/mod.rs:98–178): ten byte budgets; `from_total_mib` splits a total via fixed /1024 ratios — row_cache 512, bitmap_by_block 256, bitmap_page_blob 128, dir_by_block 32, bitmap_page_counts 24, dir_bucket/open_bitmap_stream/block_header/block_hash_to_number 16 each, tx_hash_index 8. Default total 2048 MiB. Config layer: **ingest mode defaults to 0 (cache-less); reader mode defaults to 2048 MiB**; per-table MiB overrides available (config/mod.rs:489–536).
- **Stats**: per-wrapper `take_window_stats()` drains (hits, misses); `Tables::take_cache_window_stats` aggregates keyed by logical table name (engine/tables.rs:663–672).

---

## 6. session/ — `WriteSession`

A **write-staging buffer**, not a read snapshot. `WriteSession<'a, M, B>` (session/mod.rs:34–96) holds `&Tables` + `meta_pending: Vec<MetaWriteOp>` + `blob_pending: Vec<BlobWriteOp>`.

- Staging methods: `put(&CachedKvTable, key, value)`, `scan_put(&CachedScannableKvTable, partition, clustering, value)`, `put_blob(&BlobTable, key, value)` — they take the *cached wrappers* purely to extract `TableId`s (type-safe table routing); staging never touches the caches (session/mod.rs:53–55, 79–80).
- Lifecycle: created only by `Tables::with_writes(f)` (engine/tables.rs:645–659): fresh session → run the `for<'s>` HRTB closure returning a `SessionFuture<'s>` (session/mod.rs:31–32) → `coalesce_block_blob_writes` rewrites consecutive block-blob ops into ~512 KiB coalesced objects with physical key `b'c' ∥ first_key ∥ last_key` and patches the matching `block_metadata` headers with `(physical_key, offset)` locators (engine/tables.rs:735–855) → `try_join!(meta.apply_writes, blob.apply_writes)` — **meta and blob batches commit concurrently, with no cross-store atomicity**. No abort/rollback API; dropping the session discards staged ops.
- Atomicity unit = one `with_writes` call's two `apply_writes` batches, each itself non-atomic. Safety comes from write ordering at the next level up: ingest publishes the head (`PublicationTables::publish`, a single `PutItem` of the `publication_state` row, engine/tables.rs:958–974) **only after** the data batches it covers are durable, so readers never see references to missing data; everything below the head is immutable.

---

## 7. At-rest namespace — what an operator sees

### Logical → physical mapping
- **Meta (DynamoDB/Scylla Alternator)** — `Single` layout (the Scylla production profile): one physical table holding every row; rows distinguished by the pk prefix `(kind, logical-table)`. `PerLogicalTable` layout: 30 physical tables named `{prefix}-{logical}` with underscores → dashes. The complete logical list (engine/tables.rs:106–137): `publication_state`, `ingest_snapshot`, `block_metadata`, `block_evm_header`, `block_hash_to_number_index`, `tx_hash_index`, then per family `f ∈ {log, tx, trace}`: `f_dict_by_version`, `f_dir_by_block`, `f_dir_bucket`, `f_bitmap_by_block`, `f_bitmap_page_blob`, `f_bitmap_page_counts`, `f_open_bitmap_stream`, `f_seal_chain`.
- **Blob (S3/RustFS)**: a single bucket; objects under `{root_prefix}/{blob_table}/{hex}`. Only **two** blob tables exist: `block_blob` (engine/family.rs:23) and `ingest_snapshot` (ingest/snapshot.rs:135). Blob and meta `ingest_snapshot` namespaces are disjoint by construction (snapshot.rs:132–134).
- **Blob (DynamoDynamo)**: same logical objects as chunked items with pk kind `"blob"` in the configured blob physical table (which may be the same physical table as meta — kinds keep them disjoint).

### Keys per logical table (kv unless noted)
| Table | Key / (partition, clustering) | Value |
|---|---|---|
| `publication_state` | fixed key `b"state"` (engine/tables.rs:915) | `PublicationState { indexed_finalized_head, head_row_chain }` — the **only routinely-mutated row** |
| `ingest_snapshot` (meta) | fixed key `b"latest"` (snapshot.rs:131) | snapshot manifest (magic+version, generation, payload_len, blake3 digest, previous gen) |
| `ingest_snapshot` (blob) | `u64-be(generation)` (snapshot.rs:124–126) | checkpoint payload; manifest put is the commit point; keep current+previous |
| `block_metadata` | `u64-be(block_number)` | RLP `BlockMetadataRecord { block_record, log/tx/trace headers }` |
| `block_evm_header` | `u64-be(block_number)` | RLP EVM header (split out so hot record reads skip ~500 B) |
| `block_hash_to_number_index` | 32-byte block hash | `u64-be(block_number)` |
| `tx_hash_index` | 32-byte tx hash | encoded `TxLocation` |
| `f_dict_by_version` | `u32-be(version)` | zstd dict bytes |
| `f_dir_by_block` (scan) | partition `u64-be(bucket_start)`, clustering `u64-be(block_number)` | per-block dir fragment |
| `f_dir_bucket` | `u64-be(bucket_start)` | sealed bucket summary |
| `f_bitmap_by_block` (scan) | partition `stream_id ∥ u64-be(page_start)` | per-block bitmap fragment |
| `f_bitmap_page_blob` | stream-scoped page key | sealed compacted page artifact (lives in **meta** store, not blob store) |
| `f_bitmap_page_counts` | per-256-page-group manifest key | page→count manifest |
| `f_open_bitmap_stream` (scan) | clustering `u64-be(marker_block) ∥ u32-be(chunk_idx)` | open-stream inventory delta |
| `f_seal_chain` | `u64-be(span_start)` | chained 32-byte seal digest |
| `block_blob` (blob) | `u64-be(block_number)` → S3 object `…/block_blob/{16-hex}`; **or** coalesced `b'c' ∥ key_first ∥ key_last` → `…/block_blob/63{16-hex}{16-hex}` | concatenated per-family compressed row regions for one or several coalesced blocks |

All numeric keys are fixed-width big-endian so byte order = numeric order — required by dynamo binary-sort `scan_keys` and gives lexically ordered S3 listings.

---

## 8. Operational characteristics

- **No conditional writes on the data path.** No `ConditionExpression`, no CAS, no versioning (sole exception, at provisioning time: the blob chunk-size marker put is conditional on absence, so racing provisioners cannot mask a mismatch). Correctness rests on (a) idempotent, content-deterministic writes, and (b) the **head-publication visibility gate**: readers only chase references at or below `publication_state.indexed_finalized_head`, published strictly after the covered data is durable. Partial `apply_writes` failures leave **orphan, unreferenced data**, never torn reads. Re-running ingest overwrites orphans idempotently. (Single-writer discipline is assumed; the lease layer was removed — nothing in the store prevents two writers, see §10.)
- **Read-after-write / consistency**: the dynamo meta backend forces `consistent_read(true)` on every GetItem/Query — recovery (rebuilding `OpenState` from fragments) and the publish→read handoff depend on read-your-writes. On real DynamoDB this doubles read cost; on Scylla Alternator it's a LOCAL_QUORUM read. S3 (RustFS/MinIO) provide strong read-after-write natively; nothing relies on S3 list consistency (no ListObjects anywhere — all access by computed key).
- **Cost/access patterns**:
  - Hot reader path: meta GetItems absorbed by byte-budgeted caches; blob GETs are server-side range reads coalesced by the engine, bounded by the global `blob_io_concurrency` semaphore.
  - Ingest path: large `BatchWriteItem` bursts (interleaved across physical tables, bounded globally + per-table) and concurrent S3 PUTs; block blobs coalesced to ~512 KiB objects; ingest runs cache-less by default.
  - Zero-length blob range reads (and 416'd EOF-boundary windows) cost an S3 HeadObject.
- **Retry/timeout behavior**: dynamo batch writes retry per-chunk ≤8 times with jittered exponential backoff (50 ms→20 s) and drain `UnprocessedItems`; point reads/puts and S3 ops rely on the AWS SDK's default retry policy only. Every SDK client carries operation deadlines (attempt 30 s, operation 120 s — store/sdk.rs; hard limits), so a silently-dead connection errors instead of hanging and re-enters the retry/endpoint-failover path; the **watchdog logs** are a separate, log-only mechanism flagging slow sends below those deadlines (S3 put: 30 s then 90 s; dynamo batch: 10 s then every 60 s). An exhausted retry budget surfaces as `Backend(...)` and fails the whole `with_writes` commit.
- **Startup checks fail fast**: bucket `HeadBucket` per endpoint; dynamo `describe_table` schema validation for every physical table; the batch-limit probe; all before ingest workers write (meta/dynamo.rs:370–385, config/mod.rs:604–631).
- **Metrics**: per-window read stats (`take_read_stats`) from the dynamo meta and S3 blob backends; per-table cache hit/miss windows; consumed by the configured reader/ingest stats loops (config/reader.rs:166–176).

---

## 9. Glossary

- **Blob store** — unversioned object storage for big byte payloads (block blobs, snapshot payloads); range-readable; S3-compatible or chunked-dynamo.
- **Meta store** — small-row KV + partitioned/clustered storage for every index/metadata row.
- **Logical table** (`TableId`/`ScannableTableId`/`BlobTableId`) — `&'static str` name the engine writes against; backends map it to physical resources.
- **Scannable table** — `(partition, clustering) -> value` table supporting ordered `scan_keys` within one partition.
- **kind** — pk discriminator byte-string (`kv`, `scan`, `blob`) folded into the dynamo pk so the three keyspaces coexist in one physical table.
- **SK sentinel** — `[0x00]` sort key for kv rows.
- **chunk** — one ≤chunk_size piece of a blob in the dynamo blob backend; `sk = u32-be(index)`; chunk 0 carries the `len` attribute.
- **ClientRing** — round-robin set of per-endpoint SDK clients (Alternator coordinator spreading).
- **SharedDynamoConnection** — meta-store connection state (ring + probed batch limit + endpoints) reused by a co-deployed dynamo blob store.
- **batch-write limit probe** — startup `BatchWriteItem` of no-op deletes discovering whether the backend accepts >25 items (Alternator: 100).
- **WriteSession** — staging buffer of meta+blob ops committed in one `with_writes` (concurrent, non-atomic batches).
- **head publication** — the `publication_state` row write that makes a block range reader-visible; the visibility gate substituting for atomic batches.
- **single-flight** — coalescing concurrent cache misses on one key onto one shared backend fetch.
- **Weighted** — decoded cache value charged at its pre-decode byte length + 64 B overhead.
- **coalesced block blob** — several consecutive blocks' regions concatenated into one ~512 KiB object keyed `b'c' ∥ first ∥ last`; headers carry physical-key+offset locators.
- **read stats window** — drained-on-read counter snapshot.
- **Redacted** — `Option<String>` config newtype whose Debug prints `[REDACTED]`.
- **scylla_profile** — config switch forcing single-table layout, the Alternator batch candidate (100), and higher `scylla_concurrency` defaults.

## 10. Gotchas / surprising choices, and open questions

**Gotchas / design choices**
1. **`MetaStore` has no delete** — `MetaWriteOp` is Put/ScanPut only; the meta keyspace is grow-only (GC exists only for snapshot blobs via `BlobStore::delete_blob`).
2. **Bitmap page artifacts live in the meta store** (`f_bitmap_page_blob` is a `TableId`), not the blob store — sized for dynamo items, hot-cached, point-read.
3. **Two different multi-endpoint strategies**: dynamo round-robins per request; S3 hash-partitions by object key. Easy to conflate.
4. **DynamoDynamo co-deploy silently ignores blob-store endpoint/region/profile/credentials** (uses meta's clients); only a warn when they diverge (config/mod.rs:660–686).
5. **`chunk_size` is a frozen wire contract** per dynamo blob table; changing it on existing data silently corrupts range reads. Config validation bounds it, and a marker item written at provisioning makes startup validation reject a mismatched store (pre-marker tables have no marker and stay unchecked).
6. **`BlobTableId`/`TableId` names are wire contracts too** — baked into S3 object keys and dynamo pks; renaming a logical table orphans all existing data.
7. **Trait-default `read_range` end-clamping vs strict start**: end past EOF is clamped, start past EOF is an error — and the dynamo backend distinguishes a *torn* blob (`len` > stored chunks) as a distinct `Decode` error.
8. **Zero-length range read = HeadObject** on S3 (can't express `bytes=x-(x-1)`; must distinguish missing vs empty vs start past EOF) — also used to disambiguate a 416 (start == len clamps to empty; start > len errors).
9. **Deterministic jitter** in batch retry backoff — reproducible, seeded from (table, chunk_idx, attempt); not actually random (dynamo_common.rs:226–243).
10. **Probe pollution-free**: the Alternator limit probe uses deletes of reserved nonexistent keys, so it never writes data; but it runs on every startup.
11. **Every awaiter populates the cache** in single-flight (not just the leader) — deliberate, for leader-cancellation; relies on idempotent inserts.
12. **Coalesced-follower misses**: single-flight followers count as cache misses, so hit-ratio under-reports effectiveness; backend-read counts are the true signal (cache/mod.rs:322–323).
13. **`create_table`/`create_bucket` are explicitly tests/dev/bootstrap** — `new()` never provisions; production assumes pre-provisioned tables and validates instead.
14. **Empty blob writes a real chunk-0 item** in dynamo (so presence is observable) — `get_blob` of an empty blob returns `Some(empty)`, matching S3.
15. **Error-string-only backend errors**: no typed error codes cross the trait boundary; callers can't distinguish throttling from schema errors without parsing strings.

**Open questions**
1. **Read-path retries**: point reads (`GetItem`, S3 GET) have no crate-level retry; is the SDK default retry config sufficient for Alternator node restarts, or should reads get the same bounded-retry treatment as batch writes?
2. **Multi-writer safety**: with the lease layer removed, nothing in the store prevents two ingest processes interleaving head publications; idempotency covers identical content, but divergent writers (e.g. different dict training outcomes) could race `publication_state`. Operational discipline (one systemd ingest) is the current guard.
3. **`max_in_flight` accounting** in `ReadStats` resets on `take()` while `in_flight` is sampled — a long window straddling the reset can momentarily report `max_in_flight < in_flight`; harmless but worth a doc note.
4. **S3 multi-endpoint + `create_bucket`** only provisions via `clients[0]`; other endpoints are only `HeadBucket`-validated — correct for peers, masks a misconfigured non-peer endpoint set until first write.
5. **Per-logical-table layout on Alternator**: `scylla_profile` forces `Single`; whether `PerLogicalTable` is ever sensible on Scylla is undocumented.
6. **`BATCH_WRITE_PAYLOAD_SOFT_LIMIT` (12 MiB) vs Alternator body cap**: the margin is heuristic; the actual Alternator `max_request_size` isn't probed the way the item count is.
