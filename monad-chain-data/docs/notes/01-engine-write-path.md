# monad-chain-data engine core / write path — source notes

All paths relative to `/home/jhow/monad-bft/monad-chain-data/`. The engine module tree is `src/engine/` (`mod.rs:16-23` exports `bitmap`, `clause`, `digest`, `family`, `primary_dir`, `query` (pub(crate), read path — out of scope here), `row_codec`, `tables`). `src/lib.rs:51-54` re-exports the public engine surface: `Family`, `DictConfig`, `PublicationTables`, `QueryRuntimeConfig`, `Tables`.

## 1. Purpose & responsibilities per file

- **`src/engine/mod.rs`** — pure module manifest; no code. The read-path internals (`query`) are crate-private; everything else is public API for the ingest tracks, the query service (`src/api.rs`), and config wiring.

- **`src/engine/family.rs`** — defines the closed set of indexed record **families** (`Log`, `Tx`, `Trace`), the per-family logical table-name bundle (`FamilyTableIds`), and the `PerFamily<T>` fixed-slot container used everywhere a value exists once per family. Also declares the single shared blob table id `BLOCK_BLOB_TABLE` (`family.rs:23`).

- **`src/engine/tables.rs`** — the engine's storage façade and the biggest file (~1425 lines). Owns: `Tables<M,B>` (every cached KV table + the blob table + the per-family table sets), `DictConfig`/`DictManager` (epoch dictionary lifecycle: training, persistence, codec install), `QueryRuntimeConfig` (read-path knobs), `WriteSession` orchestration via `with_writes` (including the block-blob **coalescer**), `PublicationTables` (the published-head row), `BlockTables` (block metadata / EVM header / hash→number index, continuity validation), and `FamilyTables` (per-family dir + bitmap + dict + seal-chain tables).

- **`src/engine/row_codec.rs`** — row-level zstd codec. Each row is an independent self-delimiting zstd frame, optionally dictionary-compressed under a versioned per-family dictionary. Provides the write-side `RowCodecState`/`RowCodec`/`BlockRowCompressor`, the read-side `RowDecompressor`/`decode_row_frame`, the shared block-framing helper `encode_block_rows` (`row_codec.rs:142-175`), and the deterministic dict-training sampler `should_sample_row`.

- **`src/engine/bitmap.rs`** — the inverted-index storage layer: roaring-bitmap **pages** (64Ki ids, page-relative bits), open-page delta **fragments**, sealed **page artifacts**, per-256-page-group **page-count manifests**, and the **open-streams inventory** rows. Also page/group arithmetic, stream-id naming (`IndexKind`, `StreamKey`, `render_stream_id`), and every byte-level encode/decode for these artifacts.

- **`src/engine/clause.rs`** — the tiny query-contract module: `IndexedClause` (one AND-term: an `IndexKind` plus OR'd values), the `IndexedFilter` trait per-domain filters implement, and the `set_allows` helper. It is the *join point* between what ingest writes (stream ids) and what queries read.

- **`src/engine/digest.rs`** — standby-verification checksums (blake3). Two independent chains: the per-block **row chain** (logical block content) and per-family **seal chains** (sealed index artifacts). Defines `ChainDigest` (= `Hash32`/`B256`), `EMPTY_DIGEST`, `RowDigest`, `family_content_digest`, `block_content_digest`, `chain`, and `SealDigest`. The module doc (`digest.rs:16-49`) is the canonical statement of determinism rules.

- **`src/engine/primary_dir.rs`** — the **primary directory**: the id↔block mapping per family. Open-region per-block **fragments** (`PrimaryDirFragment`) and sealed per-64Ki-span compacted **buckets** (`PrimaryDirBucket`), plus the invariant-enforcing constructor and the bucket arithmetic.

## 2. Key types and traits

### family.rs
- **`Family`** (`family.rs:59-63`) — `Log | Tx | Trace`. Copy enum; `Family::ALL` (`:67`) fixes the canonical order (also the snapshot codec field order and the digest fold order). `table_ids()` (`:69-75`) expands `FamilyTableIds` via the `family_table_ids!` macro with prefixes `log_`/`tx_`/`trace_`. `window_in(&BlockRecord)` (`:79-85`) picks the family's id window out of a block record.
- **`FamilyTableIds`** (`family.rs:25-39`) — the 8 logical tables per family: `dict_by_version`, `dir_by_block` (scannable), `dir_bucket`, `bitmap_by_block` (scannable), `bitmap_page_blob`, `bitmap_page_counts`, `open_bitmap_stream` (scannable), `seal_chain`.
- **`PerFamily<T>`** (`family.rs:92-148`) — one slot per family with typed accessors (`get/get_mut/iter/iter_mut/map`), iteration always in `Family::ALL` order. Used for `Codecs`, `OpenTail`, `OpenState`, `FamilyFrontier`, seal-chain points, etc.

### tables.rs
- **`Tables<M: MetaStore, B: BlobStore>`** (`tables.rs:250-267`) — the engine root object. Fields: `meta_store`, `blob_store`, `blocks: BlockTables`, `tx_hash_index`, `block_blobs: BlobTable` (raw, uncached), `families: BTreeMap<Family, FamilyTables>`, `row_caches`, `dicts: DictManager`, `materialize_budget: Semaphore`, `query: QueryRuntimeConfig`. Constructed by `new` (`:347`) or `with_all_configs` (`:359-388`) — the binary/service builds one `Arc<Tables>` shared by ingest *and* queries.
- **`DictConfig`** (`tables.rs:64-97`) — epoch dictionary lifecycle: `epoch_blocks` (default 1,000,000; epoch number doubles as dict version), `sample_span_blocks` (900,000), `max_dict_size_bytes` (112 KiB), `min_training_samples` (4096). `training_range(version)` (`:91-97`) = leading `sample_span` blocks of epoch `version-1`.
- **`DictManager`** (`tables.rs:141-248`) — write-side codecs keyed `(family, version)` (version 0 plain pre-installed), read-side `DecoderDictionary` cache, per-family single-flight `train_locks`. `install_version` (`:201-222`): empty bytes = plain sentinel; non-empty also seeds the decoder cache.
- **`QueryRuntimeConfig`** (`tables.rs:274-343`) — read-path knobs (blob IO concurrency, materialize fan-out/budget/span coalescing); `sanitized()` clamps semaphore-feeding values to ≥1. Write path mostly irrelevant, but constructed with `Tables`.
- **`PublicationTables<M>`** (`tables.rs:909-975`) — the single `publication_state` row (`table "publication_state"`, key `b"state"`). `publish(new_head, head_row_chain)` (`:958-964`) writes `PublicationState`. `queryable_head` (`:947-949`) maps head 0 → `None` (0 = acquire-before-first-publish sentinel; real heads start at 1).
- **`BlockTables<M>`** (`tables.rs:977-1160`) — three tables: `block_metadata` (cached, decodes `BlockRecord`), `block_evm_header` (cold, raw RLP), `block_hash_to_number_index` (8-byte BE value). `stage_metadata` (`:1064-1087`) writes the `BlockMetadataRecord` + EVM header; `validate_continuity` (`:1127-1159`) enforces first-block==1 / head+1 / parent-hash linkage.
- **`BlockMetadataRecord`** (`tables.rs:987-1003`, private) — RLP struct `{block_record: Bytes, log_header: Bytes, tx_header: Bytes, trace_header: Bytes}`: one metadata row holds the block record *and* all three family blob headers as nested opaque byte strings (so the coalescer can rewrite headers without understanding the record, `:822-844`).
- **`FamilyTables<M>`** (`tables.rs:1163-1352`) — per-family view: a second cached decoder over the *same* `block_metadata` row that extracts just that family's `BlockBlobHeader` as `Arc` (`:1186-1197`); `dict_by_version` (deliberately uncached, `:1199-1202` comment); `dir: PrimaryDirTables`; `bitmap: BitmapTables`; `seal_chain` (cache size 0). `stage_seal_chain` (`:1330-1341`) / `load_seal_chain` (`:1324-1326`).
- **`Coalescer`** (`tables.rs:735-797`, private) — groups consecutive block-blob writes into ≤512 KiB (`BLOCK_BLOB_COALESCE_TARGET_BYTES`, `:99`) physical objects; records `(physical_key, offset)` locators so `coalesce_block_blob_writes` (`:799-845`) can rewrite the matching `BlockMetadataRecord` headers in the same op batch.
- **`scan_get_all`** (`tables.rs:710-730`) — keys-only scan + ordered concurrent point gets (`FRAGMENT_GET_CONCURRENCY = 32`, `:704`); the canonical fragment-load pattern (order preserved — compaction relies on it).
- **`ALL_LOGICAL_TABLE_NAMES`** (`tables.rs:106-137`) — the provisioning source of truth (30 names); test at `:1369-1410` pins it to the declared `TableId`s via exhaustive destructure.

### row_codec.rs
- **`RowCodecState`** (`row_codec.rs:68-111`) — immutable write-side state for one `(family, version)`: version + optional prepared `EncoderDictionary` + level. Constructors: `bootstrap()` (v0 plain), `plain(version)` (empty-dict sentinel for v≥1), `with_dictionary`. Built by `DictManager`.
- **`RowCodec`** (`row_codec.rs:115-136`) — `Arc` snapshot of the state, handed to ingest workers per batch so a mid-batch dict hot-swap cannot tear a batch. `block_compressor()` builds one `BlockRowCompressor` per block.
- **`BlockRowCompressor`** (`row_codec.rs:179-196`) — zstd `Compressor` bound to one block's dictionary, reused across rows; `compress_row_into` appends a frame to the blob buffer.
- **`RowDecompressor`** / **`decode_row_frame`** (`row_codec.rs:202-245`) — read side; sizes the output buffer from the zstd frame content size (fallback `ROW_DECODE_CAP` = 16 MiB).
- **`encode_block_rows`** (`row_codec.rs:142-175`, pub(crate)) — the shared per-family framing function (see §4 step 3).
- Constants: `DICT_VERSION_NONE = 0` (`:42`), `ROW_ZSTD_LEVEL = 3` (`:45`), `PER_BLOCK_SAMPLE_CAP = 64` (`:53`); `should_sample_row` (`:57-64`) strided per-block sampling.

### bitmap.rs
- Constants: `STREAM_PAGE_ID_SPAN = 64*1024` (`bitmap.rs:33`, the seal granule), `PAGE_GROUP_ID_SPAN = 1<<24` (`:37`, 256 pages — the manifest window only). Format versions: blob v1, page-counts v1, page-artifact v2 (`:38-44`; artifact version *must* differ from blob version — leading-byte discrimination).
- **`DecodedBitmapFragment`** (`:50-55`) — decoded blob: `min_offset/max_offset/count` (page-relative u32s) + `RoaringBitmap`.
- **`BitmapPageArtifact`** (`:57-61`) / **`BitmapPageMeta`** (`:64-69`) — sealed-page write-path type: meta + the inner encoded blob bytes. **`DecodedBitmapPage`** (`:74-78`) — the read-cache form (meta + decoded bitmap, `Arc`-shared).
- **`BitmapTables<M>`** (`:109-273`) — four tables: `fragments` (scannable: partition = `stream_id/page_start`, clustering = `flush_block` BE), `page_blobs` (sealed artifacts), `page_counts` (manifests), `open_streams` (scannable inventory deltas). Stage/load methods listed in §4.
- **`BitmapPageCounts`** (`:399-465`) — sparse `(page_start_in_group: u32, count: u32)` list, sorted, zero-count pages dropped (`from_pairs`, `:409-415`); `count_for_page` binary-searches.
- Page math (`:468-488`): `page_start(id)` = clear low 16 bits; `page_offset(id)`; `page_group_start(id)` = clear low 24 bits; `page_start_in_group(page_start)` = low 24 bits.
- **`IndexKind`** (`:501-562`) — `Addr, Topic0..3, From, To, Selector, TopLevel, HasTransfer`; `as_str()` gives the stream-id prefix; `expected_value_len` = 20/32/4/0.
- **`StreamKey`** (`:569-623`, pub(crate)) — compact `Copy` hot-path key `{kind, len: u8, value: [u8;32]}`; `render()` is byte-identical to `render_stream_id(kind, value)` (`:493-495`, format `"{kind}/{lowercase_hex}"`); `parse()` is a *strict* inverse (lowercase hex only, exact length, rejects shard-era trailing segments — test `:765-785`).
- Keying helpers: `stream_page_key` / `stream_group_key` = `"{stream_id}/" ++ u64 BE` (`:627-642`); `open_streams_delta_key(marker_block, chunk_idx)` = `u64 BE ++ u32 BE` (`:652-657`); `OPEN_STREAMS_DELTA_TARGET_BYTES = 4 MiB` (`:646`).

### clause.rs
- **`IndexedClause`** (`clause.rs:25-59`) — `{kind: IndexKind, values: Vec<Bytes>}`; `marker(kind)` for value-less flag streams; `stream_ids()` expands to `render_stream_id` strings.
- **`IndexedFilter`** trait (`clause.rs:72-84`) — `indexed_clauses()`, `matches(&Record)` (an *invariant check*, not a release-mode post-filter), `has_indexed_clause()` (false ⇒ block-scan path).
- `set_allows` (`:63-67`) — "absent filter ⇒ allow; absent value ⇒ reject".

### digest.rs
- **`ChainDigest = Hash32`** (`digest.rs:60`); **`EMPTY_DIGEST = Hash32::ZERO`** (`:64`).
- **`FieldHasher`** (`:72-95`, private) — blake3 with u64-LE length prefixes on variable-length fields; fixed-width scalars LE.
- **`RowDigest`** (`:100-118`) — folds *uncompressed* row payloads (length-prefixed ⇒ boundaries/count captured).
- **`family_content_digest(window, rows)`** (`:123-129`) — `H(first_primary_id, count, rows_digest)`. Excludes dict version, fragments, anything cadence/compression-dependent.
- **`block_content_digest(...)`** (`:134-152`) — `H(number, block_hash, parent_hash, evm_header_rlp, log_fam, tx_fam, trace_fam)`.
- **`chain(prev, content)`** (`:157-162`) — `H(prev ‖ content)`, no prefixing (fixed 32B inputs).
- **`SealDigest`** (`:176-211`) — per sealed 64Ki span: seeded with `(family tag u32, span_start u64)`; `page(stream_id, artifact_bytes)` fed in **ascending stream-id order**; `finish(bucket_bytes)` appends the directory-bucket summary. Page-counts manifest deliberately NOT hashed (`:171-174` — counts already inside the hashed artifact bytes).

### primary_dir.rs
- **`DIRECTORY_BUCKET_SIZE = STREAM_PAGE_ID_SPAN`** (`primary_dir.rs:27`) — buckets aligned to pages so both indexes share one seal frontier.
- **`PrimaryDirEntry`** (`:34-37`) — `{block_number, first_primary_id}`; id-producing blocks only.
- **`PrimaryDirBucket`** (`:39-88`) — `{entries: Vec<PrimaryDirEntry>, end_primary_id_exclusive}`. The only constructor `new` (`:54-77`) enforces: strictly increasing in *both* fields; sentinel > last entry's first id. RLP decode funnels through it.
- **`PrimaryDirFragment`** (`:90-106`) — `{block_number, first_primary_id, end_primary_id_exclusive}` per open-region block.
- **`PrimaryDirTables<M>`** (`:119-195`) — `fragments` scannable (partition = bucket_start BE, clustering = block_number BE) + `buckets` KV (key = bucket_start BE). `stage_block_fragment` (`:175-194`) writes the *same* encoded fragment under **every** bucket it overlaps (`fragment_bucket_starts`, `:201-214`).

### Caller-side types the engine contract depends on (src/primitives/records.rs)
- **`PrimaryId`** (`records.rs:23-52`) + per-family newtypes `LogId/TxId/TraceId` (`:81-83`) — global, dense, monotonically minted per family.
- **`BlockBlobHeader`** (`records.rs:89-144`) — `{offsets: Vec<u32> (row_count+1, last = blob len), dict_version, base_offset (family region start within combined blob), physical_key (empty ⇒ own object under BE block-number key), physical_base_offset}`. RLP. `abs_range(idx)` / `region_range()` compute absolute byte ranges in the physical object. *Superseded by the external-payload change*: the struct gained `encoding`/`container_rows`/`container_status`, `offsets` delimits *units* (rows in native encoding, archive containers in external), and `row_count()` is no longer always `offsets.len()-1` — see `src/external.rs` and the `ENCODING_EXTERNAL_V1` docs in `records.rs`.
- **`FamilyWindowRecord`** (`:152-162`) — `{first_primary_id, count}`.
- **`BlockRecord`** (`:164-188`) — `{block_number, block_hash, parent_hash, logs, txs, traces, row_chain}`. RLP; `traces` and `row_chain` additions were hard format breaks (doc comments `:171-177`).
- **`PublicationState`** (`:193-209`) — `{indexed_finalized_head, head_row_chain}` RLP.

### Store contract (src/store/session/mod.rs)
- **`WriteSession<'a,M,B>`** (`session/mod.rs:34-96`) — accumulates `MetaWriteOp`/`BlobWriteOp`; `put` / `scan_put` / `put_blob`. **Staging never touches read caches** (read-populate only, `:53-55`) so abandoned sessions leave no phantoms. Applied by `Tables::with_writes` (`tables.rs:645-659`): run closure → coalesce block blobs → `try_join!(meta.apply_writes, blob.apply_writes)`. Durable on return.

## 3. On-disk / at-rest artifact formats

All multi-byte integers in hand-rolled formats are **big-endian** in keys and artifact framing; digest inputs are **little-endian** (digest.rs only). Structured records are **RLP** (`alloy_rlp`, decoded with `decode_exact`).

| Artifact | Table | Key | Value layout |
|---|---|---|---|
| Block blob (rows) | blob table `block_blob` | own: `block_number u64 BE`; coalesced: `b'c' ++ first_key ++ last_key` (`tables.rs:761-766`) | concatenation `log_region ++ tx_region ++ trace_region`; each region = concatenated independent zstd frames, one per row. No header in the blob itself — all framing lives in `BlockBlobHeader` |
| Block metadata | `block_metadata` | `block_number u64 BE` | RLP `BlockMetadataRecord{block_record RLP bytes, log/tx/trace BlockBlobHeader RLP bytes}` |
| EVM header | `block_evm_header` | `block_number u64 BE` | RLP `EvmBlockHeader` |
| Hash→number | `block_hash_to_number_index` | 32-byte block hash | `u64 BE` |
| Tx hash index | `tx_hash_index` | tx hash | `TxLocation` (src/txs/hash_index.rs) |
| Dict | `{fam}_dict_by_version` | `version u32 BE` | raw zstd dict bytes; **empty value = plain-frames sentinel** |
| Bitmap fragment (open page delta) | `{fam}_bitmap_by_block` (scannable) | partition `"{stream_id}/" ++ page_start u64 BE`, clustering `flush_block u64 BE` | bitmap blob v1: `[0x01][min u32][max u32][count u32][roaring serialize]` (`bitmap.rs:276-289`); offsets page-relative |
| Sealed bitmap page | `{fam}_bitmap_page_blob` | `"{stream_id}/" ++ page_start u64 BE` | page artifact v2: `[0x02][min u32][max u32][count u32][bitmap blob v1 bytes]` (`bitmap.rs:354-362`) — i.e. meta header wrapped around an unchanged inner blob |
| Page-count manifest | `{fam}_bitmap_page_counts` | `"{stream_id}/" ++ group_start u64 BE` | `[0x01][len u32]([page_start_in_group u32][count u32])*` sorted, non-empty pages only (`bitmap.rs:428-437`) |
| Open-streams delta | `{fam}_open_bitmap_stream` (scannable) | partition `page_start u64 BE`, clustering `marker_block u64 BE ++ chunk_idx u32 BE` | `[0x01][count u32]([len u32][utf8 stream id])*` (`bitmap.rs:660-669`); chunked ≤4 MiB |
| Dir fragment | `{fam}_dir_by_block` (scannable) | partition `bucket_start u64 BE`, clustering `block_number u64 BE` | RLP `PrimaryDirFragment{block, first_id, end_id_excl}`; duplicated into every overlapped bucket partition |
| Dir bucket (sealed) | `{fam}_dir_bucket` | `bucket_start u64 BE` | RLP `PrimaryDirBucket{entries[], end_primary_id_exclusive}` |
| Seal chain | `{fam}_seal_chain` | `span_start u64 BE` | raw 32-byte chained digest |
| Publication state | `publication_state` | `b"state"` | RLP `PublicationState{head u64, head_row_chain B256}` |

Validation on decode: bitmap blob header is cross-checked against the decoded payload (`min`/`max`/`count` must match the roaring content, `bitmap.rs:330-341`) because the read path trusts the header for skip decisions; version bytes are checked everywhere; a pre-de-shard blob (old version byte) **fails loudly** (`bitmap.rs:855-866` test) since its bits were shard-local, not page-relative.

## 4. Write-path data flow (end to end)

Pipeline shape (`ingest/mod.rs:16-34`): one **producer** fans the same block stream into two tracks over mpsc channels, a **data track** (rows) and an **index track** (the "branchless index engine"); a **publisher** advances the head to the newest index flush boundary at/below `data_durable`.

**Step 0 — boot.** `run_ingest` (`ingest/mod.rs:204-392`): read published head, run recovery, seed `chain_seed` from the resume block's stored `row_chain` (`:258-263`; `EMPTY_DIGEST` on cold), seed `Progress` (`data_durable` at the durable floor, the head at the prior published head), spawn the four tasks.

**Step 1 — id assignment (producer).** `FamilyFrontier::assign` (`ingest/mod.rs:103-120`) mints contiguous per-family primary-id ranges for each `FinalizedBlock`, producing `AssignedBlock{number, ranges, block}` sent to both tracks as `IngestMsg::Block`. Cadence signals: `BatchFlush` (reader-visibility) and `Checkpoint` (snapshot rendezvous: data track signals durability via oneshot, index track awaits it — `ingest/mod.rs:132-144`).

**Step 2 — data track epoch/codec resolution.** `run_data_track` (`ingest/data_track.rs:147-253`): `version = block_number / epoch_blocks`; on an epoch boundary, `flush_barrier()` first (prior epoch's rows must be durable before training reads them back), then `resolver.ensure(version)` → `TablesCodecResolver` (`ingest/resolver.rs:61-78`) → `Tables::ensure_epoch_dicts` → `ensure_epoch_dict` (`tables.rs:521-576`): single-flight per family; adopt a published dict, else sample (`read_back_training_samples`, `tables.rs:589-641`: 256 strided blocks of epoch V−1, `should_sample_row` per block, decode rows back to raw), train via `zstd::dict::from_samples` on rayon, **persist the dict (or empty sentinel) durably BEFORE installing the codec** (`:564-574`). `maybe_prewarm` (`data_track.rs:358-376`) kicks background training once the next epoch's corpus is durable.

**Step 3 — block encode (parallel, CPU).** `encode_pack_entry` (`data_track.rs:387-451`) on `spawn_blocking`, drained strictly in block order through `FuturesOrdered`:
- Per family: `encode_block_logs/txs/traces` (src/logs/ingest.rs:60, src/txs/ingest.rs:57, src/traces/ingest.rs:36) all funnel into `encode_block_rows` (`row_codec.rs:142-175`), which per row: records the offset, RLP-encodes the row, folds the *raw* bytes into `RowDigest`, compresses into a zstd frame. Returns `(BlockBlobHeader, blob, rows_digest)`.
- Family blobs concatenated `log ++ tx ++ trace`; `base_offset` stamped per header (`data_track.rs:394-409`).
- Builds `BlockRecord` (row_chain placeholder) and the standalone `content_digest` via `family_content_digest` ×3 + `block_content_digest` (`data_track.rs:431-439`).

**Step 4 — serial chain fold + pack.** `DataPipeline::pop_encode` (`data_track.rs:334-343`) — the ONLY place `chain_head` advances: `chain_head = chain(chain_head, entry.content_digest)`; stamped into `record.row_chain`; pushed into `BlobPacker` (flush at `target_bytes` or `max_blocks`).

**Step 5 — pack flush (durable rows).** `start_flush` (`data_track.rs:293-309`), pipelined depth 1 → `stage_pack` (`data_track.rs:456-490`): one `with_writes` session staging, per block: `stage_metadata` (`tables.rs:1064`), `stage_block_blob` (`tables.rs:450`), `stage_hash_index` (`tables.rs:1108`), tx-hash index puts. Inside `with_writes` (`tables.rs:645-659`), `coalesce_block_blob_writes` (`tables.rs:799-845`) merges consecutive block blobs into ≤512 KiB objects and rewrites each affected `BlockMetadataRecord`'s three headers with `(physical_key, physical_base_offset)` (`:847-857`). On completion `set_data_durable(last_block)`.

**Step 6 — index track accumulate (per block).** `run_index_track` (`ingest/index.rs:373-402`) → `IndexEngine::accumulate` (`:267-302`) → `accumulate_family` (`:714-739`): for each record, derive `StreamKey`s (`stream_entries_for_log/tx/trace`), set bit `page_offset(id)` in `tail.pages[(stream, page_start(id))]`; push one `(block, first_id, end_id)` dir entry per family if `count > 0`. Advance `frontier`.

**Step 7 — continuous seal (per block).** `seal_ready` (`index.rs:306-318`) → `seal_family` (`:418-541`) per family. With `from = state.sealed_id`, `open = seal_boundary(frontier)` (`:66-68`); if `open > from`:
- **Pages**: every `(stream, page)` with `page + PAGE_SPAN <= frontier` → union `state[g] ∪ tail[g]` → `encode_open_bitmap` (`:546-559`) → `ArtifactWrite::Page`; `(page_in_group, count)` accumulates into `state.sealed_page_counts`.
- **Buckets**: `seal_family_dir` (`:623-637`) compacts each span `[bs, bs+SEAL_SPAN)` from the `state.dir → tail.dir` chain (`compact_dir_bucket`, `:642-685`, defensively rejecting id gaps / block disorder), then drains fully sealed entries (`:690-708`).
- **Seal chain**: per sealed span ascending, `SealDigest::new(family, span)` fed pages (stream-id sorted) then `finish(bucket_bytes)`; `state.seal_chain = chain(prev, digest)`; emits `ArtifactWrite::SealChain` *in the same burst* (`:471-489`).
- **Seal-time inventory**: the sealed page's complete stream list as `ArtifactWrite::OpenStreams` keyed by seal block (`:494-504`).
- **Manifests**: when `page_group_start(open)` advances, each completed `(stream, group)` drains the accumulator into exactly one `ArtifactWrite::PageCounts` row (`:517-536`).
- `state.sealed_id = open`; `open_streams_seen` cleared.

**Step 8 — burst commit.** `IndexEngine::write_all` (`index.rs:229-247`): drain the previous in-flight burst (depth-1 pipeline; bursts commit *in order*), then one `with_writes` over all `ArtifactWrite`s via `stage_artifact_write` (`:743-819`), which dispatches to the table `stage_*` methods so write keying matches the reader byte-for-byte.

**Step 9 — `BatchFlush` (reader visibility).** `batch_flush` (`index.rs:322-339`) → `flush_family` (`:563-615`): each tail page becomes a `PageFragment` delta (keyed by flush block) AND is unioned into carry-over `state.pages`; newly seen streams of the single open page emit a flush-marker `OpenStreams` delta; tail dir entries become `DirFragment`s and move into `state.dir`. After `drain_write()` (writes durable), `record_flush_boundary(last_block)`.

**Step 10 — `Checkpoint` (recovery only).** `IndexEngine::checkpoint` (`index.rs:344-370`): drain writes, await the data track's durability oneshot, `persist_snapshot(state, tail, last_block, frontier)`. Writes no fragments, advances no head.

**Step 11 — publish.** `run_publisher` / `publish_if_ahead` (`ingest/publisher.rs`): when the newest recorded flush boundary at/below `data_durable` advances past `published`, load that block's `BlockRecord` and `PublicationTables::publish(head, record.row_chain)` — head and `head_row_chain` land in one row.

## 5. Invariants and assumptions

- **Block continuity**: first ingested block is 1; each block extends head+1 with matching parent hash — `BlockTables::validate_continuity` (`tables.rs:1127-1159`).
- **Primary ids are dense and monotone per family**; minted only by `FamilyFrontier::assign` on the single producer. `accumulate_family` panics on id overflow (`index.rs:732`).
- **Page/bucket alignment**: `const _: () = assert!(PAGE_SPAN == BUCKET_SPAN)` (`index.rs:59`) — the branchless seal path requires one shared `SEAL_SPAN` granule and a single `sealed_id` frontier per family.
- **Page-relative bits**: bit `v` in page `P` is id `P + v` — enforced by construction in `accumulate_family` (`page_offset`), tested at `index.rs:1159-1175`.
- **Dict-before-data**: a block under version V is only written after V's dict (or empty sentinel) is durably published (`ensure_epoch_dict` persists before installing, `tables.rs:564-574`); the read side treats missing dict bytes for v≥1 as a hard error (`tables.rs:492-494`).
- **Epoch barrier determinism**: prior-epoch encodes are flushed durable before dict training reads them back (`data_track.rs:196-200`); training corpus selection is RNG-free strided (`tables.rs:588-603`), per-block sampling capped at 64 rows.
- **Chain fold order**: `row_chain` advances only on the serial `pop_encode` drain (block order regardless of encode parallelism, `data_track.rs:330-343`); seal chains fold per family in ascending span order (`index.rs:471-489`); index bursts commit in order (depth-1 pipeline, `index.rs:229-233`).
- **SealDigest page order**: callers MUST feed ascending stream ids (`digest.rs:194`; sorted at `index.rs:480`); order changes the digest (test `digest.rs:270-272`).
- **Digest cadence-independence**: fragments are never hashed; only final sealed content + the logical block stream are (digest.rs module doc). Row bytes hashed pre-compression; offsets excluded.
- **Idempotent replay**: re-ingesting a block after a crash recomputes the identical record bytes (same logical content + same prior chain value, `data_track.rs:141-146`); a recovery replay of a group-boundary seal re-emits byte-identical manifest rows (`index.rs:128-137`; test `index.rs:919-978`). `PageCounts` rows are written exactly once and never merged with a stored row.
- **Atomicity batches**: pack durability == head durability (one `with_writes` per pack, `data_track.rs:300-308`); a span's seal-chain row is staged in the SAME session as its sealed artifacts (`tables.rs:1328-1329`, `index.rs:485-489`).
- **Bucket structure**: entries strictly increasing in both block and first id; sentinel > last first id — enforced by the single `PrimaryDirBucket::new` constructor (`primary_dir.rs:54-77`) and re-checked defensively during compaction (`index.rs:658-669`); an empty sealed bucket is an error (`index.rs:678-683`).
- **Zero-count dir fragments never staged** (`primary_dir.rs:175-179` doc; `if count > 0` at `index.rs:734`); straddling blocks are filed in every overlapped bucket (`fragment_bucket_starts`).
- **Single open page per family per flush**: seal removes everything below the frontier and the open span is narrower than a page (`flush_family` debug_assert, `index.rs:571-582`).
- **Snapshot never outruns durable rows**: checkpoint's cross-track oneshot rendezvous (`ingest/mod.rs:135-139`, `index.rs:344-358`).
- **Only `batch_flush` records publishable flush boundaries** — the boot head seed is the prior published head, never the checkpoint block (whose tail index lives only in the snapshot, with no fragments), so the published head can't cover index data that isn't durable as fragments or sealed artifacts (`ingest/mod.rs` seed comment; `ingest/publisher.rs`).
- **Caches are read-populated only**; staging never seeds them (`session/mod.rs:53-55`) — no phantom values from failed sessions.
- **Hash-index lookups can lead the head**: the hash→number entry lands before publication advances (`tables.rs:1099-1101`), so `Some(n)` doesn't guarantee block n is published.

## 6. Glossary

- **Family** — one of the three indexed record domains: `Log`, `Tx`, `Trace` (transfers are a view over traces).
- **Primary id** — the globally dense, monotonically minted per-family row id; all index structures are keyed in primary-id space, not block space.
- **Family window** — a block's `(first_primary_id, count)` slice of a family's id space (`FamilyWindowRecord`).
- **Block blob** — one block's compressed rows: per-family regions of concatenated per-row zstd frames, concatenated log++tx++trace into one object (possibly coalesced with neighbors).
- **Blob header (`BlockBlobHeader`)** — the per-family frame directory for a block blob: `offsets[]` + dict version + region/physical location.
- **Dict / epoch** — a per-family zstd dictionary; version = `block_number / epoch_blocks`; v0 and the empty-dict sentinel mean plain frames.
- **Stream** — one inverted-index posting list, named `"{kind}/{hex value}"` (e.g. `addr/ab…`, `topic0/…`); spans the whole id space.
- **Index kind** — the indexed field naming a stream (`addr`, `topic0..3`, `from`, `to`, `selector`, `top_level`, `has_transfer`).
- **Page** — a 64Ki-id aligned span of one stream; the **seal granule**. Bits stored page-relative in a roaring bitmap.
- **Fragment (bitmap)** — a per-flush delta bitmap for a stream's *open* page, keyed by flush block; superseded by the sealed page artifact.
- **Page artifact** — the sealed, compacted bitmap of one stream-page (meta header + blob), written once when the page seals.
- **Page group** — 256 pages (2²⁴ ids); purely the keying/completeness window for page-count manifests.
- **Manifest (page counts)** — per `(stream, group)` row listing each non-empty sealed page's count; lets queries judge selectivity/emptiness without fetching bitmaps; written exactly once when the seal boundary leaves the group.
- **Open-streams inventory** — per-page listing of stream ids, written as union-semantics delta rows: first-seen sets at each flush and the full inventory at seal; recovery's enumeration handle.
- **Primary directory (dir)** — the per-family id↔block mapping: open-region **dir fragments** (one per id-producing block, copied into each overlapped bucket partition) and sealed **buckets**.
- **Bucket** — the compacted directory summary of one 64Ki-id span: id-producing blocks + exclusive end sentinel; aligned 1:1 with pages.
- **Seal / seal boundary / `sealed_id`** — the per-family frontier (multiple of 64Ki) below which pages and buckets are final artifacts; everything at/above is "open".
- **Open tail / open state** — the in-memory accumulators: tail = since-last-flush delta; state = flushed-but-unsealed carry-over.
- **Clause (`IndexedClause`)** — one AND-term of an indexed query: a kind + OR'd values, expanding to stream ids.
- **Row chain** — the per-block blake3 chain over logical block content (rows hashed uncompressed); stored in every `BlockRecord.row_chain` and mirrored as `head_row_chain` in `PublicationState`.
- **Seal chain** — the per-family blake3 chain over sealed span artifacts (pages sorted by stream id + bucket bytes); one row per sealed span in `{fam}_seal_chain`.
- **Publication state / head** — the single row publishing the reader-visible finalized head = the newest flush boundary covered by `data_durable`; head 0 is the not-yet-queryable sentinel.
- **Pack** — a group of consecutive encoded blocks flushed in one write session by the data track.
- **Coalescer** — the `with_writes` post-pass merging consecutive block blobs into one ≤512 KiB physical object and rewriting their headers' physical locators.
- **Checkpoint vs flush** — `BatchFlush` makes fragments reader-visible and records a publishable flush boundary; `Checkpoint` only snapshots the open working set for fast recovery (no fragments, no head movement).

## 7. Gotchas / surprising design choices

- **"Branchless" means mode-less**, not branch-free code: there is no backfill/live mode split — the engine always runs the same accumulate→seal→flush→checkpoint loop; "backfill" is just the catch-up phase and the only knobs are signal cadences (`ingest/mod.rs:30-34`, `index.rs:16-18`). This removed an entire class of mode-transition bugs (per the project history, the lease layer and shard layer went with it).
- **No writer task / no sink barrier**: index artifact writes are computed synchronously and committed inline (depth-1 pipelined); "durable on return" is what lets `batch_flush` advance the head without a barrier (`index.rs:81-86, 229-247`).
- **Two different per-block decoders over the same `block_metadata` row**: `BlockTables` decodes the `BlockRecord`; each `FamilyTables` independently caches just its `BlockBlobHeader` as `Arc` (`tables.rs:1186-1197`). One physical row, four logical cache views.
- **Header rewriting after the fact**: `BlockBlobHeader.physical_key/physical_base_offset` are stamped *by the coalescer at flush time*, not by the encoder — the metadata value is decoded, mutated, and re-encoded inside `coalesce_block_blob_writes` (`tables.rs:822-844`). Lone blobs keep an empty key that falls back to the BE block-number key.
- **Bits are page-relative; old shard-era blobs are version-rejected** (`bitmap.rs:855-866`). The de-shard was a format break; all stores re-backfill from genesis.
- **Page-counts manifests are deliberately NOT in the seal digest** (`digest.rs:171-174`): every count is already inside the hashed artifact bytes (the blob header), so hashing the manifest would add nothing — this is the resolution of the "page_counts not digested" design note.
- **SealDigest framing injectivity argument** (`digest.rs:199-206`): no page count is hashed because length-prefixed `(stream, artifact)` pairs terminated by length-prefixed bucket bytes are already unambiguous; a trailing fixed-width scalar after an unbounded run is the one aliasing pattern avoided.
- **Open-streams partitions hold two row flavors with one union semantics** (flush first-seen + seal full inventory, `bitmap.rs:230-240`), and reads are deliberately *not* head-filtered — ahead-of-head rows are "wasteful, never wrong" (`bitmap.rs:239-240`).
- **Dir fragments are duplicated** into every bucket they overlap (straddlers appear in 2+ partitions) so each bucket partition is self-sufficient for compaction (`primary_dir.rs:189-193`).
- **Buckets omit empty blocks** — encoded size is bounded by id-producing blocks, keeping sparse buckets under backend write limits (`primary_dir.rs:29-32`); the next entry (or sentinel) closes ranges across them.
- **`dict_by_version` is uncached on purpose** — `DictManager` memoizes decoders, so the table is read at most once per (family, version); a cache here could never see a second lookup to hit (`tables.rs:1198-1201`).
- **Row digests are zstd-independent**: hashed pre-compression with offsets excluded, so changing zstd level/dict/version never perturbs verification (`digest.rs:46-49`).
- **Head 0 sentinel**: `published_head() == 0` means "claimed but nothing finalized"; real heads start at 1 (`tables.rs:947-949`). Cold-start `resume` is `start - 1`.
- **Warm-resume genesis subtlety**: the chain seed is keyed on the recovery *regime*, not `resume == 0` — a warm resume at block 0 must fold block 0's record (`ingest/mod.rs:251-263`).
- **Error surfacing latency**: a background pack-flush failure surfaces at the *next* flush/barrier, not inline; safe because `data_durable` never passes a failed pack (`data_track.rs:289-293`).
- **`stage_artifact_write` exists so writer keying can't drift from reader keying** — all writes go through the same table `stage_*` methods the reader's `load_*` mirrors (`index.rs:741-744`).
- **The `ALL_LOGICAL_TABLE_NAMES` pinning test** uses exhaustive struct destructuring so adding a table id without updating provisioning is a compile error (`tables.rs:1380-1393`).

## 8. Open questions

- **Fetch-retry** on the producer/source side is noted as open work in project memory; nothing in the engine core addresses transient fetch failures (out of these files' scope, but docs should not claim retry semantics).
- **`u32` offset space per family region**: `encode_block_rows` errors when a family's blob exceeds `u32` offsets (`row_codec.rs:152-153`); whether any real block can approach 4 GiB per family is presumably impossible by protocol gas limits, but no engine-level guard documents the assumed bound.
- **`StreamKey.value` is capped at 32 bytes** (`bitmap.rs:574`); fine for all current kinds — adding a kind with longer values would silently require a layout change (debug_assert only on length, `:579-583`).
- **`encode_open_bitmap` casts `bm.len()` to `u32`** (`index.rs:550`) — safe because a page holds ≤64Ki bits, but the cast itself is unchecked; the invariant lives in the page math, not at the cast site.
- **Coalesced physical keys embed both endpoint keys** (`b'c' ++ first ++ last`) — uniqueness rests on packs never being re-cut over the same block range with different contents; replay idempotency arguments cover the value bytes, and identical re-cuts produce identical keys, but a *differently-cut* replay would write new objects and orphan old ones (garbage, never corruption). No GC path is visible in these files.
- **Seal-chain rows for spans sealed before the seal chain existed** return `None` (`tables.rs:1322-1324`) — how verification tooling treats a `None` point (skip vs error) lives outside these files (`api.rs:349-374` just passes it through).
