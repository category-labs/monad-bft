# Deep dive: archive-backed payloads

> Prerequisite: read `docs/onboarding.md`,
> `docs/deep-dive-ingest-batching.md`, and
> `docs/deep-dive-frontier-model.md`. This document scopes an implementation
> that lets chain-data build and publish indexes while serving row payloads from
> an external payload source such as `monad-archive`.

## Goal

Today chain-data ingest reads finalized block data from `monad-archive`,
canonicalizes it into chain-data row types, builds bitmap/primary indexes, writes
new chain-data block blobs, and stores headers that point at those blobs. The
same upstream data already exists in archive objects.

The goal is to split index writes from payload writes:

- Chain-data always owns the index and publication model.
- Payload rows can come from either chain-data's current blob table or an
  external source.
- Archive remains responsible for archive storage codecs and validates that a
  stored range decodes as the component it claims to be.
- Chain-data stores enough payload locator metadata to serve indexed queries
  without rescanning archive objects for offsets.

The first external source is archive-backed payloads. The design should remain
generic enough to add other payload stores later.

## Non-goals

- Do not change the primary-id model. Log, tx, and trace ids remain dense,
  gap-free, and assigned by chain-data ingest.
- Do not move bitmap or primary-directory indexes into archive.
- Do not make chain-data parse archive storage representations directly.
- Do not require deep per-log or per-trace byte offsets initially. Tx-granularity
  receipt/trace ranges are acceptable for the first implementation.
- Do not preserve binary compatibility for stores published with a new payload
  header format unless the format explicitly supports old headers.

## Current coupling

The relevant current flow is:

1. `plan_ingest_blocks` assigns primary-id windows for each family.
2. `LogIngestPlan::build`, `TxIngestPlan::build`, and
   `TraceIngestPlan::build` each produce:
   - family window
   - bitmap fragments
   - uncompressed row digest
   - row-codec `BlockBlobHeader`
   - compressed family blob bytes
3. `plan_ingest_blocks` sets `base_offset` for the three family blobs, combines
   them into one per-block blob, and stages it with `Tables::stage_block_blob`.
4. Block metadata stores three opaque family header byte strings. Today those
   bytes are all `BlockBlobHeader`.
5. `coalesce_block_blob_writes` may rewrite those headers so multiple block
   blobs point into one coalesced physical blob.
6. Materializers load the family header, decode `BlockBlobHeader`, read a frame
   from `BLOCK_BLOB_TABLE`, decompress it via row codec, then decode the family
   row type.

The key problem is that `BlockBlobHeader` is both a physical blob locator and a
codec descriptor for chain-data zstd row frames. Archive payloads need different
locator and codec semantics.

## Design overview

Introduce a versioned row-payload header stored in the existing per-block family
header slots:

```rust
pub enum RowPayloadHeader {
    ChainDataV1(ChainDataBlobHeader),
    ArchiveV1(ArchivePayloadHeader),
}
```

`ChainDataV1` preserves current behavior. `ArchiveV1` points at archive
component ranges and contains enough row-local mapping to turn a chain-data row
ordinal into a typed archive range read.

Ingest becomes:

```text
Finalized block + payload provenance
    -> family index plans
    -> family payload plans
    -> block metadata with versioned payload headers
    -> optional chain-data blob writes
    -> index writes
    -> publication CAS
```

Query becomes:

```text
primary id -> (block_number, idx_in_block)
block metadata -> RowPayloadHeader
RowPayloadSource::load_record_at(...)
```

`RowPayloadSource` dispatches by header variant. Chain-data blob headers use the
existing blob + row-codec path. Archive headers call typed archive range-read
methods.

## Data model

### Versioned payload header

Add a new type in `src/primitives/state.rs` or a new
`src/primitives/payload.rs` module:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowPayloadHeader {
    ChainDataV1(ChainDataBlobHeader),
    ArchiveV1(ArchivePayloadHeader),
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct ChainDataBlobHeader {
    pub offsets: Vec<u32>,
    pub dict_version: u32,
    pub base_offset: u32,
    pub physical_key: Vec<u8>,
    pub physical_base_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct ArchivePayloadHeader {
    pub source_id: PayloadSourceId,
    pub family: Family,
    pub codec: ArchivePayloadCodec,
    pub rows: ArchiveRows,
}
```

`ChainDataBlobHeader` should initially be the current `BlockBlobHeader` fields.
The implementation can either rename `BlockBlobHeader` to `ChainDataBlobHeader`
and type-alias the old name temporarily, or keep `BlockBlobHeader` and wrap it
inside `RowPayloadHeader::ChainDataV1`.

### RLP encoding

Use a sentinel + version byte so old `BlockBlobHeader` bytes can still decode:

```text
legacy bytes:
    decode as BlockBlobHeader -> RowPayloadHeader::ChainDataV1

new bytes:
    [sentinel, version] || rlp(payload)
```

Suggested markers:

```rust
const ROW_PAYLOAD_SENTINEL: u8 = 0x50;
const ROW_PAYLOAD_CHAIN_DATA_V1: u8 = 1;
const ROW_PAYLOAD_ARCHIVE_V1: u8 = 2;
```

Decode rules:

1. If the first two bytes match the sentinel format, decode the selected new
   variant.
2. Otherwise, decode the full byte slice as legacy `BlockBlobHeader` and return
   `ChainDataV1`.
3. If sentinel bytes are present but the version is unknown, fail loudly. Do not
   silently fall back to legacy.

This keeps existing stores readable while making new archive-backed metadata
self-describing.

### Payload source id

`PayloadSourceId` identifies the archive/source configuration used at query
time:

```rust
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct PayloadSourceId(pub Vec<u8>);
```

For the ingest binary, this can be a user-provided string:

```text
--payload-source-id mainnet-deu-009-0
```

The runtime service must map this id to an actual payload reader. A single-source
deployment can default to one configured archive reader, but the metadata should
carry an id so future multi-source deployments are possible.

### Archive row metadata

Archive offsets are per transaction component:

- tx range within the encoded block object
- receipt range within the encoded receipts object
- trace range within the encoded traces object

For tx rows, row ordinal equals tx index. For logs and traces, chain-data row
ordinal is the flattened family row index, so the header needs a map from row
ordinal to tx-local position.

```rust
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub enum ArchiveRows {
    Txs {
        tx_ranges: Vec<ArchiveRange>,
    },
    Logs {
        receipt_ranges: Vec<ArchiveRange>,
        log_locs: Vec<TxLocalLogLoc>,
    },
    Traces {
        trace_ranges: Vec<ArchiveRange>,
        trace_locs: Vec<TxLocalTraceLoc>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct ArchiveRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct TxLocalLogLoc {
    pub tx_idx: u32,
    pub log_idx: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct TxLocalTraceLoc {
    pub tx_idx: u32,
    pub trace_idx: u32,
}
```

`ArchiveRange` uses `u64` even though archive currently uses `usize`, because
the metadata is persisted and should not depend on host pointer width.

### Archive codec descriptor

Archive owns its codec, but chain-data still needs to persist which archive
component codec contract the header expects:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub enum ArchivePayloadCodec {
    MonadArchiveV1,
}
```

This descriptor is not permission for chain-data to decode archive bytes. It is
an input to payload-source dispatch and validation. Archive range-read methods
should reject unsupported codec/source combinations.

## Archive API changes

Add typed range-read APIs to `monad-archive`. Chain-data should not use a public
"raw byte range" method for payload materialization.

Suggested trait:

```rust
pub trait BlockDataRangeReader: Clone {
    async fn get_tx_in_range(
        &self,
        block_num: u64,
        range: RangeRlp,
    ) -> Result<TxEnvelopeWithSender>;

    async fn get_receipt_in_range(
        &self,
        block_num: u64,
        range: RangeRlp,
    ) -> Result<ReceiptWithLogIndex>;

    async fn get_trace_in_range(
        &self,
        block_num: u64,
        range: RangeRlp,
    ) -> Result<Vec<u8>>;
}
```

Each method must:

1. Resolve the correct archive key for the component.
2. Read only the provided byte range where the backend supports native range
   reads. For unsupported backends, full-read and slice internally.
3. Decode the returned bytes using archive's storage representation rules.
4. Verify the byte range decodes exactly one value of the expected type and
   consumes the whole range.
5. Return the typed decoded value.

The trait can live next to `BlockDataReader`, and `BlockDataReaderErased` should
expose it for the same source variants where possible.

### Internal archive range bytes

Archive backends need an internal byte-range primitive. Keep it below the typed
API:

```rust
pub(crate) trait KVRangeReader: KVReader {
    async fn get_range(
        &self,
        key: &str,
        start: usize,
        end_exclusive: usize,
    ) -> Result<Option<Bytes>>;
}
```

Backend behavior:

- S3: use HTTP `Range`.
- FS: open file and read the requested span.
- Fjall/memory: full get and slice is acceptable.
- DynamoDB/MongoDB: initially full get and slice unless their representation can
  efficiently project a byte range.
- TrieDB reader: may return no offsets today; archive-reference ingest should be
  strict and reject sources that cannot provide ranges.

### Offset provider

`BlockDataReader::get_block_data_with_offsets` already returns
`BlockDataWithOffsets { offsets: Option<Vec<TxByteOffsets>> }`. Archive-backed
chain-data ingest should require `Some(offsets)` in strict mode.

Add conversion helpers in archive so chain-data does not depend on archive's
`usize` range layout:

```rust
impl TxByteOffsets {
    pub fn tx_range(&self) -> RangeRlp;
    pub fn receipt_range(&self) -> RangeRlp;
    pub fn trace_range(&self) -> RangeRlp;
}
```

Chain-data can convert those to persisted `ArchiveRange` after validating they
fit in `u64`.

## Chain-data ingest changes

### CLI

Add a payload mode to `chain-data-ingest`:

```text
--payload-mode write-chain-data-blobs
--payload-mode reference-archive
--payload-source-id <id>
```

Default: `write-chain-data-blobs`.

`reference-archive` requires:

- `--block-data-source ...` is configured.
- `--payload-source-id` is present, or defaulted from the source string in a
  stable documented way.
- `get_block_data_with_offsets` returns offsets for every block.

Do not silently fall back to writing chain-data blobs in reference mode.

### Input type

Replace `Vec<FinalizedBlock>` at the planning boundary with a block plus payload
provenance:

```rust
pub struct IngestBlock {
    pub block: FinalizedBlock,
    pub payload: PayloadIngestSource,
}

pub enum PayloadIngestSource {
    WriteChainDataBlobs,
    ReferenceArchive {
        source_id: PayloadSourceId,
        offsets: Vec<ArchiveTxOffsets>,
    },
}

pub struct ArchiveTxOffsets {
    pub tx: ArchiveRange,
    pub receipt: ArchiveRange,
    pub trace: ArchiveRange,
}
```

The library can keep a convenience wrapper:

```rust
pub async fn plan_ingest_blocks(
    &self,
    blocks: Vec<FinalizedBlock>,
) -> Result<Option<IngestPlan>> {
    self.plan_ingest_blocks_with_payloads(
        blocks.into_iter().map(IngestBlock::write_chain_data).collect(),
    ).await
}
```

This avoids breaking tests and callers immediately.

### Family planning split

Refactor family ingest plans into index and payload pieces:

```rust
pub struct FamilyIndexPlan {
    pub window: FamilyWindowRecord,
    pub bitmap_fragments: Vec<BitmapFragmentWrite>,
    pub touched_bitmap_streams_by_page: BTreeMap<u64, BTreeSet<String>>,
    pub rows_digest: ArtifactChecksum,
    pub row_count: usize,
}

pub struct FamilyPayloadPlan {
    pub header: RowPayloadHeader,
    pub blob: Option<Vec<u8>>,
}
```

Then each family returns both:

```rust
pub struct LogIngestPlan {
    pub index: FamilyIndexPlan,
    pub payload: FamilyPayloadPlan,
    pub written_logs: usize,
}
```

For `WriteChainDataBlobs`:

- Encode the same canonical row bytes used today.
- Compress into row frames with the current `RowCodec`.
- Build `RowPayloadHeader::ChainDataV1`.
- Return `blob: Some(...)`.

For `ReferenceArchive`:

- Still build canonical row bytes for digest and index semantics.
- Do not compress row frames.
- Build `RowPayloadHeader::ArchiveV1`.
- Return `blob: None`.

This means archive-backed ingest still decodes full block data during planning.
That is intentional: index generation needs logical rows anyway, and digesting
canonical row bytes keeps standby verification stable across payload modes.

### Archive header construction

For txs:

```rust
ArchiveRows::Txs {
    tx_ranges: offsets.iter().map(|o| o.tx).collect(),
}
```

For logs:

```rust
let mut log_locs = Vec::new();
for (tx_idx, logs) in block.logs_by_tx.iter().enumerate() {
    for log_idx in 0..logs.len() {
        log_locs.push(TxLocalLogLoc { tx_idx, log_idx });
    }
}

ArchiveRows::Logs {
    receipt_ranges: offsets.iter().map(|o| o.receipt).collect(),
    log_locs,
}
```

For traces:

The current `FinalizedBlock::traces` is already flattened across all txs. It
contains `tx_index`, so build locators from the flattened row order:

```rust
ArchiveRows::Traces {
    trace_ranges: offsets.iter().map(|o| o.trace).collect(),
    trace_locs: block.traces.iter().map(|trace| TxLocalTraceLoc {
        tx_idx: trace.tx_index,
        trace_idx: trace.tx_trace_index,
    }).collect(),
}
```

`IngestTrace` does not currently carry `tx_trace_index`. Add it while
building ingest traces in the archive ingest binary. It is the ordinal of the
flattened frame within that transaction's trace payload. This is distinct from
`trace_address`, which is a path in the call tree and is not a stable vector
index unless the tree is reconstructed.

```rust
pub struct IngestTrace {
    ...
    pub tx_index: u32,
    pub tx_trace_index: u32,
    pub trace_address: Vec<u32>,
    ...
}
```

The stored chain-data trace row can choose whether to include this field. It is
needed for archive locator construction, not necessarily for API output.

### Stage A changes

Where Phase A currently stages metadata and then always stages a combined
chain-data blob, change to:

```rust
block_tables.stage_metadata(
    w,
    block.block_number(),
    &st.block_record,
    &block.header,
    Bytes::from(st.log_plan.payload.header.encode()),
    Bytes::from(st.tx_plan.payload.header.encode()),
    Bytes::from(st.trace_plan.payload.header.encode()),
);

if let Some(combined) = combine_chain_data_blobs(&st) {
    w.tables().stage_block_blob(w, block.block_number(), combined);
}
```

`combine_chain_data_blobs` should return `None` if all three families are
archive-backed. It should reject mixed payload modes for one block until mixed
mode is explicitly supported.

### Coalescing

`coalesce_block_blob_writes` must decode each family header as
`RowPayloadHeader`. It should rewrite only `ChainDataV1` headers. It should leave
`ArchiveV1` headers unchanged.

If a metadata row has no corresponding block blob write and all three headers
are archive-backed, that is valid. If a metadata row has chain-data headers but
no matching block blob write, preserve the existing behavior and fail or leave
legacy single-object locators as appropriate.

### Artifact checksum

Keep the checksum over canonical uncompressed chain-data row bytes, not over
archive bytes. This preserves the existing storage-codec independence:

```text
same logical block rows -> same content digest
```

The family content digest should include the payload header's logical codec
identity if needed to distinguish payload-mode semantics. Avoid folding physical
archive ranges into the digest unless the requirement is "same source/ranges" and
not just "same logical rows." Recommended first version: keep the digest logical,
as today.

## Query/materialization changes

### Payload source registry

Add a runtime registry on `ChainDataService` or `Tables`:

```rust
pub struct PayloadSources {
    chain_data: ChainDataPayloadSource,
    archive: BTreeMap<PayloadSourceId, ArchivePayloadSource>,
}
```

The ingest binary and API construction paths should register the configured
archive source under `--payload-source-id` when archive-backed reads are needed.

### Materializer boundary

Replace direct `read_block_blob_frame` + `decode_block_row` calls in
`logs/materialize.rs`, `txs/materialize.rs`, and `traces/materialize.rs` with a
typed row loader:

```rust
impl<M: MetaStore, B: BlobStore> Tables<M, B> {
    pub async fn load_log_row(
        &self,
        block_record: &BlockRecord,
        header: &RowPayloadHeader,
        idx_in_block: usize,
    ) -> Result<LogEntry>;

    pub async fn load_tx_row(... ) -> Result<TxEntry>;
    pub async fn load_trace_row(... ) -> Result<TraceEntry>;
}
```

The chain-data variant delegates to the existing frame/decompress/decode logic.
The archive variant delegates to typed archive range reads.

### Archive materialization

Tx:

```rust
let range = tx_ranges[idx_in_block];
let tx = archive.get_tx_in_range(block_number, range.into()).await?;
convert_archive_tx_to_tx_entry(tx, block_record, idx_in_block)
```

Logs:

```rust
let loc = log_locs[idx_in_block];
let receipt_range = receipt_ranges[loc.tx_idx as usize];
let receipt = archive.get_receipt_in_range(block_number, receipt_range.into()).await?;
let log = receipt.receipt.logs()
    .get(loc.log_idx as usize)
    .ok_or(...)?;
convert_log_to_log_entry(log, block_record)
```

Traces:

```rust
let loc = trace_locs[idx_in_block];
let trace_range = trace_ranges[loc.tx_idx as usize];
let raw_trace = archive.get_trace_in_range(block_number, trace_range.into()).await?;
let frames = archive.decode_trace_payload(raw_trace)?;
let frame = frames.get(loc.trace_idx as usize).ok_or(...)?;
convert_frame_to_trace_entry(frame, block_record)
```

Prefer archive exposing `get_log_in_range` and `get_trace_frame_in_range` once
the API settles. The first implementation can keep tx-local selection in
chain-data if archive provides typed receipt and typed raw trace payload reads.

### Grouping optimization

For indexed queries, multiple primary ids may hit the same `(block, tx)`. Add a
small per-block materialization cache inside each query call:

```rust
HashMap<(Family, block_number, tx_idx), DecodedArchiveComponent>
```

This avoids decoding the same receipt or trace payload repeatedly in one query.
It is not required for correctness and can be a follow-up after the basic path.

For block scans, load block-level archive data once:

- Logs: load all receipts or range-read all receipt ranges.
- Txs: load all tx ranges or the full block object.
- Traces: load all trace ranges or the full traces object.

The simplest first implementation may use point row loading even for block
scans, but that will be unnecessarily expensive for broad scans.

## Error handling and invariants

Fail loudly on:

- Archive-backed ingest requested but offsets are absent.
- Archive header row count does not match the family window count.
- `idx_in_block` is outside the header's row map.
- A row locator references a tx index outside the corresponding range vector.
- Archive typed range read returns missing data.
- Archive typed range read does not decode exactly one component of the expected
  type.
- Archive decoded component does not contain the requested log/trace local index.
- Header family does not match the materializer family.
- Payload source id is not registered at query time.

Do not fall back from archive-backed metadata to chain-data blobs unless the
metadata explicitly says both are available. Silent fallback would hide corrupt
headers or missing archive data.

## Migration and compatibility

### Existing stores

Existing stores contain legacy `BlockBlobHeader` bytes. `RowPayloadHeader::decode`
must treat non-sentinel bytes as `ChainDataV1`.

### New stores

New writes should use sentinel-encoded `RowPayloadHeader` even for chain-data
blob mode. This tests the new format continuously while preserving legacy reads.

### Mixed deployments

A reader that does not know `ArchiveV1` cannot serve archive-backed data. That is
expected. The binary should log payload header versions and source ids during
startup or first query so configuration errors are obvious.

## Implementation plan

### Phase 1: archive typed range reads

In `monad-archive`:

- Add internal range-byte reads for KV backends.
- Add typed range-read trait methods for tx, receipt, and trace payloads.
- Validate exact RLP consumption.
- Add tests for:
  - valid tx range
  - valid receipt range
  - valid trace range
  - wrong component range fails
  - truncated range fails
  - range with extra bytes fails
  - backend fallback full-read-and-slice path

### Phase 2: chain-data payload header format

In chain-data:

- Add `RowPayloadHeader`, `ChainDataV1`, `ArchiveV1`, `ArchiveRows`, and range
  locator types.
- Implement encode/decode with legacy fallback.
- Update `coalesce_block_blob_writes` to rewrite only `ChainDataV1`.
- Add tests for:
  - legacy `BlockBlobHeader` decode
  - chain-data sentinel decode
  - archive sentinel decode
  - unknown sentinel version fails
  - coalescing ignores archive headers
  - coalescing rewrites chain-data headers exactly as before

### Phase 3: split ingest plans

- Refactor family ingest plans into `FamilyIndexPlan` and `FamilyPayloadPlan`.
- Keep existing public `plan_ingest_blocks(Vec<FinalizedBlock>)` as a wrapper for
  chain-data blob mode.
- Add `plan_ingest_blocks_with_payloads(Vec<IngestBlock>)`.
- Add archive locator construction from `TxByteOffsets`.
- Add `tx_trace_index` to `IngestTrace` construction.
- Add tests for:
  - chain-data mode produces the same writes as before
  - archive mode stages no block blob writes
  - archive mode still stages block metadata, primary dir, bitmap fragments, tx
    hash index, and publication advance
  - archive header row counts match family windows
  - archive mode requires offsets

### Phase 4: query materialization

- Add payload source registry/configuration.
- Add typed row loading APIs on `Tables` or a dedicated materialization helper.
- Convert log, tx, trace materializers to call the payload row loader.
- Implement chain-data variant by delegating to existing functions.
- Implement archive variant using typed archive range reads.
- Add tests with an in-memory archive source:
  - tx lookup by hash
  - indexed log query
  - indexed trace query
  - block scan logs
  - block scan traces
  - missing source id fails
  - corrupt range fails

### Phase 5: CLI and benchmark path

- Add `--payload-mode` and `--payload-source-id`.
- In archive-reference mode, fetch via `get_block_data_with_offsets`.
- Register the archive reader as a query payload source when serving reads in the
  same process.
- Add telemetry:
  - payload mode
  - archive range reads
  - archive full-object fallback reads
  - archive decode failures
  - staged blob ops skipped by archive mode

## Open decisions

- Whether `ArchivePayloadHeader` should store tx/receipt/trace key strings or
  derive keys from `(source_id, block_number, component)`. Prefer deriving keys
  through archive so key layout stays owned by archive.
- Whether archive should expose `get_log_in_range` and
  `get_trace_frame_in_range` directly. Prefer typed component reads first,
  tx-local helpers later if chain-data starts depending on too much archive
  trace codec detail.
- Whether artifact checksums should include payload header mode. Prefer keeping
  checksums logical for the first version.
- Whether mixed payload modes per block should be allowed. Prefer rejecting mixed
  modes initially.

## Minimal first milestone

The smallest useful milestone is:

1. Archive typed tx/receipt/trace range reads work and validate ranges.
2. Chain-data can publish archive-backed metadata with no block blob writes.
3. Indexed tx/log/trace queries can materialize rows from archive-backed headers.
4. Existing chain-data blob mode still reads legacy and new headers.

That milestone proves the architecture while leaving performance refinements
like grouped query decode and block-scan batching for follow-up work.
