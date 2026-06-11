# monad-chain-data — doc-prep source notes (2026-06-11)

Knowledge capture from a six-agent deep read of the crate at `jhow/chain-data/feature` (e2d8bba7),
produced as raw material for writing/repairing the crate docs. Each file is self-contained dense
notes with `file:line` references, a glossary, gotchas, and open questions.

| File | Subsystem | Scope |
|---|---|---|
| [01-engine-write-path.md](01-engine-write-path.md) | Engine core / write path | `src/engine/{family,tables,row_codec,bitmap,clause,digest,primary_dir}.rs` — artifact formats, write flow, invariants |
| [02-query-read-path.md](02-query-read-path.md) | Query / read path | `src/api.rs`, `src/engine/query/*`, `src/mem_scan.rs`, `src/blocks.rs`, hash index, queryX mapping |
| [03-ingest-orchestration.md](03-ingest-orchestration.md) | Ingest orchestration | `src/ingest/*`, `src/config/*`, recovery regimes, concurrency model, archiver embedding |
| [04-families-and-primitives.md](04-families-and-primitives.md) | Data families & primitives | `src/{logs,txs,traces,transfers}/*`, `src/primitives/*`, `src/error.rs`, `src/testkit.rs` |
| [05-storage-layer.md](05-storage-layer.md) | Storage layer | `src/store/*` — traits, S3/Dynamo/in-memory backends, caching, sessions, at-rest namespace |
| [06-docs-tests-audit.md](06-docs-tests-audit.md) | Docs & tests audit | onboarding.md/ops-runbook.md accuracy audit, queryX spec summary, behavior catalog from ~110 integration tests, gap analysis |

## Architecture in one paragraph

The crate is the storage + query engine for the queryX spec (transport/projection lives in
monad-rpc). Ingest is a **mode-less ("branchless") pipeline**: a producer fetches finalized blocks
and mints dense per-family primary ids, fanning each block to a **data track** (per-row
RLP→zstd-framed block blobs + the per-block blake3 **row chain**) and an **index track**
(page-relative roaring-bitmap streams + a primary directory, continuously **sealed** per 64Ki-id
granule with per-family **seal chains**, flushed as fragments on an adaptive cadence). A publisher
advances the single reader-visible head to `min(data_durable, index_visible)`. Reads run a
3-stage ordered pipeline (group plan → page intersect/resolve → count-gated materialize) over
sealed artifacts + open fragments, with block-aligned limits and stateless block-cursor pagination.
Storage is two traits — `MetaStore` (Dynamo/Alternator) and `BlobStore` (S3/RustFS or chunked
Dynamo) — with no CAS anywhere: correctness rests on idempotent content-deterministic writes plus
head-publication visibility gating.

## Cross-cutting invariants every doc should respect

- Page order == id order == block order; the whole read pipeline is sort-free because ids are dense and monotone per family.
- `PAGE_SPAN == BUCKET_SPAN == SEAL_SPAN = 64Ki` (compile-asserted); one frontier classifies bitmap pages and directory buckets.
- Bits and clamp cutoffs are **page-relative**; pre-de-shard blobs are version-rejected.
- Caches are read-populated only; writes never seed them. Published data is immutable.
- `limit` is a target, not a cap — the current block always completes.
- Row chain & seal chains are cadence/restart-independent functions of logical content; page-count manifests are deliberately NOT digested.
- Recovery: `resume = max(checkpoint_block, published_head)`; checkpoint regime vs live fragment-rebuild regime; reader and recovery must track the data model in lockstep.
- No CAS / no lease — single writer is an operational rule, atomicity unit is the single `publication_state` row.

## Prioritized doc plan (from the audit in 06)

1. **Fix onboarding.md in place** (don't rewrite): remove the lease/"write authority" section and `engine/authority.rs` reference; fix CAS language → single-row put + ops one-writer rule; fix stale paths (`ingest_core.rs`/`backfill.rs`/`live.rs`/etc. → `src/ingest/*`, `src/config/*`); fix `artifact_checksum`→`row_chain`, `RawLogEntry`→`StoredLog`, bucket size 10,000→65,536, per-family-blob → one shared block blob with family regions, stale line numbers, dead deep-dive links. Full quote-level list in 06 §6.
2. **Write the five deep-dives** the onboarding already links (frontier model, ingest batching, recovery, bitmap index, standby verification — re-scoped from "primary/standby" since the lease is gone).
3. **On-disk format spec** — tables, key encodings, artifact byte layouts, version bytes, documented hard format breaks. The table in 01 §3 + 05 §7 is the seed.
4. **Query execution walkthrough** referencing the instructive tests named in 06 §3.
5. **queryX ↔ crate mapping** (what's engine vs transport: tags, fields projection, error codes).
6. **Testing guide** (testkit pattern, observed-store fetch counting, gated dynamo/s3 suites, coverage gaps: no integration tests for crash recovery / publisher frontier / standby comparison).
7. **Glossary** — merge the per-file glossaries (01 §6, 02 §9, 03 §9, 04 §9, 05 §9).

ops-runbook.md is current and accurate — leave it alone.
