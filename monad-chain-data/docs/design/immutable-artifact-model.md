# Immutable Artifact Model

This is the second design doc. Read [Bitmap-Indexed History Queries](bitmap-indexed-history-queries.md) first.

This is also introductory reading material. It explains the storage and
publication model that makes indefinite caching and append-only reads
practical.

## The constraint

The bitmap doc claims sealed artifacts can be cached indefinitely with no invalidation. This doc describes the storage model behind that claim.

## One Reader-Visible Mutable Value

Readers see a single mutable value: the **publication head**
(`publication_state.indexed_finalized_head`), which records the highest block
number whose artifacts have all been written. Everything behind the head is
immutable. Everything ahead of it doesn't exist yet from a reader's
perspective.

A reader loads the publication head once at the start of a query and clips the
requested block range against it. Readers and the writer don't coordinate on
anything else.

Writers also maintain a small amount of mutable ingest inventory such as open
bitmap-page markers, but that state is not part of the reader-visible data
boundary.

## What gets written per block

When a finalized block is ingested, the writer produces a fixed set of
artifacts.

This doc uses logs as the main example because they are the easiest family to
explain first. The same publication model also applies to the txs and traces
families, each with its own payload artifacts, per-block headers, directory
fragments, and stream fragments.

For the logs example, the writer produces:

- **Log blob** — the concatenated encoded logs for the block, stored as a single object keyed by block number. Individual logs are retrieved via byte-range reads using an offset table.
- **Block log header** — the offset table mapping local log ordinals to byte ranges within the log blob.
- **Block hash index** — a reverse lookup from block hash to block number.
- **Shared block record** — block hash, parent hash, plus the per-family
  primary-ID windows for whichever indexed families participated in the block.
  This is the authoritative per-block sequencing record.
- **Directory fragments** — one small record per sub-bucket of the log ID space that the block's logs touch. Each fragment says "block N contributed log IDs X through Y to this sub-bucket." Together, fragments in a sub-bucket let readers resolve any log ID in that range to a block number.
- **Stream fragments** — for each address or topic value that appears in the block's logs, a roaring bitmap recording which local IDs within the relevant page matched that value. This is the per-block contribution to the bitmap indexes described in the first doc.

All of these are immutable once written. None are updated or overwritten by later blocks.

## Publication ordering

The writer must make all artifacts for a block durable before advancing the
publication head past it. A reader that sees head = N is guaranteed that every
artifact for blocks 0 through N exists and is readable.

The write order within a block matters for crash safety, but readers do not
observe intermediate states because the head does not move until everything is
in place.

At a high level, the shared ingest envelope writes:

1. the block-hash index
2. family payload artifacts
3. family directory and stream fragments
4. any eager compaction for sealed ranges
5. the shared block record
6. the publication-head advance

## How reads work

### Directory lookup

Directory fragments are keyed by sub-bucket (a fixed-size range of the log ID space) and block number. To resolve a log ID to a block:

1. Compute which sub-bucket the log ID falls in.
2. Load the fragments for that sub-bucket. Each fragment records a block's first log ID and count within the sub-bucket.
3. Find the fragment whose range contains the target log ID.
4. Read the block number from that fragment.

That's one read per block that contributed to the sub-bucket. Compacted summaries can reduce this to a single read for older history, but the fragment path is always correct.

### Bitmap lookup

Given a filter and a log ID window:

1. Determine which bitmap pages overlap the ID window.
2. For each page, load the stream fragments for the relevant address or topic value. Each fragment is one block's bitmap contribution to that page.
3. Merge the fragments into a page-level bitmap.
4. Intersect across clauses.

Compacted page summaries can replace the per-block fragment loads for sealed pages, but merging fragments on read is the baseline.

### Materialization

Once the reader has matched log IDs and can resolve them to blocks via the
directory:

1. Looks up the block number and local ordinal for each log ID.
2. Loads the block log header to get byte offsets.
3. Issues a byte-range read against the log blob.
4. Decodes and exact-match filters the log.

Contiguous IDs within the same block share the header and blob lookups, so sequential pagination through a busy contract's logs is efficient.

## Why immutability matters

Artifacts behind the publication head are never modified.

A cached directory fragment or stream bitmap is valid forever. Eviction is the
only reason to drop it. Readers and the writer do not contend on individual
artifacts.

If the writer crashes mid-block, the head has not moved, so partial artifacts
are invisible to readers. On ownership transition, recovery repairs mutable
ingest inventory and resumes from the published head. Unpublished suffix
artifacts remain outside the visible read boundary because the head never
advanced to include them.

## Compaction

Without optimization, queries over long history ranges load many small fragments per sub-bucket or page. Compaction reduces this read amplification by merging sealed ranges of fragments into summary artifacts (merged directory buckets, merged stream page bitmaps). The ingest pipeline triggers compaction eagerly when a sub-bucket or page boundary is crossed.

Summaries coexist with source fragments — they're an acceleration layer, not a replacement. Queries prefer summaries when available and fall back to fragments for recent (not-yet-compacted) data. If a summary is missing or corrupt, the fragment path is always correct.

Compaction is not required for correctness. The fragment-based read path described above is the complete system; compaction just makes it faster for older history.
