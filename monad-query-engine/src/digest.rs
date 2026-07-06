// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Artifact content checksums for standby ingest verification.
//!
//! Two replicas ingesting the same finalized block stream must reach
//! byte-identical digests, so every chain here is a function of the LOGICAL
//! block stream only — never of flush cadence, pack boundaries, compression,
//! dict versions, physical keys, or timing.
//!
//! Two independent chains:
//!
//! 1. **Row chain** (data track). Each block's logical content (number, hash,
//!    parent hash, EVM header bytes, per-family windows + uncompressed row
//!    payloads) folds into [`block_content_digest`]; per-block digests chain
//!    into the running value stored in every
//!    [`BlockRecord`](monad_query_primitives::records::BlockRecord)
//!    `row_chain`, so one 32-byte value certifies the whole history.
//!
//! 2. **Seal chains** (index track), one per family. Open-page FRAGMENTS are
//!    flush-cadence artifacts and MUST NOT be hashed; sealed page/bucket
//!    artifacts are content-deterministic (the union of everything in a 64K
//!    id span) and keyed by id-space position, so they chain deterministically.
//!    When a span seals, [`SealDigest`] hashes the exact persisted artifact
//!    bytes in canonical order and the result folds into the family's chain,
//!    persisted alongside the seal batch.
//!
//! Verification recipe: a standby comparison reads the primary's
//! `row_chain` at height N (row data equality through N) plus each
//! family's `(last sealed span, seal_chain)` row (index equality through the
//! sealed frontier) and compares against its own — see
//! `MonadChainDataService::standby_digests`.
//!
//! Determinism: row bytes are hashed pre-compression (and frame offsets are
//! excluded) so the checksum is independent of the zstd codec/version;
//! variable-length fields are length-prefixed; unordered sets are folded in
//! canonical sorted order; fixed-width fields are little-endian.

use blake3::Hasher;
use monad_query_primitives::{records::FamilyWindowRecord, Hash32};

use crate::family::Family;

/// A 32-byte chain digest, shared by the row chain and the per-family seal
/// chains. Stored as [`Hash32`] (`B256`) so it round-trips through the existing
/// RLP layouts of `BlockRecord` and `PublicationState`.
pub type ChainDigest = Hash32;

/// The chain seed, used before any block is ingested and as the stored value
/// for an owner-less / empty publication head.
pub const EMPTY_DIGEST: ChainDigest = Hash32::ZERO;

fn to_checksum(hash: blake3::Hash) -> ChainDigest {
    Hash32::from(*hash.as_bytes())
}

/// Length-prefixing field hasher. Domain-separates variable-length inputs so
/// concatenation is unambiguous, and writes fixed-width scalars little-endian.
#[derive(Debug, Default)]
struct FieldHasher(Hasher);

impl FieldHasher {
    fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.0.update(&(b.len() as u64).to_le_bytes());
        self.0.update(b);
        self
    }

    fn u64(&mut self, v: u64) -> &mut Self {
        self.0.update(&v.to_le_bytes());
        self
    }

    fn u32(&mut self, v: u32) -> &mut Self {
        self.0.update(&v.to_le_bytes());
        self
    }

    fn finish(&self) -> ChainDigest {
        to_checksum(self.0.finalize())
    }
}

/// Incrementally folds a family's uncompressed row payloads in the same pass
/// that compresses them. Each row is length-prefixed, so row count and
/// boundaries are captured implicitly.
#[derive(Debug, Default)]
pub(crate) struct RowDigest(FieldHasher);

impl RowDigest {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Folds one uncompressed (pre-compression) row payload.
    pub(crate) fn row(&mut self, raw: &[u8]) {
        self.0.bytes(raw);
    }

    /// Finalizes the row digest. Equal results mean byte-identical row payloads
    /// in the same order.
    pub(crate) fn finish(&self) -> ChainDigest {
        self.0.finish()
    }
}

/// Combines one family's window (id position + row count) and sealed
/// [`RowDigest`] into a content digest. Deliberately excludes dict versions,
/// fragments, and anything else cadence- or compression-dependent.
pub fn family_content_digest(window: FamilyWindowRecord, rows: ChainDigest) -> ChainDigest {
    let mut h = FieldHasher::default();
    h.u64(window.first_primary_id.as_u64())
        .u32(window.count)
        .bytes(rows.as_slice());
    h.finish()
}

/// Combines the block-level artifacts (identity + EVM header) with the three
/// per-family content digests into the block's standalone content digest. The
/// family order is fixed: log, tx, trace.
pub fn block_content_digest(
    block_number: u64,
    block_hash: &Hash32,
    parent_hash: &Hash32,
    evm_header_rlp: &[u8],
    log_family: ChainDigest,
    tx_family: ChainDigest,
    trace_family: ChainDigest,
) -> ChainDigest {
    let mut h = FieldHasher::default();
    h.u64(block_number)
        .bytes(block_hash.as_slice())
        .bytes(parent_hash.as_slice())
        .bytes(evm_header_rlp)
        .bytes(log_family.as_slice())
        .bytes(tx_family.as_slice())
        .bytes(trace_family.as_slice());
    h.finish()
}

/// Folds a block's content digest into the running chain:
/// `chain(prev, block) = H(prev ‖ block)`. Both inputs are fixed 32-byte
/// values, so no length prefixing is needed.
pub fn chain(previous: ChainDigest, block_content: ChainDigest) -> ChainDigest {
    let mut hasher = Hasher::new();
    hasher.update(previous.as_slice());
    hasher.update(block_content.as_slice());
    to_checksum(hasher.finalize())
}

/// Digest over the canonical FINAL sealed content of one 64K id span of one
/// family: the per-stream sealed page artifacts (each framed as
/// `(stream id, persisted artifact bytes)`, fed in ascending stream-id order)
/// followed by the span's directory-bucket summary bytes. Hashes the exact
/// bytes being persisted, so a standby that stores the same sealed artifacts
/// reaches the same digest regardless of how many flushes produced them.
///
/// The page-group page-counts manifest is deliberately NOT folded in: every
/// per-page count is already embedded in the hashed artifact bytes (the
/// `encode_bitmap_blob` header), so the manifest row is a pure function of
/// content this digest covers and hashing it would add nothing.
#[derive(Debug)]
pub struct SealDigest {
    hasher: FieldHasher,
}

impl SealDigest {
    pub fn new(family: Family, span_start: u64) -> Self {
        // Stable per-family tag (Family::ALL order) for domain separation.
        let tag = Family::ALL
            .iter()
            .position(|f| *f == family)
            .expect("family is one of Family::ALL") as u32;
        let mut hasher = FieldHasher::default();
        hasher.u32(tag).u64(span_start);
        Self { hasher }
    }

    /// Folds one sealed page artifact. Callers MUST feed pages in ascending
    /// `stream_id` order; `artifact` is the exact persisted value bytes.
    pub fn page(&mut self, stream_id: &str, artifact: &[u8]) -> &mut Self {
        self.hasher.bytes(stream_id.as_bytes()).bytes(artifact);
        self
    }

    /// Seals the digest with the span's directory-bucket summary bytes (the
    /// exact persisted value). No page count is hashed: the length-prefixed
    /// `(stream_id, artifact)` pairs are already injective and the
    /// length-prefixed bucket bytes cleanly terminate the page run, so the
    /// framing's injectivity rests on structure alone (no trailing fixed-width
    /// scalar following an unbounded length-prefixed run, which is the one
    /// mixing pattern that could alias across different (pages, bucket) splits).
    /// This matches the `RowDigest` discipline.
    pub fn finish(mut self, bucket: &[u8]) -> ChainDigest {
        self.hasher.bytes(bucket);
        self.hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use monad_query_primitives::records::PrimaryId;

    use super::*;

    fn window(first: u64, count: u32) -> FamilyWindowRecord {
        FamilyWindowRecord {
            first_primary_id: PrimaryId::new(first),
            count,
        }
    }

    #[test]
    fn row_digest_is_order_and_boundary_sensitive() {
        let mut a = RowDigest::new();
        a.row(b"ab");
        a.row(b"c");

        let mut b = RowDigest::new();
        b.row(b"a");
        b.row(b"bc");

        // Length-prefixing makes the row boundary significant.
        assert_ne!(a.finish(), b.finish());

        let mut c = RowDigest::new();
        c.row(b"c");
        c.row(b"ab");
        assert_ne!(a.finish(), c.finish());
    }

    #[test]
    fn family_digest_distinguishes_window_and_rows() {
        let rows = RowDigest::new().finish();
        let mut other = RowDigest::new();
        other.row(b"r");
        let base = family_content_digest(window(10, 2), rows);
        assert_ne!(base, family_content_digest(window(11, 2), rows));
        assert_ne!(base, family_content_digest(window(10, 3), rows));
        assert_ne!(base, family_content_digest(window(10, 2), other.finish()));
    }

    #[test]
    fn seal_digest_distinguishes_family_span_pages_and_bucket() {
        let seal = |family, span, pages: &[(&str, &[u8])], bucket: &[u8]| {
            let mut d = SealDigest::new(family, span);
            for (stream, artifact) in pages {
                d.page(stream, artifact);
            }
            d.finish(bucket)
        };
        let pages: &[(&str, &[u8])] = &[("addr/aa/0", b"\x01"), ("topic0/bb/0", b"\x02")];
        let base = seal(Family::Log, 0, pages, b"bucket");
        assert_ne!(base, seal(Family::Tx, 0, pages, b"bucket"));
        assert_ne!(base, seal(Family::Log, 65_536, pages, b"bucket"));
        assert_ne!(base, seal(Family::Log, 0, &pages[..1], b"bucket"));
        assert_ne!(base, seal(Family::Log, 0, pages, b"other"));
        // Page order is significant: callers must feed ascending stream ids.
        let swapped: &[(&str, &[u8])] = &[("topic0/bb/0", b"\x02"), ("addr/aa/0", b"\x01")];
        assert_ne!(base, seal(Family::Log, 0, swapped, b"bucket"));
    }

    #[test]
    fn chain_depends_on_history_and_order() {
        let b1 = block_content_digest(
            1,
            &Hash32::ZERO,
            &Hash32::ZERO,
            b"h1",
            EMPTY_DIGEST,
            EMPTY_DIGEST,
            EMPTY_DIGEST,
        );
        let b2 = block_content_digest(
            2,
            &Hash32::repeat_byte(7),
            &Hash32::ZERO,
            b"h2",
            EMPTY_DIGEST,
            EMPTY_DIGEST,
            EMPTY_DIGEST,
        );

        let forward = chain(chain(EMPTY_DIGEST, b1), b2);
        let swapped = chain(chain(EMPTY_DIGEST, b2), b1);
        assert_ne!(forward, swapped);

        assert_ne!(forward, chain(chain(Hash32::repeat_byte(1), b1), b2));
    }
}
