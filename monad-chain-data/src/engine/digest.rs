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
//! Every ingested block produces a deterministic *content digest* over the
//! logical artifacts it would write: the uncompressed (pre-compression) row
//! payloads, the EVM header, the per-family primary-id windows, the dictionary
//! versions, and the index bitmap fragments. Per-block digests are folded into
//! a hash chain whose running value is stored in every
//! [`BlockRecord`](crate::primitives::state::BlockRecord) and surfaced in the
//! publication head, so a single 32-byte value certifies the whole ingested
//! history.
//!
//! A standby that ingests the same finalized blocks re-derives the same chain
//! and compares — proving it *would have written the same artifacts* without
//! re-reading a single stored byte.
//!
//! ## Why uncompressed values
//!
//! The digest folds the row bytes *before* zstd framing, so it never observes a
//! compressed frame. Two nodes agree on the checksum whenever they agree on the
//! logical content, even if their zstd library versions produce different
//! compressed bytes. (The compressed-frame offsets in the per-family block
//! headers are likewise excluded — they are a pure function of the frame sizes,
//! which is exactly the zstd-dependent detail we want the digest to ignore.)
//!
//! ## Determinism rules
//!
//! - Every variable-length field is length-prefixed, so the field stream is
//!   unambiguous: `["ab", "c"]` and `["a", "bc"]` hash differently.
//! - Sets with no inherent storage order (bitmap fragments) are folded in a
//!   canonical sorted order, so map/iteration order cannot perturb the result.
//! - Fixed-width fields use little-endian byte order.

use blake3::Hasher;

use crate::{
    engine::bitmap::BitmapFragmentWrite, family::Hash32, primitives::state::FamilyWindowRecord,
};

/// A 32-byte artifact checksum. Stored as [`Hash32`] (`B256`) so it round-trips
/// through the existing RLP layouts of `BlockRecord` and `PublicationState`.
pub type ArtifactChecksum = Hash32;

/// The chain seed, used before any block is ingested and as the stored value
/// for an owner-less / empty publication head.
pub const EMPTY_CHECKSUM: ArtifactChecksum = Hash32::ZERO;

fn to_checksum(hash: blake3::Hash) -> ArtifactChecksum {
    Hash32::from(*hash.as_bytes())
}

/// Length-prefixing field hasher. Domain-separates variable-length inputs so
/// concatenation is unambiguous, and writes fixed-width scalars little-endian.
#[derive(Default)]
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

    fn finish(self) -> ArtifactChecksum {
        to_checksum(self.0.finalize())
    }
}

/// Incrementally folds a family's uncompressed row payloads into a digest as
/// the rows are framed, so the row bytes are hashed in the same pass that
/// compresses them — no extra pass and no retained payloads. Each row is
/// length-prefixed, so the row count and boundaries are captured implicitly.
#[derive(Debug, Default)]
pub struct RowDigest(Hasher);

impl RowDigest {
    pub fn new() -> Self {
        Self(Hasher::new())
    }

    /// Folds one uncompressed (pre-compression) row payload.
    pub fn row(&mut self, raw: &[u8]) {
        self.0.update(&(raw.len() as u64).to_le_bytes());
        self.0.update(raw);
    }

    /// Seals the row digest. Equality of two `RowDigest` results means the two
    /// blocks framed byte-identical row payloads in the same order.
    pub fn finish(&self) -> ArtifactChecksum {
        to_checksum(self.0.finalize())
    }
}

/// Combines one family's logical artifacts into a content digest: its
/// primary-id window, dictionary version, sealed [`RowDigest`], and index
/// bitmap fragments. `fragments` need not be pre-sorted — they are folded in a
/// canonical `(stream_id, page_start_local)` order.
pub fn family_content_digest(
    window: FamilyWindowRecord,
    dict_version: u32,
    rows: ArtifactChecksum,
    fragments: &[BitmapFragmentWrite],
) -> ArtifactChecksum {
    let mut h = FieldHasher::default();
    h.u64(window.first_primary_id.as_u64())
        .u32(window.count)
        .u32(dict_version)
        .bytes(rows.as_slice());

    let mut ordered: Vec<&BitmapFragmentWrite> = fragments.iter().collect();
    ordered.sort_unstable_by(|a, b| {
        a.stream_id
            .cmp(&b.stream_id)
            .then(a.page_start_local.cmp(&b.page_start_local))
    });
    h.u64(ordered.len() as u64);
    for f in ordered {
        h.bytes(f.stream_id.as_bytes())
            .u32(f.page_start_local)
            .bytes(&f.bitmap_blob);
    }
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
    log_family: ArtifactChecksum,
    tx_family: ArtifactChecksum,
    trace_family: ArtifactChecksum,
) -> ArtifactChecksum {
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
pub fn chain(previous: ArtifactChecksum, block_content: ArtifactChecksum) -> ArtifactChecksum {
    let mut hasher = Hasher::new();
    hasher.update(previous.as_slice());
    hasher.update(block_content.as_slice());
    to_checksum(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::primitives::state::PrimaryId;

    fn frag(stream: &str, page: u32, blob: &[u8]) -> BitmapFragmentWrite {
        BitmapFragmentWrite {
            stream_id: stream.to_string(),
            page_start_local: page,
            bitmap_blob: Bytes::copy_from_slice(blob),
        }
    }

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

        // Length-prefixing makes the boundary significant.
        assert_ne!(a.finish(), b.finish());

        let mut c = RowDigest::new();
        c.row(b"c");
        c.row(b"ab");
        // Order matters.
        assert_ne!(a.finish(), c.finish());
    }

    #[test]
    fn family_digest_ignores_fragment_order() {
        let rows = RowDigest::new().finish();
        let f1 = frag("addr/0", 0, b"\x01\x02");
        let f2 = frag("topic0/0", 64, b"\x03");
        let forward = family_content_digest(window(10, 2), 1, rows, &[f1.clone(), f2.clone()]);
        let reversed = family_content_digest(window(10, 2), 1, rows, &[f2, f1]);
        assert_eq!(forward, reversed);
    }

    #[test]
    fn family_digest_distinguishes_window_and_dict() {
        let rows = RowDigest::new().finish();
        let base = family_content_digest(window(10, 2), 1, rows, &[]);
        assert_ne!(base, family_content_digest(window(11, 2), 1, rows, &[]));
        assert_ne!(base, family_content_digest(window(10, 3), 1, rows, &[]));
        assert_ne!(base, family_content_digest(window(10, 2), 2, rows, &[]));
    }

    #[test]
    fn chain_depends_on_history_and_order() {
        let b1 = block_content_digest(
            1,
            &Hash32::ZERO,
            &Hash32::ZERO,
            b"h1",
            EMPTY_CHECKSUM,
            EMPTY_CHECKSUM,
            EMPTY_CHECKSUM,
        );
        let b2 = block_content_digest(
            2,
            &Hash32::repeat_byte(7),
            &Hash32::ZERO,
            b"h2",
            EMPTY_CHECKSUM,
            EMPTY_CHECKSUM,
            EMPTY_CHECKSUM,
        );

        let forward = chain(chain(EMPTY_CHECKSUM, b1), b2);
        let swapped = chain(chain(EMPTY_CHECKSUM, b2), b1);
        assert_ne!(forward, swapped);

        // A different seed yields a different chain head.
        assert_ne!(forward, chain(chain(Hash32::repeat_byte(1), b1), b2));
    }
}
