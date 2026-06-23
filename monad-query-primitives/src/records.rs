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

use alloy_rlp::{RlpDecodable, RlpEncodable};
use bytes::Bytes;

use monad_query_errors::{MonadChainDataError, Result};

use crate::Hash32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, RlpEncodable, RlpDecodable)]
pub struct PrimaryId(u64);

impl PrimaryId {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    pub fn checked_add(self, rhs: u64) -> Result<Self> {
        self.0
            .checked_add(rhs)
            .map(Self)
            .ok_or(MonadChainDataError::Decode("primary id overflow"))
    }

    pub fn idx_in_block(self, first: PrimaryId) -> Result<usize> {
        let delta = self
            .0
            .checked_sub(first.0)
            .ok_or(MonadChainDataError::Decode("primary id below block start"))?;
        usize::try_from(delta)
            .map_err(|_| MonadChainDataError::Decode("primary block index overflow"))
    }
}

/// Defines a `PrimaryId` newtype scoped to one record family, so a family's
/// signatures can't accidentally accept ids minted for another family. Each
/// variant shares `PrimaryId`'s representation.
macro_rules! family_id {
    ($name:ident, $family:literal) => {
        #[doc = concat!("`PrimaryId` scoped to the ", $family, " family. Kept as a distinct")]
        #[doc = "type so that family's signatures don't accept primary ids from other"]
        #[doc = "families by mistake."]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, RlpEncodable, RlpDecodable,
        )]
        pub struct $name(PrimaryId);

        impl From<PrimaryId> for $name {
            fn from(id: PrimaryId) -> Self {
                Self(id)
            }
        }

        impl From<$name> for PrimaryId {
            fn from(id: $name) -> Self {
                id.0
            }
        }
    };
}

family_id!(LogId, "log");
family_id!(TxId, "tx");
family_id!(TraceId, "trace");

/// Payload encoding `0`: native per-row zstd frames in a chain-data-owned
/// blob object.
pub const ENCODING_NATIVE: u8 = 0;
/// Payload encoding `1`: "archive containers v1" — `offsets` delimit
/// uncompressed RLP items inside an EXISTING monad-archive object
/// (`physical_key` is the raw archive key), each item a container holding one
/// or more rows.
pub const ENCODING_EXTERNAL_V1: u8 = 1;

/// Byte offsets into one family's per-block payload: one entry per stored unit
/// plus a trailing sentinel equal to the region length, so the length is
/// `unit_count + 1`. In native encoding a unit is one row (zstd frame); in
/// external encoding a unit is one container (an archive RLP item carrying
/// `container_rows`-many rows). The log, tx and trace families all use this
/// identical on-disk layout.
///
/// `encoding`/`container_rows`/`container_status` were a hard break of the
/// on-disk RLP layout (like the `BlockRecord` precedents below); pre-existing
/// data dirs must be wiped and re-ingested.
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct BlockBlobHeader {
    pub offsets: Vec<u32>,
    /// Row-codec dictionary version every frame in this block's blob was
    /// compressed under. `0` = plain zstd frames (no dictionary). Always `0`
    /// in external encoding (containers are never compressed).
    pub dict_version: u32,
    /// Start of this family's region within the shared per-block blob
    /// (native), or of the first container within the archive object
    /// (external).
    pub base_offset: u32,
    /// Physical blob object key. Native: empty means this block owns its
    /// object under the deterministic `block_number` key (set explicitly once
    /// the coalescer repoints the block into a shared object). External: the
    /// raw archive object key — never empty.
    pub physical_key: Vec<u8>,
    /// Byte offset of this block's shared blob inside `physical_key`.
    /// Always `0` in external encoding.
    pub physical_base_offset: u64,
    /// Payload encoding: [`ENCODING_NATIVE`] or [`ENCODING_EXTERNAL_V1`].
    pub encoding: u8,
    /// Cumulative row counts per container (last == row count). Empty means
    /// identity — rows == containers (external txs) — and is always empty in
    /// native encoding.
    pub container_rows: Vec<u32>,
    /// Per-container status bitvec, LSB-first within each byte (bit `j` =
    /// containing tx of container `j` succeeded). Used by external traces;
    /// empty otherwise.
    pub container_status: Bytes,
}

impl BlockBlobHeader {
    pub fn is_external(&self) -> bool {
        self.encoding != ENCODING_NATIVE
    }

    /// Number of stored rows in this block: the last cumulative container
    /// count when present, else one row per offsets unit.
    pub fn row_count(&self) -> usize {
        match self.container_rows.last() {
            Some(&last) => last as usize,
            None => self.offsets.len().saturating_sub(1),
        }
    }

    /// Number of offset-delimited units (rows in native encoding, containers
    /// in external).
    pub fn container_count(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Maps a row index to `(container, ordinal_within_container)`. Identity
    /// (`(idx, 0)`) when `container_rows` is empty. The caller bounds `idx`
    /// by [`Self::row_count`].
    pub fn container_of_row(&self, idx: usize) -> (usize, usize) {
        if self.container_rows.is_empty() {
            return (idx, 0);
        }
        // First container whose cumulative count exceeds `idx`; empty
        // containers (repeated cumulative values) are skipped.
        let container = self
            .container_rows
            .partition_point(|&cum| cum as usize <= idx);
        (container, idx - self.container_row_base(container))
    }

    /// Global row index of container `j`'s first row (`j` itself under the
    /// identity mapping).
    pub fn container_row_base(&self, container: usize) -> usize {
        if self.container_rows.is_empty() {
            return container;
        }
        match container.checked_sub(1) {
            Some(prev) => self.container_rows.get(prev).copied().unwrap_or(0) as usize,
            None => 0,
        }
    }

    /// Rows in container `j`: `1` under the identity mapping.
    pub fn container_row_len(&self, container: usize) -> usize {
        if self.container_rows.is_empty() {
            return 1;
        }
        let end = self.container_rows.get(container).copied().unwrap_or(0) as usize;
        end.saturating_sub(self.container_row_base(container))
    }

    /// Internal consistency of the container manifest. The read paths trust
    /// these shapes for indexing math and preallocation (`container_of_row`,
    /// the scan's `row_count` capacity), so a corrupt metadata row must fail
    /// at decode rather than panic or over-allocate later. External: one
    /// cumulative entry per container (or none = identity), non-decreasing,
    /// total row count bounded by the region length (every row occupies at
    /// least one payload byte), status bitvec absent or sized for the
    /// containers. Native: both container fields empty.
    fn container_manifest_is_consistent(&self) -> bool {
        if !self.is_external() {
            return self.container_rows.is_empty() && self.container_status.is_empty();
        }
        let containers = self.container_count();
        if !self.container_rows.is_empty()
            && (self.container_rows.len() != containers
                || self.container_rows.windows(2).any(|w| w[0] > w[1]))
        {
            return false;
        }
        self.row_count() <= *self.offsets.last().unwrap_or(&0) as usize
            && (self.container_status.is_empty()
                || self.container_status.len() == containers.div_ceil(8))
    }

    /// Container `j`'s status bit (LSB-first); `false` when the bitvec does
    /// not cover `j`.
    pub fn container_status_bit(&self, container: usize) -> bool {
        self.container_status
            .get(container / 8)
            .is_some_and(|byte| byte >> (container % 8) & 1 == 1)
    }

    /// Absolute byte offset of this family's region within `physical_key`.
    fn base(&self) -> usize {
        self.physical_base_offset as usize + self.base_offset as usize
    }

    /// Absolute byte range of offsets unit `idx` (a row frame in native
    /// encoding, a container in external) within `physical_key`.
    pub fn abs_range(&self, idx: usize) -> (usize, usize) {
        let base = self.base();
        (
            base + self.offsets[idx] as usize,
            base + self.offsets[idx + 1] as usize,
        )
    }

    pub fn region_range(&self) -> (usize, usize) {
        let base = self.base();
        (base, base + *self.offsets.last().unwrap_or(&0) as usize)
    }

    pub fn physical_key_or<'a>(&'a self, default: &'a [u8]) -> &'a [u8] {
        if self.physical_key.is_empty() {
            default
        } else {
            &self.physical_key
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header: Self = decode_rlp(bytes, "invalid block blob header rlp")?;
        if !header.container_manifest_is_consistent() {
            return Err(MonadChainDataError::Decode(
                "block blob header container manifest is inconsistent",
            ));
        }
        Ok(header)
    }
}

/// Shared RLP decode body: `decode_exact` mapping any failure to a
/// [`MonadChainDataError::Decode`] carrying the call site's message.
fn decode_rlp<T: alloy_rlp::Decodable>(bytes: &[u8], err: &'static str) -> Result<T> {
    alloy_rlp::decode_exact(bytes).map_err(|_| MonadChainDataError::Decode(err))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct FamilyWindowRecord {
    pub first_primary_id: PrimaryId,
    pub count: u32,
}

impl FamilyWindowRecord {
    pub fn next_primary_id_exclusive(self) -> Result<PrimaryId> {
        self.first_primary_id.checked_add(u64::from(self.count))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct BlockRecord {
    pub block_number: u64,
    pub block_hash: Hash32,
    pub parent_hash: Hash32,
    pub logs: FamilyWindowRecord,
    pub txs: FamilyWindowRecord,
    /// Per-block trace family window. Its addition was a hard break of the
    /// on-disk RLP layout; pre-trace data dirs must be wiped and re-ingested.
    pub traces: FamilyWindowRecord,
    /// Head of the per-block content-digest chain (see [`crate::engine::digest`]):
    /// a standby re-deriving the chain from the same finalized blocks must reach
    /// this exact value. Its addition was a hard break of the on-disk RLP layout.
    pub row_chain: Hash32,
}

impl BlockRecord {
    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode_rlp(bytes, "invalid block record rlp")
    }
}

/// The single publication row: the reader-visible head watermark plus the
/// row chain at that head (see [`crate::engine::digest`]).
/// Field order is the on-disk RLP list order; reordering changes the wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct PublicationState {
    pub indexed_finalized_head: u64,
    /// Row chain at `indexed_finalized_head`, mirroring the
    /// published head block's [`BlockRecord::row_chain`].
    pub head_row_chain: Hash32,
}

impl PublicationState {
    pub fn encode(self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode_rlp(bytes, "invalid publication state rlp")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlockBlobHeader, Bytes, Hash32, PrimaryId, PublicationState, ENCODING_EXTERNAL_V1,
        ENCODING_NATIVE,
    };

    fn native_header() -> BlockBlobHeader {
        BlockBlobHeader {
            offsets: vec![0, 10, 25, 25, 40],
            dict_version: 3,
            base_offset: 7,
            physical_key: b"phys".to_vec(),
            physical_base_offset: 100,
            encoding: ENCODING_NATIVE,
            container_rows: Vec::new(),
            container_status: Bytes::new(),
        }
    }

    fn external_header() -> BlockBlobHeader {
        BlockBlobHeader {
            offsets: vec![0, 30, 30, 75],
            dict_version: 0,
            base_offset: 5,
            physical_key: b"receipts/000000000001".to_vec(),
            physical_base_offset: 0,
            encoding: ENCODING_EXTERNAL_V1,
            container_rows: vec![2, 2, 5],
            container_status: Bytes::from_static(&[0b101]),
        }
    }

    #[test]
    fn block_blob_header_round_trips_both_encodings() {
        for header in [native_header(), external_header()] {
            let decoded = BlockBlobHeader::decode(&header.encode()).expect("decode header");
            assert_eq!(decoded, header);
        }
    }

    #[test]
    fn native_header_counts_rows_from_offsets() {
        let header = native_header();
        assert!(!header.is_external());
        assert_eq!(header.row_count(), 4);
        assert_eq!(header.container_count(), 4);
        // Identity mapping when container_rows is empty.
        assert_eq!(header.container_of_row(2), (2, 0));
        assert_eq!(header.container_row_base(2), 2);
        assert_eq!(header.abs_range(1), (117, 132));
    }

    #[test]
    fn external_header_maps_rows_through_containers() {
        let header = external_header();
        assert!(header.is_external());
        assert_eq!(header.row_count(), 5);
        assert_eq!(header.container_count(), 3);
        // Container 0 holds rows 0..2, container 1 is empty, container 2
        // holds rows 2..5.
        assert_eq!(header.container_of_row(0), (0, 0));
        assert_eq!(header.container_of_row(1), (0, 1));
        assert_eq!(header.container_of_row(2), (2, 0));
        assert_eq!(header.container_of_row(4), (2, 2));
        assert_eq!(header.container_row_base(0), 0);
        assert_eq!(header.container_row_base(2), 2);
        assert!(header.container_status_bit(0));
        assert!(!header.container_status_bit(1));
        assert!(header.container_status_bit(2));
        // Bits beyond the vec read as false.
        assert!(!header.container_status_bit(64));
        assert_eq!(header.region_range(), (5, 80));
    }

    #[test]
    fn external_header_with_zero_containers_has_zero_rows() {
        let header = BlockBlobHeader {
            offsets: vec![0],
            container_rows: Vec::new(),
            container_status: Bytes::new(),
            ..external_header()
        };
        assert_eq!(header.row_count(), 0);
        assert_eq!(header.container_count(), 0);
    }

    #[test]
    fn external_header_container_row_len() {
        let header = external_header();
        assert_eq!(header.container_row_len(0), 2);
        assert_eq!(header.container_row_len(1), 0);
        assert_eq!(header.container_row_len(2), 3);
        // Identity mapping: always one row per container.
        assert_eq!(native_header().container_row_len(3), 1);
    }

    /// The read paths index and preallocate from the container manifest, so a
    /// corrupt metadata row must be rejected at decode, not panic later.
    #[test]
    fn header_decode_rejects_inconsistent_container_manifests() {
        let corruptions: Vec<(&str, fn(&mut BlockBlobHeader))> = vec![
            ("manifest shorter than the partition", |h| {
                h.container_rows = vec![5]
            }),
            ("manifest longer than the partition", |h| {
                h.container_rows = vec![1, 2, 3, 5]
            }),
            ("non-monotonic cumulative counts", |h| {
                h.container_rows = vec![5, 2, 5]
            }),
            ("row count exceeding the region bytes", |h| {
                h.container_rows = vec![2, 2, u32::MAX]
            }),
            ("status bitvec not sized for the containers", |h| {
                h.container_status = Bytes::from_static(&[0, 0])
            }),
            ("native header carrying a manifest", |h| {
                h.encoding = ENCODING_NATIVE;
                h.dict_version = 1;
            }),
        ];
        for (what, corrupt) in corruptions {
            let mut header = external_header();
            corrupt(&mut header);
            assert!(
                BlockBlobHeader::decode(&header.encode()).is_err(),
                "decode must reject: {what}"
            );
        }
    }

    #[test]
    fn primary_id_checked_add_rejects_overflow() {
        assert!(PrimaryId::new(u64::MAX).checked_add(1).is_err());
        assert_eq!(
            PrimaryId::new(7).checked_add(35).expect("no overflow"),
            PrimaryId::new(42)
        );
    }

    #[test]
    fn publication_state_round_trips() {
        let state = PublicationState {
            indexed_finalized_head: 91,
            head_row_chain: Hash32::repeat_byte(0xab),
        };
        let encoded = state.encode();
        let decoded = PublicationState::decode(&encoded).expect("decode state");
        assert_eq!(decoded, state);
    }
}
