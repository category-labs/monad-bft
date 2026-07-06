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

//! Row-level zstd codec for per-block blobs.
//!
//! Each row is its own self-delimiting zstd frame (delimited by the block
//! header's `offsets[]`) so a point read decompresses one frame. Rows share a
//! versioned per-family dictionary; `dict_version == 0` means plain frames.
//!
//! [`RowCodec`] is an immutable per-batch snapshot of the write-side state,
//! `Arc`-shared across ingest workers so a mid-batch dictionary hot-swap can
//! never tear a batch. Read-side decode lives on [`crate::tables::Tables`].

use std::{io::Cursor, sync::Arc};

use bytes::Bytes;
use monad_query_errors::{QueryError, Result};
use monad_query_primitives::records::{BlockBlobHeader, ENCODING_NATIVE};
use zstd::{
    bulk::{Compressor, Decompressor},
    dict::{DecoderDictionary, EncoderDictionary},
};

use crate::digest::{ChainDigest, RowDigest};

/// Bootstrap dictionary version: plain zstd frames, no dependency on any
/// dictionary bytes existing in the dict table.
pub const DICT_VERSION_NONE: u32 = 0;

/// Default zstd level used for row frames.
pub const ROW_ZSTD_LEVEL: i32 = 3;

/// Fallback decompressed-size bound, used only when a frame lacks a content
/// size; frames written by [`BlockRowCompressor`] always record it.
pub(crate) const ROW_DECODE_CAP: usize = 16 * 1024 * 1024;

/// Cap on rows sampled per block for the dict-training corpus, so one huge
/// block cannot dominate it.
pub const PER_BLOCK_SAMPLE_CAP: usize = 64;

/// Picks up to [`PER_BLOCK_SAMPLE_CAP`] indices uniformly across `count` rows
/// for the dict-training corpus.
pub fn should_sample_row(idx: usize, count: usize) -> bool {
    if count <= PER_BLOCK_SAMPLE_CAP {
        return true;
    }
    // Uniform stride, rounded UP so at most PER_BLOCK_SAMPLE_CAP indices are
    // multiples of it (floor division would select every row for counts in
    // 65..=127). Index 0 is always sampled.
    let stride = count.div_ceil(PER_BLOCK_SAMPLE_CAP);
    idx.is_multiple_of(stride)
}

/// Immutable write-side codec state for one family. Shared behind an `Arc` and
/// read once per ingest batch (off the per-row path).
pub(crate) struct RowCodecState {
    version: u32,
    /// Prepared encoder dictionary, `None` for [`DICT_VERSION_NONE`].
    encoder: Option<Arc<EncoderDictionary<'static>>>,
    level: i32,
}

impl RowCodecState {
    /// The bootstrap state: version 0, dict-less plain zstd frames.
    pub fn bootstrap() -> Self {
        Self::plain(DICT_VERSION_NONE)
    }

    /// A dict-less ("plain") state that still stamps `version` onto headers.
    /// Used for version 0 (bootstrap) and for the empty-dict sentinel of a
    /// version `V >= 1` whose epoch did not produce a useful dictionary.
    pub(crate) fn plain(version: u32) -> Self {
        Self {
            version,
            encoder: None,
            level: ROW_ZSTD_LEVEL,
        }
    }

    /// Builds a dictionary-backed state from raw dict bytes.
    pub(crate) fn with_dictionary(version: u32, dict_bytes: &[u8], level: i32) -> Self {
        Self {
            version,
            encoder: Some(Arc::new(EncoderDictionary::copy(dict_bytes, level))),
            level,
        }
    }

    /// Cheap snapshot for one ingest batch.
    pub(crate) fn snapshot(self: &Arc<Self>) -> RowCodec {
        RowCodec {
            state: Arc::clone(self),
        }
    }
}

/// Per-batch snapshot handed to ingest workers. Cheaply cloneable (`Arc`).
#[derive(Clone)]
pub struct RowCodec {
    state: Arc<RowCodecState>,
}

impl RowCodec {
    /// The dictionary version every row frame in this batch is encoded under;
    /// callers stamp it onto the block header.
    pub fn version(&self) -> u32 {
        self.state.version
    }

    /// Builds one compressor for an entire block; reuse it across that block's
    /// rows via [`BlockRowCompressor::compress_row_into`].
    pub(crate) fn block_compressor(&self) -> Result<BlockRowCompressor<'_>> {
        let compressor = match &self.state.encoder {
            Some(enc) => Compressor::with_prepared_dictionary(enc),
            None => Compressor::new(self.state.level),
        }
        .map_err(|e| QueryError::Backend(format!("zstd compressor: {e}")))?;
        Ok(BlockRowCompressor { compressor })
    }
}

/// Frames one block's rows into the per-family blob: per-row `offsets`, zstd
/// frames, and the digest over the *uncompressed* payloads (so the checksum is
/// zstd-independent). `too_large` is the family-specific error for a blob
/// exceeding the `u32` offset space.
pub fn encode_block_rows<T>(
    rows: &[T],
    codec: &RowCodec,
    too_large: &'static str,
    mut encode_row: impl FnMut(&T) -> Vec<u8>,
) -> Result<(BlockBlobHeader, Vec<u8>, ChainDigest)> {
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    let mut blob = Vec::new();
    let mut rows_digest = RowDigest::new();
    let mut compressor = codec.block_compressor()?;
    let offset = |len: usize| u32::try_from(len).map_err(|_| QueryError::Decode(too_large));

    for row in rows {
        offsets.push(offset(blob.len())?);
        let raw = encode_row(row);
        rows_digest.row(&raw);
        compressor.compress_row_into(&raw, &mut blob)?;
    }

    offsets.push(offset(blob.len())?);

    Ok((
        BlockBlobHeader {
            offsets,
            dict_version: codec.version(),
            base_offset: 0,
            physical_key: Vec::new(),
            physical_base_offset: 0,
            encoding: ENCODING_NATIVE,
            container_rows: Vec::new(),
            container_status: Bytes::new(),
        },
        blob,
        rows_digest.finish(),
    ))
}

/// The digest half of [`encode_block_rows`] alone: folds the uncompressed row
/// RLP without compressing or retaining anything. External-payload ingest uses
/// this so its `row_chain` stays byte-identical to a native ingest of the same
/// blocks.
pub fn digest_block_rows<T>(rows: &[T], mut encode_row: impl FnMut(&T) -> Vec<u8>) -> ChainDigest {
    let mut rows_digest = RowDigest::new();
    for row in rows {
        rows_digest.row(&encode_row(row));
    }
    rows_digest.finish()
}

/// A zstd compressor bound to one block's dictionary, reused across its rows.
/// The borrow keeps the prepared dictionary (owned by [`RowCodec`]) alive.
pub(crate) struct BlockRowCompressor<'a> {
    compressor: Compressor<'a>,
}

impl BlockRowCompressor<'_> {
    /// Compresses one already-RLP-encoded row into its own zstd frame, appending
    /// the frame directly to `out`.
    pub(crate) fn compress_row_into(&mut self, row: &[u8], out: &mut Vec<u8>) -> Result<()> {
        let start = out.len();
        out.reserve(zstd::zstd_safe::compress_bound(row.len()));
        let mut cursor = Cursor::new(out);
        cursor.set_position(start as u64);
        self.compressor
            .compress_to_buffer(row, &mut cursor)
            .map(|_| ())
            .map_err(|e| QueryError::Backend(format!("zstd compress row: {e}")))
    }
}

/// A zstd decompressor reused across one block's rows (allocates the `DCtx`
/// once). The supplied decoder is authoritative: `Some` means dictionary
/// frames, `None` means plain frames; mapping `(family, version)` to the right
/// decoder is the read-side resolver's job before reaching here.
pub(crate) struct RowDecompressor<'a> {
    inner: Decompressor<'a>,
}

impl<'a> RowDecompressor<'a> {
    pub(crate) fn new(decoder: Option<&'a Arc<DecoderDictionary<'static>>>) -> Result<Self> {
        let inner = match decoder {
            Some(dec) => Decompressor::with_prepared_dictionary(dec),
            None => Decompressor::new(),
        }
        .map_err(|e| QueryError::Backend(format!("zstd decompressor: {e}")))?;
        Ok(Self { inner })
    }

    /// Decompresses one row frame, sizing the buffer from the frame header's
    /// content size rather than a worst-case bound.
    pub(crate) fn decompress(&mut self, frame: &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(frame_decompressed_capacity(frame));
        self.inner
            .decompress_to_buffer(frame, &mut out)
            .map_err(|e| QueryError::Backend(format!("zstd decompress row: {e}")))?;
        Ok(out)
    }
}

/// Exact decompressed length from the frame header, falling back to
/// [`ROW_DECODE_CAP`] only if the size is absent.
fn frame_decompressed_capacity(frame: &[u8]) -> usize {
    match zstd::zstd_safe::get_frame_content_size(frame) {
        Ok(Some(size)) => usize::try_from(size).unwrap_or(ROW_DECODE_CAP),
        _ => ROW_DECODE_CAP,
    }
}

/// Decompresses a single row frame (one-shot — allocates a `DCtx`). To decode
/// many rows of one block, use [`RowDecompressor`] to reuse the context.
pub fn decode_row_frame(
    decoder: Option<&Arc<DecoderDictionary<'static>>>,
    frame: &[u8],
) -> Result<Bytes> {
    Ok(Bytes::from(
        RowDecompressor::new(decoder)?.decompress(frame)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn train_dict(samples: &[Vec<u8>]) -> Vec<u8> {
        zstd::dict::from_samples(samples, 16 * 1024).expect("train dict")
    }

    /// Test helper: compress one row into a fresh frame via the production
    /// `compress_row_into` entry point.
    fn compress_row(block: &mut BlockRowCompressor<'_>, row: &[u8]) -> Result<Vec<u8>> {
        let mut frame = Vec::new();
        block.compress_row_into(row, &mut frame)?;
        Ok(frame)
    }

    #[test]
    fn should_sample_row_respects_per_block_cap() {
        // Boundary counts around the cap plus a large block; the cap must hold
        // for every count and index 0 must always be sampled.
        for count in [1, 64, 65, 127, 128, 129, 10_000] {
            let sampled = (0..count)
                .filter(|&idx| should_sample_row(idx, count))
                .count();
            assert!(sampled > 0, "count {count}: nothing sampled");
            assert!(
                sampled <= PER_BLOCK_SAMPLE_CAP,
                "count {count}: sampled {sampled} rows, cap is {PER_BLOCK_SAMPLE_CAP}"
            );
            assert!(should_sample_row(0, count), "count {count}: idx 0 skipped");
        }
        // At or below the cap every row is sampled.
        assert_eq!(
            (0..64).filter(|&idx| should_sample_row(idx, 64)).count(),
            64
        );
    }

    #[test]
    fn version_zero_round_trip() {
        let state = Arc::new(RowCodecState::bootstrap());
        let codec = state.snapshot();
        assert_eq!(codec.version(), DICT_VERSION_NONE);
        let mut block = codec.block_compressor().expect("compressor");

        let row = b"hello row payload that is reasonably sized for zstd".to_vec();
        let frame = compress_row(&mut block, &row).expect("compress");
        let decoded = decode_row_frame(None, &frame).expect("decode");
        assert_eq!(decoded.as_ref(), row.as_slice());
    }

    #[test]
    fn plain_version_stamps_version_but_uses_plain_frames() {
        let state = Arc::new(RowCodecState::plain(3));
        let codec = state.snapshot();
        assert_eq!(codec.version(), 3);
        let mut block = codec.block_compressor().expect("compressor");
        let row = b"sentinel plain frame payload padding padding".to_vec();
        let frame = compress_row(&mut block, &row).expect("compress");
        let decoded = decode_row_frame(None, &frame).expect("decode");
        assert_eq!(decoded.as_ref(), row.as_slice());
    }

    #[test]
    fn dictionary_round_trip() {
        let samples: Vec<Vec<u8>> = (0..256u32)
            .map(|i| {
                let mut v = b"common-prefix-addr-0xabcdef-topic0-".to_vec();
                v.extend_from_slice(&i.to_le_bytes());
                v.extend_from_slice(b"-common-suffix-padding-padding-padding");
                v
            })
            .collect();
        let dict = train_dict(&samples);
        assert!(!dict.is_empty());

        let state = Arc::new(RowCodecState::with_dictionary(1, &dict, ROW_ZSTD_LEVEL));
        let codec = state.snapshot();
        assert_eq!(codec.version(), 1);
        let mut block = codec.block_compressor().expect("compressor");

        let decoder = Arc::new(DecoderDictionary::copy(&dict));
        for row in &samples {
            let frame = compress_row(&mut block, row).expect("compress");
            let decoded = decode_row_frame(Some(&decoder), &frame).expect("decode");
            assert_eq!(decoded.as_ref(), row.as_slice());
        }
    }

    #[test]
    fn empty_row_round_trips() {
        let state = Arc::new(RowCodecState::bootstrap());
        let codec = state.snapshot();
        let mut block = codec.block_compressor().expect("compressor");
        let frame = compress_row(&mut block, &[]).expect("compress empty");
        let decoded = decode_row_frame(None, &frame).expect("decode empty");
        assert!(decoded.is_empty());
    }
}
