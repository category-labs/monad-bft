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
//! Each row in a block blob is its own self-delimiting zstd frame so a
//! single-row point read only has to decompress that one frame. All rows in a
//! family share a versioned dictionary; `dict_version == 0` is the bootstrap
//! sentinel meaning "plain zstd frames, no dictionary". The block header's
//! `offsets[]` delimit the compressed frames.
//!
//! [`RowCodec`] is an immutable per-batch snapshot of the write-side state
//! (dictionary version + prepared encoder dictionary + level). Ingest workers
//! share one snapshot via `Arc` across the rayon `par_iter` so a mid-batch
//! dictionary hot-swap can never tear a single batch. The matching read-side
//! decode helper lives on [`crate::engine::tables::Tables`].

use std::sync::Arc;

use bytes::Bytes;
use zstd::{
    bulk::{Compressor, Decompressor},
    dict::{DecoderDictionary, EncoderDictionary},
};

use crate::error::{MonadChainDataError, Result};

/// Bootstrap dictionary version: plain zstd frames with no shared dictionary.
/// Fully functional from the first block; blocks written under it never depend
/// on any dictionary bytes existing in the dict table.
pub const DICT_VERSION_NONE: u32 = 0;

/// Default zstd level used for row frames.
pub const ROW_ZSTD_LEVEL: i32 = 3;

/// Upper bound on a single decompressed row. The safe bulk API needs an
/// explicit capacity; rows are individual logs/txs/traces so this is generous.
pub const ROW_DECODE_CAP: usize = 16 * 1024 * 1024;

/// Cap on raw row samples collected from a single block for dict training, so
/// a pathologically large block cannot dominate the reservoir. Rows beyond
/// this many are uniformly strided.
pub const PER_BLOCK_SAMPLE_CAP: usize = 64;

/// Picks up to [`PER_BLOCK_SAMPLE_CAP`] indices uniformly across `count` rows.
/// Returns `true` for indices that should be sampled into the training
/// reservoir during a single block's encode pass.
pub fn should_sample_row(idx: usize, count: usize) -> bool {
    if count <= PER_BLOCK_SAMPLE_CAP {
        return true;
    }
    // Uniform stride; guaranteed >= 1 because count > PER_BLOCK_SAMPLE_CAP.
    let stride = count / PER_BLOCK_SAMPLE_CAP;
    idx.is_multiple_of(stride)
}

/// Immutable write-side codec state for one family. Shared behind an `Arc` and
/// read once per ingest batch (off the per-row path).
pub struct RowCodecState {
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
    pub fn plain(version: u32) -> Self {
        Self {
            version,
            encoder: None,
            level: ROW_ZSTD_LEVEL,
        }
    }

    /// Builds a dictionary-backed state from raw dict bytes.
    pub fn with_dictionary(version: u32, dict_bytes: &[u8], level: i32) -> Self {
        Self {
            version,
            encoder: Some(Arc::new(EncoderDictionary::copy(dict_bytes, level))),
            level,
        }
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    /// Cheap snapshot for one ingest batch.
    pub fn snapshot(self: &Arc<Self>) -> RowCodec {
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
    /// rows via [`BlockRowCompressor::compress_row`].
    pub fn block_compressor(&self) -> Result<BlockRowCompressor<'_>> {
        let compressor = match &self.state.encoder {
            Some(enc) => Compressor::with_prepared_dictionary(enc)
                .map_err(|e| MonadChainDataError::Backend(format!("zstd compressor: {e}")))?,
            None => Compressor::new(self.state.level)
                .map_err(|e| MonadChainDataError::Backend(format!("zstd compressor: {e}")))?,
        };
        Ok(BlockRowCompressor { compressor })
    }
}

/// A zstd compressor bound to one block's dictionary, reused across its rows.
/// The borrow keeps the prepared dictionary (owned by [`RowCodec`]) alive.
pub struct BlockRowCompressor<'a> {
    compressor: Compressor<'a>,
}

impl BlockRowCompressor<'_> {
    /// Compresses one already-RLP-encoded row into its own zstd frame.
    pub fn compress_row(&mut self, row: &[u8]) -> Result<Vec<u8>> {
        self.compressor
            .compress(row)
            .map_err(|e| MonadChainDataError::Backend(format!("zstd compress row: {e}")))
    }
}

/// Decompresses a single row frame. The supplied decoder is authoritative:
/// `Some` means the frame was written under a non-empty dictionary, `None`
/// means a plain (dict-less) frame — version 0, or a version `V >= 1` whose
/// epoch published the empty-dict sentinel. The caller (the read-side
/// resolver) is responsible for mapping `(family, version)` to the right
/// decoder and for treating an *absent* published dictionary as a hard error
/// before reaching here.
pub fn decode_row_frame(
    decoder: Option<&Arc<DecoderDictionary<'static>>>,
    frame: &[u8],
) -> Result<Bytes> {
    let mut decompressor = match decoder {
        Some(dec) => Decompressor::with_prepared_dictionary(dec)
            .map_err(|e| MonadChainDataError::Backend(format!("zstd decompressor: {e}")))?,
        None => Decompressor::new()
            .map_err(|e| MonadChainDataError::Backend(format!("zstd decompressor: {e}")))?,
    };
    let decoded = decompressor
        .decompress(frame, ROW_DECODE_CAP)
        .map_err(|e| MonadChainDataError::Backend(format!("zstd decompress row: {e}")))?;
    Ok(Bytes::from(decoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn train_dict(samples: &[Vec<u8>]) -> Vec<u8> {
        zstd::dict::from_samples(samples, 16 * 1024).expect("train dict")
    }

    #[test]
    fn version_zero_round_trip() {
        let state = Arc::new(RowCodecState::bootstrap());
        let codec = state.snapshot();
        assert_eq!(codec.version(), DICT_VERSION_NONE);
        let mut block = codec.block_compressor().expect("compressor");

        let row = b"hello row payload that is reasonably sized for zstd".to_vec();
        let frame = block.compress_row(&row).expect("compress");
        let decoded = decode_row_frame(None, &frame).expect("decode");
        assert_eq!(decoded.as_ref(), row.as_slice());
    }

    #[test]
    fn plain_version_stamps_version_but_uses_plain_frames() {
        // The empty-dict sentinel: version > 0 but dict-less plain frames.
        let state = Arc::new(RowCodecState::plain(3));
        let codec = state.snapshot();
        assert_eq!(codec.version(), 3);
        let mut block = codec.block_compressor().expect("compressor");
        let row = b"sentinel plain frame payload padding padding".to_vec();
        let frame = block.compress_row(&row).expect("compress");
        // Decoded with no dictionary because the sentinel means "plain".
        let decoded = decode_row_frame(None, &frame).expect("decode");
        assert_eq!(decoded.as_ref(), row.as_slice());
    }

    #[test]
    fn dictionary_round_trip() {
        // Build a corpus with shared structure so a dictionary is trainable.
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
            let frame = block.compress_row(row).expect("compress");
            let decoded = decode_row_frame(Some(&decoder), &frame).expect("decode");
            assert_eq!(decoded.as_ref(), row.as_slice());
        }
    }

    #[test]
    fn empty_row_round_trips() {
        let state = Arc::new(RowCodecState::bootstrap());
        let codec = state.snapshot();
        let mut block = codec.block_compressor().expect("compressor");
        let frame = block.compress_row(&[]).expect("compress empty");
        let decoded = decode_row_frame(None, &frame).expect("decode empty");
        assert!(decoded.is_empty());
    }
}
