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

use bytes::Bytes;

use super::super::{
    assignment::ChunkId,
    chunk::{ChunkData, InvalidChunk},
    types::MerkleRoot,
};

// decoded proposal does not reencode back to root
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidEncoding;

pub type Resolved = Result<Bytes, InvalidEncoding>;

// every chunk of one message, under their root
pub(crate) trait Encoded {
    fn root(&self) -> MerkleRoot;
    fn chunk_data(&self, chunk_id: ChunkId) -> ChunkData;
}

// the chunks received under one root, and the rule that finishes them
pub(crate) trait Decoder {
    type Encoded: Encoded;

    fn root(&self) -> MerkleRoot;
    fn contains(&self, chunk_id: ChunkId) -> bool;
    // None unless contained
    fn chunk_data(&self, chunk_id: ChunkId) -> Option<ChunkData>;

    // takes a chunk that binds to the root. the caller must pass a
    // chunk id not yet contained.
    fn ingest(&mut self, chunk_id: ChunkId, data: ChunkData) -> Result<(), InvalidChunk>;

    // once enough chunks arrived: the message they decode to, and its
    // encoding
    fn try_decode(&mut self) -> Option<(Bytes, Self::Encoded)>;
}

// the chunks of one proposal under its root: complete once encoded,
// received one by one until they decode
pub(crate) enum Chunks<D: Decoder> {
    Decoding(D),
    // any chunk is derivable
    Encoded(D::Encoded),
    // the received chunks decode to a message that does not encode to
    // the root
    Invalid(D),
}

// what an instance asks of a proposal's chunks, whatever their scheme
pub(crate) trait ProposalChunks: Send {
    // received, or derivable once decoded
    fn contains(&self, chunk_id: ChunkId) -> bool;

    // true when the chunk is new. nothing is new once resolved.
    fn ingest(&mut self, chunk_id: ChunkId, data: ChunkData) -> Result<bool, InvalidChunk>;

    // once: the message when enough chunks arrived, or invalid when it
    // does not encode to the root
    fn try_resolve(&mut self) -> Option<Resolved>;
    fn is_resolved(&self) -> bool;

    // None unless contained
    fn chunk_data(&self, chunk_id: ChunkId) -> Option<ChunkData>;
}

impl<D> ProposalChunks for Chunks<D>
where
    D: Decoder + Send,
    D::Encoded: Send,
{
    fn contains(&self, chunk_id: ChunkId) -> bool {
        match self {
            Self::Decoding(decoder) | Self::Invalid(decoder) => decoder.contains(chunk_id),
            Self::Encoded(_) => true,
        }
    }

    fn ingest(&mut self, chunk_id: ChunkId, data: ChunkData) -> Result<bool, InvalidChunk> {
        let Self::Decoding(decoder) = self else {
            return Ok(false);
        };
        if decoder.contains(chunk_id) {
            return Ok(false);
        }
        decoder.ingest(chunk_id, data)?;
        Ok(true)
    }

    fn try_resolve(&mut self) -> Option<Resolved> {
        let Self::Decoding(decoder) = self else {
            return None;
        };
        let (message, encoded) = decoder.try_decode()?;
        if encoded.root() == decoder.root() {
            *self = Self::Encoded(encoded);
            return Some(Ok(message));
        }

        // the decoder moves to Invalid, with the encoding in its place
        // for the moment
        let Self::Decoding(decoder) = std::mem::replace(self, Self::Encoded(encoded)) else {
            unreachable!("the chunks are decoding");
        };
        *self = Self::Invalid(decoder);
        Some(Err(InvalidEncoding))
    }

    fn is_resolved(&self) -> bool {
        !matches!(self, Self::Decoding(_))
    }

    fn chunk_data(&self, chunk_id: ChunkId) -> Option<ChunkData> {
        match self {
            Self::Decoding(decoder) | Self::Invalid(decoder) => decoder.chunk_data(chunk_id),
            Self::Encoded(encoded) => {
                let data = encoded.chunk_data(chunk_id);
                Some(data)
            }
        }
    }
}
