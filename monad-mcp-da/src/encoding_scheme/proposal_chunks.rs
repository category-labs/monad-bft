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

use std::sync::Arc;

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

// an encoding shared, as between the author's proposal and its own
// chunks
impl<E: Encoded + ?Sized> Encoded for Arc<E> {
    fn root(&self) -> MerkleRoot {
        (**self).root()
    }

    fn chunk_data(&self, chunk_id: ChunkId) -> ChunkData {
        (**self).chunk_data(chunk_id)
    }
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

    // takes a chunk as ingest does, without checking it. the caller must
    // ensure it binds to the root, and pass a chunk id not yet
    // contained.
    fn hold(&mut self, chunk_id: ChunkId, data: ChunkData);

    // whether try_decode would decode the chunks held
    fn decodes(&self) -> bool;

    // once enough chunks arrived: the message they decode to, and its
    // encoding
    fn try_decode(&mut self) -> Option<(Bytes, Self::Encoded)>;
}

// the chunks of one proposal under its root: complete once encoded,
// received one by one until they decode
pub(crate) enum Chunks<D: Decoder> {
    Decoding(D),
    // a proposal received as any other, but with its author's message and
    // encoding known: our own, or in a simulation one shared by its
    // author in this process (see shared)
    Authored(Authored<D>),
    // any chunk is derivable
    Encoded(Arc<D::Encoded>),
    // the received chunks decode to a message that does not encode to
    // the root
    Invalid(D),
}

// Chunks whose author's encoding is known: the author's own (they
// arrive as its first hop), or, in a simulation, a receiver's of an
// encoding its author shared (see shared). They resolve when the
// decoder's rule says they decode, so events and their order are a
// receiver's. A chunk equal to its known one binds
// to the root, as the known encoding is under it, and is held without a
// check; any other chunk is checked. The message and encoding are taken
// as known, without decoding or re-encoding.
pub(crate) struct Authored<D: Decoder> {
    decoder: D,
    message: Bytes,
    encoded: Arc<D::Encoded>,
}

impl<D: Decoder> Chunks<D> {
    // the caller must ensure `encoded` encodes `message` under the
    // decoder's root
    pub(crate) fn authored(decoder: D, message: Bytes, encoded: Arc<D::Encoded>) -> Self {
        assert_eq!(
            decoder.root(),
            encoded.root(),
            "our encoding is under our root"
        );
        Self::Authored(Authored {
            decoder,
            message,
            encoded,
        })
    }
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
    D::Encoded: Send + Sync,
{
    fn contains(&self, chunk_id: ChunkId) -> bool {
        match self {
            Self::Decoding(decoder) | Self::Invalid(decoder) => decoder.contains(chunk_id),
            Self::Authored(authored) => authored.decoder.contains(chunk_id),
            Self::Encoded(_) => true,
        }
    }

    fn ingest(&mut self, chunk_id: ChunkId, data: ChunkData) -> Result<bool, InvalidChunk> {
        match self {
            Self::Decoding(decoder) => {
                if decoder.contains(chunk_id) {
                    return Ok(false);
                }
                decoder.ingest(chunk_id, data)?;
            }
            Self::Authored(Authored {
                decoder, encoded, ..
            }) => {
                if decoder.contains(chunk_id) {
                    return Ok(false);
                }
                if encoded.chunk_data(chunk_id) == data {
                    decoder.hold(chunk_id, data);
                } else {
                    decoder.ingest(chunk_id, data)?;
                }
            }
            Self::Encoded(_) | Self::Invalid(_) => return Ok(false),
        }
        Ok(true)
    }

    fn try_resolve(&mut self) -> Option<Resolved> {
        if let Self::Authored(authored) = self {
            if !authored.decoder.decodes() {
                return None;
            }
            // what the decoder would resolve to, every chunk held being
            // bound to the root our encoding is under
            debug_assert!(
                authored
                    .decoder
                    .try_decode()
                    .is_some_and(|(message, encoded)| {
                        message == authored.message && encoded.root() == authored.encoded.root()
                    })
            );
            let message = authored.message.clone();
            *self = Self::Encoded(authored.encoded.clone());
            return Some(Ok(message));
        }

        let Self::Decoding(decoder) = self else {
            return None;
        };
        let (message, encoded) = decoder.try_decode()?;
        if encoded.root() == decoder.root() {
            *self = Self::Encoded(Arc::new(encoded));
            return Some(Ok(message));
        }

        // the decoder moves to Invalid, with the encoding in its place
        // for the moment
        let encoded = Self::Encoded(Arc::new(encoded));
        let Self::Decoding(decoder) = std::mem::replace(self, encoded) else {
            unreachable!("the chunks are decoding");
        };
        *self = Self::Invalid(decoder);
        Some(Err(InvalidEncoding))
    }

    fn is_resolved(&self) -> bool {
        matches!(self, Self::Encoded(_) | Self::Invalid(_))
    }

    fn chunk_data(&self, chunk_id: ChunkId) -> Option<ChunkData> {
        match self {
            Self::Decoding(decoder) | Self::Invalid(decoder) => decoder.chunk_data(chunk_id),
            Self::Authored(authored) => authored.decoder.chunk_data(chunk_id),
            Self::Encoded(encoded) => {
                let data = encoded.chunk_data(chunk_id);
                Some(data)
            }
        }
    }
}
