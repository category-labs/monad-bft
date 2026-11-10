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

//! The chunks of a d33 proposal: Reed-Solomon symbols, each a leaf of
//! one merkle tree.

use std::collections::BTreeMap;

use bytes::Bytes;
use monad_crypto::hasher::{Hash, HasherType};

use super::super::{
    super::{
        assignment::ChunkId,
        chunk::{ChunkData, WireChunkId},
        types::{D33, MerkleRoot},
        wire::{DAHeaderScheme as _, PacketLayout as _},
    },
    BadProof, DAEncodingScheme as _, Decoder, Encoded,
    merkle::Tree,
    reed_solomon::ReedSolomon,
};

// every symbol of a message under its tree, so any chunk is derivable
pub(crate) struct D33Encoded {
    // in chunk id order
    symbols: Vec<Bytes>,
    tree: Tree,
}

impl D33Encoded {
    // None unless the message is of the scheme's length
    pub(crate) fn encode(d33: &D33, message: &Bytes, num_chunks: usize) -> Option<Self> {
        if message.is_empty() || message.len() != d33.msg_len() {
            return None;
        }
        let symbol_len = d33.v1_layout().symbol_len();
        let code = ReedSolomon::new(num_chunks, d33.num_source_chunks());
        let symbols = code.encode(message, symbol_len);
        assert!(symbols.len() == num_chunks);
        let encoded = Self::from_symbols(d33, symbols);
        Some(encoded)
    }

    // the tree over any symbols. the caller must pass them in chunk id
    // order.
    pub(crate) fn from_symbols(d33: &D33, symbols: Vec<Bytes>) -> Self {
        let mut leaves = Vec::with_capacity(symbols.len());
        for (leaf_idx, symbol) in symbols.iter().enumerate() {
            leaves.push(leaf_hash(d33, leaf_idx as WireChunkId, symbol));
        }
        let tree = Tree::from_leaves(d33.depth(), leaves);
        Self { symbols, tree }
    }
}

impl Encoded for D33Encoded {
    fn root(&self) -> MerkleRoot {
        self.tree.root()
    }

    fn chunk_data(&self, chunk_id: ChunkId) -> ChunkData {
        ChunkData {
            symbol: self.symbols[usize::from(chunk_id)].clone(),
            proof: self.tree.proof(chunk_id.to_wire()),
        }
    }
}

// the chunks received under a d33 root. they decode once they are any
// num_source_chunks of the proposal.
pub(crate) struct D33Decoder {
    d33: D33,
    root: MerkleRoot,
    num_chunks: usize,
    num_source_chunks: usize,
    received: BTreeMap<ChunkId, ChunkData>,
}

impl D33Decoder {
    pub(crate) fn new(d33: D33, root: MerkleRoot, num_chunks: usize) -> Self {
        let num_source_chunks = d33.num_source_chunks();
        assert!(0 < num_source_chunks && num_source_chunks < num_chunks);
        Self {
            d33,
            root,
            num_chunks,
            num_source_chunks,
            received: BTreeMap::new(),
        }
    }
}

impl Decoder for D33Decoder {
    type Encoded = D33Encoded;

    fn root(&self) -> MerkleRoot {
        self.root
    }

    fn contains(&self, chunk_id: ChunkId) -> bool {
        self.received.contains_key(&chunk_id)
    }

    fn chunk_data(&self, chunk_id: ChunkId) -> Option<ChunkData> {
        self.received.get(&chunk_id).cloned()
    }

    fn ingest(&mut self, chunk_id: ChunkId, data: ChunkData) -> Result<(), BadProof> {
        if !verify_proof(chunk_id, &data, &self.d33, &self.root) {
            return Err(BadProof);
        }
        self.received.insert(chunk_id, data);
        Ok(())
    }

    fn try_decode(&mut self) -> Option<(Bytes, D33Encoded)> {
        if self.received.len() < self.num_source_chunks {
            return None;
        }
        let symbols = self
            .received
            .iter()
            .map(|(chunk_id, data)| (*chunk_id, &data.symbol));
        let code = ReedSolomon::new(self.num_chunks, self.num_source_chunks);
        // todo: None after enough chunks means the codec doesn't
        // guarantee decodability (e.g. r10).
        let message = code.decode(symbols, self.d33.msg_len())?;

        let encoded = D33Encoded::encode(&self.d33, &message, self.num_chunks);
        let encoded = encoded.expect("a decoded message is of the scheme's length");
        Some((message, encoded))
    }
}

// the message length makes the root pin the decoded message, not only the symbols
fn leaf_hash(d33: &D33, chunk_id: WireChunkId, symbol: &[u8]) -> Hash {
    use monad_crypto::hasher::Hasher as _;

    let mut hasher = HasherType::new();
    hasher.update(d33.msg_len.to_le_bytes());
    hasher.update(chunk_id.to_le_bytes());
    hasher.update(symbol);
    hasher.hash()
}

// whether the chunk's proof binds it to the root
fn verify_proof(chunk_id: ChunkId, data: &ChunkData, d33: &D33, root: &MerkleRoot) -> bool {
    debug_assert_eq!(data.symbol.len(), d33.v1_layout().symbol_len());
    let leaf_idx = chunk_id.to_wire();
    let leaf = leaf_hash(d33, leaf_idx, &data.symbol);
    Tree::verify_proof(root, leaf_idx, &leaf, &data.proof)
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            super::{
                super::{
                    test_util::{MESSAGE_LEN, SLOT},
                    types::Lane,
                },
                Chunks, ProposalChunks as _,
            },
            for_message,
        },
        *,
    };

    // the default fixture's scheme: 2 source chunks
    fn scheme() -> D33 {
        for_message(SLOT, MESSAGE_LEN, 0, Lane::new_unchecked(0), 4).expect("fits")
    }

    fn message() -> Bytes {
        Bytes::from(vec![7u8; MESSAGE_LEN])
    }

    fn encoded() -> D33Encoded {
        D33Encoded::encode(&scheme(), &message(), 9).expect("of the scheme's length")
    }

    #[test]
    fn an_encoding_derives_every_chunk_with_its_proof() {
        let scheme = scheme();
        let encoded = encoded();
        for wire in 0..9 {
            let chunk_id = ChunkId::unchecked(wire);
            let data = encoded.chunk_data(chunk_id);
            assert_eq!(data.symbol.len(), scheme.v1_layout().symbol_len());
            assert!(verify_proof(chunk_id, &data, &scheme, &encoded.root()));
        }

        let short = message().slice(1..);
        assert!(D33Encoded::encode(&scheme, &short, 9).is_none());
    }

    #[test]
    fn received_chunks_are_held_until_decoded_then_every_chunk_is() {
        let encoded = encoded();
        let chunk = |wire| {
            let chunk_id = ChunkId::unchecked(wire);
            (chunk_id, encoded.chunk_data(chunk_id))
        };
        let decoder = D33Decoder::new(scheme(), encoded.root(), 9);
        let mut chunks = Chunks::Decoding(decoder);

        let (eight, data) = chunk(8);
        assert_eq!(chunks.ingest(eight, data.clone()), Ok(true));
        assert_eq!(chunks.ingest(eight, data), Ok(false));
        assert!(chunks.contains(eight));
        let (zero, zero_data) = chunk(0);
        assert!(!chunks.contains(zero));
        assert_eq!(chunks.chunk_data(zero), None);
        assert_eq!(chunks.try_resolve(), None);

        let (five, data) = chunk(5);
        assert_eq!(chunks.ingest(five, data), Ok(true));
        assert_eq!(chunks.try_resolve(), Some(Ok(message())));
        assert_eq!(chunks.try_resolve(), None);
        assert!(chunks.is_resolved());

        // every chunk is derivable, and nothing is new
        assert!(chunks.contains(zero));
        assert_eq!(chunks.ingest(zero, zero_data), Ok(false));
    }
}
