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
    pub(crate) fn encode(d33: &D33, message: &[u8], num_chunks: usize) -> Option<Self> {
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
        Self::from_symbols_with(d33, symbols, [])
    }

    // from_symbols, taking a symbol's leaf hash from `known` where it
    // holds one for exactly that symbol. The caller must pass `known` in
    // ascending leaf index order, and each hash must be the leaf hash of
    // its symbol at its index.
    fn from_symbols_with<'a>(
        d33: &D33,
        symbols: Vec<Bytes>,
        known: impl IntoIterator<Item = (WireChunkId, &'a Bytes, Hash)>,
    ) -> Self {
        let mut known = known.into_iter().peekable();
        let mut leaves = Vec::with_capacity(symbols.len());
        for (leaf_idx, symbol) in symbols.iter().enumerate() {
            let leaf_idx = leaf_idx as WireChunkId;
            let known = known.next_if(|(known_idx, _, _)| *known_idx <= leaf_idx);
            let reused = known.and_then(|(known_idx, known_symbol, hash)| {
                (known_idx == leaf_idx && known_symbol == symbol).then_some(hash)
            });
            let leaf = match reused {
                Some(hash) => {
                    debug_assert_eq!(hash, leaf_hash(d33, leaf_idx, symbol));
                    hash
                }
                None => leaf_hash(d33, leaf_idx, symbol),
            };
            leaves.push(leaf);
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
    received: BTreeMap<ChunkId, Received>,
}

// a chunk whose proof checked, and the leaf hash the check computed from
// its symbol
struct Received {
    data: ChunkData,
    leaf: Hash,
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
        let received = self.received.get(&chunk_id)?;
        Some(received.data.clone())
    }

    fn ingest(&mut self, chunk_id: ChunkId, data: ChunkData) -> Result<(), BadProof> {
        let Some(leaf) = verify_proof(chunk_id, &data, &self.d33, &self.root) else {
            return Err(BadProof);
        };
        self.received.insert(chunk_id, Received { data, leaf });
        Ok(())
    }

    fn try_decode(&mut self) -> Option<(Bytes, D33Encoded)> {
        if self.received.len() < self.num_source_chunks {
            return None;
        }
        let symbols = self
            .received
            .iter()
            .map(|(chunk_id, received)| (*chunk_id, &received.data.symbol));
        let code = ReedSolomon::new(self.num_chunks, self.num_source_chunks);
        // todo: None after enough chunks means the codec doesn't
        // guarantee decodability (e.g. r10).
        let message = code.decode(symbols, self.d33.msg_len())?;

        // the re-encode: a symbol equal to a received one has the leaf
        // hash computed when that chunk's proof checked; only the other
        // leaves are hashed
        debug_assert_eq!(message.len(), self.d33.msg_len());
        let symbol_len = self.d33.v1_layout().symbol_len();
        let symbols = code.encode(&message, symbol_len);
        let known = self
            .received
            .iter()
            .map(|(chunk_id, received)| (chunk_id.to_wire(), &received.data.symbol, received.leaf));
        let encoded = D33Encoded::from_symbols_with(&self.d33, symbols, known);
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

// the chunk's leaf hash, if its proof binds it to the root
fn verify_proof(chunk_id: ChunkId, data: &ChunkData, d33: &D33, root: &MerkleRoot) -> Option<Hash> {
    debug_assert_eq!(data.symbol.len(), d33.v1_layout().symbol_len());
    let leaf_idx = chunk_id.to_wire();
    let leaf = leaf_hash(d33, leaf_idx, &data.symbol);
    Tree::verify_proof(root, leaf_idx, &leaf, &data.proof).then_some(leaf)
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
            assert!(verify_proof(chunk_id, &data, &scheme, &encoded.root()).is_some());
        }

        let short = &message()[1..];
        assert!(D33Encoded::encode(&scheme, short, 9).is_none());
    }

    #[test]
    fn known_leaves_build_the_same_tree() {
        let scheme = scheme();
        let encoded = encoded();
        let symbols = encoded.symbols.clone();
        // the even leaves known for their symbols, the odd ones for
        // another symbol, which must not be taken
        let other = Bytes::from(vec![0u8; symbols[0].len()]);
        let mut known = Vec::new();
        for (leaf_idx, symbol) in symbols.iter().enumerate() {
            let leaf_idx = leaf_idx as WireChunkId;
            if leaf_idx.is_multiple_of(2) {
                known.push((leaf_idx, symbol, leaf_hash(&scheme, leaf_idx, symbol)));
            } else {
                known.push((leaf_idx, &other, leaf_hash(&scheme, leaf_idx, &other)));
            }
        }
        let reused = D33Encoded::from_symbols_with(&scheme, symbols.clone(), known);
        assert_eq!(reused.root(), encoded.root());
        assert_eq!(reused.symbols, symbols);
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
