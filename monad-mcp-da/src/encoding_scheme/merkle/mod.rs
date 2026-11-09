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

mod tree;

use std::collections::BTreeMap;

use bytes::Bytes;
use monad_crypto::hasher::Hash;

pub(crate) use self::tree::Tree;
use super::{
    super::{
        assignment::ChunkId,
        chunk::{ChunkData, WireChunkId},
        types::MerkleRoot,
        wire::{DAHeaderScheme, PacketLayout as _},
    },
    BadProof, DAEncodingScheme, InvalidEncoding, ProposalChunks, Resolved,
    symbol_code::{SymbolDecoder, SymbolEncoder},
};

// the codec and leaf rule of a scheme whose chunks sit under a merkle
// tree its frame sizes
pub(crate) trait MerkleCode: DAEncodingScheme + DAHeaderScheme + Send {
    type Encoder: SymbolEncoder;
    type Decoder: SymbolDecoder;

    fn encoder(&self, num_chunks: usize) -> Self::Encoder;
    fn decoder(&self, num_chunks: usize) -> Self::Decoder;
    fn leaf_hash(&self, chunk_id: WireChunkId, symbol: &[u8]) -> Hash;
}

// the chunks of one proposal under its merkle root
pub(crate) enum MerkleChunks<S: MerkleCode> {
    // the chunks received so far, their symbols fed to the decoder
    Decoding {
        scheme: S,
        root: MerkleRoot,
        num_chunks: usize,
        received: BTreeMap<ChunkId, ChunkData>,
        // the leaf hash each received symbol had when its proof checked,
        // by leaf index, for the re-encode
        leaves: BTreeMap<WireChunkId, Leaf>,
        decoder: S::Decoder,
    },
    // every symbol under its tree, so any chunk is derivable
    Decoded {
        // in chunk id order
        symbols: Vec<Bytes>,
        tree: Tree,
    },
    // the received chunks decode to a message that does not re-encode
    // to the root
    Invalid {
        root: MerkleRoot,
        received: BTreeMap<ChunkId, ChunkData>,
    },
}

// a symbol and the leaf hash this node computed from it
pub(crate) struct Leaf {
    symbol: Bytes,
    hash: Hash,
}

impl<S: MerkleCode> MerkleChunks<S> {
    pub(crate) fn new(scheme: S, root: MerkleRoot, num_chunks: usize) -> Self {
        let num_source_chunks = scheme.num_source_chunks();
        assert!(0 < num_source_chunks && num_source_chunks < num_chunks);
        let decoder = scheme.decoder(num_chunks);
        Self::Decoding {
            scheme,
            root,
            num_chunks,
            received: BTreeMap::new(),
            leaves: BTreeMap::new(),
            decoder,
        }
    }

    // None unless the message is of the scheme's length
    pub(crate) fn encode(scheme: &S, message: &Bytes, num_chunks: usize) -> Option<Self> {
        let (symbols, tree) = chunk_tree(scheme, message, num_chunks)?;
        let encoded = Self::Decoded { symbols, tree };
        Some(encoded)
    }

    // complete chunks over any symbols, in chunk id order
    #[cfg(test)]
    pub(crate) fn from_symbols(scheme: &S, symbols: Vec<Bytes>) -> Self {
        let tree = tree_over(scheme, &symbols);
        Self::Decoded { symbols, tree }
    }
}

impl<S: MerkleCode> ProposalChunks for MerkleChunks<S> {
    fn root(&self) -> MerkleRoot {
        match self {
            Self::Decoding { root, .. } | Self::Invalid { root, .. } => *root,
            Self::Decoded { tree, .. } => tree.root(),
        }
    }

    fn contains(&self, chunk_id: ChunkId) -> bool {
        match self {
            Self::Decoding { received, .. } | Self::Invalid { received, .. } => {
                received.contains_key(&chunk_id)
            }
            Self::Decoded { .. } => true,
        }
    }

    fn ingest(&mut self, chunk_id: ChunkId, data: ChunkData) -> Result<bool, BadProof> {
        let Self::Decoding {
            scheme,
            root,
            received,
            leaves,
            decoder,
            ..
        } = self
        else {
            return Ok(false);
        };
        if received.contains_key(&chunk_id) {
            return Ok(false);
        }
        let Some(hash) = verify_proof(chunk_id, &data, scheme, root) else {
            return Err(BadProof);
        };
        decoder.ingest(chunk_id, &data.symbol);
        let symbol = data.symbol.clone();
        leaves.insert(chunk_id.to_wire(), Leaf { symbol, hash });
        received.insert(chunk_id, data);
        Ok(true)
    }

    fn try_decode(&mut self) -> Option<Resolved> {
        let Self::Decoding {
            scheme,
            root,
            num_chunks,
            received,
            leaves,
            decoder,
        } = self
        else {
            return None;
        };
        // todo: None after enough chunks means the codec doesn't
        // guarantee decodability (e.g. r10).
        let message = decoder.try_decode()?;

        // a re-encoded symbol equal to a received one has the leaf hash
        // computed when that chunk's proof checked; only the other
        // leaves are hashed
        let root = *root;
        let known_leaf = |leaf_idx, symbol: &[u8]| {
            let leaf: &Leaf = leaves.get(&leaf_idx)?;
            (leaf.symbol == symbol).then_some(leaf.hash)
        };
        let reencoded = chunk_tree_with(scheme, &message, *num_chunks, known_leaf);
        let Some((symbols, tree)) = reencoded.filter(|(_, tree)| tree.root() == root) else {
            let received = std::mem::take(received);
            *self = Self::Invalid { root, received };
            return Some(Err(InvalidEncoding));
        };
        *self = Self::Decoded { symbols, tree };
        Some(Ok(message))
    }

    fn is_resolved(&self) -> bool {
        !matches!(self, Self::Decoding { .. })
    }

    fn chunk_data(&self, chunk_id: ChunkId) -> Option<ChunkData> {
        match self {
            Self::Decoding { received, .. } | Self::Invalid { received, .. } => {
                received.get(&chunk_id).cloned()
            }
            Self::Decoded { symbols, tree, .. } => Some(ChunkData {
                symbol: symbols[usize::from(chunk_id)].clone(),
                proof: tree.proof(chunk_id.to_wire()),
            }),
        }
    }
}

// the symbols of a message of the scheme's length, under their tree
fn chunk_tree<S: MerkleCode>(
    scheme: &S,
    message: &Bytes,
    num_chunks: usize,
) -> Option<(Vec<Bytes>, Tree)> {
    chunk_tree_with(scheme, message, num_chunks, |_, _| None)
}

// chunk_tree, taking a symbol's leaf hash from `known_leaf` where it has
// one. The caller must ensure a leaf it returns is the scheme's leaf hash
// of exactly that symbol at that index.
fn chunk_tree_with<S: MerkleCode>(
    scheme: &S,
    message: &Bytes,
    num_chunks: usize,
    known_leaf: impl Fn(WireChunkId, &[u8]) -> Option<Hash>,
) -> Option<(Vec<Bytes>, Tree)> {
    if message.is_empty() || message.len() != scheme.msg_len() {
        return None;
    }
    let symbols = scheme.encoder(num_chunks).encode(message);
    assert!(symbols.len() == num_chunks);
    let tree = tree_over_with(scheme, &symbols, known_leaf);
    Some((symbols, tree))
}

// the caller must pass the symbols in chunk id order
#[cfg(test)]
fn tree_over<S: MerkleCode>(scheme: &S, symbols: &[Bytes]) -> Tree {
    tree_over_with(scheme, symbols, |_, _| None)
}

// tree_over, with known leaves as for chunk_tree_with
fn tree_over_with<S: MerkleCode>(
    scheme: &S,
    symbols: &[Bytes],
    known_leaf: impl Fn(WireChunkId, &[u8]) -> Option<Hash>,
) -> Tree {
    let mut leaves = Vec::with_capacity(symbols.len());
    for (leaf_idx, symbol) in symbols.iter().enumerate() {
        let leaf_idx = leaf_idx as WireChunkId;
        let leaf = match known_leaf(leaf_idx, symbol) {
            Some(leaf) => {
                debug_assert_eq!(leaf, scheme.leaf_hash(leaf_idx, symbol));
                leaf
            }
            None => scheme.leaf_hash(leaf_idx, symbol),
        };
        leaves.push(leaf);
    }
    Tree::from_leaves(scheme.depth(), leaves)
}

// the chunk's leaf hash, if its proof binds it to the root
fn verify_proof<S: MerkleCode>(
    chunk_id: ChunkId,
    data: &ChunkData,
    scheme: &S,
    root: &MerkleRoot,
) -> Option<Hash> {
    debug_assert_eq!(data.symbol.len(), scheme.v1_layout().symbol_len());
    let leaf_idx = chunk_id.to_wire();
    let leaf = scheme.leaf_hash(leaf_idx, &data.symbol);
    Tree::verify_proof(root, leaf_idx, &leaf, &data.proof).then_some(leaf)
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            super::{
                test_util::{MESSAGE_LEN, SLOT},
                types::{D33, Lane},
            },
            d33::{D33Chunks, for_message},
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

    fn encoded() -> D33Chunks {
        D33Chunks::encode(&scheme(), &message(), 9).expect("of the scheme's length")
    }

    #[test]
    fn encoded_chunks_derive_every_chunk_with_its_proof() {
        let scheme = scheme();
        let encoded = encoded();
        for wire in 0..9 {
            let chunk_id = ChunkId::unchecked(wire);
            let data = encoded.chunk_data(chunk_id).expect("derivable");
            assert!(verify_proof(chunk_id, &data, &scheme, &encoded.root()).is_some());
            assert!(encoded.contains(chunk_id));
        }

        let short = message().slice(1..);
        assert!(D33Chunks::encode(&scheme, &short, 9).is_none());
    }

    #[test]
    fn known_leaves_build_the_same_tree() {
        let scheme = scheme();
        let (symbols, tree) = chunk_tree(&scheme, &message(), 9).expect("of the length");
        // the even leaves are known, as for received chunks
        let known = |leaf_idx: WireChunkId, symbol: &[u8]| {
            leaf_idx.is_multiple_of(2).then(|| scheme.leaf_hash(leaf_idx, symbol))
        };
        let (with_known, reused) =
            chunk_tree_with(&scheme, &message(), 9, known).expect("of the length");
        assert_eq!(with_known, symbols);
        assert_eq!(reused.root(), tree.root());
    }

    #[test]
    fn received_chunks_are_held_until_decoded_then_every_chunk_is() {
        let encoded = encoded();
        let chunk = |wire| {
            let chunk_id = ChunkId::unchecked(wire);
            let data = encoded.chunk_data(chunk_id).expect("derivable");
            (chunk_id, data)
        };
        let mut chunks = D33Chunks::new(scheme(), encoded.root(), 9);

        let (eight, data) = chunk(8);
        assert_eq!(chunks.ingest(eight, data.clone()), Ok(true));
        assert_eq!(chunks.ingest(eight, data), Ok(false));
        assert!(chunks.contains(eight));
        let (zero, zero_data) = chunk(0);
        assert!(!chunks.contains(zero));
        assert_eq!(chunks.chunk_data(zero), None);
        assert_eq!(chunks.try_decode(), None);

        let (five, data) = chunk(5);
        assert_eq!(chunks.ingest(five, data), Ok(true));
        assert_eq!(chunks.try_decode(), Some(Ok(message())));
        assert_eq!(chunks.try_decode(), None);
        assert!(chunks.is_resolved());

        // every chunk is derivable, and nothing is new
        assert!(chunks.contains(zero));
        assert_eq!(chunks.ingest(zero, zero_data), Ok(false));
    }
}
