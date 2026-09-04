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

use monad_crypto::hasher::{Hash, Hasher as _, HasherType};

use super::chunk::WireChunkId;

#[derive(Debug, Clone, Copy)]
pub struct PacketLayout {
    app_message_len: usize,
    merkle_tree_depth: u8,
}

impl PacketLayout {
    // prod v1 wire constants
    const SEGMENT_LEN: usize = 1440;
    const HEADER_LEN: usize = 117;
    const CHUNK_HEADER_LEN: usize = 4;
    const MERKLE_HASH_LEN: usize = 20;

    pub(crate) fn new(app_message_len: usize, merkle_tree_depth: u8) -> Self {
        Self {
            app_message_len,
            merkle_tree_depth,
        }
    }

    pub fn num_source_chunks(&self) -> usize {
        self.app_message_len.div_ceil(self.symbol_len())
    }

    pub fn symbol_len(&self) -> usize {
        Self::SEGMENT_LEN - Self::HEADER_LEN - self.merkle_proof_len() - Self::CHUNK_HEADER_LEN
    }

    pub fn merkle_proof_len(&self) -> usize {
        Self::MERKLE_HASH_LEN * (self.merkle_tree_depth as usize - 1)
    }

    pub fn merkle_tree_depth(&self) -> u8 {
        self.merkle_tree_depth
    }

    // the v1 chunk header
    pub(crate) fn chunk_header(&self, chunk_id: WireChunkId) -> [u8; Self::CHUNK_HEADER_LEN] {
        let [lo, hi] = chunk_id.to_le_bytes();
        [0, 0, lo, hi]
    }

    pub(crate) fn merkle_leaf_hash(&self, chunk_id: WireChunkId, symbol: &[u8]) -> Hash {
        let mut hasher = HasherType::new();
        hasher.update(self.chunk_header(chunk_id));
        hasher.update(symbol);
        hasher.hash()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_len_leaves_room_for_headers_and_proof() {
        assert_eq!(PacketLayout::new(0, 2).symbol_len(), 1440 - 117 - 4 - 20);
        assert_eq!(PacketLayout::new(0, 5).symbol_len(), 1440 - 117 - 4 - 80);
    }

    #[test]
    fn source_chunks_round_up() {
        let symbol_len = PacketLayout::new(0, 2).symbol_len();
        assert_eq!(PacketLayout::new(symbol_len, 2).num_source_chunks(), 1);
        assert_eq!(PacketLayout::new(symbol_len + 1, 2).num_source_chunks(), 2);
    }

    #[test]
    fn chunk_header_is_the_little_endian_id_after_two_zero_bytes() {
        assert_eq!(
            PacketLayout::new(0, 2).chunk_header(0x0201),
            [0, 0, 0x01, 0x02]
        );
    }

    #[test]
    fn leaf_hash_covers_the_chunk_id() {
        let layout = PacketLayout::new(0, 2);
        assert_eq!(
            layout.merkle_leaf_hash(1, b"x"),
            layout.merkle_leaf_hash(1, b"x")
        );
        assert_ne!(
            layout.merkle_leaf_hash(1, b"x"),
            layout.merkle_leaf_hash(2, b"x")
        );
        assert_ne!(
            layout.merkle_leaf_hash(1, b"x"),
            layout.merkle_leaf_hash(1, b"y")
        );
    }
}
