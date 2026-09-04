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

use monad_mcp_chorus::spec::{Stake as _, validator::ValidatorData as _};
use monad_merkle::MerkleTree;

use super::{
    super::{
        assignment::{ChunkAssignment, StakePartition},
        chorus::env::D25,
        layout::PacketLayout,
        types::{NodeId, Stake, ValidatorData},
    },
    DAEncodingScheme,
    stub::{StubSymbolDecoder, StubSymbolEncoder},
};

const REDUNDANCY: f32 = 2.5;

// the encoding scheme of deterministic raptorcast for the current
// monad-bft. This encoding scheme is not going to be used by mcp. I'm
// re-implementing it here to mainly serve as a reference to ensure
// the da implementation is versatile enough to support other encoding
// schemes.
//
// D25 encoding scheme:
// - 2.5x redundancy
// - author owns no chunks, and is the last node of the table
// - stake partition with round up chunks
// - todo: valset pre-shuffled based on (slot, ...)
// - assigned round-robin
// - todo: symbols encoded using raptor code
impl DAEncodingScheme for D25 {
    type Encoder = StubSymbolEncoder;
    type Decoder = StubSymbolDecoder;

    // the smallest merkle tree depth whose leaves fit the assignment's
    // chunk count (bounded by one rounding chunk per owner)
    fn packet_layout(&self, num_validators: usize) -> Option<PacketLayout> {
        let num_owners = num_validators.saturating_sub(1);
        for depth in 2..=MerkleTree::MAX_DEPTH {
            let layout = PacketLayout::new(self.msg_len, depth);
            let scaled_source_chunks =
                (layout.num_source_chunks() as f32 * REDUNDANCY).ceil() as usize;
            let chunk_bound = scaled_source_chunks + num_owners;
            if chunk_bound <= 1usize << (depth - 1) {
                return Some(layout);
            }
        }
        None
    }

    fn chunk_assignment(
        &self,
        layout: &PacketLayout,
        author: &NodeId,
        validator_data: &ValidatorData,
    ) -> ChunkAssignment {
        let mut weights = vec![];
        for node_id in validator_data.nodes() {
            if node_id == author {
                continue;
            }
            let stake = validator_data.get_stake(node_id);
            weights.push((*node_id, *stake));
        }
        weights.push((*author, Stake::ZERO));

        let partition = StakePartition::new(weights);
        let num_source_chunks = layout.num_source_chunks();
        partition.assign(author, num_source_chunks, REDUNDANCY)
    }

    fn encoder(&self, _layout: PacketLayout, num_chunks: usize) -> StubSymbolEncoder {
        StubSymbolEncoder::new(num_chunks)
    }

    fn decoder(&self, layout: PacketLayout, _num_chunks: usize) -> StubSymbolDecoder {
        StubSymbolDecoder::new(layout)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        super::{
            super::{
                assignment::ChunkId,
                test_util::{MESSAGE_LEN, author, epoch_handle},
            },
            SymbolDecoder as _, SymbolEncoder as _,
        },
        *,
    };

    fn scheme() -> D25 {
        D25 {
            msg_len: MESSAGE_LEN,
            unix_ts: 0,
        }
    }

    #[test]
    fn packet_layout_is_the_smallest_depth_that_fits() {
        // 2 source chunks at 2.5x is 5, plus one rounding chunk per
        // owner: 8 leaves for 3 owners (depth 4), 16 for 11 (depth 5)
        let layout = scheme().packet_layout(4).expect("fits");
        assert_eq!(layout.merkle_tree_depth(), 4);
        assert_eq!(layout.num_source_chunks(), 2);
        assert_eq!(scheme().packet_layout(12).unwrap().merkle_tree_depth(), 5);

        let oversized = D25 {
            msg_len: 1 << 40,
            unix_ts: 0,
        };
        assert!(oversized.packet_layout(4).is_none());
    }

    #[test]
    fn the_author_comes_last_and_owns_nothing() {
        let epoch_handle = epoch_handle();
        let layout = scheme().packet_layout(4).unwrap();
        let assignment =
            scheme().chunk_assignment(&layout, &author(), &epoch_handle.validator_data);

        let author_index = assignment.index_of(&author()).expect("in the table");
        assert_eq!(usize::from(author_index), 3);
        assert_eq!(assignment.owned_chunks(author_index).count(), 0);

        assert_eq!(assignment.num_chunks(), 6);
        for id in 1..=3 {
            let index = assignment.index_of(&NodeId::dummy(id)).unwrap();
            assert_eq!(assignment.owned_chunks(index).count(), 2);
        }
    }

    #[test]
    fn decoding_needs_one_more_symbol_than_the_source_count() {
        let layout = scheme().packet_layout(4).unwrap();
        let message = vec![7u8; MESSAGE_LEN];
        let symbols = scheme().encoder(layout, 6).encode(&message);
        assert_eq!(symbols.len(), 6);

        let mut decoder = scheme().decoder(layout, 6);
        decoder.ingest(ChunkId::unchecked(0), &symbols[0]);
        decoder.ingest(ChunkId::unchecked(1), &symbols[1]);
        assert!(decoder.try_decode().is_none());
        decoder.ingest(ChunkId::unchecked(2), &symbols[2]);
        assert_eq!(decoder.try_decode(), Some(Bytes::from(message)));
    }
}
