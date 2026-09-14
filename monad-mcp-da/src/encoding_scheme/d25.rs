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

use super::{
    super::{
        assignment::{ChunkAssignment, StakePartition},
        types::{D25, NodeId, Stake, ValidatorData},
        wire,
    },
    DAEncodingScheme,
    stub::{StubSymbolDecoder, StubSymbolEncoder},
};

const REDUNDANCY: f32 = 2.5;

// prod's bound on a raptorcast message
pub const MAX_MESSAGE_LEN: usize = 3 * 1024 * 1024;

// the scheme a proposer among num_validators picks for its message:
// the least depth whose leaves fit the chunks. None if the message is
// empty, over the bound, or fits no depth.
pub fn for_message(msg_len: usize, unix_ts: u64, num_validators: usize) -> Option<D25> {
    let msg_len = bounded_len(msg_len)?;
    for depth in wire::MIN_DEPTH..=wire::MAX_DEPTH {
        if fits(msg_len, depth, num_validators) {
            return Some(D25 {
                msg_len,
                unix_ts,
                depth,
            });
        }
    }
    None
}

fn bounded_len(msg_len: usize) -> Option<u32> {
    if msg_len == 0 || msg_len > MAX_MESSAGE_LEN {
        return None;
    }
    u32::try_from(msg_len).ok()
}

// whether the chunks fit the depth's leaves. the caller must ensure
// depth is in MIN_DEPTH..MAX_DEPTH.
fn fits(msg_len: u32, depth: u8, num_validators: usize) -> bool {
    // in d25, proposer is not in assignment
    let num_owners = num_validators.saturating_sub(1);
    let scaled_source_chunks = scale(num_source_chunks(msg_len, depth));
    scaled_source_chunks + num_owners <= 1usize << (depth - 1)
}

fn scale(num_source_chunks: usize) -> usize {
    (num_source_chunks as f32 * REDUNDANCY).ceil() as usize
}

// the caller must ensure depth is in MIN_DEPTH..MAX_DEPTH.
fn num_source_chunks(msg_len: u32, depth: u8) -> usize {
    (msg_len as usize).div_ceil(wire::symbol_len(depth))
}

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

    fn depth(&self) -> u8 {
        self.depth
    }

    fn msg_len(&self) -> usize {
        self.msg_len as usize
    }

    fn num_source_chunks(&self) -> usize {
        num_source_chunks(self.msg_len, self.depth)
    }

    fn is_canonical(&self, num_validators: usize) -> bool {
        if bounded_len(self.msg_len as usize).is_none() {
            return false;
        }
        if !(wire::MIN_DEPTH..=wire::MAX_DEPTH).contains(&self.depth) {
            return false;
        }
        if !fits(self.msg_len, self.depth, num_validators) {
            return false;
        }
        // the least fitting depth, as fits is monotone
        self.depth == wire::MIN_DEPTH || !fits(self.msg_len, self.depth - 1, num_validators)
    }

    fn chunk_assignment(&self, author: &NodeId, validator_data: &ValidatorData) -> ChunkAssignment {
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
        partition.assign(author, self.num_source_chunks(), REDUNDANCY)
    }

    fn encoder(&self, num_chunks: usize) -> StubSymbolEncoder {
        StubSymbolEncoder::new(
            num_chunks,
            self.num_source_chunks(),
            wire::symbol_len(self.depth),
        )
    }

    fn decoder(&self, _num_chunks: usize) -> StubSymbolDecoder {
        StubSymbolDecoder::new(self.num_source_chunks(), self.msg_len())
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
        for_message(MESSAGE_LEN, 0, 4).expect("fits")
    }

    #[test]
    fn for_message_picks_the_least_depth_that_fits() {
        // 2 source chunks at 2.5x is 5, plus one rounding chunk per
        // owner: 8 leaves for 3 owners (depth 4), 16 for 11 (depth 5)
        assert_eq!(scheme().depth(), 4);
        assert_eq!(scheme().num_source_chunks(), 2);
        assert_eq!(for_message(MESSAGE_LEN, 0, 12).unwrap().depth(), 5);

        assert!(for_message(0, 0, 4).is_none());
        assert!(for_message(MAX_MESSAGE_LEN + 1, 0, 4).is_none());
        // a message in range that no depth can carry
        assert!(for_message(MAX_MESSAGE_LEN, 0, 20_000).is_none());
    }

    #[test]
    fn the_canonical_scheme_is_the_one_for_message_picks() {
        for msg_len in [1, 1000, MESSAGE_LEN, 5000, 100_000, 1_000_000] {
            for num_validators in [2, 4, 12, 100, 500] {
                let scheme = for_message(msg_len, 7, num_validators).expect("fits");
                assert!(scheme.is_canonical(num_validators), "{scheme:?}");

                let deeper = D25 {
                    depth: scheme.depth + 1,
                    ..scheme
                };
                assert!(!deeper.is_canonical(num_validators), "{deeper:?}");
                let shallower = D25 {
                    depth: scheme.depth - 1,
                    ..scheme
                };
                assert!(!shallower.is_canonical(num_validators), "{shallower:?}");
            }
        }

        let empty = D25 {
            msg_len: 0,
            ..scheme()
        };
        assert!(!empty.is_canonical(4));
    }

    #[test]
    fn the_author_comes_last_and_owns_nothing() {
        let epoch_handle = epoch_handle();
        let assignment = scheme().chunk_assignment(&author(), &epoch_handle.validator_data);

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
        let message = vec![7u8; MESSAGE_LEN];
        let symbols = scheme().encoder(6).encode(&message);
        assert_eq!(symbols.len(), 6);
        for symbol in &symbols {
            assert_eq!(symbol.len(), wire::symbol_len(scheme().depth()));
        }

        let mut decoder = scheme().decoder(6);
        decoder.ingest(ChunkId::unchecked(0), &symbols[0]);
        decoder.ingest(ChunkId::unchecked(1), &symbols[1]);
        assert!(decoder.try_decode().is_none());
        decoder.ingest(ChunkId::unchecked(2), &symbols[2]);
        assert_eq!(decoder.try_decode(), Some(Bytes::from(message)));
    }
}
