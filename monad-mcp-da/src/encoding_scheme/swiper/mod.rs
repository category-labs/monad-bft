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

mod qualification;
mod reed_solomon;

use monad_mcp_chorus::spec::validator::ValidatorData as _;
use rand::{SeedableRng as _, seq::SliceRandom as _};
use rand_chacha::ChaCha20Rng;

use self::reed_solomon::{RsSymbolDecoder, RsSymbolEncoder};
use super::{
    super::{
        assignment::ChunkAssignment,
        types::{NodeId, ProposalIndex, PubKey, S11, Slot, ValidatorData},
        wire::{DAHeaderScheme as _, PacketLayout as _, v1},
    },
    DAEncodingScheme,
};
use crate::spec::{DAPubKey as _, MAX_PROPOSER_SET_SIZE, PUBKEY_LEN};

pub const MAX_MESSAGE_LEN: usize = 1024 * 1024;
pub const REDUNDANCY: f32 = 1.1;

// the layout s11 proposals travel in
pub(crate) type S11Layout<'a> = v1::Layout<'a, S11>;

// the scheme a proposer among num_validators picks for its message:
// the least depth whose leaves fit the chunk bound. None if the
// message is empty, over the bound, fits no depth, or the index is
// beyond the header's.
pub fn for_message(
    slot: Slot,
    msg_len: usize,
    unix_ts: u64,
    proposer_index: ProposalIndex,
    num_validators: usize,
) -> Option<S11> {
    let msg_len = bounded_len(msg_len)?;
    let proposer_index = bounded_index(proposer_index)?;
    for depth in header::MIN_DEPTH..=header::MAX_DEPTH {
        if fits(msg_len, depth, num_validators) {
            return Some(S11 {
                slot,
                proposer_index,
                depth,
                msg_len,
                unix_ts,
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

fn bounded_index(proposer_index: ProposalIndex) -> Option<u8> {
    if proposer_index >= MAX_PROPOSER_SET_SIZE {
        return None;
    }
    u8::try_from(proposer_index).ok()
}

// redundancy multiplied by the source count
const fn target(num_source_chunks: usize) -> usize {
    (num_source_chunks * 11).div_ceil(10)
}

// statically assert the `target` calculation is consistent with
// multiplying by REDUNDANCY.
const _: () = {
    let symbol_len = S11Layout::symbol_len_at(header::MAX_DEPTH);
    let max_source_chunks = MAX_MESSAGE_LEN.div_ceil(symbol_len);

    let mut source_chunks = 1;
    while source_chunks <= max_source_chunks {
        let scaled = source_chunks as f32 * REDUNDANCY;
        let whole = scaled as usize;
        // manual rounding because f32::ceil is not const
        let rounded_up = if whole as f32 == scaled {
            whole
        } else {
            whole + 1
        };
        assert!(target(source_chunks) == rounded_up);
        source_chunks += 1;
    }
};

// whether the bound on the assignment's chunks fits the depth's
// leaves. the caller must ensure depth is in MIN_DEPTH..MAX_DEPTH.
fn fits(msg_len: u32, depth: u8, num_validators: usize) -> bool {
    let target = target(num_source_chunks(msg_len, depth));
    let max_chunks = qualification::max_total_tickets(target, num_validators);
    max_chunks <= 1usize << (depth - 1)
}

// the caller must ensure depth is in MIN_DEPTH..MAX_DEPTH.
fn num_source_chunks(msg_len: u32, depth: u8) -> usize {
    (msg_len as usize).div_ceil(S11Layout::symbol_len_at(depth))
}

// the prod seed: slot(8) unix_ts_ms/2048(8) pubkey[1..17]. the first
// pubkey byte is a low entropy tag.
fn seed(slot: Slot, unix_ts: u64, author: &PubKey) -> [u8; 32] {
    let mut pubkey = [0u8; PUBKEY_LEN];
    author.to_bytes(&mut pubkey);
    let coarse_ts = unix_ts / 2048;

    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&slot.get().to_le_bytes());
    seed[8..16].copy_from_slice(&coarse_ts.to_le_bytes());
    seed[16..].copy_from_slice(&pubkey[1..17]);
    seed
}

// S11 encoding scheme:
// - every validator, the author included, owns its Swiper tickets
//   (weight qualification at W/3 targeting the source count with a
//   margin for packet loss)
// - the validator order is shuffled by the prod seed
// - assigned round-robin
// - symbols are Reed-Solomon coded
impl DAEncodingScheme for S11 {
    type Encoder = RsSymbolEncoder;
    type Decoder = RsSymbolDecoder;

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
        if bounded_index(ProposalIndex::from(self.proposer_index)).is_none() {
            return false;
        }
        if !(header::MIN_DEPTH..=header::MAX_DEPTH).contains(&self.depth) {
            return false;
        }
        if !fits(self.msg_len, self.depth, num_validators) {
            return false;
        }
        // the least fitting depth, as fits is monotone
        self.depth == header::MIN_DEPTH || !fits(self.msg_len, self.depth - 1, num_validators)
    }

    fn chunk_assignment(&self, author: &NodeId, validator_data: &ValidatorData) -> ChunkAssignment {
        let mut nodes = Vec::with_capacity(validator_data.len());
        let mut weights = Vec::with_capacity(validator_data.len());
        for node_id in validator_data.nodes() {
            nodes.push(*node_id);
            weights.push(*validator_data.get_stake(node_id));
        }
        let tickets = qualification::tickets(&weights, target(self.num_source_chunks()));

        let mut holders: Vec<(NodeId, usize)> = nodes.into_iter().zip(tickets).collect();
        let seed = seed(self.slot, self.unix_ts, validator_data.get_pubkey(author));
        holders.shuffle(&mut ChaCha20Rng::from_seed(seed));

        ChunkAssignment::deal(author, holders)
    }

    fn encoder(&self, num_chunks: usize) -> RsSymbolEncoder {
        let symbol_len = self.v1_layout().symbol_len();
        RsSymbolEncoder::new(num_chunks, self.num_source_chunks(), symbol_len)
    }

    fn decoder(&self, num_chunks: usize) -> RsSymbolDecoder {
        RsSymbolDecoder::new(num_chunks, self.num_source_chunks(), self.msg_len())
    }
}

// the v1 header of a s11 proposal, after the version(2):
// proposer_index+depth(1) variant(1) slot(8) unix_ts(8) root(20) msg_len(3)
// and its chunk header: chunk_id(2)
mod header {
    use bytes::{Buf, BufMut};

    use super::{
        super::super::{
            chunk::WireChunkId,
            types::{MerkleRoot, S11, Slot},
            util::Tree,
            wire::{DAHeaderScheme, MalformedPacket, v1},
        },
        MAX_MESSAGE_LEN, S11Layout,
    };
    use crate::spec::{DAMerkleRoot as _, MAX_PROPOSER_SET_SIZE, ROOT_LEN};

    pub(crate) const VARIANT: u8 = 0x2;

    // the smallest message among the fewest validators bounds to
    // 3 * 2 + 1 = 7 leaves
    pub(super) const MIN_DEPTH: u8 = 4;
    // the depth the largest message among MAX_VALIDATOR_SET_SIZE
    // validators needs
    pub(super) const MAX_DEPTH: u8 = 13;

    // the index+depth byte: 4 proposer index bits over 4 depth bits
    const PROPOSER_INDEX_SHIFT: u32 = 4;
    const DEPTH_MASK: u8 = 0b0000_1111;

    const MSG_LEN_LEN: usize = 3;
    const PROPOSAL_HEADER_LEN: usize = 1 + 1 + 8 + 8 + ROOT_LEN + MSG_LEN_LEN;
    const CHUNK_HEADER_LEN: usize = 2;

    impl DAHeaderScheme for S11 {
        const VARIANT: u8 = VARIANT;
        const PROPOSAL_HEADER_LEN: usize = PROPOSAL_HEADER_LEN;
        const CHUNK_HEADER_LEN: usize = CHUNK_HEADER_LEN;

        fn pack_header(&self, root: &MerkleRoot, out: &mut impl BufMut) {
            assert!((MIN_DEPTH..=MAX_DEPTH).contains(&self.depth));
            assert!(usize::from(self.proposer_index) < MAX_PROPOSER_SET_SIZE);
            assert!(self.msg_len as usize <= MAX_MESSAGE_LEN);

            out.put_u8(self.proposer_index << PROPOSER_INDEX_SHIFT | self.depth);
            out.put_u8(VARIANT);
            out.put_u64_le(self.slot.get());
            out.put_u64_le(self.unix_ts);
            root.to_bytes(out);
            out.put_uint_le(u64::from(self.msg_len), MSG_LEN_LEN);
        }

        fn unpack_header(bytes: &mut impl Buf) -> Result<(MerkleRoot, S11), MalformedPacket> {
            let index_depth = bytes.get_u8();
            let depth = index_depth & DEPTH_MASK;
            if !(MIN_DEPTH..=MAX_DEPTH).contains(&depth) {
                return Err(MalformedPacket::DepthOutOfRange(depth));
            }
            let proposer_index = index_depth >> PROPOSER_INDEX_SHIFT;

            let variant = bytes.get_u8();
            if variant != VARIANT {
                return Err(MalformedPacket::UnknownScheme(variant));
            }

            let slot = bytes.get_u64_le();
            let slot = Slot::from_u64(slot).ok_or(MalformedPacket::SlotOutOfRange(slot))?;

            let unix_ts = bytes.get_u64_le();

            let mut root = [0u8; ROOT_LEN];
            bytes.copy_to_slice(&mut root);
            let root = MerkleRoot::from_bytes(&root).ok_or(MalformedPacket::BadRoot)?;

            let msg_len = bytes.get_uint_le(MSG_LEN_LEN) as u32;

            let scheme = S11 {
                slot,
                proposer_index,
                depth,
                msg_len,
                unix_ts,
            };
            Ok((root, scheme))
        }

        fn pack_chunk_header(chunk_id: WireChunkId, out: &mut impl BufMut) {
            out.put_u16_le(chunk_id);
        }

        fn unpack_chunk_header(bytes: &mut impl Buf) -> Result<WireChunkId, MalformedPacket> {
            Ok(bytes.get_u16_le())
        }

        fn depth(&self) -> u8 {
            self.depth
        }

        fn v1_layout(&self) -> v1::Layout<'_, S11> {
            v1::Layout::new(self)
        }
    }

    const _: () = {
        assert!(PROPOSAL_HEADER_LEN == 41);
        assert!(v1::VERSION == 1);
        assert!(S11Layout::SIGNED_LEN == 43);
        assert!(S11Layout::HEADER_LEN == 108);
        assert!(v1::SEGMENT_LEN == 1440);
        assert!(MIN_DEPTH == 4);
        assert!(MAX_DEPTH == 13);
        assert!(MAX_DEPTH <= DEPTH_MASK);
        assert!(MAX_DEPTH <= Tree::MAX_DEPTH);
        assert!(MAX_DEPTH <= S11Layout::MAX_DEPTH);
        // the index+depth byte carries the whole index range
        assert!(MAX_PROPOSER_SET_SIZE - 1 <= (u8::MAX >> PROPOSER_INDEX_SHIFT) as usize);
        assert!(MAX_MESSAGE_LEN < 1 << (8 * MSG_LEN_LEN));
        // the leaves address as u16 chunk ids and as Reed-Solomon shards
        assert!(1usize << (MAX_DEPTH - 1) <= 1 << 14);
        assert!(S11Layout::symbol_len_at(4) == 1270);
        assert!(S11Layout::symbol_len_at(5) == 1250);
        assert!(S11Layout::symbol_len_at(6) == 1230);
        assert!(S11Layout::symbol_len_at(7) == 1210);
        assert!(S11Layout::symbol_len_at(8) == 1190);
        assert!(S11Layout::symbol_len_at(9) == 1170);
        assert!(S11Layout::symbol_len_at(10) == 1150);
        assert!(S11Layout::symbol_len_at(11) == 1130);
        assert!(S11Layout::symbol_len_at(12) == 1110);
        assert!(S11Layout::symbol_len_at(13) == 1090);
        // Reed-Solomon shards are even
        let mut depth = MIN_DEPTH;
        while depth <= MAX_DEPTH {
            assert!(S11Layout::symbol_len_at(depth).is_multiple_of(2));
            depth += 1;
        }
    };

    #[cfg(test)]
    mod tests {
        use bytes::Bytes;
        use monad_mcp_chorus::spec::{ProposalHeader as _, SignedProposalHeader as _};

        use super::{
            super::{
                super::super::{
                    chunk::Chunk,
                    test_util::{SLOT, epoch_handle},
                    types::EncodingScheme,
                    wire::{v1, write_chunk},
                },
                tests::proposal_chunks,
            },
            *,
        };
        use crate::spec::{DAProposalHeader as _, DAProposalSignature as _};

        fn corrupted(
            chunk: &Chunk<'_>,
            corrupt: impl FnOnce(&mut Vec<u8>),
        ) -> Result<Chunk<'static>, MalformedPacket> {
            let mut bytes = write_chunk(chunk).to_vec();
            corrupt(&mut bytes);
            S11Layout::read_chunk(Bytes::from(bytes))
        }

        #[test]
        fn a_packet_is_one_segment_at_the_s11_offsets() {
            let epoch_handle = epoch_handle();
            let (header, chunks) = proposal_chunks(&epoch_handle, 1);
            let EncodingScheme::S11(s11) = header.scheme() else {
                panic!("the fixture is a s11 proposal");
            };
            let (_, chunk_id, data) = chunks[3].clone().into_parts();

            let bytes = write_chunk(&chunks[3]);
            assert_eq!(bytes.len(), 1440);

            let mut signature = Vec::new();
            header.sig().to_bytes(&mut signature);
            assert_eq!(bytes[..65], signature);
            assert_eq!(bytes[65..67], [1, 0]);
            assert_eq!(bytes[67], s11.proposer_index << 4 | s11.depth);
            assert_eq!(bytes[68], VARIANT);
            assert_eq!(v1::SCHEME_VARIANT_OFFSET, 68);
            assert_eq!(bytes[69..77], SLOT.get().to_le_bytes());
            assert_eq!(bytes[77..85], s11.unix_ts.to_le_bytes());
            assert_eq!(bytes[85..105], header.root().0.0);
            assert_eq!(bytes[105..108], s11.msg_len.to_le_bytes()[..3]);

            let proof_end = 108 + 20 * (s11.depth as usize - 1);
            assert_eq!(bytes[108..128], data.proof[0].0);
            assert_eq!(bytes[proof_end..proof_end + 2], chunk_id.to_le_bytes());
            assert_eq!(bytes[proof_end + 2..], data.symbol);
        }

        #[test]
        fn headers_round_trip() {
            let epoch_handle = epoch_handle();
            let (_, chunks) = proposal_chunks(&epoch_handle, 1);
            for chunk in &chunks {
                let packet = write_chunk(chunk);
                assert_eq!(S11Layout::read_chunk(packet), Ok(chunk.clone()));
            }
        }

        #[test]
        fn malformed_header_fields_are_rejected() {
            let epoch_handle = epoch_handle();
            let (_, chunks) = proposal_chunks(&epoch_handle, 1);
            let chunk = &chunks[0];

            let shallow = corrupted(chunk, |b| b[67] = 2);
            assert_eq!(shallow, Err(MalformedPacket::DepthOutOfRange(2)));
            let scheme = corrupted(chunk, |b| b[68] = 1);
            assert_eq!(scheme, Err(MalformedPacket::UnknownScheme(1)));
            let slot = corrupted(chunk, |b| {
                b[69..77].copy_from_slice(&u64::MAX.to_le_bytes())
            });
            assert_eq!(slot, Err(MalformedPacket::SlotOutOfRange(u64::MAX)));
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        super::{
            super::{
                assignment::ChunkId,
                chunk::Chunk,
                runtime::EpochHandle,
                test_util::{MESSAGE_LEN, SLOT, author, epoch_handle, proposal_chunks_under},
                types::{EncodingScheme, SignedProposalHeader},
            },
            SymbolDecoder as _, SymbolEncoder as _,
        },
        *,
    };
    use crate::spec::MAX_VALIDATOR_SET_SIZE;

    const PROPOSER_INDEX: ProposalIndex = 0;

    fn scheme_for(num_validators: usize) -> S11 {
        for_message(SLOT, MESSAGE_LEN, 0, PROPOSER_INDEX, num_validators).expect("fits")
    }

    fn scheme() -> S11 {
        scheme_for(4)
    }

    // the default fixture's proposal under s11
    pub(super) fn proposal_chunks(
        epoch_handle: &EpochHandle,
        payload: u8,
    ) -> (SignedProposalHeader, Vec<Chunk<'static>>) {
        let scheme = EncodingScheme::S11(scheme());
        proposal_chunks_under(epoch_handle, scheme, 0, payload)
    }

    #[test]
    fn for_message_picks_the_least_depth_that_fits_the_bound() {
        // 2 source chunks target 3, bounding to 3 * 3 + n + 2n/3
        // chunks: 15 for 4 validators (16 leaves, depth 5), 29 for 12
        // (32 leaves, depth 6)
        assert_eq!(scheme().depth, 5);
        assert_eq!(scheme().num_source_chunks(), 2);
        assert_eq!(scheme_for(12).depth, 6);

        assert!(for_message(SLOT, 0, 0, PROPOSER_INDEX, 4).is_none());
        assert!(for_message(SLOT, MAX_MESSAGE_LEN + 1, 0, PROPOSER_INDEX, 4).is_none());
        assert!(for_message(SLOT, MESSAGE_LEN, 0, 16, 4).is_none());
        // a message in range that no depth can carry
        assert!(for_message(SLOT, MAX_MESSAGE_LEN, 0, PROPOSER_INDEX, 20_000).is_none());
    }

    #[test]
    fn the_max_depth_is_the_one_the_largest_proposal_needs() {
        let deepest = for_message(
            SLOT,
            MAX_MESSAGE_LEN,
            0,
            PROPOSER_INDEX,
            MAX_VALIDATOR_SET_SIZE,
        );
        assert_eq!(deepest.expect("fits").depth, header::MAX_DEPTH);
    }

    #[test]
    fn the_canonical_scheme_is_the_one_for_message_picks() {
        for msg_len in [1, 1000, MESSAGE_LEN, 5000, 100_000, 1_000_000] {
            for num_validators in [1, 2, 4, 12, 100, 500] {
                let scheme = for_message(SLOT, msg_len, 7, 3, num_validators).expect("fits");
                assert!(scheme.is_canonical(num_validators), "{scheme:?}");

                let deeper = S11 {
                    depth: scheme.depth + 1,
                    ..scheme
                };
                assert!(!deeper.is_canonical(num_validators), "{deeper:?}");
                let shallower = S11 {
                    depth: scheme.depth - 1,
                    ..scheme
                };
                assert!(!shallower.is_canonical(num_validators), "{shallower:?}");
            }
        }

        let empty = S11 {
            msg_len: 0,
            ..scheme()
        };
        assert!(!empty.is_canonical(4));
        let oversized = S11 {
            msg_len: MAX_MESSAGE_LEN as u32 + 1,
            ..scheme()
        };
        assert!(!oversized.is_canonical(4));
    }

    #[test]
    fn every_validator_owns_its_tickets_including_the_author() {
        let epoch_handle = epoch_handle();
        let validator_data = &epoch_handle.validator_data;
        let assignment = scheme().chunk_assignment(&author(), validator_data);

        // 4 equal validators, target 3: two tickets each, as any two
        // of them must hold three chunks
        assert_eq!(assignment.num_chunks(), 8);
        assert_eq!(assignment.num_nodes(), 4);
        for id in 0..4 {
            let index = assignment
                .index_of(&NodeId::dummy(id))
                .expect("in the table");
            assert_eq!(assignment.owned_chunks(index).count(), 2);
        }
    }

    #[test]
    fn the_assignment_is_seeded_by_slot_time_and_author() {
        let epoch_handle = epoch_handle();
        let validator_data = &epoch_handle.validator_data;
        let scheme = scheme();

        let same = scheme.chunk_assignment(&author(), validator_data);
        assert_eq!(same, scheme.chunk_assignment(&author(), validator_data));

        // within a 2048ms window the order holds
        let near = S11 {
            unix_ts: 2047,
            ..scheme
        };
        assert_eq!(same, near.chunk_assignment(&author(), validator_data));

        let orders = [
            S11 {
                slot: Slot(2),
                ..scheme
            }
            .chunk_assignment(&author(), validator_data),
            S11 {
                unix_ts: 2048,
                ..scheme
            }
            .chunk_assignment(&author(), validator_data),
            scheme.chunk_assignment(&NodeId::dummy(1), validator_data),
        ];
        let owners = |assignment: &ChunkAssignment| -> Vec<NodeId> {
            assignment
                .chunk_ids()
                .map(|id| *assignment.node(assignment.routing(id).owner_index()))
                .collect()
        };
        let mut distinct = vec![owners(&same)];
        for other in &orders {
            let order = owners(other);
            assert!(!distinct.contains(&order), "{order:?}");
            distinct.push(order);
        }
    }

    #[test]
    fn decoding_needs_exactly_the_source_count() {
        let message = vec![7u8; MESSAGE_LEN];
        let symbols = scheme().encoder(4).encode(&message);
        assert_eq!(symbols.len(), 4);
        for symbol in &symbols {
            assert_eq!(symbol.len(), scheme().v1_layout().symbol_len());
        }

        let mut decoder = scheme().decoder(4);
        decoder.ingest(ChunkId::unchecked(3), &symbols[3]);
        assert!(decoder.try_decode().is_none());
        decoder.ingest(ChunkId::unchecked(1), &symbols[1]);
        assert_eq!(decoder.try_decode(), Some(Bytes::from(message)));
    }
}
