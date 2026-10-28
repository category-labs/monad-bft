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

use monad_crypto::hasher::{Hash, HasherType};
use monad_mcp_chorus::spec::validator::ValidatorData as _;
use rand_chacha::ChaCha20Rng;

use super::{
    super::{
        assignment::{ChunkAssignment, PacketLossResistance, StakePartition},
        chunk::WireChunkId,
        types::{D33, Lane, MAX_PROPOSER_SET_SIZE, NodeId, Slot, ValidatorData},
        wire::{DAHeaderScheme as _, PacketLayout as _, v1},
    },
    DAEncodingScheme,
    reed_solomon::{RsSymbolDecoder, RsSymbolEncoder},
    swiper::seed,
};

// over the loss-scaled source count, for 3.3x in total
const REDUNDANCY: f32 = 3.0;

const PACKET_LOSS_RESISTANCE: PacketLossResistance = PacketLossResistance::new(11, 10);

pub const MAX_MESSAGE_LEN: usize = 1024 * 1024;

pub(crate) type D33Layout<'a> = v1::Layout<'a, D33>;

// the scheme a proposer among num_validators picks for its
// message.
pub fn for_message(
    slot: Slot,
    msg_len: usize,
    unix_ts: u64,
    lane: Lane,
    num_validators: usize,
) -> Option<D33> {
    let msg_len = bounded_len(msg_len)?;
    let lane = bounded_lane(lane)?;
    for depth in header::MIN_DEPTH..=header::MAX_DEPTH {
        if fits(msg_len, depth, num_validators) {
            return Some(D33 {
                slot,
                lane,
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

fn bounded_lane(lane: Lane) -> Option<Lane> {
    (usize::from(lane) < MAX_PROPOSER_SET_SIZE).then_some(lane)
}

// the chunks a decoder needs once packet loss trims each obligation
const fn target(num_source_chunks: usize) -> usize {
    PACKET_LOSS_RESISTANCE.scale(num_source_chunks)
}

fn scale(target: usize) -> usize {
    (target as f32 * REDUNDANCY).ceil() as usize
}

// whether the chunks fit the depth's leaves. the caller must ensure
// depth is in MIN_DEPTH..MAX_DEPTH.
fn fits(msg_len: u32, depth: u8, num_validators: usize) -> bool {
    // one rounding chunk per validator
    let scaled_target = scale(target(num_source_chunks(msg_len, depth)));
    scaled_target + num_validators <= 1usize << (depth - 1)
}

// the caller must ensure depth is in MIN_DEPTH..MAX_DEPTH.
fn num_source_chunks(msg_len: u32, depth: u8) -> usize {
    (msg_len as usize).div_ceil(D33Layout::symbol_len_at(depth))
}

// D33 encoding scheme:
// - 3.3x redundancy: 3x over the source count scaled for 10% packet loss
// - every validator, the author included, owns its stake share
// - stake partition with round up chunks
// - round up chunks are rebroadcast to a stake-proportional subset in second-hop
// - valset shuffled by the prod seed (slot, unix_ts / 2048, author)
// - assigned round-robin
// - symbols are Reed-Solomon coded
impl DAEncodingScheme for D33 {
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
        if bounded_lane(self.lane).is_none() {
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
        use rand::{SeedableRng as _, seq::SliceRandom as _};

        let mut weights = Vec::with_capacity(validator_data.len());
        for node_id in validator_data.nodes() {
            let stake = validator_data.get_stake(node_id);
            weights.push((*node_id, *stake));
        }
        let seed = seed(self.slot, self.unix_ts, validator_data.get_pubkey(author));
        weights.shuffle(&mut ChaCha20Rng::from_seed(seed));

        let partition = StakePartition::new(weights);
        let target = target(self.num_source_chunks());
        partition.assign_spm(author, target, REDUNDANCY, PACKET_LOSS_RESISTANCE)
    }

    fn encoder(&self, num_chunks: usize) -> RsSymbolEncoder {
        let symbol_len = self.v1_layout().symbol_len();
        RsSymbolEncoder::new(num_chunks, self.num_source_chunks(), symbol_len)
    }

    fn decoder(&self, num_chunks: usize) -> RsSymbolDecoder {
        RsSymbolDecoder::new(num_chunks, self.num_source_chunks(), self.msg_len())
    }

    fn depth(&self) -> u8 {
        self.depth
    }

    // the message length makes the root pin the decoded message, not only the symbols
    fn leaf_hash(&self, chunk_id: WireChunkId, symbol: &[u8]) -> Hash {
        use monad_crypto::hasher::Hasher as _;

        let mut hasher = HasherType::new();
        hasher.update(self.msg_len.to_le_bytes());
        hasher.update(chunk_id.to_le_bytes());
        hasher.update(symbol);
        hasher.hash()
    }
}

// the v1 header of a d33 proposal, after the version(2):
// lane+depth(1) variant(1) slot(8) unix_ts(8) root(20) msg_len(3)
// and its chunk header: chunk_id(2)
mod header {
    use bytes::{Buf, BufMut};

    use super::{
        super::super::{
            chunk::WireChunkId,
            types::{D33, Lane, MAX_PROPOSER_SET_SIZE, MerkleRoot, Slot},
            util::Tree,
            wire::{DAHeaderScheme, MalformedPacket, v1},
        },
        D33Layout, MAX_MESSAGE_LEN,
    };
    use crate::spec::{DAMerkleRoot as _, ROOT_LEN};

    pub(crate) const VARIANT: u8 = 0x3;

    // the smallest message targets ceil(1 * 1.1) = 2 chunks, scaled to
    // 6, plus one rounding chunk for the only validator: 7 leaves
    pub(super) const MIN_DEPTH: u8 = 4;
    // the depth the largest message among MAX_VALIDATOR_SET_SIZE
    // validators needs
    pub(super) const MAX_DEPTH: u8 = 13;

    // the lane+depth byte: 4 lane bits over 4 depth bits
    const LANE_SHIFT: u32 = 4;
    const DEPTH_MASK: u8 = 0b0000_1111;

    const MSG_LEN_LEN: usize = 3;
    const PROPOSAL_HEADER_LEN: usize = 1 + 1 + 8 + 8 + ROOT_LEN + MSG_LEN_LEN;
    const CHUNK_HEADER_LEN: usize = 2;

    impl DAHeaderScheme for D33 {
        const VARIANT: u8 = VARIANT;
        const PROPOSAL_HEADER_LEN: usize = PROPOSAL_HEADER_LEN;
        const CHUNK_HEADER_LEN: usize = CHUNK_HEADER_LEN;

        fn pack_header(&self, root: &MerkleRoot, out: &mut impl BufMut) {
            assert!((MIN_DEPTH..=MAX_DEPTH).contains(&self.depth));
            assert!(usize::from(self.lane) < MAX_PROPOSER_SET_SIZE);
            assert!(self.msg_len as usize <= MAX_MESSAGE_LEN);

            out.put_u8((usize::from(self.lane) as u8) << LANE_SHIFT | self.depth);
            out.put_u8(VARIANT);
            out.put_u64_le(self.slot.get());
            out.put_u64_le(self.unix_ts);
            root.to_bytes(out);
            out.put_uint_le(u64::from(self.msg_len), MSG_LEN_LEN);
        }

        fn unpack_header(bytes: &mut impl Buf) -> Result<(MerkleRoot, D33), MalformedPacket> {
            let lane_depth = bytes.get_u8();
            let depth = lane_depth & DEPTH_MASK;
            if !(MIN_DEPTH..=MAX_DEPTH).contains(&depth) {
                return Err(MalformedPacket::DepthOutOfRange(depth));
            }
            let lane = lane_depth >> LANE_SHIFT;
            let lane = Lane::from_u8(lane).ok_or(MalformedPacket::LaneOutOfRange(lane))?;

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

            let scheme = D33 {
                slot,
                lane,
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

        fn v1_layout(&self) -> v1::Layout<'_, D33> {
            v1::Layout::new(self)
        }
    }

    const _: () = {
        assert!(PROPOSAL_HEADER_LEN == 41);
        assert!(v1::VERSION == 1);
        assert!(D33Layout::SIGNED_LEN == 43);
        assert!(D33Layout::HEADER_LEN == 108);
        assert!(v1::SEGMENT_LEN == 1440);
        assert!(MIN_DEPTH == 4);
        assert!(MAX_DEPTH == 13);
        assert!(MAX_DEPTH <= DEPTH_MASK);
        assert!(MAX_DEPTH <= Tree::MAX_DEPTH);
        assert!(MAX_DEPTH <= D33Layout::MAX_DEPTH);
        // the lane+depth byte carries the whole lane range
        assert!(MAX_PROPOSER_SET_SIZE - 1 <= (u8::MAX >> LANE_SHIFT) as usize);
        assert!(MAX_MESSAGE_LEN < 1 << (8 * MSG_LEN_LEN));
        // the leaves address as u16 chunk ids and as Reed-Solomon shards
        assert!(1usize << (MAX_DEPTH - 1) <= 1 << 14);
        assert!(D33Layout::symbol_len_at(4) == 1270);
        assert!(D33Layout::symbol_len_at(5) == 1250);
        assert!(D33Layout::symbol_len_at(6) == 1230);
        assert!(D33Layout::symbol_len_at(7) == 1210);
        assert!(D33Layout::symbol_len_at(8) == 1190);
        assert!(D33Layout::symbol_len_at(9) == 1170);
        assert!(D33Layout::symbol_len_at(10) == 1150);
        assert!(D33Layout::symbol_len_at(11) == 1130);
        assert!(D33Layout::symbol_len_at(12) == 1110);
        assert!(D33Layout::symbol_len_at(13) == 1090);
        // Reed-Solomon shards are even
        let mut depth = MIN_DEPTH;
        while depth <= MAX_DEPTH {
            assert!(D33Layout::symbol_len_at(depth).is_multiple_of(2));
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
            D33Layout::read_chunk(Bytes::from(bytes))
        }

        #[test]
        fn a_packet_is_one_segment_at_the_s11_offsets() {
            let epoch_handle = epoch_handle();
            let (header, chunks) = proposal_chunks(&epoch_handle, 1);
            let EncodingScheme::D33(d33) = header.scheme() else {
                panic!("the fixture is a d33 proposal");
            };
            let (_, chunk_id, data) = chunks[3].clone().into_parts();

            let bytes = write_chunk(&chunks[3]);
            assert_eq!(bytes.len(), 1440);

            let mut signature = Vec::new();
            header.sig().to_bytes(&mut signature);
            assert_eq!(bytes[..65], signature);
            assert_eq!(bytes[65..67], [1, 0]);
            assert_eq!(bytes[67], (usize::from(d33.lane) as u8) << 4 | d33.depth);
            assert_eq!(bytes[68], VARIANT);
            assert_eq!(v1::SCHEME_VARIANT_OFFSET, 68);
            assert_eq!(bytes[69..77], SLOT.get().to_le_bytes());
            assert_eq!(bytes[77..85], d33.unix_ts.to_le_bytes());
            assert_eq!(bytes[85..105], header.root().0.0);
            assert_eq!(bytes[105..108], d33.msg_len.to_le_bytes()[..3]);

            let proof_end = 108 + 20 * (d33.depth as usize - 1);
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
                assert_eq!(D33Layout::read_chunk(packet), Ok(chunk.clone()));
            }
        }

        #[test]
        fn malformed_header_fields_are_rejected() {
            let epoch_handle = epoch_handle();
            let (_, chunks) = proposal_chunks(&epoch_handle, 1);
            let chunk = &chunks[0];

            let shallow = corrupted(chunk, |b| b[67] = 2);
            assert_eq!(shallow, Err(MalformedPacket::DepthOutOfRange(2)));
            let scheme = corrupted(chunk, |b| b[68] = 2);
            assert_eq!(scheme, Err(MalformedPacket::UnknownScheme(2)));
            let slot = corrupted(chunk, |b| {
                b[69..77].copy_from_slice(&u64::MAX.to_le_bytes())
            });
            assert_eq!(slot, Err(MalformedPacket::SlotOutOfRange(u64::MAX)));
            let beyond = MAX_PROPOSER_SET_SIZE as u8;
            let lane = corrupted(chunk, |b| b[67] = beyond << LANE_SHIFT | b[67] & DEPTH_MASK);
            assert_eq!(lane, Err(MalformedPacket::LaneOutOfRange(beyond)));
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
                test_util::{
                    MESSAGE_LEN, SLOT, author, epoch_handle, proposal_chunks_under,
                    staked_validator_data,
                },
                types::{EncodingScheme, SignedProposalHeader},
            },
            SymbolDecoder as _, SymbolEncoder as _,
        },
        *,
    };
    use crate::spec::MAX_VALIDATOR_SET_SIZE;

    const LANE: Lane = Lane::new_unchecked(0);

    fn scheme_for(num_validators: usize) -> D33 {
        for_message(SLOT, MESSAGE_LEN, 0, LANE, num_validators).expect("fits")
    }

    fn scheme() -> D33 {
        scheme_for(4)
    }

    // the default fixture's proposal under d33
    pub(super) fn proposal_chunks(
        epoch_handle: &EpochHandle,
        payload: u8,
    ) -> (SignedProposalHeader, Vec<Chunk<'static>>) {
        let scheme = EncodingScheme::D33(scheme());
        proposal_chunks_under(epoch_handle, scheme, 0, payload)
    }

    #[test]
    fn for_message_picks_the_least_depth_that_fits() {
        // 2 source chunks target 3, scaled to 9, plus one rounding
        // chunk per validator: 13 for 4 (16 leaves, depth 5), 21 for 12
        // (32 leaves, depth 6)
        assert_eq!(scheme().depth, 5);
        assert_eq!(scheme().num_source_chunks(), 2);
        assert_eq!(scheme_for(12).depth, 6);

        assert!(for_message(SLOT, 0, 0, LANE, 4).is_none());
        assert!(for_message(SLOT, MAX_MESSAGE_LEN + 1, 0, LANE, 4).is_none());
        assert!(for_message(SLOT, MESSAGE_LEN, 0, Lane::new_unchecked(16), 4).is_none());
        // a message in range that no depth can carry
        assert!(for_message(SLOT, MAX_MESSAGE_LEN, 0, LANE, 20_000).is_none());
    }

    #[test]
    fn the_max_depth_is_the_one_the_largest_proposal_needs() {
        let deepest = for_message(SLOT, MAX_MESSAGE_LEN, 0, LANE, MAX_VALIDATOR_SET_SIZE);
        assert_eq!(deepest.expect("fits").depth, header::MAX_DEPTH);
    }

    #[test]
    fn the_canonical_scheme_is_the_one_for_message_picks() {
        for msg_len in [1, 1000, MESSAGE_LEN, 5000, 100_000, 1_000_000] {
            for num_validators in [1, 2, 4, 12, 100, 300] {
                let scheme = for_message(SLOT, msg_len, 7, Lane::new_unchecked(3), num_validators)
                    .expect("fits");
                assert!(scheme.is_canonical(num_validators), "{scheme:?}");

                let deeper = D33 {
                    depth: scheme.depth + 1,
                    ..scheme
                };
                assert!(!deeper.is_canonical(num_validators), "{deeper:?}");
                let shallower = D33 {
                    depth: scheme.depth - 1,
                    ..scheme
                };
                assert!(!shallower.is_canonical(num_validators), "{shallower:?}");
            }
        }

        let empty = D33 {
            msg_len: 0,
            ..scheme()
        };
        assert!(!empty.is_canonical(4));
        let oversized = D33 {
            msg_len: MAX_MESSAGE_LEN as u32 + 1,
            ..scheme()
        };
        assert!(!oversized.is_canonical(4));
    }

    #[test]
    fn every_validator_is_in_the_table_however_small_its_stake() {
        let validator_data = staked_validator_data(&[1000, 1000, 1000, 1000, 1]);
        let assignment = scheme_for(5).chunk_assignment(&author(), &validator_data);

        assert_eq!(assignment.num_nodes(), validator_data.len());
        for node in validator_data.nodes() {
            assert!(assignment.index_of(node).is_some());
        }
    }

    #[test]
    fn every_validator_owns_its_share_including_the_author() {
        let epoch_handle = epoch_handle();
        let assignment = scheme().chunk_assignment(&author(), &epoch_handle.validator_data);

        // target 3 scaled to 9 over 4 equal validators: 2 each, plus
        // a rounding chunk each
        assert_eq!(assignment.num_chunks(), 12);
        assert_eq!(assignment.num_nodes(), 4);
        for id in 0..4 {
            let index = assignment
                .index_of(&NodeId::dummy(id))
                .expect("in the table");
            assert_eq!(assignment.owned_chunks(index).count(), 3);
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
        let near = D33 {
            unix_ts: 2047,
            ..scheme
        };
        assert_eq!(same, near.chunk_assignment(&author(), validator_data));

        let orders = [
            D33 {
                slot: Slot(2),
                ..scheme
            }
            .chunk_assignment(&author(), validator_data),
            D33 {
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
    fn any_source_count_of_symbols_decodes() {
        let message = vec![7u8; MESSAGE_LEN];
        let symbols = scheme().encoder(9).encode(&message);
        assert_eq!(symbols.len(), 9);
        for symbol in &symbols {
            assert_eq!(symbol.len(), scheme().v1_layout().symbol_len());
        }

        let mut decoder = scheme().decoder(9);
        decoder.ingest(ChunkId::unchecked(8), &symbols[8]);
        assert!(decoder.try_decode().is_none());
        decoder.ingest(ChunkId::unchecked(5), &symbols[5]);
        assert_eq!(decoder.try_decode(), Some(Bytes::from(message)));
    }
}
