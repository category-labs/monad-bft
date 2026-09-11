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
        types::{D25, NodeId, Slot, Stake, ValidatorData},
        wire::{DAHeaderScheme as _, PacketLayout as _, v1},
    },
    DAEncodingScheme,
    stub::{StubSymbolDecoder, StubSymbolEncoder},
};

const REDUNDANCY: f32 = 2.5;

// prod's bound on a raptorcast message
pub const MAX_MESSAGE_LEN: usize = 3 * 1024 * 1024;

// the encoding symbols R10 can address (r10::MAX_TRIPLES)
pub(crate) const MAX_CHUNKS: usize = 65521;

// the layout d25 proposals travel in
pub(crate) type D25Layout<'a> = v1::Layout<'a, D25>;

// the scheme a proposer among num_validators picks for its message:
// the least depth whose leaves fit the chunks. None if the message is
// empty, over the bound, or fits no depth.
pub fn for_message(slot: Slot, msg_len: usize, unix_ts: u64, num_validators: usize) -> Option<D25> {
    let msg_len = bounded_len(msg_len)?;
    for depth in header::MIN_DEPTH..=header::MAX_DEPTH {
        if fits(msg_len, depth, num_validators) {
            return Some(D25 {
                slot,
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
    (msg_len as usize).div_ceil(D25Layout::symbol_len_at(depth))
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
        let symbol_len = self.v1_layout().symbol_len();
        StubSymbolEncoder::new(num_chunks, self.num_source_chunks(), symbol_len)
    }

    fn decoder(&self, _num_chunks: usize) -> StubSymbolDecoder {
        StubSymbolDecoder::new(self.num_source_chunks(), self.msg_len())
    }
}

// the v1 header of a d25 proposal, after the version(2):
// mode+depth(1) variant(1) slot(8) epoch(8) unix_ts(8) root(20) msg_len(4)
// and its chunk header: reserved(2) chunk_id(2)
mod header {
    use bytes::{Buf, BufMut};

    use super::{
        super::super::{
            chunk::WireChunkId,
            types::{D25, MerkleRoot, Slot},
            util::Tree,
            wire::{DAHeaderScheme, MalformedPacket, v1},
        },
        D25Layout, MAX_CHUNKS,
    };
    use crate::spec::{DAMerkleRoot as _, ROOT_LEN};

    pub(crate) const VARIANT: u8 = 0x1;

    // the smallest message needs ceil(1 * 2.5) + 1 owner = 4 leaves
    pub(super) const MIN_DEPTH: u8 = 3;
    // the depth the largest message among MAX_VALIDATOR_SET_SIZE
    // validators needs
    pub(super) const MAX_DEPTH: u8 = 14;

    const PROPOSAL_HEADER_LEN: usize = 1 + 1 + 8 + 8 + 8 + ROOT_LEN + 4;
    const CHUNK_HEADER_LEN: usize = 2 + 2;

    // the mode+depth byte: 2 mode bits, 2 reserved bits, 4 depth bits
    const MODE_MASK: u8 = 0b1100_0000;
    const PRIMARY_MODE: u8 = 0b10 << 6;
    const RESERVED_MASK: u8 = 0b0011_0000;
    const DEPTH_MASK: u8 = 0b0000_1111;

    // mcp only uses slot. fix the epoch field to constant.
    const EPOCH: u64 = 0;

    impl DAHeaderScheme for D25 {
        const VARIANT: u8 = VARIANT;
        const PROPOSAL_HEADER_LEN: usize = PROPOSAL_HEADER_LEN;
        const CHUNK_HEADER_LEN: usize = CHUNK_HEADER_LEN;

        fn pack_header(&self, root: &MerkleRoot, out: &mut impl BufMut) {
            assert!((MIN_DEPTH..=MAX_DEPTH).contains(&self.depth));

            out.put_u8(PRIMARY_MODE | self.depth);
            out.put_u8(VARIANT);
            out.put_u64_le(self.slot.get());
            out.put_u64_le(EPOCH);
            out.put_u64_le(self.unix_ts);
            root.to_bytes(out);
            out.put_u32_le(self.msg_len);
        }

        fn unpack_header(bytes: &mut impl Buf) -> Result<(MerkleRoot, D25), MalformedPacket> {
            let mode_depth = bytes.get_u8();
            if mode_depth & RESERVED_MASK != 0 {
                return Err(MalformedPacket::ReservedNonZero);
            }
            let mode = mode_depth & MODE_MASK;
            if mode != PRIMARY_MODE {
                return Err(MalformedPacket::BadMode(mode));
            }
            let depth = mode_depth & DEPTH_MASK;
            if !(MIN_DEPTH..=MAX_DEPTH).contains(&depth) {
                return Err(MalformedPacket::DepthOutOfRange(depth));
            }

            let variant = bytes.get_u8();
            if variant != VARIANT {
                return Err(MalformedPacket::UnknownScheme(variant));
            }

            let slot = bytes.get_u64_le();
            let slot = Slot::from_u64(slot).ok_or(MalformedPacket::SlotOutOfRange(slot))?;

            let epoch = bytes.get_u64_le();
            if epoch != EPOCH {
                return Err(MalformedPacket::NonZeroEpoch(epoch));
            }

            let unix_ts = bytes.get_u64_le();

            let mut root = [0u8; ROOT_LEN];
            bytes.copy_to_slice(&mut root);
            let root = MerkleRoot::from_bytes(&root).ok_or(MalformedPacket::BadRoot)?;

            let msg_len = bytes.get_u32_le();

            let scheme = D25 {
                slot,
                msg_len,
                unix_ts,
                depth,
            };
            Ok((root, scheme))
        }

        fn pack_chunk_header(chunk_id: WireChunkId, out: &mut impl BufMut) {
            out.put_u16_le(0);
            out.put_u16_le(chunk_id);
        }

        fn unpack_chunk_header(bytes: &mut impl Buf) -> Result<WireChunkId, MalformedPacket> {
            let reserved = bytes.get_u16_le();
            if reserved != 0 {
                return Err(MalformedPacket::ReservedNonZero);
            }
            Ok(bytes.get_u16_le())
        }

        fn depth(&self) -> u8 {
            self.depth
        }

        fn v1_layout(&self) -> v1::Layout<'_, D25> {
            v1::Layout::new(self)
        }
    }

    const _: () = {
        assert!(PROPOSAL_HEADER_LEN == 50);
        assert!(v1::VERSION == 1);
        assert!(D25Layout::SIGNED_LEN == 52);
        assert!(D25Layout::HEADER_LEN == 117);
        assert!(v1::SEGMENT_LEN == 1440);
        assert!(MIN_DEPTH == 3);
        assert!(MAX_DEPTH == 14);
        assert!(MAX_DEPTH <= DEPTH_MASK);
        assert!(MAX_DEPTH <= Tree::MAX_DEPTH);
        assert!(MAX_DEPTH <= D25Layout::MAX_DEPTH);
        assert!(1usize << (MAX_DEPTH - 1) < MAX_CHUNKS);
        assert!(D25Layout::symbol_len_at(3) == 1279);
        assert!(D25Layout::symbol_len_at(4) == 1259);
        assert!(D25Layout::symbol_len_at(5) == 1239);
        assert!(D25Layout::symbol_len_at(6) == 1219);
        assert!(D25Layout::symbol_len_at(7) == 1199);
        assert!(D25Layout::symbol_len_at(8) == 1179);
        assert!(D25Layout::symbol_len_at(9) == 1159);
        assert!(D25Layout::symbol_len_at(10) == 1139);
        assert!(D25Layout::symbol_len_at(11) == 1119);
        assert!(D25Layout::symbol_len_at(12) == 1099);
        assert!(D25Layout::symbol_len_at(13) == 1079);
        assert!(D25Layout::symbol_len_at(14) == 1059);
    };

    #[cfg(test)]
    mod tests {
        use bytes::Bytes;
        use monad_mcp_chorus::spec::{ProposalHeader as _, SignedProposalHeader as _};

        use super::{
            super::super::super::{
                chunk::Chunk,
                test_util::{SLOT, epoch_handle, proposal_chunks},
                types::EncodingScheme,
                wire::write_chunk,
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
            D25Layout::read_chunk(Bytes::from(bytes))
        }

        #[test]
        fn a_packet_is_one_segment_at_the_prod_offsets() {
            let epoch_handle = epoch_handle();
            let (header, chunks) = proposal_chunks(&epoch_handle, 1);
            let EncodingScheme::D25(d25) = header.scheme();
            let (_, chunk_id, data) = chunks[3].clone().into_parts();

            let bytes = write_chunk(&chunks[3]);
            assert_eq!(bytes.len(), 1440);

            let mut signature = Vec::new();
            header.sig().to_bytes(&mut signature);
            assert_eq!(bytes[..65], signature);
            assert_eq!(bytes[65..67], [1, 0]);
            assert_eq!(bytes[67], 0b1000_0000 | d25.depth);
            assert_eq!(bytes[68], VARIANT);
            assert_eq!(v1::SCHEME_VARIANT_OFFSET, 68);
            assert_eq!(bytes[69..77], SLOT.get().to_le_bytes());
            assert_eq!(bytes[77..85], [0; 8]);
            assert_eq!(bytes[85..93], d25.unix_ts.to_le_bytes());
            assert_eq!(bytes[93..113], header.root().0.0);
            assert_eq!(bytes[113..117], d25.msg_len.to_le_bytes());

            let proof_end = 117 + 20 * (d25.depth as usize - 1);
            assert_eq!(bytes[117..137], data.proof[0].0);
            assert_eq!(bytes[proof_end..proof_end + 4], [0, 0, chunk_id as u8, 0]);
            assert_eq!(bytes[proof_end + 4..], data.symbol);
        }

        #[test]
        fn malformed_header_fields_are_rejected() {
            let epoch_handle = epoch_handle();
            let (_, chunks) = proposal_chunks(&epoch_handle, 1);
            let chunk = &chunks[0];

            let mode = corrupted(chunk, |b| b[67] |= 0b0100_0000);
            assert_eq!(mode, Err(MalformedPacket::BadMode(0b1100_0000)));
            let reserved_bits = corrupted(chunk, |b| b[67] |= 0b0010_0000);
            assert_eq!(reserved_bits, Err(MalformedPacket::ReservedNonZero));
            let reserved_bits = corrupted(chunk, |b| b[67] |= 0b0001_0000);
            assert_eq!(reserved_bits, Err(MalformedPacket::ReservedNonZero));
            let shallow = corrupted(chunk, |b| b[67] = 0b1000_0000 | 2);
            assert_eq!(shallow, Err(MalformedPacket::DepthOutOfRange(2)));
            let scheme = corrupted(chunk, |b| b[68] = 2);
            assert_eq!(scheme, Err(MalformedPacket::UnknownScheme(2)));
            let slot = corrupted(chunk, |b| {
                b[69..77].copy_from_slice(&u64::MAX.to_le_bytes())
            });
            assert_eq!(slot, Err(MalformedPacket::SlotOutOfRange(u64::MAX)));
            let epoch = corrupted(chunk, |b| b[77] = 1);
            assert_eq!(epoch, Err(MalformedPacket::NonZeroEpoch(1)));
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
                test_util::{MESSAGE_LEN, SLOT, author, epoch_handle},
            },
            SymbolDecoder as _, SymbolEncoder as _,
        },
        *,
    };
    use crate::spec::MAX_VALIDATOR_SET_SIZE;

    fn scheme() -> D25 {
        for_message(SLOT, MESSAGE_LEN, 0, 4).expect("fits")
    }

    #[test]
    fn for_message_picks_the_least_depth_that_fits() {
        // 2 source chunks at 2.5x is 5, plus one rounding chunk per
        // owner: 8 leaves for 3 owners (depth 4), 16 for 11 (depth 5)
        assert_eq!(scheme().depth, 4);
        assert_eq!(scheme().num_source_chunks(), 2);
        assert_eq!(for_message(SLOT, MESSAGE_LEN, 0, 12).unwrap().depth, 5);

        assert!(for_message(SLOT, 0, 0, 4).is_none());
        assert!(for_message(SLOT, MAX_MESSAGE_LEN + 1, 0, 4).is_none());
        // a message in range that no depth can carry
        assert!(for_message(SLOT, MAX_MESSAGE_LEN, 0, 20_000).is_none());
    }

    #[test]
    fn the_max_depth_is_the_one_the_largest_proposal_needs() {
        let deepest = for_message(SLOT, MAX_MESSAGE_LEN, 0, MAX_VALIDATOR_SET_SIZE);
        assert_eq!(deepest.expect("fits").depth, header::MAX_DEPTH);
    }

    #[test]
    fn the_canonical_scheme_is_the_one_for_message_picks() {
        for msg_len in [1, 1000, MESSAGE_LEN, 5000, 100_000, 1_000_000] {
            for num_validators in [2, 4, 12, 100, 500] {
                let scheme = for_message(SLOT, msg_len, 7, num_validators).expect("fits");
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
            assert_eq!(symbol.len(), scheme().v1_layout().symbol_len());
        }

        let mut decoder = scheme().decoder(6);
        decoder.ingest(ChunkId::unchecked(0), &symbols[0]);
        decoder.ingest(ChunkId::unchecked(1), &symbols[1]);
        assert!(decoder.try_decode().is_none());
        decoder.ingest(ChunkId::unchecked(2), &symbols[2]);
        assert_eq!(decoder.try_decode(), Some(Bytes::from(message)));
    }
}
