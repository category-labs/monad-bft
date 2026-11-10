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

//! Hollow schemes: a real scheme's assignment and messages, without its
//! chunk data on the wire, for simulations.
//!
//! A hollow chunk carries a tag in place of its symbol and proof:
//! `tag(root, chunk id)`. A chunk whose tag is not the one of its root and
//! id is rejected as a bad proof, as a tampered, misplaced or foreign real
//! chunk would be. Whether a proposal's chunks decode is stated by its
//! proposer in the signed header (`HollowD33::decodes`): real chunks show
//! it only at decoding, when the decoded message does or does not
//! re-encode to the root. So every outcome follows from the packets a node
//! received: a proposal resolves once `k` distinct valid chunks arrived,
//! to its message if it decodes and to `InvalidEncoding` if not.
//!
//! The messages themselves are not on the wire. The proposer publishes
//! its message in a store shared by the nodes of the process, and a
//! decoding node takes it from there. The store supplies bytes only: a
//! node that does not find the message (it lives in another process, or
//! every node of this one dropped it) resolves to a zero-filled payload of
//! the message's length. Nothing in a simulation reads payload contents.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, LazyLock, Mutex, MutexGuard, Weak},
};

use bytes::{Buf, BufMut, Bytes};

use super::{
    super::{
        assignment::{ChunkAssignment, ChunkId},
        chunk::{ChunkData, WireChunkId},
        types::{D33, EncodingScheme, HollowD33, MerkleRoot, NodeId, ValidatorData},
        wire::{DAHeaderScheme, MalformedPacket, v1},
    },
    BadProof, DAEncodingScheme, InvalidEncoding, ProposalChunks, Resolved, d33,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Hollow<S> {
    scheme: S,
    // whether the proposal's chunks decode: false for a Byzantine
    // proposer whose chunks form no codeword
    decodes: bool,
}

impl<S> Hollow<S> {
    pub(crate) fn new(scheme: S, decodes: bool) -> Self {
        Self { scheme, decodes }
    }
}

impl From<Hollow<D33>> for EncodingScheme {
    fn from(hollow: Hollow<D33>) -> Self {
        Self::D33Hollow(HollowD33 {
            d33: hollow.scheme,
            decodes: hollow.decodes,
        })
    }
}

impl From<HollowD33> for Hollow<D33> {
    fn from(hollow: HollowD33) -> Self {
        Self::new(hollow.d33, hollow.decodes)
    }
}

impl Hollow<D33> {
    // the variant byte of a proposal whose chunks do not decode
    pub(crate) const UNDECODABLE_VARIANT: u8 = Self::VARIANT | 0x40;

    fn variant(&self) -> u8 {
        if self.decodes {
            Self::VARIANT
        } else {
            Self::UNDECODABLE_VARIANT
        }
    }

    pub(crate) fn is_variant(variant: u8) -> bool {
        variant == Self::VARIANT || variant == Self::UNDECODABLE_VARIANT
    }
}

impl DAHeaderScheme for Hollow<D33> {
    const VARIANT: u8 = D33::VARIANT | 0x80;
    const PROPOSAL_HEADER_LEN: usize = D33::PROPOSAL_HEADER_LEN;
    const CHUNK_HEADER_LEN: usize = D33::CHUNK_HEADER_LEN;

    type Frame<'a> = v1::HollowLayout<'a, Self>;

    fn pack_header(&self, root: &MerkleRoot, out: &mut impl BufMut) {
        d33::header::pack(&self.scheme, self.variant(), root, out);
    }

    fn unpack_header(bytes: &mut impl Buf) -> Result<(MerkleRoot, Self), MalformedPacket> {
        let (root, d33, variant) = d33::header::unpack(bytes)?;
        let decodes = match variant {
            Self::VARIANT => true,
            Self::UNDECODABLE_VARIANT => false,
            other => return Err(MalformedPacket::UnknownScheme(other)),
        };
        Ok((root, Self::new(d33, decodes)))
    }

    fn pack_chunk_header(chunk_id: WireChunkId, out: &mut impl BufMut) {
        D33::pack_chunk_header(chunk_id, out);
    }

    fn unpack_chunk_header(bytes: &mut impl Buf) -> Result<WireChunkId, MalformedPacket> {
        D33::unpack_chunk_header(bytes)
    }

    fn depth(&self) -> u8 {
        self.scheme.depth()
    }

    fn v1_layout(&self) -> v1::HollowLayout<'_, Self> {
        v1::HollowLayout::new(self)
    }
}

impl<S: DAEncodingScheme> DAEncodingScheme for Hollow<S> {
    type Chunks = HollowChunks;

    fn msg_len(&self) -> usize {
        self.scheme.msg_len()
    }

    fn num_source_chunks(&self) -> usize {
        self.scheme.num_source_chunks()
    }

    fn is_canonical(&self, num_validators: usize) -> bool {
        self.scheme.is_canonical(num_validators)
    }

    fn chunk_assignment(&self, author: &NodeId, validator_data: &ValidatorData) -> ChunkAssignment {
        self.scheme.chunk_assignment(author, validator_data)
    }

    // the real chunks, once, for the real root
    fn encode(&self, message: &Bytes, num_chunks: usize) -> Option<HollowChunks> {
        let chunks = self.scheme.encode(message, num_chunks)?;
        let shared = SharedMessage::publish(chunks.root(), message.clone());
        Some(HollowChunks::Decoded {
            root: shared.root,
            _shared: Some(shared),
        })
    }

    fn empty_chunks(&self, root: MerkleRoot, num_chunks: usize) -> HollowChunks {
        let num_source_chunks = self.num_source_chunks();
        assert!(0 < num_source_chunks && num_source_chunks < num_chunks);
        HollowChunks::Decoding {
            root,
            decodes: self.decodes,
            msg_len: self.msg_len(),
            num_source_chunks,
            received: BTreeSet::new(),
            shared: SharedMessage::lookup(&root),
        }
    }
}

// the length of a hollow chunk's tag
pub(crate) const TAG_LEN: usize = 8;

// the tag of chunk `chunk_id` of the proposal under `root`: what a hollow
// chunk carries in place of its symbol and proof
pub(crate) fn tag(root: &MerkleRoot, chunk_id: ChunkId) -> Bytes {
    use crate::spec::DAMerkleRoot as _;

    let mut preimage = Vec::with_capacity(crate::spec::ROOT_LEN + 2);
    root.to_bytes(&mut preimage);
    preimage.extend_from_slice(&chunk_id.to_wire().to_le_bytes());
    let tag = monad_mcp_common::stub::noncrypto_digest(&preimage);
    Bytes::copy_from_slice(&tag.to_le_bytes())
}

pub(crate) enum HollowChunks {
    Decoding {
        root: MerkleRoot,
        decodes: bool,
        msg_len: usize,
        num_source_chunks: usize,
        received: BTreeSet<ChunkId>,
        // the message, when this process holds it
        shared: Option<Arc<SharedMessage>>,
    },
    // every chunk is derivable
    Decoded {
        root: MerkleRoot,
        // keeps the message in the store for nodes that decode later
        _shared: Option<Arc<SharedMessage>>,
    },
    // the proposal's chunks do not decode
    Invalid {
        root: MerkleRoot,
        received: BTreeSet<ChunkId>,
    },
}

impl ProposalChunks for HollowChunks {
    fn root(&self) -> MerkleRoot {
        match self {
            Self::Decoding { root, .. }
            | Self::Decoded { root, .. }
            | Self::Invalid { root, .. } => *root,
        }
    }

    fn contains(&self, chunk_id: ChunkId) -> bool {
        match self {
            Self::Decoding { received, .. } | Self::Invalid { received, .. } => {
                received.contains(&chunk_id)
            }
            Self::Decoded { .. } => true,
        }
    }

    // a chunk is valid when its tag is the one of its root and id
    fn ingest(&mut self, chunk_id: ChunkId, data: ChunkData) -> Result<bool, BadProof> {
        let Self::Decoding { root, received, .. } = self else {
            return Ok(false);
        };
        if received.contains(&chunk_id) {
            return Ok(false);
        }
        if data.symbol != tag(root, chunk_id) || !data.proof.is_empty() {
            return Err(BadProof);
        }
        Ok(received.insert(chunk_id))
    }

    fn try_decode(&mut self) -> Option<Resolved> {
        let Self::Decoding {
            root,
            decodes,
            msg_len,
            num_source_chunks,
            received,
            shared,
        } = self
        else {
            return None;
        };
        if received.len() < *num_source_chunks {
            return None;
        }
        let root = *root;
        if !*decodes {
            let received = std::mem::take(received);
            *self = Self::Invalid { root, received };
            return Some(Err(InvalidEncoding));
        }
        let shared = shared.take();
        let message = match &shared {
            Some(shared) => shared.message.clone(),
            None => zero_payload(*msg_len),
        };
        *self = Self::Decoded {
            root,
            _shared: shared,
        };
        Some(Ok(message))
    }

    fn is_resolved(&self) -> bool {
        !matches!(self, Self::Decoding { .. })
    }

    fn chunk_data(&self, chunk_id: ChunkId) -> Option<ChunkData> {
        self.contains(chunk_id)
            .then(|| ChunkData::hollow(tag(&self.root(), chunk_id)))
    }
}

// a payload of `len` zero bytes, without allocating
fn zero_payload(len: usize) -> Bytes {
    static ZEROS: [u8; d33::MAX_MESSAGE_LEN] = [0; d33::MAX_MESSAGE_LEN];
    Bytes::from_static(&ZEROS[..len])
}

static THIN_AIR: LazyLock<Mutex<HashMap<MerkleRoot, Weak<SharedMessage>>>> =
    LazyLock::new(Default::default);

fn thin_air() -> MutexGuard<'static, HashMap<MerkleRoot, Weak<SharedMessage>>> {
    THIN_AIR.lock().expect("no thread panics holding thin air")
}

pub(crate) struct SharedMessage {
    root: MerkleRoot,
    message: Bytes,
}

impl SharedMessage {
    // a root determines its message, so a live one is reused
    fn publish(root: MerkleRoot, message: Bytes) -> Arc<Self> {
        let mut thin_air = thin_air();
        if let Some(shared) = thin_air.get(&root).and_then(Weak::upgrade) {
            return shared;
        }
        let shared = Arc::new(Self { root, message });
        thin_air.insert(root, Arc::downgrade(&shared));
        shared
    }

    fn lookup(root: &MerkleRoot) -> Option<Arc<Self>> {
        thin_air().get(root).and_then(Weak::upgrade)
    }
}

impl Drop for SharedMessage {
    fn drop(&mut self) {
        let mut thin_air = thin_air();
        let in_use = thin_air
            .get(&self.root)
            .is_some_and(|entry| entry.strong_count() > 0);
        if !in_use {
            thin_air.remove(&self.root);
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::{SeedableRng as _, seq::SliceRandom as _};
    use rand_chacha::ChaCha20Rng;

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
    use crate::spec::DAMerkleRoot as _;

    fn scheme_for(msg_len: usize) -> D33 {
        for_message(SLOT, msg_len, 0, Lane::new_unchecked(0), 4).expect("fits")
    }

    fn chunk_ids(num_chunks: usize) -> impl Iterator<Item = ChunkId> {
        (0..num_chunks as u16).map(ChunkId::unchecked)
    }

    // every chunk twice, in an order fixed by the seed
    fn arrivals(num_chunks: usize, seed: u64) -> Vec<ChunkId> {
        let mut arrivals: Vec<_> = chunk_ids(num_chunks).chain(chunk_ids(num_chunks)).collect();
        arrivals.shuffle(&mut ChaCha20Rng::seed_from_u64(seed));
        arrivals
    }

    fn hollow_data(root: &MerkleRoot, chunk_id: ChunkId) -> ChunkData {
        ChunkData::hollow(tag(root, chunk_id))
    }

    fn assert_hollow_of(hollow: &HollowChunks, real: &impl ProposalChunks, num_chunks: usize) {
        assert_eq!(hollow.root(), real.root());
        assert_eq!(hollow.is_resolved(), real.is_resolved());
        for chunk_id in chunk_ids(num_chunks) {
            assert_eq!(hollow.contains(chunk_id), real.contains(chunk_id));
            let hollowed = real
                .chunk_data(chunk_id)
                .map(|_| hollow_data(&real.root(), chunk_id));
            assert_eq!(hollow.chunk_data(chunk_id), hollowed);
        }
    }

    // the symbols of every chunk, in chunk id order
    fn symbols_of(chunks: &impl ProposalChunks, num_chunks: usize) -> Vec<Bytes> {
        chunk_ids(num_chunks)
            .map(|chunk_id| chunks.chunk_data(chunk_id).expect("complete").symbol)
            .collect()
    }

    // feed one chunk to both and require the same outcome
    fn ingest_both(
        real: &mut impl ProposalChunks,
        hollow: &mut HollowChunks,
        chunk_id: ChunkId,
        real_data: ChunkData,
        hollow_data: ChunkData,
        num_chunks: usize,
    ) {
        let ingested = real.ingest(chunk_id, real_data);
        assert_eq!(hollow.ingest(chunk_id, hollow_data), ingested);
        assert_eq!(hollow.try_decode(), real.try_decode());
        assert_hollow_of(hollow, real, num_chunks);
    }

    #[test]
    fn removing_the_data_commutes_with_every_operation() {
        for msg_len in [MESSAGE_LEN, 5000] {
            let scheme = scheme_for(msg_len);
            let hollow = Hollow::new(scheme, true);
            let num_chunks = 3 * scheme.num_source_chunks();
            let message = Bytes::from(vec![7u8; msg_len]);
            let foreign_message = Bytes::from(vec![8u8; msg_len]);

            let real = scheme.encode(&message, num_chunks).expect("of the length");
            let encoded = hollow.encode(&message, num_chunks).expect("of the length");
            assert_hollow_of(&encoded, &real, num_chunks);
            let root = real.root();
            let foreign = scheme.encode(&foreign_message, num_chunks).expect("len");

            for seed in 0..32 {
                let mut receiving = scheme.empty_chunks(root, num_chunks);
                let mut hollow_receiving = hollow.empty_chunks(root, num_chunks);
                for (i, chunk_id) in arrivals(num_chunks, seed).into_iter().enumerate() {
                    // a bad chunk before every few good ones: tampered, under
                    // another id, or of another proposal
                    let other_id = ChunkId::unchecked((chunk_id.to_wire() + 1) % num_chunks as u16);
                    let (bad_real, bad_hollow) = match i % 6 {
                        0 => {
                            let mut data = real.chunk_data(chunk_id).expect("derivable");
                            let mut symbol = data.symbol.to_vec();
                            symbol[0] ^= 1;
                            data.symbol = Bytes::from(symbol);
                            let mut tag = tag(&root, chunk_id).to_vec();
                            tag[0] ^= 1;
                            (data, ChunkData::hollow(Bytes::from(tag)))
                        }
                        2 => (
                            real.chunk_data(other_id).expect("derivable"),
                            hollow_data(&root, other_id),
                        ),
                        4 => (
                            foreign.chunk_data(chunk_id).expect("derivable"),
                            hollow_data(&foreign.root(), chunk_id),
                        ),
                        _ => (
                            real.chunk_data(chunk_id).expect("derivable"),
                            hollow_data(&root, chunk_id),
                        ),
                    };
                    let (receiving, hollow_receiving) = (&mut receiving, &mut hollow_receiving);
                    ingest_both(
                        receiving,
                        hollow_receiving,
                        chunk_id,
                        bad_real,
                        bad_hollow,
                        num_chunks,
                    );
                    let data = real.chunk_data(chunk_id).expect("derivable");
                    ingest_both(
                        receiving,
                        hollow_receiving,
                        chunk_id,
                        data,
                        hollow_data(&root, chunk_id),
                        num_chunks,
                    );
                }
                assert!(hollow_receiving.is_resolved());
            }
        }
    }

    #[test]
    fn bad_chunks_are_rejected_as_bad_proofs() {
        let scheme = scheme_for(MESSAGE_LEN);
        let num_chunks = 3 * scheme.num_source_chunks();
        let root = MerkleRoot::from_bytes(&[3; 20]).expect("20 bytes");
        let other_root = MerkleRoot::from_bytes(&[4; 20]).expect("20 bytes");
        let mut chunks = Hollow::new(scheme, true).empty_chunks(root, num_chunks);
        let id = |wire| ChunkId::unchecked(wire);

        let mut flipped = tag(&root, id(0)).to_vec();
        flipped[7] ^= 0x80;
        let bad = [
            (id(0), ChunkData::hollow(Bytes::from(flipped))),
            (id(0), hollow_data(&root, id(1))),
            (id(0), hollow_data(&other_root, id(0))),
            (id(0), ChunkData::hollow(Bytes::new())),
        ];
        for (chunk_id, data) in bad {
            assert_eq!(chunks.ingest(chunk_id, data), Err(BadProof));
            assert!(!chunks.contains(chunk_id));
        }
        assert_eq!(chunks.ingest(id(0), hollow_data(&root, id(0))), Ok(true));
        assert_eq!(chunks.ingest(id(0), hollow_data(&root, id(0))), Ok(false));
    }

    // real chunks that form no codeword, and a hollow proposal that says
    // so, resolve invalid at the same chunk, whichever chunks arrive
    #[test]
    fn an_inconsistent_proposal_resolves_invalid_like_its_real_counterpart() {
        let scheme = scheme_for(MESSAGE_LEN);
        let num_chunks = 3 * scheme.num_source_chunks();
        let message = Bytes::from(vec![7u8; MESSAGE_LEN]);
        let honest = scheme.encode(&message, num_chunks).expect("of the length");

        // one recovery symbol altered: the tree is valid, the word is not
        let mut symbols = symbols_of(&honest, num_chunks);
        let altered = num_chunks - 1;
        let mut symbol = symbols[altered].to_vec();
        symbol[0] ^= 1;
        symbols[altered] = Bytes::from(symbol);
        let inconsistent = D33Chunks::from_symbols(&scheme, symbols);
        let root = inconsistent.root();
        let hollow = Hollow::new(scheme, false);

        for seed in 0..32 {
            let mut receiving = scheme.empty_chunks(root, num_chunks);
            let mut hollow_receiving = hollow.empty_chunks(root, num_chunks);
            let mut resolved = Vec::new();
            for chunk_id in arrivals(num_chunks, seed) {
                let data = inconsistent.chunk_data(chunk_id).expect("derivable");
                let ingested = receiving.ingest(chunk_id, data);
                assert_eq!(
                    hollow_receiving.ingest(chunk_id, hollow_data(&root, chunk_id)),
                    ingested
                );
                let decoded = receiving.try_decode();
                assert_eq!(hollow_receiving.try_decode(), decoded);
                resolved.extend(decoded);
                assert_hollow_of(&hollow_receiving, &receiving, num_chunks);
            }
            assert_eq!(resolved, [Err(InvalidEncoding)]);
        }
    }

    #[test]
    fn a_proposal_decodes_without_its_message_in_the_process() {
        let scheme = scheme_for(MESSAGE_LEN);
        let num_chunks = 3 * scheme.num_source_chunks();
        // a root no node of this process encoded
        let root = MerkleRoot::from_bytes(&[5; 20]).expect("20 bytes");
        let mut chunks = Hollow::new(scheme, true).empty_chunks(root, num_chunks);

        for chunk_id in chunk_ids(scheme.num_source_chunks()) {
            assert_eq!(chunks.try_decode(), None);
            let ingested = chunks.ingest(chunk_id, hollow_data(&root, chunk_id));
            assert_eq!(ingested, Ok(true));
        }
        let zeros = Bytes::from(vec![0u8; MESSAGE_LEN]);
        assert_eq!(chunks.try_decode(), Some(Ok(zeros)));
        assert_eq!(chunks.try_decode(), None);
        let last = ChunkId::unchecked(num_chunks as u16 - 1);
        assert_eq!(chunks.chunk_data(last), Some(hollow_data(&root, last)));
    }

    #[test]
    fn the_store_supplies_the_message_while_any_node_holds_it() {
        let hollow = Hollow::new(scheme_for(MESSAGE_LEN), true);
        let num_chunks = 9;
        // a payload no other test publishes, so no other test holds the root
        let message = Bytes::from(vec![0x41; MESSAGE_LEN]);
        let deliver_all = |chunks: &mut HollowChunks| {
            let root = chunks.root();
            for chunk_id in chunk_ids(num_chunks) {
                chunks
                    .ingest(chunk_id, hollow_data(&root, chunk_id))
                    .expect("its tag");
            }
            chunks.try_decode()
        };

        let encoded = hollow.encode(&message, num_chunks).expect("of the length");
        let root = encoded.root();
        let mut early = hollow.empty_chunks(root, num_chunks);
        drop(encoded);
        assert_eq!(deliver_all(&mut early), Some(Ok(message.clone())));

        // a decoded node still holds the message for a later one
        let mut late = hollow.empty_chunks(root, num_chunks);
        drop(early);
        assert_eq!(deliver_all(&mut late), Some(Ok(message)));

        // once no node holds it, a node still decodes, to zeros: the store
        // supplies bytes, never the outcome
        drop(late);
        assert!(!thin_air().contains_key(&root));
        let mut orphan = hollow.empty_chunks(root, num_chunks);
        let zeros = Bytes::from(vec![0u8; MESSAGE_LEN]);
        assert_eq!(deliver_all(&mut orphan), Some(Ok(zeros)));
    }

    // an equivocating proposer may send one proposal that decodes and one
    // that does not: two pids, each with its own outcome
    #[test]
    fn an_equivocation_resolves_each_proposal_by_its_own_flag() {
        use monad_mcp_cadence::spec::ProposalHeader as _;

        use super::super::super::types::ProposalHeader;

        let scheme = scheme_for(MESSAGE_LEN);
        let num_chunks = 3 * scheme.num_source_chunks();
        let message = Bytes::from(vec![9u8; MESSAGE_LEN]);
        let header = |decodes| {
            let hollow = Hollow::new(scheme, decodes);
            let encoded = hollow.encode(&message, num_chunks).expect("of the length");
            let header = ProposalHeader {
                root: encoded.root(),
                scheme: hollow.into(),
            };
            (hollow, header, encoded)
        };
        let (decoding, decoding_header, _held) = header(true);
        let (undecodable, undecodable_header, _held_too) = header(false);
        assert_eq!(decoding_header.root, undecodable_header.root);
        assert_ne!(decoding_header.pid(), undecodable_header.pid());

        for (hollow, header, expected) in [
            (decoding, decoding_header, Ok(message.clone())),
            (undecodable, undecodable_header, Err(InvalidEncoding)),
        ] {
            let mut chunks = hollow.empty_chunks(header.root, num_chunks);
            for chunk_id in chunk_ids(scheme.num_source_chunks()) {
                let ingested = chunks.ingest(chunk_id, hollow_data(&header.root, chunk_id));
                assert_eq!(ingested, Ok(true));
            }
            assert_eq!(chunks.try_decode(), Some(expected));
        }
    }
}
