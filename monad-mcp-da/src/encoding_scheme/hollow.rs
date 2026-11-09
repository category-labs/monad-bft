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

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, LazyLock, Mutex, MutexGuard, Weak},
};

use bytes::{Buf, BufMut, Bytes};

use super::{
    super::{
        assignment::{ChunkAssignment, ChunkId},
        chunk::{ChunkData, WireChunkId},
        types::{D33, EncodingScheme, MerkleRoot, NodeId, ValidatorData},
        wire::{DAHeaderScheme, MalformedPacket, v1},
    },
    BadProof, DAEncodingScheme, ProposalChunks, Resolved, d33,
};

// S with the chunk data taken out, for tests: the same shape, root and
// decode rule. A decoder takes the message from thin air.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Hollow<S>(S);

impl<S> Hollow<S> {
    pub(crate) fn new(scheme: S) -> Self {
        Self(scheme)
    }
}

impl From<Hollow<D33>> for EncodingScheme {
    fn from(hollow: Hollow<D33>) -> Self {
        Self::D33Hollow(hollow.0)
    }
}

// d33's header under its own variant, in the hollow frame
impl DAHeaderScheme for Hollow<D33> {
    const VARIANT: u8 = D33::VARIANT | 0x80;
    const PROPOSAL_HEADER_LEN: usize = D33::PROPOSAL_HEADER_LEN;
    const CHUNK_HEADER_LEN: usize = D33::CHUNK_HEADER_LEN;

    type Frame<'a> = v1::HollowLayout<'a, Self>;

    fn pack_header(&self, root: &MerkleRoot, out: &mut impl BufMut) {
        d33::header::pack::<Self>(&self.0, root, out);
    }

    fn unpack_header(bytes: &mut impl Buf) -> Result<(MerkleRoot, Self), MalformedPacket> {
        let (root, d33) = d33::header::unpack::<Self>(bytes)?;
        Ok((root, Self(d33)))
    }

    fn pack_chunk_header(chunk_id: WireChunkId, out: &mut impl BufMut) {
        D33::pack_chunk_header(chunk_id, out);
    }

    fn unpack_chunk_header(bytes: &mut impl Buf) -> Result<WireChunkId, MalformedPacket> {
        D33::unpack_chunk_header(bytes)
    }

    fn depth(&self) -> u8 {
        self.0.depth()
    }

    fn v1_layout(&self) -> v1::HollowLayout<'_, Self> {
        v1::HollowLayout::new(self)
    }
}

impl<S: DAEncodingScheme> DAEncodingScheme for Hollow<S> {
    type Chunks = HollowChunks;

    fn msg_len(&self) -> usize {
        self.0.msg_len()
    }

    fn num_source_chunks(&self) -> usize {
        self.0.num_source_chunks()
    }

    fn is_canonical(&self, num_validators: usize) -> bool {
        self.0.is_canonical(num_validators)
    }

    fn chunk_assignment(&self, author: &NodeId, validator_data: &ValidatorData) -> ChunkAssignment {
        self.0.chunk_assignment(author, validator_data)
    }

    // the real chunks, once, for the real root
    fn encode(&self, message: &Bytes, num_chunks: usize) -> Option<HollowChunks> {
        let chunks = self.0.encode(message, num_chunks)?;
        let shared = SharedMessage::publish(chunks.root(), message.clone());
        Some(HollowChunks::Decoded(shared))
    }

    fn empty_chunks(&self, root: MerkleRoot, num_chunks: usize) -> HollowChunks {
        HollowChunks::new(root, self.num_source_chunks(), num_chunks)
    }
}

// the chunk ids of a hollow proposal. any num_source_chunks of them
// decode, which is exact for an MDS code such as Reed-Solomon.
pub(crate) enum HollowChunks {
    Decoding {
        root: MerkleRoot,
        num_source_chunks: usize,
        received: BTreeSet<ChunkId>,
        // None when no node holds the message, as under a forged root
        shared: Option<Arc<SharedMessage>>,
    },
    // every chunk is derivable
    Decoded(Arc<SharedMessage>),
}

impl HollowChunks {
    fn new(root: MerkleRoot, num_source_chunks: usize, num_chunks: usize) -> Self {
        assert!(0 < num_source_chunks && num_source_chunks < num_chunks);
        Self::Decoding {
            root,
            num_source_chunks,
            received: BTreeSet::new(),
            shared: SharedMessage::lookup(&root),
        }
    }
}

impl ProposalChunks for HollowChunks {
    fn root(&self) -> MerkleRoot {
        match self {
            Self::Decoding { root, .. } => *root,
            Self::Decoded(shared) => shared.root,
        }
    }

    fn contains(&self, chunk_id: ChunkId) -> bool {
        match self {
            Self::Decoding { received, .. } => received.contains(&chunk_id),
            Self::Decoded(_) => true,
        }
    }

    // a hollow chunk has no proof to fail
    fn ingest(&mut self, chunk_id: ChunkId, _data: ChunkData) -> Result<bool, BadProof> {
        let Self::Decoding { received, .. } = self else {
            return Ok(false);
        };
        Ok(received.insert(chunk_id))
    }

    fn try_decode(&mut self) -> Option<Resolved> {
        let Self::Decoding {
            num_source_chunks,
            received,
            shared,
            ..
        } = self
        else {
            return None;
        };
        if received.len() < *num_source_chunks {
            return None;
        }
        let shared = shared.take()?;
        let message = shared.message.clone();
        *self = Self::Decoded(shared);
        Some(Ok(message))
    }

    fn is_resolved(&self) -> bool {
        matches!(self, Self::Decoded(_))
    }

    fn chunk_data(&self, chunk_id: ChunkId) -> Option<ChunkData> {
        self.contains(chunk_id).then(ChunkData::hollow)
    }
}

// the messages of the hollow proposals that some node holds, by root
static THIN_AIR: LazyLock<Mutex<HashMap<MerkleRoot, Weak<SharedMessage>>>> =
    LazyLock::new(Default::default);

fn thin_air() -> MutexGuard<'static, HashMap<MerkleRoot, Weak<SharedMessage>>> {
    THIN_AIR.lock().expect("no thread panics holding thin air")
}

// a proposal's message, shared by every node that holds its hollow
// chunks. it leaves thin air with its last holder.
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
    use super::{
        super::{
            super::{
                test_util::{MESSAGE_LEN, SLOT},
                types::{D33, Lane},
            },
            d33::for_message,
        },
        *,
    };

    fn scheme_for(msg_len: usize) -> D33 {
        for_message(SLOT, msg_len, 0, Lane::new_unchecked(0), 4).expect("fits")
    }

    fn chunk_ids(num_chunks: usize) -> impl Iterator<Item = ChunkId> {
        (0..num_chunks as u16).map(ChunkId::unchecked)
    }

    fn assert_hollow_of(hollow: &HollowChunks, real: &impl ProposalChunks, num_chunks: usize) {
        assert_eq!(hollow.root(), real.root());
        assert_eq!(hollow.is_resolved(), real.is_resolved());
        for chunk_id in chunk_ids(num_chunks) {
            assert_eq!(hollow.contains(chunk_id), real.contains(chunk_id));
            let hollowed = real.chunk_data(chunk_id).map(|_| ChunkData::hollow());
            assert_eq!(hollow.chunk_data(chunk_id), hollowed);
        }
    }

    #[test]
    fn removing_the_data_commutes_with_every_operation() {
        use rand::{SeedableRng as _, seq::SliceRandom as _};
        use rand_chacha::ChaCha20Rng;

        for msg_len in [MESSAGE_LEN, 5000] {
            let scheme = scheme_for(msg_len);
            let hollow = Hollow::new(scheme);
            let num_chunks = 3 * scheme.num_source_chunks();
            let message = Bytes::from(vec![7u8; msg_len]);

            let real = scheme.encode(&message, num_chunks).expect("of the length");
            let encoded = hollow.encode(&message, num_chunks).expect("of the length");
            assert_hollow_of(&encoded, &real, num_chunks);

            for seed in 0..32 {
                // every chunk twice, in any order
                let mut arrivals: Vec<_> =
                    chunk_ids(num_chunks).chain(chunk_ids(num_chunks)).collect();
                arrivals.shuffle(&mut ChaCha20Rng::seed_from_u64(seed));

                let mut receiving = scheme.empty_chunks(real.root(), num_chunks);
                let mut hollow_receiving = hollow.empty_chunks(real.root(), num_chunks);
                for chunk_id in arrivals {
                    let data = real.chunk_data(chunk_id).expect("derivable");
                    let ingested = receiving.ingest(chunk_id, data);
                    assert_eq!(
                        hollow_receiving.ingest(chunk_id, ChunkData::hollow()),
                        ingested
                    );
                    assert_eq!(hollow_receiving.try_decode(), receiving.try_decode());
                    assert_hollow_of(&hollow_receiving, &receiving, num_chunks);
                }
                assert!(hollow_receiving.is_resolved());
            }
        }
    }

    #[test]
    fn a_message_stays_in_thin_air_while_any_node_holds_its_chunks() {
        let hollow = Hollow::new(scheme_for(MESSAGE_LEN));
        let num_chunks = 9;
        // a payload no other test publishes, so no other test holds the root
        let message = Bytes::from(vec![0x41; MESSAGE_LEN]);
        let deliver_all = |chunks: &mut HollowChunks| {
            for chunk_id in chunk_ids(num_chunks) {
                chunks
                    .ingest(chunk_id, ChunkData::hollow())
                    .expect("no proof");
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

        drop(late);
        assert!(!thin_air().contains_key(&root));
        let mut orphan = hollow.empty_chunks(root, num_chunks);
        assert_eq!(deliver_all(&mut orphan), None);
    }
}
