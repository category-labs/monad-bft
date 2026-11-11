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
        types::{D33, EncodingScheme, MerkleHash, MerkleRoot, NodeId, ValidatorData},
        wire::{DAHeaderScheme, MalformedPacket, v1},
    },
    BadProof, DAEncodingScheme, Decoder, Encoded, d33,
};

// S with the chunk data taken out, for tests: the same shape and decode
// rule. The root is a digest of the message, and a decoder takes the
// message from thin air.
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
    type Decoder = HollowDecoder;

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

    fn encode(&self, message: &Bytes, _num_chunks: usize) -> Option<HollowEncoded> {
        if message.is_empty() || message.len() != self.msg_len() {
            return None;
        }
        let shared = SharedMessage::publish(message.clone());
        Some(shared)
    }

    fn decoder(&self, root: MerkleRoot, num_chunks: usize) -> HollowDecoder {
        HollowDecoder::new(root, self.num_source_chunks(), num_chunks)
    }
}

// the chunk ids received under a hollow root. any num_source_chunks of
// them decode, which is exact for an MDS code such as Reed-Solomon.
pub(crate) struct HollowDecoder {
    root: MerkleRoot,
    num_source_chunks: usize,
    received: BTreeSet<ChunkId>,
    // None when no node holds the message, as under a forged root
    shared: Option<HollowEncoded>,
}

impl HollowDecoder {
    fn new(root: MerkleRoot, num_source_chunks: usize, num_chunks: usize) -> Self {
        assert!(0 < num_source_chunks && num_source_chunks <= num_chunks);
        Self {
            root,
            num_source_chunks,
            received: BTreeSet::new(),
            shared: SharedMessage::lookup(&root),
        }
    }
}

impl Decoder for HollowDecoder {
    type Encoded = HollowEncoded;

    fn root(&self) -> MerkleRoot {
        self.root
    }

    fn contains(&self, chunk_id: ChunkId) -> bool {
        self.received.contains(&chunk_id)
    }

    fn chunk_data(&self, chunk_id: ChunkId) -> Option<ChunkData> {
        if !self.contains(chunk_id) {
            return None;
        }
        let data = hollow_chunk(chunk_id, &self.root);
        Some(data)
    }

    fn ingest(&mut self, chunk_id: ChunkId, data: ChunkData) -> Result<(), BadProof> {
        if !verify_proof(chunk_id, &data, &self.root) {
            return Err(BadProof);
        }
        self.received.insert(chunk_id);
        Ok(())
    }

    fn try_decode(&mut self) -> Option<(Bytes, HollowEncoded)> {
        if self.received.len() < self.num_source_chunks {
            return None;
        }
        let shared = self.shared.clone()?;
        let message = shared.message.clone();
        Some((message, shared))
    }
}

// a published message is the encoding of itself, under the root it
// encodes to
pub(crate) type HollowEncoded = Arc<SharedMessage>;

impl Encoded for HollowEncoded {
    fn root(&self) -> MerkleRoot {
        self.actual
    }

    fn chunk_data(&self, chunk_id: ChunkId) -> ChunkData {
        hollow_chunk(chunk_id, &self.actual)
    }
}

// what a chunk under root carries: no symbol, and a one-element proof
fn hollow_chunk(chunk_id: ChunkId, root: &MerkleRoot) -> ChunkData {
    ChunkData {
        symbol: Bytes::new(),
        proof: Box::new([stand_in_proof(chunk_id, root)]),
    }
}

// the root with the chunk id folded into its last bytes. it binds the
// chunk to both without a hash.
fn stand_in_proof(chunk_id: ChunkId, root: &MerkleRoot) -> MerkleHash {
    let MerkleRoot(mut proof) = *root;
    let id = chunk_id.to_wire().to_le_bytes();
    let tail = proof.0.len() - id.len();
    for (byte, id_byte) in proof.0[tail..].iter_mut().zip(id) {
        *byte ^= id_byte;
    }
    proof
}

// whether the chunk's proof binds it to the root
fn verify_proof(chunk_id: ChunkId, data: &ChunkData, root: &MerkleRoot) -> bool {
    *data.proof == [stand_in_proof(chunk_id, root)]
}

// the messages of the hollow proposals that some node holds, by the root
// their proposer declared
static THIN_AIR: LazyLock<Mutex<HashMap<MerkleRoot, Weak<SharedMessage>>>> =
    LazyLock::new(Default::default);

fn thin_air() -> MutexGuard<'static, HashMap<MerkleRoot, Weak<SharedMessage>>> {
    THIN_AIR.lock().expect("no thread panics holding thin air")
}

// a proposal's message, shared by every node that holds its hollow
// chunks. it leaves thin air with its last holder.
pub(crate) struct SharedMessage {
    // the root the proposer signed, which is the key in thin air
    declared: MerkleRoot,
    // the root the message encodes to
    actual: MerkleRoot,
    message: Bytes,
}

impl SharedMessage {
    fn publish(message: Bytes) -> Arc<Self> {
        let root = root_of(&message);
        Self::publish_under(root, root, message)
    }

    // a message under a root that it does not encode to
    #[allow(unused)]
    fn publish_inconsistent(message: Bytes) -> Arc<Self> {
        let actual = root_of(&message);
        let MerkleRoot(mut declared) = actual;
        declared.0[0] ^= 1;
        Self::publish_under(MerkleRoot(declared), actual, message)
    }

    // a declared root determines what it was published with, so a live
    // one is reused
    fn publish_under(declared: MerkleRoot, actual: MerkleRoot, message: Bytes) -> Arc<Self> {
        let mut thin_air = thin_air();
        if let Some(shared) = thin_air.get(&declared).and_then(Weak::upgrade) {
            let same = shared.actual == actual && shared.message == message;
            assert!(same, "two publications share a declared root");
            return shared;
        }
        let shared = Arc::new(Self {
            declared,
            actual,
            message,
        });
        thin_air.insert(declared, Arc::downgrade(&shared));
        shared
    }

    fn lookup(root: &MerkleRoot) -> Option<Arc<Self>> {
        thin_air().get(root).and_then(Weak::upgrade)
    }
}

// the root a message encodes to under a hollow scheme: its digest
fn root_of(message: &[u8]) -> MerkleRoot {
    let digest = monad_mcp_common::stub::noncrypto_digest(message).to_le_bytes();
    let mut root = MerkleHash([0; 20]);
    root.0[..digest.len()].copy_from_slice(&digest);
    MerkleRoot(root)
}

impl Drop for SharedMessage {
    fn drop(&mut self) {
        let mut thin_air = thin_air();
        let in_use = thin_air
            .get(&self.declared)
            .is_some_and(|entry| entry.strong_count() > 0);
        if !in_use {
            thin_air.remove(&self.declared);
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
                wire::PacketLayout as _,
            },
            Chunks, ProposalChunks as _,
            d33::{D33Encoded, for_message},
        },
        *,
    };

    type HollowChunks = Chunks<HollowDecoder>;
    type RealChunks = Chunks<<D33 as DAEncodingScheme>::Decoder>;

    fn scheme_for(msg_len: usize) -> D33 {
        for_message(SLOT, msg_len, 0, Lane::new_unchecked(0), 4).expect("fits")
    }

    fn chunk_ids(num_chunks: usize) -> impl Iterator<Item = ChunkId> {
        (0..num_chunks as u16).map(ChunkId::unchecked)
    }

    // `hollow`, under `root`, holds what `real` holds, without the data
    fn assert_hollow_of(
        hollow: &HollowChunks,
        root: &MerkleRoot,
        real: &RealChunks,
        num_chunks: usize,
    ) {
        assert_eq!(hollow.is_resolved(), real.is_resolved());
        for chunk_id in chunk_ids(num_chunks) {
            assert_eq!(hollow.contains(chunk_id), real.contains(chunk_id));
            let real_data = real.chunk_data(chunk_id);
            let hollowed = real_data.map(|_| hollow_chunk(chunk_id, root));
            assert_eq!(hollow.chunk_data(chunk_id), hollowed);
        }
    }

    // delivers every chunk of `encoded` twice, in an order drawn from
    // `seed`, to a real receiver and to a hollow one under `hollow_root`.
    // returns the two after they resolved alike.
    fn receive_alike(
        scheme: D33,
        encoded: &D33Encoded,
        hollow_root: MerkleRoot,
        seed: u64,
    ) -> (RealChunks, HollowChunks) {
        let num_chunks = 3 * scheme.num_source_chunks();
        let real_decoder = scheme.decoder(encoded.root(), num_chunks);
        let hollow_decoder = Hollow::new(scheme).decoder(hollow_root, num_chunks);
        let mut receiving = Chunks::Decoding(real_decoder);
        let mut hollow_receiving = Chunks::Decoding(hollow_decoder);

        let mut arrivals: Vec<_> = chunk_ids(num_chunks).chain(chunk_ids(num_chunks)).collect();
        arrivals.shuffle(&mut ChaCha20Rng::seed_from_u64(seed));

        for chunk_id in arrivals {
            let data = encoded.chunk_data(chunk_id);
            let ingested = receiving.ingest(chunk_id, data);
            let hollow_data = hollow_chunk(chunk_id, &hollow_root);
            assert_eq!(hollow_receiving.ingest(chunk_id, hollow_data), ingested);
            assert_eq!(hollow_receiving.try_resolve(), receiving.try_resolve());
            assert_hollow_of(&hollow_receiving, &hollow_root, &receiving, num_chunks);
        }
        assert!(hollow_receiving.is_resolved());
        (receiving, hollow_receiving)
    }

    #[test]
    fn removing_the_data_commutes_with_every_operation() {
        for msg_len in [MESSAGE_LEN, 5000] {
            let scheme = scheme_for(msg_len);
            let hollow = Hollow::new(scheme);
            let num_chunks = 3 * scheme.num_source_chunks();
            let message = Bytes::from(vec![7u8; msg_len]);

            let real = scheme.encode(&message, num_chunks).expect("of the length");
            let encoded = hollow.encode(&message, num_chunks).expect("of the length");
            let hollow_root = encoded.root();

            let short = message.slice(1..);
            assert!(scheme.encode(&short, num_chunks).is_none());
            assert!(hollow.encode(&short, num_chunks).is_none());

            for seed in 0..32 {
                let (_, hollow_received) = receive_alike(scheme, &real, hollow_root, seed);
                assert!(matches!(hollow_received, Chunks::Encoded(_)));
            }

            let (real, encoded) = (Chunks::Encoded(real), Chunks::Encoded(encoded));
            assert_hollow_of(&encoded, &hollow_root, &real, num_chunks);
        }
    }

    #[test]
    fn a_chunk_that_does_not_bind_to_its_id_and_root_is_a_bad_proof() {
        let scheme = scheme_for(MESSAGE_LEN);
        let hollow = Hollow::new(scheme);
        let num_chunks = 3 * scheme.num_source_chunks();
        let message = |payload| Bytes::from(vec![payload; MESSAGE_LEN]);
        let encode = |payload| scheme.encode(&message(payload), num_chunks);
        let real = encode(0x43).expect("of the length");
        let other = encode(0x44).expect("of the length");
        let (root, other_root) = (root_of(&message(0x43)), root_of(&message(0x44)));
        let (first, second) = (ChunkId::unchecked(0), ChunkId::unchecked(1));

        let mut receiving = Chunks::Decoding(scheme.decoder(real.root(), num_chunks));
        let mut hollow_receiving = Chunks::Decoding(hollow.decoder(root, num_chunks));

        // a chunk delivered under another id
        let relabelled = real.chunk_data(second);
        assert_eq!(receiving.ingest(first, relabelled), Err(BadProof));
        let relabelled = hollow_chunk(second, &root);
        assert_eq!(hollow_receiving.ingest(first, relabelled), Err(BadProof));

        // a chunk of another proposal
        let foreign = other.chunk_data(first);
        assert_eq!(receiving.ingest(first, foreign), Err(BadProof));
        let foreign = hollow_chunk(first, &other_root);
        assert_eq!(hollow_receiving.ingest(first, foreign), Err(BadProof));

        // an altered proof
        let mut altered = real.chunk_data(first);
        altered.proof[0].0[0] ^= 1;
        assert_eq!(receiving.ingest(first, altered), Err(BadProof));
        let mut altered = hollow_chunk(first, &root);
        altered.proof[0].0[0] ^= 1;
        assert_eq!(hollow_receiving.ingest(first, altered), Err(BadProof));

        // a rejected chunk leaves the chunk new
        assert_hollow_of(&hollow_receiving, &root, &receiving, num_chunks);
        let honest = real.chunk_data(first);
        assert_eq!(receiving.ingest(first, honest), Ok(true));
        let honest = hollow_chunk(first, &root);
        assert_eq!(hollow_receiving.ingest(first, honest), Ok(true));
    }

    #[test]
    fn a_message_that_does_not_encode_to_its_declared_root_is_an_invalid_encoding() {
        let scheme = scheme_for(MESSAGE_LEN);
        let hollow = Hollow::new(scheme);
        let num_chunks = 3 * scheme.num_source_chunks();

        // real chunks that verify under their root and are no codeword
        let symbol_len = scheme.v1_layout().symbol_len();
        let mut symbols = Vec::with_capacity(num_chunks);
        for i in 0..num_chunks {
            let payload = if i < num_chunks / 2 { 1 } else { 2 };
            symbols.push(Bytes::from(vec![payload; symbol_len]));
        }
        let real = D33Encoded::from_symbols(&scheme, symbols);

        // a payload no other test publishes, so no other test holds the root
        let message = Bytes::from(vec![0x42; MESSAGE_LEN]);
        let honest = hollow.encode(&message, num_chunks).expect("of the length");
        let shared = SharedMessage::publish_inconsistent(message);
        let declared = shared.declared;
        assert_ne!(declared, honest.root());

        for seed in 0..32 {
            let (_, mut hollow_received) = receive_alike(scheme, &real, declared, seed);
            assert!(matches!(hollow_received, Chunks::Invalid(_)));
            assert_eq!(hollow_received.try_resolve(), None);
        }

        // it leaves thin air under the root it was declared with
        drop(shared);
        assert!(!thin_air().contains_key(&declared));
        assert!(thin_air().contains_key(&honest.root()));
    }

    #[test]
    fn a_message_stays_in_thin_air_while_any_node_holds_its_chunks() {
        let hollow = Hollow::new(scheme_for(MESSAGE_LEN));
        let num_chunks = 9;
        // a payload no other test publishes, so no other test holds the root
        let message = Bytes::from(vec![0x41; MESSAGE_LEN]);
        let deliver_all = |chunks: &mut HollowChunks, root: MerkleRoot| {
            for chunk_id in chunk_ids(num_chunks) {
                let data = hollow_chunk(chunk_id, &root);
                chunks.ingest(chunk_id, data).expect("bound to the root");
            }
            chunks.try_resolve()
        };

        let encoded = hollow.encode(&message, num_chunks).expect("of the length");
        let root = encoded.root();
        let mut early = Chunks::Decoding(hollow.decoder(root, num_chunks));
        drop(encoded);
        assert_eq!(deliver_all(&mut early, root), Some(Ok(message.clone())));

        // a decoded node still holds the message for a later one
        let mut late = Chunks::Decoding(hollow.decoder(root, num_chunks));
        drop(early);
        assert_eq!(deliver_all(&mut late, root), Some(Ok(message)));

        drop(late);
        assert!(!thin_air().contains_key(&root));
        let mut orphan = Chunks::Decoding(hollow.decoder(root, num_chunks));
        assert_eq!(deliver_all(&mut orphan, root), None);
    }
}
