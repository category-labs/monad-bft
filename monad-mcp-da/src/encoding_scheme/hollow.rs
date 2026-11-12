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

#[cfg(debug_assertions)]
use std::collections::HashSet;
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, LazyLock, Mutex, MutexGuard, Weak},
};

use bytes::{Buf, BufMut, Bytes};

use super::{
    super::{
        assignment::{ChunkAssignment, ChunkId},
        chunk::{ChunkData, InvalidChunk, WireChunkId},
        types::{
            D33, EncodingScheme, MerkleHash, MerkleRoot, NodeId, Timestamp, TimestampDelta,
            ValidatorData,
        },
        wire::{DAHeaderScheme, MalformedPacket, v1},
    },
    DAEncodingScheme, Decoder, Encoded, d33,
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
        self.publish(message)
    }

    // a decoder that takes whatever the store holds under root, as a node
    // within the horizon does. Nodes create theirs with decoder_at.
    fn decoder(&self, root: MerkleRoot, num_chunks: usize) -> HollowDecoder {
        let shared = SharedMessage::lookup(&root);
        HollowDecoder::new(root, self.num_source_chunks(), num_chunks, shared)
    }
}

impl<S: DAEncodingScheme> Hollow<S> {
    // the encoding of `message`: the message, published. None for a
    // message of another length than the scheme's.
    pub(crate) fn publish(&self, message: &Bytes) -> Option<HollowEncoded> {
        if message.is_empty() || message.len() != self.msg_len() {
            return None;
        }
        let shared = SharedMessage::publish(message.clone());
        Some(shared)
    }
}

// Retention and misses.
//
// A hollow message travels in no chunk: the store is the only place a
// receiver takes it from, so what a node finds there decides its outcome.
// To keep every outcome a function of the node's own inputs, a node does
// not ask whether an entry happens to be in the store; it applies a rule
// when it creates its decoder for a proposal authored at τ (the header's
// unix_ts), at its own time t:
//
// - t < τ + HORIZON: it takes the message published under the root, and
//   at k chunks decodes it, or resolves invalid if the message was
//   published under a root it does not encode to (Inconsistent);
// - t ≥ τ + HORIZON, or no node of the process published the root (a
//   forged or withheld root): it does not read the store, and resolves
//   InvalidEncoding at k chunks, as real chunks that verify and form no
//   codeword do.
//
// The author keeps every message it publishes (Retained, held by its
// DARuntime) until its own clock reads τ + HORIZON + SLACK. So whenever
// the rule sends a node to the store, the entry is there, sequentially and
// on any number of threads:
//
// - a node learns a root from a message, so its lookup comes after the
//   author's publish;
// - the parallel engine (monad-sim/docs/parallel.md) runs one window at a
//   time: every step running at one moment lies in the current window,
//   which is at most the smallest lookahead between domains wide (about
//   1 ms in the scenarios), and earlier windows are done. The author's
//   release, at its time ≥ τ + HORIZON + SLACK, and a lookup at t <
//   τ + HORIZON are more than SLACK minus the spread of node clock offsets
//   apart, so the release lands in a later window than the lookup.
//
// This assumes SLACK ≫ the window width plus the spread of clock offsets
// (`harness::with_clock_offset`; tens of milliseconds in the scenarios).
// Debug builds check it: a node that misses within the horizon a root
// whose author already released it panics.
//
// Other holders (each node's instance, until it drops the slot) only
// lengthen an entry's life, and no node reads it beyond the horizon, so
// their drops change no outcome. A sweep of the store by simulation time
// would not do: the store is shared by every simulation in the process,
// such as the tests of one binary, each with a clock of its own.
//
// A node removed from a swarm (Swarm::remove_node) takes its retention
// with it; nodes that look up its messages within the horizon then depend
// on other holders.
//
// HORIZON covers any node a scenario keeps in sync: 100 slots at the
// scenarios' 100 ms, twice the slots DA keeps past the cap
// (completed_slot_retention), so a node that pulls its chunks from peers
// still holding the slot decodes. A node beyond it resolves the
// proposal invalid where a D33 node could still decode.
pub(crate) const HORIZON: TimestampDelta = TimestampDelta::from_millis(10_000);
pub(crate) const SLACK: TimestampDelta = TimestampDelta::from_millis(1_000);

impl Hollow<D33> {
    // when the proposal was authored, to the second its header carries
    fn authored_at(&self) -> Timestamp {
        let nanos_per_second = 1_000_000_000;
        Timestamp::from_nanos(u128::from(self.0.unix_ts) * nanos_per_second)
    }

    // a node that creates its decoder before then takes the message
    fn visible_until(&self) -> Timestamp {
        self.authored_at() + HORIZON
    }

    // the decoder of a node that creates it at `now`: with the published
    // message within the horizon, without beyond it
    pub(crate) fn decoder_at(
        &self,
        root: MerkleRoot,
        num_chunks: usize,
        now: Timestamp,
    ) -> HollowDecoder {
        let within = now < self.visible_until();
        let shared = within.then(|| SharedMessage::lookup(&root)).flatten();
        #[cfg(debug_assertions)]
        if within && shared.is_none() {
            assert!(
                !released().contains(&root),
                "a hollow message left the store within its horizon: its author's \
                 release ran before this lookup (see Retention and misses)"
            );
        }
        HollowDecoder::new(root, self.num_source_chunks(), num_chunks, shared)
    }

    // the author's hold on a message it published under this scheme
    pub(crate) fn retain(&self, shared: Arc<SharedMessage>) -> Retained {
        Retained {
            until: self.visible_until() + SLACK,
            shared,
        }
    }
}

// a published message its author keeps until `until`, so that every node
// that looks it up within the horizon finds it
pub(crate) struct Retained {
    until: Timestamp,
    shared: Arc<SharedMessage>,
}

impl Retained {
    // the author may let go of it once its clock reads this
    pub(crate) fn until(&self) -> Timestamp {
        self.until
    }
}

// roots whose author let go of them, for the check in decoder_at
#[cfg(debug_assertions)]
static RELEASED: LazyLock<Mutex<HashSet<MerkleRoot>>> = LazyLock::new(Default::default);

#[cfg(debug_assertions)]
fn released() -> MutexGuard<'static, HashSet<MerkleRoot>> {
    RELEASED
        .lock()
        .expect("no thread panics holding the released roots")
}

#[cfg(debug_assertions)]
impl Drop for Retained {
    fn drop(&mut self) {
        released().insert(self.shared.declared);
    }
}

// the chunk ids received under a hollow root. any num_source_chunks of
// them decode, which is exact for an MDS code such as Reed-Solomon.
pub(crate) struct HollowDecoder {
    root: MerkleRoot,
    num_source_chunks: usize,
    received: BTreeSet<ChunkId>,
    // the message this node takes; None under a root no node published,
    // or beyond the horizon
    shared: Option<HollowEncoded>,
}

impl HollowDecoder {
    fn new(
        root: MerkleRoot,
        num_source_chunks: usize,
        num_chunks: usize,
        shared: Option<HollowEncoded>,
    ) -> Self {
        assert!(0 < num_source_chunks && num_source_chunks <= num_chunks);
        Self {
            root,
            num_source_chunks,
            received: BTreeSet::new(),
            shared,
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

    fn ingest(&mut self, chunk_id: ChunkId, data: ChunkData) -> Result<(), InvalidChunk> {
        if !verify_proof(chunk_id, &data, &self.root) {
            return Err(InvalidChunk::BadProof);
        }
        self.received.insert(chunk_id);
        Ok(())
    }

    // at k chunks, by count. Without a message to take, the chunks decode
    // to nothing that encodes to the root: an invalid encoding.
    fn try_decode(&mut self) -> Option<(Bytes, HollowEncoded)> {
        if self.received.len() < self.num_source_chunks {
            return None;
        }
        let shared = match &self.shared {
            Some(shared) => shared.clone(),
            None => SharedMessage::unpublished(self.root),
        };
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

// The chunks of a Byzantine proposer that declares a root its message does
// not encode to, for simulations. They are bound to the declared root, so
// every node takes them as valid chunks, and decodes them to a message
// whose encoding is under another root: an invalid encoding, as with real
// chunks that verify under their root and form no codeword.
pub(crate) struct Inconsistent(Arc<SharedMessage>);

impl Inconsistent {
    // the message, as published under the declared root
    pub(crate) fn shared(&self) -> Arc<SharedMessage> {
        self.0.clone()
    }
}

impl<S: DAEncodingScheme> Hollow<S> {
    // the encoding of `message` under a root it does not encode to. None
    // for a message of another length than the scheme's.
    pub(crate) fn encode_inconsistent(&self, message: &Bytes) -> Option<Inconsistent> {
        if message.is_empty() || message.len() != self.msg_len() {
            return None;
        }
        let shared = SharedMessage::publish_inconsistent(message.clone());
        Some(Inconsistent(shared))
    }

    // chunks of `message` under a root that no store entry has. None for
    // a message of another length than the scheme's.
    pub(crate) fn encode_withheld(&self, message: &Bytes) -> Option<Withheld> {
        if message.is_empty() || message.len() != self.msg_len() {
            return None;
        }
        // root_of leaves the bytes after the digest zero
        let MerkleRoot(mut root) = root_of(message);
        let last = root.0.len() - 1;
        root.0[last] ^= 1;
        Some(Withheld(MerkleRoot(root)))
    }
}

impl Encoded for Inconsistent {
    fn root(&self) -> MerkleRoot {
        self.0.declared
    }

    fn chunk_data(&self, chunk_id: ChunkId) -> ChunkData {
        hollow_chunk(chunk_id, &self.0.declared)
    }
}

// The chunks of a Byzantine proposer that publishes no message, for
// simulations: a root as a forger would make one. Its chunks are valid
// under the root, so nodes count them and vote; at k chunks every node,
// the proposer included, finds no message and resolves the proposal
// InvalidEncoding.
pub(crate) struct Withheld(MerkleRoot);

impl Encoded for Withheld {
    fn root(&self) -> MerkleRoot {
        self.0
    }

    fn chunk_data(&self, chunk_id: ChunkId) -> ChunkData {
        hollow_chunk(chunk_id, &self.0)
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
    // false for the stand-in of a message no node published, which is
    // not in thin air
    published: bool,
}

impl SharedMessage {
    fn publish(message: Bytes) -> Arc<Self> {
        let root = root_of(&message);
        Self::publish_under(root, root, message)
    }

    // a message under a root that it does not encode to
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
            published: true,
        });
        thin_air.insert(declared, Arc::downgrade(&shared));
        #[cfg(debug_assertions)]
        released().remove(&declared);
        shared
    }

    // what chunks under a root with no message decode to: nothing, which
    // encodes to another root
    fn unpublished(declared: MerkleRoot) -> Arc<Self> {
        let MerkleRoot(mut actual) = declared;
        actual.0[0] ^= 1;
        Arc::new(Self {
            declared,
            actual: MerkleRoot(actual),
            message: Bytes::new(),
            published: false,
        })
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

// whether some node holds the message published under root
#[cfg(test)]
pub(crate) fn is_published(root: &MerkleRoot) -> bool {
    SharedMessage::lookup(root).is_some()
}

// the root an honest hollow proposal of `message` declares
#[cfg(test)]
pub(crate) fn root_of_message(message: &[u8]) -> MerkleRoot {
    root_of(message)
}

impl Drop for SharedMessage {
    fn drop(&mut self) {
        if !self.published {
            return;
        }
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
            Chunks, InvalidEncoding, ProposalChunks as _, Resolved,
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
        let bad_proof = Err(InvalidChunk::BadProof);

        // a chunk delivered under another id
        let relabelled = real.chunk_data(second);
        assert_eq!(receiving.ingest(first, relabelled), bad_proof);
        let relabelled = hollow_chunk(second, &root);
        assert_eq!(hollow_receiving.ingest(first, relabelled), bad_proof);

        // a chunk of another proposal
        let foreign = other.chunk_data(first);
        assert_eq!(receiving.ingest(first, foreign), bad_proof);
        let foreign = hollow_chunk(first, &other_root);
        assert_eq!(hollow_receiving.ingest(first, foreign), bad_proof);

        // an altered proof
        let mut altered = real.chunk_data(first);
        altered.proof[0].0[0] ^= 1;
        assert_eq!(receiving.ingest(first, altered), bad_proof);
        let mut altered = hollow_chunk(first, &root);
        altered.proof[0].0[0] ^= 1;
        assert_eq!(hollow_receiving.ingest(first, altered), bad_proof);

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
        assert_eq!(deliver_all(&mut orphan, root), Some(Err(InvalidEncoding)));
    }

    // a node's chunks under `root`, one by one: what each resolves to
    fn resolutions(
        mut chunks: HollowChunks,
        root: MerkleRoot,
        num_chunks: usize,
    ) -> Vec<Option<Resolved>> {
        let mut resolved = Vec::new();
        for chunk_id in chunk_ids(num_chunks) {
            let data = hollow_chunk(chunk_id, &root);
            chunks.ingest(chunk_id, data).expect("bound to the root");
            resolved.push(chunks.try_resolve());
        }
        resolved
    }

    // nothing until k chunks, then `outcome`, then nothing
    fn at_k(
        num_source_chunks: usize,
        num_chunks: usize,
        outcome: Resolved,
    ) -> Vec<Option<Resolved>> {
        let mut expected = vec![None; num_chunks];
        expected[num_source_chunks - 1] = Some(outcome);
        expected
    }

    fn at_seconds(seconds: u64) -> Timestamp {
        Timestamp::from_millis(seconds * 1000)
    }

    #[test]
    fn chunks_under_a_root_no_node_published_resolve_invalid_at_k() {
        let hollow = Hollow::new(scheme_for(MESSAGE_LEN));
        let num_chunks = 9;
        let k = hollow.num_source_chunks();
        // a payload no other test publishes
        let message = Bytes::from(vec![0x61; MESSAGE_LEN]);

        // a forged root, and a withheld proposer's: in no store
        let MerkleRoot(mut forged) = root_of(&message);
        forged.0[3] ^= 0x10;
        let withheld = hollow.encode_withheld(&message).expect("of the length");
        for root in [MerkleRoot(forged), withheld.root()] {
            assert!(!thin_air().contains_key(&root));
            let invalid = at_k(k, num_chunks, Err(InvalidEncoding));
            let chunks = Chunks::Decoding(hollow.decoder_at(root, num_chunks, at_seconds(0)));
            assert_eq!(resolutions(chunks, root, num_chunks), invalid);
        }

        // a withheld root is no root a message published here digests to
        let published = hollow.publish(&message).expect("of the length");
        assert_ne!(published.root(), withheld.root());
    }

    #[test]
    fn a_node_takes_a_message_within_the_horizon_only() {
        let d33 = D33 {
            unix_ts: 20,
            ..scheme_for(MESSAGE_LEN)
        };
        let hollow = Hollow::new(d33);
        let num_chunks = 9;
        let k = hollow.num_source_chunks();
        // a payload no other test publishes
        let message = Bytes::from(vec![0x62; MESSAGE_LEN]);
        let encoded = hollow.publish(&message).expect("of the length");
        let root = encoded.root();

        let horizon = at_seconds(20) + HORIZON;
        let just_before = Timestamp::from_nanos(horizon.as_nanos() - 1);
        let decoded = at_k(k, num_chunks, Ok(message));
        let invalid = at_k(k, num_chunks, Err(InvalidEncoding));
        for (now, expected) in [
            (at_seconds(20), &decoded),
            (just_before, &decoded),
            (horizon, &invalid),
            (horizon + HORIZON, &invalid),
        ] {
            // the message is published all along: beyond the horizon a
            // node does not take it
            let chunks = Chunks::Decoding(hollow.decoder_at(root, num_chunks, now));
            assert_eq!(&resolutions(chunks, root, num_chunks), expected, "{now:?}");
        }

        // nor does a node beyond the horizon hold it
        let late = hollow.decoder_at(root, num_chunks, horizon);
        drop(encoded);
        assert!(!thin_air().contains_key(&root));
        drop(late);
    }

    #[test]
    fn an_inconsistent_message_resolves_invalid_on_both_sides_of_the_horizon() {
        let hollow = Hollow::new(scheme_for(MESSAGE_LEN));
        let num_chunks = 9;
        let k = hollow.num_source_chunks();
        // a payload no other test publishes
        let message = Bytes::from(vec![0x63; MESSAGE_LEN]);
        let inconsistent = hollow.encode_inconsistent(&message).expect("of the length");
        let declared = inconsistent.root();

        let invalid = at_k(k, num_chunks, Err(InvalidEncoding));
        for now in [at_seconds(0), at_seconds(0) + HORIZON] {
            let chunks = Chunks::Decoding(hollow.decoder_at(declared, num_chunks, now));
            assert_eq!(resolutions(chunks, declared, num_chunks), invalid);
        }
    }

    #[test]
    fn an_author_retains_its_message_after_every_node_let_go() {
        let hollow = Hollow::new(scheme_for(MESSAGE_LEN));
        let num_chunks = 9;
        let k = hollow.num_source_chunks();
        // a payload no other test publishes
        let message = Bytes::from(vec![0x64; MESSAGE_LEN]);
        let encoded = hollow.publish(&message).expect("of the length");
        let root = encoded.root();
        let retained = hollow.retain(encoded.clone());
        assert_eq!(retained.until(), at_seconds(0) + HORIZON + SLACK);

        // every node that decoded dropped the slot, and so did the author's
        // own encoding
        let early = Chunks::Decoding(hollow.decoder_at(root, num_chunks, at_seconds(1)));
        let decoded = at_k(k, num_chunks, Ok(message));
        assert_eq!(resolutions(early, root, num_chunks), decoded);
        drop(encoded);

        // a late node within the horizon still decodes
        let late = Chunks::Decoding(hollow.decoder_at(root, num_chunks, at_seconds(9)));
        assert_eq!(resolutions(late, root, num_chunks), decoded);

        drop(retained);
        assert!(!thin_air().contains_key(&root));
    }

    // the check of the determinism argument: a release before a lookup
    // within the horizon
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "left the store within its horizon")]
    fn a_miss_within_the_horizon_after_the_release_panics() {
        let hollow = Hollow::new(scheme_for(MESSAGE_LEN));
        // a payload no other test publishes
        let message = Bytes::from(vec![0x65; MESSAGE_LEN]);
        let encoded = hollow.publish(&message).expect("of the length");
        let root = encoded.root();
        drop(hollow.retain(encoded));
        let _ = hollow.decoder_at(root, 9, at_seconds(1));
    }
}
