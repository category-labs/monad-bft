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

//! Cadence messages to and from `Packet::Cadence` payloads (§6.4).
//! `Unfragmented` sends one payload per message (stub signatures);
//! `Fragmenting` splits a message that exceeds one frame into fragments
//! inside the same packet kind (ML-DSA sizes, or K > 19).

use std::collections::HashMap;

use bytes::{BufMut as _, Bytes, BytesMut};
use monad_mcp_chorus::spec::{Deserializable as _, ProposalHeader as _, Serializable as _};
use monad_pq_chorus::{
    PqMessage,
    message::{CodaMessage, PathVoteBody, VoteBody},
    sig::{Signed, StubPq},
    types::{CodaValue, Entry, PathEntry},
};

use crate::{
    chorus::{
        CadenceMessage,
        env::{EncodingScheme, MerkleHash, MerkleRoot, ProposalHeader, S11},
        types::{NodeId, ProposalId, ProposalMap, Slot, Timestamp, TimestampDelta},
    },
    component::PqCadenceWireMsg,
};

// the udp frame budget less the 9-byte frame header (monad-mcp-node's
// network/stub.rs)
pub const MAX_PAYLOAD_LEN: usize = 1500 - 20 - 8 - 9;

// a fragment: marker, msg_id (u32 be), idx, count, then its bytes. RLP
// lists start at 0xc0, so the marker never begins a whole message.
pub const FRAGMENT_MARKER: u8 = 0x01;
pub const FRAGMENT_HEADER_LEN: usize = 1 + 4 + 1 + 1;
pub const MAX_FRAGMENT_DATA_LEN: usize = MAX_PAYLOAD_LEN - FRAGMENT_HEADER_LEN;
pub const MAX_FRAGMENTS: usize = 16;
pub const MAX_MESSAGE_LEN: usize = MAX_FRAGMENTS * MAX_FRAGMENT_DATA_LEN;
// partially received messages kept per sender
pub const MAX_PARTIAL_PER_SENDER: usize = 64;

pub trait SlotWire: Send {
    // one or more payloads, none if the message cannot be sent
    fn encode(&mut self, message: &PqCadenceWireMsg) -> Vec<Bytes>;
    fn decode(&mut self, from: NodeId, payload: Bytes, now: Timestamp) -> Option<PqCadenceWireMsg>;
}

#[derive(Default)]
pub struct Unfragmented {
    dropped: u64,
}

impl SlotWire for Unfragmented {
    fn encode(&mut self, message: &PqCadenceWireMsg) -> Vec<Bytes> {
        let payload: Bytes = message.serialize();
        if payload.len() > MAX_PAYLOAD_LEN {
            self.dropped += 1;
            warn_dropped(message, payload.len(), self.dropped);
            return Vec::new();
        }
        vec![payload]
    }

    fn decode(
        &mut self,
        from: NodeId,
        payload: Bytes,
        _now: Timestamp,
    ) -> Option<PqCadenceWireMsg> {
        deserialize(from, &payload)
    }
}

// the 1st, 2nd, 4th, 8th, ... drop, so a misfit cannot flood the log
fn warn_dropped(message: &PqCadenceWireMsg, len: usize, dropped: u64) {
    if dropped.is_power_of_two() {
        tracing::warn!(
            kind = kind(message),
            len,
            dropped,
            "pq message exceeds the wire limit, dropped"
        );
    }
}

fn deserialize(from: NodeId, payload: &Bytes) -> Option<PqCadenceWireMsg> {
    match PqCadenceWireMsg::deserialize(payload) {
        Ok(message) => Some(message),
        Err(error) => {
            tracing::debug!(?from, %error, "malformed cadence message");
            None
        }
    }
}

struct FragmentHeader {
    msg_id: u32,
    idx: u8,
    count: u8,
}

// None unless a well-formed fragment of a message of 2..=MAX_FRAGMENTS
fn parse_fragment(payload: &Bytes) -> Option<(FragmentHeader, Bytes)> {
    if payload.len() <= FRAGMENT_HEADER_LEN
        || payload.len() > MAX_PAYLOAD_LEN
        || payload[0] != FRAGMENT_MARKER
    {
        return None;
    }
    let msg_id = u32::from_be_bytes(payload[1..5].try_into().ok()?);
    let (idx, count) = (payload[5], payload[6]);
    if count < 2 || usize::from(count) > MAX_FRAGMENTS || idx >= count {
        return None;
    }
    let header = FragmentHeader { msg_id, idx, count };
    Some((header, payload.slice(FRAGMENT_HEADER_LEN..)))
}

struct Partial {
    first_seen: Timestamp,
    parts: Vec<Option<Bytes>>,
    received: usize,
}

impl Partial {
    fn new(count: u8, now: Timestamp) -> Self {
        Self {
            first_seen: now,
            parts: vec![None; usize::from(count)],
            received: 0,
        }
    }

    fn expired(&self, now: Timestamp, ttl: TimestampDelta) -> bool {
        now.duration_since(self.first_seen)
            .is_some_and(|age| age > ttl)
    }

    fn assemble(self) -> Bytes {
        let mut message = BytesMut::new();
        for part in self.parts.into_iter().flatten() {
            message.put(part);
        }
        message.freeze()
    }
}

/// Splits messages over `MAX_PAYLOAD_LEN` into fragments and reassembles
/// them per `(sender, msg_id)`. A lost fragment loses the message; the
/// slot's retransmission resends it whole under a new id.
pub struct Fragmenting {
    next_id: u32,
    ttl: TimestampDelta,
    partial: HashMap<NodeId, HashMap<u32, Partial>>,
    swept_at: Timestamp,
    dropped: u64,
}

impl Fragmenting {
    // §6.4: a partial message lives at most `ttl` (2Δ)
    pub fn new(ttl: TimestampDelta) -> Self {
        Self {
            next_id: 0,
            ttl,
            partial: HashMap::new(),
            swept_at: Timestamp::GENESIS,
            dropped: 0,
        }
    }

    fn fragment(&mut self, payload: &Bytes) -> Vec<Bytes> {
        let msg_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let chunks: Vec<&[u8]> = payload.chunks(MAX_FRAGMENT_DATA_LEN).collect();
        let count = u8::try_from(chunks.len()).expect("at most MAX_FRAGMENTS");
        chunks
            .into_iter()
            .enumerate()
            .map(|(idx, data)| {
                let mut fragment = BytesMut::with_capacity(FRAGMENT_HEADER_LEN + data.len());
                fragment.put_u8(FRAGMENT_MARKER);
                fragment.put_u32(msg_id);
                fragment.put_u8(u8::try_from(idx).expect("at most MAX_FRAGMENTS"));
                fragment.put_u8(count);
                fragment.put_slice(data);
                fragment.freeze()
            })
            .collect()
    }

    // drops every expired partial message, at most once per ttl
    fn sweep(&mut self, now: Timestamp) {
        if !now
            .duration_since(self.swept_at)
            .is_some_and(|since| since >= self.ttl)
        {
            return;
        }
        self.swept_at = now;
        let ttl = self.ttl;
        self.partial.retain(|_, messages| {
            messages.retain(|_, partial| !partial.expired(now, ttl));
            !messages.is_empty()
        });
    }

    // the whole message once its last fragment arrives
    fn reassemble(&mut self, from: NodeId, payload: &Bytes, now: Timestamp) -> Option<Bytes> {
        self.sweep(now);
        let Some((header, data)) = parse_fragment(payload) else {
            tracing::debug!(?from, len = payload.len(), "malformed fragment");
            return None;
        };
        let messages = self.partial.entry(from).or_default();
        let fresh = match messages.get(&header.msg_id) {
            None => true,
            Some(partial) => {
                // a stale id, or a sender that restarted its ids: start over
                partial.expired(now, self.ttl)
                    || partial.parts.len() != usize::from(header.count)
                    || partial.parts[usize::from(header.idx)]
                        .as_ref()
                        .is_some_and(|seen| *seen != data)
            }
        };
        if fresh {
            messages.remove(&header.msg_id);
            if messages.len() >= MAX_PARTIAL_PER_SENDER {
                let oldest = messages
                    .iter()
                    .min_by_key(|(id, partial)| (partial.first_seen, **id))
                    .map(|(id, _)| *id);
                if let Some(oldest) = oldest {
                    messages.remove(&oldest);
                }
            }
            messages.insert(header.msg_id, Partial::new(header.count, now));
        }
        let partial = messages.get_mut(&header.msg_id).expect("just ensured");
        let slot = &mut partial.parts[usize::from(header.idx)];
        if slot.is_some() {
            return None;
        }
        *slot = Some(data);
        partial.received += 1;
        if partial.received < partial.parts.len() {
            return None;
        }
        let partial = messages.remove(&header.msg_id).expect("just completed");
        if messages.is_empty() {
            self.partial.remove(&from);
        }
        Some(partial.assemble())
    }

    #[cfg(test)]
    fn partial_count(&self, from: NodeId) -> usize {
        self.partial.get(&from).map_or(0, HashMap::len)
    }
}

impl SlotWire for Fragmenting {
    fn encode(&mut self, message: &PqCadenceWireMsg) -> Vec<Bytes> {
        let payload: Bytes = message.serialize();
        if payload.len() <= MAX_PAYLOAD_LEN {
            return vec![payload];
        }
        if payload.len() > MAX_MESSAGE_LEN {
            self.dropped += 1;
            warn_dropped(message, payload.len(), self.dropped);
            return Vec::new();
        }
        self.fragment(&payload)
    }

    fn decode(&mut self, from: NodeId, payload: Bytes, now: Timestamp) -> Option<PqCadenceWireMsg> {
        if payload.first() != Some(&FRAGMENT_MARKER) {
            return deserialize(from, &payload);
        }
        let message = self.reassemble(from, &payload, now)?;
        deserialize(from, &message)
    }
}

pub fn kind(message: &PqCadenceWireMsg) -> &'static str {
    match message {
        CadenceMessage::Conductor(_) => "conductor",
        CadenceMessage::Slot(_, PqMessage::Vote(_)) => "vote",
        CadenceMessage::Slot(_, PqMessage::PathVote(_)) => "path-vote",
        CadenceMessage::Slot(_, PqMessage::Finalized(_)) => "finalized",
        CadenceMessage::Slot(_, PqMessage::Coda(coda)) => match coda {
            CodaMessage::Prepare { .. } => "coda-prepare",
            CodaMessage::Vote1 { .. } => "coda-vote1",
            CodaMessage::Vote2 { .. } => "coda-vote2",
            CodaMessage::Echo { .. } => "coda-echo",
        },
    }
}

const LARGEST_SLOT: Slot = Slot(u64::MAX / 2);

// a pid whose header encodes at its largest
fn largest_pid(j: usize) -> ProposalId {
    ProposalHeader {
        root: MerkleRoot(MerkleHash([j as u8; 20])),
        scheme: EncodingScheme::S11(S11 {
            slot: LARGEST_SLOT,
            msg_len: u32::MAX,
            unix_ts: u64::MAX,
            proposer_index: u8::MAX,
            depth: u8::MAX,
        }),
    }
    .pid()
}

// every v1 slot message of a k-proposal slot at its largest, signed with
// sig_len-byte stub signatures (the wire size of any scheme of that length)
pub fn largest_messages(k: usize, sig_len: usize) -> Vec<PqMessage> {
    let signer = NodeId::dummy(u64::MAX);
    let key = StubPq::new(sig_len).signer(signer);
    let roots = ProposalMap::new(k, |j| Entry::Root(largest_pid(j)));
    let coda = ProposalMap::new(k, |j| CodaValue::Value(Entry::Root(largest_pid(j))));
    let vote = VoteBody {
        entries: roots.clone(),
    };
    let path_vote = PathVoteBody {
        entries: ProposalMap::new(k, |j| PathEntry::Fallback(Entry::Root(largest_pid(j)))),
    };
    vec![
        PqMessage::Vote(Signed::sign(LARGEST_SLOT, signer, vote, &key)),
        PqMessage::PathVote(Signed::sign(LARGEST_SLOT, signer, path_vote, &key)),
        PqMessage::Finalized(roots),
        PqMessage::Coda(CodaMessage::Prepare {
            view: u64::MAX,
            x: coda.clone(),
        }),
        PqMessage::Coda(CodaMessage::Vote1 {
            view: u64::MAX,
            x: coda.clone(),
        }),
        PqMessage::Coda(CodaMessage::Vote2 {
            view: u64::MAX,
            lock: Some(coda.clone()),
            guard: coda,
        }),
        PqMessage::Coda(CodaMessage::Echo {
            view: u64::MAX,
            j: u16::MAX,
            v: CodaValue::Value(Entry::Root(largest_pid(0))),
        }),
    ]
}

// the first largest message longer than `limit`, with its encoded length
fn first_longer_than(k: usize, sig_len: usize, limit: usize) -> Option<(&'static str, usize)> {
    largest_messages(k, sig_len)
        .into_iter()
        .find_map(|message| {
            let message: PqCadenceWireMsg = CadenceMessage::Slot(LARGEST_SLOT, message);
            let payload: Bytes = message.serialize();
            (payload.len() > limit).then(|| (kind(&message), payload.len()))
        })
}

// `Unfragmented` sends each message in one frame: the first that cannot
// fit, with its encoded length
pub fn first_misfit(k: usize, sig_len: usize) -> Option<(&'static str, usize)> {
    first_longer_than(k, sig_len, MAX_PAYLOAD_LEN)
}

// `Fragmenting` sends up to MAX_FRAGMENTS frames per message
pub fn first_unfragmentable(k: usize, sig_len: usize) -> Option<(&'static str, usize)> {
    first_longer_than(k, sig_len, MAX_MESSAGE_LEN)
}

#[cfg(test)]
mod tests {
    use monad_pq_chorus::sig::{ML_DSA_44_SIGNATURE_LEN, STUB_SIGNATURE_LEN};

    use super::*;

    const K: usize = 5;
    const SLOT: Slot = LARGEST_SLOT;
    const TTL: TimestampDelta = TimestampDelta::from_millis(100);

    fn at(ms: u64) -> Timestamp {
        Timestamp::from_millis(ms)
    }

    fn largest_vote(sig_len: usize) -> PqCadenceWireMsg {
        let [vote, ..] = &largest_messages(K, sig_len)[..] else {
            unreachable!()
        };
        CadenceMessage::Slot(SLOT, vote.clone())
    }

    fn slot_message(message: Option<PqCadenceWireMsg>) -> Option<PqMessage> {
        match message? {
            CadenceMessage::Slot(_, message) => Some(message),
            CadenceMessage::Conductor(_) => None,
        }
    }

    fn assert_same(decoded: Option<PqCadenceWireMsg>, sent: &PqCadenceWireMsg) {
        let CadenceMessage::Slot(_, sent) = sent else {
            unreachable!()
        };
        assert_eq!(slot_message(decoded).as_ref(), Some(sent));
    }

    // §6.3: at K = 5 with the stub signature every v1 message fits a frame
    #[test]
    fn every_message_fits_a_frame_at_k5_with_the_stub_signature() {
        let mut wire = Unfragmented::default();
        assert_eq!(first_misfit(K, STUB_SIGNATURE_LEN), None);
        for message in largest_messages(K, STUB_SIGNATURE_LEN) {
            let message = CadenceMessage::Slot(SLOT, message);
            let payloads = wire.encode(&message);
            assert_eq!(payloads.len(), 1, "{} does not fit", kind(&message));
            let decoded = wire.decode(NodeId::dummy(0), payloads[0].clone(), Timestamp::GENESIS);
            assert_same(decoded, &message);
        }
    }

    #[test]
    fn an_oversized_message_is_dropped() {
        let mut wire = Unfragmented::default();
        let message = largest_vote(ML_DSA_44_SIGNATURE_LEN);
        for _ in 0..3 {
            assert!(wire.encode(&message).is_empty());
        }
        assert_eq!(wire.dropped, 3);
    }

    // ML-DSA-44 sized signatures, or too many proposals, need fragmentation
    #[test]
    fn the_misfits_are_found() {
        assert_eq!(
            first_misfit(K, ML_DSA_44_SIGNATURE_LEN).map(|(kind, _)| kind),
            Some("vote")
        );
        let (kind, len) = first_misfit(64, STUB_SIGNATURE_LEN).unwrap();
        assert_eq!(kind, "vote");
        assert!(len > MAX_PAYLOAD_LEN);
        assert_eq!(first_unfragmentable(K, ML_DSA_44_SIGNATURE_LEN), None);
        assert_eq!(first_unfragmentable(64, ML_DSA_44_SIGNATURE_LEN), None);
        assert!(first_unfragmentable(K, MAX_MESSAGE_LEN).is_some());
    }

    #[test]
    fn a_malformed_payload_is_rejected() {
        let mut wire = Unfragmented::default();
        let payload = Bytes::from_static(b"\xc1\x09");
        assert!(
            wire.decode(NodeId::dummy(0), payload, Timestamp::GENESIS)
                .is_none()
        );
    }

    // with stub signatures the fragmenting wire is byte-identical
    #[test]
    fn single_frame_messages_keep_the_unfragmented_format() {
        let mut plain = Unfragmented::default();
        let mut fragmenting = Fragmenting::new(TTL);
        for message in largest_messages(K, STUB_SIGNATURE_LEN) {
            let message = CadenceMessage::Slot(SLOT, message);
            let payloads = fragmenting.encode(&message);
            assert_eq!(payloads, plain.encode(&message));
            let decoded = fragmenting.decode(NodeId::dummy(0), payloads[0].clone(), at(0));
            assert_same(decoded, &message);
        }
    }

    #[test]
    fn every_ml_dsa_message_roundtrips_in_order() {
        let mut sender = Fragmenting::new(TTL);
        let mut receiver = Fragmenting::new(TTL);
        for message in largest_messages(K, ML_DSA_44_SIGNATURE_LEN) {
            let message = CadenceMessage::Slot(SLOT, message);
            let payloads = sender.encode(&message);
            assert!(payloads.iter().all(|p| p.len() <= MAX_PAYLOAD_LEN));
            let (last, rest) = payloads.split_last().unwrap();
            for payload in rest {
                assert!(
                    receiver
                        .decode(NodeId::dummy(1), payload.clone(), at(0))
                        .is_none()
                );
            }
            let decoded = receiver.decode(NodeId::dummy(1), last.clone(), at(0));
            assert_same(decoded, &message);
        }
        assert_eq!(receiver.partial_count(NodeId::dummy(1)), 0);
    }

    #[test]
    fn a_vote_with_an_ml_dsa_signature_takes_two_fragments() {
        let payloads = Fragmenting::new(TTL).encode(&largest_vote(ML_DSA_44_SIGNATURE_LEN));
        assert_eq!(payloads.len(), 2);
        assert!(payloads.iter().all(|p| p[0] == FRAGMENT_MARKER));
    }

    #[test]
    fn reordered_fragments_reassemble() {
        let message = CadenceMessage::Slot(SLOT, largest_messages(40, 2420)[0].clone());
        let mut sender = Fragmenting::new(TTL);
        let mut receiver = Fragmenting::new(TTL);
        let mut payloads = sender.encode(&message);
        assert!(payloads.len() >= 3);
        payloads.reverse();
        payloads.swap(0, 1);
        let (last, rest) = payloads.split_last().unwrap();
        for payload in rest {
            assert!(
                receiver
                    .decode(NodeId::dummy(1), payload.clone(), at(0))
                    .is_none()
            );
        }
        assert_same(
            receiver.decode(NodeId::dummy(1), last.clone(), at(1)),
            &message,
        );
    }

    #[test]
    fn a_lost_fragment_expires_and_the_retransmission_arrives() {
        let message = largest_vote(ML_DSA_44_SIGNATURE_LEN);
        let mut sender = Fragmenting::new(TTL);
        let mut receiver = Fragmenting::new(TTL);
        let from = NodeId::dummy(1);

        let lost = sender.encode(&message);
        assert!(receiver.decode(from, lost[0].clone(), at(0)).is_none());
        assert_eq!(receiver.partial_count(from), 1);

        // the retransmission is a new message id
        let resent = sender.encode(&message);
        assert!(receiver.decode(from, resent[0].clone(), at(50)).is_none());
        assert_same(receiver.decode(from, resent[1].clone(), at(50)), &message);

        // the lost one's partial is swept once past the ttl
        assert_eq!(receiver.partial_count(from), 1);
        assert!(
            receiver
                .decode(from, Bytes::from_static(b"\x01"), at(201))
                .is_none()
        );
        assert_eq!(receiver.partial_count(from), 0);
        // its late tail starts a new partial, which never completes
        assert!(receiver.decode(from, lost[1].clone(), at(202)).is_none());
    }

    #[test]
    fn duplicate_fragments_are_ignored() {
        let message = largest_vote(ML_DSA_44_SIGNATURE_LEN);
        let mut receiver = Fragmenting::new(TTL);
        let from = NodeId::dummy(1);
        let payloads = Fragmenting::new(TTL).encode(&message);

        assert!(receiver.decode(from, payloads[0].clone(), at(0)).is_none());
        assert!(receiver.decode(from, payloads[0].clone(), at(0)).is_none());
        assert_same(receiver.decode(from, payloads[1].clone(), at(0)), &message);
        // a duplicate of the whole message reassembles again, harmlessly
        assert!(receiver.decode(from, payloads[1].clone(), at(0)).is_none());
        assert_same(receiver.decode(from, payloads[0].clone(), at(0)), &message);
    }

    // a sender that reuses an id (a restart) replaces the stale partial
    #[test]
    fn a_conflicting_fragment_restarts_the_message() {
        let mut receiver = Fragmenting::new(TTL);
        let from = NodeId::dummy(1);
        let old = Fragmenting::new(TTL).encode(&largest_vote(ML_DSA_44_SIGNATURE_LEN));
        let other = CadenceMessage::Slot(Slot(7), largest_messages(K, 2420)[1].clone());
        let new = Fragmenting::new(TTL).encode(&other);
        assert_eq!(old[0][..5], new[0][..5], "same msg id");

        assert!(receiver.decode(from, old[0].clone(), at(0)).is_none());
        assert!(receiver.decode(from, new[0].clone(), at(1)).is_none());
        assert_same(receiver.decode(from, new[1].clone(), at(1)), &other);
    }

    #[test]
    fn malformed_fragments_are_rejected_without_state() {
        let mut receiver = Fragmenting::new(TTL);
        let from = NodeId::dummy(1);
        let fragment = |idx: u8, count: u8, data: &[u8]| {
            let mut bytes = vec![FRAGMENT_MARKER, 0, 0, 0, 9, idx, count];
            bytes.extend_from_slice(data);
            Bytes::from(bytes)
        };
        let too_many = u8::try_from(MAX_FRAGMENTS + 1).unwrap();
        for payload in [
            Bytes::from_static(b"\x01"),
            Bytes::from_static(b"\x01\x00\x00\x00\x09\x00\x02"),
            fragment(0, 1, b"x"),
            fragment(0, 0, b"x"),
            fragment(2, 2, b"x"),
            fragment(0, too_many, b"x"),
            Bytes::from(vec![FRAGMENT_MARKER; MAX_PAYLOAD_LEN + 1]),
        ] {
            assert!(receiver.decode(from, payload, at(0)).is_none());
        }
        assert_eq!(receiver.partial_count(from), 0);

        // fragments whose bytes are not a message reassemble and are dropped
        assert!(
            receiver
                .decode(from, fragment(0, 2, b"\xc5"), at(0))
                .is_none()
        );
        assert!(
            receiver
                .decode(from, fragment(1, 2, b"\x01"), at(0))
                .is_none()
        );
        assert_eq!(receiver.partial_count(from), 0);
    }

    #[test]
    fn a_message_over_the_fragment_limit_is_dropped() {
        let mut wire = Fragmenting::new(TTL);
        let message = largest_vote(MAX_MESSAGE_LEN);
        assert!(wire.encode(&message).is_empty());
        assert_eq!(wire.dropped, 1);
    }

    #[test]
    fn partials_are_bounded_per_sender_and_evict_the_oldest() {
        let message = largest_vote(ML_DSA_44_SIGNATURE_LEN);
        let mut sender = Fragmenting::new(TTL);
        let mut receiver = Fragmenting::new(TTL);
        let spammer = NodeId::dummy(6);
        let honest = NodeId::dummy(1);

        let first = sender.encode(&message);
        assert!(receiver.decode(spammer, first[0].clone(), at(0)).is_none());
        for _ in 0..MAX_PARTIAL_PER_SENDER {
            let payloads = sender.encode(&message);
            assert!(
                receiver
                    .decode(spammer, payloads[0].clone(), at(1))
                    .is_none()
            );
        }
        assert_eq!(receiver.partial_count(spammer), MAX_PARTIAL_PER_SENDER);
        // the oldest partial was evicted: its tail starts a fresh one
        assert!(receiver.decode(spammer, first[1].clone(), at(2)).is_none());

        // another sender is unaffected
        let payloads = Fragmenting::new(TTL).encode(&message);
        assert!(
            receiver
                .decode(honest, payloads[0].clone(), at(2))
                .is_none()
        );
        assert_same(
            receiver.decode(honest, payloads[1].clone(), at(2)),
            &message,
        );
    }

    #[test]
    fn many_senders_interleave() {
        const SENDERS: u64 = 20;
        let mut receiver = Fragmenting::new(TTL);
        let sent: Vec<(NodeId, PqCadenceWireMsg, Vec<Bytes>)> = (0..SENDERS)
            .map(|id| {
                // every sender starts at msg id 0
                let message = CadenceMessage::Slot(
                    Slot(id),
                    largest_messages(K, 2420)[(id % 2) as usize].clone(),
                );
                let payloads = Fragmenting::new(TTL).encode(&message);
                (NodeId::dummy(id), message, payloads)
            })
            .collect();
        for (from, _, payloads) in &sent {
            assert!(receiver.decode(*from, payloads[0].clone(), at(0)).is_none());
        }
        for (from, message, payloads) in &sent {
            assert_same(receiver.decode(*from, payloads[1].clone(), at(1)), message);
        }
        assert!(receiver.partial.is_empty());
    }
}
