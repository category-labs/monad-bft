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

//! Cadence messages to and from `Packet::Cadence` payloads (§6.4). v1 is
//! one payload per message; fragmentation (needed for ML-DSA sizes, or
//! K > 19) plugs in behind the same trait.

use bytes::Bytes;
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
        types::{NodeId, ProposalId, ProposalMap, Slot, Timestamp},
    },
    component::PqCadenceWireMsg,
};

// the udp frame budget less the 9-byte frame header (monad-mcp-node's
// network/stub.rs)
pub const MAX_PAYLOAD_LEN: usize = 1500 - 20 - 8 - 9;

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
            // the 1st, 2nd, 4th, 8th, ... drop, so a misfit cannot flood the log
            if self.dropped.is_power_of_two() {
                tracing::warn!(
                    kind = kind(message),
                    len = payload.len(),
                    dropped = self.dropped,
                    "pq message exceeds frame, dropped"
                );
            }
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
        match PqCadenceWireMsg::deserialize(&payload) {
            Ok(message) => Some(message),
            Err(error) => {
                tracing::debug!(?from, %error, "malformed cadence message");
                None
            }
        }
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
// sig_len-byte stub signatures
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

// v1 sends each message in one frame: the first that cannot fit, with
// its encoded length
pub fn first_misfit(k: usize, sig_len: usize) -> Option<(&'static str, usize)> {
    largest_messages(k, sig_len)
        .into_iter()
        .find_map(|message| {
            let message: PqCadenceWireMsg = CadenceMessage::Slot(LARGEST_SLOT, message);
            let payload: Bytes = message.serialize();
            (payload.len() > MAX_PAYLOAD_LEN).then(|| (kind(&message), payload.len()))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const K: usize = 5;
    const SLOT: Slot = LARGEST_SLOT;

    // §6.3: at K = 5 with the stub signature every v1 message fits a frame
    #[test]
    fn every_message_fits_a_frame_at_k5_with_the_stub_signature() {
        let mut wire = Unfragmented::default();
        assert_eq!(
            first_misfit(K, monad_pq_chorus::sig::STUB_SIGNATURE_LEN),
            None
        );
        for message in largest_messages(K, monad_pq_chorus::sig::STUB_SIGNATURE_LEN) {
            let message = CadenceMessage::Slot(SLOT, message);
            let payloads = wire.encode(&message);
            assert_eq!(payloads.len(), 1, "{} does not fit", kind(&message));
            let decoded = wire.decode(NodeId::dummy(0), payloads[0].clone(), Timestamp::GENESIS);
            let Some(CadenceMessage::Slot(slot, decoded)) = decoded else {
                panic!("{} does not decode", kind(&message));
            };
            let CadenceMessage::Slot(_, sent) = message else {
                unreachable!()
            };
            assert_eq!((slot, decoded), (SLOT, sent));
        }
    }

    #[test]
    fn an_oversized_message_is_dropped() {
        let mut wire = Unfragmented::default();
        let [vote, ..] = &largest_messages(K, 2420)[..] else {
            unreachable!()
        };
        let message = CadenceMessage::Slot(SLOT, vote.clone());
        for _ in 0..3 {
            assert!(wire.encode(&message).is_empty());
        }
        assert_eq!(wire.dropped, 3);
    }

    // ML-DSA-44 sized signatures, or too many proposals, need fragmentation
    #[test]
    fn the_misfits_are_found() {
        assert_eq!(first_misfit(K, 2420).map(|(kind, _)| kind), Some("vote"));
        let (kind, len) = first_misfit(64, monad_pq_chorus::sig::STUB_SIGNATURE_LEN).unwrap();
        assert_eq!(kind, "vote");
        assert!(len > MAX_PAYLOAD_LEN);
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
}
