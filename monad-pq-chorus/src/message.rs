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

//! Wire messages, RLP-encoded as `[tag, ..]` lists.

use alloy_rlp::{Decodable, EMPTY_LIST_CODE, Encodable, Header, encode_list, list_length};
use bytes::BufMut;

use crate::{
    sig::{PathVoteDomain, Signed, SignedBody, VoteDomain},
    types::{CodaMetablock, CodaValue, Entry, Metablock, PathEntry, ProposalMap},
};

/// `E` of `ln:qv:vote`: one entry per proposal index.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct VoteBody {
    pub entries: ProposalMap<Entry>,
}

/// `F` of `ln:qv:pathvote`: one path-vote entry per proposal index.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PathVoteBody {
    pub entries: ProposalMap<PathEntry>,
}

// both bodies encode as their bare entry list
macro_rules! entries_wrapper_rlp {
    ($body:ty) => {
        impl Encodable for $body {
            fn encode(&self, out: &mut dyn BufMut) {
                self.entries.encode(out)
            }

            fn length(&self) -> usize {
                self.entries.length()
            }
        }

        impl Decodable for $body {
            fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
                Ok(Self {
                    entries: Decodable::decode(buf)?,
                })
            }
        }
    };
}

entries_wrapper_rlp!(VoteBody);
entries_wrapper_rlp!(PathVoteBody);

impl SignedBody for VoteBody {
    type Domain = VoteDomain;
}

impl SignedBody for PathVoteBody {
    type Domain = PathVoteDomain;
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum PqMessage {
    Vote(Signed<VoteBody>),
    PathVote(Signed<PathVoteBody>),
    // unsigned: the authenticated channel names the sender
    Finalized(Metablock),
    Coda(CodaMessage<Entry>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum CodaMessage<V> {
    Prepare {
        view: u64,
        x: CodaMetablock<V>,
    },
    Vote1 {
        view: u64,
        x: CodaMetablock<V>,
    },
    Vote2 {
        view: u64,
        lock: Option<CodaMetablock<V>>,
        guard: CodaMetablock<V>,
    },
    Echo {
        view: u64,
        j: u16,
        v: CodaValue<V>,
    },
}

impl<V> CodaMessage<V> {
    pub fn view(&self) -> u64 {
        match self {
            Self::Prepare { view, .. }
            | Self::Vote1 { view, .. }
            | Self::Vote2 { view, .. }
            | Self::Echo { view, .. } => *view,
        }
    }
}

const VOTE_TAG: u8 = 1;
const PATH_VOTE_TAG: u8 = 2;
const FINALIZED_TAG: u8 = 3;
const CODA_TAG: u8 = 4;

impl PqMessage {
    fn tag(&self) -> u8 {
        match self {
            Self::Vote(_) => VOTE_TAG,
            Self::PathVote(_) => PATH_VOTE_TAG,
            Self::Finalized(_) => FINALIZED_TAG,
            Self::Coda(_) => CODA_TAG,
        }
    }

    fn payload(&self) -> &dyn Encodable {
        match self {
            Self::Vote(message) => message,
            Self::PathVote(message) => message,
            Self::Finalized(message) => message,
            Self::Coda(message) => message,
        }
    }
}

impl Encodable for PqMessage {
    fn encode(&self, out: &mut dyn BufMut) {
        let fields: [&dyn Encodable; 2] = [&self.tag(), self.payload()];
        encode_list::<_, dyn Encodable>(&fields, out);
    }

    fn length(&self) -> usize {
        let fields: [&dyn Encodable; 2] = [&self.tag(), self.payload()];
        list_length::<_, dyn Encodable>(&fields)
    }
}

impl Decodable for PqMessage {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let result = match u8::decode(&mut payload)? {
            VOTE_TAG => Self::Vote(Decodable::decode(&mut payload)?),
            PATH_VOTE_TAG => Self::PathVote(Decodable::decode(&mut payload)?),
            FINALIZED_TAG => Self::Finalized(Decodable::decode(&mut payload)?),
            CODA_TAG => Self::Coda(Decodable::decode(&mut payload)?),
            _ => return Err(alloy_rlp::Error::Custom("unknown PqMessage tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

const PREPARE_TAG: u8 = 1;
const VOTE1_TAG: u8 = 2;
const VOTE2_TAG: u8 = 3;
const ECHO_TAG: u8 = 4;

// Some(x) is the list [x], None the empty list, so a lock never collides
// with a metablock however many entries it has.
struct OptionalLock<'a, V>(&'a Option<CodaMetablock<V>>);

impl<V: Encodable> Encodable for OptionalLock<'_, V> {
    fn encode(&self, out: &mut dyn BufMut) {
        match self.0 {
            Some(lock) => {
                let fields: [&dyn Encodable; 1] = [lock];
                encode_list::<_, dyn Encodable>(&fields, out);
            }
            None => out.put_u8(EMPTY_LIST_CODE),
        }
    }

    fn length(&self) -> usize {
        match self.0 {
            Some(lock) => {
                let fields: [&dyn Encodable; 1] = [lock];
                list_length::<_, dyn Encodable>(&fields)
            }
            None => 1,
        }
    }
}

fn decode_lock<V: Decodable>(buf: &mut &[u8]) -> alloy_rlp::Result<Option<CodaMetablock<V>>> {
    let mut payload = Header::decode_bytes(buf, true)?;
    if payload.is_empty() {
        return Ok(None);
    }
    let lock = CodaMetablock::<V>::decode(&mut payload)?;
    if !payload.is_empty() {
        return Err(alloy_rlp::Error::UnexpectedLength);
    }
    Ok(Some(lock))
}

impl<V: Encodable> CodaMessage<V> {
    fn with_fields<R>(&self, f: impl FnOnce(&[&dyn Encodable]) -> R) -> R {
        match self {
            Self::Prepare { view, x } => f(&[&PREPARE_TAG, view, x]),
            Self::Vote1 { view, x } => f(&[&VOTE1_TAG, view, x]),
            Self::Vote2 { view, lock, guard } => f(&[&VOTE2_TAG, view, &OptionalLock(lock), guard]),
            Self::Echo { view, j, v } => f(&[&ECHO_TAG, view, j, v]),
        }
    }
}

impl<V: Encodable> Encodable for CodaMessage<V> {
    fn encode(&self, out: &mut dyn BufMut) {
        self.with_fields(|fields| encode_list::<_, dyn Encodable>(fields, out))
    }

    fn length(&self) -> usize {
        self.with_fields(|fields| list_length::<_, dyn Encodable>(fields))
    }
}

impl<V: Decodable> Decodable for CodaMessage<V> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let tag = u8::decode(&mut payload)?;
        let view = u64::decode(&mut payload)?;
        let result = match tag {
            PREPARE_TAG => Self::Prepare {
                view,
                x: Decodable::decode(&mut payload)?,
            },
            VOTE1_TAG => Self::Vote1 {
                view,
                x: Decodable::decode(&mut payload)?,
            },
            VOTE2_TAG => Self::Vote2 {
                view,
                lock: decode_lock(&mut payload)?,
                guard: Decodable::decode(&mut payload)?,
            },
            ECHO_TAG => Self::Echo {
                view,
                j: Decodable::decode(&mut payload)?,
                v: Decodable::decode(&mut payload)?,
            },
            _ => return Err(alloy_rlp::Error::Custom("unknown CodaMessage tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use monad_mcp_chorus::stub::{CadenceMessage, types::Slot};

    use super::*;
    use crate::{
        sig::StubPq,
        types::{NodeId, test_util::pid},
    };

    const K: usize = 5;
    // frame payload budget: 1472-byte datagram minus the 9-byte frame header
    const FRAME_BUDGET: usize = 1463;

    fn roundtrip<T: Encodable + Decodable + PartialEq + std::fmt::Debug>(value: &T) {
        let bytes = alloy_rlp::encode(value);
        assert_eq!(bytes.len(), value.length());
        assert_eq!(&alloy_rlp::decode_exact::<T>(&bytes).unwrap(), value);
    }

    fn positive(k: usize) -> ProposalMap<Entry> {
        ProposalMap::new(k, |j| Entry::Root(pid(j as u8)))
    }

    fn coda_mb(k: usize) -> CodaMetablock<Entry> {
        ProposalMap::new(k, |j| CodaValue::Value(Entry::Root(pid(j as u8))))
    }

    fn vote(k: usize, sig_len: usize) -> PqMessage {
        let signer = NodeId::dummy(1);
        let body = VoteBody {
            entries: positive(k),
        };
        PqMessage::Vote(Signed::sign(
            Slot(10),
            signer,
            body,
            &StubPq::new(sig_len).signer(signer),
        ))
    }

    fn path_vote(k: usize, sig_len: usize) -> PqMessage {
        let signer = NodeId::dummy(1);
        let entries = ProposalMap::new(k, |j| match j % 2 {
            0 => PathEntry::Fast(Entry::Root(pid(j as u8))),
            _ => PathEntry::Fallback(Entry::Root(pid(j as u8))),
        });
        let body = PathVoteBody { entries };
        PqMessage::PathVote(Signed::sign(
            Slot(10),
            signer,
            body,
            &StubPq::new(sig_len).signer(signer),
        ))
    }

    fn all_messages(k: usize) -> Vec<PqMessage> {
        let view = 3;
        vec![
            vote(k, 32),
            path_vote(k, 32),
            PqMessage::Finalized(positive(k)),
            PqMessage::Coda(CodaMessage::Prepare {
                view,
                x: coda_mb(k),
            }),
            PqMessage::Coda(CodaMessage::Vote1 {
                view,
                x: coda_mb(k),
            }),
            PqMessage::Coda(CodaMessage::Vote2 {
                view,
                lock: Some(coda_mb(k)),
                guard: coda_mb(k),
            }),
            PqMessage::Coda(CodaMessage::Vote2 {
                view,
                lock: None,
                guard: coda_mb(k),
            }),
            PqMessage::Coda(CodaMessage::Echo {
                view,
                j: 4,
                v: CodaValue::Value(Entry::Root(pid(4))),
            }),
            PqMessage::Coda(CodaMessage::Echo {
                view,
                j: 0,
                v: CodaValue::Empty,
            }),
        ]
    }

    // the slot wrapper the runtime adds on the wire, with a u64 conductor
    // stand-in
    fn framed_len(message: &PqMessage) -> usize {
        let wire = CadenceMessage::<PqMessage, u64>::Slot(Slot(u64::MAX / 2), message.clone());
        alloy_rlp::encode(&wire).len()
    }

    #[test]
    fn every_message_roundtrips() {
        for message in all_messages(K) {
            roundtrip(&message);
        }
        roundtrip(&CodaMessage::Vote2 {
            view: 1,
            lock: Some(ProposalMap::new(1, |_| CodaValue::Value(7u8))),
            guard: ProposalMap::new(1, |_| CodaValue::Empty),
        });
    }

    #[test]
    fn unknown_tags_rejected() {
        assert!(alloy_rlp::decode_exact::<PqMessage>(&[0xc2, 9, 0x80][..]).is_err());
        assert!(alloy_rlp::decode_exact::<CodaMessage<u8>>(&[0xc3, 9, 1, 0x80][..]).is_err());
    }

    // §6.3: every message at K = 5 with the stub signature fits one frame
    #[test]
    fn size_table_at_k5() {
        let sizes: Vec<usize> = all_messages(K).iter().map(framed_len).collect();
        let [
            vote,
            path_vote,
            finalized,
            prepare,
            vote1,
            vote2,
            _,
            echo,
            _,
        ] = sizes[..]
        else {
            unreachable!()
        };
        println!(
            "framed at K = {K}: vote {vote}, path vote {path_vote}, finalized {finalized}, \
             prepare/vote1 {prepare}, vote2 {vote2}, echo {echo}"
        );
        // body sizes of the design's table, plus signature and framing
        assert!((200..=240).contains(&vote), "vote {vote}");
        assert!((220..=260).contains(&path_vote), "path vote {path_vote}");
        assert!((170..=200).contains(&finalized), "finalized {finalized}");
        assert!((180..=230).contains(&prepare), "prepare {prepare}");
        assert_eq!(prepare, vote1);
        assert!((350..=420).contains(&vote2), "vote2 {vote2}");
        assert!((40..=80).contains(&echo), "echo {echo}");
        assert!(sizes.iter().all(|size| *size <= FRAME_BUDGET));
    }

    #[test]
    fn unfragmented_limits() {
        let max_k = |make: &dyn Fn(usize) -> PqMessage| {
            (1..200)
                .take_while(|k| framed_len(&make(*k)) <= FRAME_BUDGET)
                .last()
                .unwrap()
        };
        assert!(max_k(&|k| vote(k, 32)) >= 40);
        assert!(max_k(&|k| path_vote(k, 32)) >= 36);
        let vote2 = |k| {
            PqMessage::Coda(CodaMessage::Vote2 {
                view: 1 << 40,
                lock: Some(coda_mb(k)),
                guard: coda_mb(k),
            })
        };
        assert!(max_k(&vote2) >= 19);
        // ML-DSA-44 sized votes need fragmentation (WP8)
        assert!(framed_len(&vote(K, 2420)) > FRAME_BUDGET);
    }
}
