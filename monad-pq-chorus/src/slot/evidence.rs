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

//! Transferable evidence of misbehavior (§11). Signed bodies are kept as
//! received; with the slot and signer they verify against the signer's key.

use std::collections::BTreeSet;

use alloy_rlp::{Decodable, Encodable, Header, encode_list, list_length};
use bytes::BufMut;
use monad_mcp_chorus::stub::types::SignedProposalHeader;

use crate::{
    message::{PathVoteBody, VoteBody},
    rlp::{decode_signed_list, signed_list},
    sig::Signed,
    types::{NodeId, ProposalIndex},
};

pub type SignedVotes = Vec<(NodeId, Signed<VoteBody>)>;
pub type SignedPathVotes = Vec<(NodeId, Signed<PathVoteBody>)>;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Evidence {
    /// two proposer-signed headers with different roots for one index
    EquivocatingHeaders {
        j: ProposalIndex,
        a: SignedProposalHeader,
        b: SignedProposalHeader,
    },
    /// two different signed votes of one validator
    ConflictingVotes {
        signer: NodeId,
        a: Signed<VoteBody>,
        b: Signed<VoteBody>,
    },
    ConflictingPathVotes {
        signer: NodeId,
        a: Signed<PathVoteBody>,
        b: Signed<PathVoteBody>,
    },
    /// `lem:qv:acc-half` (2): a header with ⌊f/2⌋+1 negative votes; convicts
    /// only under at most ⌊f/2⌋ Byzantine validators
    PartialDissemination {
        j: ProposalIndex,
        header: SignedProposalHeader,
        negatives: SignedVotes,
    },
    /// `lem:qv:acc-f` case 1: `strong` path votes with a fallback entry
    FallbackQuorum {
        j: ProposalIndex,
        proposer: NodeId,
        path_votes: SignedPathVotes,
    },
    /// D20: every signed vote of an unfinalized slot, for offline
    /// comparison across validators (`lem:qv:acc-f` case 2)
    VoteDump {
        votes: SignedVotes,
        path_votes: SignedPathVotes,
    },
}

impl Evidence {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::EquivocatingHeaders { .. } => "equivocating-headers",
            Self::ConflictingVotes { .. } => "conflicting-votes",
            Self::ConflictingPathVotes { .. } => "conflicting-path-votes",
            Self::PartialDissemination { .. } => "suspect-partial-dissemination",
            Self::FallbackQuorum { .. } => "fallback-quorum",
            Self::VoteDump { .. } => "votes",
        }
    }

    /// The validator the evidence points at; none for a vote dump.
    pub fn culprit(&self) -> Option<NodeId> {
        match self {
            Self::EquivocatingHeaders { a, .. } => Some(a.sig.signer),
            Self::ConflictingVotes { signer, .. } | Self::ConflictingPathVotes { signer, .. } => {
                Some(*signer)
            }
            Self::PartialDissemination { header, .. } => Some(header.sig.signer),
            Self::FallbackQuorum { proposer, .. } => Some(*proposer),
            Self::VoteDump { .. } => None,
        }
    }

    fn key(&self) -> (&'static str, Option<NodeId>, Option<ProposalIndex>) {
        let j = match self {
            Self::EquivocatingHeaders { j, .. }
            | Self::PartialDissemination { j, .. }
            | Self::FallbackQuorum { j, .. } => Some(*j),
            _ => None,
        };
        (self.kind(), self.culprit(), j)
    }
}

/// Deduplicates evidence: one record per kind, culprit and index.
#[derive(Default)]
pub(crate) struct EvidenceLog {
    filed: BTreeSet<(&'static str, Option<NodeId>, Option<ProposalIndex>)>,
}

impl EvidenceLog {
    /// True if the evidence is new.
    pub fn file(&mut self, evidence: &Evidence) -> bool {
        self.filed.insert(evidence.key())
    }
}

const EQUIVOCATING_HEADERS: u8 = 1;
const CONFLICTING_VOTES: u8 = 2;
const CONFLICTING_PATH_VOTES: u8 = 3;
const PARTIAL_DISSEMINATION: u8 = 4;
const FALLBACK_QUORUM: u8 = 5;
const VOTE_DUMP: u8 = 6;

impl Evidence {
    fn with_fields<R>(&self, f: impl FnOnce(&[&dyn Encodable]) -> R) -> R {
        match self {
            Self::EquivocatingHeaders { j, a, b } => f(&[&EQUIVOCATING_HEADERS, j, a, b]),
            Self::ConflictingVotes { signer, a, b } => f(&[&CONFLICTING_VOTES, signer, a, b]),
            Self::ConflictingPathVotes { signer, a, b } => {
                f(&[&CONFLICTING_PATH_VOTES, signer, a, b])
            }
            Self::PartialDissemination {
                j,
                header,
                negatives,
            } => f(&[&PARTIAL_DISSEMINATION, j, header, &signed_list(negatives)]),
            Self::FallbackQuorum {
                j,
                proposer,
                path_votes,
            } => f(&[&FALLBACK_QUORUM, j, proposer, &signed_list(path_votes)]),
            Self::VoteDump { votes, path_votes } => {
                f(&[&VOTE_DUMP, &signed_list(votes), &signed_list(path_votes)])
            }
        }
    }
}

impl Encodable for Evidence {
    fn encode(&self, out: &mut dyn BufMut) {
        self.with_fields(|fields| encode_list::<_, dyn Encodable>(fields, out))
    }

    fn length(&self) -> usize {
        self.with_fields(|fields| list_length::<_, dyn Encodable>(fields))
    }
}

impl Decodable for Evidence {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let p = &mut payload;
        let result = match u8::decode(p)? {
            EQUIVOCATING_HEADERS => Self::EquivocatingHeaders {
                j: Decodable::decode(p)?,
                a: Decodable::decode(p)?,
                b: Decodable::decode(p)?,
            },
            CONFLICTING_VOTES => Self::ConflictingVotes {
                signer: Decodable::decode(p)?,
                a: Decodable::decode(p)?,
                b: Decodable::decode(p)?,
            },
            CONFLICTING_PATH_VOTES => Self::ConflictingPathVotes {
                signer: Decodable::decode(p)?,
                a: Decodable::decode(p)?,
                b: Decodable::decode(p)?,
            },
            PARTIAL_DISSEMINATION => Self::PartialDissemination {
                j: Decodable::decode(p)?,
                header: Decodable::decode(p)?,
                negatives: decode_signed_list(p)?,
            },
            FALLBACK_QUORUM => Self::FallbackQuorum {
                j: Decodable::decode(p)?,
                proposer: Decodable::decode(p)?,
                path_votes: decode_signed_list(p)?,
            },
            VOTE_DUMP => Self::VoteDump {
                votes: decode_signed_list(p)?,
                path_votes: decode_signed_list(p)?,
            },
            _ => return Err(alloy_rlp::Error::Custom("unknown Evidence tag")),
        };
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        sig::StubPq,
        slot::test_util::header,
        types::{Entry, PathEntry, ProposalMap, Slot},
    };

    fn vote(signer: u64, entry: Entry) -> (NodeId, Signed<VoteBody>) {
        let signer = NodeId::dummy(signer);
        let body = VoteBody {
            entries: ProposalMap::new(2, |_| entry),
        };
        let signed = Signed::sign(Slot(3), signer, body, &StubPq::default().signer(signer));
        (signer, signed)
    }

    fn path_vote(signer: u64) -> (NodeId, Signed<PathVoteBody>) {
        let signer = NodeId::dummy(signer);
        let body = PathVoteBody {
            entries: ProposalMap::new(2, |_| PathEntry::Fallback(Entry::Bot)),
        };
        let signed = Signed::sign(Slot(3), signer, body, &StubPq::default().signer(signer));
        (signer, signed)
    }

    fn all() -> Vec<Evidence> {
        let (signer, a) = vote(1, Entry::Bot);
        let (_, b) = vote(1, Entry::Root(crate::types::test_util::pid(2)));
        let (_, pa) = path_vote(2);
        let mut pb = pa.clone();
        pb.body.entries[0] = PathEntry::Fast(Entry::Bot);
        vec![
            Evidence::EquivocatingHeaders {
                j: 1,
                a: header(Slot(3), 4, 1),
                b: header(Slot(3), 4, 2),
            },
            Evidence::ConflictingVotes { signer, a, b },
            Evidence::ConflictingPathVotes {
                signer: NodeId::dummy(2),
                a: pa,
                b: pb,
            },
            Evidence::PartialDissemination {
                j: 0,
                header: header(Slot(3), 4, 1),
                negatives: vec![vote(1, Entry::Bot), vote(2, Entry::Bot)],
            },
            Evidence::FallbackQuorum {
                j: 1,
                proposer: NodeId::dummy(4),
                path_votes: (0..3).map(path_vote).collect(),
            },
            Evidence::VoteDump {
                votes: vec![],
                path_votes: vec![path_vote(0)],
            },
        ]
    }

    #[test]
    fn evidence_rlp_roundtrip() {
        for evidence in all() {
            let bytes = alloy_rlp::encode(&evidence);
            assert_eq!(bytes.len(), evidence.length());
            assert_eq!(
                alloy_rlp::decode_exact::<Evidence>(&bytes).unwrap(),
                evidence
            );
        }
    }

    #[test]
    fn culprits_and_dedupe() {
        let evidence = all();
        assert_eq!(evidence[0].culprit(), Some(NodeId::dummy(4)));
        assert_eq!(evidence[1].culprit(), Some(NodeId::dummy(1)));
        assert_eq!(evidence[5].culprit(), None);

        let mut log = EvidenceLog::default();
        assert!(log.file(&evidence[1]));
        assert!(!log.file(&evidence[1]));
        assert!(log.file(&evidence[2]));
    }

    // the stored bodies stay verifiable by a third party
    #[test]
    fn conflicting_votes_verify() {
        let Evidence::ConflictingVotes { signer, a, b } = &all()[1] else {
            unreachable!()
        };
        let verifier = StubPq::default();
        assert!(a.verify(Slot(3), *signer, &verifier));
        assert!(b.verify(Slot(3), *signer, &verifier));
        assert_ne!(a.body, b.body);
    }
}
