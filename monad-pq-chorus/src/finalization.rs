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

//! What the slot reports to its observer (§3.2): `PqEvent` through
//! `CommitOptimistic`, and `PqHalt` through `Finalize` at halt.

use alloy_rlp::{Decodable, Encodable, Header, encode_list, list_length};
use bytes::BufMut;

use crate::{
    message::PqMessage,
    rlp::{decode_signed_list, signed_list},
    slot::evidence::{Evidence, SignedPathVotes},
    types::{Metablock, ProposalId, ProposalMap},
};

/// How a metablock was finalized.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PqPath {
    /// `ln:qv:fastdecide`
    Fast,
    /// `ln:qv:fallbackdecide`
    Coda,
    /// `ln:qv:echo` or `ln:qv:halt`
    Gadget,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PqEvent {
    /// `ln:qv:speculate`
    Speculative {
        metablock: Metablock,
    },
    /// The ledger-visible finalization, emitted once per slot.
    Final {
        metablock: Metablock,
        path: PqPath,
        /// the Coda view of the decision, for the Coda path
        at_view: Option<u64>,
        /// on the fast path, the path votes behind the fast entries
        path_votes: SignedPathVotes,
    },
    Evidence(Evidence),
}

/// The slot's completion at halt (`ln:qv:halt`, D4).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct PqHalt {
    pub metablock: Metablock,
    pub path: PqPath,
}

impl PqHalt {
    pub fn pids(&self) -> ProposalMap<Option<ProposalId>> {
        self.metablock.as_ref().map(|entry| entry.root())
    }

    pub fn path(&self) -> PqPath {
        self.path
    }

    /// The message the node keeps repeating after halt (§7).
    pub fn finalized_message(&self) -> PqMessage {
        PqMessage::Finalized(self.metablock.clone())
    }
}

/// The ledger `proof.rlp` (§11). No transferable certificate exists by
/// design; fast-path votes are optional supporting evidence.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PqProof {
    pub version: u8,
    pub path: PqPath,
    pub metablock: Metablock,
    pub path_votes: SignedPathVotes,
}

impl PqProof {
    pub const VERSION: u8 = 1;
}

impl PqPath {
    const fn tag(self) -> u8 {
        match self {
            Self::Fast => 1,
            Self::Coda => 2,
            Self::Gadget => 3,
        }
    }
}

impl Encodable for PqPath {
    fn encode(&self, out: &mut dyn BufMut) {
        self.tag().encode(out)
    }

    fn length(&self) -> usize {
        self.tag().length()
    }
}

impl Decodable for PqPath {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        match u8::decode(buf)? {
            1 => Ok(Self::Fast),
            2 => Ok(Self::Coda),
            3 => Ok(Self::Gadget),
            _ => Err(alloy_rlp::Error::Custom("unknown PqPath")),
        }
    }
}

impl Encodable for PqProof {
    fn encode(&self, out: &mut dyn BufMut) {
        let votes = signed_list(&self.path_votes);
        let fields: [&dyn Encodable; 4] = [&self.version, &self.path, &self.metablock, &votes];
        encode_list::<_, dyn Encodable>(&fields, out);
    }

    fn length(&self) -> usize {
        let votes = signed_list(&self.path_votes);
        let fields: [&dyn Encodable; 4] = [&self.version, &self.path, &self.metablock, &votes];
        list_length::<_, dyn Encodable>(&fields)
    }
}

impl Decodable for PqProof {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = Header::decode_bytes(buf, true)?;
        let version = u8::decode(&mut payload)?;
        let path = PqPath::decode(&mut payload)?;
        let metablock = Metablock::decode(&mut payload)?;
        let path_votes = decode_signed_list(&mut payload)?;
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(Self {
            version,
            path,
            metablock,
            path_votes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        message::PathVoteBody,
        sig::{Signed, StubPq},
        types::{Entry, NodeId, PathEntry, Slot, test_util::pid},
    };

    #[test]
    fn halt_accessors() {
        let halt = PqHalt {
            metablock: ProposalMap::new(2, |j| [Entry::Root(pid(1)), Entry::Bot][j]),
            path: PqPath::Coda,
        };
        assert_eq!(
            halt.pids(),
            ProposalMap::new(2, |j| [Some(pid(1)), None][j])
        );
        assert_eq!(halt.path(), PqPath::Coda);
        assert_eq!(
            halt.finalized_message(),
            PqMessage::Finalized(halt.metablock)
        );
    }

    #[test]
    fn proof_rlp_roundtrip() {
        let signer = NodeId::dummy(2);
        let body = PathVoteBody {
            entries: ProposalMap::new(2, |_| PathEntry::Fast(Entry::Root(pid(1)))),
        };
        let vote = Signed::sign(Slot(1), signer, body, &StubPq::default().signer(signer));
        for path_votes in [vec![], vec![(signer, vote.clone()), (signer, vote)]] {
            let proof = PqProof {
                version: PqProof::VERSION,
                path: PqPath::Fast,
                metablock: ProposalMap::new(2, |_| Entry::Root(pid(1))),
                path_votes,
            };
            let bytes = alloy_rlp::encode(&proof);
            assert_eq!(alloy_rlp::decode_exact::<PqProof>(&bytes).unwrap(), proof);
        }
    }
}
