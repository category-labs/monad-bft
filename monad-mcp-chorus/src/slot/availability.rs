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

use std::collections::{HashMap, HashSet};

use super::{
    chorus::ProposalDAEvent,
    types::{EquivCert, MerkleRoot, ProposalHeader},
};

// (slot, j)-scoped proposal availability state, built from DA events.
#[derive(Clone, Default)]
pub(crate) struct ProposalAvailability {
    // seen signed headers
    headers: HashMap<MerkleRoot, ProposalHeader>,

    // proposals for which all our own chunks have been received from
    // the author.
    author_fulfilled: Vec<MerkleRoot>,

    // proposals that have been successfully decoded.
    decoded: HashSet<MerkleRoot>,

    // proposals whose decoding or re-encoding failed.
    invalid: HashSet<MerkleRoot>,
}

impl ProposalAvailability {
    // the equivocation certificate a newly seen header forms, if any
    pub fn ingest(&mut self, event: ProposalDAEvent) -> Option<EquivCert> {
        match event {
            ProposalDAEvent::HeaderSeen(header) => return self.record_header(header),
            ProposalDAEvent::ProposerObligationFulfilled(root) => {
                if !self.author_fulfilled.contains(&root) {
                    self.author_fulfilled.push(root);
                }
            }
            ProposalDAEvent::Decoded(root) => {
                self.decoded.insert(root);
                // decoding implies possession of every chunk, our own
                // author-assigned ones included
                if !self.author_fulfilled.contains(&root) {
                    self.author_fulfilled.push(root);
                }
            }
            ProposalDAEvent::OwnerObligationFulfilled { .. } => {
                // handled in GatedVotePool
            }
            ProposalDAEvent::DecodingFailed(root) => {
                self.invalid.insert(root);
            }
        }
        None
    }

    // invariant: every recorded header is authenticated, so any two
    // with distinct roots form an equivocation certificate. The
    // certificate is formed once, by the second distinct root.
    pub fn record_header(&mut self, header: ProposalHeader) -> Option<EquivCert> {
        if self.headers.contains_key(&header.root) {
            return None;
        }
        let rival = match self.headers.len() {
            1 => self.headers.values().next().cloned(),
            _ => None,
        };
        self.headers.insert(header.root, header.clone());
        Some(EquivCert(rival?, header))
    }

    // The proposal to vote positively on at D_s: the first root
    // whose author obligation was fulfilled.
    pub fn fetch_proposal(&self) -> Option<&ProposalHeader> {
        let root = self.author_fulfilled.first()?;
        self.headers.get(root)
    }

    pub fn decoded(&self, root: &MerkleRoot) -> bool {
        self.decoded.contains(root)
    }

    // whether all our own chunks under root arrived
    pub fn author_fulfilled(&self, root: &MerkleRoot) -> bool {
        self.author_fulfilled.contains(root)
    }

    pub fn is_resolved(&self, root: &MerkleRoot) -> bool {
        self.decoded.contains(root) || self.invalid.contains(root)
    }

    pub fn header_for(&self, root: &MerkleRoot) -> Option<&ProposalHeader> {
        self.headers.get(root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::stub::{D25, EncodingScheme, MerkleHash, ProposalSignature};

    fn root(byte: u8) -> MerkleRoot {
        MerkleRoot(MerkleHash([byte; 20]))
    }

    fn header(byte: u8) -> ProposalHeader {
        ProposalHeader {
            slot: crate::stub::types::Slot(1),
            root: root(byte),
            sig: ProposalSignature(0),
            scheme: EncodingScheme::D25(D25 {
                msg_len: 0,
                unix_ts: 0,
            }),
        }
    }

    #[test]
    fn fetch_proposal_returns_first_fulfilled_root() {
        let mut avail = ProposalAvailability::default();

        assert!(avail.fetch_proposal().is_none());
        avail.ingest(ProposalDAEvent::HeaderSeen(header(1)));
        // header alone is not enough to vote positively
        assert!(avail.fetch_proposal().is_none());

        avail.ingest(ProposalDAEvent::ProposerObligationFulfilled(root(1)));
        assert_eq!(avail.fetch_proposal(), Some(&header(1)));

        // a later fulfillment does not displace the vote target
        avail.ingest(ProposalDAEvent::HeaderSeen(header(2)));
        avail.ingest(ProposalDAEvent::ProposerObligationFulfilled(root(2)));
        assert_eq!(avail.fetch_proposal(), Some(&header(1)));
    }

    #[test]
    fn conflicting_headers_form_equivocation_cert() {
        let mut avail = ProposalAvailability::default();

        assert!(
            avail
                .ingest(ProposalDAEvent::HeaderSeen(header(1)))
                .is_none()
        );
        // repeated root is not a conflict
        assert!(avail.record_header(header(1)).is_none());

        let cert = avail.record_header(header(2));
        let EquivCert(a, b) = cert.expect("distinct roots conflict");
        assert!(a.root != b.root);

        // a third root adds no evidence
        assert!(avail.record_header(header(3)).is_none());

        // both headers remain queryable
        assert_eq!(avail.header_for(&root(1)), Some(&header(1)));
        assert_eq!(avail.header_for(&root(2)), Some(&header(2)));
    }

    #[test]
    fn decoded_implies_positive_vote_target() {
        let mut avail = ProposalAvailability::default();

        avail.ingest(ProposalDAEvent::HeaderSeen(header(1)));
        avail.ingest(ProposalDAEvent::Decoded(root(1)));

        assert_eq!(avail.fetch_proposal(), Some(&header(1)));
    }

    #[test]
    fn resolved_by_decode_or_failure() {
        let mut avail = ProposalAvailability::default();

        assert!(!avail.is_resolved(&root(1)));
        avail.ingest(ProposalDAEvent::Decoded(root(1)));
        avail.ingest(ProposalDAEvent::DecodingFailed(root(2)));

        assert!(avail.is_resolved(&root(1)));
        assert!(avail.is_resolved(&root(2)));
        assert!(!avail.decoded(&root(2)));
        assert!(!avail.is_resolved(&root(3)));
    }

    #[test]
    fn decoded_is_root_scoped() {
        let mut avail = ProposalAvailability::default();

        assert!(!avail.decoded(&root(1)));
        avail.ingest(ProposalDAEvent::Decoded(root(1)));

        assert!(avail.decoded(&root(1)));
        assert!(!avail.decoded(&root(2)));
    }
}
