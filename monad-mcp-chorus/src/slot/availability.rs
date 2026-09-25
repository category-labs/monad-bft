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
    types::{EquivCert, MerkleRoot, ProposalMeta},
};

// (slot, j)-scoped proposal availability state, built from DA events.
#[derive(Clone, Default)]
pub(crate) struct ProposalAvailability {
    // seen signed headers
    headers: HashMap<MerkleRoot, ProposalMeta>,

    // proposals that all our own chunks have been fully received from the
    // author.
    author_fulfilled: Vec<MerkleRoot>,

    // proposals that have been successfully decoded.
    decoded: HashSet<MerkleRoot>,
}

impl ProposalAvailability {
    pub fn ingest(&mut self, event: ProposalDAEvent) {
        match event {
            ProposalDAEvent::HeaderSeen(meta) => self.record_header(meta),
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
            ProposalDAEvent::Equivocation(cert) => {
                let EquivCert(a, b) = cert;
                self.record_header(a);
                self.record_header(b);
            }
            ProposalDAEvent::OwnerObligationFulfilled { .. } => {
                // relay obligations gate vote admission, which is not
                // part of this seam yet
            }
            ProposalDAEvent::DecodingFailed(_root) => todo!(),
        }
    }

    pub fn equiv_cert(&self) -> Option<EquivCert> {
        if self.headers.len() < 2 {
            return None;
        }

        let mut iter = self.headers.values().cloned();
        let a = iter.next().expect("at least two");
        let b = iter.next().expect("at least two");
        Some(EquivCert(a, b))
    }

    // invariant: every recorded header is authenticated, so any two
    // with distinct roots form an equivocation certificate.
    pub fn record_header(&mut self, meta: ProposalMeta) {
        if self.headers.contains_key(&meta.root) {
            return;
        }
        self.headers.insert(meta.root, meta);
    }

    // The proposal to vote positively on at D_s: the first root
    // whose author obligation was fulfilled.
    pub fn fetch_proposal(&self) -> Option<&ProposalMeta> {
        let root = self.author_fulfilled.first()?;
        self.headers.get(root)
    }

    pub fn decoded(&self, root: &MerkleRoot) -> bool {
        self.decoded.contains(root)
    }

    pub fn header_for(&self, root: &MerkleRoot) -> Option<&ProposalMeta> {
        self.headers.get(root)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::types::{OpaqueChunkHeader, ProposalSignature},
        *,
    };

    fn root(byte: u8) -> MerkleRoot {
        MerkleRoot(byte as u64)
    }

    fn meta(byte: u8) -> ProposalMeta {
        ProposalMeta {
            root: root(byte),
            sig: ProposalSignature,
            opaque_header: OpaqueChunkHeader,
        }
    }

    #[test]
    fn fetch_proposal_returns_first_fulfilled_root() {
        let mut avail = ProposalAvailability::default();

        assert!(avail.fetch_proposal().is_none());
        avail.ingest(ProposalDAEvent::HeaderSeen(meta(1)));
        // header alone is not enough to vote positively
        assert!(avail.fetch_proposal().is_none());

        avail.ingest(ProposalDAEvent::ProposerObligationFulfilled(root(1)));
        assert_eq!(avail.fetch_proposal(), Some(&meta(1)));

        // a later fulfillment does not displace the vote target
        avail.ingest(ProposalDAEvent::HeaderSeen(meta(2)));
        avail.ingest(ProposalDAEvent::ProposerObligationFulfilled(root(2)));
        assert_eq!(avail.fetch_proposal(), Some(&meta(1)));
    }

    #[test]
    fn conflicting_headers_form_equivocation_cert() {
        let mut avail = ProposalAvailability::default();

        avail.ingest(ProposalDAEvent::HeaderSeen(meta(1)));
        assert!(avail.equiv_cert().is_none());
        // repeated root is not a conflict
        avail.record_header(meta(1));
        assert!(avail.equiv_cert().is_none());

        avail.record_header(meta(2));
        let EquivCert(a, b) = avail.equiv_cert().expect("distinct roots conflict");
        assert!(a.root != b.root);

        // both headers remain queryable
        assert_eq!(avail.header_for(&root(1)), Some(&meta(1)));
        assert_eq!(avail.header_for(&root(2)), Some(&meta(2)));
    }

    #[test]
    fn equivocation_event_records_both_headers() {
        let mut avail = ProposalAvailability::default();

        let cert = EquivCert(meta(1), meta(2));
        avail.ingest(ProposalDAEvent::Equivocation(cert));

        assert!(avail.equiv_cert().is_some());
        assert_eq!(avail.header_for(&root(1)), Some(&meta(1)));
        assert_eq!(avail.header_for(&root(2)), Some(&meta(2)));
    }

    #[test]
    fn decoded_implies_positive_vote_target() {
        let mut avail = ProposalAvailability::default();

        avail.ingest(ProposalDAEvent::HeaderSeen(meta(1)));
        avail.ingest(ProposalDAEvent::Decoded(root(1)));

        assert_eq!(avail.fetch_proposal(), Some(&meta(1)));
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
