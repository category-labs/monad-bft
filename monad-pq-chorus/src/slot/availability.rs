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

//! Copy of `monad-mcp-chorus` `slot/availability.rs` (crate-private there),
//! plus the decode-check result and the first-seen header.

use std::collections::{HashMap, HashSet};

use monad_mcp_chorus::{
    spec::ProposalHeader as _,
    stub::{
        slot::chorus::ProposalDAEvent,
        types::{EquivCert, ProposalId, SignedProposalHeader},
    },
};

use crate::types::Entry;

// (slot, j)-scoped proposal availability state, built from DA events.
#[derive(Clone, Default)]
pub(crate) struct ProposalAvailability {
    // seen signed headers
    headers: HashMap<ProposalId, SignedProposalHeader>,
    // the pid of the first seen header
    first_header: Option<ProposalId>,

    // proposals for which all our own chunks have been received from
    // the author.
    author_fulfilled: Vec<ProposalId>,

    // proposals that have been successfully decoded.
    decoded: HashSet<ProposalId>,

    // proposals whose decoding or re-encoding failed.
    invalid: HashSet<ProposalId>,
}

impl ProposalAvailability {
    // the equivocation certificate a newly seen header forms, if any
    pub fn ingest(&mut self, event: ProposalDAEvent) -> Option<EquivCert> {
        match event {
            ProposalDAEvent::HeaderSeen(header) => return self.record_header(header),
            ProposalDAEvent::ProposerObligationFulfilled(pid) => {
                if !self.author_fulfilled.contains(&pid) {
                    self.author_fulfilled.push(pid);
                }
            }
            ProposalDAEvent::Decoded(pid) => {
                self.decoded.insert(pid);
                // decoding implies possession of every chunk, our own
                // author-assigned ones included
                if !self.author_fulfilled.contains(&pid) {
                    self.author_fulfilled.push(pid);
                }
            }
            ProposalDAEvent::DecodingFailed(pid) => {
                self.invalid.insert(pid);
            }
        }
        None
    }

    // invariant: every recorded header is authenticated, so any two
    // with distinct roots form an equivocation certificate. The
    // certificate is formed once, by the second distinct pid.
    //
    // todo: turn signed into & or Cow
    fn record_header(&mut self, signed: SignedProposalHeader) -> Option<EquivCert> {
        let pid = signed.pid();
        if self.headers.contains_key(&pid) {
            return None;
        }
        let rival = match self.headers.len() {
            1 => self.headers.values().next().cloned(),
            _ => None,
        };
        self.first_header.get_or_insert(pid);
        self.headers.insert(pid, signed.clone());
        Some(EquivCert(rival?, signed))
    }

    // The proposal to vote positively on at D_s: the first one
    // whose author obligation was fulfilled.
    pub fn fetch_proposal(&self) -> Option<&SignedProposalHeader> {
        let pid = self.author_fulfilled.first()?;
        self.headers.get(pid)
    }

    #[cfg(test)]
    pub fn is_decoded(&self, pid: &ProposalId) -> bool {
        self.decoded.contains(pid)
    }

    pub fn is_resolved(&self, pid: &ProposalId) -> bool {
        self.decoded.contains(pid) || self.invalid.contains(pid)
    }

    pub fn header_for(&self, pid: &ProposalId) -> Option<&SignedProposalHeader> {
        self.headers.get(pid)
    }

    pub fn first_header(&self) -> Option<&SignedProposalHeader> {
        self.headers.get(self.first_header.as_ref()?)
    }

    // decode then re-encode under `pid` (ln:qv:decode, adoptcheck,
    // speccheck, fastcheck): the root if valid, ⊥ if not, None while
    // unresolved. Independent of the chunks used (paper §4).
    pub fn checked(&self, pid: &ProposalId) -> Option<Entry> {
        if self.decoded.contains(pid) {
            Some(Entry::Root(*pid))
        } else if self.invalid.contains(pid) {
            Some(Entry::Bot)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{slot::test_util, types::Slot};

    fn pid(byte: u8) -> ProposalId {
        header(byte).pid()
    }

    fn header(byte: u8) -> SignedProposalHeader {
        test_util::header(Slot(1), 0, byte)
    }

    #[test]
    fn fetch_proposal_returns_first_fulfilled_root() {
        let mut avail = ProposalAvailability::default();

        assert!(avail.fetch_proposal().is_none());
        avail.ingest(ProposalDAEvent::HeaderSeen(header(1)));
        // header alone is not enough to vote positively
        assert!(avail.fetch_proposal().is_none());

        avail.ingest(ProposalDAEvent::ProposerObligationFulfilled(pid(1)));
        assert_eq!(avail.fetch_proposal(), Some(&header(1)));

        // a later fulfillment does not displace the vote target
        avail.ingest(ProposalDAEvent::HeaderSeen(header(2)));
        avail.ingest(ProposalDAEvent::ProposerObligationFulfilled(pid(2)));
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
        assert!(a.pid() != b.pid());

        // a third root adds no evidence
        assert!(avail.record_header(header(3)).is_none());

        // both headers remain queryable
        assert_eq!(avail.header_for(&pid(1)), Some(&header(1)));
        assert_eq!(avail.header_for(&pid(2)), Some(&header(2)));
    }

    #[test]
    fn decoded_implies_positive_vote_target() {
        let mut avail = ProposalAvailability::default();

        avail.ingest(ProposalDAEvent::HeaderSeen(header(1)));
        avail.ingest(ProposalDAEvent::Decoded(pid(1)));

        assert_eq!(avail.fetch_proposal(), Some(&header(1)));
    }

    #[test]
    fn resolved_by_decode_or_failure() {
        let mut avail = ProposalAvailability::default();

        assert!(!avail.is_resolved(&pid(1)));
        avail.ingest(ProposalDAEvent::Decoded(pid(1)));
        avail.ingest(ProposalDAEvent::DecodingFailed(pid(2)));

        assert!(avail.is_resolved(&pid(1)));
        assert!(avail.is_resolved(&pid(2)));
        assert!(!avail.is_decoded(&pid(2)));
        assert!(!avail.is_resolved(&pid(3)));
    }

    #[test]
    fn decoded_is_root_scoped() {
        let mut avail = ProposalAvailability::default();

        assert!(!avail.is_decoded(&pid(1)));
        avail.ingest(ProposalDAEvent::Decoded(pid(1)));

        assert!(avail.is_decoded(&pid(1)));
        assert!(!avail.is_decoded(&pid(2)));
    }

    #[test]
    fn checked_maps_resolution_to_entries() {
        let mut avail = ProposalAvailability::default();

        assert_eq!(avail.checked(&pid(1)), None);
        avail.ingest(ProposalDAEvent::Decoded(pid(1)));
        avail.ingest(ProposalDAEvent::DecodingFailed(pid(2)));
        assert_eq!(avail.checked(&pid(1)), Some(Entry::Root(pid(1))));
        assert_eq!(avail.checked(&pid(2)), Some(Entry::Bot));
    }

    #[test]
    fn first_header_is_stable() {
        let mut avail = ProposalAvailability::default();

        assert!(avail.first_header().is_none());
        avail.ingest(ProposalDAEvent::HeaderSeen(header(2)));
        avail.ingest(ProposalDAEvent::HeaderSeen(header(1)));
        assert_eq!(avail.first_header(), Some(&header(2)));
    }
}
