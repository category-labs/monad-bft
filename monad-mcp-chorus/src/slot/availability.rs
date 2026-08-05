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

    // proposals that all our own chunks have been fully received from the
    // author.
    author_fulfilled: Vec<MerkleRoot>,

    // proposals that have been successfully decoded.
    decoded: HashSet<MerkleRoot>,
}

impl ProposalAvailability {
    pub fn ingest(&mut self, event: ProposalDAEvent) {
        match event {
            ProposalDAEvent::HeaderSeen(header) => self.record_header(header),
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
                // handled in GatedVotePool
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
    pub fn record_header(&mut self, header: ProposalHeader) {
        if self.headers.contains_key(&header.root) {
            return;
        }
        self.headers.insert(header.root, header);
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

    pub fn header_for(&self, root: &MerkleRoot) -> Option<&ProposalHeader> {
        self.headers.get(root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::stub::{DaFields, EncodingScheme, MerkleHash, ProposalSignature};

    fn root(byte: u8) -> MerkleRoot {
        MerkleRoot(MerkleHash([byte; 20]))
    }

    fn header(byte: u8) -> ProposalHeader {
        ProposalHeader {
            slot: crate::stub::types::Slot(1),
            root: root(byte),
            da: DaFields {
                sig: ProposalSignature(0),
                scheme: EncodingScheme(0),
                unix_ts: 0,
                msg_len: 0,
            },
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

        avail.ingest(ProposalDAEvent::HeaderSeen(header(1)));
        assert!(avail.equiv_cert().is_none());
        // repeated root is not a conflict
        avail.record_header(header(1));
        assert!(avail.equiv_cert().is_none());

        avail.record_header(header(2));
        let EquivCert(a, b) = avail.equiv_cert().expect("distinct roots conflict");
        assert!(a.root != b.root);

        // both headers remain queryable
        assert_eq!(avail.header_for(&root(1)), Some(&header(1)));
        assert_eq!(avail.header_for(&root(2)), Some(&header(2)));
    }

    #[test]
    fn equivocation_event_records_both_headers() {
        let mut avail = ProposalAvailability::default();

        let cert = EquivCert(header(1), header(2));
        avail.ingest(ProposalDAEvent::Equivocation(cert));

        assert!(avail.equiv_cert().is_some());
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
    fn decoded_is_root_scoped() {
        let mut avail = ProposalAvailability::default();

        assert!(!avail.decoded(&root(1)));
        avail.ingest(ProposalDAEvent::Decoded(root(1)));

        assert!(avail.decoded(&root(1)));
        assert!(!avail.decoded(&root(2)));
    }
}
