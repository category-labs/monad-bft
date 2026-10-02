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

use std::collections::HashMap;

use bytes::Bytes;
use monad_mcp_chorus::spec::ProposalHeader as _;

use super::{
    chunk::{ChunkRequest, ProposalEnvelope},
    egress::ChunkEgress,
    instance_rc::RaptorcastInstance,
    runtime::EpochHandle,
    types::{EquivCert, MerkleRoot, NodeId, ProposalDAEvent, SignedProposalHeader},
};

// per-(slot, proposer) raptorcast: root-keyed
pub struct ProposerRaptorcast {
    proposer: NodeId,

    // the headers reported to consensus. A third adds no evidence
    headers: Option<HeaderState>,

    // every root ever pinned, kept until commit since our votes may
    // oblige us to serve any of them. todo: make the chorus commitment
    // to a digest of the whole signed header.
    instances: HashMap<MerkleRoot, RaptorcastInstance>,

    // the first header's root. todo: AddDecodedHolders and PinFinal
    pinned_root: Option<MerkleRoot>,

    // events pending delivery since the last drain
    out_events: Vec<ProposalDAEvent>,
}

enum HeaderState {
    Single(SignedProposalHeader),
    Equivocation(EquivCert),
}

impl HeaderState {
    // todo: used again once recovery pulls the pinned root
    #[expect(dead_code)]
    fn header_for(&self, root: &MerkleRoot) -> Option<&SignedProposalHeader> {
        let headers: &[&SignedProposalHeader] = match self {
            HeaderState::Single(header) => &[header],
            HeaderState::Equivocation(EquivCert(first, second)) => &[first, second],
        };
        headers.iter().copied().find(|header| header.root() == root)
    }
}

impl ProposerRaptorcast {
    pub(crate) fn new(proposer: NodeId) -> Self {
        Self {
            proposer,
            headers: None,
            instances: HashMap::new(),
            pinned_root: None,
            out_events: Vec::new(),
        }
    }

    pub(crate) fn drain_events(&mut self) -> Vec<ProposalDAEvent> {
        std::mem::take(&mut self.out_events)
    }

    // the decoded message under root, once decoding succeeded
    pub(crate) fn decoded_message(&self, root: &MerkleRoot) -> Option<&Bytes> {
        let instance = self.instances.get(root)?;
        instance.decoded_message()
    }

    // The caller must ensure the header is authenticated.
    pub(crate) fn ingest(
        &mut self,
        envelope: ProposalEnvelope,
        epoch_handle: &EpochHandle,
        egress: &mut ChunkEgress,
    ) {
        let (header, chunks) = envelope.into_parts();
        let root = *header.root();
        self.record_header(&header);

        let pinned_root = *self.pinned_root.get_or_insert(root);
        if pinned_root == root && !self.instances.contains_key(&root) {
            let instance = RaptorcastInstance::new(epoch_handle, header, &self.proposer);
            self.instances.insert(root, instance);
        }

        let Some(instance) = self.instances.get_mut(&root) else {
            // never pinned: its chunks are dropped
            return;
        };

        for (chunk_id, data) in chunks {
            let event = match instance.ingest_chunk(chunk_id, data, egress) {
                Ok(event) => event,
                Err(err) => {
                    tracing::debug!(?root, chunk_id, ?err, "dropping invalid chunk");
                    continue;
                }
            };

            self.out_events.extend(event);
        }
        self.out_events.extend(instance.drain_obligation_events());
    }

    // report a header to consensus while it is the first or second root
    fn record_header(&mut self, header: &SignedProposalHeader) {
        let next = match self.headers.take() {
            None => HeaderState::Single(header.clone()),
            Some(HeaderState::Single(first)) if first.root() != header.root() => {
                HeaderState::Equivocation(EquivCert(first, header.clone()))
            }
            known => {
                self.headers = known;
                return;
            }
        };
        self.headers = Some(next);
        self.out_events
            .push(ProposalDAEvent::HeaderSeen(header.clone()));
    }

    pub(crate) fn handle_chunk_request(
        &mut self,
        requester: &NodeId,
        root: &MerkleRoot,
        request: ChunkRequest,
        egress: &mut ChunkEgress,
    ) {
        let Some(instance) = self.instances.get_mut(root) else {
            return;
        };
        instance.handle_chunk_request(requester, request, egress);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{
        super::{
            chunk::ChunkRequestType,
            egress::Dissemination,
            test_util::{epoch_handle, group, proposal_chunks},
        },
        *,
    };

    fn released_egress() -> ChunkEgress {
        let mut egress = ChunkEgress::new();
        egress.release();
        egress
    }

    #[test]
    fn first_unpinned_root_is_admitted() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header, _) = proposal_chunks(&epoch_handle, 1);

        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        instance.ingest(
            ProposalEnvelope::from_header(header.clone()),
            &epoch_handle,
            &mut egress,
        );
        let events = instance.drain_events();

        assert!(events.contains(&ProposalDAEvent::HeaderSeen(header)));
    }

    #[test]
    fn header_alone_creates_the_instance() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);

        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        instance.ingest(
            ProposalEnvelope::from_header(header.clone()),
            &epoch_handle,
            &mut egress,
        );
        let events = instance.drain_events();
        assert_eq!(events, vec![ProposalDAEvent::HeaderSeen(header.clone())]);

        // the instance exists: later chunks decode it
        instance.ingest(group(&chunks[..3]), &epoch_handle, &mut egress);
        let events = instance.drain_events();
        assert!(events.contains(&ProposalDAEvent::Decoded(*header.root())));
    }

    #[test]
    fn second_unpinned_root_announces_its_header_once() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, chunks_b) = proposal_chunks(&epoch_handle, 2);

        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        instance.ingest(
            ProposalEnvelope::from_header(header_a),
            &epoch_handle,
            &mut egress,
        );
        instance.drain_events();

        instance.ingest(
            ProposalEnvelope::from_header(header_b.clone()),
            &epoch_handle,
            &mut egress,
        );
        let events = instance.drain_events();
        assert_eq!(events, vec![ProposalDAEvent::HeaderSeen(header_b.clone())]);
        instance.ingest(
            ProposalEnvelope::from_header(header_b.clone()),
            &epoch_handle,
            &mut egress,
        );
        let events = instance.drain_events();
        assert!(events.is_empty());

        // announced once; the rival is not assembled, so its chunks
        // are dropped silently and it never decodes
        instance.ingest(group(&chunks_b), &epoch_handle, &mut egress);
        assert!(instance.drain_events().is_empty());
        assert!(instance.decoded_message(header_b.root()).is_none());
    }

    #[test]
    #[ignore] // FIXME: failing
    fn own_chunk_recovery_serves_the_requesters_chunks() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);

        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        instance.ingest(group(&chunks[..3]), &epoch_handle, &mut egress);
        // discard the rebroadcast of our own chunks
        egress.drain();

        // node 3 asks without naming ids; it owns 2 of the 6 chunks
        let requester = NodeId::dummy(3);
        let my_chunks = || ChunkRequest::all(ChunkRequestType::MyChunks);
        instance.handle_chunk_request(&requester, header.root(), my_chunks(), &mut egress);
        let recovered = egress.drain();
        let [Dissemination { to, envelope }] = &recovered[..] else {
            panic!("recovery unicasts to the requester");
        };
        assert_eq!(*to, HashSet::from([requester]));
        assert_eq!(envelope.chunk_data().len(), 2);

        // served once
        instance.handle_chunk_request(&requester, header.root(), my_chunks(), &mut egress);
        assert!(egress.drain().is_empty());
    }

    #[test]
    fn your_chunk_recovery_serves_what_we_hold_before_decoding() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);

        // chunk ids 0 and 3 are ours (validator 1); ingesting ids 0
        // and 1 is short of the decoding threshold
        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        instance.ingest(group(&chunks[..2]), &epoch_handle, &mut egress);
        assert!(instance.decoded_message(header.root()).is_none());
        // discard the rebroadcast of our own chunk
        egress.drain();

        let requester = NodeId::dummy(3);
        let your_chunks = ChunkRequest::all(ChunkRequestType::YourChunks);
        instance.handle_chunk_request(&requester, header.root(), your_chunks, &mut egress);
        let recovered = egress.drain();
        let [Dissemination { to, envelope }] = &recovered[..] else {
            panic!("recovery unicasts to the requester");
        };
        assert_eq!(*to, HashSet::from([requester]));
        assert_eq!(
            envelope.chunk_data().keys().copied().collect::<Vec<_>>(),
            [0]
        );

        // the requester's own chunks (ids 2 and 5) are not held yet
        let my_chunks = ChunkRequest::all(ChunkRequestType::MyChunks);
        instance.handle_chunk_request(&requester, header.root(), my_chunks, &mut egress);
        assert!(egress.drain().is_empty());
    }
}
