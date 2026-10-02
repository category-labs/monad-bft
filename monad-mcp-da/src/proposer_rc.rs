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
    chunk::{ChunkRequest, ChunkRequestType, ProposalEnvelope},
    egress::ChunkEgress,
    instance_rc::RaptorcastInstance,
    runtime::EpochHandle,
    types::{
        EquivCert, Holding, MerkleRoot, NodeId, Pin, PinTarget, ProposalDAEvent,
        SignedProposalHeader,
    },
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

    // None until the first header arrives
    pin: Option<PinState>,

    // events pending delivery since the last drain
    out_events: Vec<ProposalDAEvent>,
}

enum HeaderState {
    Single(SignedProposalHeader),
    Equivocation(EquivCert),
}

impl HeaderState {
    fn header_for(&self, root: &MerkleRoot) -> Option<&SignedProposalHeader> {
        let headers: &[&SignedProposalHeader] = match self {
            HeaderState::Single(header) => &[header],
            HeaderState::Equivocation(EquivCert(first, second)) => &[first, second],
        };
        headers.iter().copied().find(|header| header.root() == root)
    }
}

#[derive(PartialEq)]
enum PinState {
    // the first authenticated header, with no holders to ask
    FirstSeen(MerkleRoot),
    Tentative(PinTarget),
    // None when the committed entry needs no data
    Final(Option<PinTarget>),
}

impl From<Pin> for PinState {
    fn from(pin: Pin) -> Self {
        match pin {
            Pin::Tentative(target) => PinState::Tentative(target),
            Pin::Final(target) => PinState::Final(target),
        }
    }
}

impl PinState {
    fn is_final(&self) -> bool {
        matches!(self, PinState::Final(_))
    }

    fn root(&self) -> Option<MerkleRoot> {
        match self {
            PinState::FirstSeen(root) => Some(*root),
            PinState::Tentative(target) => Some(target.root),
            PinState::Final(target) => target.as_ref().map(|target| target.root),
        }
    }

    // the pinned root with holders to ask
    fn target(&self) -> Option<&PinTarget> {
        match self {
            PinState::FirstSeen(_) => None,
            PinState::Tentative(target) => Some(target),
            PinState::Final(target) => target.as_ref(),
        }
    }

    // what we miss under the pinned root, asked of every holder. The
    // instance is the pinned root's, None until its header is known
    fn chunk_requests(
        &self,
        instance: Option<&RaptorcastInstance>,
        self_id: &NodeId,
        at_least: Holding,
    ) -> Option<(MerkleRoot, Vec<(NodeId, ChunkRequest)>)> {
        let target = self.target()?;

        let mut requests = Vec::new();
        for (holder, holding) in target.holders.iter() {
            if holder == self_id || holding < at_least {
                continue;
            }
            // a decoded holder also serves our own share
            let request_types: &[ChunkRequestType] = match holding {
                Holding::Eventual | Holding::Owned => &[ChunkRequestType::YourChunks],
                Holding::Decoded => &[ChunkRequestType::MyChunks, ChunkRequestType::YourChunks],
            };
            for request_type in request_types {
                let request = match instance {
                    Some(instance) => instance.chunk_request(*request_type, holder),
                    // without the header there is no assignment to narrow by
                    None => Some(ChunkRequest::all(*request_type)),
                };
                let Some(request) = request else {
                    continue;
                };
                requests.push((*holder, request));
            }
        }
        Some((target.root, requests))
    }
}

impl ProposerRaptorcast {
    pub(crate) fn new(proposer: NodeId) -> Self {
        Self {
            proposer,
            headers: None,
            instances: HashMap::new(),
            pin: None,
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

        if self.pin.is_none() {
            self.pin = Some(PinState::FirstSeen(root));
        }
        let pinned = self.pin.as_ref().and_then(PinState::root);
        if pinned == Some(root) && !self.instances.contains_key(&root) {
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

    // follow consensus to the pinned root, returning its requests
    pub(crate) fn pin(
        &mut self,
        pin: Pin,
        epoch_handle: &EpochHandle,
    ) -> Option<(MerkleRoot, Vec<(NodeId, ChunkRequest)>)> {
        if self.pin.as_ref().is_some_and(PinState::is_final) {
            // a tentative pin still in flight at commit
            tracing::debug!(?pin, "ignoring a pin after the final one");
            return None;
        }

        let state = PinState::from(pin);
        if self.pin.as_ref() == Some(&state) {
            // the requests already went out; asking again is the retry's job
            return None;
        }
        if state.is_final() {
            // only the committed root is still needed
            let committed = state.root();
            self.instances.retain(|root, _| Some(*root) == committed);
        }
        self.pin = Some(state);

        self.assemble_pinned(epoch_handle);
        // eventual holders may not hold their share yet, so they wait for a retry
        self.requests(&epoch_handle.self_id, Holding::Owned)
    }

    // without a seen header, the pinned root's instance starts with
    // the first chunk that arrives for it
    fn assemble_pinned(&mut self, epoch_handle: &EpochHandle) {
        let Some(root) = self.pin.as_ref().and_then(PinState::root) else {
            return;
        };
        if self.instances.contains_key(&root) {
            return;
        }
        let Some(header) = self.headers.as_ref().and_then(|h| h.header_for(&root)) else {
            return;
        };

        let instance = RaptorcastInstance::new(epoch_handle, header.clone(), &self.proposer);
        self.instances.insert(root, instance);
    }

    // ask every holder of the pinned root for what it holds and we miss
    // todo: unused until a retry timer calls it
    #[cfg_attr(not(test), expect(unused))]
    pub fn retry(&self, self_id: &NodeId) -> Option<(MerkleRoot, Vec<(NodeId, ChunkRequest)>)> {
        self.requests(self_id, Holding::Eventual)
    }

    fn requests(
        &self,
        self_id: &NodeId,
        at_least: Holding,
    ) -> Option<(MerkleRoot, Vec<(NodeId, ChunkRequest)>)> {
        let pin = self.pin.as_ref()?;
        let instance = pin.root().and_then(|root| self.instances.get(&root));
        pin.chunk_requests(instance, self_id, at_least)
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
            chunk::ChunksSubset,
            egress::Dissemination,
            test_util::{
                Holders, Holding, chunk_id, epoch_handle, group, holders, proposal_chunks,
            },
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

    fn tentative(root: &MerkleRoot) -> Pin {
        let holders = Holders::default();
        Pin::Tentative(PinTarget {
            root: *root,
            holders,
        })
    }

    #[test]
    fn a_pinned_root_past_the_header_cap_is_assembled_unannounced() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, _) = proposal_chunks(&epoch_handle, 2);
        let (header_c, chunks_c) = proposal_chunks(&epoch_handle, 3);

        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        for header in [header_a, header_b] {
            instance.ingest(
                ProposalEnvelope::from_header(header),
                &epoch_handle,
                &mut egress,
            );
        }
        instance.drain_events();

        // a third header adds no evidence, but the pin admits its root
        instance.pin(tentative(header_c.root()), &epoch_handle);
        instance.ingest(
            ProposalEnvelope::from_header(header_c.clone()),
            &epoch_handle,
            &mut egress,
        );
        assert!(instance.drain_events().is_empty());

        instance.ingest(group(&chunks_c[..3]), &epoch_handle, &mut egress);
        let events = instance.drain_events();
        assert!(events.contains(&ProposalDAEvent::Decoded(*header_c.root())));
    }

    #[test]
    fn pinning_a_seen_root_admits_without_reannouncing() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, chunks_a) = proposal_chunks(&epoch_handle, 1);
        let (header_b, chunks_b) = proposal_chunks(&epoch_handle, 2);

        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        instance.ingest(
            ProposalEnvelope::from_header(header_a.clone()),
            &epoch_handle,
            &mut egress,
        );
        instance.drain_events();

        // rejected rival: header reported once
        instance.ingest(
            ProposalEnvelope::from_header(header_b.clone()),
            &epoch_handle,
            &mut egress,
        );
        let events = instance.drain_events();
        assert_eq!(events, vec![ProposalDAEvent::HeaderSeen(header_b.clone())]);

        // admitted now from the seen header, without a second announcement
        instance.pin(tentative(header_b.root()), &epoch_handle);
        instance.ingest(group(&chunks_b[..3]), &epoch_handle, &mut egress);
        let events = instance.drain_events();
        assert_eq!(events, vec![ProposalDAEvent::Decoded(*header_b.root())]);

        // the first root is still assembled: our votes may name it
        instance.ingest(group(&chunks_a[..3]), &epoch_handle, &mut egress);
        let events = instance.drain_events();
        assert!(events.contains(&ProposalDAEvent::Decoded(*header_a.root())));
    }

    #[test]
    fn a_committed_pin_keeps_only_its_root() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, chunks_a) = proposal_chunks(&epoch_handle, 1);
        let (header_b, chunks_b) = proposal_chunks(&epoch_handle, 2);

        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        instance.ingest(group(&chunks_a[..3]), &epoch_handle, &mut egress);
        assert!(instance.decoded_message(header_a.root()).is_some());

        let target = PinTarget {
            root: *header_b.root(),
            holders: Holders::default(),
        };
        instance.pin(Pin::Final(Some(target)), &epoch_handle);
        assert!(instance.decoded_message(header_a.root()).is_none());

        instance.ingest(group(&chunks_b[..3]), &epoch_handle, &mut egress);
        assert!(instance.decoded_message(header_b.root()).is_some());
    }

    #[test]
    fn an_unchanged_pin_sends_no_requests() {
        let epoch_handle = epoch_handle();
        let (header, _) = proposal_chunks(&epoch_handle, 1);
        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        let pin = |holding_ids: &[u64]| {
            let holdings: Vec<_> = holding_ids.iter().map(|id| (*id, Holding::Owned)).collect();
            Pin::Tentative(PinTarget {
                root: *header.root(),
                holders: holders(&holdings),
            })
        };

        assert!(instance.pin(pin(&[2]), &epoch_handle).is_some());
        assert!(instance.pin(pin(&[2]), &epoch_handle).is_none());

        // more holders replace the pin and ask them all
        let (_, requests) = instance
            .pin(pin(&[2, 3]), &epoch_handle)
            .expect("the pin changed");
        assert_eq!(requests.len(), 2);
    }

    #[test]
    fn retries_ask_each_holder_for_what_it_holds() {
        let epoch_handle = epoch_handle();
        let (header, _) = proposal_chunks(&epoch_handle, 1);

        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        let target = PinTarget {
            root: *header.root(),
            holders: holders(&[
                (0, Holding::Eventual),
                (1, Holding::Decoded),
                (2, Holding::Decoded),
                (3, Holding::Owned),
            ]),
        };
        // we are validator 1, so only 2 and 3 are asked; 0 may not hold
        // its share yet
        let (root, requests) = instance
            .pin(Pin::Final(Some(target)), &epoch_handle)
            .expect("the pin names a root");
        assert_eq!(root, *header.root());
        let my_chunks = ChunkRequest::all(ChunkRequestType::MyChunks);
        let your_chunks = ChunkRequest::all(ChunkRequestType::YourChunks);
        assert_eq!(
            requests,
            [
                (NodeId::dummy(2), my_chunks.clone()),
                (NodeId::dummy(2), your_chunks.clone()),
                (NodeId::dummy(3), your_chunks.clone()),
            ]
        );

        // a retry asks the eventual holder too
        let (_, requests) = instance
            .retry(&epoch_handle.self_id)
            .expect("the pin names a root");
        assert_eq!(
            requests,
            [
                (NodeId::dummy(0), your_chunks.clone()),
                (NodeId::dummy(2), my_chunks),
                (NodeId::dummy(2), your_chunks.clone()),
                (NodeId::dummy(3), your_chunks),
            ]
        );
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

    #[test]
    fn requests_narrow_to_the_missing_chunks_once_the_assignment_is_known() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        let peer = NodeId::dummy(2);
        let target = PinTarget {
            root: *header.root(),
            holders: holders(&[(2, Holding::Decoded)]),
        };
        let requests = |instance: &ProposerRaptorcast| {
            let (_, requests) = instance
                .retry(&epoch_handle.self_id)
                .expect("the pin names a root");
            requests
        };

        // without the header every request is for the full set
        instance.pin(Pin::Final(Some(target)), &epoch_handle);
        assert_eq!(
            requests(&instance),
            [
                (peer, ChunkRequest::all(ChunkRequestType::MyChunks)),
                (peer, ChunkRequest::all(ChunkRequestType::YourChunks)),
            ]
        );

        // holding ids 0 and 1: we still miss our own id 3, node 2
        // still owes id 4
        instance.ingest(group(&chunks[..2]), &epoch_handle, &mut egress);
        let narrowed = |kind, id| ChunkRequest {
            kind,
            subset: ChunksSubset::narrowed([chunk_id(&epoch_handle, &header, id)]),
        };
        assert_eq!(
            requests(&instance),
            [
                (peer, narrowed(ChunkRequestType::MyChunks, 3)),
                (peer, narrowed(ChunkRequestType::YourChunks, 4)),
            ]
        );

        // decoded: nothing left to ask for
        instance.ingest(group(&chunks[2..3]), &epoch_handle, &mut egress);
        assert!(instance.decoded_message(header.root()).is_some());
        assert!(requests(&instance).is_empty());
    }
}
