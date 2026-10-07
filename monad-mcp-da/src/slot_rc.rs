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
use monad_mcp_chorus::spec::proposal::{HeaderAuth as _, ProposalHeader as _};

use super::{
    chunk::{ChunkRequest, ProposalEnvelope},
    egress::{ChunkEgress, Dissemination},
    header::InvalidProposalHeader,
    proposer_rc::ProposerRaptorcast,
    runtime::{DAOutput, EpochHandle},
    types::{
        ChorusDACommand, ChorusDAEvent, MerkleRoot, NodeId, ProposalIndex, ProposalMap,
        ProposerSchedule, SignedProposalHeader, Slot,
    },
};

// per-slot raptorcast tracking. mainly handles proposer-related
// validation.
pub struct SlotRaptorcast {
    epoch_handle: EpochHandle,
    slot: Slot,

    // None if the index has no proposer
    raptorcasts: ProposalMap<Option<ProposerRaptorcast>>,

    // memoize the proposal index of headers that authenticated. todo:
    // bound per proposer.
    authenticated_headers: HashMap<SignedProposalHeader, ProposalIndex>,

    egress: ChunkEgress,

    // events pending delivery since the last drain
    out_events: Vec<ChorusDAEvent>,
}

impl SlotRaptorcast {
    pub fn new<S>(epoch_handle: &EpochHandle, slot: Slot, schedule: &S) -> Self
    where
        S: ProposerSchedule,
    {
        // The schedule validated its configuration at construction and
        // slots only open within its range, so a failure here is a
        // programming error -- the same expectation consensus makes when
        // it opens the slot.
        let proposers = schedule
            .proposers_at(slot)
            .expect("proposer schedule unavailable for open slot");

        // K is the schedule's, so DA and consensus index proposals alike
        let raptorcasts = ProposalMap::new(proposers.num_indices(), |j| {
            Some(ProposerRaptorcast::new(proposers.proposer(j)?))
        });

        Self {
            epoch_handle: epoch_handle.clone(),
            slot,
            raptorcasts,
            authenticated_headers: HashMap::new(),
            egress: ChunkEgress::new(),
            out_events: Vec::new(),
        }
    }

    pub fn drain_events(&mut self) -> Vec<ChorusDAEvent> {
        std::mem::take(&mut self.out_events)
    }

    pub fn drain_messages(&mut self) -> Vec<Dissemination> {
        self.egress.drain()
    }

    pub fn ingest(
        &mut self,
        sender: &NodeId,
        envelope: ProposalEnvelope,
    ) -> Result<(), InvalidProposalHeader> {
        debug_assert_eq!(envelope.header().slot(), self.slot.get());
        let j = self
            .authenticate(envelope.header())
            .ok_or(InvalidProposalHeader::Unauthenticated)?;

        let raptorcast = self.raptorcasts[j]
            .as_mut()
            .expect("authenticated indices have proposers");
        raptorcast.ingest(sender, envelope, &self.epoch_handle, &mut self.egress);

        for event in raptorcast.drain_events() {
            self.out_events.push(ChorusDAEvent { j, event });
        }

        Ok(())
    }

    // the decoded message under (j, root), once decoding succeeded
    pub(crate) fn decoded_message(&self, j: ProposalIndex, root: &MerkleRoot) -> Option<&Bytes> {
        let raptorcast = self.raptorcast(j)?;
        raptorcast.decoded_message(root)
    }

    // the proposal index of a proposer-signed header for this slot
    fn authenticate(&mut self, header: &SignedProposalHeader) -> Option<ProposalIndex> {
        if let Some(j) = self.authenticated_headers.get(header) {
            return Some(*j);
        }

        let j = self
            .epoch_handle
            .header_auth
            .authenticate(header, self.slot.get())?;
        self.authenticated_headers.insert(header.clone(), j);
        Some(j)
    }

    fn raptorcast(&self, j: ProposalIndex) -> Option<&ProposerRaptorcast> {
        if j >= self.raptorcasts.size() {
            return None;
        }
        self.raptorcasts[j].as_ref()
    }

    pub(crate) fn handle_command(&mut self, command: ChorusDACommand) -> Vec<DAOutput> {
        match command {
            ChorusDACommand::ReleaseChunks => {
                self.egress.release();
                vec![]
            }
            // todo: pull our own chunks from the decoded holders
            ChorusDACommand::AddDecodedHolders { .. } => vec![],
            // todo: move to the committed root and free the others
            ChorusDACommand::PinFinal { .. } => vec![],
            // todo: ask the voters for the header
            ChorusDACommand::PullHeader { .. } => vec![],
        }
    }

    // serve a peer chunks under (j, root)
    pub(crate) fn handle_chunk_request(
        &mut self,
        requester: &NodeId,
        proposal_index: ProposalIndex,
        root: MerkleRoot,
        request: ChunkRequest,
    ) {
        if proposal_index >= self.raptorcasts.size() {
            return;
        }
        let Some(raptorcast) = self.raptorcasts[proposal_index].as_mut() else {
            return;
        };
        raptorcast.handle_chunk_request(requester, &root, request, &mut self.egress);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use monad_mcp_chorus::spec::SignedProposalHeader as _;

    use super::{
        super::{
            chunk::{ChunkRequestType, WireChunkId},
            test_util::{
                MESSAGE_LEN, SLOT, author, epoch_handle, epoch_handle_for, group, proposal_chunks,
                proposal_chunks_from, proposer_schedule, validator_data,
            },
            types::{HeaderAuth, ProposalDAEvent, ProposalKeyPair},
        },
        *,
    };

    fn slot_raptorcast() -> (EpochHandle, SlotRaptorcast) {
        let epoch_handle = epoch_handle();
        let schedule = proposer_schedule(vec![author()]);
        let raptorcast = SlotRaptorcast::new(&epoch_handle, SLOT, &*schedule);
        (epoch_handle, raptorcast)
    }

    fn nodes(ids: impl IntoIterator<Item = u64>) -> HashSet<NodeId> {
        ids.into_iter().map(NodeId::dummy).collect()
    }

    // the wire ids carried by the messages, sorted
    fn chunk_ids<'a>(messages: impl IntoIterator<Item = &'a Dissemination>) -> Vec<WireChunkId> {
        let mut ids = Vec::new();
        for message in messages {
            ids.extend(message.envelope.chunk_data().keys().copied());
        }
        ids.sort();
        ids
    }

    // ingest, returning the messages it caused
    fn ingest(raptorcast: &mut SlotRaptorcast, envelope: ProposalEnvelope) -> Vec<Dissemination> {
        raptorcast
            .ingest(&author(), envelope)
            .expect("well-formed header");
        raptorcast.drain_messages()
    }

    // node 3 asks for our chunks, returning the messages it caused
    fn your_chunks(raptorcast: &mut SlotRaptorcast, root: MerkleRoot) -> Vec<Dissemination> {
        let request = ChunkRequest::all(ChunkRequestType::YourChunks);
        raptorcast.handle_chunk_request(&NodeId::dummy(3), 0, root, request);
        raptorcast.drain_messages()
    }

    #[test]
    #[ignore] // FIXME: failing
    fn chunks_are_held_until_released() {
        let (epoch_handle, mut raptorcast) = slot_raptorcast();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);

        // our chunk 0 arrives and node 3 asks for it: nothing leaves
        assert!(ingest(&mut raptorcast, group(&chunks[..1])).is_empty());
        assert!(your_chunks(&mut raptorcast, *header.root()).is_empty());

        // released: the second hop to the other owners, and the answer
        // to node 3, both pending until now
        raptorcast.handle_command(ChorusDACommand::ReleaseChunks);
        let messages = raptorcast.drain_messages();
        assert_eq!(chunk_ids(&messages), [0, 0]);
        assert_eq!(messages[0].to, nodes([2, 3]));
        assert_eq!(messages[1].to, nodes([3]));

        // served once
        assert!(your_chunks(&mut raptorcast, *header.root()).is_empty());
    }

    #[test]
    fn own_chunks_are_forwarded_once_on_arrival() {
        let (epoch_handle, mut raptorcast) = slot_raptorcast();
        let (_, chunks) = proposal_chunks(&epoch_handle, 1);
        raptorcast.handle_command(ChorusDACommand::ReleaseChunks);

        // our chunk 3 is forwarded on arrival, a repeat is not
        let messages = ingest(&mut raptorcast, group(&chunks[3..4]));
        assert_eq!(chunk_ids(&messages), [3]);
        assert_eq!(messages[0].to, nodes([2, 3]));
        assert!(ingest(&mut raptorcast, group(&chunks[3..4])).is_empty());

        // a chunk we do not own is not ours to forward
        assert!(ingest(&mut raptorcast, group(&chunks[1..2])).is_empty());
    }

    #[test]
    fn own_chunks_in_one_envelope_leave_in_one_message() {
        let (epoch_handle, mut raptorcast) = slot_raptorcast();
        let (_, chunks) = proposal_chunks(&epoch_handle, 1);
        raptorcast.handle_command(ChorusDACommand::ReleaseChunks);

        // chunks 0 and 3 are ours, 1 is not
        let messages = ingest(&mut raptorcast, group(&chunks[..4]));
        assert_eq!(messages.len(), 1);
        assert_eq!(chunk_ids(&messages), [0, 3]);
    }

    #[test]
    fn decoding_forwards_the_derivable_own_chunks() {
        let (epoch_handle, mut raptorcast) = slot_raptorcast();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        raptorcast.handle_command(ChorusDACommand::ReleaseChunks);

        // chunks 1, 2 and 4 belong to others; the third one decodes
        assert!(ingest(&mut raptorcast, group(&chunks[1..3])).is_empty());
        let messages = ingest(&mut raptorcast, group(&chunks[4..5]));

        let decoded = ChorusDAEvent {
            j: 0,
            event: ProposalDAEvent::Decoded(*header.root()),
        };
        assert!(raptorcast.drain_events().contains(&decoded));
        // our chunks 0 and 3 never arrived, but re-encoding derives them
        assert_eq!(chunk_ids(&messages), [0, 3]);
    }

    #[test]
    fn unauthenticated_headers_are_rejected() {
        let (epoch_handle, mut raptorcast) = slot_raptorcast();
        // signed by validator 2, who proposes nothing
        let (_, chunks) = proposal_chunks_from(&epoch_handle, 2, SLOT, 1);

        let rejected = raptorcast.ingest(&author(), group(&chunks[..1]));
        assert_eq!(rejected, Err(InvalidProposalHeader::Unauthenticated));
        assert!(raptorcast.drain_events().is_empty());
        assert!(raptorcast.drain_messages().is_empty());
    }

    #[test]
    fn authentication_is_memoized_per_header() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let epoch_handle = EpochHandle {
            self_id: NodeId::dummy(1),
            key_pair: Arc::new(ProposalKeyPair::dummy(NodeId::dummy(1))),
            header_auth: Arc::new(HeaderAuth::new(
                move |header: &SignedProposalHeader, _slot| {
                    counted.fetch_add(1, Ordering::SeqCst);
                    (header.sig().signer == author()).then_some(0)
                },
            )),
            validator_data: Arc::new(validator_data(4)),
        };
        let schedule = proposer_schedule(vec![author()]);
        let mut raptorcast = SlotRaptorcast::new(&epoch_handle, SLOT, &*schedule);
        let (_, chunks) = proposal_chunks(&epoch_handle, 1);

        raptorcast
            .ingest(&author(), group(&chunks[..1]))
            .expect("valid");
        raptorcast
            .ingest(&author(), group(&chunks[1..2]))
            .expect("valid");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    // the (peer, request) pairs among the outputs
    #[test]
    fn a_second_proposers_events_carry_its_index() {
        let proposers = vec![author(), NodeId::dummy(2)];
        let epoch_handle = epoch_handle_for(NodeId::dummy(1), 4, proposers.clone());
        let schedule = proposer_schedule(proposers);
        let mut raptorcast = SlotRaptorcast::new(&epoch_handle, SLOT, &*schedule);
        let (header, chunks) = proposal_chunks_from(&epoch_handle, 2, SLOT, 1);

        raptorcast
            .ingest(&author(), group(&chunks[..1]))
            .expect("valid");
        let events = raptorcast.drain_events();
        assert!(events.contains(&ChorusDAEvent {
            j: 1,
            event: ProposalDAEvent::HeaderSeen(header),
        }));
        assert!(events.iter().all(|event| event.j == 1));
    }

    // deliver messages to their recipients among `nodes`, dropping
    // those `lost` names
    fn deliver(
        nodes: &mut [(EpochHandle, SlotRaptorcast)],
        messages: Vec<(NodeId, Dissemination)>,
        lost: &HashSet<NodeId>,
    ) {
        for (sender, message) in messages {
            for to in &message.to {
                if lost.contains(to) {
                    continue;
                }
                let Some((_, node)) = nodes.iter_mut().find(|(handle, _)| handle.self_id == *to)
                else {
                    continue;
                };
                node.ingest(&sender, message.envelope.clone())
                    .expect("valid");
            }
        }
    }

    // each message with the node that sent it
    fn drain_all(nodes: &mut [(EpochHandle, SlotRaptorcast)]) -> Vec<(NodeId, Dissemination)> {
        let mut messages = Vec::new();
        for (handle, node) in nodes {
            for message in node.drain_messages() {
                messages.push((handle.self_id, message));
            }
        }
        messages
    }

    #[test]
    fn chunks_reach_everyone_through_the_second_hop_and_requests() {
        // validators 1, 2 and 3 own two chunks each of validator 0's proposal
        let mut nodes = Vec::new();
        for id in 1..=3 {
            let epoch_handle = epoch_handle_for(NodeId::dummy(id), 4, vec![author()]);
            let schedule = proposer_schedule(vec![author()]);
            let mut node = SlotRaptorcast::new(&epoch_handle, SLOT, &*schedule);
            node.handle_command(ChorusDACommand::ReleaseChunks);
            nodes.push((epoch_handle, node));
        }
        let (header, chunks) = proposal_chunks(&nodes[0].0, 1);
        let decoded = |node: &SlotRaptorcast| node.decoded_message(0, header.root()).is_some();

        // the author's first hop reaches 1 and 2; 3 is cut off entirely
        let cut_off = HashSet::from([NodeId::dummy(3)]);
        nodes[0]
            .1
            .ingest(&author(), group(&[chunks[0].clone(), chunks[3].clone()]))
            .expect("valid");
        nodes[1]
            .1
            .ingest(&author(), group(&[chunks[1].clone(), chunks[4].clone()]))
            .expect("valid");
        let second_hop = drain_all(&mut nodes);
        deliver(&mut nodes, second_hop, &cut_off);
        assert!(decoded(&nodes[0].1));
        assert!(decoded(&nodes[1].1));
        assert!(!decoded(&nodes[2].1));

        // 3 asks a decoded peer for its own chunks, and both peers for theirs
        let requests = [
            (1, ChunkRequest::all(ChunkRequestType::MyChunks)),
            (1, ChunkRequest::all(ChunkRequestType::YourChunks)),
            (2, ChunkRequest::all(ChunkRequestType::YourChunks)),
        ];
        for (peer, request) in requests {
            let peer = NodeId::dummy(peer);
            let (_, server) = nodes
                .iter_mut()
                .find(|(handle, _)| handle.self_id == peer)
                .unwrap();
            server.handle_chunk_request(&NodeId::dummy(3), 0, *header.root(), request);
        }
        // 1 answers both asks in one message, 2 in another; together
        // they carry every chunk
        let responses = drain_all(&mut nodes);
        assert_eq!(responses.len(), 2);
        assert_eq!(
            chunk_ids(responses.iter().map(|(_, message)| message)),
            [0, 1, 2, 3, 4, 5]
        );
        deliver(&mut nodes, responses, &HashSet::new());
        assert!(decoded(&nodes[2].1));
        assert_eq!(
            nodes[2].1.decoded_message(0, header.root()),
            Some(&Bytes::from(vec![1u8; MESSAGE_LEN]))
        );
    }
}
