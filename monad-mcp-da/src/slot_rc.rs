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

use std::collections::HashSet;

use monad_mcp_cadence::spec::proposal::ProposalHeader as _;

use super::{
    chunk::{ChunkRequest, ProposalEnvelope},
    egress::{ChunkEgress, Dissemination},
    encoding_scheme::Resolved,
    header::InvalidProposalHeader,
    proposer_rc::ProposerRaptorcast,
    runtime::{ChunkRecoveryRequest, EpochHandle, Timing},
    types::{
        ChorusDACommand, ChorusDAEvent, Lane, Lanes, NodeId, ProposalId, ProposerSchedule,
        SignedProposalHeader, Slot, Timestamp,
    },
};

// per-slot raptorcast tracking. mainly handles proposer-related
// validation.
pub struct SlotRaptorcast {
    epoch_handle: EpochHandle,
    slot: Slot,

    // None if the lane has no proposer
    raptorcasts: Lanes<Option<ProposerRaptorcast>>,

    // memoize the lane of headers that authenticated. todo:
    // bound per proposer.
    authenticated_headers: HashSet<SignedProposalHeader>,

    egress: ChunkEgress,

    // events pending delivery since the last drain
    out_events: Vec<ChorusDAEvent>,
    // recovery requests pending delivery since the last drain
    out_requests: Vec<(NodeId, ChunkRecoveryRequest)>,
}

impl SlotRaptorcast {
    pub(crate) fn new<S>(
        epoch_handle: &EpochHandle,
        slot: Slot,
        deadline: Timestamp,
        schedule: &S,
        timing: Timing,
    ) -> Self
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
        let raptorcasts = Lanes::new(proposers.num_lanes(), |j| {
            let proposer = proposers.proposer(j)?;
            let self_id = epoch_handle.self_id;
            Some(ProposerRaptorcast::new(proposer, self_id, deadline, timing))
        });

        Self {
            epoch_handle: epoch_handle.clone(),
            slot,
            raptorcasts,
            authenticated_headers: HashSet::new(),
            egress: ChunkEgress::new(),
            out_events: Vec::new(),
            out_requests: Vec::new(),
        }
    }

    pub fn drain_events(&mut self) -> Vec<ChorusDAEvent> {
        std::mem::take(&mut self.out_events)
    }

    pub(crate) fn drain_requests(&mut self) -> Vec<(NodeId, ChunkRecoveryRequest)> {
        std::mem::take(&mut self.out_requests)
    }

    pub fn drain_messages(&mut self) -> Vec<Dissemination> {
        self.egress.drain()
    }

    pub fn ingest(
        &mut self,
        sender: &NodeId,
        envelope: ProposalEnvelope,
    ) -> Result<(), InvalidProposalHeader> {
        debug_assert_eq!(envelope.header().slot(), self.slot);
        let j = self
            .authenticate(envelope.header())
            .ok_or(InvalidProposalHeader::Unauthenticated)?;

        let raptorcast = self.raptorcasts[j]
            .as_mut()
            .expect("authenticated indices have proposers");
        raptorcast.ingest(envelope, sender, &self.epoch_handle, &mut self.egress);
        self.collect_events(j);
        Ok(())
    }

    fn collect_events(&mut self, j: Lane) {
        let Some(raptorcast) = self.raptorcasts[j].as_mut() else {
            return;
        };
        for event in raptorcast.drain_events() {
            self.out_events.push(ChorusDAEvent { j, event });
        }
    }

    // proposals resolved since the last drain, under j, with their roots and messages
    pub(crate) fn drain_resolved(&mut self) -> Vec<(Lane, ProposalId, Resolved)> {
        let mut resolved = Vec::new();
        for j in self.raptorcasts.lanes() {
            let Some(raptorcast) = self.raptorcasts[j].as_mut() else {
                continue;
            };
            for (pid, message) in raptorcast.drain_resolved() {
                resolved.push((j, pid, message));
            }
        }
        resolved
    }

    // the lane of a proposer-signed header for this slot
    fn authenticate(&mut self, header: &SignedProposalHeader) -> Option<Lane> {
        use monad_mcp_cadence::spec::{ProposalHeader as _, proposal::HeaderAuth as _};

        let lane = header.lane();
        if self.authenticated_headers.contains(header) {
            return Some(lane);
        }
        if !self.epoch_handle.header_auth.authenticate(header) {
            return None;
        }
        self.authenticated_headers.insert(header.clone());
        Some(lane)
    }

    pub(crate) fn handle_command(&mut self, command: ChorusDACommand, now: Timestamp) {
        match command {
            ChorusDACommand::ReleaseChunks => {
                self.egress.release();
            }
            ChorusDACommand::AddDecodedHolders { j, pid, holders } => {
                let Some(j) = self.raptorcasts.checked_lane(usize::from(j)) else {
                    return;
                };
                let Some(raptorcast) = self.raptorcasts[j].as_mut() else {
                    return;
                };
                let requests =
                    raptorcast.add_decoded_holders(&holders, &pid, now, &self.epoch_handle);
                for (to, request) in requests {
                    let request = ChunkRecoveryRequest {
                        slot: self.slot,
                        lane: j,
                        request,
                    };
                    self.out_requests.push((to, request));
                }
            }
            ChorusDACommand::PinFinal { j, pid } => {
                let Some(j) = self.raptorcasts.checked_lane(usize::from(j)) else {
                    return;
                };
                let Some(raptorcast) = self.raptorcasts[j].as_mut() else {
                    return;
                };
                raptorcast.pin_final(pid);
                self.collect_events(j);
            }
            ChorusDACommand::PullHeader { j, missing } => {
                let Some(j) = self.raptorcasts.checked_lane(usize::from(j)) else {
                    return;
                };
                let Some(raptorcast) = self.raptorcasts[j].as_ref() else {
                    return;
                };
                let requests = raptorcast.pull_header(missing, &self.epoch_handle.validator_data);

                for (to, request) in requests {
                    let request = ChunkRecoveryRequest {
                        slot: self.slot,
                        lane: j,
                        request,
                    };
                    self.out_requests.push((to, request));
                }
            }
            ChorusDACommand::IngestProposalHeader { sender, header } => {
                let envelope = ProposalEnvelope::from_header(header);
                if let Err(err) = self.ingest(&sender, envelope) {
                    tracing::debug!(?err, "dropping a header from consensus");
                }
            }
        }
    }

    // every proposer's pulls due by now
    pub(crate) fn tick(&mut self, now: Timestamp) {
        for j in self.raptorcasts.lanes() {
            let Some(raptorcast) = self.raptorcasts[j].as_mut() else {
                continue;
            };
            let requests = raptorcast.poll_chunk_requests(now, &self.epoch_handle);
            for (to, request) in requests {
                let request = ChunkRecoveryRequest {
                    slot: self.slot,
                    lane: j,
                    request,
                };
                self.out_requests.push((to, request));
            }
        }
    }

    // serve a peer chunks of proposer j's proposal
    pub(crate) fn handle_chunk_request(
        &mut self,
        request: ChunkRequest,
        lane: Lane,
        requester: &NodeId,
        now: Timestamp,
    ) {
        let Some(lane) = self.raptorcasts.checked_lane(usize::from(lane)) else {
            return;
        };
        let Some(raptorcast) = self.raptorcasts[lane].as_mut() else {
            return;
        };
        raptorcast.handle_chunk_request(request, requester, now, &mut self.egress);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use bytes::Bytes;
    use monad_mcp_cadence::spec::SignedProposalHeader as _;

    use super::{
        super::{
            chunk::{ChunkRequestType, WireChunkId},
            test_util::{
                MESSAGE_LEN, NOW, SLOT, author, d33_scheme_at, epoch_handle, epoch_handle_for,
                group, proposal_chunks, proposal_chunks_from, proposal_chunks_under,
                proposer_schedule, timing, validator_data,
            },
            types::{HeaderAuth, ProposalDAEvent, ProposalKeyPair},
        },
        *,
    };

    fn slot_raptorcast() -> (EpochHandle, SlotRaptorcast) {
        let epoch_handle = epoch_handle();
        let schedule = proposer_schedule(vec![author()]);
        let raptorcast = SlotRaptorcast::new(&epoch_handle, SLOT, NOW, &*schedule, timing());
        (epoch_handle, raptorcast)
    }

    fn nodes(ids: impl IntoIterator<Item = u64>) -> BTreeSet<NodeId> {
        ids.into_iter().map(NodeId::dummy).collect()
    }

    // recipients go out in a shuffled order; the tests check who, not when
    fn set(to: &[NodeId]) -> BTreeSet<NodeId> {
        to.iter().copied().collect()
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
    fn your_chunks(
        raptorcast: &mut SlotRaptorcast,
        pid: ProposalId,
        now: Timestamp,
    ) -> Vec<Dissemination> {
        let request = ChunkRequest::all(pid, ChunkRequestType::YourChunks);
        raptorcast.handle_chunk_request(request, Lane::new_unchecked(0), &NodeId::dummy(3), now);
        raptorcast.drain_messages()
    }

    #[test]
    fn chunks_are_held_until_released() {
        let (epoch_handle, mut raptorcast) = slot_raptorcast();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);

        // our chunk 2 arrives and node 3 asks for it: nothing leaves
        assert!(ingest(&mut raptorcast, group(&chunks[2..3])).is_empty());
        assert!(your_chunks(&mut raptorcast, header.pid(), NOW).is_empty());

        // released: the second hop to the other owners, and the answer
        // to node 3, both pending until now
        raptorcast.handle_command(ChorusDACommand::ReleaseChunks, NOW);
        let messages = raptorcast.drain_messages();
        assert_eq!(chunk_ids(&messages), [2, 2]);
        assert_eq!(set(&messages[0].to), nodes([2, 3]));
        assert_eq!(set(&messages[1].to), nodes([3]));

        // released: a repeat a window later is answered at once
        let a_window_later = NOW + timing().serve_window;
        let repeat = your_chunks(&mut raptorcast, header.pid(), a_window_later);
        assert_eq!(chunk_ids(&repeat), [2]);
    }

    #[test]
    fn a_requester_is_cut_off_at_its_quota() {
        let (epoch_handle, mut raptorcast) = slot_raptorcast();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        raptorcast.handle_command(ChorusDACommand::ReleaseChunks, NOW);
        ingest(&mut raptorcast, group(&chunks[2..3]));

        // node 3's 3 chunks, our 3 and the header: 7 per window
        let pid = header.pid();
        for _ in 0..7 {
            assert_eq!(chunk_ids(&your_chunks(&mut raptorcast, pid, NOW)), [2]);
        }
        assert!(your_chunks(&mut raptorcast, pid, NOW).is_empty());

        let a_window_later = NOW + timing().serve_window;
        assert_eq!(
            chunk_ids(&your_chunks(&mut raptorcast, pid, a_window_later)),
            [2]
        );
    }

    #[test]
    fn own_chunks_are_forwarded_once_on_arrival() {
        let (epoch_handle, mut raptorcast) = slot_raptorcast();
        let (_, chunks) = proposal_chunks(&epoch_handle, 1);
        raptorcast.handle_command(ChorusDACommand::ReleaseChunks, NOW);

        // our chunk 6 is forwarded on arrival, a repeat is not
        let messages = ingest(&mut raptorcast, group(&chunks[6..7]));
        assert_eq!(chunk_ids(&messages), [6]);
        assert_eq!(set(&messages[0].to), nodes([2, 3]));
        assert!(ingest(&mut raptorcast, group(&chunks[6..7])).is_empty());

        // a chunk we do not own is not ours to forward. A second chunk
        // would decode the proposal here, so this one goes to a fresh slot
        let (_, mut fresh) = slot_raptorcast();
        fresh.handle_command(ChorusDACommand::ReleaseChunks, NOW);
        assert!(ingest(&mut fresh, group(&chunks[1..2])).is_empty());
    }

    #[test]
    fn own_chunks_in_one_envelope_leave_in_one_message() {
        let (epoch_handle, mut raptorcast) = slot_raptorcast();
        let (_, chunks) = proposal_chunks(&epoch_handle, 1);
        raptorcast.handle_command(ChorusDACommand::ReleaseChunks, NOW);

        // chunks 2 and 6 are ours, 1 is not
        let envelope = group(&[chunks[1].clone(), chunks[2].clone(), chunks[6].clone()]);
        let messages = ingest(&mut raptorcast, envelope);
        assert_eq!(messages.len(), 1);
        assert_eq!(chunk_ids(&messages), [2, 6]);
    }

    #[test]
    fn decoding_forwards_the_derivable_own_chunks() {
        let (epoch_handle, mut raptorcast) = slot_raptorcast();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);
        raptorcast.handle_command(ChorusDACommand::ReleaseChunks, NOW);

        // chunks 1 and 3 belong to others; the second one decodes
        assert!(ingest(&mut raptorcast, group(&chunks[1..2])).is_empty());
        let messages = ingest(&mut raptorcast, group(&chunks[3..4]));

        let decoded = ChorusDAEvent {
            j: Lane::new_unchecked(0),
            event: ProposalDAEvent::Decoded(header.pid()),
        };
        assert!(raptorcast.drain_events().contains(&decoded));
        // our chunks 2 and 6 never arrived, but re-encoding derives them;
        // our partial chunk 10 covers no node but us
        assert_eq!(chunk_ids(&messages), [2, 6]);
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
            header_auth: Arc::new(HeaderAuth::new(move |header: &SignedProposalHeader| {
                counted.fetch_add(1, Ordering::SeqCst);
                header.sig().signer == author()
            })),
            validator_data: Arc::new(validator_data(4)),
        };
        let schedule = proposer_schedule(vec![author()]);
        let mut raptorcast = SlotRaptorcast::new(&epoch_handle, SLOT, NOW, &*schedule, timing());
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
        let mut raptorcast = SlotRaptorcast::new(&epoch_handle, SLOT, NOW, &*schedule, timing());
        let scheme = d33_scheme_at(&epoch_handle, SLOT, Lane::new_unchecked(1));
        let (header, chunks) = proposal_chunks_under(&epoch_handle, scheme, 2, 1);

        raptorcast
            .ingest(&author(), group(&chunks[..1]))
            .expect("valid");
        let events = raptorcast.drain_events();
        assert!(events.contains(&ChorusDAEvent {
            j: Lane::new_unchecked(1),
            event: ProposalDAEvent::HeaderSeen(header),
        }));
        assert!(events.iter().all(|event| event.j == Lane::new_unchecked(1)));
    }

    #[test]
    fn a_header_from_consensus_pins_its_proposal() {
        let (epoch_handle, mut raptorcast) = slot_raptorcast();
        let (header, _) = proposal_chunks(&epoch_handle, 1);

        let command = ChorusDACommand::IngestProposalHeader {
            sender: NodeId::dummy(2),
            header: header.clone(),
        };
        raptorcast.handle_command(command, NOW);
        assert!(raptorcast.drain_requests().is_empty());
        let seen = ChorusDAEvent {
            j: Lane::new_unchecked(0),
            event: ProposalDAEvent::HeaderSeen(header),
        };
        assert_eq!(raptorcast.drain_events(), vec![seen]);

        // pinned, so its chunks are pulled once pulls are due
        raptorcast.tick(NOW + timing().pull_delay);
        assert!(!raptorcast.drain_requests().is_empty());
    }

    // deliver messages to their recipients among `nodes`, dropping
    // those `lost` names
    fn deliver(
        nodes: &mut [(EpochHandle, SlotRaptorcast)],
        messages: Vec<(NodeId, Dissemination)>,
        lost: &BTreeSet<NodeId>,
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

    // the messages decoded since the last call
    fn decoded(node: &mut SlotRaptorcast) -> Vec<Bytes> {
        let mut messages = Vec::new();
        for (_, _, message) in node.drain_resolved() {
            let Ok(message) = message else {
                continue;
            };
            messages.push(message);
        }
        messages
    }

    fn message() -> Bytes {
        Bytes::from(vec![1u8; MESSAGE_LEN])
    }

    #[test]
    fn chunks_reach_everyone_through_the_second_hop_and_requests() {
        // validators 0, 1, 2 and 3 own three chunks each of validator 0's
        // proposal
        let mut nodes = Vec::new();
        for id in 1..=3 {
            let epoch_handle = epoch_handle_for(NodeId::dummy(id), 4, vec![author()]);
            let schedule = proposer_schedule(vec![author()]);
            let mut node = SlotRaptorcast::new(&epoch_handle, SLOT, NOW, &*schedule, timing());
            node.handle_command(ChorusDACommand::ReleaseChunks, NOW);
            nodes.push((epoch_handle, node));
        }
        let (header, chunks) = proposal_chunks(&nodes[0].0, 1);

        // the author's first hop reaches 1 and 2; 3 is cut off entirely
        let cut_off = BTreeSet::from([NodeId::dummy(3)]);
        let ones = group(&[chunks[2].clone(), chunks[6].clone(), chunks[10].clone()]);
        nodes[0].1.ingest(&author(), ones).expect("valid");
        let twos = group(&[chunks[3].clone(), chunks[7].clone(), chunks[11].clone()]);
        nodes[1].1.ingest(&author(), twos).expect("valid");
        let second_hop = drain_all(&mut nodes);
        deliver(&mut nodes, second_hop, &cut_off);
        assert_eq!(decoded(&mut nodes[0].1), vec![message()]);
        assert_eq!(decoded(&mut nodes[1].1), vec![message()]);
        assert!(decoded(&mut nodes[2].1).is_empty());

        // 3 asks a decoded peer for its own chunks, and both peers for theirs
        let pid = header.pid();
        let requests = [
            (1, ChunkRequest::all(pid, ChunkRequestType::MyChunks)),
            (1, ChunkRequest::all(pid, ChunkRequestType::YourChunks)),
            (2, ChunkRequest::all(pid, ChunkRequestType::YourChunks)),
        ];
        for (peer, request) in requests {
            let peer = NodeId::dummy(peer);
            let (_, server) = nodes
                .iter_mut()
                .find(|(handle, _)| handle.self_id == peer)
                .unwrap();
            server.handle_chunk_request(request, Lane::new_unchecked(0), &NodeId::dummy(3), NOW);
        }
        // 1 answers both asks in one message, 2 in another; together
        // they carry every chunk but the author's own
        let responses = drain_all(&mut nodes);
        assert_eq!(responses.len(), 2);
        assert_eq!(
            chunk_ids(responses.iter().map(|(_, message)| message)),
            [1, 2, 3, 5, 6, 7, 9, 10, 11]
        );
        deliver(&mut nodes, responses, &BTreeSet::new());
        assert_eq!(decoded(&mut nodes[2].1), vec![message()]);
    }
}
