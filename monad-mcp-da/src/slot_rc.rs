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
use monad_mcp_chorus::spec::proposal::HeaderAuth as _;

use super::{
    chunk::ProposalEnvelope,
    egress::{ChunkEgress, Dissemination},
    election::ProposerElection,
    header::InvalidProposalHeader,
    proposer_rc::ProposerRaptorcast,
    runtime::{DAOutput, EpochHandle},
    types::{
        ChorusDACommand, ChorusDAEvent, MerkleRoot, ProposalHeader, ProposalIndex, ProposalMap,
        Slot,
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
    authenticated_headers: HashMap<ProposalHeader, ProposalIndex>,

    egress: ChunkEgress,

    // events pending delivery since the last drain
    out_events: Vec<ChorusDAEvent>,
}

impl SlotRaptorcast {
    pub fn new<E>(epoch_handle: &EpochHandle, slot: Slot, election: &E) -> Self
    where
        E: ProposerElection,
    {
        let raptorcasts = ProposalMap::new(epoch_handle.num_proposals, |j| {
            let proposer = election.get_proposer(slot, j)?;
            Some(ProposerRaptorcast::new(*proposer))
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

    pub fn ingest(&mut self, envelope: ProposalEnvelope) -> Result<(), InvalidProposalHeader> {
        debug_assert!(envelope.header().slot == self.slot);

        let j = self
            .authenticate(envelope.header())
            .ok_or(InvalidProposalHeader::Unauthenticated)?;

        let raptorcast = self.raptorcasts[j]
            .as_mut()
            .expect("authenticated indices have proposers");
        raptorcast.ingest(envelope, &self.epoch_handle, &mut self.egress)?;

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
    fn authenticate(&mut self, header: &ProposalHeader) -> Option<ProposalIndex> {
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

    fn raptorcast_mut(&mut self, j: ProposalIndex) -> Option<&mut ProposerRaptorcast> {
        if j >= self.raptorcasts.size() {
            return None;
        }
        self.raptorcasts[j].as_mut()
    }

    pub(crate) fn handle_command(&mut self, command: ChorusDACommand) -> Vec<DAOutput> {
        match command {
            ChorusDACommand::ReleaseChunks => {
                self.egress.release();
                vec![]
            }
            ChorusDACommand::PinRoot { j, root } => {
                if let Some(raptorcast) = self.raptorcast_mut(j) {
                    raptorcast.pin(&root);
                }
                vec![]
            }
            ChorusDACommand::RecoverChunks { j, root, .. } => {
                if let Some(raptorcast) = self.raptorcast_mut(j) {
                    raptorcast.pin(&root);
                }
                vec![]
            }
        }
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

    use super::{
        super::{
            chunk::WireChunkId,
            test_util::{
                Proposers, SLOT, author, epoch_handle, epoch_handle_for, group, proposal_chunks,
                proposal_chunks_from, validator_data,
            },
            types::{HeaderAuth, NodeId, ProposalDAEvent, ProposalKeyPair},
        },
        *,
    };

    fn slot_raptorcast() -> (EpochHandle, SlotRaptorcast) {
        let epoch_handle = epoch_handle();
        let election = Proposers::new(vec![author()]);
        let raptorcast = SlotRaptorcast::new(&epoch_handle, SLOT, &election);
        (epoch_handle, raptorcast)
    }

    fn nodes(ids: impl IntoIterator<Item = u64>) -> HashSet<NodeId> {
        ids.into_iter().map(NodeId::dummy).collect()
    }

    // the wire ids carried by the messages, sorted
    fn chunk_ids(messages: &[Dissemination]) -> Vec<WireChunkId> {
        let mut ids = Vec::new();
        for message in messages {
            ids.extend(message.envelope.chunks().keys().copied());
        }
        ids.sort();
        ids
    }

    // ingest, returning the messages it caused
    fn ingest(raptorcast: &mut SlotRaptorcast, envelope: ProposalEnvelope) -> Vec<Dissemination> {
        raptorcast.ingest(envelope).expect("well-formed header");
        raptorcast.drain_messages()
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
            event: ProposalDAEvent::Decoded(header.root),
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

        let rejected = raptorcast.ingest(group(&chunks[..1]));
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
            num_proposals: 1,
            key_pair: Arc::new(ProposalKeyPair::dummy(NodeId::dummy(1))),
            header_auth: Arc::new(HeaderAuth::new(move |_slot, signer| {
                counted.fetch_add(1, Ordering::SeqCst);
                (*signer == author()).then_some(0)
            })),
            validator_data: Arc::new(validator_data(4)),
        };
        let election = Proposers::new(vec![author()]);
        let mut raptorcast = SlotRaptorcast::new(&epoch_handle, SLOT, &election);
        let (_, chunks) = proposal_chunks(&epoch_handle, 1);

        raptorcast.ingest(group(&chunks[..1])).expect("valid");
        raptorcast.ingest(group(&chunks[1..2])).expect("valid");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_second_proposers_events_carry_its_index() {
        let proposers = vec![author(), NodeId::dummy(2)];
        let epoch_handle = epoch_handle_for(NodeId::dummy(1), 4, proposers.clone());
        let election = Proposers::new(proposers);
        let mut raptorcast = SlotRaptorcast::new(&epoch_handle, SLOT, &election);
        let (header, chunks) = proposal_chunks_from(&epoch_handle, 2, SLOT, 1);

        raptorcast.ingest(group(&chunks[..1])).expect("valid");
        let events = raptorcast.drain_events();
        assert!(events.contains(&ChorusDAEvent {
            j: 1,
            event: ProposalDAEvent::HeaderSeen(header),
        }));
        assert!(events.iter().all(|event| event.j == 1));
    }
}
