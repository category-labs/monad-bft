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

use bytes::Bytes;
use monad_mcp_chorus::spec::validator::ValidatorData as _;

use super::{
    chunk::ProposalEnvelope,
    egress::ChunkEgress,
    encoding_scheme::DAEncodingScheme as _,
    header::{DAProposalHeader as _, InvalidProposalHeader},
    instance_rc::RaptorcastInstance,
    runtime::EpochHandle,
    types::{MerkleRoot, NodeId, ProposalDAEvent, ProposalHeader},
};

// per-(slot, proposer) raptorcast: root-keyed
pub struct ProposerRaptorcast {
    proposer: NodeId,

    // admitted roots. todo: make the chorus commitment to a digest of
    // the whole signed header.
    instances: HashMap<MerkleRoot, RaptorcastInstance>,

    // roots received from consensus. chunks from pinned roots are
    // always admitted.
    pinned: HashSet<MerkleRoot>,

    // roots whose header has been reported to consensus.
    // invariant: every instance's root is seen
    seen: HashSet<MerkleRoot>,

    // events pending delivery since the last drain
    out_events: Vec<ProposalDAEvent>,
}

impl ProposerRaptorcast {
    pub(crate) fn new(proposer: NodeId) -> Self {
        Self {
            proposer,
            instances: HashMap::new(),
            pinned: HashSet::new(),
            seen: HashSet::new(),
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

    pub(crate) fn ingest(
        &mut self,
        envelope: ProposalEnvelope,
        epoch_handle: &EpochHandle,
        egress: &mut ChunkEgress,
    ) -> Result<(), InvalidProposalHeader> {
        let (header, chunks) = envelope.into_parts();
        let root = header.root;
        self.admit(header, epoch_handle)?;

        let Some(instance) = self.instances.get_mut(&root) else {
            // dismissed root: its chunks are dropped
            return Ok(());
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
        Ok(())
    }

    // admission & instance creation. no-op for known roots.
    fn admit(
        &mut self,
        header: ProposalHeader,
        epoch_handle: &EpochHandle,
    ) -> Result<(), InvalidProposalHeader> {
        if self.instances.contains_key(&header.root) {
            return Ok(());
        }

        if self.seen.insert(header.root) {
            // fresh header, report for equivocation evidence.
            self.out_events
                .push(ProposalDAEvent::HeaderSeen(header.clone()));
        }

        let admissible = self.pinned.contains(&header.root) || self.can_admit_unpinned();
        if !admissible {
            return Ok(());
        }

        let num_validators = epoch_handle.validator_data.len();
        let layout = header
            .encoding_scheme()
            .packet_layout(num_validators)
            .ok_or(InvalidProposalHeader::NoPacketLayout)?;

        let root = header.root;
        let instance = RaptorcastInstance::new(epoch_handle, header, layout, &self.proposer);
        self.instances.insert(root, instance);
        Ok(())
    }

    // pin a root to ensure its chunks are always admitted.
    pub(crate) fn pin(&mut self, root: &MerkleRoot) {
        self.pinned.insert(*root);
    }

    // Invariant: we only keep at most one unpinned instance to ensure
    // constant memory.
    fn can_admit_unpinned(&self) -> bool {
        self.instances.keys().all(|root| self.pinned.contains(root))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::test_util::{epoch_handle, group, proposal_chunks},
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
        instance
            .ingest(
                ProposalEnvelope::from_header(header.clone()),
                &epoch_handle,
                &mut egress,
            )
            .expect("well-formed header");
        let events = instance.drain_events();

        assert!(events.contains(&ProposalDAEvent::HeaderSeen(header)));
    }

    #[test]
    fn header_alone_creates_the_instance() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);

        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        instance
            .ingest(
                ProposalEnvelope::from_header(header.clone()),
                &epoch_handle,
                &mut egress,
            )
            .expect("well-formed header");
        let events = instance.drain_events();
        // the chunkless author owes nothing from the start
        let author_owes_nothing = ProposalDAEvent::OwnerObligationFulfilled {
            owner: NodeId::dummy(0),
            root: header.root,
        };
        assert_eq!(
            events,
            vec![
                ProposalDAEvent::HeaderSeen(header.clone()),
                author_owes_nothing
            ]
        );

        // the instance exists: later chunks decode it
        instance
            .ingest(group(&chunks[..3]), &epoch_handle, &mut egress)
            .expect("well-formed header");
        let events = instance.drain_events();
        assert!(events.contains(&ProposalDAEvent::Decoded(header.root)));
    }

    #[test]
    fn second_unpinned_root_announces_its_header_once() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, chunks_b) = proposal_chunks(&epoch_handle, 2);

        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        instance
            .ingest(
                ProposalEnvelope::from_header(header_a),
                &epoch_handle,
                &mut egress,
            )
            .expect("well-formed header");
        instance.drain_events();

        instance
            .ingest(
                ProposalEnvelope::from_header(header_b.clone()),
                &epoch_handle,
                &mut egress,
            )
            .expect("well-formed header");
        let events = instance.drain_events();
        assert_eq!(events, vec![ProposalDAEvent::HeaderSeen(header_b.clone())]);
        instance
            .ingest(
                ProposalEnvelope::from_header(header_b.clone()),
                &epoch_handle,
                &mut egress,
            )
            .expect("well-formed header");
        let events = instance.drain_events();
        assert!(events.is_empty());

        // announced once; the rival is not assembled, so its chunks
        // are dropped silently and it never decodes
        instance
            .ingest(group(&chunks_b), &epoch_handle, &mut egress)
            .expect("well-formed header");
        assert!(instance.drain_events().is_empty());
        assert!(instance.decoded_message(&header_b.root).is_none());
    }

    #[test]
    fn pinned_roots_are_always_admitted() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, _) = proposal_chunks(&epoch_handle, 2);

        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        instance
            .ingest(
                ProposalEnvelope::from_header(header_a),
                &epoch_handle,
                &mut egress,
            )
            .expect("well-formed header");
        instance.drain_events();

        instance.pin(&header_b.root);
        instance
            .ingest(
                ProposalEnvelope::from_header(header_b.clone()),
                &epoch_handle,
                &mut egress,
            )
            .expect("well-formed header");
        let events = instance.drain_events();
        assert!(events.contains(&ProposalDAEvent::HeaderSeen(header_b)));

        // still at most one unpinned instance: a third root is announced
        // but not assembled
        let (header_c, chunks_c) = proposal_chunks(&epoch_handle, 3);
        instance
            .ingest(
                ProposalEnvelope::from_header(header_c.clone()),
                &epoch_handle,
                &mut egress,
            )
            .expect("well-formed header");
        let events = instance.drain_events();
        assert_eq!(events, vec![ProposalDAEvent::HeaderSeen(header_c)]);
        instance
            .ingest(group(&chunks_c[..1]), &epoch_handle, &mut egress)
            .expect("well-formed header");
        let events = instance.drain_events();
        assert!(events.is_empty());
    }

    #[test]
    fn pinning_a_seen_root_admits_without_reannouncing() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, chunks_b) = proposal_chunks(&epoch_handle, 2);

        let mut instance = ProposerRaptorcast::new(NodeId::dummy(0));
        instance
            .ingest(
                ProposalEnvelope::from_header(header_a),
                &epoch_handle,
                &mut egress,
            )
            .expect("well-formed header");
        instance.drain_events();

        // rejected rival: header reported once
        instance
            .ingest(
                ProposalEnvelope::from_header(header_b.clone()),
                &epoch_handle,
                &mut egress,
            )
            .expect("well-formed header");
        let events = instance.drain_events();
        assert_eq!(events, vec![ProposalDAEvent::HeaderSeen(header_b.clone())]);

        instance.pin(&header_b.root);

        // admitted now, without a second announcement: only the
        // chunkless author's vacuous obligation
        instance
            .ingest(
                ProposalEnvelope::from_header(header_b.clone()),
                &epoch_handle,
                &mut egress,
            )
            .expect("well-formed header");
        let events = instance.drain_events();
        let author_owes_nothing = ProposalDAEvent::OwnerObligationFulfilled {
            owner: NodeId::dummy(0),
            root: header_b.root,
        };
        assert_eq!(events, vec![author_owes_nothing]);

        // and genuinely assembled: enough chunks decode it
        instance
            .ingest(group(&chunks_b[..3]), &epoch_handle, &mut egress)
            .expect("well-formed header");
        let events = instance.drain_events();
        assert!(events.contains(&ProposalDAEvent::Decoded(header_b.root)));
    }
}
