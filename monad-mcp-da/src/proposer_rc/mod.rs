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

mod recovery;

use std::collections::HashMap;

use monad_mcp_chorus::spec::ProposalHeader as _;
use monad_mcp_common::containers::ValidatorSet;
use recovery::{Recovery, everything_missing};

use super::{
    chunk::{ChunkRequest, ChunkRequestType, ChunksSubset, ProposalEnvelope},
    egress::ChunkEgress,
    instance_rc::{InvalidEncoding, MissingChunks, RaptorcastInstance, Resolved},
    runtime::{EpochHandle, Timing},
    types::{
        EquivCert, MissingHeaders, NodeId, ProposalDAEvent, ProposalId, SignedProposalHeader,
        Timestamp, TimestampDelta, ValidatorData,
    },
    util::shuffle_sends,
};

// per-(slot, proposer) raptorcast: keyed by proposal pid
pub struct ProposerRaptorcast {
    proposer: NodeId,
    self_id: NodeId,

    pull_interval: TimestampDelta,
    // how often each tracked proposal serves a requester
    serve_window: TimestampDelta,

    pin: Pin,
    first_pull: Timestamp, // nothing is pulled before this timestamp

    tracked: HashMap<ProposalId, RaptorcastInstance>,
    headers: Option<HeaderState>,

    out_events: Vec<ProposalDAEvent>,
    resolved: Vec<(ProposalId, Resolved)>,
}

enum HeaderState {
    Single(SignedProposalHeader),
    Equivocation(EquivCert),
}

impl HeaderState {
    fn header_for(&self, pid: &ProposalId) -> Option<&SignedProposalHeader> {
        let headers: &[&SignedProposalHeader] = match self {
            HeaderState::Single(header) => &[header],
            HeaderState::Equivocation(EquivCert(first, second)) => &[first, second],
        };
        headers.iter().copied().find(|header| header.pid() == *pid)
    }
}

// the proposal we pull: the first one seen, then the committed one
#[derive(Default)]
enum Pin {
    // no header yet
    #[default]
    Unseen,
    // before commit: the first header's proposal. A recorded rival is tracked too
    Open(Recovery),
    // the committed proposal, the only one tracked
    Committed(Recovery),
    // a negative entry: nothing is tracked
    CommittedEmpty,
}

impl Pin {
    fn recovery_mut(&mut self) -> Option<&mut Recovery> {
        match self {
            Pin::Open(recovery) | Pin::Committed(recovery) => Some(recovery),
            Pin::Unseen | Pin::CommittedEmpty => None,
        }
    }

    fn pid(&self) -> Option<ProposalId> {
        match self {
            Pin::Open(recovery) | Pin::Committed(recovery) => Some(recovery.pid()),
            Pin::Unseen | Pin::CommittedEmpty => None,
        }
    }

    fn is_committed(&self) -> bool {
        matches!(self, Pin::Committed(_) | Pin::CommittedEmpty)
    }
}

impl ProposerRaptorcast {
    pub(crate) fn new(
        proposer: NodeId,
        self_id: NodeId,
        deadline: Timestamp,
        timing: Timing,
    ) -> Self {
        Self {
            proposer,
            self_id,
            first_pull: deadline + timing.pull_delay,
            pull_interval: timing.pull_interval,
            serve_window: timing.serve_window,
            headers: None,
            tracked: HashMap::new(),
            pin: Pin::Unseen,
            out_events: Vec::new(),
            resolved: Vec::new(),
        }
    }

    pub(crate) fn drain_events(&mut self) -> Vec<ProposalDAEvent> {
        std::mem::take(&mut self.out_events)
    }

    pub(crate) fn drain_resolved(&mut self) -> Vec<(ProposalId, Resolved)> {
        std::mem::take(&mut self.resolved)
    }

    // The caller must ensure the header is authenticated.
    pub(crate) fn ingest(
        &mut self,
        envelope: ProposalEnvelope,
        sender: &NodeId,
        epoch_handle: &EpochHandle,
        egress: &mut ChunkEgress,
    ) {
        let (header, chunks) = envelope.into_parts();
        let pid = header.pid();
        self.record_header(&header);
        if !self.tracks(pid) {
            // an untracked proposal: its chunks are dropped
            return;
        }

        let author = &self.proposer;
        let serve_window = self.serve_window;
        let instance = self
            .tracked
            .entry(pid)
            .or_insert_with(|| RaptorcastInstance::new(header, epoch_handle, author, serve_window));

        for (wire_chunk_id, data) in chunks {
            if let Err(err) = instance.ingest(wire_chunk_id, data, sender) {
                tracing::debug!(?pid, wire_chunk_id, ?err, "dropping invalid chunk");
            };
        }

        self.out_events.extend(instance.take_obligation_event());
        match instance.try_decode() {
            None => {}
            Some(Ok(message)) => {
                self.out_events.push(ProposalDAEvent::Decoded(pid));
                self.resolved.push((pid, Ok(message)));
            }
            Some(Err(InvalidEncoding)) => {
                tracing::debug!(?pid, "proposal does not re-encode to its pid");
                self.out_events.push(ProposalDAEvent::DecodingFailed(pid));
                self.resolved.push((pid, Err(InvalidEncoding)));
            }
        }
        instance.rebroadcast_chunks(egress);
    }

    // the pinned proposal, or before commit a recorded rival
    fn tracks(&mut self, pid: ProposalId) -> bool {
        match &self.pin {
            Pin::Unseen => {
                self.pin = Pin::Open(self.recovery_for(pid));
                true
            }
            Pin::Open(_) => self.known_header(&pid).is_some(),
            Pin::Committed(recovery) => recovery.pid() == pid,
            Pin::CommittedEmpty => false,
        }
    }

    fn recovery_for(&self, pid: ProposalId) -> Recovery {
        Recovery::new(
            pid,
            self.proposer,
            self.self_id,
            self.first_pull,
            self.pull_interval,
        )
    }

    fn known_header(&self, pid: &ProposalId) -> Option<&SignedProposalHeader> {
        self.headers.as_ref()?.header_for(pid)
    }

    fn record_header(&mut self, header: &SignedProposalHeader) {
        let next = match self.headers.take() {
            None => HeaderState::Single(header.clone()),
            Some(HeaderState::Single(first)) if first.pid() != header.pid() => {
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

    pub(crate) fn add_decoded_holders(
        &mut self,
        holders: &ValidatorSet,
        pid: &ProposalId,
        now: Timestamp,
        epoch_handle: &EpochHandle,
    ) -> Vec<(NodeId, ChunkRequest)> {
        if self.pin.pid() != Some(*pid) {
            return vec![];
        }
        let Some(recovery) = self.pin.recovery_mut() else {
            return vec![];
        };
        recovery.add_decoded_holders(holders, now);
        self.poll_chunk_requests(now, epoch_handle)
    }

    pub(crate) fn pin_final(&mut self, pid: Option<ProposalId>) {
        if self.pin.is_committed() {
            tracing::warn!(?pid, "ignoring a second final pin");
            return;
        }

        // discard all other proposals
        self.tracked.retain(|tracked, _| Some(*tracked) == pid);

        self.pin = match (std::mem::take(&mut self.pin), pid) {
            // committed a negative entry
            (_, None) => Pin::CommittedEmpty,
            (Pin::Open(recovery), Some(pid)) if recovery.pid() == pid => Pin::Committed(recovery),
            // without its header yet, the first reply to a pull brings it
            (_, Some(pid)) => Pin::Committed(self.recovery_for(pid)),
        };
    }

    pub(crate) fn poll_chunk_requests(
        &mut self,
        now: Timestamp,
        epoch_handle: &EpochHandle,
    ) -> Vec<(NodeId, ChunkRequest)> {
        let Some(pid) = self.pin.pid() else {
            return vec![];
        };
        let Some(missing) = self.missing_chunks(&pid, epoch_handle) else {
            return vec![];
        };
        let Some(recovery) = self.pin.recovery_mut() else {
            return vec![];
        };
        recovery.pull(missing, now, &epoch_handle.validator_data)
    }

    fn missing_chunks(
        &self,
        pid: &ProposalId,
        epoch_handle: &EpochHandle,
    ) -> Option<MissingChunks> {
        use monad_mcp_chorus::spec::validator::ValidatorData as _;

        match self.tracked.get(pid) {
            Some(instance) => instance.missing_chunks(),
            // no header yet, so no assignment: any validator may own chunks we lack
            None => Some(everything_missing(epoch_handle.validator_data.nodes())),
        }
    }

    // `validator_data` is the epoch's that the voters were drawn from
    pub(crate) fn pull_header(
        &self,
        missing: MissingHeaders,
        validator_data: &ValidatorData,
    ) -> Vec<(NodeId, ChunkRequest)> {
        use monad_mcp_chorus::spec::validator::ValidatorData as _;

        let mut subset = match self.pin {
            // heuristic, HeaderOnly alone is also correct.
            Pin::Unseen => ChunksSubset::All,
            _ => ChunksSubset::HeaderOnly,
        };
        let mut requests = vec![];
        for (pid, voters) in missing {
            if self.tracked.contains_key(&pid) {
                continue;
            }

            // in index order, which the shuffle below starts from
            let mut voters: Vec<NodeId> = voters
                .iter()
                .map(|voter| *validator_data.node_id(voter))
                .collect();

            shuffle_sends(&mut voters, &pid, &self.self_id);
            for voter in voters {
                let request = ChunkRequest {
                    pid,
                    kind: ChunkRequestType::YourChunks,
                    subset: subset.clone(),
                };
                requests.push((voter, request));
            }
            subset = ChunksSubset::HeaderOnly;
        }
        requests
    }

    pub(crate) fn handle_chunk_request(
        &mut self,
        request: ChunkRequest,
        requester: &NodeId,
        now: Timestamp,
        egress: &mut ChunkEgress,
    ) {
        let Some(instance) = self.tracked.get_mut(&request.pid) else {
            return;
        };
        instance.handle_chunk_request(request, requester, now, egress);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            chunk::{ChunkRequestType, ChunksSubset, WireChunkId},
            egress::Dissemination,
            test_util::{
                NOW, author, chunk_id, epoch_handle, group, inconsistent_proposal_chunks,
                proposal_chunks, timing,
            },
            types::TimestampDelta,
        },
        *,
    };

    // the validators of the epoch with these dummy ids
    fn validators<const N: usize>(epoch_handle: &EpochHandle, ids: [u64; N]) -> ValidatorSet {
        use monad_mcp_chorus::spec::validator::ValidatorData as _;

        ids.into_iter()
            .map(|id| {
                epoch_handle
                    .validator_data
                    .index_of(&NodeId::dummy(id))
                    .expect("a validator")
            })
            .collect()
    }

    fn decoded_pids(instance: &mut ProposerRaptorcast) -> Vec<ProposalId> {
        let mut pids = Vec::new();
        for (pid, message) in instance.drain_resolved() {
            if message.is_ok() {
                pids.push(pid);
            }
        }
        pids
    }

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

        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.ingest(
            ProposalEnvelope::from_header(header.clone()),
            &author(),
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

        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.ingest(
            ProposalEnvelope::from_header(header.clone()),
            &author(),
            &epoch_handle,
            &mut egress,
        );
        let events = instance.drain_events();
        assert_eq!(events, vec![ProposalDAEvent::HeaderSeen(header.clone())]);

        // the instance exists: later chunks decode it
        instance.ingest(group(&chunks[..3]), &author(), &epoch_handle, &mut egress);
        let events = instance.drain_events();
        assert!(events.contains(&ProposalDAEvent::Decoded(header.pid())));
    }

    #[test]
    fn an_invalid_root_resolves_with_its_encoding_error() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header, chunks) = inconsistent_proposal_chunks(&epoch_handle);
        let pid = header.pid();

        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.ingest(group(&chunks[..3]), &author(), &epoch_handle, &mut egress);

        let events = instance.drain_events();
        assert!(events.contains(&ProposalDAEvent::DecodingFailed(pid)));
        let resolved = instance.drain_resolved();
        assert_eq!(resolved, vec![(pid, Err(InvalidEncoding))]);
    }

    #[test]
    fn second_unpinned_root_announces_its_header_once() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, chunks_b) = proposal_chunks(&epoch_handle, 2);

        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.ingest(
            ProposalEnvelope::from_header(header_a),
            &author(),
            &epoch_handle,
            &mut egress,
        );
        instance.drain_events();

        instance.ingest(
            ProposalEnvelope::from_header(header_b.clone()),
            &author(),
            &epoch_handle,
            &mut egress,
        );
        let events = instance.drain_events();
        assert_eq!(events, vec![ProposalDAEvent::HeaderSeen(header_b.clone())]);
        instance.ingest(
            ProposalEnvelope::from_header(header_b.clone()),
            &author(),
            &epoch_handle,
            &mut egress,
        );
        let events = instance.drain_events();
        assert!(events.is_empty());

        // announced once; the rival's chunks decode it like any tracked root
        instance.ingest(group(&chunks_b), &author(), &epoch_handle, &mut egress);
        let events = instance.drain_events();
        assert!(!events.contains(&ProposalDAEvent::HeaderSeen(header_b.clone())));
        assert_eq!(decoded_pids(&mut instance), vec![header_b.pid()]);
    }

    #[test]
    fn a_third_root_is_not_tracked() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, _) = proposal_chunks(&epoch_handle, 2);
        let (_, chunks_c) = proposal_chunks(&epoch_handle, 3);

        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.ingest(
            header_only(&header_a),
            &author(),
            &epoch_handle,
            &mut egress,
        );
        instance.ingest(
            header_only(&header_b),
            &author(),
            &epoch_handle,
            &mut egress,
        );
        instance.drain_events();
        egress.drain();

        // every chunk of the third root: no event, no decode, nothing relayed
        instance.ingest(group(&chunks_c), &author(), &epoch_handle, &mut egress);
        assert!(instance.drain_events().is_empty());
        assert!(decoded_pids(&mut instance).is_empty());
        assert!(egress.drain().is_empty());
    }

    #[test]
    fn your_chunk_recovery_serves_what_we_hold_before_decoding() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);

        // chunk ids 0 and 3 are ours (validator 1); ingesting ids 0
        // and 1 is short of the decoding threshold
        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.ingest(group(&chunks[..2]), &author(), &epoch_handle, &mut egress);
        assert!(decoded_pids(&mut instance).is_empty());
        // discard the rebroadcast of our own chunk
        egress.drain();

        let requester = NodeId::dummy(3);
        let your_chunks = ChunkRequest::all(header.pid(), ChunkRequestType::YourChunks);
        instance.handle_chunk_request(your_chunks, &requester, NOW, &mut egress);
        let recovered = egress.drain();
        let [Dissemination { to, envelope }] = &recovered[..] else {
            panic!("recovery unicasts to the requester");
        };
        assert_eq!(*to, [requester]);
        assert_eq!(
            envelope.chunk_data().keys().copied().collect::<Vec<_>>(),
            [0]
        );

        // the requester's own chunks (ids 2 and 5) are not held yet
        let my_chunks = ChunkRequest::all(header.pid(), ChunkRequestType::MyChunks);
        instance.handle_chunk_request(my_chunks, &requester, NOW, &mut egress);
        assert!(egress.drain().is_empty());
    }

    fn after(millis: u64) -> Timestamp {
        NOW + TimestampDelta::from_millis(millis)
    }

    fn header_only(header: &SignedProposalHeader) -> ProposalEnvelope {
        ProposalEnvelope::from_header(header.clone())
    }

    // the (peer, request) pairs, in a stable order
    fn requests(mut requests: Vec<(NodeId, ChunkRequest)>) -> Vec<(NodeId, ChunkRequest)> {
        requests.sort_by_key(|(to, request)| (*to, request.kind == ChunkRequestType::YourChunks));
        requests
    }

    #[test]
    fn a_recorded_rival_root_is_decoded_and_served_like_the_first() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, chunks_b) = proposal_chunks(&epoch_handle, 2);

        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.ingest(
            header_only(&header_a),
            &author(),
            &epoch_handle,
            &mut egress,
        );
        instance.ingest(group(&chunks_b), &author(), &epoch_handle, &mut egress);

        assert_eq!(decoded_pids(&mut instance), vec![header_b.pid()]);
        egress.drain();
        let your_chunks = ChunkRequest::all(header_b.pid(), ChunkRequestType::YourChunks);
        instance.handle_chunk_request(your_chunks, &NodeId::dummy(1), NOW, &mut egress);
        assert!(!egress.drain().is_empty());
    }

    #[test]
    fn a_final_pin_keeps_the_committed_rival_and_frees_the_rest() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, chunks_b) = proposal_chunks(&epoch_handle, 2);

        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.ingest(
            header_only(&header_a),
            &author(),
            &epoch_handle,
            &mut egress,
        );
        instance.ingest(group(&chunks_b[..3]), &author(), &epoch_handle, &mut egress);
        assert_eq!(decoded_pids(&mut instance), vec![header_b.pid()]);

        instance.pin_final(Some(header_b.pid()));
        assert!(!instance.tracked.contains_key(&header_a.pid()));

        // a second final pin changes nothing
        instance.pin_final(None);
        assert!(instance.tracked.contains_key(&header_b.pid()));
    }

    #[test]
    fn a_negative_final_pin_frees_every_root() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, chunks_a) = proposal_chunks(&epoch_handle, 1);

        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.ingest(group(&chunks_a[..1]), &author(), &epoch_handle, &mut egress);
        instance.pin_final(None);

        assert!(instance.tracked.is_empty());
        instance.ingest(
            group(&chunks_a[1..2]),
            &author(),
            &epoch_handle,
            &mut egress,
        );
        assert!(!instance.tracked.contains_key(&header_a.pid()));
    }

    #[test]
    fn pulls_start_a_delta_after_the_deadline_and_repeat_after_the_interval() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);

        // we are 1 and own 0 and 3; node 2 owns 1 and 4; node 3 owns 2 and 5
        let deadline = after(300);
        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, deadline, timing());
        instance.ingest(group(&chunks[..2]), &author(), &epoch_handle, &mut egress);

        let pulled = instance.poll_chunk_requests(after(350), &epoch_handle);
        assert!(requests(pulled).is_empty());

        // Delta after the deadline: the author for ours, every owner for theirs
        let narrowed = |kind, ids: &[WireChunkId]| ChunkRequest {
            pid: header.pid(),
            kind,
            subset: ChunksSubset::narrowed(
                ids.iter().map(|id| chunk_id(&epoch_handle, &header, *id)),
            ),
        };
        let expected = vec![
            (NodeId::dummy(0), narrowed(ChunkRequestType::MyChunks, &[3])),
            (
                NodeId::dummy(2),
                narrowed(ChunkRequestType::YourChunks, &[4]),
            ),
            (
                NodeId::dummy(3),
                narrowed(ChunkRequestType::YourChunks, &[2, 5]),
            ),
        ];
        let pulled = instance.poll_chunk_requests(after(400), &epoch_handle);
        assert_eq!(requests(pulled), expected);

        // asked within the interval: not again
        let pulled = instance.poll_chunk_requests(after(500), &epoch_handle);
        assert!(requests(pulled).is_empty());
        let pulled = instance.poll_chunk_requests(after(600), &epoch_handle);
        assert_eq!(requests(pulled), expected);

        // decoded: nothing more to pull
        instance.ingest(group(&chunks[2..3]), &author(), &epoch_handle, &mut egress);
        let pulled = instance.poll_chunk_requests(after(1000), &epoch_handle);
        assert!(requests(pulled).is_empty());
    }

    #[test]
    fn decoded_holders_are_asked_for_our_own_chunks() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);

        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.ingest(group(&chunks[1..2]), &author(), &epoch_handle, &mut egress);
        let holders = validators(&epoch_handle, [2]);
        instance.add_decoded_holders(&holders, &header.pid(), NOW, &epoch_handle);
        let pulled = instance.poll_chunk_requests(after(200), &epoch_handle);

        let my_chunks = ChunkRequest {
            pid: header.pid(),
            kind: ChunkRequestType::MyChunks,
            subset: ChunksSubset::narrowed([0, 3].map(|id| chunk_id(&epoch_handle, &header, id))),
        };
        assert!(requests(pulled).contains(&(NodeId::dummy(2), my_chunks)));
    }

    #[test]
    fn a_decoded_hint_asks_for_our_chunks_at_once() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);

        // we own 0 and 3; node 2 owns 1 and 4
        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.ingest(group(&chunks[1..2]), &author(), &epoch_handle, &mut egress);

        let request = |kind, ids: &[WireChunkId]| ChunkRequest {
            pid: header.pid(),
            kind,
            subset: ChunksSubset::narrowed(
                ids.iter().map(|id| chunk_id(&epoch_handle, &header, *id)),
            ),
        };
        let my_chunks = (
            NodeId::dummy(2),
            request(ChunkRequestType::MyChunks, &[0, 3]),
        );
        let your_chunks = (
            NodeId::dummy(2),
            request(ChunkRequestType::YourChunks, &[4]),
        );
        let asked_holder = |pulled: Vec<(NodeId, ChunkRequest)>| {
            let mut asked = requests(pulled);
            asked.retain(|(node, _)| *node == NodeId::dummy(2));
            asked
        };

        // before the hint, node 2 is asked as an owner only
        let pulled = instance.poll_chunk_requests(after(100), &epoch_handle);
        assert_eq!(asked_holder(pulled), vec![your_chunks.clone()]);

        // the hint asks for our chunks at once, the owner pull waits
        let holders = validators(&epoch_handle, [2]);
        let hinted =
            instance.add_decoded_holders(&holders, &header.pid(), after(150), &epoch_handle);
        assert_eq!(asked_holder(hinted), vec![my_chunks.clone()]);
        assert!(asked_holder(instance.poll_chunk_requests(after(200), &epoch_handle)).is_empty());

        // each kind repeats on its own timer
        let pulled = instance.poll_chunk_requests(after(300), &epoch_handle);
        assert_eq!(asked_holder(pulled), vec![your_chunks]);
        let pulled = instance.poll_chunk_requests(after(350), &epoch_handle);
        assert_eq!(asked_holder(pulled), vec![my_chunks]);
    }

    #[test]
    fn with_no_root_seen_a_header_pull_also_fetches_the_first_roots_chunks() {
        let epoch_handle = epoch_handle();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, _) = proposal_chunks(&epoch_handle, 2);

        let instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        let voters = validators(&epoch_handle, [2, 3]);
        let missing = MissingHeaders::from([(header_a.pid(), voters), (header_b.pid(), voters)]);
        let pulled = instance.pull_header(missing, &epoch_handle.validator_data);

        let all_of_a = ChunkRequest::all(header_a.pid(), ChunkRequestType::YourChunks);
        let header_of_b = ChunkRequest {
            pid: header_b.pid(),
            kind: ChunkRequestType::YourChunks,
            subset: ChunksSubset::HeaderOnly,
        };
        let expected = vec![
            (NodeId::dummy(2), all_of_a.clone()),
            (NodeId::dummy(2), header_of_b.clone()),
            (NodeId::dummy(3), all_of_a),
            (NodeId::dummy(3), header_of_b),
        ];
        assert_eq!(requests(pulled), expected);
    }

    #[test]
    fn a_header_pull_asks_each_voter_of_an_unknown_root_for_the_header_alone() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, _) = proposal_chunks(&epoch_handle, 2);

        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.ingest(
            header_only(&header_a),
            &author(),
            &epoch_handle,
            &mut egress,
        );

        let voters = validators(&epoch_handle, [2, 3]);
        let missing = MissingHeaders::from([(header_a.pid(), voters), (header_b.pid(), voters)]);
        let pulled = instance.pull_header(missing, &epoch_handle.validator_data);

        // a is known, so only b is asked for
        let header_alone = ChunkRequest {
            pid: header_b.pid(),
            kind: ChunkRequestType::YourChunks,
            subset: ChunksSubset::HeaderOnly,
        };
        let expected = vec![
            (NodeId::dummy(2), header_alone.clone()),
            (NodeId::dummy(3), header_alone),
        ];
        assert_eq!(requests(pulled), expected);
    }

    #[test]
    fn a_final_pin_on_the_rival_pulls_it_without_waiting_out_the_open_roots_cooldown() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (_, chunks_a) = proposal_chunks(&epoch_handle, 1);
        let (header_b, _) = proposal_chunks(&epoch_handle, 2);

        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.ingest(group(&chunks_a[..1]), &author(), &epoch_handle, &mut egress);
        instance.ingest(
            header_only(&header_b),
            &author(),
            &epoch_handle,
            &mut egress,
        );
        assert!(
            !instance
                .poll_chunk_requests(after(100), &epoch_handle)
                .is_empty()
        );

        instance.pin_final(Some(header_b.pid()));
        let pulled = instance.poll_chunk_requests(after(150), &epoch_handle);
        assert!(!pulled.is_empty());
        for (_, request) in &pulled {
            assert_eq!(&request.pid, &header_b.pid());
        }
    }

    #[test]
    fn a_final_pin_without_a_header_pulls_everyone_until_a_reply_brings_it() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header, chunks) = proposal_chunks(&epoch_handle, 1);

        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());
        instance.pin_final(Some(header.pid()));
        let holders = validators(&epoch_handle, [2]);
        let mut asked = instance.add_decoded_holders(&holders, &header.pid(), NOW, &epoch_handle);

        // owners are pulled from Delta after the deadline, even once committed
        assert!(instance.poll_chunk_requests(NOW, &epoch_handle).is_empty());
        asked.extend(instance.poll_chunk_requests(after(100), &epoch_handle));

        let your_chunks = ChunkRequest::all(header.pid(), ChunkRequestType::YourChunks);
        let my_chunks = ChunkRequest::all(header.pid(), ChunkRequestType::MyChunks);
        let expected = vec![
            (NodeId::dummy(0), my_chunks.clone()),
            (NodeId::dummy(0), your_chunks.clone()),
            (NodeId::dummy(2), my_chunks),
            (NodeId::dummy(2), your_chunks.clone()),
            (NodeId::dummy(3), your_chunks),
        ];
        assert_eq!(requests(asked), expected);

        // the first reply carries the header and starts the committed root
        instance.ingest(
            group(&chunks[1..2]),
            &NodeId::dummy(2),
            &epoch_handle,
            &mut egress,
        );
        instance.ingest(group(&chunks[..3]), &author(), &epoch_handle, &mut egress);
        assert_eq!(decoded_pids(&mut instance), vec![header.pid()]);
    }
}
