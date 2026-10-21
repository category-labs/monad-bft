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

use monad_mcp_chorus::spec::{ProposalHeader as _, validator::ValidatorData as _};
use recovery::{Recovery, everything_missing};

use super::{
    chunk::{ChunkRequest, ChunkRequestType, ChunksSubset, ProposalEnvelope},
    egress::ChunkEgress,
    instance_rc::{InvalidEncoding, MissingChunks, RaptorcastInstance, Resolved},
    runtime::{EpochHandle, Timing},
    types::{
        EquivCert, MissingHeaders, NodeId, ProposalDAEvent, ProposalId, SignedProposalHeader,
        Timestamp, TimestampDelta,
    },
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

    // before commit: roots named by PullRoot other than the pinned one
    vouched: Vec<Recovery>,
    // third and later headers, recorded only for vouched roots
    vouched_headers: Vec<SignedProposalHeader>,

    out_events: Vec<ProposalDAEvent>,
    resolved: Vec<(ProposalId, Resolved)>,
}

// each validator votes one root per index, so at most floor(n / (f+1)) <= 3
// roots reach f+1 positive voters
const MAX_VOUCHED_ROOTS: usize = 3;

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
    // before commit: the first header's proposal. Recorded rivals and
    // vouched roots are tracked too
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
            vouched: Vec::new(),
            vouched_headers: Vec::new(),
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

    // the pinned proposal, or before commit a recorded rival or a vouched root
    fn tracks(&mut self, pid: ProposalId) -> bool {
        match &self.pin {
            Pin::Unseen => {
                let recovery = match self.take_vouched(&pid) {
                    Some(mut recovery) => {
                        recovery.widen();
                        recovery
                    }
                    None => self.recovery_for(pid),
                };
                self.pin = Pin::Open(recovery);
                true
            }
            Pin::Open(_) => self.known_header(&pid).is_some() || self.is_vouched(&pid),
            Pin::Committed(recovery) => recovery.pid() == pid,
            Pin::CommittedEmpty => false,
        }
    }

    fn is_vouched(&self, pid: &ProposalId) -> bool {
        self.vouched.iter().any(|recovery| recovery.pid() == *pid)
    }

    // the pulls of the pinned root or of a vouched one
    fn recovery_of(&mut self, pid: &ProposalId) -> Option<&mut Recovery> {
        if self.pin.pid() == Some(*pid) {
            return self.pin.recovery_mut();
        }
        self.vouched
            .iter_mut()
            .find(|recovery| recovery.pid() == *pid)
    }

    fn take_vouched(&mut self, pid: &ProposalId) -> Option<Recovery> {
        let position = self
            .vouched
            .iter()
            .position(|recovery| recovery.pid() == *pid)?;
        Some(self.vouched.remove(position))
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
        let recorded = self
            .headers
            .as_ref()
            .and_then(|headers| headers.header_for(pid));
        recorded.or_else(|| {
            self.vouched_headers
                .iter()
                .find(|header| header.pid() == *pid)
        })
    }

    fn record_header(&mut self, header: &SignedProposalHeader) {
        let next = match self.headers.take() {
            None => HeaderState::Single(header.clone()),
            Some(HeaderState::Single(first)) if first.pid() != header.pid() => {
                HeaderState::Equivocation(EquivCert(first, header.clone()))
            }
            known => {
                self.headers = known;
                self.record_vouched_header(header);
                return;
            }
        };
        self.headers = Some(next);
        self.out_events
            .push(ProposalDAEvent::HeaderSeen(header.clone()));
    }

    // a third or later distinct header, recorded once its root is vouched
    fn record_vouched_header(&mut self, header: &SignedProposalHeader) {
        let pid = header.pid();
        if !self.is_vouched(&pid) || self.known_header(&pid).is_some() {
            return;
        }
        self.vouched_headers.push(header.clone());
        self.out_events
            .push(ProposalDAEvent::HeaderSeen(header.clone()));
    }

    pub(crate) fn add_decoded_holders(
        &mut self,
        node_ids: Vec<NodeId>,
        pid: &ProposalId,
        now: Timestamp,
        epoch_handle: &EpochHandle,
    ) -> Vec<(NodeId, ChunkRequest)> {
        let Some(recovery) = self.recovery_of(pid) else {
            return vec![];
        };
        recovery.add_decoded_holders(node_ids, now);
        self.poll_chunk_requests(now, epoch_handle)
    }

    // track the root and pull it from its positive voters until resolved
    pub(crate) fn pull_root(
        &mut self,
        pid: ProposalId,
        voters: Vec<NodeId>,
        now: Timestamp,
        epoch_handle: &EpochHandle,
    ) -> Vec<(NodeId, ChunkRequest)> {
        if self.pin.is_committed() {
            return vec![];
        }
        if let Some(recovery) = self.recovery_of(&pid) {
            recovery.vouch(voters, now);
        } else if self.vouched.len() < MAX_VOUCHED_ROOTS {
            let (author, self_id) = (self.proposer, self.self_id);
            let interval = self.pull_interval;
            let recovery = Recovery::vouched(pid, author, self_id, voters, now, interval);
            self.vouched.push(recovery);
        } else {
            tracing::warn!(?pid, "dropping a pull beyond the vouched roots bound");
            return vec![];
        }
        self.poll_chunk_requests(now, epoch_handle)
    }

    pub(crate) fn pin_final(&mut self, pid: Option<ProposalId>) {
        if self.pin.is_committed() {
            tracing::warn!(?pid, "ignoring a second final pin");
            return;
        }

        // discard all other proposals, vouched ones included
        self.tracked.retain(|tracked, _| Some(*tracked) == pid);
        let vouched = pid.and_then(|pid| self.take_vouched(&pid));
        self.vouched.clear();

        self.pin = match (std::mem::take(&mut self.pin), pid, vouched) {
            // committed a negative entry
            (_, None, _) => Pin::CommittedEmpty,
            (Pin::Open(recovery), Some(pid), _) if recovery.pid() == pid => {
                Pin::Committed(recovery)
            }
            // a vouched root keeps its voters and hints, now asking every owner
            (_, Some(_), Some(mut recovery)) => {
                recovery.widen();
                Pin::Committed(recovery)
            }
            // without its header yet, the first reply to a pull brings it
            (_, Some(pid), None) => Pin::Committed(self.recovery_for(pid)),
        };
    }

    pub(crate) fn poll_chunk_requests(
        &mut self,
        now: Timestamp,
        epoch_handle: &EpochHandle,
    ) -> Vec<(NodeId, ChunkRequest)> {
        let mut requests = Vec::new();
        let recoveries = self.pin.recovery_mut().into_iter();
        for recovery in recoveries.chain(self.vouched.iter_mut()) {
            let Some(missing) = missing_chunks(&self.tracked, &recovery.pid(), epoch_handle) else {
                continue;
            };
            requests.extend(recovery.pull(missing, now));
        }
        requests
    }

    pub(crate) fn pull_header(&self, missing: MissingHeaders) -> Vec<(NodeId, ChunkRequest)> {
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

fn missing_chunks(
    tracked: &HashMap<ProposalId, RaptorcastInstance>,
    pid: &ProposalId,
    epoch_handle: &EpochHandle,
) -> Option<MissingChunks> {
    match tracked.get(pid) {
        Some(instance) => instance.missing_chunks(),
        // no header yet, so no assignment: any validator may own chunks we lack
        None => Some(everything_missing(epoch_handle.validator_data.nodes())),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

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
        assert_eq!(*to, HashSet::from([requester]));
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
        let holders = vec![NodeId::dummy(2)];
        instance.add_decoded_holders(holders, &header.pid(), NOW, &epoch_handle);
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
        let holders = vec![NodeId::dummy(2)];
        let hinted =
            instance.add_decoded_holders(holders, &header.pid(), after(150), &epoch_handle);
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
        let voters = HashSet::from([NodeId::dummy(2), NodeId::dummy(3)]);
        let missing =
            MissingHeaders::from([(header_a.pid(), voters.clone()), (header_b.pid(), voters)]);
        let pulled = instance.pull_header(missing);

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

        let voters = HashSet::from([NodeId::dummy(2), NodeId::dummy(3)]);
        let missing =
            MissingHeaders::from([(header_a.pid(), voters.clone()), (header_b.pid(), voters)]);
        let pulled = instance.pull_header(missing);

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
        let holders = vec![NodeId::dummy(2)];
        let mut asked = instance.add_decoded_holders(holders, &header.pid(), NOW, &epoch_handle);

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

    fn voters() -> Vec<NodeId> {
        vec![NodeId::dummy(2), NodeId::dummy(3)]
    }

    // the requests for one root, in a stable order
    fn requests_for(
        pid: ProposalId,
        mut pulled: Vec<(NodeId, ChunkRequest)>,
    ) -> Vec<(NodeId, ChunkRequest)> {
        pulled.retain(|(_, request)| request.pid == pid);
        requests(pulled)
    }

    // a proposer that sent the headers of `pins` to us, in order
    fn seen(
        epoch_handle: &EpochHandle,
        deadline: Timestamp,
        pins: &[&SignedProposalHeader],
    ) -> ProposerRaptorcast {
        let mut egress = released_egress();
        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, deadline, timing());
        for header in pins {
            instance.ingest(header_only(header), &author(), epoch_handle, &mut egress);
        }
        instance.drain_events();
        instance
    }

    #[test]
    fn third_root_tracked_when_vouched() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, _) = proposal_chunks(&epoch_handle, 2);
        let (header_c, chunks_c) = proposal_chunks(&epoch_handle, 3);
        let mut instance = seen(&epoch_handle, NOW, &[&header_a, &header_b]);

        instance.pull_root(header_c.pid(), voters(), NOW, &epoch_handle);
        instance.ingest(group(&chunks_c), &author(), &epoch_handle, &mut egress);
        let events = instance.drain_events();
        assert!(events.contains(&ProposalDAEvent::HeaderSeen(header_c.clone())));
        assert!(events.contains(&ProposalDAEvent::Decoded(header_c.pid())));
        assert_eq!(decoded_pids(&mut instance), vec![header_c.pid()]);

        // announced once
        instance.ingest(
            header_only(&header_c),
            &author(),
            &epoch_handle,
            &mut egress,
        );
        assert!(instance.drain_events().is_empty());
    }

    #[test]
    fn vouched_roots_are_bounded() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, _) = proposal_chunks(&epoch_handle, 2);
        let mut instance = seen(&epoch_handle, NOW, &[&header_a, &header_b]);

        for payload in 3..3 + MAX_VOUCHED_ROOTS as u8 {
            let (header, _) = proposal_chunks(&epoch_handle, payload);
            let pulled = instance.pull_root(header.pid(), voters(), NOW, &epoch_handle);
            assert!(!requests_for(header.pid(), pulled).is_empty());
        }

        // one root too many: neither pulled nor tracked
        let (header_x, chunks_x) = proposal_chunks(&epoch_handle, 9);
        let pulled = instance.pull_root(header_x.pid(), voters(), NOW, &epoch_handle);
        assert!(pulled.is_empty());
        instance.ingest(group(&chunks_x), &author(), &epoch_handle, &mut egress);
        assert!(
            !instance
                .drain_events()
                .contains(&ProposalDAEvent::HeaderSeen(header_x))
        );
        assert!(decoded_pids(&mut instance).is_empty());
    }

    #[test]
    fn pull_retries_until_decoded() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, chunks_b) = proposal_chunks(&epoch_handle, 2);
        let pid = header_b.pid();
        // pulls of the pinned root start at 400
        let mut instance = seen(&epoch_handle, after(300), &[&header_a]);

        // without b's header: ours from the author, all of theirs from the voters only
        let expected = vec![
            (
                NodeId::dummy(0),
                ChunkRequest::all(pid, ChunkRequestType::MyChunks),
            ),
            (
                NodeId::dummy(2),
                ChunkRequest::all(pid, ChunkRequestType::YourChunks),
            ),
            (
                NodeId::dummy(3),
                ChunkRequest::all(pid, ChunkRequestType::YourChunks),
            ),
        ];
        let pulled = instance.pull_root(pid, voters(), NOW, &epoch_handle);
        assert_eq!(requests(pulled), expected);

        // repeated every interval, not before
        assert!(
            instance
                .poll_chunk_requests(after(100), &epoch_handle)
                .is_empty()
        );
        let pulled = instance.poll_chunk_requests(after(200), &epoch_handle);
        assert_eq!(requests_for(pid, pulled), expected);

        // a voter's reply brings the header: the pulls narrow to what is missing
        instance.ingest(
            group(&chunks_b[1..2]),
            &NodeId::dummy(2),
            &epoch_handle,
            &mut egress,
        );
        instance.ingest(
            group(&chunks_b[2..3]),
            &NodeId::dummy(3),
            &epoch_handle,
            &mut egress,
        );
        let narrowed = |to, kind, ids: &[WireChunkId]| {
            let ids = ids.iter().map(|id| chunk_id(&epoch_handle, &header_b, *id));
            let subset = ChunksSubset::narrowed(ids);
            (NodeId::dummy(to), ChunkRequest { pid, kind, subset })
        };
        let pulled = instance.poll_chunk_requests(after(400), &epoch_handle);
        let expected = vec![
            narrowed(0, ChunkRequestType::MyChunks, &[0, 3]),
            narrowed(2, ChunkRequestType::YourChunks, &[4]),
            narrowed(3, ChunkRequestType::YourChunks, &[5]),
        ];
        assert_eq!(requests_for(pid, pulled), expected);

        // decoded: no more pulls of b
        instance.ingest(
            group(&chunks_b[4..5]),
            &NodeId::dummy(2),
            &epoch_handle,
            &mut egress,
        );
        assert_eq!(decoded_pids(&mut instance), vec![pid]);
        for millis in [600, 800, 1000] {
            let pulled = instance.poll_chunk_requests(after(millis), &epoch_handle);
            assert!(requests_for(pid, pulled).is_empty());
        }
    }

    #[test]
    fn a_vouched_pinned_root_is_pulled_from_every_owner_at_once() {
        let epoch_handle = epoch_handle();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let mut instance = seen(&epoch_handle, after(300), &[&header_a]);

        // before the deadline, and owner 3 is asked although only 2 vouched
        let pulled = instance.pull_root(header_a.pid(), vec![NodeId::dummy(2)], NOW, &epoch_handle);
        let asked: HashSet<_> = pulled.iter().map(|(to, _)| *to).collect();
        assert_eq!(asked, HashSet::from([0, 2, 3].map(NodeId::dummy)));

        // a later vouch only grows the voters
        let pulled = instance.pull_root(header_a.pid(), voters(), after(50), &epoch_handle);
        assert!(pulled.is_empty());
    }

    #[test]
    fn a_vouched_root_seen_first_becomes_the_pin_and_asks_every_owner() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_b, chunks_b) = proposal_chunks(&epoch_handle, 2);
        let pid = header_b.pid();
        let mut instance =
            ProposerRaptorcast::new(NodeId::dummy(0), epoch_handle.self_id, NOW, timing());

        let pulled = instance.pull_root(pid, vec![NodeId::dummy(2)], NOW, &epoch_handle);
        let asked: HashSet<_> = pulled.iter().map(|(to, _)| *to).collect();
        assert_eq!(asked, HashSet::from([0, 2].map(NodeId::dummy)));

        // the reply pins b; one pull per peer and kind, now to every owner
        instance.ingest(
            group(&chunks_b[1..2]),
            &NodeId::dummy(2),
            &epoch_handle,
            &mut egress,
        );
        assert_eq!(instance.pin.pid(), Some(pid));
        assert!(instance.vouched.is_empty());
        let pulled = requests(instance.poll_chunk_requests(after(200), &epoch_handle));
        let asked: Vec<_> = pulled
            .iter()
            .map(|(to, request)| (*to, request.kind))
            .collect();
        let expected = vec![
            (NodeId::dummy(0), ChunkRequestType::MyChunks),
            (NodeId::dummy(2), ChunkRequestType::YourChunks),
            (NodeId::dummy(3), ChunkRequestType::YourChunks),
        ];
        assert_eq!(asked, expected);
    }

    #[test]
    fn decoded_holders_of_a_vouched_root_are_asked_for_our_chunks() {
        let epoch_handle = epoch_handle();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, _) = proposal_chunks(&epoch_handle, 2);
        let pid = header_b.pid();
        let mut instance = seen(&epoch_handle, NOW, &[&header_a]);
        instance.pull_root(pid, voters(), NOW, &epoch_handle);

        let holders = vec![NodeId::dummy(3)];
        let hinted = instance.add_decoded_holders(holders, &pid, after(50), &epoch_handle);
        let my_chunks = ChunkRequest::all(pid, ChunkRequestType::MyChunks);
        assert!(requests_for(pid, hinted).contains(&(NodeId::dummy(3), my_chunks)));
    }

    #[test]
    fn a_final_pin_drops_the_vouched_roots() {
        let epoch_handle = epoch_handle();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, _) = proposal_chunks(&epoch_handle, 2);
        let mut instance = seen(&epoch_handle, NOW, &[&header_a]);
        instance.pull_root(header_b.pid(), voters(), NOW, &epoch_handle);

        instance.pin_final(Some(header_a.pid()));
        assert!(instance.vouched.is_empty());
        let pulled = instance.poll_chunk_requests(after(1000), &epoch_handle);
        assert!(requests_for(header_b.pid(), pulled).is_empty());

        // committed: later vouches are ignored
        let pulled = instance.pull_root(header_b.pid(), voters(), after(1000), &epoch_handle);
        assert!(pulled.is_empty());
    }

    #[test]
    fn a_final_pin_on_a_vouched_root_keeps_its_recovery_and_asks_every_owner() {
        let epoch_handle = epoch_handle();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, _) = proposal_chunks(&epoch_handle, 2);
        let pid = header_b.pid();
        let mut instance = seen(&epoch_handle, NOW, &[&header_a]);
        instance.pull_root(pid, vec![NodeId::dummy(2)], NOW, &epoch_handle);
        let holders = vec![NodeId::dummy(3)];
        instance.add_decoded_holders(holders, &pid, after(50), &epoch_handle);

        instance.pin_final(Some(pid));
        assert_eq!(instance.pin.pid(), Some(pid));
        // the hinted holder is still asked for ours; every owner for theirs
        let pulled = requests_for(pid, instance.poll_chunk_requests(after(300), &epoch_handle));
        let my_chunks = ChunkRequest::all(pid, ChunkRequestType::MyChunks);
        let your_chunks = ChunkRequest::all(pid, ChunkRequestType::YourChunks);
        let expected = vec![
            (NodeId::dummy(0), my_chunks.clone()),
            (NodeId::dummy(0), your_chunks.clone()),
            (NodeId::dummy(2), your_chunks.clone()),
            (NodeId::dummy(3), my_chunks),
            (NodeId::dummy(3), your_chunks),
        ];
        assert_eq!(pulled, expected);
    }

    #[test]
    fn a_repeated_header_pull_asks_again_until_the_header_is_recorded() {
        let epoch_handle = epoch_handle();
        let mut egress = released_egress();
        let (header_a, _) = proposal_chunks(&epoch_handle, 1);
        let (header_b, _) = proposal_chunks(&epoch_handle, 2);
        let (header_c, _) = proposal_chunks(&epoch_handle, 3);
        let mut instance = seen(&epoch_handle, NOW, &[&header_a, &header_b]);
        let missing = || MissingHeaders::from([(header_c.pid(), voters().into_iter().collect())]);

        let first = requests(instance.pull_header(missing()));
        assert_eq!(first.len(), 2);
        assert_eq!(requests(instance.pull_header(missing())), first);

        // a third header that is not vouched is dropped, so the pull stays due
        instance.ingest(
            header_only(&header_c),
            &author(),
            &epoch_handle,
            &mut egress,
        );
        assert!(instance.drain_events().is_empty());
        assert_eq!(requests(instance.pull_header(missing())), first);

        // vouched, the reply is recorded and the pull is done
        instance.pull_root(header_c.pid(), voters(), NOW, &epoch_handle);
        instance.ingest(
            header_only(&header_c),
            &author(),
            &epoch_handle,
            &mut egress,
        );
        let seen_c = ProposalDAEvent::HeaderSeen(header_c.clone());
        assert_eq!(instance.drain_events(), vec![seen_c]);
        assert!(instance.pull_header(missing()).is_empty());
    }
}
