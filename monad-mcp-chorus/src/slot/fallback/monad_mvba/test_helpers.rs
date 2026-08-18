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

//! A four-validator cluster (f = 1) built on the stub environment.
//!
//! Certificates here are aggregated the same way production does it -- through
//! [`VotePool`] and [`TimeoutCollector`] -- over a signature scheme that does
//! no real cryptography. The tests therefore exercise the quorum arithmetic
//! and the signing domains, not the primitives underneath.

use std::{collections::HashMap, sync::Arc};

use super::{
    super::{
        super::{
            fast::{CertifiedEntry, EnterFallbackVote, Entry},
            types::{
                IsVote, MerkleRoot, NodeId, ProposalMap, Slot, StrongQc, TimestampDelta,
                ValidatorData, VoteMsg, VotePool,
            },
        },
        FallbackView, MVBAInputs, MVBAOutput, Mvba, PartialBlock,
        block_sync::{BlockRequestMsg, BlockResponseMsg},
    },
    Context, MonadMvba, TimerEvent,
    certificates::{PrepareQc, TimeoutCertificate},
    collectors::TimeoutCollector,
    messages::{CommitVote, Message, PrePrepareMsg, PrepareVote, TimeoutMsg},
};

pub(super) const NUM_NODES: u64 = 4;
pub(super) const NUM_PROPOSALS: usize = 2;
pub(super) const SLOT: Slot = Slot(0);
pub(super) const DELTA: TimestampDelta = TimestampDelta::from_millis(10);

pub(super) type Output = MVBAOutput<Message, TimerEvent>;

pub(super) fn view(v: u64) -> FallbackView {
    FallbackView(v)
}

pub(super) fn nodes() -> Vec<NodeId> {
    (0..NUM_NODES).map(NodeId::dummy).collect()
}

/// Three of four validators: a supermajority here, since every validator has
/// stake one.
pub(super) fn quorum() -> Vec<NodeId> {
    nodes()[..3].to_vec()
}

pub(super) fn validator_data() -> Arc<ValidatorData> {
    let valset: HashMap<_, _> = nodes()
        .into_iter()
        .map(|node| (node, 1u64.into()))
        .collect();
    let mapping: HashMap<_, _> = nodes()
        .into_iter()
        .map(|node| {
            let pubkey = crate::spec::vote::KeyPair::pubkey(&node.keypair());
            (node, pubkey)
        })
        .collect();

    Arc::new(ValidatorData::new(valset, mapping))
}

/// The same round-robin the implementation uses, restated so the tests do not
/// take their expectations from the code under test.
pub(super) fn leader_of(v: FallbackView) -> NodeId {
    let index = (SLOT.get() + v.get()) % NUM_NODES;
    nodes()[index as usize]
}

pub(super) fn mvba(node: NodeId, validator_data: &Arc<ValidatorData>) -> MonadMvba {
    MonadMvba::new(Context {
        slot: SLOT,
        num_proposals: NUM_PROPOSALS,
        node_id: node,
        key: Arc::new(node.keypair()),
        validator_data: validator_data.clone(),
        delta: DELTA,
    })
}

/// A valid metablock whose entries are determined by `seed`, so two seeds give
/// two metablocks that cannot be confused for one another.
pub(super) fn metablock(seed: u64, validator_data: &ValidatorData) -> MVBAInputs {
    let enter_fallback_cert = strong_qc(SLOT, EnterFallbackVote, &quorum(), validator_data);

    let block = ProposalMap::new(NUM_PROPOSALS, |j| {
        let entry = Entry::Positive {
            root: MerkleRoot(seed * 100 + j as u64),
        };
        let fast_qc = strong_qc((SLOT, j), entry, &quorum(), validator_data);
        CertifiedEntry::FastQc(fast_qc)
    });

    MVBAInputs {
        enter_fallback_cert,
        block,
    }
}

/// Aggregate a genuine supermajority certificate over `vote`.
pub(super) fn strong_qc<V: IsVote>(
    scope: V::Scope,
    vote: V,
    signers: &[NodeId],
    validator_data: &ValidatorData,
) -> StrongQc<V> {
    let mut pool = VotePool::new(scope.clone());
    for node in signers {
        let msg = VoteMsg::new_signed(scope.clone(), vote.clone(), &node.keypair());
        pool.add_vote(*node, msg);
    }

    pool.try_form_strong_qc(validator_data)
        .expect("the signers hold a supermajority of stake")
}

pub(super) fn prepare_qc(
    v: FallbackView,
    entries: &ProposalMap<Entry>,
    validator_data: &ValidatorData,
) -> PrepareQc {
    strong_qc(
        (SLOT, v),
        PrepareVote(entries.clone()),
        &quorum(),
        validator_data,
    )
}

pub(super) fn pre_prepare(
    v: FallbackView,
    block: &MVBAInputs,
    justification: Option<TimeoutCertificate>,
) -> (NodeId, Message) {
    let leader = leader_of(v);
    let msg = PrePrepareMsg::new_signed(SLOT, v, block.clone(), justification, &leader.keypair());

    (leader, Message::PrePrepare(msg))
}

/// A timeout certificate for `v`, aggregated from timeouts carrying
/// `high_prep_qc`. No block travels with it: a leader bound by the lock fetches
/// the block behind the entries the certificate names.
pub(super) fn timeout_certificate(
    v: FallbackView,
    high_prep_qc: Option<PrepareQc>,
    validator_data: &ValidatorData,
) -> TimeoutCertificate {
    let mut collector = TimeoutCollector::new(SLOT);
    for node in quorum() {
        collector.add(
            node,
            TimeoutMsg::new_signed(SLOT, v, high_prep_qc.clone(), &node.keypair()),
        );
    }

    collector
        .try_form_tc(v, validator_data)
        .expect("a supermajority of timeouts forms a certificate")
}

pub(super) fn feed_prepare_votes(
    instance: &mut MonadMvba,
    v: FallbackView,
    entries: &ProposalMap<Entry>,
    signers: &[NodeId],
) {
    for node in signers {
        let msg = VoteMsg::new_signed((SLOT, v), PrepareVote(entries.clone()), &node.keypair());
        instance.handle_message(*node, Message::Prepare(msg));
    }
}

/// Commit votes range over entries alone; feeding them hands the receiver no
/// block.
pub(super) fn feed_commit_votes(
    instance: &mut MonadMvba,
    v: FallbackView,
    block: &MVBAInputs,
    signers: &[NodeId],
) {
    for node in signers {
        let msg = VoteMsg::new_signed((SLOT, v), CommitVote(block.entries()), &node.keypair());
        instance.handle_message(*node, Message::Commit(msg));
    }
}

/// A commit certificate over the entries of `block`, as the wire message
/// carries it: the certificate alone.
pub(super) fn commit_qc_message(
    v: FallbackView,
    block: &MVBAInputs,
    validator_data: &ValidatorData,
) -> Message {
    let qc = strong_qc(
        (SLOT, v),
        CommitVote(block.entries()),
        &quorum(),
        validator_data,
    );

    Message::CommitQc(qc)
}

pub(super) fn feed_timeouts(
    instance: &mut MonadMvba,
    v: FallbackView,
    high_prep_qc: Option<PrepareQc>,
    signers: &[NodeId],
) {
    for node in signers {
        let msg = TimeoutMsg::new_signed(SLOT, v, high_prep_qc.clone(), &node.keypair());
        instance.handle_message(*node, Message::Timeout(msg));
    }
}

/// A block response as it arrives on the wire, carrying `block` verbatim.
pub(super) fn block_response(block: PartialBlock) -> Message {
    Message::BlockResponse(BlockResponseMsg { slot: SLOT, block })
}

pub(super) fn block_request(entries: &ProposalMap<Entry>) -> Message {
    Message::BlockRequest(BlockRequestMsg {
        slot: SLOT,
        entries: entries.clone(),
    })
}

pub(super) fn drain(instance: &mut MonadMvba) -> Vec<Output> {
    std::iter::from_fn(|| instance.poll()).collect()
}

pub(super) fn broadcasts(outputs: &[Output]) -> Vec<&Message> {
    outputs
        .iter()
        .filter_map(|output| match output {
            MVBAOutput::Broadcast(message) => Some(message),
            MVBAOutput::Unicast { .. } | MVBAOutput::ScheduleTimer { .. } => None,
        })
        .collect()
}

pub(super) fn unicasts(outputs: &[Output]) -> Vec<(NodeId, &Message)> {
    outputs
        .iter()
        .filter_map(|output| match output {
            MVBAOutput::Unicast { to, message } => Some((*to, message)),
            MVBAOutput::Broadcast(_) | MVBAOutput::ScheduleTimer { .. } => None,
        })
        .collect()
}

pub(super) fn scheduled_timers(outputs: &[Output]) -> Vec<TimerEvent> {
    outputs
        .iter()
        .filter_map(|output| match output {
            MVBAOutput::ScheduleTimer { timer_event, .. } => Some(*timer_event),
            MVBAOutput::Broadcast(_) | MVBAOutput::Unicast { .. } => None,
        })
        .collect()
}

/// The entries of every block request broadcast in `outputs`.
pub(super) fn requested_entries(outputs: &[Output]) -> Vec<ProposalMap<Entry>> {
    broadcasts(outputs)
        .into_iter()
        .filter_map(|message| match message {
            Message::BlockRequest(request) => Some(request.entries.clone()),
            _ => None,
        })
        .collect()
}

/// The entries of the single prepare vote in `outputs`, if there is one.
pub(super) fn prepared_entries(outputs: &[Output]) -> Option<ProposalMap<Entry>> {
    broadcasts(outputs)
        .into_iter()
        .find_map(|message| match message {
            Message::Prepare(vote) => Some(vote.vote.0.clone()),
            _ => None,
        })
}

pub(super) fn committed_entries(outputs: &[Output]) -> Option<ProposalMap<Entry>> {
    broadcasts(outputs)
        .into_iter()
        .find_map(|message| match message {
            Message::Commit(commit) => Some(commit.vote.0.clone()),
            _ => None,
        })
}

pub(super) fn decided_commit_qc(outputs: &[Output]) -> bool {
    broadcasts(outputs)
        .iter()
        .any(|message| matches!(message, Message::CommitQc(_)))
}

pub(super) fn timed_out_view(outputs: &[Output]) -> Option<FallbackView> {
    broadcasts(outputs)
        .into_iter()
        .find_map(|message| match message {
            Message::Timeout(timeout) => Some(timeout.view()),
            _ => None,
        })
}

pub(super) fn proposed(outputs: &[Output]) -> Option<&PrePrepareMsg> {
    broadcasts(outputs)
        .into_iter()
        .find_map(|message| match message {
            Message::PrePrepare(msg) => Some(msg),
            _ => None,
        })
}

/// Start an instance that has proposed `block` and drained its start-up
/// outputs.
pub(super) fn started(
    node: NodeId,
    block: &MVBAInputs,
    validator_data: &Arc<ValidatorData>,
) -> MonadMvba {
    let mut instance = mvba(node, validator_data);
    instance.propose(block.clone());
    instance
}
