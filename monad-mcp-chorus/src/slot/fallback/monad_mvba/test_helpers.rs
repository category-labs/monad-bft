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

//! A four-validator cluster (f = 1) built on the stub environment
//!
//! Certificates here are aggregated the same way production does it -- through
//! [`VotePool`] and [`ViewCollectors`] -- over a signature scheme that does
//! no real cryptography. The tests therefore exercise the quorum arithmetic
//! and the signing domains, not the primitives underneath

use std::{collections::HashMap, sync::Arc};

use super::{
    super::{
        super::{
            fast::{CertifiedEntry, EnterFallbackCert, EnterFallbackVote, Entry, FallbackEntry},
            types::{
                IsVote, MerkleRoot, NodeId, ProposalMap, Slot, StrongQc, TimestampDelta,
                ValidatorData, VoteMsg, VotePool, WeakQc,
            },
        },
        FallbackView, MVBAOutput, Metablock, Mvba,
    },
    MvbaContext,
    block_store::{BlockRequestMsg, BlockResponseMsg},
    messages::{FallbackCommitVote, PrepareVote},
};

// The `V = Metablock` instantiation the existing suite runs on; the toy-value
// test in `tests` is what pins genericity
pub(super) type MonadMvba = super::MonadMvba<Metablock, EnterFallbackCert>;
pub(super) type Message = super::messages::MvbaMessage<Metablock, EnterFallbackCert>;
pub(super) type PrePrepareMsg = super::messages::PrePrepareMsg<Metablock, EnterFallbackCert>;
pub(super) type Justification = super::messages::Justification<Metablock, EnterFallbackCert>;
pub(super) type TimeoutMsg = super::messages::TimeoutMsg<Metablock>;
pub(super) type PrepareQc = super::certificates::PrepareQc<Metablock>;
pub(super) type TimeoutCertificate = super::certificates::TimeoutCertificate<Metablock>;
pub(super) type ViewCollectors = super::collectors::ViewCollectors<Metablock, EnterFallbackCert>;
pub(super) type TimerEvent = super::TimerEvent<Metablock>;

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

/// Three of four validators: a supermajority, since every validator has stake
/// one
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
/// take their expectations from the code under test
pub(super) fn leader_of(v: FallbackView) -> NodeId {
    let index = (SLOT.get() + v.get()) % NUM_NODES;
    nodes()[index as usize]
}

pub(super) fn mvba(node: NodeId, validator_data: &Arc<ValidatorData>) -> MonadMvba {
    MonadMvba::new(MvbaContext {
        slot: SLOT,
        num_proposals: NUM_PROPOSALS,
        node_id: node,
        key: Arc::new(node.keypair()),
        validator_data: validator_data.clone(),
        delta: DELTA,
    })
}

/// A valid metablock whose entries are determined by `seed`, so two seeds give
/// two metablocks that cannot be confused for one another
pub(super) fn metablock(seed: u64, validator_data: &ValidatorData) -> Metablock {
    Metablock::new(ProposalMap::new(NUM_PROPOSALS, |j| {
        let entry = Entry::Positive(MerkleRoot(seed * 100 + j as u64));
        let fast_qc = strong_qc((SLOT, j), entry, &quorum(), validator_data);
        CertifiedEntry::FastQc(fast_qc)
    }))
}

/// Valid but *not* a fast metablock: its first entry rests on a fallback
/// quorum. `entries()` matches [`metablock`] on the same seed, so the two
/// differ only in their evidence
pub(super) fn mixed_evidence_metablock(seed: u64, validator_data: &ValidatorData) -> Metablock {
    Metablock::new(ProposalMap::new(NUM_PROPOSALS, |j| {
        let entry = Entry::Positive(MerkleRoot(seed * 100 + j as u64));
        if j == 0 {
            let qc = weak_qc((SLOT, j), FallbackEntry(entry), &quorum(), validator_data);
            CertifiedEntry::FallbackQc(qc)
        } else {
            CertifiedEntry::FastQc(strong_qc((SLOT, j), entry, &quorum(), validator_data))
        }
    }))
}

/// A genuine fallback certificate for this slot
pub(super) fn enter_fallback_cert(validator_data: &ValidatorData) -> EnterFallbackCert {
    strong_qc(SLOT, EnterFallbackVote, &quorum(), validator_data)
}

/// Aggregate a genuine supermajority certificate over `vote`
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

/// Aggregate a genuine `f+1` certificate over `vote`
pub(super) fn weak_qc<V: IsVote>(
    scope: V::Scope,
    vote: V,
    signers: &[NodeId],
    validator_data: &ValidatorData,
) -> WeakQc<V> {
    let mut pool = VotePool::new(scope.clone());
    for node in signers {
        let msg = VoteMsg::new_signed(scope.clone(), vote.clone(), &node.keypair());
        pool.add_vote(*node, msg);
    }

    pool.try_form_weak_qc(validator_data)
        .expect("the signers hold more than an honest threshold of stake")
        .left()
        .expect("the signers all voted the same way")
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

/// A pre-prepare as an honest leader would send it, justified by whichever arm
/// the view it is for admits
pub(super) fn pre_prepare(
    v: FallbackView,
    block: &Metablock,
    justification: Option<TimeoutCertificate>,
) -> (NodeId, Message) {
    let justification = if v == view(1) {
        Justification::FallbackCert(Some(enter_fallback_cert(&validator_data())))
    } else {
        Justification::Tc(
            justification.expect("a view above 1 is justified by a timeout certificate"),
        )
    };

    pre_prepare_justified(v, block, justification)
}

/// A view-1 pre-prepare carrying exactly the certificate given, for the cases
/// the certificate itself is under test. `None` is `fbcert = ⊥`
pub(super) fn pre_prepare_with_cert(
    v: FallbackView,
    block: &Metablock,
    cert: Option<EnterFallbackCert>,
) -> (NodeId, Message) {
    pre_prepare_justified(v, block, Justification::FallbackCert(cert))
}

fn pre_prepare_justified(
    v: FallbackView,
    block: &Metablock,
    justification: Justification,
) -> (NodeId, Message) {
    let leader = leader_of(v);
    let msg = PrePrepareMsg::new_signed(SLOT, v, block.clone(), justification, &leader.keypair());

    (leader, Message::PrePrepare(msg))
}

/// A timeout certificate for `v`, aggregated from timeouts carrying
/// `high_prep_qc`. No block travels with it
pub(super) fn timeout_certificate(
    v: FallbackView,
    high_prep_qc: Option<PrepareQc>,
    validator_data: &ValidatorData,
) -> TimeoutCertificate {
    timeout_certificate_in(SLOT, v, high_prep_qc, validator_data)
}

/// [`timeout_certificate`] for a slot other than the one under test
pub(super) fn timeout_certificate_in(
    slot: Slot,
    v: FallbackView,
    high_prep_qc: Option<PrepareQc>,
    validator_data: &ValidatorData,
) -> TimeoutCertificate {
    let mut collectors = ViewCollectors::new(slot, v);
    for node in quorum() {
        collectors.store_timeout(
            node,
            TimeoutMsg::new_signed(slot, v, high_prep_qc.clone(), &node.keypair()),
        );
    }

    collectors
        .try_form_tc(validator_data)
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
/// block
pub(super) fn feed_commit_votes(
    instance: &mut MonadMvba,
    v: FallbackView,
    block: &Metablock,
    signers: &[NodeId],
) {
    for node in signers {
        let msg = VoteMsg::new_signed(
            (SLOT, v),
            FallbackCommitVote(block.entries()),
            &node.keypair(),
        );
        instance.handle_message(*node, Message::Commit(msg));
    }
}

/// A commit certificate over the entries of `block`, as the wire message
/// carries it: the certificate alone
pub(super) fn commit_qc_message(
    v: FallbackView,
    block: &Metablock,
    validator_data: &ValidatorData,
) -> Message {
    let qc = strong_qc(
        (SLOT, v),
        FallbackCommitVote(block.entries()),
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

/// A block response as it arrives on the wire, carrying `block` verbatim
pub(super) fn block_response(block: Metablock) -> Message {
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
            MVBAOutput::ScheduleTimer { timer_event, .. } => Some(timer_event.clone()),
            MVBAOutput::Broadcast(_) | MVBAOutput::Unicast { .. } => None,
        })
        .collect()
}

/// The entries of every block request broadcast in `outputs`
pub(super) fn requested_entries(outputs: &[Output]) -> Vec<ProposalMap<Entry>> {
    broadcasts(outputs)
        .into_iter()
        .filter_map(|message| match message {
            Message::BlockRequest(request) => Some(request.entries.clone()),
            _ => None,
        })
        .collect()
}

/// The entries of the single prepare vote in `outputs`, if there is one
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

/// The timeout this instance broadcast, whose `high_prep_qc` is what it
/// reports its lock to be
pub(super) fn timeout_message(outputs: &[Output]) -> Option<&TimeoutMsg> {
    broadcasts(outputs)
        .into_iter()
        .find_map(|message| match message {
            Message::Timeout(timeout) => Some(timeout),
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

/// An instance that has run the happy path in view 1 to a decision over
/// `block`. `node` must not lead view 1, so the proposal it accepts is the one
/// fed to it here
pub(super) fn decided(
    node: NodeId,
    block: &Metablock,
    validator_data: &Arc<ValidatorData>,
) -> MonadMvba {
    assert_ne!(
        node,
        leader_of(view(1)),
        "decided() feeds the proposal; the node must not lead view 1"
    );
    let mut instance = started(node, block, validator_data);

    let (leader, proposal) = pre_prepare(view(1), block, None);
    instance.handle_message(leader, proposal);
    feed_prepare_votes(&mut instance, view(1), &block.entries(), &quorum());
    feed_commit_votes(&mut instance, view(1), block, &quorum());

    instance
}

/// Start an instance that has proposed `block` and drained its start-up
/// outputs
pub(super) fn started(
    node: NodeId,
    block: &Metablock,
    validator_data: &Arc<ValidatorData>,
) -> MonadMvba {
    let mut instance = mvba(node, validator_data);
    instance.propose(block.clone(), Some(enter_fallback_cert(validator_data)));
    instance
}
