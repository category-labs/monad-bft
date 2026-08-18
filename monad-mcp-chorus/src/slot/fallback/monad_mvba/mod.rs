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

//! Monad's MVBA (Multi-Valued Validated Byzantine Agreement) instance used by
//! the fallback path: a leader-based, view-changing agreement on a metablock.
//!
//! # Shape of the implementation
//!
//! Handlers do not decide anything. Receiving a message validates it cheaply
//! and stores it -- in a collector or the proposal buffer -- and then calls
//! [`MonadMvba::try_advance`], which repeatedly asks
//! [`MonadMvba::find_pending_transition`] what is now possible and applies it,
//! until nothing more is. Every guard in the protocol lives in that one
//! function, and it hands back the certificate it found, so applying a
//! transition cannot fail.
//!
//! Splitting it this way is what makes out-of-order arrival free: a prepare or
//! commit quorum that completed before its proposal arrived simply fires the
//! moment the proposal lands, and a validator that fell behind cascades
//! through several views in a single call once the timeout certificate that
//! explains the gap shows up.
//!
//! # Persistence
//!
//! The paper requires `v`, `lastVotedView`, `PrepQC` and `DecidedQC` to be
//! durable, persisted atomically before any message the transition enables is
//! sent. There is no persistence backend here yet; the points where the write
//! belongs are marked `persist-before-send` in the code.

mod certificates;
mod collectors;
mod messages;
mod metablock;
mod phases;
#[cfg(test)]
mod test_helpers;
#[cfg(test)]
mod tests;

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use certificates::{CommitQc, HighPrepare, PrepareQc, TimeoutCertificate};
use collectors::{TimeoutCollector, VoteCollector};
use messages::{
    CommitVote, CommitVoteMsg, Message, PrePrepareMsg, PrepareVote, PrepareVoteMsg, TimeoutMsg,
};
use metablock::entries_of;
use phases::{Decided, InnerPhase, Phase, TimedOut, Transition};

use super::{
    super::{
        fast::Entry,
        types::{
            IsVote, KeyPair, NodeId, ProposalMap, Slot, TimestampDelta, Validated, ValidatorData,
            VoteMsg,
        },
    },
    FallbackView, MVBAInputs, MVBAOutput, Mvba, PartialBlock, Validate, Votable,
};
use crate::spec::validator::ValidatorData as _;

/// How many views ahead of the current one this instance stores messages for.
/// Bounds what a lying or partitioned peer can make it hold.
const MAX_FUTURE_VIEWS: u64 = 10;

/// View timeout in multiples of Δ: a proposal, a prepare round and a commit
/// round. Constant per view for now -- the paper's timing table is still a
/// stub, and backoff can be added without touching the state machine.
const VIEW_TIMEOUT_DELTAS: u64 = 3;

/// Per-instance state handed to [`MonadMvba::new`].
pub(crate) struct Context {
    /// Slot this MVBA instance decides for.
    pub slot: Slot,
    /// How many proposers this slot has, and hence how many entries a valid
    /// metablock carries.
    pub num_proposals: usize,
    /// This validator's own identity, needed to know when it leads a view.
    pub node_id: NodeId,
    pub key: Arc<KeyPair>,
    pub validator_data: Arc<ValidatorData>,
    /// Capital Δ from the paper; the view timeout is a multiple of it.
    pub delta: TimestampDelta,
}

impl Context {
    /// `Leader(slot, view)` from the paper: a deterministic public function of
    /// the slot and the view.
    ///
    /// Placeholder round-robin over the validator set's canonical enumeration;
    /// it ignores stake. Real leader election lands with the rest of the
    /// validator-set plumbing.
    pub(crate) fn leader(&self, view: FallbackView) -> NodeId {
        let num_nodes = self.validator_data.nodes().count() as u64;
        // ValidatorData's invariant: the validator set is non-empty.
        debug_assert!(num_nodes > 0);

        let index = self.slot.get().wrapping_add(view.get()) % num_nodes;
        *self
            .validator_data
            .nodes()
            .nth(index as usize)
            .expect("index is taken modulo the validator set size")
    }

    fn view_timeout(&self) -> TimestampDelta {
        self.delta
            .checked_mul(VIEW_TIMEOUT_DELTAS)
            .expect("view timeout overflows the timestamp range")
    }
}

/// Timers driven by the MVBA state machine.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum TimerEvent {
    /// View change timeout: no decision reached in the given MVBA view.
    ViewTimeout(FallbackView),
}

/// What checking a metablock needs: it is only valid relative to a slot and a
/// validator set.
pub(crate) struct ValidationContext {
    pub slot: Slot,
    pub num_proposals: usize,
    pub validator_data: Arc<ValidatorData>,
}

impl Validate for MVBAInputs {
    type Context = ValidationContext;

    fn validate(&self, context: &Self::Context) -> bool {
        self.is_valid(context.slot, context.num_proposals, &context.validator_data)
    }
}

impl Votable for MVBAInputs {
    type Entries = ProposalMap<Entry>;

    fn entries(&self) -> Self::Entries {
        MVBAInputs::entries(self)
    }
}

pub(crate) struct MonadMvba {
    context: Context,

    /// The paper's `v`.
    view: FallbackView,
    phase: Phase,

    /// This validator's input metablock, set by [`Mvba::propose`]. It also
    /// gates participation: nothing is sent before it is set.
    input: Option<MVBAInputs>,

    /// `lastVotedView_i`: this validator votes in a view only if the view is
    /// above it.
    last_voted_view: FallbackView,
    /// `PrepQC_i`: the highest prepare certificate seen, with the certified
    /// entries it locks when this validator holds them. Certificate and block
    /// are the one [`HighPrepare`] a timeout forwards, so neither can go
    /// missing on its own.
    prep_qc: Option<HighPrepare>,
    /// `DecidedQC_i`: known as soon as a commit certificate is, which is all a
    /// decision needs.
    decided_qc: Option<CommitQc>,
    /// `entries(x)` of the decided value.
    ///
    /// FIXME: you don't need to fetch the original preprepare/mvba input, proposalmap<entry> is enough
    ///
    /// Response: agreed, and this is now the whole of the decision. The commit
    /// certificate's verdict *is* the entries, so a validator that never saw
    /// the view's pre-prepare decides from the certificate alone; there is
    /// nothing left to fetch and nothing left to remember. The metablock the
    /// entries came from added no evidence a supermajority of commit votes did
    /// not already carry, so it no longer rides on commit votes or on the
    /// commit-certificate message, and the `known_blocks` map that held the
    /// ones learned that way is gone. `Mvba::decision` returns
    /// `Votable::Entries` to match.
    ///
    /// The block is still carried where it is genuinely needed: on the
    /// pre-prepare, which a validator must check valid before voting, and on
    /// timeouts, so a locked leader has something it is allowed to propose.
    decision: Option<ProposalMap<Entry>>,

    /// The first pre-prepare seen per view, for the current view or a nearby
    /// future one. First write wins, so a Byzantine leader cannot displace the
    /// proposal a validator is already working on with a second one.
    pre_prepares: BTreeMap<FallbackView, Validated<PrePrepareMsg>>,

    prepare_votes: VoteCollector<PrepareVote>,
    commit_votes: VoteCollector<CommitVote>,
    timeouts: TimeoutCollector,

    /// Whether the timer for the *current* view has fired; cleared on entering
    /// a view.
    timer_fired: bool,

    outputs: VecDeque<MVBAOutput<Message, TimerEvent>>,
    abandoned: bool,
}

impl Mvba<MVBAInputs> for MonadMvba {
    type Message = Message;
    type Context = Context;
    type TimerEvent = TimerEvent;
    type CommitVote = CommitVote;

    fn new(context: Context) -> Self {
        let slot = context.slot;

        Self {
            context,
            view: FallbackView::FIRST,
            phase: Phase::new(),
            input: None,
            last_voted_view: FallbackView::GENESIS,
            prep_qc: None,
            decided_qc: None,
            decision: None,
            pre_prepares: BTreeMap::new(),
            prepare_votes: VoteCollector::new(slot),
            commit_votes: VoteCollector::new(slot),
            timeouts: TimeoutCollector::new(slot),
            timer_fired: false,
            outputs: VecDeque::new(),
            abandoned: false,
        }
    }

    fn propose(&mut self, data: MVBAInputs) {
        if self.abandoned || self.input.is_some() {
            // at most one proposal per instance.
            return;
        }

        self.input = Some(data);

        // entering view 1 starts the view timer and, if this validator leads
        // it, broadcasts the proposal.
        let outputs = self.enter_view(FallbackView::FIRST, None);
        self.outputs.extend(outputs);

        self.try_advance();
    }

    fn handle_message(&mut self, sender: NodeId, message: Self::Message) {
        if self.abandoned {
            return;
        }
        debug_assert!(self.context.validator_data.contains(&sender));

        match message {
            Message::PrePrepare(msg) => self.store_pre_prepare(sender, msg),
            Message::Prepare(msg) => self.store_prepare_vote(sender, msg),
            Message::Commit(msg) => self.store_commit_vote(sender, msg),
            Message::Timeout(msg) => self.store_timeout(sender, msg),
            Message::CommitQc(qc) => self.store_commit_qc(qc),
        }

        self.try_advance();
    }

    fn handle_timer(&mut self, timer_event: Self::TimerEvent) {
        if self.abandoned {
            return;
        }

        let TimerEvent::ViewTimeout(view) = timer_event;
        if view != self.view {
            // a timer from a view this instance has already left.
            return;
        }

        self.timer_fired = true;
        self.try_advance();
    }

    fn abandon(&mut self) {
        // halts all sending: pending outputs have not left this instance yet,
        // so dropping them is what "stop sending" means here.
        self.abandoned = true;
        self.outputs.clear();
    }

    fn decision(&self) -> Option<&ProposalMap<Entry>> {
        self.decision.as_ref()
    }

    fn decision_qc(&self) -> Option<&CommitQc> {
        // an instance with no input, or an abandoned one, stores a certificate
        // it never acts on; only a completed decision is exposed.
        self.decision.as_ref().and(self.decided_qc.as_ref())
    }

    fn poll(&mut self) -> Option<MVBAOutput<Self::Message, Self::TimerEvent>> {
        self.outputs.pop_front()
    }
}

// ---------------- message ingress: validate cheaply, store ----------------

impl MonadMvba {
    fn store_pre_prepare(&mut self, sender: NodeId, msg: PrePrepareMsg) {
        let view = msg.view;
        if msg.slot != self.context.slot || !self.in_window(view) {
            return;
        }

        // leader authentication: a valid metablock proposed by anyone other
        // than the view's leader is discarded.
        if sender != self.context.leader(view) {
            return;
        }
        if !msg.verify_signature(self.context.validator_data.get_pubkey(&sender)) {
            return;
        }

        // the remaining checks -- metablock validity, justification, the lock
        // rule, last_voted_view -- depend on state that may still change, so
        // they live in `find_pending_transition` instead.
        self.pre_prepares
            .entry(view)
            // first write wins.
            .or_insert_with(|| Validated::new_unchecked(msg, sender));
    }

    fn store_prepare_vote(&mut self, sender: NodeId, msg: PrepareVoteMsg) {
        let (slot, view) = msg.scope;
        if slot != self.context.slot || !self.in_window(view) {
            return;
        }

        self.prepare_votes.add(sender, msg);
    }

    fn store_commit_vote(&mut self, sender: NodeId, msg: CommitVoteMsg) {
        let (slot, view) = msg.scope;
        if slot != self.context.slot || !self.in_window(view) {
            return;
        }

        self.commit_votes.add(sender, msg);
    }

    fn store_timeout(&mut self, sender: NodeId, msg: TimeoutMsg) {
        if msg.slot() != self.context.slot || !self.in_window(msg.view()) {
            return;
        }

        // the claim in the signed digest must be backed by the certificate
        // riding along, so that a timeout certificate can trust the claims of
        // the timeouts it aggregates.
        if !msg.is_valid(&self.context.validator_data) {
            return;
        }

        self.timeouts.add(sender, msg);
    }

    fn store_commit_qc(&mut self, qc: CommitQc) {
        if qc.scope.0 != self.context.slot || !qc.verify(&self.context.validator_data) {
            return;
        }

        if self.decided_qc.is_none() {
            // persist-before-send: DecidedQC.
            self.decided_qc = Some(qc);
        }
    }

    /// Whether messages for `view` are worth keeping: not from a view already
    /// left, not further ahead than this instance will buffer.
    fn in_window(&self, view: FallbackView) -> bool {
        view >= self.view && view.get() <= self.view.get() + MAX_FUTURE_VIEWS
    }
}

// ---------------- the state machine ----------------

impl MonadMvba {
    /// Whether this instance still acts: it has an input, has not been
    /// abandoned, and has not decided.
    fn is_running(&self) -> bool {
        self.input.is_some() && !self.abandoned && !self.phase.is_decided()
    }

    /// Apply transitions until none is possible.
    ///
    /// Each iteration takes the phase by value and gets a new one back, so no
    /// caller can observe a half-applied transition, and the transition table
    /// in [`MonadMvba::step`] must handle every pair it is given.
    fn try_advance(&mut self) {
        if !self.is_running() {
            return;
        }

        while let Some(transition) = self.find_pending_transition() {
            // the placeholder is never observed: `step` reads the phase only
            // through the value it was handed.
            let phase = std::mem::replace(&mut self.phase, Phase::new());
            let (phase, outputs) = self.step(phase, transition);

            self.phase = phase;
            self.outputs.extend(outputs);

            if self.phase.is_decided() {
                break;
            }
        }

        self.gc();
    }

    /// The single place every protocol guard lives. Pure: it constructs the
    /// evidence for what it found and changes nothing.
    ///
    /// Priority matters. A decision ends the instance, so it is checked first.
    /// A prepare certificate is checked before a view change, so this
    /// validator carries the highest lock it can into the next view. A view
    /// change is checked before this validator's own timeout, so a certificate
    /// that already exists is not delayed by one more round of timeouts. The
    /// proposal comes last: it is the only transition that requires the view
    /// to be untouched.
    fn find_pending_transition(&self) -> Option<Transition> {
        if self.phase.is_decided() {
            return None;
        }

        if let Some(qc) = self.pending_commit_qc() {
            return Some(Transition::CommitQc(qc));
        }

        if let Some(qc) = self.pending_prepare_qc() {
            return Some(Transition::PrepareQc(qc));
        }

        if let Some(tc) = self.pending_tc() {
            return Some(Transition::Tc(tc));
        }

        // f+1 stake having timed out obliges this validator to send its own
        // timeout even though its timer has not fired.
        let owes_timeout = self.timer_fired
            || self
                .timeouts
                .has_echo(self.view, &self.context.validator_data);
        if owes_timeout && !self.phase.has_timed_out() {
            return Some(Transition::Timeout);
        }

        self.pending_proposal().map(Transition::Proposal)
    }

    /// `TryDecide` and `TryFormCommitQC`: a commit certificate for this slot.
    /// Either it forms from the votes of this view, or it arrived ready-made
    /// from a peer.
    ///
    /// Nothing else is required: the certificate's verdict is the decision, so
    /// it never has to wait on a value arriving.
    fn pending_commit_qc(&self) -> Option<CommitQc> {
        match &self.decided_qc {
            Some(qc) => Some(qc.clone()),
            None => self
                .commit_votes
                .try_form_qc(self.view, &self.context.validator_data),
        }
    }

    /// `TryFormPrepQC`: a prepare certificate over the entries this validator
    /// voted to prepare in this view. Firing it again is impossible because
    /// the phase moves on to `Committing`.
    fn pending_prepare_qc(&self) -> Option<PrepareQc> {
        let entries = self.phase.preparing_entries()?;
        let qc = self
            .prepare_votes
            .try_form_qc(self.view, &self.context.validator_data)?;

        // a validator commits only the vector it prepared.
        (qc.verdict.0 == *entries).then_some(qc)
    }

    /// `SyncView`: the highest timeout certificate for this view or a later
    /// one, whether formed here from collected timeouts or carried as the
    /// justification of a proposal for a future view.
    ///
    /// Taking the highest is what lets a validator that fell behind rejoin in
    /// one step instead of one view at a time.
    fn pending_tc(&self) -> Option<TimeoutCertificate> {
        let organic = self
            .timeouts
            .try_form_tc(self.view, &self.context.validator_data);

        let carried = self
            .pre_prepares
            .range(self.view..)
            .filter_map(|(_, pre_prepare)| pre_prepare.message().justification.as_ref())
            .filter(|tc| tc.view >= self.view)
            .filter(|tc| tc.verify(&self.context.validator_data))
            .max_by_key(|tc| tc.view)
            .cloned();

        match (organic, carried) {
            (Some(a), Some(b)) => Some(if a.view >= b.view { a } else { b }),
            (found, None) | (None, found) => found,
        }
    }

    /// The pre-prepare handler's acceptance test. The sender being the leader
    /// and its signature verifying were checked at ingress; what is left
    /// depends on state that may have changed since.
    fn pending_proposal(&self) -> Option<Validated<PrePrepareMsg>> {
        if !matches!(self.phase, Phase::AwaitingProposal(_)) {
            // a view accepts one proposal, and a timed-out view accepts none:
            // last_voted_view has already been raised to it.
            return None;
        }

        // to rule out double voting.
        if self.view <= self.last_voted_view {
            return None;
        }

        let pre_prepare = self.pre_prepares.get(&self.view)?;
        let msg = pre_prepare.message();

        if !msg.metablock.is_valid(
            self.context.slot,
            self.context.num_proposals,
            &self.context.validator_data,
        ) {
            return None;
        }

        match (self.view == FallbackView::FIRST, &msg.justification) {
            // view 1 needs no justification: the metablock carries the
            // fallback certificate that admits the whole path.
            (true, None) => {}
            (false, Some(tc)) => {
                if tc.view.next() != self.view || !tc.verify(&self.context.validator_data) {
                    return None;
                }

                // the lock rule: a leader may not replace a value that the
                // previous view may already have supported.
                if let Some(lock) = tc.lock()
                    && msg.metablock.entries() != *lock
                {
                    return None;
                }
            }
            _ => return None,
        }

        Some(pre_prepare.clone())
    }

    /// The one exhaustive transition table. Takes the phase by value and
    /// returns its successor together with what to send.
    #[must_use]
    fn step(
        &mut self,
        phase: Phase,
        transition: Transition,
    ) -> (Phase, Vec<MVBAOutput<Message, TimerEvent>>) {
        match transition {
            Transition::Proposal(pre_prepare) => self.accept_proposal(phase, pre_prepare),
            Transition::PrepareQc(qc) => self.apply_prepare_qc(phase, qc),
            Transition::CommitQc(qc) => self.decide(phase, qc),
            Transition::Tc(tc) => self.advance_view(tc),
            Transition::Timeout => self.time_out(phase),
        }
    }

    /// `HandleProposal`: record the view's proposal and vote to prepare it.
    fn accept_proposal(
        &mut self,
        phase: Phase,
        pre_prepare: Validated<PrePrepareMsg>,
    ) -> (Phase, Vec<MVBAOutput<Message, TimerEvent>>) {
        let Phase::AwaitingProposal(awaiting) = phase else {
            unreachable!("a proposal transition is only found while awaiting one");
        };

        let (msg, _leader) = pre_prepare.destructure();
        let preparing = awaiting.accept(msg.metablock);

        // persist-before-send: lastVotedView.
        self.last_voted_view = self.view;

        let vote = self.sign_vote(PrepareVote(preparing.entries().clone()));
        (Phase::Preparing(preparing), vec![MVBAOutput::Broadcast(
            Message::Prepare(vote),
        )])
    }

    /// `TryFormPrepQC`: adopt the certificate as this validator's lock and
    /// vote to commit.
    fn apply_prepare_qc(
        &mut self,
        phase: Phase,
        qc: PrepareQc,
    ) -> (Phase, Vec<MVBAOutput<Message, TimerEvent>>) {
        let committing = match phase {
            Phase::Preparing(p) => Ok(p.commit(qc.clone())),
            Phase::TimedOut(timed_out) => match timed_out.into_inner() {
                InnerPhase::Preparing(p) => Err(p.commit(qc.clone())),
                inner => unreachable!(
                    "a prepare certificate transition is only found while preparing: {}",
                    inner.name()
                ),
            },
            other => unreachable!(
                "a prepare certificate transition is only found while preparing: {}",
                other.name()
            ),
        };

        let (block, entries) = match &committing {
            Ok(p) | Err(p) => (p.block().clone(), p.entries().clone()),
        };

        // persist-before-send: PrepQC.
        self.update_prep_qc(qc, Some(block.block));

        let vote = self.sign_vote(CommitVote(entries));
        let outputs = vec![MVBAOutput::Broadcast(Message::Commit(vote))];

        match committing {
            Ok(p) => (Phase::Committing(p), outputs),
            // a view that timed out stays timed out even as its certificates
            // complete.
            Err(p) => (TimedOut::wrap(InnerPhase::Committing(p)), outputs),
        }
    }

    /// `TryDecide`: output the decision, and pass the certificate on so a
    /// validator that missed the commit votes can decide too.
    fn decide(
        &mut self,
        phase: Phase,
        commit_qc: CommitQc,
    ) -> (Phase, Vec<MVBAOutput<Message, TimerEvent>>) {
        let entries = commit_qc.verdict.0.clone();

        let decided = match phase {
            Phase::Preparing(p) if *p.entries() == entries => p.decide(commit_qc.clone()),
            Phase::Committing(p) if *p.entries() == entries => p.decide(commit_qc.clone()),
            Phase::TimedOut(timed_out) => match timed_out.into_inner() {
                InnerPhase::Preparing(p) if *p.entries() == entries => p.decide(commit_qc.clone()),
                InnerPhase::Committing(p) if *p.entries() == entries => p.decide(commit_qc.clone()),
                _ => Decided::from_foreign_qc(commit_qc.clone()),
            },
            _ => Decided::from_foreign_qc(commit_qc.clone()),
        };

        // persist-before-send: DecidedQC.
        self.decided_qc = Some(commit_qc.clone());
        self.decision = Some(decided.entries().clone());

        (Phase::Decided(decided), vec![MVBAOutput::Broadcast(
            Message::CommitQc(commit_qc),
        )])
    }

    /// `SyncView`: adopt the certificate's lock if it is higher than the one
    /// held, then enter the view after it.
    fn advance_view(
        &mut self,
        tc: TimeoutCertificate,
    ) -> (Phase, Vec<MVBAOutput<Message, TimerEvent>>) {
        debug_assert!(tc.view >= self.view);

        if let Some(qc) = tc.high_prep_qc() {
            self.update_prep_qc(qc.clone(), tc.high_block().cloned());
        }

        // persist-before-send: v, PrepQC, and the certificate that justified
        // entering the view.
        let outputs = self.enter_view(tc.view.next(), Some(tc));
        (Phase::new(), outputs)
    }

    /// The view timeout expired, or f+1 stake has already timed out here.
    fn time_out(&mut self, phase: Phase) -> (Phase, Vec<MVBAOutput<Message, TimerEvent>>) {
        // persist-before-send: lastVotedView.
        self.last_voted_view = self.last_voted_view.max(self.view);

        let timeout = TimeoutMsg::new_signed(
            self.context.slot,
            self.view,
            self.prep_qc.clone(),
            &self.context.key,
        );

        let phase = phase
            .time_out()
            .expect("a timeout transition is only found for a view that has not timed out");

        (phase, vec![MVBAOutput::Broadcast(Message::Timeout(
            timeout,
        ))])
    }

    /// Enter `view`: restart the view timer and, if this validator leads the
    /// view, broadcast its proposal.
    #[must_use]
    fn enter_view(
        &mut self,
        view: FallbackView,
        justification: Option<TimeoutCertificate>,
    ) -> Vec<MVBAOutput<Message, TimerEvent>> {
        self.view = view;
        self.timer_fired = false;

        let mut outputs = vec![MVBAOutput::ScheduleTimer {
            duration: self.context.view_timeout(),
            timer_event: TimerEvent::ViewTimeout(view),
        }];

        if let Some(pre_prepare) = self.leader_proposal(justification) {
            outputs.push(MVBAOutput::Broadcast(Message::PrePrepare(pre_prepare)));
        }

        outputs
    }

    /// The proposal this validator makes when it leads the view it is
    /// entering, or `None` when it does not lead it or cannot honour the lock.
    ///
    /// A leader bound by a lock may only propose the locked entries. The paper
    /// lets it fetch such a metablock from a signer of the certificate; here
    /// the certified entries instead ride on the timeout certificate, so if
    /// none of the aggregated timeouts carried them there is nothing to propose
    /// and the view runs to its timeout.
    ///
    /// Only the entries travel, so the locked proposal is rebuilt around this
    /// validator's own fallback certificate. That is sound because every
    /// fallback certificate for the slot certifies the same statement
    /// `⟨fallback, slot⟩`, and available because an instance only exists once
    /// its input carried one.
    fn leader_proposal(&self, justification: Option<TimeoutCertificate>) -> Option<PrePrepareMsg> {
        if self.context.leader(self.view) != self.context.node_id {
            return None;
        }

        let own_input = || self.input.clone();

        let metablock = match justification.as_ref() {
            None => own_input()?,
            Some(tc) => match tc.lock() {
                // nothing locked: free to propose its own input, as in view 1.
                None => own_input()?,
                Some(lock) => {
                    let block = tc.high_block()?;
                    debug_assert_eq!(entries_of(block), *lock);

                    MVBAInputs {
                        enter_fallback_cert: self.input.as_ref()?.enter_fallback_cert.clone(),
                        block: block.clone(),
                    }
                }
            },
        };

        Some(PrePrepareMsg::new_signed(
            self.context.slot,
            self.view,
            metablock,
            justification,
            &self.context.key,
        ))
    }

    /// Drop state for views this instance has left: it can no longer produce a
    /// certificate this instance would act on.
    fn gc(&mut self) {
        self.prepare_votes.gc_below(self.view);
        self.commit_votes.gc_below(self.view);
        self.timeouts.gc_below(self.view);
        self.pre_prepares = self.pre_prepares.split_off(&self.view);
    }

    // ---------------- small helpers ----------------

    /// Keep the highest prepare certificate seen, together with the certified
    /// entries it locks when those are available.
    fn update_prep_qc(&mut self, qc: PrepareQc, block: Option<PartialBlock>) {
        let is_higher = self
            .prep_qc
            .as_ref()
            .is_none_or(|held| held.qc.scope.1 < qc.scope.1);

        if is_higher {
            self.prep_qc = Some(HighPrepare { qc, block });
        }
    }

    fn sign_vote<V>(&self, vote: V) -> VoteMsg<V>
    where
        V: IsVote<Scope = (Slot, FallbackView)>,
    {
        VoteMsg::new_signed((self.context.slot, self.view), vote, &self.context.key)
    }
}
