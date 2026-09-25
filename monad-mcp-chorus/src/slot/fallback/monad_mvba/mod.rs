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

//! Monad's MVBA instance used by the fallback path: a leader-based,
//! view-changing agreement on a metablock
//!
//! Validation is split by what it needs. Ingress -- the `handle_*` functions
//! -- checks everything a message claims on its own: slot, the leader of its
//! view, signatures, carried certificates, value validity. Its one stateful
//! check is the admission window ([`MonadMvba::in_window`]; waived upward for
//! a TC-carried catch-up, windowless for commitQCs), which only decides
//! whether a message is worth buffering: it bounds memory, it never judges
//! the protocol. Ingress buffers and decides nothing else;
//! [`MonadMvba::try_advance`] repeatedly asks
//! [`MonadMvba::find_pending_transition`] what is now possible and applies it
//! Every guard that decides whether to *act* lives in that one function, and
//! a guard that fails leaves the message buffered, so out-of-order arrival
//! is free
//!
//! An individual vote's signature is the one thing buffered unverified:
//! `VotePool` verifies in aggregate once enough stake has accumulated. A
//! harvested certificate -- a timeout's carried `high_prep_qc`, a
//! pre-prepare's TC -- is verified at ingress, before anything adopts it
//!
//! Agreement runs over `entries(x)`, so certificates arrive independently of
//! the blocks behind them. [`MonadMvba::request_missing_blocks`] is the one
//! place a fetch is asked for
//!
//! `v`, `lastVotedView`, `PrepQC` and `DecidedQC` must be durable before any
//! message they enable is sent. There is no backend yet; the points are marked
//! `persist-before-send`

mod block_store;
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
    collections::{BTreeMap, HashSet, VecDeque},
    sync::Arc,
};

use block_store::{BlockRequestMsg, BlockResponseMsg, BlockStore};
use certificates::{FallbackCommitQc, PrepareQc, TimeoutCertificate};
use collectors::ViewCollectors;
use messages::{
    CommitVoteMsg, FallbackCommitVote, Justification, Message, PrePrepareMsg, PrepareVote,
    PrepareVoteMsg, TimeoutMsg,
};
use phases::{Decided, Phase, Transition};

use super::{
    super::{
        fast::EnterFallbackCert,
        types::{IsVote, KeyPair, NodeId, Slot, TimestampDelta, ValidatorData, VoteMsg},
    },
    FallbackView, MVBAOutput, Metablock, Mvba, ValidateCert, ValidateInput, Votable,
};
use crate::spec::validator::ValidatorData as _;

/// Caps memory usage of this buffer
const MAX_FUTURE_VIEWS: u64 = 10;

/// Allows lagging CommitVotes to form CommitQC after we've entered new view via
/// TC
const LOOKBACK_VIEWS: u64 = 1;

/// View timeout in multiples of Δ: proposal, prepare round, commit round
const VIEW_TIMEOUT_DELTAS: u64 = 3;

/// How often a decided instance re-broadcasts its commit certificate
const DECIDED_ECHO_DELTAS: u64 = 3;

pub(crate) struct Context {
    pub slot: Slot,
    pub num_proposals: usize,
    pub delta: TimestampDelta,

    pub node_id: NodeId,
    pub key: Arc<KeyPair>,
    pub validator_data: Arc<ValidatorData>,
}

impl Context {
    /// Placeholder leader election
    ///
    /// `Leader(slot, view)`: placeholder round-robin, ignoring stake
    pub(crate) fn leader(&self, view: FallbackView) -> NodeId {
        let num_nodes = self.validator_data.nodes().count() as u64;
        assert!(num_nodes > 0);

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

    fn decided_echo_timeout(&self) -> TimestampDelta {
        self.delta
            .checked_mul(DECIDED_ECHO_DELTAS)
            .expect("echo timeout overflows the timestamp range")
    }
}

/// Timers driven by the MVBA state machine
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum TimerEvent<V: Votable> {
    /// View change timeout. Timeouts are periodically re-sent every view change
    /// timeout
    ViewTimeout(FallbackView),
    /// Retransmit the outstanding request for one block
    BlockRetransmit(V::Entries),
    /// TODO: decide if fast or fallback path should guarantee eventual commit
    ///
    /// Retransmit decided commitQC
    DecidedEcho,
}

/// Validation context for MetaBlock and FallbackCert
pub(crate) struct ValidationContext {
    pub slot: Slot,
    pub num_proposals: usize,
    pub validator_data: Arc<ValidatorData>,
}

/// Transforms protocol context to Input/Cert validation context
pub(crate) trait MakesValidationContext<V: ValidateInput> {
    fn make_validation_context(&self) -> V::Context;
}

impl MakesValidationContext<Metablock> for Context {
    fn make_validation_context(&self) -> ValidationContext {
        ValidationContext {
            slot: self.slot,
            num_proposals: self.num_proposals,
            validator_data: self.validator_data.clone(),
        }
    }
}

impl ValidateCert for EnterFallbackCert {
    type Context = ValidationContext;

    fn validate(&self, context: &Self::Context) -> bool {
        self.scope == context.slot && self.verify(&context.validator_data)
    }
}

/// This validator's own contribution, set once by [`Mvba::propose`]. Its
/// presence gates participation: nothing is sent before it is set
struct ProposedInput<V, C> {
    value: V,
    /// Justifies this validator's own view-1 proposal
    fbcert: Option<C>,
}

pub(crate) struct MonadMvba<V: ValidateInput + Votable, C: ValidateCert> {
    context: Context,
    validation_context: V::Context,
    input: Option<ProposedInput<V, C>>,

    phase: Phase<V>,

    /// Safety
    ///
    /// `lastVotedView_i`: this validator votes only in a view above it
    last_voted_view: FallbackView,
    /// `PrepQC_i`: the highest prepare certificate seen; the entries this
    /// validator is locked on
    high_prep_qc: Option<PrepareQc<V>>,
    /// `DecidedQC_i`: recorded before the block it settles is necessarily held
    decided_qc: Option<FallbackCommitQc<V>>,

    /// Message buffer and storage
    collectors: BTreeMap<FallbackView, ViewCollectors<V, C>>,
    block_store: BlockStore<V>,

    /// `J` for the current view, which also fixes it -- see [`MonadMvba::view`]
    entry_tc: Option<TimeoutCertificate<V>>,
    /// Whether the timer for the *current* view has fired; cleared on entering
    /// a view and timer fire
    timer_fired: bool,

    outputs: VecDeque<MVBAOutput<Message<V, C>, TimerEvent<V>>>,
    abandoned: bool,
}

impl<V, C> Mvba<V> for MonadMvba<V, C>
where
    V: ValidateInput + Votable,
    C: ValidateCert<Context = V::Context>,
    Context: MakesValidationContext<V>,
{
    type Message = Message<V, C>;
    type Context = Context;
    type TimerEvent = TimerEvent<V>;
    type FallbackCert = C;
    type CommitVote = FallbackCommitVote<V>;

    fn new(context: Context) -> Self {
        let slot = context.slot;
        let validation_context = context.make_validation_context();
        let delta = context.delta;

        Self {
            context,
            validation_context,
            phase: Phase::new(),
            input: None,
            last_voted_view: FallbackView::GENESIS,
            high_prep_qc: None,
            decided_qc: None,
            collectors: BTreeMap::new(),
            block_store: BlockStore::new(slot, delta),
            entry_tc: None,
            timer_fired: false,
            outputs: VecDeque::new(),
            abandoned: false,
        }
    }

    // TODO: should we participate in fallback without an input?
    fn propose(&mut self, data: V, cert: Option<Self::FallbackCert>) {
        // prevents equivocation
        if self.abandoned || self.input.is_some() {
            return;
        }

        self.block_store.remember(data.clone());
        self.input = Some(ProposedInput {
            value: data,
            fbcert: cert,
        });

        let outputs = self.enter_view(None);
        self.outputs.extend(outputs);

        self.try_advance();
    }

    fn handle_message(&mut self, sender: NodeId, message: Self::Message) {
        if self.abandoned {
            return;
        }
        assert!(self.context.validator_data.contains(&sender));

        match message {
            Message::PrePrepare(msg) => self.handle_pre_prepare(sender, msg),
            Message::Prepare(msg) => self.handle_prepare_vote(sender, msg),
            Message::Commit(msg) => self.handle_commit_vote(sender, msg),
            Message::Timeout(msg) => self.handle_timeout(sender, msg),
            Message::CommitQc(qc) => self.handle_commit_qc(qc),
            Message::BlockRequest(msg) => self.answer_block_request(sender, &msg),
            Message::BlockResponse(msg) => self.handle_block_response(msg),
        }

        self.try_advance();
    }

    fn handle_timer(&mut self, timer_event: Self::TimerEvent) {
        if self.abandoned {
            return;
        }

        match timer_event {
            TimerEvent::ViewTimeout(view) => {
                if view != self.view() {
                    // a timer from a view this instance has already left
                    return;
                }

                self.timer_fired = true;
                self.try_advance();
            }
            // no protocol state changed, so no try_advance
            TimerEvent::BlockRetransmit(entries) => {
                if !self.phase.is_decided() {
                    self.outputs
                        .extend(self.block_store.on_retransmit_timer(&entries));
                }
            }
            TimerEvent::DecidedEcho => self.echo_decision(),
        }
    }

    fn abandon(&mut self) {
        self.abandoned = true;
        self.outputs.clear();
    }

    fn decision(&self) -> Option<&V> {
        self.decided().map(Decided::block)
    }

    fn decision_proof(&self) -> Option<&FallbackCommitQc<V>> {
        // an instance with no input, an abandoned one, or still fetching
        // decided block may hold a certificate it never acted on
        self.decided().map(Decided::commit_qc)
    }

    fn poll(&mut self) -> Option<MVBAOutput<Self::Message, Self::TimerEvent>> {
        self.outputs.pop_front()
    }
}

// ---------------- message ingress: validate, admit, store ----------------

impl<V, C> MonadMvba<V, C>
where
    V: ValidateInput + Votable,
    C: ValidateCert<Context = V::Context>,
    Context: MakesValidationContext<V>,
{
    fn handle_pre_prepare(&mut self, sender: NodeId, msg: PrePrepareMsg<V, C>) {
        // catching up is allowed past the window: only the TC arm below can
        // reach a store from there, and only behind a verified certificate
        let view = msg.view;
        if msg.slot != self.context.slot || !self.in_window_or_catching_up(view) {
            return;
        }

        if sender != self.context.leader(view) {
            return;
        }
        if !msg.verify_signature(self.context.validator_data.get_pubkey(&sender)) {
            return;
        }

        match (&msg.justification, view == FallbackView::FIRST) {
            (Justification::FallbackCert(Some(cert)), true) => {
                if !cert.validate(self.validation_context()) {
                    return;
                }
            }
            (Justification::FallbackCert(None), true) => {
                if !msg.value.fbcert_optional() {
                    return;
                }
            }
            (Justification::Tc(tc), false) => {
                // verified ahead of the cheap refusals below so that it can
                // be harvested: a valid certificate is evidence its view was
                // left, whatever becomes of the message carrying it
                if tc.slot != self.context.slot || !tc.verify(&self.context.validator_data) {
                    return;
                }
                if self.in_window_or_catching_up(tc.view) {
                    self.collectors_mut(tc.view).store_tc(tc.clone());
                }

                if tc.view.next() != view {
                    return;
                }

                // the lock rule
                if let Some(lock) = tc.lock()
                    && msg.value.entries() != *lock
                {
                    return;
                }
            }
            _ => return,
        }

        if !msg.value.validate(self.validation_context()) {
            return;
        }

        self.collectors_mut(view).store_pre_prepare(msg);
    }

    fn handle_prepare_vote(&mut self, sender: NodeId, msg: PrepareVoteMsg<V>) {
        let (slot, view) = msg.scope;
        if slot != self.context.slot || !self.in_window(view) {
            return;
        }

        self.collectors_mut(view).store_prepare_vote(sender, msg);
    }

    fn handle_commit_vote(&mut self, sender: NodeId, msg: CommitVoteMsg<V>) {
        let (slot, view) = msg.scope;
        if slot != self.context.slot || !self.in_window(view) {
            return;
        }

        self.collectors_mut(view).store_commit_vote(sender, msg);
    }

    fn handle_timeout(&mut self, sender: NodeId, msg: TimeoutMsg<V>) {
        let view = msg.view();
        if msg.slot() != self.context.slot || !self.in_window(view) {
            return;
        }

        if !msg.is_valid(&self.context.validator_data) {
            return;
        }

        // harvested now: this validator would otherwise announce a staler lock
        // in its own timeout. A lookback-view timeout harvests too -- adoption
        // is a monotone max over verified QCs, so a stale carrier can only
        // raise the lock
        if let Some(qc) = &msg.high_prep_qc {
            self.update_prep_qc(qc.clone());
        }

        self.collectors_mut(view).store_timeout(sender, msg);
    }

    fn handle_commit_qc(&mut self, qc: FallbackCommitQc<V>) {
        if qc.scope.0 != self.context.slot || !qc.verify(&self.context.validator_data) {
            return;
        }

        if self.decided_qc.is_none() {
            // persist-before-send: DecidedQC
            self.decided_qc = Some(qc);
        }
    }

    fn answer_block_request(&mut self, sender: NodeId, request: &BlockRequestMsg<V>) {
        if let Some(response) = self.block_store.handle_request(request) {
            self.outputs.push_back(MVBAOutput::Unicast {
                to: sender,
                message: Message::BlockResponse(response),
            });
        }
    }

    fn handle_block_response(&mut self, response: BlockResponseMsg<V>) {
        self.block_store
            .handle_response(response, &self.validation_context);
    }

    /// Oldest view whose messages are still admitted and buffered
    fn view_floor(&self) -> FallbackView {
        self.view()
            .saturating_sub(LOOKBACK_VIEWS)
            .max(FallbackView::FIRST)
    }

    /// The range of views we're currently buffering
    fn in_window(&self, view: FallbackView) -> bool {
        let current = self.view();
        view >= self.view_floor() && view.get() <= current.get() + MAX_FUTURE_VIEWS
    }

    /// Accepts messages within the window and indefinitely into the future to
    /// allow catching up via TC
    fn in_window_or_catching_up(&self, view: FallbackView) -> bool {
        self.in_window(view) || (self.is_running() && view >= self.view())
    }

    /// Opened on first use. Callers pass a view inside the window, or a
    /// catching-up view reachable only behind a verified TC or leader-signed
    /// pre-prepare, which immediately triggers garbage collection
    fn collectors_mut(&mut self, view: FallbackView) -> &mut ViewCollectors<V, C> {
        let slot = self.context.slot;
        self.collectors
            .entry(view)
            .or_insert_with(|| ViewCollectors::new(slot, view))
    }
}

// ---------------- the state machine ----------------

impl<V, C> MonadMvba<V, C>
where
    V: ValidateInput + Votable,
    C: ValidateCert<Context = V::Context>,
    Context: MakesValidationContext<V>,
{
    fn is_running(&self) -> bool {
        self.input.is_some() && !self.abandoned && !self.phase.is_decided()
    }

    fn validation_context(&self) -> &V::Context {
        &self.validation_context
    }

    /// Each iteration takes the phase by value and gets a new one back, so no
    /// caller can observe a half-applied transition
    fn try_advance(&mut self) {
        if !self.is_running() {
            return;
        }

        while let Some(transition) = self.find_pending_transition() {
            let phase = std::mem::replace(&mut self.phase, Phase::Poisoned);
            let (phase, outputs) = self.step(phase, transition);

            self.phase = phase;
            self.outputs.extend(outputs);

            if self.phase.is_decided() {
                break;
            }
        }

        // The certificate must be recorded even when the block is missing: a
        // view change gc's the votes it was aggregated from, and `decide`
        // cannot run until the block arrives

        // TODO: we could consider introducing a new CommitQCWait phase to
        // represent a commitQC is formed but block hasn't been fetched.
        // Deferred
        self.record_commit_qc();
        self.request_missing_blocks();
        self.gc();
    }

    /// The single place every protocol guard lives; it constructs the evidence
    /// for what it found and changes nothing
    ///
    /// Priority matters: a decision ends the instance; a prepare certificate
    /// comes before a view change, so the highest lock is carried forward; a
    /// view change before this validator's own timeout; proposals last, as the
    /// only transitions requiring an untouched view
    fn find_pending_transition(&self) -> Option<Transition<V, C>> {
        if self.phase.is_decided() {
            return None;
        }

        if let Some((qc, block)) = self.pending_decide() {
            return Some(Transition::Decide { qc, block });
        }

        if let Some(qc) = self.pending_prepare_qc() {
            return Some(Transition::PrepareQc(qc));
        }

        if let Some(tc) = self.pending_tc() {
            return Some(Transition::Tc(tc));
        }

        // the f+1 echo obliges a timeout without the timer firing, but only
        // once: the echo never clears. `local_time_out` consumes a fire
        let timeout = self.timer_fired
            || (!self.phase.has_timed_out()
                && self.current_view_collectors().is_some_and(|collectors| {
                    collectors.has_honest_timeout(&self.context.validator_data)
                }));
        if timeout {
            return Some(Transition::Timeout);
        }

        // the proposer check, run once per view and only here. A leader stays
        // until it has a proposal to make, which a lock whose block is still in
        // flight can delay
        if matches!(self.phase, Phase::NewView(_)) {
            if self.context.leader(self.view()) != self.context.node_id {
                return Some(Transition::AwaitProposal);
            }

            return self.own_proposal_ready().map(Transition::OwnProposalReady);
        }

        self.pending_proposal().map(Transition::Proposal)
    }

    // TODO: (future work) future views may form CommitQC and PrepareQC. Decide
    // how to handle them

    /// `TryFormCommitQC`. Whether the decision can be *reported* is
    /// [`MonadMvba::pending_decide`]
    fn known_commit_qc(&self) -> Option<FallbackCommitQc<V>> {
        if let Some(qc) = &self.decided_qc {
            return Some(qc.clone());
        }

        self.collectors
            .range(..=self.view())
            .rev()
            .find_map(|(_, collectors)| collectors.try_form_commit_qc(&self.context.validator_data))
    }

    /// `TryDecide`: a commit certificate together with the block it settled
    /// Waiting for the block is safe -- the certificate is already recorded
    fn pending_decide(&self) -> Option<(FallbackCommitQc<V>, V)> {
        let qc = self.known_commit_qc()?;
        let block = self.block_store.get(&qc.verdict.0)?.clone();

        Some((qc, block))
    }

    /// `TryFormPrepQC`
    fn pending_prepare_qc(&self) -> Option<PrepareQc<V>> {
        let entries = self.phase.preparing_entries()?;
        let qc = self
            .current_view_collectors()?
            .try_form_prepare_qc(&self.context.validator_data)?;

        // a validator commits only the vector it prepared
        (qc.verdict.0 == *entries).then_some(qc)
    }

    /// `SyncView`: the highest timeout certificate for this view or later,
    /// which lets a validator that fell behind rejoin in one step
    fn pending_tc(&self) -> Option<TimeoutCertificate<V>> {
        let organic = self
            .current_view_collectors()
            .and_then(|collectors| collectors.try_form_tc(&self.context.validator_data));

        let harvested = self
            .collectors
            .range(self.view()..)
            .filter_map(|(_, collectors)| collectors.harvested_tc())
            .max_by_key(|tc| tc.view)
            .cloned();

        match (organic, harvested) {
            (Some(a), Some(b)) => Some(if a.view >= b.view { a } else { b }),
            (found, None) | (None, found) => found,
        }
    }

    /// `HandleProposal`'s guard. Ingress admits only valid proposals, so this
    /// only asks whether this validator may act on one now
    fn pending_proposal(&self) -> Option<PrePrepareMsg<V, C>> {
        if !matches!(self.phase, Phase::AwaitingProposal(_)) {
            // a timed-out view accepts none: last_voted_view was raised to it
            return None;
        }

        // double voting guard
        if self.view() <= self.last_voted_view {
            return None;
        }

        self.current_view_collectors()?.pre_prepare().cloned()
    }

    /// The one exhaustive transition table
    #[must_use]
    fn step(
        &mut self,
        phase: Phase<V>,
        transition: Transition<V, C>,
    ) -> (Phase<V>, Vec<MVBAOutput<Message<V, C>, TimerEvent<V>>>) {
        match transition {
            Transition::Proposal(pre_prepare) => self.accept_proposal(phase, pre_prepare),
            Transition::OwnProposalReady(pre_prepare) => self.propose_as_leader(phase, pre_prepare),
            Transition::AwaitProposal => (Self::leave_new_view(phase), Vec::new()),
            Transition::PrepareQc(qc) => self.apply_prepare_qc(phase, qc),
            Transition::Decide { qc, block } => self.decide(phase, qc, block),
            Transition::Tc(tc) => self.advance_view(tc),
            Transition::Timeout => self.local_time_out(phase),
        }
    }

    /// `HandleProposal`: record the view's proposal and vote to prepare it
    fn accept_proposal(
        &mut self,
        phase: Phase<V>,
        msg: PrePrepareMsg<V, C>,
    ) -> (Phase<V>, Vec<MVBAOutput<Message<V, C>, TimerEvent<V>>>) {
        let Phase::AwaitingProposal(awaiting) = phase else {
            unreachable!("a proposal transition is only found while awaiting one");
        };

        let entries = msg.value.entries();
        self.block_store.remember(msg.value);
        let preparing = awaiting.accept::<V>(entries);

        // persist-before-send: lastVotedView
        self.last_voted_view = self.view();

        let vote = self.sign_vote(PrepareVote::<V>(preparing.entries().clone()));
        (
            Phase::Preparing(preparing),
            vec![MVBAOutput::Broadcast(Message::Prepare(vote))],
        )
    }

    /// `TryFormPrepQC`: adopt the certificate as this validator's lock and
    /// vote to commit
    fn apply_prepare_qc(
        &mut self,
        phase: Phase<V>,
        qc: PrepareQc<V>,
    ) -> (Phase<V>, Vec<MVBAOutput<Message<V, C>, TimerEvent<V>>>) {
        let committing = match phase {
            Phase::Preparing(p) => p.commit(qc.clone()),
            // `preparing_entries` stops looking once the timeout is sent
            other => unreachable!(
                "a prepare certificate transition is only found while preparing: {}",
                other.name()
            ),
        };

        // persist-before-send: PrepQC
        self.update_prep_qc(qc);

        let vote = self.sign_vote(FallbackCommitVote::<V>(committing.entries().clone()));

        (
            Phase::Committing(committing),
            vec![MVBAOutput::Broadcast(Message::Commit(vote))],
        )
    }

    /// `TryDecide`: output the decision, and pass the certificate on
    fn decide(
        &mut self,
        phase: Phase<V>,
        commit_qc: FallbackCommitQc<V>,
        block: V,
    ) -> (Phase<V>, Vec<MVBAOutput<Message<V, C>, TimerEvent<V>>>) {
        // `pending_decide` fetched the block by the certificate's entries
        debug_assert_eq!(block.entries(), commit_qc.verdict.0);
        // a decided instance finds no further transitions
        debug_assert!(!phase.is_decided());

        // persist-before-send: DecidedQC
        self.decided_qc = Some(commit_qc.clone());

        (
            Phase::Decided(Decided::new(commit_qc.clone(), block)),
            vec![
                MVBAOutput::Broadcast(Message::CommitQc(commit_qc)),
                MVBAOutput::ScheduleTimer {
                    duration: self.context.decided_echo_timeout(),
                    timer_event: TimerEvent::DecidedEcho,
                },
            ],
        )
    }

    /// Runs outside `try_advance` because a decision ends the state machine,
    /// not the obligation to be fetchable from
    fn echo_decision(&mut self) {
        let Some(decided) = self.phase.decided() else {
            panic!("echo decision timer scheduled while not in decided state");
        };

        self.outputs
            .push_back(MVBAOutput::Broadcast(Message::CommitQc(
                decided.commit_qc().clone(),
            )));
        self.outputs.push_back(MVBAOutput::ScheduleTimer {
            duration: self.context.decided_echo_timeout(),
            timer_event: TimerEvent::DecidedEcho,
        });
    }

    /// `SyncView`
    fn advance_view(
        &mut self,
        tc: TimeoutCertificate<V>,
    ) -> (Phase<V>, Vec<MVBAOutput<Message<V, C>, TimerEvent<V>>>) {
        debug_assert!(tc.view >= self.view());

        if let Some(qc) = &tc.high_prep_qc {
            self.update_prep_qc(qc.clone());
        }

        // persist-before-send: PrepQC and the certificate that fixes v
        let outputs = self.enter_view(Some(tc));
        (Phase::new(), outputs)
    }

    /// The view timeout expired, or f+1 stake has already timed out here. Also
    /// the retransmission path for this validator's timeout: re-arming keeps
    /// the fires coming. Block requests re-drive on their own
    /// [`TimerEvent::BlockRetransmit`]
    fn local_time_out(
        &mut self,
        phase: Phase<V>,
    ) -> (Phase<V>, Vec<MVBAOutput<Message<V, C>, TimerEvent<V>>>) {
        self.timer_fired = false;
        let view = self.view();

        // persist-before-send: lastVotedView
        self.last_voted_view = self.last_voted_view.max(view);

        // the lock can only have risen since the last retransmission
        let timeout = TimeoutMsg::new_signed(
            self.context.slot,
            view,
            self.high_prep_qc.clone(),
            &self.context.key,
        );

        let phase = phase
            .time_out()
            .expect("a timeout transition is never found once decided");

        // re-armed unconditionally: scheduling replaces any pending timer for
        // the same event, so an echo-triggered timeout still leaves exactly
        // one live timer -- and its periodic fires are what retransmit this
        // validator's timeout
        let outputs = vec![
            MVBAOutput::ScheduleTimer {
                duration: self.context.view_timeout(),
                timer_event: TimerEvent::ViewTimeout(view),
            },
            MVBAOutput::Broadcast(Message::Timeout(timeout)),
        ];

        (phase, outputs)
    }

    /// Enter the view `justification` derives (see [`MonadMvba::view`])
    /// Leading it sends nothing from here: a leader may have to wait for the
    /// locked block, so proposing is a transition
    #[must_use]
    fn enter_view(
        &mut self,
        justification: Option<TimeoutCertificate<V>>,
    ) -> Vec<MVBAOutput<Message<V, C>, TimerEvent<V>>> {
        self.timer_fired = false;
        self.entry_tc = justification;

        vec![MVBAOutput::ScheduleTimer {
            duration: self.context.view_timeout(),
            timer_event: TimerEvent::ViewTimeout(self.view()),
        }]
    }

    /// The proposal this validator owes the view it leads, `None` while bound
    /// by a lock whose block it does not hold yet
    fn own_proposal_ready(&self) -> Option<PrePrepareMsg<V, C>> {
        let view: FallbackView = self.view();
        debug_assert!(matches!(self.phase, Phase::NewView(_)));
        debug_assert_eq!(self.context.leader(view), self.context.node_id);

        // no input means this instance does not participate at all
        let input = self.input.as_ref()?;

        let block = match self.locked_entries() {
            // nothing locked: free to propose its own input, as in view 1
            None => input.value.clone(),
            Some(lock) => self.block_store.get(lock)?.clone(),
        };

        let justification = match &self.entry_tc {
            None => Justification::FallbackCert(input.fbcert.clone()),
            Some(tc) => Justification::Tc(tc.clone()),
        };

        Some(PrePrepareMsg::new_signed(
            self.context.slot,
            view,
            block,
            justification,
            &self.context.key,
        ))
    }

    /// Leaving the new view is what makes this fire once
    fn propose_as_leader(
        &mut self,
        phase: Phase<V>,
        pre_prepare: PrePrepareMsg<V, C>,
    ) -> (Phase<V>, Vec<MVBAOutput<Message<V, C>, TimerEvent<V>>>) {
        (
            Self::leave_new_view(phase),
            vec![MVBAOutput::Broadcast(Message::PrePrepare(pre_prepare))],
        )
    }

    fn leave_new_view(phase: Phase<V>) -> Phase<V> {
        let Phase::NewView(new_view) = phase else {
            unreachable!("the proposer check is only found in a new view");
        };

        Phase::AwaitingProposal(new_view.await_proposal())
    }

    /// `lock(J)` for the current view, `None` when its leader is free
    fn locked_entries(&self) -> Option<&V::Entries> {
        self.entry_tc.as_ref()?.lock()
    }

    /// Deciding waits for the block, the certificate must not: the votes it was
    /// aggregated from are dropped as views advance
    fn record_commit_qc(&mut self) {
        if self.decided_qc.is_none()
            && let Some(qc) = self.known_commit_qc()
        {
            // persist-before-send: DecidedQC
            self.decided_qc = Some(qc);
        }
    }

    /// One place for every fetch the protocol can call for. A lock is fetched
    /// as soon as it is adopted, not when a view forces the question: a 3Δ view
    /// is too short to start a round trip in
    fn request_missing_blocks(&mut self) {
        if self.phase.is_decided() {
            return;
        }

        let mut wanted = Vec::new();
        if let Some(qc) = self.known_commit_qc() {
            wanted.push(qc.verdict.0);
        }
        // high_prep_qc and TC.high_prep_qc can be different. Both cases below
        // are needed
        if let Some(qc) = &self.high_prep_qc {
            wanted.push(qc.verdict.0.clone());
        }
        // Leader stuck on NewView state means it's missing the locked block
        if matches!(self.phase, Phase::NewView(_))
            && self.context.leader(self.view()) == self.context.node_id
            && let Some(lock) = self.locked_entries()
        {
            wanted.push(lock.clone());
        }

        for entries in wanted {
            // `want` dedups, so this can run on every pass
            self.outputs.extend(self.block_store.want(&entries));
        }
    }

    fn gc(&mut self) {
        self.collectors = self.collectors.split_off(&self.view_floor());

        // Blocks are not per-view, so they go by reachability
        let mut keep = HashSet::new();
        if let Some(input) = &self.input {
            keep.insert(input.value.entries());
        }
        if let Some(entries) = self.phase.entries() {
            keep.insert(entries.clone());
        }
        if let Some(qc) = &self.high_prep_qc {
            keep.insert(qc.verdict.0.clone());
        }
        if let Some(qc) = &self.decided_qc {
            keep.insert(qc.verdict.0.clone());
        }
        if let Some(lock) = self.locked_entries() {
            keep.insert(lock.clone());
        }
        self.block_store.gc(&keep);
    }

    // ---------------- small helpers ----------------

    /// The paper's `v`, derived from how the view was entered
    fn view(&self) -> FallbackView {
        self.entry_tc
            .as_ref()
            .map_or(FallbackView::FIRST, |tc| tc.view.next())
    }

    /// `None` when nothing has arrived for the current view yet
    fn current_view_collectors(&self) -> Option<&ViewCollectors<V, C>> {
        self.collectors.get(&self.view())
    }

    /// Understating the lock would be a safety bug
    fn update_prep_qc(&mut self, qc: PrepareQc<V>) {
        let is_higher = self
            .high_prep_qc
            .as_ref()
            .is_none_or(|held| held.scope.1 < qc.scope.1);

        if is_higher {
            self.high_prep_qc = Some(qc);
        }
    }

    fn decided(&self) -> Option<&Decided<V>> {
        self.phase.decided()
    }

    fn sign_vote<T>(&self, vote: T) -> VoteMsg<T>
    where
        T: IsVote<Scope = (Slot, FallbackView)>,
    {
        VoteMsg::new_signed((self.context.slot, self.view()), vote, &self.context.key)
    }
}
