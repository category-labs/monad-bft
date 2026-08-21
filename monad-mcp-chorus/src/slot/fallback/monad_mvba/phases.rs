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

//! What a validator's view can be in, and how it may move between those
//! states.
//!
//! Each phase carries the evidence that justifies it, and the only way to
//! reach a phase is a consuming method that demands that evidence: there is no
//! `Committing` without a prepare certificate and no `Decided` without a
//! commit certificate. Together with [`Transition`], whose variants carry the
//! certificate that fired them, this keeps "checked" and "applied" from
//! drifting apart -- nothing is re-looked-up between the two.

use super::{
    super::{
        super::{fast::Entry, types::ProposalMap},
        Metablock,
    },
    certificates::{FallbackCommitQc, PrepareQc, TimeoutCertificate},
    messages::PrePrepareMsg,
};

/// The state of the current view.
#[derive(Clone)]
pub(crate) enum Phase {
    AwaitingProposal(AwaitingProposal),
    Preparing(Preparing),
    Committing(Committing),
    /// This validator has sent its timeout for the view. It no longer votes
    /// there, but certificates whose votes it already cast may still complete.
    TimedOut(TimedOut),
    /// Terminal.
    Decided(Decided),
}

/// No proposal accepted in this view yet.
#[derive(Clone)]
pub(crate) struct AwaitingProposal {
    // deliberately empty: what a validator knows before a proposal arrives is
    // instance state, not view state.
    _private: (),
}

/// The view's proposal is accepted and a prepare vote for it has been sent.
///
/// Only the entries are kept: the block the proposal carried went into the
/// block store, which is where every consumer of it now looks.
#[derive(Clone)]
pub(crate) struct Preparing {
    entries: ProposalMap<Entry>,
}

/// A prepare certificate formed for the accepted proposal and a commit vote
/// has been sent.
#[derive(Clone)]
pub(crate) struct Committing {
    entries: ProposalMap<Entry>,
    prepare_qc: PrepareQc,
}

/// Decided, with the certificate that proves it and the block it settled.
///
/// The block is here because a decision is only reached once it is held: this
/// phase is the evidence that both halves of the decision are in hand.
#[derive(Clone)]
pub(crate) struct Decided {
    // FIXME: this field seems redundant with commit_qc
    //
    // Response: it was, and it is gone. The certificate's verdict *is* the
    // entries -- every path into this phase asserted as much -- so `entries()`
    // now reads through it and there is one copy where there were two.
    commit_qc: FallbackCommitQc,
    block: Metablock,
}

/// A timed-out view, remembering what it was doing when the timeout was sent.
///
/// The inner phase is kept rather than collapsed because the votes this
/// validator already cast are still out there: a prepare or commit certificate
/// can still form for this view, and it must still be able to act on it.
#[derive(Clone)]
pub(crate) struct TimedOut {
    inner: InnerPhase,
}

/// The phases a view can be in when it times out. `Decided` is terminal and
/// never times out.
#[derive(Clone)]
pub(crate) enum InnerPhase {
    AwaitingProposal(AwaitingProposal),
    Preparing(Preparing),
    Committing(Committing),
}

impl Phase {
    /// A fresh view: nothing accepted, nothing certified.
    pub(crate) fn new() -> Self {
        Phase::AwaitingProposal(AwaitingProposal { _private: () })
    }

    pub(crate) fn is_decided(&self) -> bool {
        matches!(self, Phase::Decided(_))
    }

    /// The decision, once this view reached one.
    pub(crate) fn decided(&self) -> Option<&Decided> {
        match self {
            Phase::Decided(p) => Some(p),
            _ => None,
        }
    }

    pub(crate) fn has_timed_out(&self) -> bool {
        matches!(self, Phase::TimedOut(_))
    }

    /// `entries(x_v)` of the accepted proposal, if one was accepted.
    pub(crate) fn entries(&self) -> Option<&ProposalMap<Entry>> {
        match self {
            Phase::AwaitingProposal(_) => None,
            Phase::Preparing(p) => Some(&p.entries),
            Phase::Committing(p) => Some(&p.entries),
            Phase::TimedOut(p) => p.inner.entries(),
            Phase::Decided(p) => Some(p.entries()),
        }
    }

    /// The entries this validator voted to prepare but holds no prepare
    /// certificate for yet.
    pub(crate) fn preparing_entries(&self) -> Option<&ProposalMap<Entry>> {
        match self {
            Phase::Preparing(p) => Some(&p.entries),
            Phase::TimedOut(TimedOut {
                inner: InnerPhase::Preparing(p),
            }) => Some(&p.entries),
            _ => None,
        }
    }

    /// The prepare certificate formed in this view, if one was.
    pub(crate) fn prepare_qc(&self) -> Option<&PrepareQc> {
        match self {
            Phase::Committing(p) => Some(&p.prepare_qc),
            Phase::TimedOut(TimedOut {
                inner: InnerPhase::Committing(p),
            }) => Some(&p.prepare_qc),
            _ => None,
        }
    }

    /// Name of the phase, for assertion messages.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Phase::AwaitingProposal(_) => "awaiting proposal",
            Phase::Preparing(_) => "preparing",
            Phase::Committing(_) => "committing",
            Phase::TimedOut(p) => p.inner.name(),
            Phase::Decided(_) => "decided",
        }
    }

    /// Move into the timed-out wrapper, keeping the inner phase. `None` for a
    /// phase that cannot time out: already timed out, or decided.
    pub(crate) fn time_out(self) -> Option<Phase> {
        let inner = match self {
            Phase::AwaitingProposal(p) => InnerPhase::AwaitingProposal(p),
            Phase::Preparing(p) => InnerPhase::Preparing(p),
            Phase::Committing(p) => InnerPhase::Committing(p),
            Phase::TimedOut(_) | Phase::Decided(_) => return None,
        };

        Some(Phase::TimedOut(TimedOut { inner }))
    }
}

impl InnerPhase {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            InnerPhase::AwaitingProposal(_) => "timed out, awaiting proposal",
            InnerPhase::Preparing(_) => "timed out, preparing",
            InnerPhase::Committing(_) => "timed out, committing",
        }
    }

    fn entries(&self) -> Option<&ProposalMap<Entry>> {
        match self {
            InnerPhase::AwaitingProposal(_) => None,
            InnerPhase::Preparing(p) => Some(&p.entries),
            InnerPhase::Committing(p) => Some(&p.entries),
        }
    }
}

impl TimedOut {
    pub(crate) fn into_inner(self) -> InnerPhase {
        self.inner
    }

    /// Re-wrap an inner phase that advanced while timed out. The view stays
    /// timed out: a certificate completing does not un-send the timeout.
    pub(crate) fn wrap(inner: InnerPhase) -> Phase {
        Phase::TimedOut(TimedOut { inner })
    }
}

impl AwaitingProposal {
    /// Accept the view's proposal. The caller must have run every check in the
    /// paper's pre-prepare handler first; [`Transition::Proposal`] is the
    /// witness that it did, and it is also responsible for handing the block
    /// the proposal carried to the block store.
    pub(crate) fn accept(self, entries: ProposalMap<Entry>) -> Preparing {
        Preparing { entries }
    }
}

impl Preparing {
    pub(crate) fn entries(&self) -> &ProposalMap<Entry> {
        &self.entries
    }

    /// A prepare certificate formed for the accepted entries.
    pub(crate) fn commit(self, prepare_qc: PrepareQc) -> Committing {
        debug_assert_eq!(prepare_qc.verdict.0, self.entries);

        Committing {
            entries: self.entries,
            prepare_qc,
        }
    }

    /// A commit certificate can arrive before this validator has seen the
    /// prepare certificate for the same entries; the decision does not wait
    /// for it.
    pub(crate) fn decide(self, commit_qc: FallbackCommitQc, block: Metablock) -> Decided {
        debug_assert_eq!(commit_qc.verdict.0, self.entries);

        Decided { commit_qc, block }
    }
}

impl Committing {
    pub(crate) fn entries(&self) -> &ProposalMap<Entry> {
        &self.entries
    }

    pub(crate) fn decide(self, commit_qc: FallbackCommitQc, block: Metablock) -> Decided {
        debug_assert_eq!(commit_qc.verdict.0, self.entries);

        Decided { commit_qc, block }
    }
}

impl Decided {
    /// `entries(x)` of the decided value: the certificate's verdict, which is
    /// what the commit votes ranged over.
    pub(crate) fn entries(&self) -> &ProposalMap<Entry> {
        &self.commit_qc.verdict.0
    }

    pub(crate) fn commit_qc(&self) -> &FallbackCommitQc {
        &self.commit_qc
    }

    pub(crate) fn block(&self) -> &Metablock {
        &self.block
    }

    /// Decided by a certificate this validator received rather than formed, in
    /// a view whose phase never accepted the entries it certifies. The
    /// certificate's verdict fixes the entries; the block is whatever was
    /// retrieved for them.
    pub(crate) fn from_foreign_qc(commit_qc: FallbackCommitQc, block: Metablock) -> Self {
        Decided { commit_qc, block }
    }
}

/// What the state machine found it can do, carrying the evidence that lets it.
///
/// Every guard in the protocol lives in `find_pending_transition`, and it
/// *constructs* these: by the time a transition exists, the certificate is
/// formed and every check has passed, so applying it cannot fail and cannot
/// race with a store that changed in between.
pub(crate) enum Transition {
    /// A pre-prepare for the current view that passed every check: right
    /// leader, valid signature, valid metablock, justified, lock respected,
    /// and not already voted in this view.
    Proposal(PrePrepareMsg),
    /// A prepare certificate over the entries accepted in this view.
    PrepareQc(PrepareQc),
    /// A commit certificate over entries this validator holds the block for.
    /// Both halves travel here: the certificate settles the entries, the block
    /// is what the decision hands on.
    CommitQc {
        qc: FallbackCommitQc,
        block: Metablock,
    },
    /// This validator leads the current view and has a proposal it is allowed
    /// to make: either nothing is locked, or the locked block is in hand.
    OwnProposal(PrePrepareMsg),
    /// A timeout certificate for this view or a later one: advance to the view
    /// after it.
    Tc(TimeoutCertificate),
    /// Send this validator's own timeout for the current view, because its
    /// timer fired or because f+1 stake already timed out there.
    Timeout,
}
