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

//! What a validator's view can be in. Each phase carries the evidence that
//! justifies it, and the only way to reach one is a consuming method that
//! demands that evidence. [`Decided`] is the exception: a supermajority
//! commit certificate is complete evidence on its own, whatever phase it
//! finds this validator in

use super::{
    super::{ValidateCert, Votable},
    certificates::{FallbackCommitQc, PrepareQc, TimeoutCertificate},
    messages::PrePrepareMsg,
};

const POISON_OBSERVED: &str = "the poison phase installed by try_advance was observed";

/// The state of the current view
#[derive(Clone)]
pub(crate) enum Phase<V: Votable> {
    NewView(NewView),
    AwaitingProposal(AwaitingProposal),
    Preparing(Preparing<V>),
    Committing(Committing<V>),
    TimedOut(TimedOut<V>),
    /// Terminal
    Decided(Decided<V>),
    /// Installed by `try_advance` while the real phase is stepped by value;
    /// observing it is a bug, and every accessor but [`Phase::name`] panics
    Poisoned,
}

/// A view just entered, before the proposer check has run. The check runs here
/// and nowhere else, so a leader proposes at most once per view
#[derive(Clone)]
pub(crate) struct NewView {
    // deliberately empty: the justification for entering lives in `entry_tc`
    _private: (),
}

/// The proposer check has run and no proposal is accepted in this view yet
#[derive(Clone)]
pub(crate) struct AwaitingProposal {
    // deliberately empty: what is known before a proposal arrives is instance
    // state, not view state
    _private: (),
}

/// Proposal accepted, prepare vote sent. The block went to the block store
#[derive(Clone)]
pub(crate) struct Preparing<V: Votable> {
    entries: V::Entries,
}

/// Prepare certificate formed, commit vote sent
#[derive(Clone)]
pub(crate) struct Committing<V: Votable> {
    entries: V::Entries,
    prepare_qc: PrepareQc<V>,
}

/// Decided, with the certificate that proves it and the block it settled
#[derive(Clone)]
pub(crate) struct Decided<V: Votable> {
    commit_qc: FallbackCommitQc<V>,
    block: V,
}

/// A timed-out view, remembering the entries it accepted: this validator casts
/// no further vote here, but a commit certificate over `entries(x_v)` can still
/// complete. `None` if nothing was accepted (`x_v = ⊥`)
#[derive(Clone)]
pub(crate) struct TimedOut<V: Votable> {
    accepted: Option<V::Entries>,
}

impl<V: Votable> Phase<V> {
    pub(crate) fn new() -> Self {
        Phase::NewView(NewView { _private: () })
    }

    pub(crate) fn is_decided(&self) -> bool {
        match self {
            Phase::Decided(_) => true,
            Phase::Poisoned => unreachable!("{POISON_OBSERVED}"),
            _ => false,
        }
    }

    pub(crate) fn decided(&self) -> Option<&Decided<V>> {
        match self {
            Phase::Decided(p) => Some(p),
            Phase::Poisoned => unreachable!("{POISON_OBSERVED}"),
            _ => None,
        }
    }

    pub(crate) fn has_timed_out(&self) -> bool {
        match self {
            Phase::TimedOut(_) => true,
            Phase::Poisoned => unreachable!("{POISON_OBSERVED}"),
            _ => false,
        }
    }

    /// `entries(x_v)` of the accepted proposal, if one was accepted
    pub(crate) fn entries(&self) -> Option<&V::Entries> {
        match self {
            Phase::NewView(_) | Phase::AwaitingProposal(_) => None,
            Phase::Preparing(p) => Some(&p.entries),
            Phase::Committing(p) => Some(&p.entries),
            Phase::TimedOut(p) => p.accepted.as_ref(),
            Phase::Decided(p) => Some(p.entries()),
            Phase::Poisoned => unreachable!("{POISON_OBSERVED}"),
        }
    }

    /// Entries voted to prepare with no prepare certificate yet. Never a
    /// timed-out view
    pub(crate) fn preparing_entries(&self) -> Option<&V::Entries> {
        match self {
            Phase::Preparing(p) => Some(&p.entries),
            Phase::Poisoned => unreachable!("{POISON_OBSERVED}"),
            _ => None,
        }
    }

    pub(crate) fn prepare_qc(&self) -> Option<&PrepareQc<V>> {
        match self {
            Phase::Committing(p) => Some(&p.prepare_qc),
            Phase::Poisoned => unreachable!("{POISON_OBSERVED}"),
            _ => None,
        }
    }

    /// For assertion messages
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Phase::NewView(_) => "new view",
            Phase::AwaitingProposal(_) => "awaiting proposal",
            Phase::Preparing(_) => "preparing",
            Phase::Committing(_) => "committing",
            Phase::TimedOut(_) => "timed out",
            Phase::Decided(_) => "decided",
            // never panics: this feeds other panic messages
            Phase::Poisoned => "poisoned",
        }
    }

    /// A timed-out phase times out to itself; `None` only for a decided phase
    pub(crate) fn time_out(self) -> Option<Phase<V>> {
        let accepted = match self {
            Phase::NewView(_) | Phase::AwaitingProposal(_) => None,
            Phase::Preparing(p) => Some(p.entries),
            Phase::Committing(p) => Some(p.entries),
            Phase::TimedOut(t) => t.accepted,
            Phase::Decided(_) => return None,
            Phase::Poisoned => unreachable!("{POISON_OBSERVED}"),
        };

        Some(Phase::TimedOut(TimedOut { accepted }))
    }
}

impl NewView {
    pub(crate) fn await_proposal(self) -> AwaitingProposal {
        AwaitingProposal { _private: () }
    }
}

impl AwaitingProposal {
    /// The caller must have run the pre-prepare checks and stored the block
    pub(crate) fn accept<V: Votable>(self, entries: V::Entries) -> Preparing<V> {
        Preparing { entries }
    }
}

impl<V: Votable> Preparing<V> {
    pub(crate) fn entries(&self) -> &V::Entries {
        &self.entries
    }

    pub(crate) fn commit(self, prepare_qc: PrepareQc<V>) -> Committing<V> {
        debug_assert_eq!(prepare_qc.verdict.0, self.entries);

        Committing {
            entries: self.entries,
            prepare_qc,
        }
    }
}

impl<V: Votable> Committing<V> {
    pub(crate) fn entries(&self) -> &V::Entries {
        &self.entries
    }
}

impl<V: Votable> Decided<V> {
    pub(crate) fn entries(&self) -> &V::Entries {
        &self.commit_qc.verdict.0
    }

    pub(crate) fn commit_qc(&self) -> &FallbackCommitQc<V> {
        &self.commit_qc
    }

    pub(crate) fn block(&self) -> &V {
        &self.block
    }

    /// A supermajority commit certificate together with its block decides from
    /// any phase: the certificate is the evidence, the local phase adds none
    pub(crate) fn new(commit_qc: FallbackCommitQc<V>, block: V) -> Self {
        Decided { commit_qc, block }
    }
}

/// What the state machine found it can do, with the evidence that lets it
/// `find_pending_transition` constructs these, so applying one cannot fail
pub(crate) enum Transition<V: Votable, C: ValidateCert> {
    Proposal(PrePrepareMsg<V, C>),
    PrepareQc(PrepareQc<V>),
    Decide {
        qc: FallbackCommitQc<V>,
        block: V,
    },
    OwnProposalReady(PrePrepareMsg<V, C>),
    AwaitProposal,
    Tc(TimeoutCertificate<V>),
    /// Timer fired, or f+1 stake already timed out. Repeats in a timed-out view
    /// per retransmission
    Timeout,
}
