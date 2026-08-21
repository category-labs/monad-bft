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

// Not wired into `FallbackPath` yet: routing chorus messages into the MVBA is
// a follow-up, so nothing outside its own tests constructs it.
#[allow(dead_code)]
mod block_sync;
#[allow(dead_code)]
mod monad_mvba;
use std::{fmt::Debug, hash::Hash, sync::Arc};

use super::{
    fast::{CertifiedEntry, EnterFallbackCert},
    types::{
        IsVote, KeyPair, NodeId, Slot, StrongQc, TimestampDelta, TotalProposalMap, ValidatorData,
    },
};

// Views are 1-indexed, matching the paper; FallbackView(0) is the
// not-yet-started state.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub(crate) struct FallbackView(u64);

#[allow(dead_code)]
impl FallbackView {
    /// Before any view: `lastVotedView` starts here, and no view equals it.
    const GENESIS: Self = Self(0);
    const FIRST: Self = Self(1);

    const fn get(self) -> u64 {
        self.0
    }

    fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Clone)]
pub(crate) struct FallbackPath {
    slot: Slot,
    round: FallbackView,

    /// The fallback certificate that admitted this slot to the fallback path.
    /// Concrete here because both entry paths into `Chorus::enter_fallback`
    /// hold one; the `Option` only appears at the [`Mvba::propose`] boundary,
    /// where the paper's `fbcert = ⊥` fast metablock is also representable.
    cert: EnterFallbackCert,
    /// This validator's MVBA input value: the block alone. Agreement ranges
    /// over its entries, and the certificate travels beside it.
    block: Metablock,

    // using Arc to avoid lifetime issues.
    key: Arc<KeyPair>,
    validator_data: Arc<ValidatorData>,
}

impl FallbackPath {
    pub(crate) fn new(
        slot: Slot,
        key: Arc<KeyPair>,
        validator_data: Arc<ValidatorData>,
        cert: EnterFallbackCert,
        block: Metablock,
    ) -> Self {
        Self {
            slot,
            round: FallbackView(0),
            key,
            validator_data,
            cert,
            block,
        }
    }

    pub(crate) fn on_tick(&mut self) {
        todo!()
    }
}

/// The value the MVBA agrees on: one certified entry per proposer.
///
/// The fallback certificate is deliberately *not* part of it. Agreement ranges
/// over `entries(x)` only, so the certificate plays no part past admission,
/// and two correct validators may hold different, equally valid aggregates of
/// it -- nothing consensus can agree on. It travels beside the value instead:
/// on [`Mvba::propose`], and on the view-1 pre-prepare that needs it as its
/// justification.
pub(crate) type Metablock = TotalProposalMap<CertifiedEntry>;

/// Whether a value is acceptable, given the certificate carried beside it.
///
/// The certificate is `Option` because it can be absent -- the paper's
/// `fbcert = ⊥` *fast metablock*, whose entries are all `FastQC`. Whether a
/// value is acceptable without one is the value's own business, not the
/// carrier's, so it is decided here and nowhere else: the generic pre-prepare
/// handler passes on whatever the message carried and does not interpret it.
pub trait Validate {
    type Context;
    /// The certificate that may accompany the value; [`Mvba::FallbackCert`].
    type Cert;

    fn validate(&self, context: &Self::Context, cert: Option<&Self::Cert>) -> bool;
}

/// The votable projection of an MVBA value: `entries(x)` in the paper.
///
/// Agreement is over this projection, not over the value itself: prepare and
/// commit votes -- and hence the certificates aggregated from them -- range
/// over `entries(x)`, so the certificate is independent of how the value is
/// carried, and a fallback decision is comparable with a fast-path commitment
/// on the same entries.
pub trait Votable {
    type Entries: Clone + Eq + Hash + Debug;

    fn entries(&self) -> Self::Entries;
}

/// A protocol for Agreement on a Core Set
pub trait Mvba<V: Votable>
where
    V: Validate<Cert = Self::FallbackCert>,
{
    type Message;
    type Context;
    type TimerEvent;

    /// The certificate admitting the slot to the fallback path, carried beside
    /// the value rather than inside it. An associated type, not a parameter:
    /// which certificate admits a value is fixed by the protocol, not chosen
    /// by the caller.
    type FallbackCert;

    /// The commit vote the decision certificate aggregates. A distinct type
    /// per protocol gives commit votes their own signing domain; it wraps the
    /// decided value's [`Votable::Entries`].
    type CommitVote: IsVote + From<V::Entries>;

    fn new(ctx: Self::Context) -> Self;

    /// Propose the data to be included in the core set. At most one
    /// proposal is allowed for each Acs instance.
    ///
    /// `cert` is the certificate that admits `data`; it is kept beside the
    /// value and carried on this validator's view-1 proposal, where it is the
    /// justification. `None` is the paper's `fbcert = ⊥` case.
    fn propose(&mut self, data: V, cert: Option<Self::FallbackCert>);

    /// Handle a message received over network
    fn handle_message(&mut self, sender: NodeId, message: Self::Message);

    fn handle_timer(&mut self, timer_event: Self::TimerEvent);

    // Q: can abandon be implicit via destruction? or do we need it to inform
    // persistence
    fn abandon(&mut self);

    /// Query whether the ACS has made an decision.
    ///
    /// What a supermajority attested to is the projection votes ranged over,
    /// not the value that carried it: [`Mvba::decision_proof`] certifies
    /// `entries(x)` and nothing in the value around it adds evidence. What is
    /// returned here is the block behind that verdict, as retrieved from the
    /// proposal that carried it or from a peer that held it. Typed concretely
    /// because [`Metablock`] is; it becomes `V` itself in the generics pass.
    ///
    /// Agreement therefore completes without the block, but this returns
    /// `Some` only once it is held: what consumes a decided slot needs the
    /// certified entries themselves, so reporting a decision the caller cannot
    /// act on would only move the wait somewhere with less context. The
    /// certificate is durable evidence the moment it arrives and is kept
    /// regardless; retrieval is what this waits on, and it is bounded by an
    /// honest holder answering a request.
    fn decision(&self) -> Option<&Metablock>;

    /// The certificate proving the decision: a supermajority of commit votes
    /// over `entries(x)` of the decided value. It is the transferable
    /// commitment proof the fallback path finalizes on, so no separate commit
    /// round is needed after agreement.
    ///
    /// Returns `Some` whenever [`Mvba::decision`] does.
    fn decision_proof(&self) -> Option<&StrongQc<Self::CommitVote>>;

    fn poll(&mut self) -> Option<MVBAOutput<Self::Message, Self::TimerEvent>>;
}

pub enum MVBAOutput<M, T> {
    Broadcast(M),
    /// A reply to one validator, for a message that only the sender needs back.
    Unicast {
        to: NodeId,
        message: M,
    },
    ScheduleTimer {
        duration: TimestampDelta,
        timer_event: T,
    },
}
