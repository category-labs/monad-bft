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

    input: MVBAInputs,

    // using Arc to avoid lifetime issues.
    key: Arc<KeyPair>,
    validator_data: Arc<ValidatorData>,
}

impl FallbackPath {
    pub(crate) fn new(
        slot: Slot,
        key: Arc<KeyPair>,
        validator_data: Arc<ValidatorData>,
        input: MVBAInputs,
    ) -> Self {
        Self {
            slot,
            round: FallbackView(0),
            key,
            validator_data,
            input,
        }
    }

    pub(crate) fn on_tick(&mut self) {
        todo!()
    }
}

// FIXME: rename it. partialblock concept was deleted
pub(crate) type PartialBlock = TotalProposalMap<CertifiedEntry>;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct MVBAInputs {
    // TODO: reasonably split out fallback cert from input. Note it can be empty
    pub enter_fallback_cert: EnterFallbackCert,
    pub block: PartialBlock,
}

pub trait Validate {
    type Context;
    fn validate(&self, context: &Self::Context) -> bool;
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
pub trait Mvba<V: Validate + Votable> {
    type Message;
    type Context;
    type TimerEvent;

    /// The commit vote the decision certificate aggregates. A distinct type
    /// per protocol gives commit votes their own signing domain; it wraps the
    /// decided value's [`Votable::Entries`].
    type CommitVote: IsVote + From<V::Entries>;

    fn new(ctx: Self::Context) -> Self;

    /// Propose the data to be included in the core set. At most one
    /// proposal is allowed for each Acs instance.
    fn propose(&mut self, data: V);

    /// Handle a message received over network
    fn handle_message(&mut self, sender: NodeId, message: Self::Message);

    fn handle_timer(&mut self, timer_event: Self::TimerEvent);

    // Q: can abandon be implicit via destruction? or do we need it to inform
    // persistence
    fn abandon(&mut self);

    /// Query whether the ACS has made an decision.
    ///
    /// The decision is the projection votes ranged over, not the value that
    /// carried it: [`Mvba::decision_qc`] certifies `entries(x)`, so a
    /// supermajority has attested to exactly this much and nothing in the
    /// value around it adds evidence.
    ///
    /// Agreement therefore completes without the value, but this returns
    /// `Some` only once the block behind the entries is held as well: what
    /// consumes a decided slot needs the certified entries themselves, so
    /// reporting a decision the caller cannot act on would only move the wait
    /// somewhere with less context. The certificate is durable evidence the
    /// moment it arrives and is kept regardless; retrieval is what this waits
    /// on, and it is bounded by an honest holder answering a request.
    fn decision(&self) -> Option<&V::Entries>;

    /// The certificate proving the decision: a supermajority of commit votes
    /// over `entries(x)` of the decided value. It is the transferable
    /// commitment proof the fallback path finalizes on, so no separate commit
    /// round is needed after agreement.
    ///
    /// Returns `Some` whenever [`Mvba::decision`] does.
    fn decision_qc(&self) -> Option<&StrongQc<Self::CommitVote>>;

    /// The decided block: the certified entries [`Mvba::decision`] projects,
    /// as retrieved from the proposal that carried them or from a peer that
    /// held them.
    ///
    /// Returns `Some` exactly when the other two do. Typed concretely because
    /// [`PartialBlock`] is; it becomes `V` itself in the generics pass.
    fn decision_block(&self) -> Option<&PartialBlock>;

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
