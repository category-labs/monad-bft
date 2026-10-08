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

//! The fallback path: agreement on a *metablock* -- one certified entry per
//! proposer -- when the fast path cannot commit a slot. Votes and certificates
//! range over `entries(x)`, never over the value that carried them

// Not wired into `FallbackPath` yet; routing chorus messages in is a follow-up
#[allow(dead_code)]
mod monad_mvba;
use std::{fmt::Debug, hash::Hash, sync::Arc};

use super::{
    fast::{CertifiedEntry, EnterFallbackCert},
    types::{
        IsVote, KeyPair, NodeId, Slot, StrongQc, TimestampDelta, TotalProposalMap, ValidatorData,
    },
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub(crate) struct FallbackView(u64);

#[allow(dead_code)]
impl FallbackView {
    /// Views are 1-indexed; view 0 is the not-yet-started state
    const GENESIS: Self = Self(0);
    const FIRST: Self = Self(1);

    const fn get(self) -> u64 {
        self.0
    }

    fn next(self) -> Self {
        Self(self.0 + 1)
    }

    fn saturating_sub(self, views: u64) -> Self {
        Self(self.0.saturating_sub(views))
    }
}

#[derive(Clone)]
pub(crate) struct FallbackPath {
    slot: Slot,
    round: FallbackView,

    cert: EnterFallbackCert,
    block: Metablock,

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
}

/// The value the MVBA agrees on: one certified entry per proposer
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct Metablock(TotalProposalMap<CertifiedEntry>);

impl Metablock {
    pub(crate) fn new(entries: TotalProposalMap<CertifiedEntry>) -> Self {
        Self(entries)
    }
}

pub trait ValidateInput {
    type Context;

    fn validate(&self, context: &Self::Context) -> bool;

    /// Whether the value is admissible alone, without an fbcert
    fn fbcert_optional(&self) -> bool;
}

pub trait ValidateCert: Clone + Eq + Hash + Debug {
    type Context;

    fn validate(&self, context: &Self::Context) -> bool;
}

/// The votable projection of an MVBA value: `entries(x)` in the paper
pub trait Votable: Clone + Eq + Hash + Debug {
    type Entries: Clone + Eq + Hash + Debug;

    fn entries(&self) -> Self::Entries;
}

/// Not [`From`]: `V::Entries` could be the vote type itself, colliding with the
/// blanket reflexive impl
pub trait FromEntries<V: Votable> {
    fn from_entries(entries: V::Entries) -> Self;
}

/// A protocol for Agreement on a Core Set
pub trait Mvba<V>
where
    V: ValidateInput + Votable,
{
    type Message;
    type Context;
    type TimerEvent;

    type FallbackCert: ValidateCert;

    type CommitVote: IsVote + FromEntries<V>;

    fn new(ctx: Self::Context) -> Self;

    /// At most one proposal per instance
    fn propose(&mut self, data: V, cert: Option<Self::FallbackCert>);

    fn handle_message(&mut self, sender: NodeId, message: Self::Message);

    fn handle_timer(&mut self, timer_event: Self::TimerEvent);

    // Q: can abandon be implicit via destruction? or do we need it to inform
    // persistence
    fn abandon(&mut self);

    /// The decided value. `Some` only once the block behind the certified
    /// entries is held, which may need retrieval
    fn decision(&self) -> Option<&V>;

    /// The certificate behind [`Mvba::decision`]. `Some` exactly when the
    /// decision is
    fn decision_proof(&self) -> Option<&StrongQc<Self::CommitVote>>;

    fn poll(&mut self) -> Option<MVBAOutput<Self::Message, Self::TimerEvent>>;
}

pub enum MVBAOutput<M, T> {
    Broadcast(M),
    Unicast {
        to: NodeId,
        message: M,
    },
    /// (Re-)schedule `timer_event` to fire once after `duration`. Arming an
    /// event already pending replaces it -- the last arm wins, as in
    /// [`super::SlotOutput::ScheduleTimer`]
    ScheduleTimer {
        duration: TimestampDelta,
        timer_event: T,
    },
}
