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

pub mod monad_mvba;
use std::{collections::VecDeque, fmt::Debug, hash::Hash};

use alloy_rlp::{Decodable, Encodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};
pub use monad_mvba::FallbackCommitQc;

/// MetaBlock components are exported
pub use super::fast::CertifiedEntry;
pub use super::fast::{
    EnterFallbackCert, EnterFallbackVote, Entry, FallbackEntry, FallbackQc, FastQc,
};
use super::{
    super::{message::Outbound, runtime::Runtime, timers::Timers},
    types::{
        IsVote, Lanes, NodeId, Slot, StrongQc, Timestamp, TimestampDelta, ValidatorData,
        ValidatorIndex,
    },
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, RlpEncodableWrapper)]
pub struct FallbackView(u64);

// u64::MAX is never a view, so `next` cannot overflow on a decoded one
impl Decodable for FallbackView {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let view = u64::decode(buf)?;
        if view == u64::MAX {
            return Err(alloy_rlp::Error::Custom("view out of range"));
        }
        Ok(Self(view))
    }
}

/// The slot and view authenticated by an MVBA vote.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, RlpEncodable)]
pub struct MvbaScope {
    pub slot: Slot,
    pub view: FallbackView,
}

impl Decodable for MvbaScope {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let slot = Slot::decode(&mut payload)?;
        let view = FallbackView::decode(&mut payload)?;
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        if view < FallbackView::FIRST {
            return Err(alloy_rlp::Error::Custom("scope names no view"));
        }
        Ok(Self { slot, view })
    }
}

impl MvbaScope {
    pub const fn new(slot: Slot, view: FallbackView) -> Self {
        Self { slot, view }
    }
}

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

/// The value the MVBA agrees on: one certified entry per proposer
#[derive(Clone, PartialEq, Eq, Hash, Debug, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct Metablock(Lanes<CertifiedEntry>);

impl Metablock {
    pub fn new(entries: Lanes<CertifiedEntry>) -> Self {
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
    // votes range over the entries, so they must be signable
    type Entries: Clone + Eq + Hash + Debug + Encodable;

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

    /// The validators the sender indices refer to
    fn validator_data(&self) -> &ValidatorData;

    fn handle_message(&mut self, sender: ValidatorIndex, message: Self::Message);

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

pub struct MvbaRuntime<M, V>
where
    M: Mvba<V>,
    V: ValidateInput + Votable,
{
    mvba: M,

    outbox: VecDeque<Outbound<M::Message>>,
    timers: Timers<M::TimerEvent>,

    decisions: VecDeque<V>,
    reported: Option<V>,
}

impl<A, V> MvbaRuntime<A, V>
where
    A: Mvba<V>,
    V: ValidateInput + Votable,
    A::TimerEvent: Eq,
{
    /// A validator that is never handed an input only listens: its state
    /// machine gates voting and deciding on that input
    pub fn new(mvba: A) -> Self {
        Self {
            mvba,
            outbox: VecDeque::new(),
            timers: Timers::default(),
            decisions: VecDeque::new(),
            reported: None,
        }
    }

    fn drain(&mut self, now: Timestamp) {
        while let Some(output) = self.mvba.poll() {
            match output {
                MVBAOutput::Broadcast(message) => {
                    self.outbox.push_back(Outbound::Broadcast(message));
                }
                MVBAOutput::Unicast { to, message } => {
                    self.outbox.push_back(Outbound::Unicast(to, message));
                }
                MVBAOutput::ScheduleTimer {
                    duration,
                    timer_event,
                } => {
                    // the last arm of the same event wins
                    self.timers.retain(|armed| *armed != timer_event);
                    self.timers.schedule(now + duration, timer_event);
                }
            }
        }
        self.report_decision();
    }

    // reports the first decision, and any later one that differs from it
    fn report_decision(&mut self) {
        let Some(decision) = self.mvba.decision() else {
            return;
        };
        if self.reported.as_ref() == Some(decision) {
            return;
        }
        let decision = decision.clone();
        self.decisions.push_back(decision.clone());
        self.reported = Some(decision);
    }
}

impl<A, V> Runtime for MvbaRuntime<A, V>
where
    A: Mvba<V>,
    V: ValidateInput + Votable,
    A::TimerEvent: Eq,
{
    type Message = A::Message;
    // at most one per instance
    type LocalIn = (V, Option<A::FallbackCert>);
    type LocalOut = V;

    fn receive(&mut self, now: Timestamp, sender: NodeId, message: A::Message) {
        use crate::spec::validator::ValidatorData as _;

        // the boundary of a standalone instance: a sender becomes a validator
        // index here, as in Chorus::handle_message
        let Some(sender) = self.mvba.validator_data().index_of(&sender) else {
            return;
        };
        self.mvba.handle_message(sender, message);
        self.drain(now);
    }

    fn poll_msg(&mut self) -> Option<Outbound<A::Message>> {
        self.outbox.pop_front()
    }

    fn handle_input(&mut self, now: Timestamp, (input, cert): (V, Option<A::FallbackCert>)) {
        self.mvba.propose(input, cert);
        self.drain(now);
    }

    fn poll_output(&mut self) -> Option<V> {
        self.decisions.pop_front()
    }

    fn handle_due(&mut self, now: Timestamp) {
        while let Some(timer_event) = self.timers.pop_due(now) {
            self.mvba.handle_timer(timer_event);
            self.drain(now);
        }
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.timers.next_due()
    }
}
