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
pub mod monad_mvba;
use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    hash::Hash,
    sync::Arc,
};

/// MetaBlock components are exported
pub use super::fast::{
    CertifiedEntry, EnterFallbackCert, EnterFallbackVote, Entry, FallbackEntry, FallbackQc, FastQc,
};
use super::{
    super::{
        driver::{NodeEvent, WakeId},
        runtime::Runtime,
    },
    types::{
        IsVote, KeyPair, NodeId, Slot, StrongQc, Timestamp, TimestampDelta, TotalProposalMap,
        Validated, ValidatorData,
    },
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct FallbackView(u64);

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
pub struct Metablock(TotalProposalMap<CertifiedEntry>);

impl Metablock {
    pub fn new(entries: TotalProposalMap<CertifiedEntry>) -> Self {
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

pub struct MvbaRuntime<M, V>
where
    M: Mvba<V>,
    V: ValidateInput + Votable,
{
    mvba: M,
    /// Held until the start wake fires; `None` is a listener, which never proposes
    input: Option<(V, Option<M::FallbackCert>)>,

    // Deferred start mechanism
    start_at: Timestamp,
    start_wake: Option<WakeId>,

    outbox: VecDeque<NodeEvent<M::Message>>,
    /// Pending timers; the last arm of the same event wins, so a superseded
    /// wake misses the map
    armed: HashMap<WakeId, M::TimerEvent>,
    next_wake: WakeId,

    observer: Option<Box<dyn FnMut(Timestamp, &V)>>,
    reported: Option<V>,
}

impl<A, V> MvbaRuntime<A, V>
where
    A: Mvba<V>,
    V: ValidateInput + Votable,
    A::TimerEvent: Eq,
{
    pub fn new(mvba: A, input: V, cert: Option<A::FallbackCert>) -> Self {
        Self::with_input(mvba, Some((input, cert)))
    }

    /// A validator with no fallback input: its state machine gates
    /// participation on that input, so it listens without voting or deciding
    pub fn listening(mvba: A) -> Self {
        Self::with_input(mvba, None)
    }

    fn with_input(mvba: A, input: Option<(V, Option<A::FallbackCert>)>) -> Self {
        Self {
            mvba,
            input,
            start_at: Timestamp::GENESIS,
            start_wake: None,
            outbox: VecDeque::new(),
            armed: HashMap::new(),
            next_wake: WakeId::FIRST,
            observer: None,
            reported: None,
        }
    }

    /// When the state machine is handed its input
    pub fn set_start(&mut self, at: Timestamp) {
        self.start_at = at;
    }

    /// Reports the first decision, and any later one that differs from it
    pub fn on_decision(&mut self, observer: impl FnMut(Timestamp, &V) + 'static) {
        self.observer = Some(Box::new(observer));
    }

    fn drain(&mut self, now: Timestamp) {
        while let Some(output) = self.mvba.poll() {
            match output {
                MVBAOutput::Broadcast(message) => {
                    self.outbox.push_back(NodeEvent::Broadcast(message));
                }
                MVBAOutput::Unicast { to, message } => {
                    self.outbox.push_back(NodeEvent::Unicast { to, message });
                }
                MVBAOutput::ScheduleTimer {
                    duration,
                    timer_event,
                } => {
                    // evicting the previous arm is what makes its wake stale
                    self.armed.retain(|_, armed| *armed != timer_event);
                    let id = self.next_wake.post_increment();
                    self.armed.insert(id, timer_event);
                    self.outbox.push_back(NodeEvent::WakeAfter(duration, id));
                }
            }
        }
        self.report_decision(now);
    }

    fn report_decision(&mut self, now: Timestamp) {
        if self.observer.is_none() {
            return;
        }
        let Some(decision) = self.mvba.decision() else {
            return;
        };
        if self.reported.as_ref() == Some(decision) {
            return;
        }
        let decision = decision.clone();
        let observer = self.observer.as_mut().expect("just checked");
        observer(now, &decision);
        self.reported = Some(decision);
    }
}

impl<A, V> Runtime<A::Message> for MvbaRuntime<A, V>
where
    A: Mvba<V>,
    V: ValidateInput + Votable,
    A::TimerEvent: Eq,
{
    fn init(&mut self) {
        if self.input.is_none() {
            return;
        }
        let id = self.next_wake.post_increment();
        self.start_wake = Some(id);
        self.outbox.push_back(NodeEvent::Wake(self.start_at, id));
    }

    fn wake(&mut self, now: Timestamp, wake: WakeId) {
        if self.start_wake == Some(wake) {
            self.start_wake = None;
            let (input, cert) = self.input.take().expect("start wake armed with an input");
            self.mvba.propose(input, cert);
        } else if let Some(timer_event) = self.armed.remove(&wake) {
            self.mvba.handle_timer(timer_event);
        } else {
            // canceled: the event was re-armed after this wake was scheduled
            return;
        }
        self.drain(now);
    }

    fn receive(&mut self, now: Timestamp, message: Validated<A::Message>) {
        let (message, author) = message.destructure();
        self.mvba.handle_message(author, message);
        self.drain(now);
    }

    fn poll(&mut self) -> Option<NodeEvent<A::Message>> {
        self.outbox.pop_front()
    }
}
