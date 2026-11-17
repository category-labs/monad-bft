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

//! Re-sends the outbound slot messages of a slot that has not decided
//! yet, over a transport that may drop packets, and the finalization
//! certificate of a slot that has, until the chain moves past it.

use std::collections::BTreeMap;

use monad_mcp_common::{Slot, Timestamp, TimestampDelta};

use super::WireOutbound;
use crate::cadence::SlotLifecycle;

#[derive(Clone, Copy)]
pub struct RepeaterConfig {
    /// Tick period, and the age after which an undecided slot is repeated
    pub interval: TimestampDelta,
    /// A finalized slot's certificate is repeated until slot < cap -
    /// retention, and always at least once however fast the cap moves
    pub certificate_retention: u64,
}

// The repeats of a slot, as they go to the wire.
struct Undecided<W> {
    since: Timestamp,
    deliveries: Vec<WireOutbound<W>>,
}

struct Certificate<W> {
    delivery: WireOutbound<W>,
    repeated: bool,
}

pub(super) struct Repeater<W> {
    config: RepeaterConfig,
    undecided: BTreeMap<Slot, Undecided<W>>,
    finalized: BTreeMap<Slot, Certificate<W>>,
    // armed while anything is pending
    next_tick: Option<Timestamp>,
}

impl<W> Repeater<W> {
    pub(super) fn new(config: RepeaterConfig) -> Self {
        Self {
            config,
            undecided: BTreeMap::new(),
            finalized: BTreeMap::new(),
            next_tick: None,
        }
    }

    fn is_pending(&self) -> bool {
        !self.undecided.is_empty() || !self.finalized.is_empty()
    }

    /// Remember an outbound message of an undecided slot. Repeats are
    /// emitted past this point, so they are never recorded again.
    pub(super) fn record(&mut self, now: Timestamp, slot: Slot, delivery: WireOutbound<W>)
    where
        W: PartialEq,
    {
        if self.finalized.contains_key(&slot) {
            return;
        }

        let undecided = self.undecided.entry(slot).or_insert_with(|| Undecided {
            since: now,
            deliveries: Vec::new(),
        });
        if !undecided.deliveries.contains(&delivery) {
            undecided.deliveries.push(delivery);
        }
    }

    /// Keep the certificate of a finalized slot, in place of its messages.
    pub(super) fn certify(&mut self, slot: Slot, delivery: WireOutbound<W>) {
        self.undecided.remove(&slot);
        let certificate = Certificate {
            delivery,
            repeated: false,
        };
        self.finalized.insert(slot, certificate);
    }

    pub(super) fn handle_lifecycle(&mut self, event: SlotLifecycle) {
        match event {
            SlotLifecycle::Opened { .. } => {}
            SlotLifecycle::Completed { slot } => self.handle_completed(slot),
            SlotLifecycle::CapAdvance { cap } => self.handle_cap_advance(cap),
        }
    }

    /// A slot that ended without finalizing has nothing worth repeating.
    fn handle_completed(&mut self, slot: Slot) {
        self.undecided.remove(&slot);
    }

    fn handle_cap_advance(&mut self, cap: Slot) {
        // a peer's cap jump can close a slot without a local finalization,
        // which would otherwise leave it repeating forever
        self.undecided = self.undecided.split_off(&cap);

        // retention counts slots but the only sender is the tick, so a
        // certificate no tick has emitted yet outlives the floor
        let floor = cap
            .checked_sub(self.config.certificate_retention)
            .unwrap_or(Slot::MIN);
        self.finalized
            .retain(|slot, certificate| *slot >= floor || !certificate.repeated);
    }

    // starts the tick once something is pending
    pub(super) fn arm(&mut self, now: Timestamp) {
        if self.is_pending() && self.next_tick.is_none() {
            self.next_tick = Some(now + self.config.interval);
        }
    }

    pub(super) fn next_due(&self) -> Option<Timestamp> {
        self.next_tick
    }

    pub(super) fn tick(&mut self, now: Timestamp) -> Vec<WireOutbound<W>>
    where
        W: Clone,
    {
        let due = self.due(now);
        self.next_tick = self.is_pending().then(|| now + self.config.interval);
        due
    }

    /// The repeats owed at `now`: every recorded message of a slot whose
    /// first send is at least `interval` old, plus every retained
    /// certificate.
    fn due(&mut self, now: Timestamp) -> Vec<WireOutbound<W>>
    where
        W: Clone,
    {
        let mut due = Vec::new();

        for undecided in self.undecided.values() {
            let Some(age) = now.duration_since(undecided.since) else {
                continue;
            };
            if age < self.config.interval {
                continue;
            }
            due.extend(undecided.deliveries.iter().cloned());
        }

        for certificate in self.finalized.values_mut() {
            certificate.repeated = true;
            due.push(certificate.delivery.clone());
        }

        due
    }
}

#[cfg(test)]
mod tests {
    use monad_mcp_common::{Outbound, WirePriority};

    use super::*;
    use crate::{cadence::Recipients, common::NodeId};

    const INTERVAL: TimestampDelta = TimestampDelta::from_millis(5_000);

    fn repeater(certificate_retention: u64) -> Repeater<u64> {
        Repeater::new(RepeaterConfig {
            interval: INTERVAL,
            certificate_retention,
        })
    }

    fn at(millis: u64) -> Timestamp {
        Timestamp::from_millis(millis)
    }

    fn everyone() -> Vec<NodeId> {
        (1..4).map(NodeId::dummy).collect()
    }

    // a message to everyone, with a body that tells messages apart
    fn note(body: u64) -> WireOutbound<u64> {
        Outbound {
            to: Recipients::Nodes(everyone()),
            message: body,
            qos: WirePriority::Regular,
        }
    }

    fn note_to(node: u64, body: u64) -> WireOutbound<u64> {
        Outbound {
            to: Recipients::Node(NodeId::dummy(node)),
            message: body,
            qos: WirePriority::Regular,
        }
    }

    #[test]
    fn an_equal_re_record_is_deduped() {
        let mut repeater = repeater(2);
        repeater.record(at(0), Slot(1), note(7));
        repeater.record(at(1_000), Slot(1), note(7));
        repeater.record(at(1_000), Slot(1), note_to(3, 7));
        repeater.record(at(1_000), Slot(1), note(8));

        assert_eq!(
            repeater.due(at(5_000)),
            vec![note(7), note_to(3, 7), note(8)]
        );
    }

    #[test]
    fn finalization_drops_the_undecided_messages_and_keeps_the_certificate() {
        let mut repeater = repeater(2);
        repeater.record(at(0), Slot(1), note(7));
        repeater.certify(Slot(1), note(42));

        assert_eq!(repeater.due(at(5_000)), vec![note(42)]);
        // a finalized slot records nothing further
        repeater.record(at(5_000), Slot(1), note(7));
        assert_eq!(repeater.due(at(20_000)), vec![note(42)]);
    }

    #[test]
    fn a_faulted_slot_is_dropped() {
        let mut repeater = repeater(2);
        repeater.record(at(0), Slot(1), note(7));
        repeater.handle_lifecycle(SlotLifecycle::Completed { slot: Slot(1) });

        assert!(repeater.due(at(5_000)).is_empty());
    }

    #[test]
    fn a_cap_advance_trims_both_maps() {
        let mut repeater = repeater(2);
        for slot in 0..6 {
            repeater.record(at(0), Slot(slot), note(slot));
        }
        for slot in 0..3 {
            repeater.certify(Slot(slot), note(100 + slot));
        }
        // a tick, so every certificate is past its guaranteed repeat
        repeater.due(at(5_000));

        // cap 4: slots below it are closed, certificates survive to cap - 2
        repeater.handle_lifecycle(SlotLifecycle::CapAdvance { cap: Slot(4) });
        assert_eq!(repeater.due(at(5_000)), vec![note(4), note(5), note(102)]);

        repeater.handle_lifecycle(SlotLifecycle::CapAdvance { cap: Slot(5) });
        assert_eq!(repeater.due(at(5_000)), vec![note(5)]);
    }

    #[test]
    fn a_certificate_no_tick_has_sent_yet_outlives_the_cap() {
        let mut repeater = repeater(2);
        repeater.certify(Slot(1), note(101));

        // the cap runs past the retention before the first tick
        repeater.handle_lifecycle(SlotLifecycle::CapAdvance { cap: Slot(9) });
        assert_eq!(repeater.due(at(5_000)), vec![note(101)]);

        repeater.handle_lifecycle(SlotLifecycle::CapAdvance { cap: Slot(9) });
        assert!(repeater.due(at(5_000)).is_empty());
    }

    #[test]
    fn a_slot_is_repeated_only_once_its_first_send_is_interval_old() {
        let mut repeater = repeater(2);
        repeater.record(at(1_000), Slot(1), note(7));
        // a later message of the same slot does not reset the slot's age
        repeater.record(at(3_000), Slot(1), note(8));

        assert!(repeater.due(at(5_999)).is_empty());
        assert_eq!(repeater.due(at(6_000)), vec![note(7), note(8)]);
    }
}
