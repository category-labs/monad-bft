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

use super::{
    slot::SlotConsensus,
    types::{NodeId, Slot, Timestamp, TimestampDelta},
};

#[derive(Clone, Copy)]
pub struct RepeaterConfig {
    /// Tick period, and the age after which an undecided slot is repeated
    pub interval: TimestampDelta,
    /// A finalized slot's certificate is repeated until slot < cap -
    /// retention, and always at least once however fast the cap moves
    pub certificate_retention: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Recipients {
    Everyone,
    Node(NodeId),
}

#[derive(Clone, PartialEq)]
struct Delivery<M> {
    to: Recipients,
    message: M,
}

struct Undecided<M> {
    since: Timestamp,
    deliveries: Vec<Delivery<M>>,
}

struct Certificate<M> {
    message: M,
    repeated: bool,
}

pub struct Repeater<S>
where
    S: SlotConsensus,
{
    config: RepeaterConfig,
    undecided: BTreeMap<Slot, Undecided<S::Message>>,
    finalized: BTreeMap<Slot, Certificate<S::Message>>,
}

impl<S> Repeater<S>
where
    S: SlotConsensus,
{
    pub fn new(config: RepeaterConfig) -> Self {
        Self {
            config,
            undecided: BTreeMap::new(),
            finalized: BTreeMap::new(),
        }
    }

    pub fn interval(&self) -> TimestampDelta {
        self.config.interval
    }

    /// Remember an outbound message of an undecided slot. Repeats are
    /// emitted past this point, so they are never recorded again.
    pub fn record(&mut self, now: Timestamp, slot: Slot, to: Recipients, message: &S::Message) {
        if self.finalized.contains_key(&slot) {
            return;
        }

        let undecided = self.undecided.entry(slot).or_insert_with(|| Undecided {
            since: now,
            deliveries: Vec::new(),
        });
        let delivery = Delivery {
            to,
            message: message.clone(),
        };
        if !undecided.deliveries.contains(&delivery) {
            undecided.deliveries.push(delivery);
        }
    }

    pub fn handle_finalization(&mut self, slot: Slot, data: &S::FinalizationData) {
        self.undecided.remove(&slot);
        if let Some(message) = S::certificate_message(data) {
            self.finalized.insert(
                slot,
                Certificate {
                    message,
                    repeated: false,
                },
            );
        }
    }

    /// A slot that ended without finalizing has nothing worth repeating.
    pub fn handle_completed(&mut self, slot: Slot) {
        self.undecided.remove(&slot);
    }

    pub fn handle_cap_advance(&mut self, cap: Slot) {
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

    /// The repeats owed at `now`: every recorded message of a slot whose
    /// first send is at least `interval` old, plus every retained
    /// certificate.
    pub fn due(&mut self, now: Timestamp) -> Vec<(Recipients, Slot, S::Message)> {
        let mut due = Vec::new();

        for (slot, undecided) in &self.undecided {
            let Some(age) = now.duration_since(undecided.since) else {
                continue;
            };
            if age < self.config.interval {
                continue;
            }
            due.extend(
                undecided
                    .deliveries
                    .iter()
                    .map(|delivery| (delivery.to, *slot, delivery.message.clone())),
            );
        }

        due.extend(self.finalized.iter_mut().map(|(slot, certificate)| {
            certificate.repeated = true;
            (Recipients::Everyone, *slot, certificate.message.clone())
        }));

        due
    }
}

#[cfg(test)]
mod tests {
    use super::{super::slot::SlotOutput, *};

    const INTERVAL: TimestampDelta = TimestampDelta::from_millis(5_000);

    // A slot consensus whose finalization data is the certificate message
    // itself, so both the Some and the None case are reachable.
    struct TestConsensus;

    impl SlotConsensus for TestConsensus {
        type Config = ();
        type Context = ();
        type Message = u64;
        type Timer = ();
        type OptimisticCommitData = ();
        type FinalizationData = Option<u64>;
        type DAEvent = ();
        type DACommand = ();

        fn new(_slot: Slot, _config: &(), _context: &()) -> Self {
            Self
        }

        fn handle_deadline(&mut self) {}

        fn handle_message(&mut self, _sender: NodeId, _message: u64) {}

        fn handle_timer(&mut self, _timer: ()) {}

        fn poll(&mut self) -> Option<SlotOutput<Self>> {
            None
        }

        fn certificate_message(data: &Option<u64>) -> Option<u64> {
            *data
        }
    }

    fn repeater(certificate_retention: u64) -> Repeater<TestConsensus> {
        Repeater::new(RepeaterConfig {
            interval: INTERVAL,
            certificate_retention,
        })
    }

    fn at(millis: u64) -> Timestamp {
        Timestamp::from_millis(millis)
    }

    #[test]
    fn an_equal_re_record_is_deduped() {
        let mut repeater = repeater(2);
        repeater.record(at(0), Slot(1), Recipients::Everyone, &7);
        repeater.record(at(1_000), Slot(1), Recipients::Everyone, &7);
        repeater.record(at(1_000), Slot(1), Recipients::Node(NodeId::dummy(3)), &7);
        repeater.record(at(1_000), Slot(1), Recipients::Everyone, &8);

        assert_eq!(
            repeater.due(at(5_000)),
            vec![
                (Recipients::Everyone, Slot(1), 7),
                (Recipients::Node(NodeId::dummy(3)), Slot(1), 7),
                (Recipients::Everyone, Slot(1), 8),
            ]
        );
    }

    #[test]
    fn finalization_drops_the_undecided_messages_and_keeps_the_certificate() {
        let mut repeater = repeater(2);
        repeater.record(at(0), Slot(1), Recipients::Everyone, &7);
        repeater.handle_finalization(Slot(1), &Some(42));

        assert_eq!(
            repeater.due(at(5_000)),
            vec![(Recipients::Everyone, Slot(1), 42)]
        );
        // a finalized slot records nothing further
        repeater.record(at(5_000), Slot(1), Recipients::Everyone, &7);
        assert_eq!(
            repeater.due(at(20_000)),
            vec![(Recipients::Everyone, Slot(1), 42)]
        );
    }

    #[test]
    fn a_protocol_without_a_certificate_keeps_nothing() {
        let mut repeater = repeater(2);
        repeater.record(at(0), Slot(1), Recipients::Everyone, &7);
        repeater.handle_finalization(Slot(1), &None);

        assert!(repeater.due(at(5_000)).is_empty());
    }

    #[test]
    fn a_faulted_slot_is_dropped() {
        let mut repeater = repeater(2);
        repeater.record(at(0), Slot(1), Recipients::Everyone, &7);
        repeater.handle_completed(Slot(1));

        assert!(repeater.due(at(5_000)).is_empty());
    }

    #[test]
    fn a_cap_advance_trims_both_maps() {
        let mut repeater = repeater(2);
        for slot in 0..6 {
            repeater.record(at(0), Slot(slot), Recipients::Everyone, &slot);
        }
        for slot in 0..3 {
            repeater.handle_finalization(Slot(slot), &Some(100 + slot));
        }
        // a tick, so every certificate is past its guaranteed repeat
        repeater.due(at(5_000));

        // cap 4: slots below it are closed, certificates survive to cap - 2
        repeater.handle_cap_advance(Slot(4));
        assert_eq!(
            repeater.due(at(5_000)),
            vec![
                (Recipients::Everyone, Slot(4), 4),
                (Recipients::Everyone, Slot(5), 5),
                (Recipients::Everyone, Slot(2), 102),
            ]
        );

        repeater.handle_cap_advance(Slot(5));
        assert_eq!(
            repeater.due(at(5_000)),
            vec![(Recipients::Everyone, Slot(5), 5)]
        );
    }

    #[test]
    fn a_certificate_no_tick_has_sent_yet_outlives_the_cap() {
        let mut repeater = repeater(2);
        repeater.handle_finalization(Slot(1), &Some(101));

        // the cap runs past the retention before the first tick
        repeater.handle_cap_advance(Slot(9));
        assert_eq!(
            repeater.due(at(5_000)),
            vec![(Recipients::Everyone, Slot(1), 101)]
        );

        repeater.handle_cap_advance(Slot(9));
        assert!(repeater.due(at(5_000)).is_empty());
    }

    #[test]
    fn a_slot_is_repeated_only_once_its_first_send_is_interval_old() {
        let mut repeater = repeater(2);
        repeater.record(at(1_000), Slot(1), Recipients::Everyone, &7);
        // a later message of the same slot does not reset the slot's age
        repeater.record(at(3_000), Slot(1), Recipients::Everyone, &8);

        assert!(repeater.due(at(5_999)).is_empty());
        assert_eq!(
            repeater.due(at(6_000)),
            vec![
                (Recipients::Everyone, Slot(1), 7),
                (Recipients::Everyone, Slot(1), 8),
            ]
        );
    }
}
