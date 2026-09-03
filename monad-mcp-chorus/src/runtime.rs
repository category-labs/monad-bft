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

use super::{
    conductor::{Conductor, ConductorOutput},
    driver::{CadenceDriver, CadenceEvent, Driver, NodeEvent, WakeId},
    slot::{SlotConsensus, SlotOutput},
    slot_manager::SlotManager,
    types::{Slot, Timestamp, Validated},
};

// A Runtime describes the wiring logic of the three components of the
// consensus stack: slot manager, conductor, and driver.
pub trait Runtime<M> {
    fn init(&mut self);
    fn wake(&mut self, now: Timestamp, wake: WakeId);
    fn receive(&mut self, now: Timestamp, message: Validated<M>);
    fn poll(&mut self) -> Option<NodeEvent<M>>;
}

// A canonical runtime implementation for cadence.
pub struct CadenceRuntime<S, C, D = CadenceDriver<S, C>>
where
    S: SlotConsensus,
    C: Conductor,
    D: Driver<S, C>,
{
    clock: Timestamp,
    slot_manager: SlotManager<S>,
    conductor: C,
    driver: D,
    observer: Option<Box<ObserverOf<S>>>,
}

type ObserverOf<S> = dyn FinalizationObserver<
        <S as SlotConsensus>::OptimisticCommitData,
        <S as SlotConsensus>::FinalizationData,
    >;

impl<S, C> CadenceRuntime<S, C>
where
    S: SlotConsensus,
    C: Conductor,
{
    pub fn new(slot_manager: SlotManager<S>, conductor: C) -> Self {
        Self {
            clock: Timestamp::GENESIS,
            slot_manager,
            conductor,
            driver: CadenceDriver::default(),
            observer: None,
        }
    }

    pub fn with_driver<D>(self, driver: D) -> CadenceRuntime<S, C, D>
    where
        D: Driver<S, C>,
    {
        CadenceRuntime {
            clock: self.clock,
            slot_manager: self.slot_manager,
            conductor: self.conductor,
            observer: self.observer,
            driver,
        }
    }
}

impl<S, C, D> CadenceRuntime<S, C, D>
where
    S: SlotConsensus,
    C: Conductor,
    D: Driver<S, C>,
{
    pub fn on_finalization(
        &mut self,
        observer: impl FinalizationObserver<S::OptimisticCommitData, S::FinalizationData> + 'static,
    ) {
        self.observer = Some(Box::new(observer));
    }

    // in actual runtime this can be controlled by the system clock.
    pub fn advance_clock(&mut self, now: Timestamp) {
        assert!(now >= self.clock);
        self.clock = now;
    }

    fn step(&mut self) {
        while self.step_once() {}
    }

    // returns true if any progress was made
    fn step_once(&mut self) -> bool {
        // todo: use a fair poll order to avoid starvation
        if let Some((slot, out)) = self.slot_manager.poll_any() {
            let now = self.clock;

            match out {
                SlotOutput::ScheduleTimer(delta, timer) => {
                    self.driver.schedule_slot_timer(delta, slot, timer);
                }
                SlotOutput::Broadcast(message) => {
                    self.driver.broadcast_slot(slot, message);
                }
                SlotOutput::Unicast { to, message } => {
                    self.driver.unicast_slot(slot, to, message);
                }
                SlotOutput::CommitOptimistic(data) => {
                    if let Some(observer) = &mut self.observer {
                        observer.handle_optimistic_commit(now, slot, &data);
                    }
                }
                SlotOutput::Finalize(data) => {
                    if let Some(observer) = &mut self.observer {
                        observer.handle_finalization(now, slot, &data);
                    }
                    self.conductor.handle_slot_finalization(now, slot);
                    self.slot_manager.close(slot);
                }
                SlotOutput::Fault { reason } => {
                    tracing::warn!(?slot, reason = %reason, "slot faulted");
                    self.slot_manager.close(slot);
                }
            }
            return true;
        }

        if let Some(out) = self.conductor.poll() {
            match out {
                ConductorOutput::Broadcast(msg) => {
                    self.driver.broadcast_conductor(msg);
                }
                ConductorOutput::ScheduleAlarm(at, timer) => {
                    self.driver.schedule_alarm(at, timer);
                }
                ConductorOutput::CloseSlots { cap } => {
                    self.slot_manager.advance_cap(cap);
                }
                ConductorOutput::OpenSlots(slots) => {
                    for (slot, deadline) in slots {
                        self.slot_manager.open(slot);
                        self.driver.schedule_slot_deadline(slot, deadline);
                    }
                }
            }
            return true;
        }

        if let Some(event) = self.driver.poll_cadence_event() {
            match event {
                CadenceEvent::Alarm(alarm) => {
                    self.conductor.handle_alarm(alarm);
                }
                CadenceEvent::ConductorMessage(message) => {
                    let (message, author) = message.destructure();
                    self.conductor.handle_message(author, message);
                }
                CadenceEvent::SlotDeadline(slot) => {
                    if let Some(instance) = self.slot_manager.slot_instance(slot) {
                        instance.handle_deadline();
                    }
                }
                CadenceEvent::SlotTimer(slot, timer) => {
                    if let Some(instance) = self.slot_manager.slot_instance(slot) {
                        instance.handle_timer(timer);
                    }
                }
                CadenceEvent::SlotMessage(message) => {
                    let ((slot, message), author) = message.destructure();
                    if let Some(instance) = self.slot_manager.slot_instance(slot) {
                        instance.handle_message(author, message);
                    }
                }
            }
            return true;
        }

        false
    }
}

impl<S, C, D> Runtime<D::WireMsg> for CadenceRuntime<S, C, D>
where
    S: SlotConsensus,
    C: Conductor,
    D: Driver<S, C>,
{
    fn poll(&mut self) -> Option<NodeEvent<D::WireMsg>> {
        self.driver.poll_node_event()
    }

    fn init(&mut self) {
        self.step();
    }

    fn wake(&mut self, now: Timestamp, wake: WakeId) {
        self.advance_clock(now);
        self.driver.handle_wake(wake);
        self.step();
    }

    fn receive(&mut self, now: Timestamp, message: Validated<D::WireMsg>) {
        self.advance_clock(now);
        self.driver.handle_message(message);
        self.step();
    }
}

pub trait FinalizationObserver<OD, FD> {
    fn handle_optimistic_commit(&mut self, _now: Timestamp, _slot: Slot, _data: &OD) {}
    fn handle_finalization(&mut self, now: Timestamp, slot: Slot, data: &FD);
}

impl<OD, FD, F> FinalizationObserver<OD, FD> for F
where
    F: FnMut(Timestamp, Slot, &FD),
{
    fn handle_finalization(&mut self, now: Timestamp, slot: Slot, data: &FD) {
        self(now, slot, data)
    }
}

impl<OD, FD> FinalizationObserver<OD, FD> for Vec<(Timestamp, Slot)> {
    fn handle_finalization(&mut self, now: Timestamp, slot: Slot, _data: &FD) {
        self.push((now, slot));
    }
}
