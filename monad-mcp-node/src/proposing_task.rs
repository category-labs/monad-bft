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

use std::{collections::BTreeMap, future::pending, sync::Arc, time::Instant};

use bytes::Bytes;
use rand::{Rng as _, RngCore as _};
use tokio::task::JoinHandle;
use tracing::{Instrument as _, Span};

use crate::{
    chorus::types::{NodeId, ProposerSchedule, Slot, Timestamp, TimestampDelta},
    config::ProposalConfig,
    epoch::EpochHandle,
    network::Link,
    node::Clock,
};

// random bytes up to the S11 message bound, log-uniform in length:
// every doubling from 1 byte to 1 MiB is equally likely
const MAX_PROPOSAL_LEN: usize = 1 << 20;

// decides when we propose in a slot, and what
pub trait ProposalCreation {
    fn new(epoch_handle: &EpochHandle, config: &ProposalConfig) -> Self;

    fn handle_slot_open(&mut self, now: Timestamp, slot: Slot, deadline: Timestamp);

    // every slot strictly below cap is finalized
    fn handle_cap_advance(&mut self, now: Timestamp, cap: Slot);

    // when poll may next yield something
    fn next_due(&self) -> Option<Timestamp>;

    fn poll(&mut self, now: Timestamp) -> Option<(Slot, Bytes)>;
}

// proposes a fixed offset before the slot's deadline
pub struct OffsetProposalCreation {
    self_id: NodeId,
    proposers: Arc<dyn ProposerSchedule + Send + Sync>,
    offset: TimestampDelta,
    // when to propose, for each open slot we propose in
    due: BTreeMap<Slot, Timestamp>,
}

impl OffsetProposalCreation {
    // now if the offset already passed
    fn propose_at(&self, now: Timestamp, deadline: Timestamp) -> Timestamp {
        let Some(remaining) = deadline.duration_since(now) else {
            return now;
        };
        let Some(lead) = remaining.as_nanos().checked_sub(self.offset.as_nanos()) else {
            return now;
        };
        now.checked_add_delta(TimestampDelta::from_nanos(lead))
            .expect("before the deadline")
    }
}

impl ProposalCreation for OffsetProposalCreation {
    fn new(epoch_handle: &EpochHandle, config: &ProposalConfig) -> Self {
        Self {
            self_id: epoch_handle.self_id,
            proposers: epoch_handle.proposers.clone(),
            offset: config.propose_before_deadline,
            due: BTreeMap::new(),
        }
    }

    fn handle_slot_open(&mut self, now: Timestamp, slot: Slot, deadline: Timestamp) {
        if !matches!(
            self.proposers.proposer_index_at(slot, &self.self_id),
            Ok(Some(_))
        ) {
            return;
        }
        self.due.insert(slot, self.propose_at(now, deadline));
    }

    // cap is exclusive, so slot cap itself may still be open
    fn handle_cap_advance(&mut self, _now: Timestamp, cap: Slot) {
        self.due.retain(|slot, _| *slot >= cap);
    }

    fn next_due(&self) -> Option<Timestamp> {
        self.due.values().min().copied()
    }

    fn poll(&mut self, now: Timestamp) -> Option<(Slot, Bytes)> {
        let (slot, _) = self.due.iter().find(|(_, at)| **at <= now)?;
        let slot = *slot;
        self.due.remove(&slot);
        Some((slot, proposal_message(slot)))
    }
}

fn proposal_message(_slot: Slot) -> Bytes {
    let mut rng = rand::thread_rng();
    let max_bits = (MAX_PROPOSAL_LEN as f64).log2();
    let len = 2f64.powf(rng.gen_range(0.0..=max_bits)).round() as usize;
    let mut message = vec![0u8; len.clamp(1, MAX_PROPOSAL_LEN)];
    rng.fill_bytes(&mut message);
    Bytes::from(message)
}

pub enum ProposingInput {
    SlotOpen(Slot, Timestamp),
    CapAdvance(Slot),
}

pub type ProposingOutput = (Slot, Bytes);

// runs a ProposalCreation: slot facts in, ready proposals out
pub struct ProposingTask<P> {
    clock: Clock,
    creation: P,
    link: Link<ProposingOutput, ProposingInput>,
}

impl<P> ProposingTask<P>
where
    P: ProposalCreation + Send + 'static,
{
    pub fn spawn(
        creation: P,
        clock: Clock,
        link: Link<ProposingOutput, ProposingInput>,
    ) -> JoinHandle<()> {
        let task = Self {
            clock,
            creation,
            link,
        };
        tokio::spawn(task.run().instrument(Span::current()))
    }

    async fn run(mut self) {
        loop {
            let due = self.creation.next_due().map(|at| self.clock.instant_of(at));
            tokio::select! {
                input = self.link.recv() => {
                    let Some(input) = input else {
                        return;
                    };
                    self.handle(input);
                }
                _ = sleep_until(due) => {}
            }
            self.flush();
        }
    }

    fn handle(&mut self, input: ProposingInput) {
        let now = self.clock.now();
        match input {
            ProposingInput::SlotOpen(slot, deadline) => {
                self.creation.handle_slot_open(now, slot, deadline);
            }
            ProposingInput::CapAdvance(cap) => {
                self.creation.handle_cap_advance(now, cap);
            }
        }
    }

    fn flush(&mut self) {
        let now = self.clock.now();
        while let Some(proposal) = self.creation.poll(now) {
            self.link.send(proposal);
        }
    }
}

// pending forever without a due time
async fn sleep_until(due: Option<Instant>) {
    match due {
        Some(at) => tokio::time::sleep_until(at.into()).await,
        None => pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::{super::chorus::types::FixedProposerSchedule, *};

    const OFFSET: TimestampDelta = TimestampDelta::from_millis(500);

    fn me() -> NodeId {
        NodeId::dummy(0)
    }

    fn creation(proposers: Vec<NodeId>) -> OffsetProposalCreation {
        OffsetProposalCreation {
            self_id: me(),
            proposers: Arc::new(FixedProposerSchedule::new(proposers)),
            offset: OFFSET,
            due: BTreeMap::new(),
        }
    }

    // index 1 is ours, index 0 someone else's
    fn proposer() -> OffsetProposalCreation {
        creation(vec![NodeId::dummy(1), me()])
    }

    fn at(millis: u64) -> Timestamp {
        Timestamp::from_millis(millis)
    }

    #[test]
    fn a_proposer_slot_is_due_offset_before_the_deadline() {
        let mut creation = proposer();
        creation.handle_slot_open(at(1_000), Slot(3), at(3_000));
        assert_eq!(creation.next_due(), Some(at(2_500)));
        assert_eq!(creation.poll(at(2_499)), None);
    }

    #[test]
    fn a_non_proposer_slot_has_no_due_time() {
        let mut creation = creation(vec![NodeId::dummy(1), NodeId::dummy(2)]);
        creation.handle_slot_open(at(1_000), Slot(3), at(3_000));
        assert_eq!(creation.next_due(), None);
        assert_eq!(creation.poll(at(10_000)), None);
    }

    #[test]
    fn a_passed_offset_is_due_now() {
        // offset already passed, deadline not yet
        let mut creation = proposer();
        creation.handle_slot_open(at(2_800), Slot(3), at(3_000));
        assert_eq!(creation.next_due(), Some(at(2_800)));

        // deadline already passed
        let mut creation = proposer();
        creation.handle_slot_open(at(3_500), Slot(3), at(3_000));
        assert_eq!(creation.next_due(), Some(at(3_500)));
    }

    #[test]
    fn cap_advance_prunes_strictly_below_the_cap() {
        let mut creation = proposer();
        for slot in 0..4 {
            creation.handle_slot_open(at(0), Slot(slot), at(1_000 * (slot + 1)));
        }

        creation.handle_cap_advance(at(100), Slot(2));
        assert_eq!(
            creation.due.keys().copied().collect::<Vec<_>>(),
            vec![Slot(2), Slot(3)]
        );
        assert_eq!(creation.next_due(), Some(at(2_500)));
    }

    #[test]
    fn poll_releases_a_due_slot_once() {
        let mut creation = proposer();
        creation.handle_slot_open(at(0), Slot(0), at(1_000));
        creation.handle_slot_open(at(0), Slot(1), at(2_000));

        let (slot, message) = creation.poll(at(500)).expect("slot 0 is due");
        assert_eq!(slot, Slot(0));
        assert!(!message.is_empty());
        assert_eq!(creation.poll(at(500)), None);
        assert_eq!(creation.next_due(), Some(at(1_500)));
    }
}
