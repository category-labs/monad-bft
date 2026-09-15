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
    chorus::{
        SlotLifecycle,
        types::{NodeId, Slot, Timestamp, TimestampDelta},
    },
    config::ProposalConfig,
    da::ProposerElection as _,
    epoch::{EpochHandle, StubElection},
    network::Link,
    node::Clock,
};

// random bytes up to the S11 message bound, log-uniform in length:
// every doubling from 1 byte to 1 MiB is equally likely
const MAX_PROPOSAL_LEN: usize = 1 << 20;

// decides when we propose in a slot, and what
pub trait ProposalCreation {
    fn new(epoch_handle: &EpochHandle, config: &ProposalConfig) -> Self;

    fn handle_slot_lifecycle(&mut self, now: Timestamp, slot: Slot, event: SlotLifecycle);

    // when poll may next yield something
    fn next_due(&self) -> Option<Timestamp>;

    fn poll(&mut self, now: Timestamp) -> Option<(Slot, Bytes)>;
}

// proposes a fixed offset before the slot's deadline
pub struct OffsetProposalCreation {
    self_id: NodeId,
    election: Arc<StubElection>,
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
            election: epoch_handle.election.clone(),
            offset: config.propose_before_deadline,
            due: BTreeMap::new(),
        }
    }

    fn handle_slot_lifecycle(&mut self, now: Timestamp, slot: Slot, event: SlotLifecycle) {
        match event {
            SlotLifecycle::Opened { deadline } => {
                if self.election.get_index(slot, &self.self_id).is_none() {
                    return;
                }
                self.due.insert(slot, self.propose_at(now, deadline));
            }
            SlotLifecycle::Completed => {
                self.due.remove(&slot);
            }
        }
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
    Lifecycle(Slot, SlotLifecycle),
}

pub type ProposingOutput = (Slot, Bytes);

// runs a ProposalCreation: lifecycle in, ready proposals out
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
            ProposingInput::Lifecycle(slot, event) => {
                self.creation.handle_slot_lifecycle(now, slot, event);
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
