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

use std::{future::pending, time::Instant};

use bytes::Bytes;
use rand::{Rng as _, RngCore as _};
use tokio::task::JoinHandle;
use tracing::{Instrument as _, Span};

use crate::{
    chorus::{
        proposing::ProposalPlanner,
        types::{ProposalIndex, Slot, Timestamp},
    },
    config::ProposalConfig,
    epoch::EpochHandle,
    network::Link,
    node::Clock,
};

// random bytes up to the S11 message bound, log-uniform in length:
// every doubling from 1 byte to 1 MiB is equally likely
const MAX_PROPOSAL_LEN: usize = 1 << 20;

// decides when we propose in a slot
pub trait ProposalCreation {
    fn new(epoch_handle: &EpochHandle, config: &ProposalConfig) -> Self;

    fn handle_slot_open(&mut self, now: Timestamp, slot: Slot, deadline: Timestamp);

    // every slot strictly below cap is finalized
    fn handle_cap_advance(&mut self, now: Timestamp, cap: Slot);

    // when poll may next yield something
    fn next_due(&self) -> Option<Timestamp>;

    fn poll(&mut self, now: Timestamp) -> Option<(Slot, ProposalIndex)>;
}

impl ProposalCreation for ProposalPlanner {
    fn new(epoch_handle: &EpochHandle, config: &ProposalConfig) -> Self {
        ProposalPlanner::new(
            epoch_handle.self_id,
            epoch_handle.proposers.clone(),
            config.planner(&epoch_handle.proposers),
        )
    }

    fn handle_slot_open(&mut self, now: Timestamp, slot: Slot, deadline: Timestamp) {
        ProposalPlanner::handle_slot_open(self, now, slot, deadline);
    }

    fn handle_cap_advance(&mut self, now: Timestamp, cap: Slot) {
        ProposalPlanner::handle_cap_advance(self, now, cap);
    }

    fn next_due(&self) -> Option<Timestamp> {
        ProposalPlanner::next_due(self)
    }

    fn poll(&mut self, now: Timestamp) -> Option<(Slot, ProposalIndex)> {
        ProposalPlanner::poll(self, now)
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

pub type ProposingOutput = (Slot, ProposalIndex, Bytes);

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

    // the payload is assembled here, not by the creation policy
    fn flush(&mut self) {
        let now = self.clock.now();
        while let Some((slot, index)) = self.creation.poll(now) {
            self.link.send((slot, index, proposal_message(slot)));
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
    use super::*;

    #[test]
    fn a_proposal_message_is_non_empty_and_bounded() {
        for slot in 0..64 {
            let message = proposal_message(Slot(slot));
            assert!(!message.is_empty());
            assert!(message.len() <= MAX_PROPOSAL_LEN);
        }
    }
}
