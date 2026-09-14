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

use tokio::task::JoinHandle;
use tracing::{Instrument as _, Span};

use crate::{
    chorus::{
        SlotLifecycle,
        slot::chorus::ChorusDACommand,
        types::{NodeId, Slot},
    },
    da::{ChunkRecoveryRequest, DAOutput, DARuntime, ProposalEnvelope},
    epoch::StubElection,
    network::Link,
};

pub enum DAInput {
    Envelope(ProposalEnvelope),
    ChunkRequest(NodeId, ChunkRecoveryRequest),
    Lifecycle(Slot, SlotLifecycle),
    Command(Slot, ChorusDACommand),
}

pub struct DATask {
    da: DARuntime<StubElection>,
    link: Link<DAOutput, DAInput>,
}

impl DATask {
    pub fn spawn(da: DARuntime<StubElection>, link: Link<DAOutput, DAInput>) -> JoinHandle<()> {
        let task = Self { da, link };
        tokio::spawn(task.run().instrument(Span::current()))
    }

    async fn run(mut self) {
        while let Some(input) = self.link.recv().await {
            self.handle(input);
            self.flush();
        }
    }

    fn handle(&mut self, input: DAInput) {
        match input {
            DAInput::Envelope(envelope) => {
                if let Err(reason) = self.da.ingest(envelope) {
                    tracing::debug!(?reason, "rejected proposal envelope");
                }
            }
            DAInput::ChunkRequest(from, request) => {
                self.da.handle_chunk_request(&from, request);
            }
            DAInput::Lifecycle(slot, event) => {
                self.da.handle_slot_event(slot, event);
            }
            DAInput::Command(slot, command) => {
                self.da.handle_command(slot, command);
            }
        }
    }

    fn flush(&mut self) {
        while let Some(output) = self.da.poll() {
            self.link.send(output);
        }
    }
}
