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

//! A runtime wrapper that feeds a node the DA events of a script, and
//! resolves `PullRoot` / `PullHeader` commands after a fixed delay, the way
//! `monad-pq-da` pulls a root from its voters.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use monad_mcp_chorus::{
    spec::ProposalHeader as _,
    stub::{
        CadenceRuntime, DASink, Outbound, Runtime, SlotConsensus, SlotLifecycle,
        types::{
            NodeId, ProposalId, ProposerSchedule, SignedProposalHeader, Slot, Timestamp,
            TimestampDelta, Validated,
        },
    },
};
use monad_pq_chorus::{
    PqMessage,
    da::{ChorusDAEvent, PqDACommand, ProposalDAEvent},
};

use super::{Conductor, Params, Plan, Wire, repeater::Repeater};

type Pending = BTreeMap<(Timestamp, u64), (Slot, ChorusDAEvent)>;

/// Every proposal any node may learn of, and whether it encodes validly.
pub struct Registry {
    proposals: HashMap<ProposalId, (SignedProposalHeader, bool)>,
}

impl Registry {
    pub fn build(params: &Params, plan: &Plan, schedule: &dyn ProposerSchedule) -> Self {
        let mut proposals = HashMap::new();
        for slot in (0..params.slots).map(Slot) {
            for j in occupied(schedule, slot) {
                for node in (0..params.n).map(NodeId::dummy) {
                    for lane in plan(node, slot, j) {
                        proposals.insert(lane.header.pid(), (lane.header, lane.valid));
                    }
                }
            }
        }
        Self { proposals }
    }
}

fn occupied(schedule: &dyn ProposerSchedule, slot: Slot) -> Vec<usize> {
    schedule
        .proposers_at(slot)
        .map(|set| {
            set.iter()
                .filter(|(_, proposer)| proposer.is_some())
                .map(|(j, _)| j)
                .collect()
        })
        .unwrap_or_default()
}

struct CommandSink(Arc<Mutex<Vec<(Slot, PqDACommand)>>>);

impl DASink<PqDACommand> for CommandSink {
    fn handle_lifecycle(&mut self, _event: SlotLifecycle) {}

    fn handle_command(&mut self, slot: Slot, command: PqDACommand) {
        self.0.lock().unwrap().push((slot, command));
    }
}

pub struct ScriptedDa<S: SlotConsensus> {
    inner: CadenceRuntime<S, Conductor>,
    pending: Pending,
    seq: u64,
    commands: Arc<Mutex<Vec<(Slot, PqDACommand)>>>,
    registry: Arc<Registry>,
    pull_delay: TimestampDelta,
    now: Timestamp,
    repeater: Repeater,
    outbox: VecDeque<Outbound<Wire>>,
}

/// The scripted events of `node`, at absolute times.
pub fn script(
    node: NodeId,
    params: &Params,
    plan: &Plan,
    schedule: &dyn ProposerSchedule,
) -> Vec<(Timestamp, Slot, ChorusDAEvent)> {
    let mut events = Vec::new();
    let at = |slot: u64, offset: i64| {
        let ms = params.deadline(slot).as_nanos() as i64 / 1_000_000 + offset;
        Timestamp::from_millis(ms.max(0) as u64)
    };
    for slot in 0..params.slots {
        for j in occupied(schedule, Slot(slot)) {
            for lane in plan(node, Slot(slot), j) {
                let pid = lane.header.pid();
                let mut push = |offset, event| {
                    events.push((at(slot, offset), Slot(slot), ChorusDAEvent { j, event }))
                };
                if let Some(offset) = lane.fulfilled_at {
                    push(offset, ProposalDAEvent::HeaderSeen(lane.header.clone()));
                    push(offset, ProposalDAEvent::ProposerObligationFulfilled(pid));
                }
                if let Some(offset) = lane.decoded_at {
                    push(offset, ProposalDAEvent::HeaderSeen(lane.header.clone()));
                    push(offset, resolution(pid, lane.valid));
                }
            }
        }
    }
    events
}

impl<S> ScriptedDa<S>
where
    S: SlotConsensus<Message = PqMessage, DAEvent = ChorusDAEvent, DACommand = PqDACommand>,
{
    /// `inner` must report halts to the repeater's feed.
    pub fn new(
        mut inner: CadenceRuntime<S, Conductor>,
        repeater: Repeater,
        events: Vec<(Timestamp, Slot, ChorusDAEvent)>,
        registry: Arc<Registry>,
        params: &Params,
    ) -> Self {
        let commands = Arc::new(Mutex::new(Vec::new()));
        inner.on_da(CommandSink(commands.clone()));
        let mut this = Self {
            inner,
            pending: BTreeMap::new(),
            seq: 0,
            commands,
            registry,
            pull_delay: TimestampDelta::from_millis(params.pull_delay),
            now: Timestamp::GENESIS,
            repeater,
            outbox: VecDeque::new(),
        };
        for (at, slot, event) in events {
            this.push(at, slot, event);
        }
        this
    }

    fn push(&mut self, at: Timestamp, slot: Slot, event: ChorusDAEvent) {
        self.seq += 1;
        self.pending.insert((at, self.seq), (slot, event));
    }

    // pulls resolve after the pull delay from the voters' chunks
    fn serve_commands(&mut self) {
        let commands = std::mem::take(&mut *self.commands.lock().unwrap());
        let at = self.now + self.pull_delay;
        for (slot, command) in commands {
            let pids: Vec<(usize, ProposalId, bool)> = match command {
                PqDACommand::PullRoot { j, pid, .. } => vec![(j, pid, true)],
                PqDACommand::PullHeader { j, missing } => missing
                    .into_iter()
                    .map(|(pid, _)| (j, pid, false))
                    .collect(),
                _ => continue,
            };
            for (j, pid, decode) in pids {
                let Some((header, valid)) = self.registry.proposals.get(&pid).cloned() else {
                    continue;
                };
                self.push(
                    at,
                    slot,
                    ChorusDAEvent {
                        j,
                        event: ProposalDAEvent::HeaderSeen(header),
                    },
                );
                if decode {
                    self.push(
                        at,
                        slot,
                        ChorusDAEvent {
                            j,
                            event: resolution(pid, valid),
                        },
                    );
                }
            }
        }
    }

    fn deliver_due(&mut self, now: Timestamp) {
        while let Some(entry) = self.pending.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let (slot, event) = entry.remove();
            self.inner.handle_da_event(now, slot, event);
            self.serve_commands();
        }
    }
}

fn resolution(pid: ProposalId, valid: bool) -> ProposalDAEvent {
    match valid {
        true => ProposalDAEvent::Decoded(pid),
        false => ProposalDAEvent::DecodingFailed(pid),
    }
}

impl<S> Runtime<Wire> for ScriptedDa<S>
where
    S: SlotConsensus<Message = PqMessage, DAEvent = ChorusDAEvent, DACommand = PqDACommand>,
    CadenceRuntime<S, Conductor>: Runtime<Wire, DAEvent = ChorusDAEvent>,
{
    type DAEvent = ChorusDAEvent;

    fn init(&mut self) {
        self.inner.init();
        self.serve_commands();
    }

    fn next_due(&self) -> Option<Timestamp> {
        let da = self.pending.keys().next().map(|(at, _)| *at);
        [self.inner.next_due(), da, self.repeater.next_due()]
            .into_iter()
            .flatten()
            .min()
    }

    fn handle_due(&mut self, now: Timestamp) {
        self.now = now;
        self.deliver_due(now);
        self.inner.handle_due(now);
        self.serve_commands();
        self.outbox.extend(self.repeater.tick(now));
    }

    fn receive(&mut self, now: Timestamp, message: Validated<Wire>) {
        self.now = now;
        self.inner.receive(now, message);
        self.serve_commands();
    }

    fn poll(&mut self) -> Option<Outbound<Wire>> {
        self.inner.poll().or_else(|| self.outbox.pop_front())
    }
}
