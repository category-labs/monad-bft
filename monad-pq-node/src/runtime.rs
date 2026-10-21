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

use std::collections::BTreeSet;

use monad_mcp_chorus::{
    ledger::Tx,
    spec::{Deserializable as _, Serializable as _},
};
use monad_mcp_node::EpochHandle;
use monad_pq_chorus::{Evidence, PqEvent, PqMessage};

use crate::{
    chorus::{
        CadenceMessage, SlotLifecycle,
        types::{NodeId, Slot, Timestamp, TimestampDelta, Validated},
    },
    component::{
        Dispatch, PqCadenceInput, PqCadenceOutput, PqCadenceWireMsg, PqDAInput, ProposingInput,
        ProposingOutput, Recipients, RepeaterInput, RepeaterOutput, SharedMempool,
    },
    da::{
        self, AssembledProposal, ChunkRecoveryRequest, DAOutput, Dissemination, read_envelope,
        write_envelope,
    },
    finalization::{PqFinal, PqFinalizationCollector, PqFinalizedSlot},
    network::{Inbound, Outbound, Packet},
    wire::{SlotWire, Unfragmented},
};

// everything the wiring can emit, named by where it goes
pub enum PqEffect {
    Cadence(PqCadenceInput),
    DA(PqDAInput),
    Proposing(ProposingInput),
    Repeater(RepeaterInput<PqMessage>),
    Network(Outbound),
    Ledger(PqFinalizedSlot),
    Evidence(Slot, Evidence),
}

// the wiring: one component's output in, the effects that follow out.
// A scenario wraps the node runtime to tamper with the effects.
pub trait PqRuntime {
    fn handle_inbound(
        &mut self,
        now: Timestamp,
        inbound: Inbound,
        effects: &mut impl Dispatch<PqEffect>,
    );
    fn handle_cadence(&mut self, output: PqCadenceOutput, effects: &mut impl Dispatch<PqEffect>);
    fn handle_da(
        &mut self,
        now: Timestamp,
        output: DAOutput,
        effects: &mut impl Dispatch<PqEffect>,
    );
    fn handle_proposal(
        &mut self,
        now: Timestamp,
        proposal: ProposingOutput,
        effects: &mut impl Dispatch<PqEffect>,
    );
    fn handle_repeat(
        &mut self,
        repeat: RepeaterOutput<PqMessage>,
        effects: &mut impl Dispatch<PqEffect>,
    );
}

// M1 (§3.4): the planner's chaining gate follows the contiguous prefix of
// locally finalized slots, lifted by the conductor's cap for slots a cap
// jump skipped. The value fed only grows.
pub struct ChainGate {
    finalized: BTreeSet<Slot>,
    prefix: Slot,
    conductor: Slot,
    fed: Slot,
}

impl Default for ChainGate {
    fn default() -> Self {
        Self {
            finalized: BTreeSet::new(),
            prefix: Slot::MIN,
            conductor: Slot::MIN,
            fed: Slot::MIN,
        }
    }
}

impl ChainGate {
    // the new gate cap, if it grew
    pub fn on_final(&mut self, slot: Slot) -> Option<Slot> {
        if slot >= self.prefix {
            self.finalized.insert(slot);
        }
        self.advance()
    }

    pub fn on_conductor_cap(&mut self, cap: Slot) -> Option<Slot> {
        self.conductor = self.conductor.max(cap);
        self.advance()
    }

    fn advance(&mut self) -> Option<Slot> {
        self.prefix = self.prefix.max(self.conductor);
        self.finalized = self.finalized.split_off(&self.prefix);
        while self.finalized.remove(&self.prefix) {
            self.prefix = Slot(self.prefix.0 + 1);
        }
        (self.prefix > self.fed).then(|| {
            self.fed = self.prefix;
            self.prefix
        })
    }
}

pub struct PqNodeRuntime {
    epoch_handle: EpochHandle,
    da_handle: da::EpochHandle,
    collector: PqFinalizationCollector,
    gate: ChainGate,
    // None: the node proposes random payloads
    mempool: Option<SharedMempool>,
    wire: Box<dyn SlotWire>,
}

impl PqNodeRuntime {
    pub fn new(
        epoch_handle: EpochHandle,
        da_handle: da::EpochHandle,
        mempool: Option<SharedMempool>,
    ) -> Self {
        Self {
            epoch_handle,
            da_handle,
            collector: PqFinalizationCollector::default(),
            gate: ChainGate::default(),
            mempool,
            wire: Box::new(Unfragmented),
        }
    }

    pub fn epoch_handle(&self) -> &EpochHandle {
        &self.epoch_handle
    }

    pub fn da_handle(&self) -> &da::EpochHandle {
        &self.da_handle
    }

    pub fn mempool(&self) -> Option<&SharedMempool> {
        self.mempool.as_ref()
    }

    fn admit(&self, now: Timestamp, from: NodeId, tx: Tx) {
        let Some(mempool) = &self.mempool else {
            tracing::debug!(?from, "no mempool, tx dropped");
            return;
        };
        match mempool.lock().admit(tx, now.as_nanos() as u64) {
            Ok(()) => tracing::debug!(?from, "tx admitted"),
            Err(reject) => tracing::debug!(?from, %reject, "tx dropped"),
        }
    }

    // §7: slot broadcasts are kept alive by the slot itself, so nothing
    // is recorded in the Repeater
    fn handle_cadence_outbound(
        &mut self,
        event: crate::chorus::Outbound<PqCadenceWireMsg>,
        effects: &mut impl Dispatch<PqEffect>,
    ) {
        match event {
            crate::chorus::Outbound::Broadcast(message) => {
                for payload in self.wire.encode(&message) {
                    let packet = Packet::Cadence(payload);
                    effects.dispatch(PqEffect::Network(Outbound::Broadcast(packet)));
                }
                self.loopback(message, effects);
            }

            crate::chorus::Outbound::Unicast(to, message) => {
                if to == self.epoch_handle.self_id {
                    self.loopback(message, effects);
                    return;
                }
                for payload in self.wire.encode(&message) {
                    let packet = Packet::Cadence(payload);
                    effects.dispatch(PqEffect::Network(Outbound::Unicast(to, packet)));
                }
            }
        }
    }

    // a message we authored needs no validation
    fn loopback(&self, message: PqCadenceWireMsg, effects: &mut impl Dispatch<PqEffect>) {
        let message = Validated::new_unchecked(message, self.epoch_handle.self_id);
        effects.dispatch(PqEffect::Cadence(PqCadenceInput::Message(message)));
    }

    fn handle_event(
        &mut self,
        now: Timestamp,
        slot: Slot,
        event: PqEvent,
        effects: &mut impl Dispatch<PqEffect>,
    ) {
        match event {
            PqEvent::Speculative { .. } => {
                tracing::debug!(slot = slot.0, at = now.as_nanos(), "speculated");
                self.collector.handle_speculative(now, slot);
            }
            PqEvent::Final {
                metablock,
                path,
                at_view,
                path_votes,
            } => {
                let committed = metablock
                    .as_ref()
                    .into_iter()
                    .filter(|e| e.root().is_some());
                tracing::debug!(
                    slot = slot.0,
                    ?path,
                    committed = committed.count(),
                    "pq final"
                );
                if let Some(mempool) = &self.mempool {
                    mempool.lock().settle(slot, |j| {
                        j < metablock.size() && metablock[j].root().is_some()
                    });
                }
                if let Some(cap) = self.gate.on_final(slot) {
                    effects.dispatch(PqEffect::Proposing(ProposingInput::CapAdvance(cap)));
                }
                let finalized = PqFinal {
                    metablock,
                    path,
                    at_view,
                    path_votes,
                };
                self.collector.handle_final(now, slot, finalized);
                self.deliver_finalized(effects);
            }
            PqEvent::Evidence(evidence) => {
                effects.dispatch(PqEffect::Evidence(slot, evidence));
            }
        }
    }

    fn deliver_finalized(&mut self, effects: &mut impl Dispatch<PqEffect>) {
        while let Some(finalized) = self.collector.poll() {
            effects.dispatch(PqEffect::Ledger(finalized));
        }
    }

    fn send_slot_message(
        &mut self,
        to: Recipients,
        message: PqCadenceWireMsg,
        effects: &mut impl Dispatch<PqEffect>,
    ) {
        for payload in self.wire.encode(&message) {
            let packet = Packet::Cadence(payload);
            let outbound = match to {
                Recipients::Everyone => Outbound::Broadcast(packet),
                Recipients::Node(to) => Outbound::Unicast(to, packet),
            };
            effects.dispatch(PqEffect::Network(outbound));
        }
    }
}

impl PqRuntime for PqNodeRuntime {
    fn handle_inbound(
        &mut self,
        now: Timestamp,
        inbound: Inbound,
        effects: &mut impl Dispatch<PqEffect>,
    ) {
        let Inbound { from, packet } = inbound;
        match packet {
            Packet::Cadence(bytes) => {
                let Some(message) = self.wire.decode(from, bytes, now) else {
                    return;
                };
                // D12: the transport authenticates the sender
                let message = Validated::new_unchecked(message, from);
                effects.dispatch(PqEffect::Cadence(PqCadenceInput::Message(message)));
            }

            Packet::Chunk(bytes) => {
                let Ok(envelope) = read_envelope(bytes) else {
                    tracing::debug!(?from, "malformed chunk packet");
                    return;
                };
                effects.dispatch(PqEffect::DA(PqDAInput::Envelope(from, envelope)));
            }

            Packet::ChunkRequest(bytes) => {
                let Ok(request) = ChunkRecoveryRequest::deserialize(&bytes) else {
                    tracing::debug!(?from, "malformed chunk request");
                    return;
                };
                effects.dispatch(PqEffect::DA(PqDAInput::ChunkRequest(from, request)));
            }

            Packet::Tx(bytes) => {
                let tx = match Tx::decode_exact(&bytes) {
                    Ok(tx) => tx,
                    Err(error) => {
                        tracing::debug!(?from, %error, "malformed tx");
                        return;
                    }
                };
                self.admit(now, from, tx);
            }
        }
    }

    fn handle_cadence(&mut self, output: PqCadenceOutput, effects: &mut impl Dispatch<PqEffect>) {
        match output {
            PqCadenceOutput::Outbound(event) => {
                self.handle_cadence_outbound(event, effects);
            }

            PqCadenceOutput::Lifecycle(event) => {
                effects.dispatch(PqEffect::DA(PqDAInput::Lifecycle(event)));
                if let (SlotLifecycle::CapAdvance { cap }, Some(mempool)) = (event, &self.mempool) {
                    mempool.lock().expire_below(cap);
                }
                match event {
                    SlotLifecycle::Opened { slot, deadline } => {
                        let input = ProposingInput::SlotOpen(slot, deadline);
                        effects.dispatch(PqEffect::Proposing(input));
                    }
                    SlotLifecycle::Completed { slot } => {
                        effects.dispatch(PqEffect::Repeater(RepeaterInput::Completed(slot)));
                    }
                    SlotLifecycle::CapAdvance { .. } => {}
                }
                self.collector.handle_lifecycle(event);
            }

            PqCadenceOutput::DACommand(slot, command) => {
                effects.dispatch(PqEffect::DA(PqDAInput::Command(slot, command)));
            }

            PqCadenceOutput::Event(now, slot, event) => {
                self.handle_event(now, slot, event, effects);
            }

            // §7: after halt only Finalized stays alive, in the Repeater
            PqCadenceOutput::Halted(now, slot, halt) => {
                tracing::info!(slot = slot.0, at = now.as_nanos(), "halted");
                let message = halt.finalized_message();
                effects.dispatch(PqEffect::Repeater(RepeaterInput::Finalization(
                    slot, message,
                )));
            }

            PqCadenceOutput::CapAdvance(now, cap) => {
                tracing::debug!(cap = cap.0, at = now.as_nanos(), "chain advanced");
                effects.dispatch(PqEffect::Repeater(RepeaterInput::CapAdvance(cap)));
                if let Some(cap) = self.gate.on_conductor_cap(cap) {
                    effects.dispatch(PqEffect::Proposing(ProposingInput::CapAdvance(cap)));
                }
            }
        }
    }

    fn handle_repeat(
        &mut self,
        (to, slot, message): RepeaterOutput<PqMessage>,
        effects: &mut impl Dispatch<PqEffect>,
    ) {
        let message: PqCadenceWireMsg = CadenceMessage::Slot(slot, message);
        self.send_slot_message(to, message, effects);
    }

    // a proposal released at (slot, index): the first hop goes to the
    // other owners, plus all chunks to our own DA
    fn handle_proposal(
        &mut self,
        now: Timestamp,
        (slot, index, message): ProposingOutput,
        effects: &mut impl Dispatch<PqEffect>,
    ) {
        let self_id = self.epoch_handle.self_id;
        let proposal =
            AssembledProposal::build_s11(&self.da_handle, slot, index, &message, unix_seconds(now));
        let Some(proposal) = proposal else {
            tracing::warn!(
                ?slot,
                len = message.len(),
                "message does not fit the scheme"
            );
            return;
        };

        tracing::info!(slot = slot.0, index, len = message.len(), "proposing");

        let mut first_hop = proposal.disseminate();
        if let Some(envelope) = first_hop.split_off(&self_id) {
            effects.dispatch(PqEffect::DA(PqDAInput::Envelope(self_id, envelope)));
        }
        // ingest every chunk at once, for an early decode
        let envelope = first_hop.envelope();
        effects.dispatch(PqEffect::DA(PqDAInput::Envelope(self_id, envelope)));

        for (to, packet) in first_hop.into_packets() {
            let packet = Packet::Chunk(packet);
            effects.dispatch(PqEffect::Network(Outbound::Unicast(*to, packet)));
        }
    }

    fn handle_da(
        &mut self,
        now: Timestamp,
        output: DAOutput,
        effects: &mut impl Dispatch<PqEffect>,
    ) {
        match output {
            DAOutput::Consensus(slot, event) => {
                effects.dispatch(PqEffect::Cadence(PqCadenceInput::DAEvent(slot, event)));
            }
            DAOutput::Resolved {
                slot,
                proposal_index,
                pid,
                message,
            } => {
                self.collector
                    .handle_resolved(now, slot, proposal_index, pid, message);
                self.deliver_finalized(effects);
            }
            DAOutput::Disseminate(Dissemination { to, envelope }) => {
                let packets = write_envelope(&envelope);
                for node in to {
                    for packet in &packets {
                        let packet = Packet::Chunk(packet.clone());
                        effects.dispatch(PqEffect::Network(Outbound::Unicast(node, packet)));
                    }
                }
            }
            DAOutput::RecoveryRequest { to, request } => {
                let packet = Packet::ChunkRequest(request.serialize());
                effects.dispatch(PqEffect::Network(Outbound::Unicast(to, packet)));
            }
        }
    }
}

// the header's unix_ts, in seconds as monad-mcp-node stamps it
pub fn unix_seconds(now: Timestamp) -> u64 {
    let nanos_per_second = u128::from(TimestampDelta::NANOS_PER_MILLISECOND) * 1_000;
    (now.as_nanos() / nanos_per_second) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_gate_follows_the_contiguous_local_prefix() {
        let mut gate = ChainGate::default();
        assert_eq!(gate.on_final(Slot(1)), None);
        assert_eq!(gate.on_final(Slot(0)), Some(Slot(2)));
        assert_eq!(gate.on_final(Slot(3)), None);
        assert_eq!(gate.on_final(Slot(2)), Some(Slot(4)));
        // a duplicate or stale final changes nothing
        assert_eq!(gate.on_final(Slot(2)), None);
    }

    #[test]
    fn the_conductor_cap_lifts_the_gate_over_skipped_slots() {
        let mut gate = ChainGate::default();
        assert_eq!(gate.on_final(Slot(6)), None);
        assert_eq!(gate.on_conductor_cap(Slot(5)), Some(Slot(5)));
        assert_eq!(gate.on_final(Slot(5)), Some(Slot(7)));
        // a cap behind the local prefix is ignored
        assert_eq!(gate.on_conductor_cap(Slot(3)), None);
        assert_eq!(gate.on_final(Slot(7)), Some(Slot(8)));
        assert_eq!(gate.on_conductor_cap(Slot(10)), Some(Slot(10)));
    }
}
