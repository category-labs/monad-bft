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

use std::{
    collections::{BTreeMap, VecDeque},
    ops::Range,
    sync::Arc,
};

use super::{
    chunk::{Chunk, ProposalEnvelope},
    header::InvalidProposalHeader,
    slot_rc::SlotRaptorcast,
    types::{
        ChorusDACommand, ChorusDAEvent, HeaderAuth, NodeId, ProposalKeyPair, Slot, SlotLifecycle,
        ValidatorData,
    },
    util::SlotCompletion,
};

// todo: give this type a more proper name. it encodes the concept of
// slot-scoped config, the identity and validator set. think about if
// we can further refine this concept to have a clearer boundary. note
// this is almost the same shape as ChorusContext, so maybe we can
// just share the type.
#[derive(Clone)]
pub struct EpochHandle {
    pub self_id: NodeId,
    pub num_proposals: usize,
    pub key_pair: Arc<ProposalKeyPair>,
    pub header_auth: Arc<HeaderAuth>,
    // todo: only this field is slot-scoped, should we isolate it out?
    pub validator_data: Arc<ValidatorData>,
}

pub struct DAConfig {
    // keep completed slots for additional time (measured in slots),
    // still ingesting chunks and serving peer chunk recovery requests.
    pub completed_slot_retention: u64,
}

pub struct DARuntime {
    config: DAConfig,
    // todo: make epoch_handle slot dependent
    epoch_handle: EpochHandle,
    raptorcast_map: BTreeMap<Slot, SlotRaptorcast>,

    // inclusive start, exclusive end
    ingestion_window: Range<Slot>,
    slot_completion: SlotCompletion,

    outbox: VecDeque<DAOutput>,
}

impl DARuntime {
    pub fn new(config: DAConfig, epoch_handle: EpochHandle) -> Self {
        Self {
            config,
            epoch_handle,
            raptorcast_map: Default::default(),
            // todo: set the lower bound with finalization certificate
            ingestion_window: Slot::MIN..Slot::MIN,
            slot_completion: SlotCompletion::new(),
            outbox: Default::default(),
        }
    }

    pub fn ingest_chunk(&mut self, chunk: Chunk) -> Result<(), InvalidProposalHeader> {
        let envelope = ProposalEnvelope::from_chunk(chunk);
        self.ingest(envelope)
    }

    pub fn ingest(&mut self, envelope: ProposalEnvelope) -> Result<(), InvalidProposalHeader> {
        let slot = envelope.header().slot;
        let Some(slot_raptorcast) = self.open_slot_raptorcast(slot) else {
            return Err(InvalidProposalHeader::SlotOutOfRange);
        };

        slot_raptorcast.ingest(envelope)?;
        self.collect(slot);
        Ok(())
    }

    // move the slot's pending events to the outbox
    fn collect(&mut self, slot: Slot) {
        let Some(slot_raptorcast) = self.raptorcast_map.get_mut(&slot) else {
            return;
        };

        for event in slot_raptorcast.drain_events() {
            self.outbox.push_back(DAOutput::Consensus(slot, event));
        }
    }

    // the slot's raptorcast, created on first use. None once the slot
    // is outside the ingestion window.
    fn open_slot_raptorcast(&mut self, slot: Slot) -> Option<&mut SlotRaptorcast> {
        if !self.ingestion_window.contains(&slot) {
            return None;
        }

        let slot_raptorcast = self
            .raptorcast_map
            .entry(slot)
            .or_insert_with(|| SlotRaptorcast::new(&self.epoch_handle, slot));
        Some(slot_raptorcast)
    }

    pub fn handle_slot_event(&mut self, slot: Slot, event: SlotLifecycle) {
        match event {
            SlotLifecycle::Opened => {
                let open_ingestion_slot = slot
                    .checked_next()
                    .unwrap_or(Slot::MAX_CAP)
                    .max(self.ingestion_window.end);
                self.ingestion_window.end = open_ingestion_slot;
            }
            SlotLifecycle::Completed => {
                self.slot_completion.mark_completed(slot);
                self.close_slots();
            }
        }
    }

    pub fn handle_command(&mut self, slot: Slot, command: ChorusDACommand) {
        let Some(slot_raptorcast) = self.open_slot_raptorcast(slot) else {
            return;
        };
        let outputs = slot_raptorcast.handle_command(command);
        self.outbox.extend(outputs);
        self.collect(slot);
    }

    pub fn poll(&mut self) -> Option<DAOutput> {
        self.outbox.pop_front()
    }

    // drop the completed slots past retention and stop ingesting for
    // them
    fn close_slots(&mut self) {
        let kept_slot_cap = self
            .slot_completion
            .cap()
            .checked_sub(self.config.completed_slot_retention)
            .unwrap_or(Slot::MIN);

        self.ingestion_window.start = kept_slot_cap;
        self.raptorcast_map.retain(|&slot, _| slot >= kept_slot_cap);
    }
}

pub enum DAOutput {
    // notification for the consensus layer's slot instance
    Consensus(Slot, ChorusDAEvent),
}
