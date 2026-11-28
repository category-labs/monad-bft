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

use std::collections::{HashMap, HashSet};

use super::{
    super::{
        super::types::{Slot, TimestampDelta},
        MVBAOutput, ValidateInput, Votable,
    },
    TimerEvent,
};

/// How often outstanding block requests are re-broadcast: one full
/// request/response round trip
const BLOCK_RETRANSMIT_DELTAS: u64 = 2;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct BlockRequestMsg<V: Votable> {
    pub slot: Slot,
    pub entries: V::Entries,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct BlockResponseMsg<V> {
    pub slot: Slot,
    pub block: V,
}

/// The blocks this instance holds, and the ones it is waiting for
pub(crate) struct BlockStore<V: Votable> {
    slot: Slot,
    /// How long an unanswered request waits before it is re-sent
    retransmit_timeout: TimestampDelta,
    /// Blocks held, keyed by the entries that identify them
    known: HashMap<V::Entries, V>,
    /// Entries requested with no valid response back yet. Each carries its own
    /// [`super::TimerEvent::BlockRetransmit`], keyed by the very entries it
    /// asks for, so no arming state is tracked here: a fire that finds its
    /// entries gone is one whose timer is already dead
    pending: HashSet<V::Entries>,
}

impl<V: ValidateInput + Votable> BlockStore<V> {
    pub(crate) fn new(slot: Slot, delta: TimestampDelta) -> Self {
        Self {
            slot,
            retransmit_timeout: delta
                .checked_mul(BLOCK_RETRANSMIT_DELTAS)
                .expect("block retransmit timeout overflows the timestamp range"),
            known: HashMap::new(),
            pending: HashSet::new(),
        }
    }

    pub(crate) fn remember(&mut self, block: V) {
        let entries = block.entries();
        self.pending.remove(&entries);
        self.known.insert(entries, block);
    }

    pub(crate) fn get(&self, entries: &V::Entries) -> Option<&V> {
        self.known.get(entries)
    }

    /// Ask for the block behind `entries`: the request, and the timer that
    /// keeps re-sending it. Empty when the block is held or a request is
    /// already outstanding
    pub(crate) fn want<M>(
        &mut self,
        entries: &V::Entries,
    ) -> impl Iterator<Item = MVBAOutput<M, TimerEvent<V>>>
    where
        M: From<BlockRequestMsg<V>>,
    {
        let fresh = !self.known.contains_key(entries) && self.pending.insert(entries.clone());
        fresh.then(|| self.fetch(entries)).into_iter().flatten()
    }

    /// One block's retransmit timer fired. Re-sends that request and re-arms,
    /// or lets the timer die when the block arrived or the want was dropped
    pub(crate) fn on_retransmit_timer<M>(
        &self,
        entries: &V::Entries,
    ) -> impl Iterator<Item = MVBAOutput<M, TimerEvent<V>>>
    where
        M: From<BlockRequestMsg<V>>,
    {
        self.pending
            .contains(entries)
            .then(|| self.fetch(entries))
            .into_iter()
            .flatten()
    }

    /// A request for `entries` and the timer that re-drives it. One arming is
    /// live per pending entry: re-arming replaces, since the event is the key
    fn fetch<M>(&self, entries: &V::Entries) -> [MVBAOutput<M, TimerEvent<V>>; 2]
    where
        M: From<BlockRequestMsg<V>>,
    {
        [
            MVBAOutput::Broadcast(
                BlockRequestMsg {
                    slot: self.slot,
                    entries: entries.clone(),
                }
                .into(),
            ),
            MVBAOutput::ScheduleTimer {
                duration: self.retransmit_timeout,
                timer_event: TimerEvent::BlockRetransmit(entries.clone()),
            },
        ]
    }

    pub(crate) fn handle_request(
        &self,
        request: &BlockRequestMsg<V>,
    ) -> Option<BlockResponseMsg<V>> {
        if request.slot != self.slot {
            return None;
        }

        Some(BlockResponseMsg {
            slot: self.slot,
            block: self.get(&request.entries)?.clone(),
        })
    }

    pub(crate) fn handle_response(
        &mut self,
        response: BlockResponseMsg<V>,
        context: &V::Context,
    ) -> bool {
        if response.slot != self.slot {
            return false;
        }

        let entries = response.block.entries();
        if !self.pending.contains(&entries) {
            return false;
        }

        if !response.block.validate(context) {
            return false;
        }

        self.pending.remove(&entries);
        self.known.insert(entries, response.block);
        true
    }

    /// Drop everything unreachable from `keep`. A want that is still live is
    /// re-registered on the next pass through the state machine
    pub(crate) fn gc(&mut self, keep: &HashSet<V::Entries>) {
        self.known.retain(|entries, _| keep.contains(entries));
        self.pending.retain(|entries| keep.contains(entries));
    }
}
