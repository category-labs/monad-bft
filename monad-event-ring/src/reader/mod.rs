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

use std::marker::PhantomData;

use monad_event::{ffi::monad_event_descriptor, EventDecoder, EventDescriptor, RawEventDescriptor};

pub(crate) use self::raw::RawEventReader;
use crate::{
    ffi::{monad_event_ring_iter, monad_event_ring_iter_reset},
    EventNextResult, EventRing,
};

mod raw;

/// Used to consume events from an [`EventRing`](crate::EventRing).
pub struct EventReader<'ring, D>
where
    D: EventDecoder,
{
    pub(crate) raw: RawEventReader<'ring>,
    _phantom: PhantomData<D>,
}

impl<'ring, D> EventReader<'ring, D>
where
    D: EventDecoder,
{
    pub(crate) fn new(raw: RawEventReader<'ring>) -> Self {
        Self {
            raw,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn new_snapshot(mut raw: RawEventReader<'ring>) -> Self {
        raw.inner.cur_seqno = 1;

        Self {
            raw,
            _phantom: PhantomData,
        }
    }

    /// Produces the next event in the ring.
    pub fn next_descriptor(&mut self) -> EventNextResult<EventDescriptor<&'ring EventRing<D>, D>> {
        self.raw.next_descriptor().map(EventDescriptor::new)
    }

    /// Resets the reader to the latest event in the ring.
    pub fn reset(&mut self) {
        monad_event_ring_iter_reset(&mut self.raw.inner);
    }

    /// Exposes the underlying c-types.
    pub fn with_raw(
        &mut self,
        f: impl FnOnce(&mut monad_event_ring_iter) -> Option<monad_event_descriptor>,
    ) -> Option<EventDescriptor<&'ring EventRing<D>, D>> {
        let c_event_descriptor = f(&mut self.raw.inner)?;

        let raw_event_descriptor = RawEventDescriptor::new(c_event_descriptor, self.raw.event_ring);

        Some(EventDescriptor::new(raw_event_descriptor))
    }
}
