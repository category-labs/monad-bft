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

use monad_event::{ffi::monad_event_descriptor, RawEventDescriptor, Result};

use crate::{
    ffi::{
        self, monad_event_ring_iter, monad_event_ring_iter_try_next,
        monad_event_ring_iterator_init, monad_event_ring_result,
    },
    ring::RawEventRing,
    EventNextResult,
};

#[derive(Debug)]
pub(crate) struct RawEventReader<'ring> {
    pub(crate) inner: monad_event_ring_iter,
    pub(crate) event_ring: &'ring RawEventRing,
}

impl<'ring> RawEventReader<'ring> {
    pub(crate) fn new(event_ring: &'ring RawEventRing) -> Result<Self> {
        let inner = monad_event_ring_iterator_init(&event_ring.inner)?;

        Ok(Self { inner, event_ring })
    }

    pub(crate) fn next_descriptor(
        &mut self,
    ) -> EventNextResult<RawEventDescriptor<&'ring RawEventRing>> {
        let (c_event_iter_result, c_event_descriptor): (
            monad_event_ring_result,
            monad_event_descriptor,
        ) = monad_event_ring_iter_try_next(&mut self.inner);

        match c_event_iter_result {
            // TODO: SUCCESS should come from monad_event, NOT_READY and GAP from ffi but called MONAD_EVENT_RING_XYZ
            ffi::MONAD_EVENT_RING_SUCCESS => {
                EventNextResult::Ready(RawEventDescriptor::new(c_event_descriptor, self.event_ring))
            }
            ffi::MONAD_EVENT_RING_NOT_READY => EventNextResult::NotReady,
            ffi::MONAD_EVENT_RING_GAP => EventNextResult::Gap,
            _ => panic!(
                "RawEventReader encountered unknown try_next result status {c_event_iter_result}"
            ),
        }
    }
}

unsafe impl<'ring> Send for RawEventReader<'ring> {}
