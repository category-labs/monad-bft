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

use monad_event::{
    ffi::monad_event_descriptor, EventDecoder, EventDescriptor, EventDescriptorRead,
    RawEventDescriptor, RawEventDescriptorInfo,
};

use crate::{
    ffi::{
        self, monad_evcap_event_iter, monad_evcap_event_iter_next, monad_evcap_event_section,
        monad_evcap_event_section_open_iterator, monad_evcap_read_result,
    },
    EventCaptureNextResult,
};

pub struct EventCaptureEventIter<'section, D>
where
    D: EventDecoder,
{
    event_section: &'section monad_evcap_event_section,
    inner: monad_evcap_event_iter,
    _phantom: PhantomData<D>,
}

impl<'section, D> EventCaptureEventIter<'section, D>
where
    D: EventDecoder,
{
    pub(super) fn new(event_section: &'section monad_evcap_event_section) -> Self {
        let inner = monad_evcap_event_section_open_iterator(event_section);

        Self {
            event_section,
            inner,
            _phantom: PhantomData,
        }
    }

    pub fn next_descriptor(
        &mut self,
    ) -> EventCaptureNextResult<EventDescriptor<EventCapturePayload<'section>, D>> {
        let (result, c_event_descriptor, payload): (
            monad_evcap_read_result,
            Option<&'section monad_event_descriptor>,
            Option<&'section [u8]>,
        ) = unsafe { monad_evcap_event_iter_next(&mut self.inner) };

        match result {
            ffi::MONAD_EVCAP_READ_SUCCESS => {
                let raw_payload = EventCaptureRawPayload {
                    event_section: self.event_section,
                    payload: payload.unwrap(),
                };

                let raw_event_descriptor: RawEventDescriptor<EventCaptureRawPayload<'section>> =
                    RawEventDescriptor::new(*c_event_descriptor.unwrap(), raw_payload);

                EventCaptureNextResult::Success(EventDescriptor::new(raw_event_descriptor))
            }
            ffi::MONAD_EVCAP_READ_END => EventCaptureNextResult::End,
            ffi::MONAD_EVCAP_READ_NO_SEQNO => EventCaptureNextResult::NoSeqno,
            _ => unimplemented!(),
        }
    }

    pub fn with_raw(
        &mut self,
        f: impl FnOnce(&mut monad_evcap_event_iter) -> Option<(monad_event_descriptor, &'section [u8])>,
    ) -> Option<EventDescriptor<EventCapturePayload<'section>, D>> {
        let (c_event_descriptor, payload) = f(&mut self.inner)?;

        let raw_payload = EventCaptureRawPayload {
            event_section: self.event_section,
            payload,
        };

        let raw_event_descriptor: RawEventDescriptor<EventCaptureRawPayload<'section>> =
            RawEventDescriptor::new(c_event_descriptor, raw_payload);

        Some(EventDescriptor::new(raw_event_descriptor))
    }
}

unsafe impl<'section, D> Send for EventCaptureEventIter<'section, D> where D: EventDecoder + Send {}
unsafe impl<'section, D> Sync for EventCaptureEventIter<'section, D> where D: EventDecoder + Sync {}

pub struct EventCaptureRawPayload<'section> {
    pub event_section: &'section monad_evcap_event_section,
    pub payload: &'section [u8],
}

pub struct EventCapturePayload<'section> {
    _phantom: PhantomData<EventCaptureRawPayload<'section>>,
}

impl<'section> EventDescriptorRead for EventCapturePayload<'section> {
    type Raw = EventCaptureRawPayload<'section>;

    type Result<T> = T;

    fn try_filter_map<T>(
        this: &RawEventDescriptor<Self::Raw>,
        f: impl FnOnce(RawEventDescriptorInfo, &[u8]) -> T,
    ) -> Self::Result<T> {
        f(this.info(), this.with_inner().1.payload)
    }

    fn result_ok<T>(value: T) -> Self::Result<T> {
        value
    }

    fn result_map<T, U>(result: Self::Result<T>, f: impl FnOnce(T) -> U) -> Self::Result<U> {
        f(result)
    }

    fn result_and_then<T, U>(
        result: Self::Result<T>,
        f: impl FnOnce(T) -> Self::Result<U>,
    ) -> Self::Result<U> {
        f(result)
    }
}
