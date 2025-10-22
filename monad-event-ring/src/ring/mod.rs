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

use monad_event::{EventDecoder, EventDescriptorRead, RawEventDescriptor, RawEventDescriptorInfo};

pub use self::{raw::RawEventRing, snapshot::SnapshotEventRing};
use crate::{
    ffi::{monad_event_ring_payload_check, monad_event_ring_payload_peek},
    EventPayloadResult, EventReader, EventRingPath, RawEventReader,
};

mod raw;
mod snapshot;

/// A unified interface for event rings.
pub trait DecodedEventRing {
    /// The decoder used to read events from this event ring.
    type Decoder: EventDecoder;

    /// Produces a reader that produces events from this ring.
    fn create_reader<'ring>(&'ring self) -> EventReader<'ring, Self::Decoder>;
}

/// An event ring created from a file.
pub struct EventRing<D>
where
    D: EventDecoder,
{
    raw: RawEventRing,
    _phantom: PhantomData<D>,
}

impl<D> std::fmt::Debug for EventRing<D>
where
    D: EventDecoder,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventRing")
            .field("raw", &self.raw)
            .field("type", &D::content_type())
            .finish()
    }
}

impl<D> EventRing<D>
where
    D: EventDecoder,
{
    /// Synchronously creates a new event ring from the provided path.
    pub fn new(path: impl AsRef<EventRingPath>) -> Result<Self, String> {
        use std::os::fd::AsRawFd;

        let file = path.as_ref().open().map_err(|err| err.to_string())?;

        let raw = RawEventRing::mmap_from_fd(
            libc::PROT_READ,
            #[cfg(target_os = "linux")]
            libc::MAP_POPULATE,
            #[cfg(not(target_os = "linux"))]
            0,
            file.as_raw_fd(),
            0,
            &path.as_ref().as_error_name(),
        )?;

        Self::new_from_raw(raw)
    }

    pub(crate) fn new_from_raw(raw: RawEventRing) -> Result<Self, String> {
        raw.check_type::<D>()
            .map(|()| Self::new_from_raw_unchecked(raw))
    }

    pub(crate) fn new_from_raw_unchecked(raw: RawEventRing) -> Self {
        Self {
            raw,
            _phantom: PhantomData,
        }
    }
}

impl<'buf, D> EventDescriptorRead for &'buf EventRing<D>
where
    D: EventDecoder,
{
    type Raw = &'buf RawEventRing;

    type Result<T> = EventPayloadResult<T>;

    fn try_filter_map<T>(
        this: &RawEventDescriptor<Self::Raw>,
        f: impl FnOnce(RawEventDescriptorInfo, &[u8]) -> T,
    ) -> Self::Result<T> {
        let info = this.info();

        let (c_event_descriptor, buffer) = this.with_inner();

        let Some(bytes) = monad_event_ring_payload_peek(&buffer.inner, c_event_descriptor) else {
            return EventPayloadResult::Expired;
        };

        let value = f(info, bytes);

        if monad_event_ring_payload_check(&buffer.inner, c_event_descriptor) {
            EventPayloadResult::Ready(value)
        } else {
            EventPayloadResult::Expired
        }
    }

    fn result_ok<T>(value: T) -> Self::Result<T> {
        EventPayloadResult::Ready(value)
    }

    fn result_map<T, U>(result: Self::Result<T>, f: impl FnOnce(T) -> U) -> Self::Result<U> {
        result.map(f)
    }

    fn result_and_then<T, U>(
        result: Self::Result<T>,
        f: impl FnOnce(T) -> Self::Result<U>,
    ) -> Self::Result<U> {
        result.and_then(f)
    }
}

impl<D> DecodedEventRing for EventRing<D>
where
    D: EventDecoder,
{
    type Decoder = D;

    fn create_reader<'ring>(&'ring self) -> EventReader<'ring, Self::Decoder> {
        let raw = RawEventReader::new(&self.raw).unwrap();

        EventReader::new(raw)
    }
}
