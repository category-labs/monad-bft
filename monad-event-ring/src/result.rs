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

/// The result of attempting to retrieve the next event from an [`EventRing`](crate::EventRing).
pub enum EventNextResult<T> {
    /// The next event is available and produced through `T`.
    Ready(T),

    /// The next event is not available.
    NotReady,

    /// The next event was lost due to a gap.
    ///
    /// Receiving this variant is a strong indicator that downstream consumers must switch to a
    /// recovery phase to backfill the data lost from the missing events. You should **not** ignore
    /// this variant unless you are aware of its implications. See
    /// [`EventReader`](crate::EventReader) for more details.
    Gap,
}

impl<T> EventNextResult<T> {
    /// TODO: docs
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> EventNextResult<U> {
        match self {
            EventNextResult::Ready(descriptor) => EventNextResult::Ready(f(descriptor)),
            EventNextResult::NotReady => EventNextResult::NotReady,
            EventNextResult::Gap => EventNextResult::Gap,
        }
    }
}

/// The result of attempting to read the payload from an
/// [`EventDescriptor`](crate::EventDescriptor).
#[derive(Debug)]
pub enum EventPayloadResult<T> {
    /// The payload was successfully retrieved.
    Ready(T),

    /// The payload's bytes were overwritten while reading them and the result is thus invalid.
    Expired,
}

impl<T> EventPayloadResult<T> {
    /// Maps the event descriptor [`Payload`](EventPayloadResult::Ready) variant to another type
    /// using the provided lambda.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> EventPayloadResult<U> {
        match self {
            EventPayloadResult::Ready(payload) => EventPayloadResult::Ready(f(payload)),
            EventPayloadResult::Expired => EventPayloadResult::Expired,
        }
    }

    /// TODO: docs
    pub fn and_then<U>(self, f: impl FnOnce(T) -> EventPayloadResult<U>) -> EventPayloadResult<U> {
        match self {
            EventPayloadResult::Ready(payload) => f(payload),
            EventPayloadResult::Expired => EventPayloadResult::Expired,
        }
    }
}
