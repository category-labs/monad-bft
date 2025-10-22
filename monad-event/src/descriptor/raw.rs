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

use crate::ffi::monad_event_descriptor;

/// The raw metadata for an event.
#[derive(Debug)]
pub struct RawEventDescriptor<B>
// where
//     B: EventBuffer,
{
    inner: monad_event_descriptor,
    buffer: B,
}

impl<B> RawEventDescriptor<B> {
    /// TODO: docs
    pub fn new(c_event_descriptor: monad_event_descriptor, buffer: B) -> Self {
        Self {
            inner: c_event_descriptor,
            buffer,
        }
    }

    /// TODO: docs
    pub fn info(&self) -> RawEventDescriptorInfo {
        RawEventDescriptorInfo {
            seqno: self.inner.seqno,
            event_type: self.inner.event_type,
            record_epoch_nanos: self.inner.record_epoch_nanos,
            content_ext: self.inner.content_ext,
        }
    }

    /// TODO: docs
    pub fn with_inner(&self) -> (&monad_event_descriptor, &B) {
        (&self.inner, &self.buffer)
    }
}

/// TODO: docs
pub struct RawEventDescriptorInfo {
    pub(super) seqno: u64,
    pub(super) event_type: u16,
    pub(super) record_epoch_nanos: u64,
    pub(super) content_ext: [u64; 4],
}
