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

use monad_event::{EventDescriptor, EventDescriptorRead};

#[cfg(feature = "event-ring")]
use crate::ffi;
use crate::{ffi::monad_c_bytes32, ExecEventDecoder, ExecEventType};
#[cfg(feature = "event-ring")]
use crate::{ExecEventReader, ExecEventRing, ExecEventRingDescriptor};

/// Provides utilities for [`ExecEventDecoder`] [`EventDescriptor`]s.
pub trait ExecEventDescriptorExt {
    /// Produces the block number associated with the current [`EventDescriptor`].
    fn get_block_number(&self) -> Option<u64>;

    /// Checks whether the current [`EventDescriptor`] is associated with the provided `block_id`.
    fn get_block_id(&self) -> Option<monad_c_bytes32>;
}

#[cfg(feature = "event-ring")]
impl<'buf> ExecEventDescriptorExt for ExecEventRingDescriptor<'buf> {
    fn get_block_number(&self) -> Option<u64> {
        self.with_raw(|c_event_descriptor, raw_event_ring| {
            raw_event_ring.with_inner(|c_event_ring| {
                ffi::monad_event_ring_get_block_number(c_event_ring, c_event_descriptor)
            })
        })
    }

    fn get_block_id(&self) -> Option<monad_c_bytes32> {
        self.with_raw(|c_event_descriptor, raw_event_ring| {
            raw_event_ring.with_inner(|c_event_ring| {
                ffi::monad_event_ring_get_block_id(c_event_ring, c_event_descriptor)
            })
        })
    }
}

/// Provides utilities for [`ExecEventDecoder`] [`EventReader`]s.
pub trait ExecEventReaderExt<R>
where
    R: EventDescriptorRead,
{
    /// Rewinds the [`EventReader`] to the last consensus event specified by the `filter` argument,
    /// producing an [`EventDescriptor`] to it.
    ///
    /// If this method succeeds, the next call to [`EventReader::next_descriptor`] produces the same
    /// event descriptor. Otherwise, the reader remains unchanged.
    fn consensus_prev(
        &mut self,
        filter: Option<ExecEventType>,
    ) -> Option<EventDescriptor<R, ExecEventDecoder>>;

    /// Rewinds the [`EventReader`] to the last event in the provided `block_number` specified by
    /// the `filter` argument.
    ///
    /// If this method succeeds, the next call to [`EventReader::next_descriptor`] produces the same
    /// event descriptor. Otherwise, the reader remains unchanged.
    fn block_number_prev(
        &mut self,
        block_number: u64,
        filter: Option<ExecEventType>,
    ) -> Option<EventDescriptor<R, ExecEventDecoder>>;

    /// Rewinds the [`EventReader`] to the last event in the provided `block_id` specified by the
    /// `filter` argument.
    ///
    /// If this method succeeds, the next call to [`EventReader::next_descriptor`] produces the same
    /// event descriptor. Otherwise, the reader remains unchanged.
    fn block_id_prev(
        &mut self,
        block_id: &monad_c_bytes32,
        filter: Option<ExecEventType>,
    ) -> Option<EventDescriptor<R, ExecEventDecoder>>;
}

#[cfg(feature = "event-ring")]
impl<'buf> ExecEventReaderExt<&'buf ExecEventRing> for ExecEventReader<'buf> {
    fn consensus_prev(
        &mut self,
        filter: Option<ExecEventType>,
    ) -> Option<ExecEventRingDescriptor<'buf>> {
        self.with_raw(|c_event_ring_iter| {
            ffi::monad_event_ring_iter_consensus_prev(
                c_event_ring_iter,
                filter.map_or(ffi::MONAD_EXEC_NONE, ExecEventType::as_c_event_type),
            )
        })
    }

    fn block_number_prev(
        &mut self,
        block_number: u64,
        filter: Option<ExecEventType>,
    ) -> Option<ExecEventRingDescriptor<'buf>> {
        self.with_raw(|c_event_ring_iter| {
            ffi::monad_event_ring_iter_block_number_prev(
                c_event_ring_iter,
                block_number,
                filter.map_or(ffi::MONAD_EXEC_NONE, ExecEventType::as_c_event_type),
            )
        })
    }

    fn block_id_prev(
        &mut self,
        block_id: &monad_c_bytes32,
        filter: Option<ExecEventType>,
    ) -> Option<ExecEventRingDescriptor<'buf>> {
        self.with_raw(|c_event_ring_iter| {
            ffi::monad_event_ring_iter_block_id_prev(
                c_event_ring_iter,
                block_id,
                filter.map_or(ffi::MONAD_EXEC_NONE, ExecEventType::as_c_event_type),
            )
        })
    }
}
