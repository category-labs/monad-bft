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

use std::{fs::File, marker::PhantomData, os::fd::AsRawFd, path::Path};

pub(crate) use self::raw::RawEventRing;
pub use self::snapshot::SnapshotEventRing;
use crate::{EventDecoder, EventReader, RawEventReader};

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
            .field("type", &D::ring_content_ctype())
            .finish()
    }
}

impl<D> EventRing<D>
where
    D: EventDecoder,
{
    /// Synchronously creates a new event ring from the provided path.
    pub fn new_from_path(path: impl AsRef<Path>) -> Result<Self, String> {
        let resolved_path = crate::ffi::monad_event_resolve_ring_file(None::<&str>, path)?;
        let ring_file = File::open(&resolved_path).map_err(|e| {
            format!(
                "could not open event ring file `{}`: {}",
                resolved_path.display(),
                e.to_string()
            )
        })?;
        let mmap_prot = libc::PROT_READ;
        let raw = RawEventRing::mmap_from_fd(
            mmap_prot,
            libc::MAP_POPULATE,
            ring_file.as_raw_fd(),
            0,
            resolved_path.to_str().unwrap(),
        )?;
        Self::new(raw)
    }

    pub(crate) fn new(raw: RawEventRing) -> Result<Self, String> {
        raw.check_type::<D>()?;

        Ok(Self {
            raw,
            _phantom: PhantomData,
        })
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
