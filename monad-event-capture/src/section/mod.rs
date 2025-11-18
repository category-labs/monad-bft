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

use monad_event::{EventDecoder, Result};

pub use self::iter::{EventCaptureEventIter, EventCapturePayload};
use crate::ffi::{
    self, monad_evcap_event_section, monad_evcap_event_section_close,
    monad_evcap_event_section_open, monad_evcap_reader, monad_evcap_section_desc,
    monad_evcap_section_type,
};

mod iter;

#[derive(Debug, PartialEq, Eq)]
pub enum EventCaptureSectionType {
    Schema,
    EventBundle,
    SeqnoIndex,
    PackIndex,
}

impl EventCaptureSectionType {
    pub fn new(c_section_type: monad_evcap_section_type) -> Option<Self> {
        Some(match c_section_type {
            ffi::MONAD_EVCAP_SECTION_SCHEMA => Self::Schema,
            ffi::MONAD_EVCAP_SECTION_EVENT_BUNDLE => Self::EventBundle,
            ffi::MONAD_EVCAP_SECTION_SEQNO_INDEX => Self::SeqnoIndex,
            ffi::MONAD_EVCAP_SECTION_PACK_INDEX => Self::PackIndex,
            _ => return None,
        })
    }

    pub fn c_section_type(&self) -> monad_evcap_section_type {
        match self {
            EventCaptureSectionType::Schema => ffi::MONAD_EVCAP_SECTION_SCHEMA,
            EventCaptureSectionType::EventBundle => ffi::MONAD_EVCAP_SECTION_EVENT_BUNDLE,
            EventCaptureSectionType::SeqnoIndex => ffi::MONAD_EVCAP_SECTION_SEQNO_INDEX,
            EventCaptureSectionType::PackIndex => ffi::MONAD_EVCAP_SECTION_PACK_INDEX,
        }
    }
}

pub struct EventCaptureSectionDescriptor<'reader> {
    reader: &'reader monad_evcap_reader,
    inner: &'reader monad_evcap_section_desc,
}

impl<'reader> EventCaptureSectionDescriptor<'reader> {
    pub(crate) fn new(
        reader: &'reader monad_evcap_reader,
        inner: &'reader monad_evcap_section_desc,
    ) -> Self {
        Self { reader, inner }
    }

    pub fn section_type(&self) -> Option<EventCaptureSectionType> {
        EventCaptureSectionType::new(self.inner.type_)
    }

    pub fn open_event_section(self) -> Result<EventCaptureEventSection<'reader>> {
        EventCaptureEventSection::new(self.reader, self.inner)
    }
}

pub struct EventCaptureEventSection<'reader> {
    inner: monad_evcap_event_section,
    _phantom: PhantomData<&'reader ()>,
}

impl<'reader> EventCaptureEventSection<'reader> {
    fn new(
        reader: &'reader monad_evcap_reader,
        section_desc: &monad_evcap_section_desc,
    ) -> Result<Self> {
        let inner = monad_evcap_event_section_open(reader, section_desc)?;

        Ok(Self {
            inner,
            _phantom: PhantomData,
        })
    }

    pub fn open_iterator<'section, D: EventDecoder>(
        &'section self,
    ) -> EventCaptureEventIter<'section, D> {
        EventCaptureEventIter::new(&self.inner)
    }
}

impl<'reader> Drop for EventCaptureEventSection<'reader> {
    fn drop(&mut self) {
        unsafe { monad_evcap_event_section_close(&mut self.inner) };
    }
}

unsafe impl<'reader> Send for EventCaptureEventSection<'reader> {}
unsafe impl<'reader> Sync for EventCaptureEventSection<'reader> {}
