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

use crate::{
    ffi::{
        self, monad_evcap_reader, monad_evcap_reader_create, monad_evcap_reader_destroy,
        monad_evcap_reader_next_section, monad_evcap_section_desc,
    },
    EventCaptureEventSection, EventCaptureSectionDescriptor, EventCaptureSectionType,
};

pub struct EventCaptureReader {
    inner: &'static mut monad_evcap_reader,
    last_evcap_section_desc: Option<&'static monad_evcap_section_desc>,
}

impl EventCaptureReader {
    pub(crate) fn new(file: &std::fs::File) -> Result<Self, String> {
        let inner = monad_evcap_reader_create(file, "todo")?;

        Ok(Self {
            inner,
            last_evcap_section_desc: None,
        })
    }

    /// TODO: docs
    ///
    /// # Safety
    ///
    /// todo
    pub unsafe fn new_from_raw(inner: &'static mut monad_evcap_reader) -> Self {
        Self {
            inner,
            last_evcap_section_desc: None,
        }
    }

    pub fn next_section(
        &mut self,
        filter: Option<EventCaptureSectionType>,
    ) -> Option<EventCaptureSectionDescriptor<'_>> {
        let evcap_section_desc: Option<&monad_evcap_section_desc> = unsafe {
            monad_evcap_reader_next_section(
                self.inner,
                filter.map_or(ffi::MONAD_EVCAP_SECTION_NONE, |section_type| {
                    section_type.c_section_type()
                }),
                self.last_evcap_section_desc,
            )
        };

        if let Some(evcap_section_desc) = evcap_section_desc {
            self.last_evcap_section_desc = Some(evcap_section_desc);
        }

        evcap_section_desc.map(|evcap_section_desc| {
            EventCaptureSectionDescriptor::new(self.inner, evcap_section_desc)
        })
    }

    pub fn next_event_section(&mut self) -> Option<EventCaptureEventSection<'_>> {
        let section_descriptor = self.next_section(Some(EventCaptureSectionType::EventBundle))?;

        assert_eq!(
            section_descriptor.section_type(),
            Some(EventCaptureSectionType::EventBundle)
        );

        let event_section = section_descriptor
            .open_event_section()
            .expect("EventBundle section filter specified");

        Some(event_section)
    }
}

impl Drop for EventCaptureReader {
    fn drop(&mut self) {
        unsafe {
            monad_evcap_reader_destroy(self.inner);
        }
    }
}
