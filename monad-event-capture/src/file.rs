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

use std::{fs::File, path::Path};

use monad_event::Result;

use crate::EventCaptureReader;

pub struct EventCaptureFile {
    file: File,
}

impl EventCaptureFile {
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = File::open(path)?;

        Ok(Self { file })
    }

    pub fn create_reader(&self) -> Result<EventCaptureReader> {
        EventCaptureReader::new(&self.file)
    }
}
