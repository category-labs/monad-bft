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

use monad_event_capture::EventCaptureReader;

use crate::ffi::{
    monad_bcap_block_archive, monad_block_capture_block_archive_close,
    monad_block_capture_block_archive_open, monad_block_capture_block_archive_open_block,
};

pub struct BlockCaptureBlockArchive {
    inner: &'static mut monad_bcap_block_archive,
}

impl BlockCaptureBlockArchive {
    pub fn new(dir: &std::fs::File) -> Result<Self, String> {
        use std::os::fd::AsRawFd;

        let inner = monad_block_capture_block_archive_open(dir.as_raw_fd(), "todo")?;

        Ok(Self { inner })
    }

    pub fn open_block(&self, block_number: u64) -> Result<EventCaptureReader, String> {
        let c_event_capture_reader =
            monad_block_capture_block_archive_open_block(self.inner, block_number)?;

        Ok(unsafe { EventCaptureReader::new_from_raw(c_event_capture_reader) })
    }
}

impl Drop for BlockCaptureBlockArchive {
    fn drop(&mut self) {
        monad_block_capture_block_archive_close(self.inner);
    }
}
