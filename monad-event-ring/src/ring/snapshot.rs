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

use std::path::Path;

use super::{raw::RawEventRing, DecodedEventRing, EventRing, RawEventReader};
use crate::{ffi, EventDecoder, EventReader};

/// A special kind of event ring mapped to a static file for replaying events.
///
/// This type is intended to be used for testing / recovery where, during normal operation, an
/// [`EventRing`] would be used.
#[derive(Debug)]
pub struct SnapshotEventRing<D>
where
    D: EventDecoder,
{
    ring: EventRing<D>,
    snapshot_fd: libc::c_int,
}

impl<D> SnapshotEventRing<D>
where
    D: EventDecoder,
{
    /// Produces an event ring by decoding the provided `bytes` input, which is expected to contain
    /// a snapshot file (a single zstd-compressed frame containing an event ring).
    ///
    /// Internally, this function writes the decoded bytes to an anonymous file which is destroyed
    /// when the [`SnapshotEventRing`] is dropped.
    pub fn new_from_zstd_bytes(
        zstd_bytes: &[u8],
        max_size: Option<usize>,
        name: impl AsRef<str>,
    ) -> Result<Self, String> {
        let name = name.as_ref();
        if let Some(decompressed_file) =
            ffi::monad_event_decompress_snapshot_mem(zstd_bytes, max_size, name)?
        {
            Self::new_from_decompressed_file(decompressed_file, name)
        } else {
            Err(format!("{name} is not an event ring snapshot"))
        }
    }

    /// Produces an event ring by decoding the file at the provided input path, which is expected to
    /// be a snapshot file (a single zstd-compressed frame containing an event ring).
    ///
    /// Internally, this function writes the decoded bytes to an anonymous file which is destroyed
    /// when the [`SnapshotEventRing`] is dropped.
    pub fn new_from_zstd_path(
        path: impl AsRef<Path>,
        max_size: Option<usize>,
    ) -> Result<Self, String> {
        let file = Self::resolve_path_to_file(&path)?;
        let error_name = path.as_ref().display().to_string();
        if let Some(decompressed_file) =
            ffi::monad_event_decompress_snapshot_fd(&file, max_size, &error_name)?
        {
            Self::new_from_decompressed_file(decompressed_file, &error_name)
        } else {
            Err(format!("{error_name} is not an event ring snapshot"))
        }
    }

    /// Returns true if the given file is likely to be an event ring snapshot file.
    pub fn is_snapshot_file(path: impl AsRef<Path>) -> Result<bool, String> {
        let file = Self::resolve_path_to_file(&path)?;
        ffi::monad_event_is_snapshot_file(&file, path.as_ref().display().to_string())
    }

    /// Ensure that snapshot event ring paths are translated using the same mechanism as
    /// [`EventRing::new_from_path`]; this may seem odd at first, as snapshot files should not live
    /// on a hugetlbfs mount. Treating the path translation exactly the same for both types makes it
    /// easier to open an either kind of event ring file behind a common interface, without
    /// encountering subtle bugs.
    fn resolve_path_to_file(path: impl AsRef<Path>) -> Result<std::fs::File, String> {
        let resolved_path = ffi::monad_event_resolve_ring_file(None::<&str>, path.as_ref())?;
        let file = std::fs::File::open(path.as_ref()).map_err(|err| {
            format!(
                "could not open event ring file `{}`: {}",
                resolved_path.display(),
                err
            )
        })?;
        Ok(file)
    }

    fn new_from_decompressed_file(
        file: std::fs::File,
        name: impl AsRef<str>,
    ) -> Result<Self, String> {
        use std::os::fd::{AsRawFd, IntoRawFd};
        let snapshot_off: libc::off_t = 0;
        let raw = RawEventRing::mmap_from_fd(
            libc::PROT_READ,
            0,
            file.as_raw_fd(),
            snapshot_off,
            name.as_ref(),
        )?;
        Ok(Self {
            ring: EventRing::new(raw)?,
            snapshot_fd: file.into_raw_fd(),
        })
    }
}

impl<D> Drop for SnapshotEventRing<D>
where
    D: EventDecoder,
{
    fn drop(&mut self) {
        let ret = unsafe { libc::close(self.snapshot_fd) };
        assert_eq!(ret, 0);
    }
}

impl<D> DecodedEventRing for SnapshotEventRing<D>
where
    D: EventDecoder,
{
    type Decoder = D;

    fn create_reader<'ring>(&'ring self) -> EventReader<'ring, D> {
        let raw = RawEventReader::new(&self.ring.raw).unwrap();

        EventReader::new_snapshot(raw)
    }
}
