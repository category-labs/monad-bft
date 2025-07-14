use std::ffi::CString;

use super::{raw::RawEventRing, DecodedEventRing, EventRing, RawEventReader};
use crate::{EventDecoder, EventReader};

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
    /// Produces an event ring by [`zstd`] decoding the provided `zstd_bytes` input.
    ///
    /// Internally, this function writes the decoded bytes to an anonymous file which is destroyed
    /// when the [`SnapshotEventRing`] is dropped.
    pub fn new_from_zstd_bytes(zstd_bytes: &[u8], name: impl AsRef<str>) -> Result<Self, String> {
        let error_name_cstr = CString::new(name.as_ref()).unwrap();

        let snapshot_fd: libc::c_int =
            unsafe { libc::memfd_create(error_name_cstr.as_ptr(), libc::MFD_CLOEXEC) };
        assert_ne!(snapshot_fd, -1);

        let snapshot_off: libc::off_t = 0;

        let mut decompressed = Vec::new();

        zstd::stream::copy_decode(zstd_bytes, &mut decompressed)
            .expect(format!("could not decompress `{}`", name.as_ref()).as_str());

        let n_write = unsafe {
            libc::write(
                snapshot_fd,
                decompressed.as_ptr() as *const libc::c_void,
                decompressed.len() as libc::size_t,
            )
        };
        assert_eq!(n_write as usize, decompressed.len());

        let raw = RawEventRing::mmap_from_fd(
            libc::PROT_READ,
            0,
            snapshot_fd,
            snapshot_off,
            name.as_ref(),
        )?;

        Ok(Self {
            ring: EventRing::new(raw)?,
            snapshot_fd,
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
