use std::{fs::File, marker::PhantomData, os::fd::AsRawFd, path::Path};

pub(crate) use self::raw::RawEventRing;
pub use self::snapshot::SnapshotEventRing;
use crate::{
    ffi::monad_check_path_supports_map_hugetlb, EventReader, EventRingType, RawEventReader,
};

mod raw;
mod snapshot;

/// A unified interface for event rings.
pub trait TypedEventRing {
    /// The underlying type of this event ring.
    type Type: EventRingType;

    /// Produces a reader that produces events from this ring.
    fn create_reader<'ring>(&'ring self) -> EventReader<'ring, Self::Type>;
}

/// An event ring created from a file.
pub struct EventRing<T>
where
    T: EventRingType,
{
    raw: RawEventRing,
    _phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for EventRing<T>
where
    T: EventRingType,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventRing")
            .field("raw", &self.raw)
            .field("type", &T::ring_ctype())
            .finish()
    }
}

impl<T> EventRing<T>
where
    T: EventRingType,
{
    /// Synchronously creates a new event ring from the provided path.
    pub fn new_from_path(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::new_from_path_with_offset(path, 0)
    }

    /// Synchronously creates a new event ring from the provided path and offset.
    ///
    /// This method should only be used if the event ring starts at an offset within the file at the
    /// provided path. In most cases, you should use [`new_from_path()`](Self::new_from_path)
    /// instead.
    pub fn new_from_path_with_offset(
        path: impl AsRef<Path>,
        ring_offset: libc::off_t,
    ) -> Result<Self, String> {
        let mmap_prot = libc::PROT_READ;

        let supports_hugetlb = monad_check_path_supports_map_hugetlb(&path)
            .expect("failed to determine if event ring file supports MAP_HUGETLB");

        let mmap_extra_flags = if supports_hugetlb {
            libc::MAP_POPULATE | libc::MAP_HUGETLB
        } else {
            libc::MAP_POPULATE
        };

        let ring_file = File::open(&path).map_err(|e| e.to_string())?;

        let ring_fd = ring_file.as_raw_fd();

        let raw = RawEventRing::mmap_from_fd(
            mmap_prot,
            mmap_extra_flags,
            ring_fd,
            ring_offset,
            path.as_ref().to_str().unwrap(),
        )?;

        Self::new(raw)
    }

    pub(crate) fn new(raw: RawEventRing) -> Result<Self, String> {
        raw.check_type::<T>()?;

        Ok(Self {
            raw,
            _phantom: PhantomData,
        })
    }
}

impl<T> TypedEventRing for EventRing<T>
where
    T: EventRingType,
{
    type Type = T;

    fn create_reader<'ring>(&'ring self) -> EventReader<'ring, Self::Type> {
        let raw = RawEventReader::new(&self.raw).unwrap();

        EventReader::new(raw)
    }
}
