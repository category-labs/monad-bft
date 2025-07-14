use crate::{
    ffi::{monad_event_ring, monad_event_ring_mmap, monad_event_ring_unmap},
    EventDecoder,
};

#[derive(Debug)]
pub(crate) struct RawEventRing {
    pub(crate) inner: monad_event_ring,
}

impl RawEventRing {
    pub(crate) fn mmap_from_fd(
        mmap_prot: libc::c_int,
        mmap_extra_flags: libc::c_int,
        ring_fd: libc::c_int,
        ring_offset: libc::off_t,
        error_name: &str,
    ) -> Result<Self, String> {
        monad_event_ring_mmap(
            mmap_prot,
            mmap_extra_flags,
            ring_fd,
            ring_offset,
            error_name,
        )
        .map(|inner| Self { inner })
    }

    pub(crate) fn check_type<D>(&self) -> Result<(), String>
    where
        D: EventDecoder,
    {
        D::check_ring_type(&self.inner)
    }
}

impl Drop for RawEventRing {
    fn drop(&mut self) {
        monad_event_ring_unmap(&mut self.inner);
    }
}

unsafe impl Send for RawEventRing {}
unsafe impl Sync for RawEventRing {}
