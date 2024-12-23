//! Module defining the EventReader object and its API

use std::sync::atomic::{AtomicU64, Ordering};

use crate::event::{monad_event_descriptor, monad_event_payload_page};
use crate::event_client::ImportedEventRing;

/// Result of calling the zero-copy non-blocking API, polling for a new
/// event
pub enum PeekResult<'shm> {
    NotReady,
    Gap(&'shm monad_event_descriptor),
    Ready(&'shm monad_event_descriptor),
}

/// Result of calling the copy_next non-blocking API, polling for a new
/// event
pub enum CopyResult {
    NotReady,
    PayloadExpired,
    Gap,
    Ready,
}

/// Holds the iterator state of a single event reader; these are initialized
/// from the ImportedEventRing they read from. This is a native Rust
/// reimplementation of the functionality in event_iterator.h, in the C API.
///
/// It is not called EventIterator in Rust, to prevent confusing with the
/// formal Rust iterator concept (it does not implement the Iterator trait)
/// because of the more complex nature of the zero-copy API.
///
/// As in the C API, readers are lightweight and an arbitrary number may
/// exist, but they are single-threaded.
pub struct EventReader<'meta, 'shm> {
    descriptor_table: &'shm [monad_event_descriptor],
    payload_pages: &'shm [*const monad_event_payload_page],
    pub last_seqno: u64,
    capacity_mask: usize,
    prod_next: &'shm AtomicU64,
    imported_ring: ImportedEventRing<'meta, 'shm>,
}

impl<'meta, 'shm> EventReader<'meta, 'shm> {
    /// Initialize a reader of the event ring; each reader has its own state,
    /// and this is called once to initialize that state and set the initial
    /// iteration point; afterwards, the EventReader's reset method can be
    /// used to reseat the iterator
    pub fn new(imported_ring: &ImportedEventRing<'meta, 'shm>) -> EventReader<'meta, 'shm> {
        EventReader {
            descriptor_table: imported_ring.descriptor_table,
            payload_pages: imported_ring.payload_pages,
            last_seqno: imported_ring.prod_next.load(Ordering::Acquire),
            capacity_mask: imported_ring.descriptor_table.len() - 1,
            prod_next: imported_ring.prod_next,
            imported_ring: imported_ring.clone(),
        }
    }

    /// Obtain an immutable reference to the next event descriptor in a
    /// zero-copy fashion, if one is available
    #[inline]
    pub fn peek(&'_ self) -> PeekResult<'shm> {
        let event: &'shm monad_event_descriptor =
            &self.descriptor_table[(self.last_seqno as usize) & self.capacity_mask];
        let seqno = event.seqno.load(Ordering::Acquire);
        if seqno == self.last_seqno + 1 {
            PeekResult::Ready(event)
        } else if seqno < self.last_seqno {
            PeekResult::NotReady
        } else {
            PeekResult::Gap(event)
        }
    }

    /// Advance to the next event, returning true only if the consumed event
    /// was still valid immediately before advancing past it. Note that if
    /// false is returned, a gap has already occurred, or is almost certainly
    /// about to occur
    #[inline]
    pub fn advance(&'_ mut self) -> bool {
        let event: &'shm monad_event_descriptor =
            &self.descriptor_table[(self.last_seqno as usize) & self.capacity_mask];
        let seqno = event.seqno.load(Ordering::Acquire);
        if seqno == self.last_seqno + 1 {
            self.last_seqno += 1;
            true
        } else {
            false
        }
    }

    /// Obtain a pointer to the event's payload in shared memory in a
    /// zero-copy fashion
    #[inline]
    pub fn payload_peek(
        &'_ self,
        event: &'shm monad_event_descriptor,
    ) -> (&'shm [u8], &'shm AtomicU64) {
        let payload_page_ptr: *const monad_event_payload_page =
            self.payload_pages[event.payload_page as usize];
        let event_payload_bytes_ptr: *const u8 =
            (payload_page_ptr as usize).wrapping_add(event.offset as usize) as *const u8;
        let payload_page: &monad_event_payload_page = unsafe { &*payload_page_ptr };
        (
            unsafe {
                std::slice::from_raw_parts(event_payload_bytes_ptr, event.get_length() as usize)
            },
            &payload_page.overwrite_seqno,
        )
    }

    // Given output buffers to an event descriptor and a payload array (which
    // must be sufficiently sized for any possible event payload), copy an
    // event if one is available. If this is successful, it advances the
    // iterator to the next event
    #[inline]
    pub fn copy_next(
        &'_ mut self,
        event: &'_ mut monad_event_descriptor,
        payload_buf: &'_ mut [u8]
    )  -> CopyResult {
        let src_seqno: &AtomicU64;
        match self.peek() {
            PeekResult::NotReady => return CopyResult::NotReady,
            PeekResult::Gap(e) => return {
                let src = e as *const monad_event_descriptor;
                unsafe {
                    std::ptr::copy_nonoverlapping(src, event as *mut monad_event_descriptor, 1);
                }
                CopyResult::Gap
            },
            PeekResult::Ready(e) => {
                let src = e as *const monad_event_descriptor;
                src_seqno = &e.seqno;
                unsafe {
                    std::ptr::copy_nonoverlapping(src, event as *mut monad_event_descriptor, 1);
                }
            }
        }
        let seqno = event.seqno.load(Ordering::Relaxed);
        if seqno != src_seqno.load(Ordering::Acquire) {
            return CopyResult::Gap
        }
        let (payload, overwrite_seqno): (&[u8], &AtomicU64) = self.payload_peek(event);
        unsafe {
            std::ptr::copy_nonoverlapping(payload.as_ptr(), payload_buf.as_mut_ptr(),
                std::cmp::min(payload.len(), payload_buf.len()));
        }
        if overwrite_seqno.load(Ordering::Acquire) > seqno {
            return CopyResult::PayloadExpired
        }
        _ = self.advance();
        CopyResult::Ready
    }

    #[inline]
    pub fn payload_new<T: Sized>(&'_ mut self, event: &'shm monad_event_descriptor) -> Option<T>  {
        let (payload, _): (&[u8], &AtomicU64) = self.payload_peek(event);
        let t: T = unsafe {
            std::mem::transmute_copy(&*(payload.as_ptr() as *const T))
        };
        if self.advance() { Some(t) } else { None }
    }

    /// Reset the reader to point to the latest event produced; used for
    /// "hard" gap recovery
    #[inline]
    pub fn reset(&mut self) -> u64 {
        self.last_seqno = self.sync_wait();
        self.last_seqno
    }

    #[inline]
    fn sync_wait(&'_ mut self) -> u64 {
        let prod_next = self.prod_next.load(Ordering::Acquire);
        if prod_next == 0 {
            // TODO(ken): same issue here as in C++
            self.last_seqno = 0;
            self.last_seqno
        } else {
            // `prod_next` is atomically incremented before the contents of
            // the associated descriptor table slot (which is `prod_next - 1`)
            // are written. The contents are definitely commited when the
            // sequence number (which is equal to `prod_next`) is atomically
            // stored (with Ordering::Acquire). This waits for that to happen,
            // if it hasn't happened already.
            let index = ((prod_next - 1) as usize) & self.capacity_mask;
            let slot_seqno: &AtomicU64 = &self.descriptor_table[index].seqno;
            while slot_seqno.load(Ordering::Acquire) < prod_next {} // Empty
            prod_next - 1
        }
    }
}