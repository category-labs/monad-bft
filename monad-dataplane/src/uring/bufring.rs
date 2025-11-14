use std::{
    mem,
    sync::atomic::{AtomicU16, Ordering},
};

use bytes::BytesMut;
use io_uring::types;

use super::reader::ReaderError;

pub(super) struct BufferRing {
    buffers: Vec<Vec<u8>>,
    buf_ring_ptr: *mut types::BufRingEntry,
    ring_entries: u16,
    buf_len: usize,
    shared_tail: *const AtomicU16,
    pub(super) buf_group_id: u16,
}

impl BufferRing {
    pub(super) fn new(total_buf_count: usize, thread_id: usize) -> Result<Self, ReaderError> {
        let buf_len = 1500;
        let ring_entries = total_buf_count.next_power_of_two() as u16;
        let entry_size = mem::size_of::<types::BufRingEntry>();
        let ring_size = entry_size * ring_entries as usize;

        let ring_mem = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                ring_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED | libc::MAP_POPULATE,
                -1,
                0,
            )
        };

        if ring_mem == libc::MAP_FAILED {
            return Err(ReaderError::CreateRing(std::io::Error::other(
                "mmap failed for buffer ring",
            )));
        }

        let buf_ring_ptr = ring_mem as *mut types::BufRingEntry;

        let mut buffers: Vec<Vec<u8>> = (0..total_buf_count).map(|_| vec![0u8; buf_len]).collect();

        for (bid, buf) in buffers.iter_mut().enumerate() {
            let ring_idx = (bid as u16) & (ring_entries - 1);
            let entry = unsafe { &mut *buf_ring_ptr.add(ring_idx as usize) };
            entry.set_addr(buf.as_mut_ptr() as u64);
            entry.set_len(buf_len as u32);
            entry.set_bid(bid as u16);
        }

        let buf_group_id = 0xbee0 + thread_id as u16;
        let shared_tail = unsafe { types::BufRingEntry::tail(buf_ring_ptr) } as *const AtomicU16;

        unsafe {
            (*shared_tail).store(total_buf_count as u16, Ordering::Release);
        }

        Ok(Self {
            buffers,
            buf_ring_ptr,
            ring_entries,
            buf_len,
            shared_tail,
            buf_group_id,
        })
    }

    pub(super) fn register(&self, ring: &mut io_uring::IoUring) -> Result<(), ReaderError> {
        unsafe {
            ring.submitter()
                .register_buf_ring_with_flags(
                    self.buf_ring_ptr as u64,
                    self.ring_entries,
                    self.buf_group_id,
                    0,
                )
                .map_err(|e| {
                    ReaderError::CreateRing(std::io::Error::other(format!(
                        "failed to register buffer ring: {}",
                        e
                    )))
                })?;
        }
        Ok(())
    }

    pub(super) fn copy_data_from_buffer(&self, bid: u16, data_len: usize) -> BytesMut {
        let mut payload = BytesMut::with_capacity(data_len);
        unsafe {
            payload.set_len(data_len);
            std::ptr::copy_nonoverlapping(
                self.buffers[bid as usize].as_ptr(),
                payload.as_mut_ptr(),
                data_len,
            );
        }
        payload
    }

    pub(super) fn return_buffer(&mut self, bid: u16) {
        let ring_idx = bid & (self.ring_entries - 1);
        let entry = unsafe { &mut *self.buf_ring_ptr.add(ring_idx as usize) };
        entry.set_addr(self.buffers[bid as usize].as_mut_ptr() as u64);
        entry.set_len(self.buf_len as u32);
        entry.set_bid(bid);

        let local_tail = unsafe { (*self.shared_tail).load(Ordering::Acquire) };
        unsafe {
            (*self.shared_tail).store(local_tail.wrapping_add(1), Ordering::Release);
        }
    }
}
