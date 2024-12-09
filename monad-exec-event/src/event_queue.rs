//! Module for connecting to an event queue from an external process and
//! exporting its shared memory segments

use std::{
    ffi::{CStr, CString},
    path::PathBuf,
    sync::atomic::AtomicU64,
};

use crate::{
    event::{monad_event_descriptor, EventQueueType},
    event_reader::{monad_event_payload_page, EventReader},
    event_types,
};

/// Configuration options needed to connect to an event queue
pub struct EventQueueOptions {
    pub socket_path: PathBuf,
    pub timeout: libc::timeval,
    pub queue_type: EventQueueType,
}

// C FFI form of the above structure; needed to call the extern "C"
// monad_event_queue_connect
#[allow(non_camel_case_types)]
#[repr(C)]
struct monad_event_queue_options {
    socket_path: *const libc::c_char,
    timeout: libc::timeval,
    queue_type: libc::c_uchar,
}

// C compatible definition of `struct monad_event_reader` from
// `event_reader.h`, needed for the extern "C" call to
// monad_event_queue_init_reader. The reader itself has a pure Rust
// implementation (in event_reader.rs) for performance reasons, so this
// structure is only needed privately by the queue module, for reader
// initialization.
#[allow(non_camel_case_types)]
#[repr(C)]
struct monad_event_reader {
    desc_table: *const monad_event_descriptor,
    payload_pages: *const *const monad_event_payload_page,
    last_seqno: u64,
    capacity_mask: libc::size_t,
    prod_next: *mut AtomicU64,
}

#[allow(non_camel_case_types)]
#[repr(C)]
struct monad_event_queue_ffi_extra {
    desc_table_size: libc::size_t,
    num_payload_pages: u16,
}

// Declaration of public interface from event_queue.h. In the C version,
// the queue object itself is an opaque structure type,
// `struct monad_event_queue;` referred to only through pointers. Here it
// is just `void`, since we won't assume the definition of it anywhere.
#[link(name = "monad_event_queue")]
extern "C" {
    fn monad_event_queue_connect(
        user_opts: *const monad_event_queue_options,
        queue_p: *mut *const libc::c_void,
        thread_table: *mut *const event_types::thread_info,
        block_header_table: *mut *const event_types::block_exec_header,
    ) -> libc::c_int;

    fn monad_event_queue_disconnect(queue: *const libc::c_void);

    fn monad_event_queue_is_connected(queue: *const libc::c_void) -> bool;

    fn monad_event_queue_init_reader(
        queue: *const libc::c_void,
        reader: *mut monad_event_reader,
        ffi_exta: *mut monad_event_queue_ffi_extra,
    ) -> u64;

    fn monad_event_queue_get_last_error() -> *const libc::c_char;
}

/// Smart pointer to the underlying C queue object; upon Drop it will call
/// monad_event_queue_disconnect and all the queue's shared memory segments
/// will be unmapped. The explicit reference lifetime 'shm refers to the
/// lifetime of these shared memory segments, and all references derived
/// from pointers inside the segment will share that lifetime
pub struct EventQueue<'shm> {
    queue: *const libc::c_void,
    pub thread_table: &'shm [event_types::thread_info],
    pub block_header_table: &'shm [event_types::block_exec_header],
}

impl<'shm> EventQueue<'shm> {
    /// Connect to an event queue with the provided options
    pub fn connect(options: &EventQueueOptions) -> Result<Self, String> {
        let socket_path_cstr = CString::new(options.socket_path.as_os_str().as_encoded_bytes())
            .expect("embedded nul in pathbuf?");
        let queue_type: libc::c_uchar = match options.queue_type {
            EventQueueType::Exec => 0,
            EventQueueType::Trace => 1,
        };
        let c_opts = monad_event_queue_options {
            socket_path: socket_path_cstr.as_ptr(),
            timeout: options.timeout,
            queue_type,
        };
        let mut queue: *const libc::c_void = std::ptr::null_mut();
        let mut thread_table: *const event_types::thread_info = std::ptr::null_mut();
        let mut block_header_table: *const event_types::block_exec_header = std::ptr::null_mut();

        let r = unsafe {
            monad_event_queue_connect(
                &c_opts,
                &mut queue,
                &mut thread_table,
                &mut block_header_table,
            )
        };
        if r != 0 {
            let err_str = unsafe {
                CStr::from_ptr(monad_event_queue_get_last_error())
                    .to_str() // Convert to a &str
                    .unwrap_or("invalid UTF-8 in monad_event_queue_get_last_error?")
            };
            return Err(String::from(err_str));
        }
        Ok(EventQueue {
            queue,
            thread_table: unsafe { std::slice::from_raw_parts(thread_table, 1 << 8) },
            block_header_table: unsafe { std::slice::from_raw_parts(block_header_table, 1 << 12) },
        })
    }

    /// Test whether the event server is still connected; this is an expensive
    /// function (it requires a system call on the socket), so high performance
    /// clients should not call this in a tight event polling loop
    pub fn is_connected(&self) -> bool {
        unsafe { monad_event_queue_is_connected(self.queue) }
    }

    /// Initialize a reader of the queue; each reader has its own state, and
    /// this is called once to initialize that state and set the initial
    /// iteration point; afterwards, the EventReader's reset method can be
    /// used to reseat the iterator
    pub fn create_reader(&'shm self) -> EventReader<'shm> {
        let mut c_reader = unsafe { std::mem::MaybeUninit::<monad_event_reader>::zeroed().assume_init() };
        let mut c_ffi_extra = unsafe { std::mem::MaybeUninit::<monad_event_queue_ffi_extra>::zeroed().assume_init() };
        let last_seqno: u64 = unsafe {
            monad_event_queue_init_reader(
                self.queue,
                &mut c_reader,
                &mut c_ffi_extra,
            )
        };
        let desc_table: &'shm [monad_event_descriptor] =
            unsafe { std::slice::from_raw_parts(c_reader.desc_table, c_ffi_extra.desc_table_size) };
        let payload_pages: &'shm [*const monad_event_payload_page] = unsafe {
            std::slice::from_raw_parts(
                c_reader.payload_pages,
                c_ffi_extra.num_payload_pages as usize,
            )
        };
        let prod_next: &'shm mut AtomicU64 =
            unsafe { c_reader.prod_next.as_mut() }.expect("exported prod_next was nullptr?");
        EventReader::new(
            desc_table,
            payload_pages,
            last_seqno,
            c_reader.capacity_mask,
            prod_next,
        )
    }
}

impl Drop for EventQueue<'_> {
    fn drop(&mut self) {
        unsafe { monad_event_queue_disconnect(self.queue) };
    }
}
