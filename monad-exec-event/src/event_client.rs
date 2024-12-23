//! Module for connecting to an event ring from an external process and
//! exporting its shared memory segments

use std::{
    ffi::{CStr, CString},
    path::PathBuf,
    sync::atomic::{AtomicU32, AtomicU64},
    sync::Arc,
};

use crate::{
    event::{monad_event_descriptor, monad_event_payload_page, EventRingType},
    event_types,
};

/// Configuration options needed to connect to a monad process
pub struct ConnectOptions {
    pub socket_path: PathBuf,
    pub timeout: libc::timeval,
}

//
// C FFI forms of various structures
//

#[allow(non_camel_case_types)]
#[repr(C)]
struct monad_event_connect_options {
    socket_path: *const libc::c_char,
    timeout: libc::timeval,
}

#[allow(non_camel_case_types)]
#[repr(C)]
struct monad_event_proc {
    refcount: AtomicU32,
    sock_fd: libc::c_int,
    metadata_page: *const monad_event_payload_page,
    thread_table: *const event_types::thread_info,
    block_header_table: *const event_types::block_exec_header,
    // other fields are present which we cannot easily
    // be poked at from Rust
}

#[allow(non_camel_case_types)]
#[repr(C)]
struct monad_event_ring_control {
    prod_next: AtomicU64
}

#[allow(non_camel_case_types)]
#[repr(C)]
struct monad_event_ring {
    control: *const monad_event_ring_control,
    descriptor_table: *const monad_event_descriptor,
    capacity_mask: usize,
    capacity: usize,
}

#[allow(non_camel_case_types)]
#[repr(C)]
struct monad_event_imported_ring {
    refcount: AtomicU32,
    num_payload_pages: libc::c_ushort,
    ring_type: libc::c_uchar,
    ring: monad_event_ring,
    payload_pages: *const *const monad_event_payload_page,
    true_desc_table_size: usize,
    proc: *const monad_event_proc,
}

// Declaration of public interface from event_client.h.
#[link(name = "monad_event_client")]
extern "C" {
    fn monad_event_proc_connect(
        user_opts: *const monad_event_connect_options,
        proc_p: *mut *mut monad_event_proc,
    ) -> libc::c_int;

    fn monad_event_proc_disconnect(proc: *mut monad_event_proc);

    fn monad_event_proc_is_connected(proc: *const monad_event_proc) -> bool;

    fn monad_event_proc_import_ring(
        proc: *mut monad_event_proc,
        ring_type: libc::c_uchar,
        imported_ring: *mut *mut monad_event_imported_ring) -> libc::c_int;

    fn monad_event_imported_ring_acquire(imported_ring: *mut monad_event_imported_ring)
        -> *mut monad_event_imported_ring;

    fn monad_event_imported_ring_release(imported_ring: *mut monad_event_imported_ring)
        -> libc::c_uchar;

    fn monad_event_proc_get_last_error() -> *const libc::c_char;
}

/// Rust view of the underlying C proc object; upon Drop it will call
/// monad_event_proc_disconnect and all the metadata shared memory segments
/// will be unmapped. The explicit reference lifetime 'meta refers to the
/// lifetime of these shared memory segments
pub struct EventProc<'meta> {
    proc: *mut monad_event_proc,
    pub thread_table: &'meta [event_types::thread_info],
    pub block_header_table: &'meta [event_types::block_exec_header],
}

/// Smart pointer to the underlying C imported ring object; it is itself
/// an Arc-like object. It introduces a lifetime for the shared memory
/// segment it imports, which is potentially shorted than 'meta
pub struct ImportedEventRing<'meta, 'shm> {
    imported_ring: *mut monad_event_imported_ring,
    pub(crate) wrapped_descriptor_table: &'shm [monad_event_descriptor],
    pub(crate) descriptor_table: &'shm [monad_event_descriptor],
    pub(crate) payload_pages: &'shm [*const monad_event_payload_page],
    pub(crate) prod_next: &'shm AtomicU64,
    pub parent: Arc<EventProc<'meta>>,
}

impl<'meta> EventProc<'meta> {
    /// Connect to an event process with the provided options
    pub fn connect(options: &ConnectOptions) -> Result<Arc<Self>, String> {
        let socket_path_cstr = CString::new(options.socket_path.as_os_str().as_encoded_bytes())
            .expect("embedded nul in pathbuf?");
        let c_opts = monad_event_connect_options {
            socket_path: socket_path_cstr.as_ptr(),
            timeout: options.timeout,
        };
        let mut proc: *mut monad_event_proc = std::ptr::null_mut();
        let r = unsafe {
            monad_event_proc_connect(
                &c_opts,
                &mut proc,
            )
        };
        if r != 0 {
            let err_str = unsafe {
                CStr::from_ptr(monad_event_proc_get_last_error())
                    .to_str() // Convert to a &str
                    .unwrap_or("invalid UTF-8 in monad_event_proc_get_last_error?")
            };
            return Err(String::from(err_str));
        }
        Ok(Arc::new(EventProc {
            proc,
            thread_table: unsafe { std::slice::from_raw_parts((*proc).thread_table, 1 << 8) },
            block_header_table: unsafe { std::slice::from_raw_parts((*proc).block_header_table, 1 << 12) },
        }))
    }

    /// Test whether the event server is still connected; this is an expensive
    /// function (it requires a system call on the socket), so high performance
    /// clients should not call this in a tight event polling loop
    pub fn is_connected(&self) -> bool {
        unsafe { monad_event_proc_is_connected(self.proc) }
    }
}

impl<'meta, 'shm> ImportedEventRing<'meta, 'shm>
{
    pub fn import(proc: Arc<EventProc<'meta>>, ring_type: EventRingType)
        -> Result<ImportedEventRing<'meta, 'shm>, String>
    where 'meta: 'shm {
        let ring_type: libc::c_uchar = match ring_type {
            EventRingType::Exec => 0,
            EventRingType::Trace => 1,
        };
        let mut imported_ring: *mut monad_event_imported_ring = std::ptr::null_mut();

        let r = unsafe {
            monad_event_proc_import_ring(
                proc.proc,
                ring_type,
                &mut imported_ring
            )
        };
        if r != 0 {
            let err_str = unsafe {
                CStr::from_ptr(monad_event_proc_get_last_error())
                    .to_str() // Convert to a &str
                    .unwrap_or("invalid UTF-8 in monad_event_proc_get_last_error?")
            };
            return Err(String::from(err_str));
        }
        let prod_next: &'shm AtomicU64 = unsafe{ &(*(*imported_ring).ring.control).prod_next };
        Ok(ImportedEventRing{
            imported_ring,
            wrapped_descriptor_table: unsafe {
                std::slice::from_raw_parts((*imported_ring).ring.descriptor_table,
                                           (*imported_ring).true_desc_table_size)
            },
            descriptor_table: unsafe {
                std::slice::from_raw_parts((*imported_ring).ring.descriptor_table,
                                           (*imported_ring).ring.capacity)
            },
            payload_pages: unsafe {
                std::slice::from_raw_parts((*imported_ring).payload_pages,
                                           (*imported_ring).num_payload_pages as usize)
            },
            prod_next,
            parent: proc.clone(),
        })
    }
}

impl Clone for ImportedEventRing<'_, '_> {
    fn clone(&self) -> Self {
        let r = unsafe {
            monad_event_imported_ring_acquire(self.imported_ring)
        };
        assert!(!r.is_null());
        ImportedEventRing {
            imported_ring: self.imported_ring,
            wrapped_descriptor_table: self.wrapped_descriptor_table,
            descriptor_table: self.descriptor_table,
            payload_pages: self.payload_pages,
            prod_next: self.prod_next,
            parent: self.parent.clone()
        }
    }
}

impl Drop for ImportedEventRing<'_, '_> {
    fn drop(&mut self) {
        unsafe { monad_event_imported_ring_release(self.imported_ring); }
    }
}

impl Drop for EventProc<'_> {
    fn drop(&mut self) {
        unsafe { monad_event_proc_disconnect(self.proc) };
    }
}
