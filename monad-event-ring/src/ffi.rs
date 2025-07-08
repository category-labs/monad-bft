//! This module contains low-level bindings to the monad execution client event ring library. This
//! is **not** a regular Rust module and is mostly hidden.

use std::{
    ffi::{CStr, CString},
    path::Path,
};

pub(crate) use self::bindings::{
    g_monad_event_ring_type_names, monad_event_descriptor, monad_event_iterator,
    monad_event_next_result, monad_event_next_result_MONAD_EVENT_GAP,
    monad_event_next_result_MONAD_EVENT_NOT_READY, monad_event_next_result_MONAD_EVENT_SUCCESS,
    monad_event_ring, monad_event_ring_type_MONAD_EVENT_RING_TYPE_COUNT,
    monad_event_ring_type_MONAD_EVENT_RING_TYPE_NONE,
};
pub use self::bindings::{monad_event_ring_type, monad_event_ring_type_MONAD_EVENT_RING_TYPE_TEST};

#[allow(dead_code, non_camel_case_types, non_upper_case_globals)]
mod bindings {
    use libc::{off_t, pid_t};

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

#[inline]
fn get_last_ring_library_error(r: libc::c_int) -> Result<(), String> {
    if r == 0 {
        return Ok(());
    }

    let err_str = unsafe {
        CStr::from_ptr(self::bindings::monad_event_ring_get_last_error())
            .to_str()
            .unwrap_or("Invalid UTF-8 in monad_event_ring_get_last_error")
    };

    Err(String::from(err_str))
}

pub(crate) fn monad_event_ring_mmap(
    mmap_prot: libc::c_int,
    mmap_extra_flags: libc::c_int,
    ring_fd: libc::c_int,
    ring_offset: libc::off_t,
    error_name: &str,
) -> Result<monad_event_ring, String> {
    let mut c_event_ring: monad_event_ring = unsafe { std::mem::zeroed() };

    let error_name_cstr = CString::new(error_name)
        .map_err(|err| format!("error_name contained embedded nul: {err}"))?;

    let r = unsafe {
        self::bindings::monad_event_ring_mmap(
            &mut c_event_ring,
            mmap_prot,
            mmap_extra_flags,
            ring_fd,
            ring_offset,
            error_name_cstr.as_ptr(),
        )
    };

    get_last_ring_library_error(r).map(|()| c_event_ring)
}

pub(crate) fn monad_event_ring_check_type(
    c_event_ring: &monad_event_ring,
    c_event_ring_type: monad_event_ring_type,
    metadata_hash: &[u8; 32],
) -> Result<(), String> {
    let r = unsafe {
        self::bindings::monad_event_ring_check_type(
            c_event_ring,
            c_event_ring_type,
            metadata_hash.as_ptr(),
        )
    };

    get_last_ring_library_error(r)
}

pub(crate) fn monad_event_ring_unmap(c_event_ring: &mut monad_event_ring) {
    unsafe {
        self::bindings::monad_event_ring_unmap(c_event_ring);
    }
}

pub(crate) fn monad_event_ring_iterator_init(
    c_event_ring: &monad_event_ring,
) -> Result<monad_event_iterator, String> {
    let mut c_event_iterator: monad_event_iterator = unsafe { std::mem::zeroed() };

    let r = unsafe {
        self::bindings::monad_event_ring_init_iterator(c_event_ring, &mut c_event_iterator)
    };

    get_last_ring_library_error(r).map(|()| c_event_iterator)
}

#[inline]
pub(crate) fn monad_event_iterator_try_next(
    c_event_iterator: &mut monad_event_iterator,
) -> (monad_event_next_result, monad_event_descriptor) {
    let mut c_event_descriptor: monad_event_descriptor = unsafe { std::mem::zeroed() };

    let c_next_result: monad_event_next_result = unsafe {
        self::bindings::monad_event_iterator_try_next(c_event_iterator, &mut c_event_descriptor)
    };

    (c_next_result, c_event_descriptor)
}

pub(crate) fn monad_event_iterator_reset(c_event_iterator: &mut monad_event_iterator) {
    unsafe { self::bindings::monad_event_iterator_reset(c_event_iterator) };
}

#[inline]
pub(crate) fn monad_event_ring_payload_peek<'ring>(
    c_event_ring: &'ring monad_event_ring,
    c_event_descriptor: &monad_event_descriptor,
) -> Option<&'ring [u8]> {
    let payload_ptr = unsafe {
        self::bindings::monad_event_ring_payload_peek(c_event_ring, c_event_descriptor) as *const u8
    };

    if payload_ptr.is_null() {
        return None;
    }

    Some(unsafe {
        std::slice::from_raw_parts(payload_ptr, c_event_descriptor.payload_size as usize)
    })
}

#[inline]
pub(crate) fn monad_event_ring_payload_check(
    c_event_ring: &monad_event_ring,
    c_event_descriptor: &monad_event_descriptor,
) -> bool {
    unsafe { self::bindings::monad_event_ring_payload_check(c_event_ring, c_event_descriptor) }
}

pub(crate) fn monad_check_path_supports_map_hugetlb(
    path: impl AsRef<Path>,
) -> Result<bool, String> {
    let path_bytes = path.as_ref().to_str().ok_or(format!(
        "cannot extract path bytes from `{:?}`",
        path.as_ref()
    ))?;

    let c_path = CString::new(path_bytes).map_err(|e| e.to_string())?;

    let mut supported = false;

    let r = unsafe {
        self::bindings::monad_check_path_supports_map_hugetlb(c_path.as_ptr(), &mut supported)
    };

    get_last_ring_library_error(r).map(|()| supported)
}
