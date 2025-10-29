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

use std::ffi::{CStr, CString};

pub use self::bindings::{
    monad_evcap_event_iter, monad_evcap_event_section, monad_evcap_reader, monad_evcap_section_desc,
};
pub(crate) use self::bindings::{
    monad_evcap_read_result, monad_evcap_section_type, MONAD_EVCAP_READ_END,
    MONAD_EVCAP_READ_NO_SEQNO, MONAD_EVCAP_READ_SUCCESS, MONAD_EVCAP_SECTION_EVENT_BUNDLE,
    MONAD_EVCAP_SECTION_NONE, MONAD_EVCAP_SECTION_PACK_INDEX, MONAD_EVCAP_SECTION_SCHEMA,
    MONAD_EVCAP_SECTION_SEQNO_INDEX,
};
use crate::ffi::bindings::monad_evcap_file_header;

#[allow(dead_code, missing_docs, non_camel_case_types, non_upper_case_globals)]
mod bindings {
    use ::monad_event::ffi::{monad_event_content_type, monad_event_descriptor};

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use ::monad_event::ffi::{monad_event_content_type, monad_event_descriptor};

#[inline]
fn get_last_evcap_reader_error(r: libc::c_int) -> Result<(), String> {
    if r == 0 {
        return Ok(());
    }

    let err_str = unsafe {
        CStr::from_ptr(self::bindings::monad_evcap_reader_get_last_error())
            .to_str()
            .unwrap_or("Invalid UTF-8 in monad_evcap_reader_get_last_error")
    };

    Err(String::from(err_str))
}

#[inline]
fn error_name_to_cstring(str_ref: impl AsRef<str>) -> Result<CString, String> {
    CString::new(str_ref.as_ref()).map_err(|nul_err| nul_err.to_string())
}

pub(crate) fn monad_evcap_reader_create(
    file: &impl std::os::fd::AsRawFd,
    error_name: &str,
) -> Result<&'static mut monad_evcap_reader, String> {
    let mut c_evcap_reader: *mut monad_evcap_reader = std::ptr::null_mut();

    let error_name_cstring = error_name_to_cstring(error_name)?;

    let r = unsafe {
        self::bindings::monad_evcap_reader_create(
            &mut c_evcap_reader,
            file.as_raw_fd(),
            error_name_cstring.as_ptr(),
        )
    };

    get_last_evcap_reader_error(r).map(|()| unsafe { &mut *c_evcap_reader })
}

pub(crate) unsafe fn monad_evcap_reader_destroy(evcap_reader: &mut monad_evcap_reader) {
    unsafe {
        self::bindings::monad_evcap_reader_destroy(evcap_reader);
    }
}

pub(crate) fn monad_evcap_reader_refresh(
    evcap_reader: &mut monad_evcap_reader,
) -> Result<bool, String> {
    let mut invalidated = false;

    let r = unsafe { self::bindings::monad_evcap_reader_refresh(evcap_reader, &mut invalidated) };

    get_last_evcap_reader_error(r).map(|()| invalidated)
}

pub(crate) fn monad_evcap_reader_get_file_header(
    evcap_reader: &monad_evcap_reader,
) -> &monad_evcap_file_header {
    unsafe { &*self::bindings::monad_evcap_reader_get_file_header(evcap_reader) }
}

pub(crate) fn monad_evcap_reader_load_linked_section_desc(
    evcap_reader: &monad_evcap_reader,
    offset: u64,
) -> &monad_evcap_section_desc {
    unsafe { &*self::bindings::monad_evcap_reader_load_linked_section_desc(evcap_reader, offset) }
}

// TODO: add comment about how method is unsafe because of 'file lifetime
pub(crate) unsafe fn monad_evcap_reader_next_section<'file>(
    evcap_reader: &monad_evcap_reader,
    filter: monad_evcap_section_type,
    last_evcap_section_desc: Option<&monad_evcap_section_desc>,
) -> Option<&'file monad_evcap_section_desc> {
    let mut evcap_section_desc: *const monad_evcap_section_desc =
        last_evcap_section_desc.map_or(std::ptr::null(), |evcap_section_desc| evcap_section_desc);

    self::bindings::monad_evcap_reader_next_section(evcap_reader, filter, &mut evcap_section_desc);

    (!evcap_section_desc.is_null()).then(|| &*evcap_section_desc)
}

pub(crate) fn monad_evcap_reader_check_schema(
    evcap_reader: &monad_evcap_reader,
    ring_magic: &[u8],
    content_type: monad_event_content_type,
    schema_hash: &[u8],
) -> Result<(), String> {
    let r = unsafe {
        self::bindings::monad_evcap_reader_check_schema(
            evcap_reader,
            ring_magic.as_ptr(),
            content_type,
            schema_hash.as_ptr(),
        )
    };

    get_last_evcap_reader_error(r)
}

pub(crate) fn monad_evcap_event_section_open(
    evcap_reader: &monad_evcap_reader,
    section_desc: &monad_evcap_section_desc,
) -> Result<monad_evcap_event_section, String> {
    let mut evcap_event_section: monad_evcap_event_section = unsafe { std::mem::zeroed() };

    let r = unsafe {
        self::bindings::monad_evcap_event_section_open(
            &mut evcap_event_section,
            evcap_reader,
            section_desc,
        )
    };

    get_last_evcap_reader_error(r).map(|()| evcap_event_section)
}

pub(crate) unsafe fn monad_evcap_event_section_close(
    evcap_event_section: &mut monad_evcap_event_section,
) {
    unsafe { self::bindings::monad_evcap_event_section_close(evcap_event_section) }
}

pub(crate) fn monad_evcap_event_section_open_iterator(
    evcap_event_section: &monad_evcap_event_section,
) -> monad_evcap_event_iter {
    let mut evcap_event_iter: monad_evcap_event_iter = unsafe { std::mem::zeroed() };

    unsafe {
        self::bindings::monad_evcap_event_section_open_iterator(
            evcap_event_section,
            &mut evcap_event_iter,
        )
    };

    evcap_event_iter
}

pub(crate) fn monad_evcap_event_section_copy_seqno(
    evcap_event_section: &monad_evcap_event_section,
    seqno: u64,
) -> (
    monad_evcap_read_result,
    Option<&monad_event_descriptor>,
    Option<&[u8]>,
) {
    let mut c_event_descriptor: *const monad_event_descriptor = std::ptr::null();
    let mut c_payload: *const std::os::raw::c_void = std::ptr::null();

    let evcap_read_result = unsafe {
        self::bindings::monad_evcap_event_section_copy_seqno(
            evcap_event_section,
            seqno,
            &mut c_event_descriptor,
            &mut c_payload,
        )
    };

    let event_descriptor;
    let payload;

    match (c_event_descriptor.is_null(), c_payload.is_null()) {
        (true, true) => {
            event_descriptor = None;
            payload = None;
        }
        (false, false) => {
            let event_descriptor_ref = unsafe { &*c_event_descriptor };

            event_descriptor = Some(event_descriptor_ref);
            payload = Some(unsafe {
                std::slice::from_raw_parts(
                    c_payload as *const u8,
                    event_descriptor_ref.payload_size as usize,
                )
            });
        }
        _ => panic!("ffi::monad_evcap_event_section_copy_seqno produced event_descriptor and payload ptr where only one is non-null")
    }

    (evcap_read_result, event_descriptor, payload)
}

pub(crate) unsafe fn monad_evcap_event_iter_next<'reader>(
    evcap_event_iter: &mut monad_evcap_event_iter,
) -> (
    monad_evcap_read_result,
    Option<&'reader monad_event_descriptor>,
    Option<&'reader [u8]>,
) {
    let mut c_event_descriptor: *const monad_event_descriptor = std::ptr::null();
    let mut c_payload: *const std::os::raw::c_void = std::ptr::null();

    let evcap_read_result = unsafe {
        self::bindings::monad_evcap_event_iter_next(
            evcap_event_iter,
            &mut c_event_descriptor,
            &mut c_payload,
        )
    };

    let event_descriptor;
    let payload;

    match (c_event_descriptor.is_null(), c_payload.is_null()) {
        (true, true) => {
            event_descriptor = None;
            payload = None;
        }
        (false, false) => {
            let event_descriptor_ref = unsafe { &*c_event_descriptor };

            event_descriptor = Some(event_descriptor_ref);
            payload = Some(unsafe {
                std::slice::from_raw_parts(
                    c_payload as *const u8,
                    event_descriptor_ref.payload_size as usize,
                )
            });
        }
        _ => panic!("ffi::monad_evcap_event_iter_next produced event_descriptor and payload ptr where only one is non-null")
    }

    (evcap_read_result, event_descriptor, payload)
}

pub(crate) fn monad_evcap_event_iter_prev(
    evcap_event_iter: &mut monad_evcap_event_iter,
) -> (
    monad_evcap_read_result,
    Option<&monad_event_descriptor>,
    Option<&[u8]>,
) {
    let mut c_event_descriptor: *const monad_event_descriptor = std::ptr::null();
    let mut c_payload: *const std::os::raw::c_void = std::ptr::null();

    let evcap_read_result = unsafe {
        self::bindings::monad_evcap_event_iter_prev(
            evcap_event_iter,
            &mut c_event_descriptor,
            &mut c_payload,
        )
    };

    let event_descriptor;
    let payload;

    match (c_event_descriptor.is_null(), c_payload.is_null()) {
        (true, true) => {
            event_descriptor = None;
            payload = None;
        }
        (false, false) => {
            let event_descriptor_ref = unsafe { &*c_event_descriptor };

            event_descriptor = Some(event_descriptor_ref);
            payload = Some(unsafe {
                std::slice::from_raw_parts(
                    c_payload as *const u8,
                    event_descriptor_ref.payload_size as usize,
                )
            });
        }
        _ => panic!("ffi::monad_evcap_event_iter_prev produced event_descriptor and payload ptr where only one is non-null")
    }

    (evcap_read_result, event_descriptor, payload)
}

pub(crate) fn monad_evcap_event_iter_copy(
    evcap_event_iter: &mut monad_evcap_event_iter,
) -> (
    monad_evcap_read_result,
    Option<&monad_event_descriptor>,
    Option<&[u8]>,
) {
    let mut c_event_descriptor: *const monad_event_descriptor = std::ptr::null();
    let mut c_payload: *const std::os::raw::c_void = std::ptr::null();

    let evcap_read_result = unsafe {
        self::bindings::monad_evcap_event_iter_copy(
            evcap_event_iter,
            &mut c_event_descriptor,
            &mut c_payload,
        )
    };

    let event_descriptor;
    let payload;

    match (c_event_descriptor.is_null(), c_payload.is_null()) {
        (true, true) => {
            event_descriptor = None;
            payload = None;
        }
        (false, false) => {
            let event_descriptor_ref = unsafe { &*c_event_descriptor };

            event_descriptor = Some(event_descriptor_ref);
            payload = Some(unsafe {
                std::slice::from_raw_parts(
                    c_payload as *const u8,
                    event_descriptor_ref.payload_size as usize,
                )
            });
        }
        _ => panic!("ffi::monad_evcap_event_iter_copy produced event_descriptor and payload ptr where only one is non-null")
    }

    (evcap_read_result, event_descriptor, payload)
}

pub(crate) fn monad_evcap_event_iter_set_seqno(
    evcap_event_iter: &mut monad_evcap_event_iter,
    seqno: u64,
) -> monad_evcap_read_result {
    unsafe { self::bindings::monad_evcap_event_iter_set_seqno(evcap_event_iter, seqno) }
}
