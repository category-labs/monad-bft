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

pub(crate) use crate::ffi::bindings::monad_bcap_block_archive;

#[allow(dead_code, missing_docs, non_camel_case_types, non_upper_case_globals)]
mod bindings {
    use ::monad_event_capture::ffi::{monad_evcap_reader, monad_evcap_section_desc};
    use ::monad_exec_events::ffi::monad_exec_block_tag;
    use libc::mode_t;

    type monad_evcap_writer = ();
    type monad_vbuf_chain = [u8; 32];
    type monad_vbuf_segment_allocator = ();

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use ::monad_event::Result;
use ::monad_event_capture::ffi::{monad_evcap_reader, monad_evcap_section_desc};

#[inline]
fn get_last_bcap_library_error(r: libc::c_int) -> Result<()> {
    if r == 0 {
        return Ok(());
    }

    let err_str = unsafe {
        CStr::from_ptr(self::bindings::monad_bcap_get_last_error())
            .to_str()
            .unwrap_or("Invalid UTF-8 in monad_bcap_get_last_error")
    };

    let err = std::io::Error::from_raw_os_error(r);

    Err(std::io::Error::new(err.kind(), err_str))
}

#[inline]
fn error_name_to_cstring(str_ref: impl AsRef<str>) -> Result<CString> {
    Ok(CString::new(str_ref.as_ref())?)
}

pub(crate) fn monad_block_capture_block_archive_open(
    dirfd: libc::c_int,
    error_name: &str,
) -> Result<&'static mut monad_bcap_block_archive> {
    let mut c_block_capture_block_archive: *mut monad_bcap_block_archive = std::ptr::null_mut();

    let error_name_cstring = error_name_to_cstring(error_name)?;

    let r = unsafe {
        self::bindings::monad_bcap_block_archive_open(
            &mut c_block_capture_block_archive,
            dirfd,
            error_name_cstring.as_ptr(),
        )
    };

    get_last_bcap_library_error(r).map(|()| unsafe { &mut *c_block_capture_block_archive })
}

pub(crate) fn monad_block_capture_block_archive_close(
    c_block_capture_block_archive: &mut monad_bcap_block_archive,
) {
    unsafe { self::bindings::monad_bcap_block_archive_close(c_block_capture_block_archive) };
}

pub(crate) fn monad_block_capture_block_archive_open_block(
    c_block_capture_block_archive: &monad_bcap_block_archive,
    block_number: u64,
) -> Result<&'static mut monad_evcap_reader> {
    let mut c_event_capture_reader: *mut monad_evcap_reader = std::ptr::null_mut();
    let mut c_evcap_section_desc: *const monad_evcap_section_desc = unsafe { std::mem::zeroed() };

    let r = unsafe {
        self::bindings::monad_bcap_block_archive_open_block_reader(
            c_block_capture_block_archive,
            block_number,
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            &mut c_event_capture_reader,
            &mut c_evcap_section_desc,
        )
    };

    get_last_bcap_library_error(r).map(|()| unsafe { &mut *c_event_capture_reader })
}
