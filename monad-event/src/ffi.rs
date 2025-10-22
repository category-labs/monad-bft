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

//! This module contains low-level bindings to the monad execution client event ring library. This
//! is **not** a regular Rust module and is mostly hidden.

pub use self::bindings::{
    g_monad_event_content_type_names, monad_event_content_type, monad_event_descriptor,
    MONAD_EVENT_CONTENT_TYPE_COUNT, MONAD_EVENT_CONTENT_TYPE_EXEC, MONAD_EVENT_CONTENT_TYPE_NONE,
    MONAD_EVENT_CONTENT_TYPE_TEST,
};

#[allow(
    dead_code,
    missing_docs,
    non_camel_case_types,
    non_upper_case_globals,
    unused_imports,
    clippy::ptr_offset_with_cast
)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
