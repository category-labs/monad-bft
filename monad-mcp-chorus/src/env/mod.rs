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

//! This module emulates the first-class module/functor system of OCaml in Rust.
//!
//! Here's how it works on the high level:
//!
//! - In the implementation of the chorus, when referring types or
//!   sibiling modules, we only use them through the relative path
//!   (e.g. super::types::NodeId, self::conductor::Conductor).
//! - In the types module, we import the implementation-dependent
//!   types from super::env
//! - In the top_level module, we expose public modules/types
//!   (e.g. CadenceDriver)
//! - Then we define global variant modules crate::{stub, prod} that
//!   + define/import the env module: pub use crate::env::stub as env;
//!   + include the top_level.rs file
//!
//! - now we have all types accessible through the variant modules
//!   (e.g. crate::{stub,prod}::CadenceDriver)
//!
//! In order to include top_level.rs multiple times (once in each
//! variant), we use the #[path] attribute on `mod top_level`. It is
//! equivalent to `mod top_level { include!("top_level.rs"); }` so the
//! file's content is expanded in place. rust-analyzer should have
//! good support of navigating code of this pattern.

// a stub implementation of spec for development & testing
pub mod stub;

// demonstration only. reuses all stub types.
pub mod prod;
