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

//! Numeric helpers whose results are the same bits on every platform.
//!
//! `f64::exp`, `f64::ln` and the other transcendental methods of the
//! standard library call the platform's C library, and two platforms may
//! round the last bit differently. A seeded simulation that takes a decision
//! from such a value can then diverge between, say, macOS and Linux.
//!
//! Everything here goes through the [`libm`] crate, which is the same Rust
//! code everywhere. The rule for code in this crate: no transcendental
//! method of `f64`. Arithmetic, comparisons, `abs` and `sqrt` are exact or
//! correctly rounded by IEEE 754 and are fine; `sqrt` still goes through
//! `libm` so the rule needs no exception.
//!
//! The crate depends on nothing else in the workspace.

pub mod normal;
