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

//! Seal-granule alignment. Indexes seal in fixed id spans equal to the bitmap
//! page span, so both the write path (sealing) and the read path (query
//! planning) floor/relate ids to seal boundaries through these helpers — the
//! single source of truth keeps the two from ever computing a different open
//! granule.

use crate::bitmap::STREAM_PAGE_ID_SPAN;

/// Per-family seal granule: a page and its directory bucket seal together, so
/// the seal span equals the page span.
pub const SEAL_SPAN: u64 = STREAM_PAGE_ID_SPAN as u64;

/// Largest multiple of [`SEAL_SPAN`] at/below `frontier` — the start of the open
/// granule.
pub fn seal_boundary(frontier: u64) -> u64 {
    (frontier / SEAL_SPAN) * SEAL_SPAN
}

/// Start of the most recently sealed granule given the open-granule start
/// `sealed_id` (`None` when nothing has sealed yet). The persisted seal-chain
/// row for this span carries the family's current chain value.
pub fn last_sealed_span(sealed_id: u64) -> Option<u64> {
    sealed_id.checked_sub(SEAL_SPAN)
}
