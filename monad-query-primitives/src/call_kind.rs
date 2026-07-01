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

/// EVM call-frame kind — a flat per-frame tag (calls, creates, and
/// self-destruct in one enum). Neither alloy nor revm fits as-is: they split
/// these across `CallType` / `CreateScheme` / suicide actions.
///
/// Mirrored locally rather than reused from `monad-archive` (whose own
/// `CallKind` has a different byte layout): `monad-archive` depends on this
/// crate family, so depending back would cycle — the ingest path converts
/// archive→query at the boundary. `as_u8`/`from_u8` are a persisted wire
/// contract; don't renumber.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CallKind {
    Call,
    DelegateCall,
    CallCode,
    Create,
    Create2,
    SelfDestruct,
    StaticCall,
}

impl CallKind {
    pub const fn as_u8(self) -> u8 {
        match self {
            CallKind::Call => 0,
            CallKind::StaticCall => 1,
            CallKind::DelegateCall => 2,
            CallKind::CallCode => 3,
            CallKind::Create => 4,
            CallKind::Create2 => 5,
            CallKind::SelfDestruct => 6,
        }
    }

    pub fn from_u8(byte: u8) -> Option<Self> {
        Some(match byte {
            0 => CallKind::Call,
            1 => CallKind::StaticCall,
            2 => CallKind::DelegateCall,
            3 => CallKind::CallCode,
            4 => CallKind::Create,
            5 => CallKind::Create2,
            6 => CallKind::SelfDestruct,
            _ => return None,
        })
    }
}
