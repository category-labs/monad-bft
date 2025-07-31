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

pub const MAX_V: u32 = 1048576;
pub const MAX_DEGREE: usize = 40;

// Compute the Degree Generator function Deg() defined in RFC 5053 section 5.4.4.2.
pub fn deg(v: u32) -> u8 {
    match v {
        0..=10240 => 1,
        10241..=491581 => 2,
        491582..=712793 => 3,
        712794..=831694 => 4,
        831695..=948445 => 10,
        948446..=1032188 => 11,
        1032189..=1048575 => 40,
        _ => panic!("Can't find Deg({})", v),
    }
}
