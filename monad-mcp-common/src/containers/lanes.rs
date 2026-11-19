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

//! The lanes of one slot, addressed by [`Lane`]. Iteration is in lane order.

use alloy_rlp::{Decodable, Encodable, RlpEncodableWrapper};

use super::dense::{DenseVec, Idx};
use crate::MAX_PROPOSER_SET_SIZE;

// The position of a proposal in a slot. Only meaningful under the
// context of a given slot. Invariant: .0 < MAX_PROPOSER_SET_SIZE.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, RlpEncodableWrapper)]
pub struct Lane(u8);

impl Lane {
    pub const fn new_unchecked(lane: u8) -> Self {
        debug_assert!((lane as usize) < MAX_PROPOSER_SET_SIZE);
        Self(lane)
    }

    pub const fn from_u8(lane: u8) -> Option<Self> {
        if lane as usize >= MAX_PROPOSER_SET_SIZE {
            return None;
        }
        Some(Self(lane))
    }
}

impl Decodable for Lane {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let lane = u8::decode(buf)?;
        Self::from_u8(lane).ok_or(alloy_rlp::Error::Custom("lane out of range"))
    }
}

impl From<Lane> for usize {
    fn from(lane: Lane) -> Self {
        usize::from(lane.0)
    }
}

impl Idx for Lane {
    const MAX: usize = MAX_PROPOSER_SET_SIZE;

    fn new(index: usize) -> Self {
        Self::new_unchecked(index as u8)
    }

    fn index(self) -> usize {
        usize::from(self)
    }
}

/// A value for each lane of one slot, in lane order.
pub type Lanes<T> = DenseVec<Lane, T>;

impl<T> Encodable for Lanes<T>
where
    T: Encodable,
{
    fn encode(&self, out: &mut dyn bytes::BufMut) {
        alloy_rlp::encode_list(self.as_slice(), out);
    }

    fn length(&self) -> usize {
        alloy_rlp::list_length(self.as_slice())
    }
}

impl<T> Decodable for Lanes<T>
where
    T: Decodable,
{
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        let mut values = Vec::new();
        while !payload.is_empty() {
            if values.len() == MAX_PROPOSER_SET_SIZE {
                return Err(alloy_rlp::Error::Custom("too many lanes"));
            }
            values.push(T::decode(&mut payload)?);
        }
        if values.is_empty() {
            return Err(alloy_rlp::Error::Custom("no lanes"));
        }
        Self::from_vec(values).ok_or(alloy_rlp::Error::Custom("too many lanes"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lane_decodes_only_below_the_proposer_set_size() {
        for lane in 0..=u8::MAX {
            let bytes = alloy_rlp::encode(lane);
            let decoded = Lane::decode(&mut bytes.as_slice());
            let in_range = usize::from(lane) < MAX_PROPOSER_SET_SIZE;
            assert_eq!(decoded.is_ok(), in_range, "lane {lane}");
        }
    }
}
