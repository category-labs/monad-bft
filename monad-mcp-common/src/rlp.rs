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

//! RLP codecs for optional wire fields, which alloy leaves out

use alloy_rlp::{BufMut, Decodable, EMPTY_LIST_CODE, Encodable};

/// Stands in for an absent optional item, since alloy has no `Encodable` for
/// `Option`
pub struct RlpNone;

impl Encodable for RlpNone {
    fn encode(&self, out: &mut dyn BufMut) {
        out.put_u8(EMPTY_LIST_CODE);
    }

    fn length(&self) -> usize {
        1
    }
}

/// An optional wire field as a list of zero or one item: alloy has no codec
/// for `Option`, and a bare item could itself encode as the empty list
pub struct RlpOption<T>(pub Option<T>);

impl<T: Encodable> Encodable for RlpOption<T> {
    fn encode(&self, out: &mut dyn BufMut) {
        match &self.0 {
            Some(item) => {
                alloy_rlp::encode_list::<_, dyn Encodable>(&[item as &dyn Encodable], out)
            }
            None => RlpNone.encode(out),
        }
    }

    fn length(&self) -> usize {
        match &self.0 {
            Some(item) => alloy_rlp::list_length::<_, dyn Encodable>(&[item as &dyn Encodable]),
            None => RlpNone.length(),
        }
    }
}

impl<T: Decodable> Decodable for RlpOption<T> {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut payload = alloy_rlp::Header::decode_bytes(buf, true)?;
        if payload.is_empty() {
            return Ok(Self(None));
        }
        let item = T::decode(&mut payload)?;
        if !payload.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        Ok(Self(Some(item)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded<T: Encodable>(value: &T) -> Vec<u8> {
        let mut out = Vec::new();
        value.encode(&mut out);
        assert_eq!(out.len(), value.length());
        out
    }

    #[test]
    fn some_and_none_round_trip_and_stay_distinct() {
        let none = encoded(&RlpOption::<Vec<u64>>(None));
        let empty = encoded(&RlpOption(Some(Vec::<u64>::new())));
        assert_ne!(none, empty);
        assert_eq!(none, encoded(&RlpNone));

        let decoded = RlpOption::<Vec<u64>>::decode(&mut none.as_slice()).unwrap();
        assert_eq!(decoded.0, None);
        let decoded = RlpOption::<Vec<u64>>::decode(&mut empty.as_slice()).unwrap();
        assert_eq!(decoded.0, Some(vec![]));

        let some = encoded(&RlpOption(Some(vec![1u64, 2])));
        let decoded = RlpOption::<Vec<u64>>::decode(&mut some.as_slice()).unwrap();
        assert_eq!(decoded.0, Some(vec![1, 2]));
    }

    #[test]
    fn more_than_one_item_is_rejected() {
        let mut out = Vec::new();
        alloy_rlp::encode_list::<_, u64>(&[1, 2], &mut out);
        assert!(RlpOption::<u64>::decode(&mut out.as_slice()).is_err());
    }
}
