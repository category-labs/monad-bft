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

use alloy_rlp::{Decodable, Encodable};
use bytes::Bytes;

use crate::spec::{Deserializable, Serializable};

pub(crate) fn assert_roundtrip<T>(value: &T) -> T
where
    T: Encodable + Decodable + std::fmt::Debug + PartialEq,
{
    let encoded = alloy_rlp::encode(value);
    assert_eq!(value.length(), encoded.len());
    let decoded: T = alloy_rlp::decode_exact(&encoded).unwrap();
    assert_eq!(&decoded, value);
    for end in 0..encoded.len() {
        assert!(
            alloy_rlp::decode_exact::<T>(&encoded[..end]).is_err(),
            "accepted truncated input at {end}"
        );
    }
    let mut stream = encoded;
    stream.push(0x42);
    let mut remaining = stream.as_slice();
    assert_eq!(&T::decode(&mut remaining).unwrap(), value);
    assert_eq!(remaining, &[0x42]);
    assert!(alloy_rlp::decode_exact::<T>(&stream).is_err());
    decoded
}

/// Check the network byte traits as well as the underlying streaming RLP codecs.
pub(crate) fn assert_serialization_roundtrip<T>(value: &T) -> T
where
    T: Encodable
        + Decodable
        + Serializable<Bytes>
        + Deserializable<Bytes, ReadError = alloy_rlp::Error>
        + std::fmt::Debug
        + PartialEq,
{
    assert_roundtrip(value);
    let bytes = value.serialize();
    assert_eq!(bytes.as_ref(), alloy_rlp::encode(value));
    let decoded = T::deserialize(&bytes).unwrap();
    assert_eq!(&decoded, value);
    for end in 0..bytes.len() {
        assert!(T::deserialize(&bytes.slice(..end)).is_err());
    }
    let mut extra = bytes.to_vec();
    extra.push(0x42);
    assert!(T::deserialize(&Bytes::from(extra)).is_err());
    decoded
}
