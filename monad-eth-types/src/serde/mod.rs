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

use std::{fmt, str::FromStr};

use alloy_primitives::{Address, FixedBytes, U160};
use serde::{de::Visitor, Deserializer};

/// Deserialize Eth address from a hex string or raw bytes
pub fn deserialize_eth_address_from_str<'de, D>(deserializer: D) -> Result<Address, D::Error>
where
    D: Deserializer<'de>,
{
    struct EthAddressVisitor;

    impl<'de> Visitor<'de> for EthAddressVisitor {
        type Value = Address;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("EthAddress as a hex string or an array of bytes")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Address(U160::from_str(value).unwrap().into()))
        }

        fn visit_seq<S>(self, mut seq: S) -> Result<Self::Value, S::Error>
        where
            S: serde::de::SeqAccess<'de>,
        {
            let mut bytes = [0u8; 20];

            for byte in &mut bytes {
                *byte = seq.next_element()?.ok_or(serde::de::Error::custom(
                    "EthAddress has less than 20 elements",
                ))?;
            }

            if seq.next_element::<u8>()?.is_some() {
                return Err(serde::de::Error::custom(
                    "EthAddress has more than 20 elements",
                ));
            }

            Ok(Address(FixedBytes(bytes)))
        }
    }

    deserializer.deserialize_any(EthAddressVisitor)
}
