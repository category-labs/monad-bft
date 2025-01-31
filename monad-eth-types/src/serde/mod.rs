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
