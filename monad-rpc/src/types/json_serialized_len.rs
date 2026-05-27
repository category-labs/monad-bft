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

use alloy_primitives::FixedBytes;
use alloy_rpc_types::Log;

pub trait JsonSerializedLen {
    fn json_serialized_len(&self) -> usize;
}

macro_rules! proptest_json_serialized_len_arbitrary {
    (@final [$($t:tt)*] $mod_name:ident) => {
        #[allow(non_snake_case)]
        mod $mod_name {
            use proptest::proptest;

            use super::*;

            proptest! {
                #[allow(non_snake_case)]
                #[test]
                fn proptest_any(value in proptest::arbitrary::any::<$($t)*>()) {
                    let json_serialized_len = JsonSerializedLen::json_serialized_len(&value);

                    let json_serialized_str = serde_json::to_string(&value).unwrap();

                    assert_eq!(
                        json_serialized_len, json_serialized_str.len(),
                        "JsonSerializedLen does not match! Serialized: \"{json_serialized_str}\""
                    );
                }
            }
        }
    };

    ($t:ty) => {
        #[cfg(test)]
        paste::paste! {
            proptest_json_serialized_len_arbitrary!(
                @final
                [$t]
                [<test_json_serialized_len_arbitrary__ $t:snake>]
            );
        }
    };

    ($mod_name:ident, $($t:tt)*) => {
        #[cfg(test)]
        paste::paste! {
            proptest_json_serialized_len_arbitrary!(
                @final
                [$($t)*]
                [<test_json_serialized_len_arbitrary__ $mod_name>]
            );
        }
    };
}

impl JsonSerializedLen for bool {
    fn json_serialized_len(&self) -> usize {
        if *self {
            4
        } else {
            5
        }
    }
}
proptest_json_serialized_len_arbitrary!(bool);

impl<T> JsonSerializedLen for Option<T>
where
    T: JsonSerializedLen,
{
    fn json_serialized_len(&self) -> usize {
        match self {
            Some(value) => value.json_serialized_len(),
            None => 4,
        }
    }
}
proptest_json_serialized_len_arbitrary!(option_bool, Option<bool>);
proptest_json_serialized_len_arbitrary!(option_fixed_bytes_20, Option<FixedBytes<20>>);
proptest_json_serialized_len_arbitrary!(option_fixed_bytes_32, Option<FixedBytes<32>>);

impl<const N: usize> JsonSerializedLen for FixedBytes<N> {
    fn json_serialized_len(&self) -> usize {
        // enclosing double quotes
        2
        // 0x
        + 2
        // bytes hex encoded
        + 2 * N
    }
}
proptest_json_serialized_len_arbitrary!(fixed_bytes_20, FixedBytes<20>);
proptest_json_serialized_len_arbitrary!(fixed_bytes_32, FixedBytes<32>);

fn json_serialized_hex_int_len(value: Option<u64>) -> usize {
    value
        .map(|index| {
            let bits = (64 - index.leading_zeros()) as usize;

            // enclosing double quotes
            2
                // 0x
                + 2
                // value hex encoded
                + bits.div_ceil(4).max(1)
        })
        .unwrap_or(
            // null
            4,
        )
}

impl JsonSerializedLen for Log {
    fn json_serialized_len(&self) -> usize {
        const BASE_JSON: &str = r#"{"address":"0x0000000000000000000000000000000000000000","blockHash":,"blockNumber":,"data":"0x","logIndex":,"removed":,"topics":[],"transactionHash":,"transactionIndex":}"#;

        BASE_JSON.len()
            + JsonSerializedLen::json_serialized_len(&self.block_hash)
            + json_serialized_hex_int_len(self.block_number)
            + self
                .block_timestamp
                .map(|t| "\"blockTimestamp\":,".len() + json_serialized_hex_int_len(Some(t)))
                .unwrap_or_default()
            + 2 * self.inner.data.data.len()
            + json_serialized_hex_int_len(self.log_index)
            + JsonSerializedLen::json_serialized_len(&self.removed)
            + (
                // topic data
                self.inner.data.topics().len()
            * (
                // enclosing double quotes
                2
                // 0x
                + 2
                // 32 bytes hex encoded -> 64 bytes
                + 64
            )
            // topic data comma separators
            + self.inner.data.topics().len().saturating_sub(1)
            )
            + JsonSerializedLen::json_serialized_len(&self.transaction_hash)
            + json_serialized_hex_int_len(self.transaction_index)
    }
}

#[cfg(test)]
mod test_json_serialized_len_arbitrary {
    use arbitrary::Unstructured;
    use proptest::{prelude::Strategy, proptest};

    use super::JsonSerializedLen;

    fn gen_arbitrary<T: for<'a> arbitrary::Arbitrary<'a> + std::fmt::Debug>(
    ) -> impl Strategy<Value = T> {
        proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256).prop_filter_map(
            "invalid arbitrary",
            |bytes| {
                let mut u = Unstructured::new(&bytes);
                u.arbitrary().ok()
            },
        )
    }

    proptest! {
        #[allow(non_snake_case)]
        #[test]
        fn proptest_alloy_rpc_types_eth__log(log in gen_arbitrary::<alloy_rpc_types::eth::Log>()) {
            let json_serialized_len = JsonSerializedLen::json_serialized_len(&log);

            let json_serialized_str = serde_json::to_string(&log).unwrap();

            assert_eq!(
                json_serialized_len, json_serialized_str.len(),
                "JsonSerializedLen does not match! Serialized: \"{json_serialized_str}\""
            );
        }
    }
}
