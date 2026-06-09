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

pub trait JsonSerializedLen {
    fn json_serialized_len(&self) -> usize;
}

#[cfg(test)]
mod proptest_json_serialized {
    use arbitrary::Unstructured;
    use proptest::prelude::*;

    pub(super) fn strategy_arbitrary<T: for<'a> arbitrary::Arbitrary<'a> + std::fmt::Debug>(
    ) -> impl Strategy<Value = T> {
        proptest::collection::vec(proptest::prelude::any::<u8>(), 0..1024).prop_filter_map(
            "invalid arbitrary",
            |bytes| {
                let mut u = Unstructured::new(&bytes);
                u.arbitrary().ok()
            },
        )
    }

    pub(super) fn strategy_serde_json_value() -> impl Strategy<Value = serde_json::Value> {
        use serde_json::{Map, Number, Value};

        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            prop_oneof![
                any::<f64>()
                    .prop_filter_map("valid JSON number", Number::from_f64)
                    .prop_map(Value::Number),
                any::<u64>().prop_map(|n| Value::Number(n.into())),
                any::<i64>().prop_map(|n| Value::Number(n.into())),
            ],
            any::<String>().prop_map(Value::String),
        ];

        leaf.prop_recursive(8, 256, 10, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..10).prop_map(Value::Array),
                proptest::collection::hash_map(any::<String>(), inner, 0..10)
                    .prop_map(|m| { Value::Object(m.into_iter().collect::<Map<String, Value>>()) }),
            ]
        })
    }
}

macro_rules! proptest_json_serialized_len {
    (@final [$($defs:tt)*] [$($strategy:tt)+] $mod_name:ident) => {
        #[allow(non_snake_case)]
        mod $mod_name {
            use super::*;

            $($defs)*

            proptest::proptest! {
                #[allow(non_snake_case)]
                #[test]
                fn proptest(value in $($strategy)+) {
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
            proptest_json_serialized_len!(
                @final
                []
                [proptest::arbitrary::any::<$t>()]
                [<test_json_serialized_len__ $t:snake>]
            );
        }
    };

    ($t:ty, $mod_name:ident) => {
        #[cfg(test)]
        paste::paste! {
            proptest_json_serialized_len!(
                @final
                []
                [proptest::arbitrary::any::<$t>()]
                [<test_json_serialized_len__ $mod_name>]
            );
        }
    };

    ($t:ty, $mod_name:ident, { $($strategy:tt)+ }) => {
        #[cfg(test)]
        paste::paste! {
            proptest_json_serialized_len!(
                @final
                [
                    pub(super) fn strategy() -> impl proptest::prelude::Strategy<Value = $t> {
                        #[allow(unused)]
                        use proptest::prelude::*;

                        $($strategy)+
                    }
                ]
                [strategy()]
                [<test_json_serialized_len__ $mod_name>]
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
proptest_json_serialized_len!(bool);

impl JsonSerializedLen for i32 {
    fn json_serialized_len(&self) -> usize {
        let neg = if *self < 0 { 1 } else { 0 };

        neg + 1 + self.unsigned_abs().checked_ilog10().unwrap_or_default() as usize
    }
}
proptest_json_serialized_len!(i32);

impl JsonSerializedLen for i64 {
    fn json_serialized_len(&self) -> usize {
        let neg = if *self < 0 { 1 } else { 0 };

        neg + 1 + self.unsigned_abs().checked_ilog10().unwrap_or_default() as usize
    }
}
proptest_json_serialized_len!(i64);

impl JsonSerializedLen for String {
    fn json_serialized_len(&self) -> usize {
        2 + self
            .chars()
            .map(|c| match c {
                '"' | '\\' => 2,
                '\u{000B}' => 6,
                '\u{0008}'..='\u{000D}' => 2,
                '\u{0000}'..='\u{001F}' => 6,
                _ => c.len_utf8(),
            })
            .sum::<usize>()
    }
}
proptest_json_serialized_len!(String, string, {
    proptest::string::string_regex("\\p{C}*").unwrap()
});

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
proptest_json_serialized_len!(Option<bool>, option_bool);
proptest_json_serialized_len!(
    Option<alloy_primitives::FixedBytes<20>>,
    option_fixed_bytes_20
);
proptest_json_serialized_len!(
    Option<alloy_primitives::FixedBytes<32>>,
    option_fixed_bytes_32
);

impl JsonSerializedLen for Box<serde_json::value::RawValue> {
    fn json_serialized_len(&self) -> usize {
        self.get().len()
    }
}
proptest_json_serialized_len!(
    Box<serde_json::value::RawValue>,
    box_serde_json_value_raw_value,
    {
        proptest_json_serialized::strategy_serde_json_value()
            .prop_map(|value| serde_json::value::to_raw_value(&value).unwrap())
    }
);

impl<const N: usize> JsonSerializedLen for alloy_primitives::FixedBytes<N> {
    fn json_serialized_len(&self) -> usize {
        // enclosing double quotes
        2
        // 0x
        + 2
        // bytes hex encoded
        + 2 * N
    }
}
proptest_json_serialized_len!(alloy_primitives::FixedBytes<20>, fixed_bytes_20);
proptest_json_serialized_len!(alloy_primitives::FixedBytes<32>, fixed_bytes_32);

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

impl JsonSerializedLen for alloy_rpc_types::Log {
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
proptest_json_serialized_len!(alloy_rpc_types::Log, alloy_rpc_types__log, {
    proptest_json_serialized::strategy_arbitrary()
});

impl JsonSerializedLen for crate::types::jsonrpc::RequestId {
    fn json_serialized_len(&self) -> usize {
        match self {
            super::jsonrpc::RequestId::Number(number) => number.json_serialized_len(),
            super::jsonrpc::RequestId::String(string) => string.json_serialized_len(),
            super::jsonrpc::RequestId::Null => 4,
        }
    }
}
proptest_json_serialized_len!(
    crate::types::jsonrpc::RequestId,
    crate__types__jsonrpc__request_id,
    {
        use crate::types::jsonrpc::RequestId;

        prop_oneof![
            any::<i64>().prop_map(RequestId::Number),
            any::<String>().prop_map(RequestId::String),
            Just(RequestId::Null),
        ]
    }
);

impl JsonSerializedLen for crate::types::jsonrpc::JsonRpcError {
    fn json_serialized_len(&self) -> usize {
        const BASE_JSON: &str = r#"{"code":,"message":}"#;

        let Self {
            code,
            message,
            data,
        } = self;

        BASE_JSON.len()
            + code.json_serialized_len()
            + message.json_serialized_len()
            + data
                .as_ref()
                .map_or(0, |data| "\"data\":,".len() + data.json_serialized_len())
    }
}
proptest_json_serialized_len!(
    crate::types::jsonrpc::JsonRpcError,
    crate__types__jsonrpc__json_rpc_error,
    {
        (
            any::<i32>(),
            any::<String>(),
            prop_oneof![
                Just(None),
                proptest_json_serialized::strategy_serde_json_value().prop_map(Some)
            ],
        )
            .prop_map(
                |(code, message, data)| crate::types::jsonrpc::JsonRpcError {
                    code,
                    message,
                    data: data.map(|data| serde_json::value::to_raw_value(&data).unwrap()),
                },
            )
    }
);

impl JsonSerializedLen for crate::types::jsonrpc::Response {
    fn json_serialized_len(&self) -> usize {
        const BASE_JSON: &str = r#"{"jsonrpc":,"id":}"#;

        let Self {
            jsonrpc,
            result,
            error,
            id,
        } = self;

        BASE_JSON.len()
            + JsonSerializedLen::json_serialized_len(jsonrpc)
            + result.as_ref().map_or(0, |result| {
                "\"result\":,".len() + result.json_serialized_len()
            })
            + error
                .as_ref()
                .map_or(0, |error| "\"error\":,".len() + error.json_serialized_len())
            + id.json_serialized_len()
    }
}
proptest_json_serialized_len!(
    crate::types::jsonrpc::Response,
    crate__types__jsonrpc__response,
    {
        (
            any::<String>(),
            prop_oneof![
                Just(None),
                proptest_json_serialized::strategy_serde_json_value().prop_map(Some)
            ],
            prop_oneof![
                Just(None),
                test_json_serialized_len__crate__types__jsonrpc__json_rpc_error::strategy()
                    .prop_map(Some),
            ],
            test_json_serialized_len__crate__types__jsonrpc__request_id::strategy(),
        )
            .prop_map(
                |(jsonrpc, result, error, id)| crate::types::jsonrpc::Response {
                    jsonrpc,
                    result: result.map(|result| serde_json::value::to_raw_value(&result).unwrap()),
                    error,
                    id,
                },
            )
    }
);
