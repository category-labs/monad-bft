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

pub trait HeuristicSize {
    fn heuristic_json_len(&self) -> usize;
}

impl HeuristicSize for Log {
    fn heuristic_json_len(&self) -> usize {
        const HEURISTIC_SMALLEST_LOG_RESPONSE: &str = r#"{"address":"0x0000000000000000000000000000000000000000","blockHash":,"blockNumber":,"data":"0x","logIndex":,"removed":,"topics":[],"transactionHash":,"transactionIndex":}"#;

        HEURISTIC_SMALLEST_LOG_RESPONSE.len()
            + compute_heuristic_fixed_bytes_len(self.block_hash)
            + compute_heuristic_int_len(self.block_number)
            + self
                .block_timestamp
                .map(|t| "\"blockTimestamp\":,".len() + compute_heuristic_int_len(Some(t)))
                .unwrap_or_default()
            + 2 * self.inner.data.data.len()
            + compute_heuristic_int_len(self.log_index)
            + compute_heuristic_bool_len(self.removed)
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
            + compute_heuristic_fixed_bytes_len(self.transaction_hash)
            + compute_heuristic_int_len(self.transaction_index)
    }
}

fn compute_heuristic_bool_len(value: bool) -> usize {
    if value {
        4
    } else {
        5
    }
}

fn compute_heuristic_int_len(value: Option<u64>) -> usize {
    value
        .map(|index| {
            let bits = (64 - index.leading_zeros()) as usize;

            // enclosing double quotes
            2
                // 0x
                + 2
                // value hex encoded
                + ((bits + 3) / 4).max(1)
        })
        .unwrap_or(
            // null
            4,
        )
}

fn compute_heuristic_fixed_bytes_len<const N: usize>(value: Option<FixedBytes<N>>) -> usize {
    value
        .map(|_| {
            // enclosing double quotes
            2
                // 0x
                + 2
                // bytes hex encoded
               + 2 * N
        })
        .unwrap_or(
            // null
            4,
        )
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, LogData};
    use arbitrary::Unstructured;
    use proptest::{prelude::Strategy, proptest};

    use super::HeuristicSize;

    #[test]
    fn test_heuristic_log_len_empty() {
        let log = alloy_rpc_types::Log {
            inner: alloy_primitives::Log {
                address: Address::default(),
                data: LogData::empty(),
            },
            ..Default::default()
        };

        let heuristic_log_len = HeuristicSize::heuristic_json_len(&log);

        let serialized_log = serde_json::to_string(&log).unwrap();

        assert_eq!(
            heuristic_log_len,
            serialized_log.len(),
            "Heuristic log len does not match! Serialized log: {serialized_log:?}"
        );
    }

    fn arbitrary_log() -> impl Strategy<Value = alloy_rpc_types::Log> {
        proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256).prop_filter_map(
            "invalid arbitrary",
            |bytes| {
                let mut u = Unstructured::new(&bytes);
                u.arbitrary().ok()
            },
        )
    }

    proptest! {
        #[test]
        fn proptest_heuristic_log_len(log in arbitrary_log()) {
            let heuristic_log_len = HeuristicSize::heuristic_json_len(&log);

            let serialized_log = serde_json::to_string(&log).unwrap();

            assert_eq!(
                heuristic_log_len, serialized_log.len(),
                "Heuristic log len does not match! Serialized log: {serialized_log:?}"
            );
        }
    }
}
