// Copyright (C) 2026 Category Labs, Inc.
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

use alloy_eips::Encodable2718;
use serde_json::{json, Value};

use super::*;

#[path = "master_ethereum_conversion.rs"]
mod master;

fn assert_same_conversion(value: Value) {
    let request: CallRequest = serde_json::from_value(value.clone()).unwrap();
    let expected = master::convert(request.clone()).map(|tx| tx.encoded_2718());
    let actual = TxEnvelope::try_from(request).map(|tx| tx.encoded_2718());
    assert_eq!(actual, expected, "request: {value}");
}

#[test]
fn ethereum_simulation_conversion_matches_master() {
    let to = "0x1111111111111111111111111111111111111111";
    let access_list = json!([{"address": to, "storageKeys": [format!("{:#x}", B256::ZERO)]}]);
    let authorization = alloy_eips::eip7702::Authorization {
        chain_id: U256::from(20143),
        address: to.parse().unwrap(),
        nonce: 3,
    }
    .into_signed(Signature::new(U256::ONE, U256::ONE, false));
    // Master infers Ethereum encoding from fee/auth fields, including for an
    // explicit type. Preserve that existing behavior outside the new 0x40 arm.
    for transaction_type in [
        None,
        Some("0x0"),
        Some("0x1"),
        Some("0x2"),
        Some("0x3"),
        Some("0x4"),
        Some("0x7f"),
    ] {
        for legacy in [false, true] {
            for destination in [None, Some(to)] {
                for access in [None, Some(json!([])), Some(access_list.clone())] {
                    for auth in [None, Some(json!([])), Some(json!([authorization]))] {
                        let mut request = json!({
                            "chainId": "0x4eaf", "nonce": "0x7", "gas": "0x186a0",
                            "value": "0x1", "input": "0x0001ff",
                        });
                        if let Some(ty) = transaction_type {
                            request["type"] = json!(ty);
                        }
                        if let Some(to) = destination {
                            request["to"] = json!(to);
                        }
                        if let Some(access) = &access {
                            request["accessList"] = access.clone();
                        }
                        if let Some(auth) = auth {
                            request["authorizationList"] = auth;
                        }
                        if legacy {
                            request["gasPrice"] = json!("0xc8");
                        } else {
                            request["maxFeePerGas"] = json!("0xc8");
                            request["maxPriorityFeePerGas"] = json!("0x3");
                        }
                        assert_same_conversion(request);
                    }
                }
            }
        }
    }
}

#[test]
fn ethereum_simulation_defaults_widths_and_errors_match_master() {
    assert_same_conversion(json!({}));
    assert_same_conversion(json!({"gasPrice": "0x0"}));
    let base = json!({"type": "0x2", "to": "0x1111111111111111111111111111111111111111"});
    for (key, value) in [
        ("gas", json!("0x0")),
        ("gas", json!(format!("{:#x}", u64::MAX))),
        ("gas", json!(format!("{:#x}", U256::ONE << 64))),
        ("nonce", json!(format!("{:#x}", u64::MAX))),
        ("chainId", json!(format!("{:#x}", u64::MAX))),
        ("value", json!(format!("{:#x}", U256::MAX))),
        ("maxFeePerGas", json!(format!("{:#x}", u128::MAX))),
        ("maxFeePerGas", json!(format!("{:#x}", U256::ONE << 128))),
        (
            "maxPriorityFeePerGas",
            json!(format!("{:#x}", U256::ONE << 128)),
        ),
        ("gasPrice", json!(format!("{:#x}", u128::MAX))),
        ("gasPrice", json!(format!("{:#x}", U256::ONE << 128))),
        ("maxFeePerBlobGas", json!("0x1")),
        (
            "blobVersionedHashes",
            json!([format!("{:#x}", B256::repeat_byte(1))]),
        ),
    ] {
        let mut request = base.clone();
        request[key] = value;
        assert_same_conversion(request);
    }
    let limit = 2 * MonadExecutionRevision::LATEST
        .execution_chain_params()
        .max_code_size;
    for size in [limit, limit + 1] {
        assert_same_conversion(
            json!({"gasPrice": "0x1", "input": format!("0x{}", "00".repeat(size))}),
        );
        assert_same_conversion(json!({"input": format!("0x{}", "00".repeat(size))}));
    }
}
