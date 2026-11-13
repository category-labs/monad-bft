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

// Frozen conversion from master e844f6086acac664f4bd2a8ffcc99f3b5ce4b9af.
// Uses the unchanged CallRequest and creation-size validation; only the function
// signature and unused bindings are adapted for this test oracle.

use super::*;

pub(super) fn convert(
    call_request: CallRequest,
) -> Result<alloy_consensus::TxEnvelope, JsonRpcError> {
    let CallRequest {
        from: _,
        to,
        gas,
        gas_price_details,
        value,
        input: CallInput { input, data: _ },
        nonce,
        chain_id,
        access_list,
        authorization_list,
        max_fee_per_blob_gas: _,
        blob_versioned_hashes: _,
        transaction_type: _,
    } = call_request;

    let nonce = nonce
        .unwrap_or_default()
        .try_into()
        .map_err(|_| JsonRpcError::invalid_params())?;

    let gas_limit = gas
        .unwrap_or(Uint::from(u64::MAX))
        .try_into()
        .map_err(|_| JsonRpcError::invalid_params())?;

    let value = value.unwrap_or_default();
    let input = input.unwrap_or_default();

    // default signature as eth_call doesn't require it
    let signature = Signature::new(U256::ZERO, U256::ZERO, false);

    match gas_price_details {
        GasPriceDetails::Legacy { gas_price } => {
            let transaction = TxLegacy {
                chain_id: chain_id
                    .map(|id| id.try_into())
                    .transpose()
                    .map_err(|_| JsonRpcError::invalid_params())?,
                nonce,
                gas_price: gas_price
                    .try_into()
                    .map_err(|_| JsonRpcError::invalid_params())?,
                gas_limit,
                to: if let Some(to) = to {
                    TxKind::Call(to)
                } else {
                    // EIP-3860
                    check_contract_creation_size(Some(&input))?;
                    TxKind::Create
                },
                value,
                input,
            };

            Ok(transaction.into_signed(signature).into())
        }
        GasPriceDetails::Eip1559 {
            max_fee_per_gas,
            max_priority_fee_per_gas,
        } => {
            let chain_id = chain_id
                .unwrap_or_default()
                .try_into()
                .map_err(|_| JsonRpcError::invalid_params())?;

            let max_fee_per_gas = max_fee_per_gas
                .unwrap_or_default()
                .try_into()
                .map_err(|_| JsonRpcError::invalid_params())?;

            let max_priority_fee_per_gas = max_priority_fee_per_gas
                .unwrap_or_default()
                .try_into()
                .map_err(|_| JsonRpcError::invalid_params())?;

            let access_list = access_list.unwrap_or_default();

            if let Some(authorization_list) = authorization_list {
                let transaction = TxEip7702 {
                    chain_id,
                    nonce,
                    gas_limit,
                    max_fee_per_gas,
                    max_priority_fee_per_gas,
                    to: to.ok_or(JsonRpcError::invalid_params())?,
                    value,
                    access_list,
                    authorization_list,
                    input,
                };

                Ok(transaction.into_signed(signature).into())
            } else {
                let transaction = TxEip1559 {
                    chain_id,
                    nonce,
                    gas_limit,
                    max_fee_per_gas,
                    max_priority_fee_per_gas,
                    value,
                    to: if let Some(to) = to {
                        TxKind::Call(to)
                    } else {
                        // EIP-3860
                        check_contract_creation_size(Some(&input))?;
                        TxKind::Create
                    },
                    access_list,
                    input,
                };

                Ok(transaction.into_signed(signature).into())
            }
        }
    }
}
