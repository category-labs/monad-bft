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

use alloy_consensus::{
    SignableTransaction, Transaction as _, TxEip1559, TxEip2930, TxEip4844, TxEip7702, TxLegacy,
};
use alloy_eips::{Decodable2718, Encodable2718};
use alloy_primitives::{Signature, U256};

use super::*;

// Frozen ingress policy from master e844f6086acac664f4bd2a8ffcc99f3b5ce4b9af.
fn master_decode(
    raw: &[u8],
    chain: u64,
    allow_unprotected: bool,
    decode_error: fn() -> JsonRpcError,
) -> Result<alloy_consensus::TxEnvelope, JsonRpcError> {
    let tx = alloy_consensus::TxEnvelope::decode_2718_exact(raw).map_err(|_| decode_error())?;
    if !allow_unprotected && tx.chain_id().is_none() {
        return Err(JsonRpcError::with_message(
            ErrorCode::ServerError,
            "Unprotected transactions (pre-EIP155) are not allowed over RPC".to_string(),
        ));
    }
    if let Some(tx_chain) = tx.chain_id() {
        if tx_chain != chain {
            return Err(JsonRpcError::invalid_chain_id(chain, tx_chain));
        }
    }
    Ok(tx)
}

#[test]
fn ethereum_raw_submission_policy_and_errors_match_master() {
    let signature = Signature::new(U256::ONE, U256::ONE, false);
    let mut txs = Vec::<alloy_consensus::TxEnvelope>::new();
    for chain_id in [None, Some(20143), Some(143), Some(u64::MAX)] {
        txs.push(
            TxLegacy {
                chain_id,
                gas_limit: 21_000,
                ..Default::default()
            }
            .into_signed(signature)
            .into(),
        );
    }
    txs.push(
        TxEip1559 {
            chain_id: 20143,
            gas_limit: 21_000,
            ..Default::default()
        }
        .into_signed(signature)
        .into(),
    );
    txs.push(
        TxEip2930 {
            chain_id: 20143,
            ..Default::default()
        }
        .into_signed(signature)
        .into(),
    );
    txs.push(
        TxEip4844 {
            chain_id: 20143,
            ..Default::default()
        }
        .into_signed(signature)
        .into(),
    );
    txs.push(
        TxEip7702 {
            chain_id: 20143,
            ..Default::default()
        }
        .into_signed(signature)
        .into(),
    );
    let mut differences = Vec::new();
    for tx in txs {
        let raw = tx.encoded_2718();
        for raw in [
            raw.clone(),
            raw[..raw.len() - 1].to_vec(),
            [raw, vec![0]].concat(),
            vec![],
        ] {
            for chain in [143, 20143, u64::MAX] {
                for allow_unprotected in [false, true] {
                    // The asynchronous and synchronous methods deliberately use
                    // different decode-error codes; preserve both contracts.
                    for decode_error in [JsonRpcError::txn_decode_error as fn() -> _, || {
                        JsonRpcError::new(ErrorCode::TransactionUnready)
                    }] {
                        let expected = master_decode(&raw, chain, allow_unprotected, decode_error)
                            .map(|tx| *tx.tx_hash());
                        let actual =
                            validate_and_decode_tx(&raw, chain, allow_unprotected, decode_error)
                                .map(|tx| *tx.tx_hash());
                        if actual != expected {
                            differences.push(format!(
                                "chain={chain}, allow_unprotected={allow_unprotected}, raw={raw:?}: expected {expected:?}, got {actual:?}"
                            ));
                        }
                    }
                }
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} ingress compatibility differences; first examples:\n{}",
        differences.len(),
        differences
            .iter()
            .take(8)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn native_chain_rejection_preserves_full_width_id_and_activation_error() {
    let chain_id = U256::ONE << 200;
    let tx = monad_transaction::TxMonad {
        chain_id,
        gas_limit: 21_000,
        max_fee_per_gas: U256::from(200),
        ..Default::default()
    };
    let raw = tx.encode_submission(&Signature::new(U256::ONE, U256::ONE, false));
    for decode_error in [JsonRpcError::txn_decode_error as fn() -> _, || {
        JsonRpcError::new(ErrorCode::TransactionUnready)
    }] {
        assert_eq!(
            validate_and_decode_tx(&raw, 20143, false, decode_error).unwrap_err(),
            JsonRpcError::with_message(
                ErrorCode::TransactionRejected,
                format!("Invalid chain ID: expected 20143, got {chain_id}")
            )
        );
        assert_eq!(
            validate_and_decode_tx(&raw, 143, false, decode_error).unwrap_err(),
            JsonRpcError::with_message(
                ErrorCode::InvalidParams,
                "Native transactions are enabled only on devnet"
            )
        );
    }
}
