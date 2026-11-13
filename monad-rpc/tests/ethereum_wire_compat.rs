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

//! Compare the native-aware RPC representations with the Alloy types used by
//! master e844f6086acac664f4bd2a8ffcc99f3b5ce4b9af (Alloy 2.0.4 in Cargo.lock).
//! Blob cases exercise representation compatibility, not Monad admission.

use alloy_consensus::{
    transaction::{Recovered, SignerRecoverable},
    Eip658Value, Receipt, ReceiptEnvelope as EthereumReceipt, ReceiptWithBloom,
    SignableTransaction, Transaction as _, TxEip1559, TxEip2930, TxEip4844, TxEip7702,
    TxEnvelope as EthereumTransaction, TxLegacy,
};
use alloy_eips::{
    eip2930::{AccessList, AccessListItem},
    eip7702::Authorization,
    Decodable2718, Encodable2718,
};
use alloy_primitives::{Address, Bloom, Bytes, Signature, TxKind, B256, U256};
use alloy_rpc_types::{BlockTransactions, Log};
use monad_transaction::{rpc, ReceiptEnvelope, TxEnvelope};
use serde_json::{json, Value};

const CHAIN_ID: u64 = 20143;
const BASE_FEE: u64 = 100;

fn signature() -> Signature {
    Signature::new(U256::ONE, U256::ONE, false)
}

fn transactions() -> Vec<(&'static str, EthereumTransaction)> {
    let to = Address::repeat_byte(0x11);
    let input = Bytes::from_static(&[0, 1, 0xff]);
    let access_list = AccessList(vec![AccessListItem {
        address: to,
        storage_keys: vec![B256::ZERO, B256::repeat_byte(0x22)],
    }]);
    let mut result = Vec::new();
    for (name, chain_id, destination) in [
        ("unprotected legacy", None, TxKind::Call(to)),
        ("protected legacy creation", Some(CHAIN_ID), TxKind::Create),
    ] {
        result.push((
            name,
            TxLegacy {
                chain_id,
                nonce: 7,
                gas_price: u128::MAX,
                gas_limit: 100_000,
                to: destination,
                value: U256::ONE << 200,
                input: input.clone(),
            }
            .into_signed(signature())
            .into(),
        ));
    }
    result.push((
        "access-list",
        TxEip2930 {
            chain_id: CHAIN_ID,
            nonce: 7,
            gas_price: u128::MAX,
            gas_limit: 100_000,
            to: TxKind::Call(to),
            value: U256::ONE << 200,
            input: input.clone(),
            access_list: access_list.clone(),
        }
        .into_signed(signature())
        .into(),
    ));
    result.push((
        "dynamic-fee creation",
        TxEip1559 {
            chain_id: CHAIN_ID,
            nonce: 7,
            max_fee_per_gas: u128::MAX,
            max_priority_fee_per_gas: 3,
            gas_limit: 100_000,
            to: TxKind::Create,
            value: U256::ONE << 200,
            input: input.clone(),
            access_list: access_list.clone(),
        }
        .into_signed(signature())
        .into(),
    ));
    result.push((
        "blob",
        TxEip4844 {
            chain_id: CHAIN_ID,
            nonce: 7,
            max_fee_per_gas: u128::MAX,
            max_priority_fee_per_gas: 3,
            gas_limit: 100_000,
            to,
            value: U256::ONE << 200,
            input: input.clone(),
            access_list: access_list.clone(),
            max_fee_per_blob_gas: u128::MAX,
            blob_versioned_hashes: vec![B256::repeat_byte(1)],
        }
        .into_signed(signature())
        .into(),
    ));
    result.push((
        "authorization-list",
        TxEip7702 {
            chain_id: CHAIN_ID,
            nonce: 7,
            max_fee_per_gas: u128::MAX,
            max_priority_fee_per_gas: 3,
            gas_limit: 100_000,
            to,
            value: U256::ONE << 200,
            input,
            access_list,
            authorization_list: vec![Authorization {
                chain_id: U256::from(CHAIN_ID),
                address: to,
                nonce: 4,
            }
            .into_signed(signature())],
        }
        .into_signed(signature())
        .into(),
    ));
    result
}

fn rpc_transactions(
    tx: EthereumTransaction,
    mined: bool,
) -> (alloy_rpc_types::Transaction, rpc::Transaction) {
    let effective_gas_price = mined.then(|| tx.effective_gas_price(Some(BASE_FEE)));
    let original = alloy_rpc_types::Transaction {
        inner: Recovered::new_unchecked(tx.clone(), Address::repeat_byte(0x33)),
        block_hash: mined.then_some(B256::repeat_byte(0x44)),
        block_number: mined.then_some(123),
        transaction_index: mined.then_some(2),
        effective_gas_price,
        block_timestamp: mined.then_some(1_788_359_400),
    };
    let current = rpc::Transaction {
        inner: Recovered::new_unchecked(tx.into(), original.inner.signer()),
        block_hash: original.block_hash,
        block_number: original.block_number,
        transaction_index: original.transaction_index,
        effective_gas_price: effective_gas_price.map(U256::from),
        block_timestamp: original.block_timestamp,
    };
    (original, current)
}

#[test]
fn transaction_json_matches_master_for_all_ethereum_types_and_pending_metadata() {
    for (name, tx) in transactions() {
        for mined in [false, true] {
            let (original, current) = rpc_transactions(tx.clone(), mined);
            let expected = serde_json::to_value(&original).unwrap();
            assert_eq!(
                serde_json::to_value(&current).unwrap(),
                expected,
                "{name}, mined={mined}"
            );
            let decoded: rpc::Transaction = serde_json::from_value(expected.clone()).unwrap();
            assert_eq!(
                serde_json::to_value(decoded).unwrap(),
                expected,
                "{name} JSON read"
            );
        }
    }
}

#[test]
fn ethereum_submission_hash_signature_and_raw_bytes_match_master() {
    for (name, original) in transactions() {
        let raw = original.encoded_2718();
        let current = TxEnvelope::decode_submission(&raw).unwrap();
        assert_eq!(current.encoded_2718(), raw, "{name}");
        assert_eq!(current.tx_hash(), original.tx_hash(), "{name}");
        assert_eq!(
            current.signature_hash(),
            original.signature_hash(),
            "{name}"
        );
        assert_eq!(
            current.recover_signer().unwrap(),
            original.recover_signer().unwrap(),
            "{name}"
        );
        assert_eq!(
            alloy_rlp::encode(&current),
            alloy_rlp::encode(&original),
            "{name} block RLP"
        );
    }
}

#[test]
fn ethereum_effective_fees_match_master_at_caps_and_integer_boundaries() {
    let mut txs = transactions();
    for (max_fee_per_gas, max_priority_fee_per_gas) in
        [(0, 0), (1, 0), (100, 3), (100, 200), (u128::MAX, u128::MAX)]
    {
        txs.push((
            "dynamic fee boundary",
            TxEip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
                ..Default::default()
            }
            .into_signed(signature())
            .into(),
        ));
    }
    for (name, original) in txs {
        let current = TxEnvelope::from(original.clone());
        for base_fee in [
            None,
            Some(0),
            Some(99),
            Some(100),
            Some(101),
            Some(u64::MAX),
        ] {
            assert_eq!(
                current.effective_gas_price(base_fee),
                U256::from(original.effective_gas_price(base_fee)),
                "{name}: price with base fee {base_fee:?}"
            );
            if let Some(base_fee) = base_fee {
                assert_eq!(
                    current.effective_tip_per_gas(base_fee),
                    original.effective_tip_per_gas(base_fee).map(U256::from),
                    "{name}: tip with base fee {base_fee}"
                );
            }
        }
    }
}

#[test]
fn ethereum_blob_sidecar_framing_matches_master() {
    let mut sidecar: alloy_consensus::TxEip4844WithSidecar = Default::default();
    sidecar.tx.chain_id = CHAIN_ID;
    sidecar.tx.gas_limit = 100_000;
    sidecar.tx.max_fee_per_gas = 200;
    let original: EthereumTransaction =
        alloy_consensus::TxEip4844Variant::TxEip4844WithSidecar(sidecar)
            .into_signed(signature())
            .into();
    let raw = original.encoded_2718();
    let current = TxEnvelope::decode_submission(&raw).unwrap();
    assert_eq!(current.encoded_2718(), raw);
    assert_eq!(current.tx_hash(), original.tx_hash());
    assert_eq!(
        serde_json::to_value(current).unwrap(),
        serde_json::to_value(original).unwrap()
    );
    for changed in [raw[..raw.len() - 1].to_vec(), [raw, vec![0]].concat()] {
        assert_eq!(
            TxEnvelope::decode_submission(&changed).is_ok(),
            EthereumTransaction::decode_2718_exact(&changed).is_ok()
        );
    }
}

#[test]
fn malformed_ethereum_submissions_keep_master_acceptance_rules() {
    for (name, tx) in transactions() {
        let raw = tx.encoded_2718();
        let mut candidates = vec![vec![], vec![0x7f], vec![0x80], vec![0xc0]];
        for length in 0..raw.len() {
            candidates.push(raw[..length].to_vec());
        }
        for suffix in [vec![0], vec![0xc0], raw.clone()] {
            candidates.push([raw.clone(), suffix].concat());
        }
        for raw in candidates {
            assert_eq!(
                TxEnvelope::decode_submission(&raw).is_ok(),
                EthereumTransaction::decode_2718_exact(&raw).is_ok(),
                "{name}: {}",
                alloy_primitives::hex::encode(&raw),
            );
        }
    }
}

fn receipts(status: Eip658Value) -> Vec<(EthereumReceipt<Log>, ReceiptEnvelope<Log>)> {
    let inner = ReceiptWithBloom {
        receipt: Receipt {
            status,
            cumulative_gas_used: 100_000,
            logs: vec![Log {
                inner: alloy_primitives::Log::new_unchecked(
                    Address::repeat_byte(0x55),
                    vec![B256::repeat_byte(0x66)],
                    Bytes::from_static(&[0, 1]),
                ),
                block_hash: Some(B256::repeat_byte(0x44)),
                block_number: Some(123),
                transaction_hash: Some(B256::repeat_byte(0x77)),
                transaction_index: Some(2),
                log_index: Some(3),
                removed: false,
                ..Default::default()
            }],
        },
        logs_bloom: Bloom::repeat_byte(0x88),
    };
    vec![
        (
            EthereumReceipt::Legacy(inner.clone()),
            ReceiptEnvelope::Legacy(inner.clone()),
        ),
        (
            EthereumReceipt::Eip2930(inner.clone()),
            ReceiptEnvelope::Eip2930(inner.clone()),
        ),
        (
            EthereumReceipt::Eip1559(inner.clone()),
            ReceiptEnvelope::Eip1559(inner.clone()),
        ),
        (
            EthereumReceipt::Eip4844(inner.clone()),
            ReceiptEnvelope::Eip4844(inner.clone()),
        ),
        (
            EthereumReceipt::Eip7702(inner.clone()),
            ReceiptEnvelope::Eip7702(inner),
        ),
    ]
}

fn rpc_receipts(
    original: EthereumReceipt<Log>,
    current: ReceiptEnvelope<Log>,
    creation: bool,
) -> (alloy_rpc_types::TransactionReceipt, rpc::TransactionReceipt) {
    let blob = matches!(original, EthereumReceipt::Eip4844(_));
    let original = alloy_rpc_types::TransactionReceipt {
        inner: original,
        transaction_hash: B256::repeat_byte(0x77),
        transaction_index: Some(2),
        block_hash: Some(B256::repeat_byte(0x44)),
        block_number: Some(123),
        gas_used: 21_000,
        effective_gas_price: u128::MAX,
        blob_gas_used: blob.then_some(131_072),
        blob_gas_price: blob.then_some(u128::MAX),
        from: Address::repeat_byte(0x33),
        to: (!creation).then_some(Address::repeat_byte(0x11)),
        contract_address: creation.then_some(Address::repeat_byte(0x99)),
    };
    let current = rpc::TransactionReceipt {
        inner: current,
        transaction_hash: original.transaction_hash,
        transaction_index: original.transaction_index,
        block_hash: original.block_hash,
        block_number: original.block_number,
        gas_used: original.gas_used,
        effective_gas_price: U256::from(original.effective_gas_price),
        blob_gas_used: original.blob_gas_used,
        blob_gas_price: original.blob_gas_price.map(U256::from),
        from: original.from,
        to: original.to,
        contract_address: original.contract_address,
    };
    (original, current)
}

#[test]
fn receipt_json_and_raw_bytes_match_master_for_success_revert_and_creation() {
    for status in [false, true] {
        for (original, current) in receipts(status.into()) {
            let original_raw = original.clone().map_logs(|log| log.inner);
            let current_raw = current.clone().map_logs(|log| log.inner);
            assert_eq!(current_raw.encoded_2718(), original_raw.encoded_2718());
            assert_eq!(
                alloy_rlp::encode(&current_raw),
                alloy_rlp::encode(&original_raw)
            );
            for creation in [false, true] {
                let (original, current) = rpc_receipts(original.clone(), current.clone(), creation);
                let expected = serde_json::to_value(original).unwrap();
                assert_eq!(serde_json::to_value(current).unwrap(), expected);
                let decoded: rpc::TransactionReceipt =
                    serde_json::from_value(expected.clone()).unwrap();
                assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
            }
        }
    }
}

#[test]
fn historical_legacy_receipt_root_matches_master() {
    let (original, current) = receipts(Eip658Value::PostState(B256::repeat_byte(0xaa))).remove(0);
    assert_eq!(
        serde_json::to_value(&current).unwrap(),
        serde_json::to_value(&original).unwrap()
    );
    assert_eq!(
        current.map_logs(|log| log.inner).encoded_2718(),
        original.map_logs(|log| log.inner).encoded_2718()
    );
}

#[test]
fn full_and_hash_only_block_json_match_master() {
    let (original_txs, current_txs): (Vec<_>, Vec<_>) = transactions()
        .into_iter()
        .map(|(_, tx)| rpc_transactions(tx, true))
        .unzip();
    let original: alloy_rpc_types::Block = alloy_rpc_types::Block {
        transactions: BlockTransactions::Full(original_txs),
        ..Default::default()
    };
    let current: rpc::Block = rpc::Block {
        transactions: BlockTransactions::Full(current_txs),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(current).unwrap(),
        serde_json::to_value(&original).unwrap()
    );
    let hashes = original.transactions.hashes().collect::<Vec<_>>();
    let original = alloy_rpc_types::Block::<alloy_rpc_types::Transaction> {
        transactions: BlockTransactions::Hashes(hashes.clone()),
        ..Default::default()
    };
    let current = rpc::Block::<rpc::Transaction> {
        transactions: BlockTransactions::Hashes(hashes),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(current).unwrap(),
        serde_json::to_value(original).unwrap()
    );
}

#[test]
fn fee_history_json_matches_master_with_optional_and_full_width_rewards() {
    for reward in [
        None,
        Some(vec![]),
        Some(vec![vec![]]),
        Some(vec![vec![0, 1, u128::MAX]]),
    ] {
        let original = alloy_rpc_types::FeeHistory {
            oldest_block: 123,
            base_fee_per_gas: vec![0, u128::MAX],
            gas_used_ratio: vec![0.5],
            base_fee_per_blob_gas: vec![0, 1],
            blob_gas_used_ratio: vec![0.0],
            reward: reward.clone(),
        };
        let current = rpc::FeeHistory {
            inner: alloy_rpc_types::FeeHistory {
                reward: None,
                ..original.clone()
            },
            reward: reward.map(|rows| {
                rows.into_iter()
                    .map(|row| row.into_iter().map(U256::from).collect())
                    .collect()
            }),
        };
        let expected = serde_json::to_value(original).unwrap();
        assert_eq!(serde_json::to_value(&current).unwrap(), expected);
        let decoded: rpc::FeeHistory = serde_json::from_value(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
    }
}

#[test]
fn receipt_reader_preserves_optional_fields_price_alias_and_padded_types() {
    for (original, current) in receipts(true.into()) {
        let (original, _) = rpc_receipts(original, current, false);
        let original = serde_json::to_value(original).unwrap();
        for alias in [false, true] {
            let mut value = original.clone();
            let fields = value.as_object_mut().unwrap();
            let price = fields.remove("effectiveGasPrice").unwrap();
            if alias {
                fields.insert("gasPrice".into(), price);
            }
            for key in [
                "blockHash",
                "blockNumber",
                "transactionIndex",
                "to",
                "contractAddress",
            ] {
                fields.remove(key);
            }
            let ty = fields["type"].as_str().unwrap().to_owned();
            fields.insert("type".into(), json!(ty.replacen("0x", "0x0", 1)));
            let expected: alloy_rpc_types::TransactionReceipt =
                serde_json::from_value(value.clone()).unwrap();
            let current: rpc::TransactionReceipt = serde_json::from_value(value).unwrap();
            assert_eq!(
                serde_json::to_value(current).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
    }
}

#[test]
fn receipt_reader_accepts_null_effective_gas_price_like_master() {
    let mut differences = Vec::new();
    for (original, current) in receipts(true.into()) {
        let (original, _) = rpc_receipts(original, current, false);
        let original = serde_json::to_value(original).unwrap();
        for field in ["effectiveGasPrice", "gasPrice"] {
            let mut value = original.clone();
            value.as_object_mut().unwrap().remove("effectiveGasPrice");
            value[field] = Value::Null;
            let expected: alloy_rpc_types::TransactionReceipt =
                serde_json::from_value(value.clone()).unwrap();
            match serde_json::from_value::<rpc::TransactionReceipt>(value.clone()) {
                Ok(current) => assert_eq!(
                    serde_json::to_value(current).unwrap(),
                    serde_json::to_value(expected).unwrap()
                ),
                Err(error) => differences.push(format!("type={}, {field}: {error}", value["type"])),
            }
        }
    }
    assert!(
        differences.is_empty(),
        "receipt compatibility differences:\n{}",
        differences.join("\n")
    );
}

#[test]
fn receipt_reader_requires_a_type_like_master() {
    let (original, current) = receipts(true.into()).remove(0);
    let (original, _) = rpc_receipts(original, current, false);
    let mut json = serde_json::to_value(original).unwrap();
    json.as_object_mut().unwrap().remove("type");
    assert!(serde_json::from_value::<alloy_rpc_types::TransactionReceipt>(json.clone()).is_err());
    assert!(serde_json::from_value::<rpc::TransactionReceipt>(json).is_err());
}

#[test]
fn native_control_still_serializes_an_explicit_new_type() {
    let transaction: TxEnvelope = monad_transaction::TxMonad {
        chain_id: U256::from(CHAIN_ID),
        gas_limit: 21_000,
        max_fee_per_gas: U256::from(200),
        ..Default::default()
    }
    .into_signed(signature())
    .into();
    assert_eq!(
        serde_json::to_value(transaction).unwrap()["type"],
        json!("0x40")
    );
}
