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

#![cfg(feature = "alloy-rpc-types-eth")]

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, Bytes, Signature, TxKind, B256, U256};
use monad_query_types::StoredTxEnvelope;

#[test]
fn to_rpc_transaction_carries_envelope_sender_and_block_context() {
    let recipient_address = Address::repeat_byte(0xaa);
    let stored_tx_hash = B256::repeat_byte(0xcc);
    let sender_address = Address::repeat_byte(0xdd);
    let block_hash = B256::repeat_byte(0xbb);
    let block_number = 123u64;
    let tx_index = 4u32;

    let signed_legacy = TxLegacy {
        chain_id: Some(1),
        nonce: 7,
        gas_price: 10,
        gas_limit: 21_000,
        to: TxKind::Call(recipient_address),
        value: U256::from(42u64),
        input: Bytes::from_static(&[0x11, 0x22, 0x33, 0x44, 0x55]),
    }
    .into_signed(Signature::test_signature());

    let mut encoded_bytes = Vec::new();
    TxEnvelope::Legacy(signed_legacy).encode_2718(&mut encoded_bytes);

    let tx_entry = StoredTxEnvelope {
        tx_hash: stored_tx_hash,
        sender: sender_address,
        signed_tx_bytes: encoded_bytes.into(),
    }
    .into_tx_entry(block_number, block_hash, tx_index)
    .expect("decode entry");

    let rpc_tx = tx_entry.to_rpc_transaction();

    assert_eq!(rpc_tx.block_number, Some(block_number));
    assert_eq!(rpc_tx.block_hash, Some(block_hash));
    assert_eq!(rpc_tx.transaction_index, Some(tx_index as u64));
    assert_eq!(rpc_tx.effective_gas_price, None);
    assert_eq!(rpc_tx.inner.signer(), sender_address);
    assert_eq!(rpc_tx.inner.inner().tx_type() as u8, 0);
    assert_eq!(*rpc_tx.inner.inner().tx_hash(), stored_tx_hash);

    assert_eq!(tx_entry.to(), Some(recipient_address));
    assert_eq!(tx_entry.selector(), Some([0x11, 0x22, 0x33, 0x44]));
}
