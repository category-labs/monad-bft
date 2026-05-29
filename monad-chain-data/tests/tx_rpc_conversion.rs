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
use alloy_primitives::{Signature, TxKind, U256};
use monad_chain_data::{Address, Bytes, TxEntry, B256};

#[test]
fn to_rpc_transaction_carries_envelope_sender_and_block_context() {
    let recipient = Address::repeat_byte(0xaa);

    let signed = TxLegacy {
        chain_id: Some(1),
        nonce: 7,
        gas_price: 10,
        gas_limit: 21_000,
        to: TxKind::Call(recipient),
        value: U256::from(42u64),
        input: Bytes::from_static(&[0x11, 0x22, 0x33, 0x44, 0x55]),
    }
    .into_signed(Signature::test_signature());

    let mut signed_tx_bytes = Vec::new();
    TxEnvelope::Legacy(signed).encode_2718(&mut signed_tx_bytes);

    let tx = TxEntry {
        block_number: 123,
        block_hash: B256::repeat_byte(0xbb),
        tx_idx: 4,
        tx_hash: B256::repeat_byte(0xcc),
        sender: Address::repeat_byte(0xdd),
        signed_tx_bytes: signed_tx_bytes.into(),
    };

    let rpc = tx.to_rpc_transaction().expect("convert");

    assert_eq!(rpc.block_number, Some(123));
    assert_eq!(rpc.block_hash, Some(B256::repeat_byte(0xbb)));
    assert_eq!(rpc.transaction_index, Some(4));
    assert_eq!(rpc.effective_gas_price, None);
    assert_eq!(rpc.inner.signer(), Address::repeat_byte(0xdd));

    // The inner envelope round-trips the original fields.
    let envelope = rpc.inner.inner();
    assert_eq!(tx.to().expect("to"), Some(recipient));
    assert_eq!(
        tx.selector().expect("selector"),
        Some([0x11, 0x22, 0x33, 0x44])
    );
    assert_eq!(envelope.tx_type() as u8, 0);
}
