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

use alloy_consensus::Signed;
use alloy_eips::Encodable2718;
use alloy_primitives::{Address, Signature, U256};
use alloy_rlp::Header;
use monad_eth_txpool_types::{EthTxPoolEventType, EthTxPoolIpcTx};
use monad_transaction::{TxEnvelope, TxMonad};
use serde::Serialize;

fn transaction() -> TxEnvelope {
    Signed::new_unhashed(
        TxMonad {
            chain_id: U256::from(20143),
            gas_limit: 21_000,
            ..Default::default()
        },
        Signature::new(U256::from(1), U256::from(1), false),
    )
    .into()
}

fn frame(list: bool, length: usize, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    Header {
        list,
        payload_length: length,
    }
    .encode(&mut out);
    out.extend_from_slice(payload);
    out
}

fn malformed_frames(tx: &TxEnvelope) -> Vec<Vec<u8>> {
    let raw = tx.encoded_2718();
    let mut frames = vec![raw.clone()];
    for length in 0..=raw.len() + 1 {
        if length != raw.len() {
            frames.push(frame(false, length, &raw));
        }
    }
    let suffix = [raw, vec![0x80]].concat();
    frames.push(alloy_rlp::encode(suffix.as_slice()));
    frames
}

#[test]
fn ipc_admission_requires_exact_transaction_framing() {
    let tx = transaction();
    let ipc = EthTxPoolIpcTx::new_with_default_priority(tx.clone(), vec![1, 2, 3]);
    let encoded = alloy_rlp::encode(&ipc);
    let decoded: EthTxPoolIpcTx = alloy_rlp::decode_exact(&encoded).unwrap();
    assert_eq!(decoded.tx, tx);
    assert_eq!(decoded.tx.hash(), tx.hash());
    assert_eq!(decoded.tx.signature_hash(), tx.signature_hash());
    assert_eq!(alloy_rlp::encode(&decoded), encoded);

    for malformed in malformed_frames(&tx) {
        let fields = [
            malformed,
            alloy_rlp::encode(ipc.priority),
            alloy_rlp::encode(ipc.extra_data.as_slice()),
        ]
        .concat();
        let encoded = frame(true, fields.len(), &fields);
        assert!(alloy_rlp::decode_exact::<EthTxPoolIpcTx>(encoded).is_err());
    }
}

#[test]
fn ipc_insert_events_reject_raw_and_misframed_transaction_bytes() {
    // The event's transaction field is a byte vector containing network RLP.
    #[derive(Serialize)]
    enum WireEvent {
        Insert {
            address: Address,
            owned: bool,
            tx: Vec<u8>,
        },
    }

    let tx = transaction();
    let address = Address::repeat_byte(0x11);
    let serialize = |bytes| {
        bincode::serialize(&WireEvent::Insert {
            address,
            owned: true,
            tx: bytes,
        })
        .unwrap()
    };
    let expected = EthTxPoolEventType::Insert {
        address,
        owned: true,
        tx: tx.clone(),
    };
    let encoded = serialize(alloy_rlp::encode(&tx));
    assert_eq!(bincode::serialize(&expected).unwrap(), encoded);
    assert_eq!(
        bincode::deserialize::<EthTxPoolEventType>(&encoded).unwrap(),
        expected
    );
    for bytes in malformed_frames(&tx) {
        assert!(bincode::deserialize::<EthTxPoolEventType>(&serialize(bytes)).is_err());
    }
}
