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

use std::fmt::Debug;

use alloy_consensus::{Receipt, Signed, TxEip1559, TxLegacy};
use alloy_eips::Encodable2718;
use alloy_primitives::{Address, Signature, U256};
use alloy_rlp::{Decodable, Encodable, Header};
use monad_eth_types::{EthBlockBody, ReceiptWithLogIndex, TxEnvelopeWithSender};
use monad_transaction::{ReceiptEnvelope, TxEnvelope, TxMonad, TxType};

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

fn list(payload: &[u8]) -> Vec<u8> {
    frame(true, payload.len(), payload)
}

fn transactions() -> Vec<TxEnvelope> {
    let signature = Signature::new(U256::from(1), U256::from(1), false);
    vec![
        Signed::new_unhashed(TxLegacy::default(), signature).into(),
        Signed::new_unhashed(TxEip1559::default(), signature).into(),
        Signed::new_unhashed(
            TxMonad {
                chain_id: U256::from(20143),
                gas_limit: 21_000,
                ..Default::default()
            },
            signature,
        )
        .into(),
    ]
}

fn record(payload: &[u8], metadata: &[u8]) -> Vec<u8> {
    list(&[alloy_rlp::encode(payload), metadata.to_vec()].concat())
}

fn assert_stored_formats<T: Decodable + Encodable + Debug + PartialEq>(
    value: T,
    raw: &[u8],
    network: &[u8],
    metadata: &[u8],
) {
    let archive = record(network, metadata);
    // Preserve the existing archive writer format; no storage version migration.
    assert_eq!(alloy_rlp::encode(&value), archive);
    for encoded in [record(raw, metadata), archive] {
        assert_eq!(alloy_rlp::decode_exact::<T>(&encoded).unwrap(), value);
        let stream = [encoded.clone(), encoded.clone()].concat();
        let mut rest = stream.as_slice();
        assert_eq!(T::decode(&mut rest).unwrap(), value);
        assert_eq!(rest, encoded);
        assert_eq!(T::decode(&mut rest).unwrap(), value);
        assert!(rest.is_empty());
    }
}

fn assert_record_bounds<T: Decodable>(raw: &[u8], network: &[u8], metadata: &[u8]) {
    for payload in [raw, network] {
        let field = alloy_rlp::encode(payload);
        let fields = [field.clone(), metadata.to_vec()].concat();
        for length in 0..fields.len() {
            // Metadata is present in the input but outside the record's boundary.
            assert!(T::decode(&mut frame(true, length, &fields).as_slice()).is_err());
        }
        for length in 0..=payload.len() + 1 {
            if length != payload.len() {
                let malformed_field = frame(false, length, payload);
                let malformed = list(&[malformed_field, metadata.to_vec()].concat());
                assert!(T::decode(&mut malformed.as_slice()).is_err());
            }
        }
        let payload_with_suffix = [payload.to_vec(), vec![0x80]].concat();
        assert!(T::decode(&mut record(&payload_with_suffix, metadata).as_slice()).is_err());
        let extra_element = list(&[fields, vec![0x80]].concat());
        assert!(T::decode(&mut extra_element.as_slice()).is_err());
        // A list cannot replace the record's byte string field.
        let wrong_kind = list(&[list(payload), metadata.to_vec()].concat());
        assert!(T::decode(&mut wrong_kind.as_slice()).is_err());
    }
    let extra_wrapper = alloy_rlp::encode(network);
    assert!(T::decode(&mut record(&extra_wrapper, metadata).as_slice()).is_err());
}

#[test]
fn transaction_database_and_archive_formats_preserve_identity_and_sender() {
    for tx in transactions() {
        let raw = tx.encoded_2718();
        let network = alloy_rlp::encode(&tx);
        let hash = *tx.hash();
        let digest = tx.signature_hash();
        let value = TxEnvelopeWithSender {
            tx,
            sender: Address::repeat_byte(0x11),
        };
        let metadata = alloy_rlp::encode(value.sender);
        let from_db: TxEnvelopeWithSender =
            alloy_rlp::decode_exact(record(&raw, &metadata)).unwrap();
        assert_eq!(*from_db.tx.hash(), hash);
        assert_eq!(from_db.tx.signature_hash(), digest);
        assert_stored_formats(value, &raw, &network, &metadata);
    }
}

#[test]
fn receipt_database_and_archive_formats_preserve_extensions_and_log_index() {
    for ty in [TxType::Legacy, TxType::Eip1559, TxType::Monad] {
        let receipt = ReceiptEnvelope::from_typed(ty, Receipt::default().with_bloom());
        let raw = receipt.encoded_2718();
        let network = alloy_rlp::encode(&receipt);
        let value = ReceiptWithLogIndex {
            receipt,
            starting_log_index: 257,
        };
        let metadata = alloy_rlp::encode(value.starting_log_index);
        assert_stored_formats(value, &raw, &network, &metadata);
    }
}

#[test]
fn transaction_database_decoder_bounds_both_record_and_envelope() {
    for tx in transactions() {
        assert_record_bounds::<TxEnvelopeWithSender>(
            &tx.encoded_2718(),
            &alloy_rlp::encode(tx),
            &alloy_rlp::encode(Address::repeat_byte(0x11)),
        );
    }
}

#[test]
fn receipt_database_decoder_bounds_both_record_and_envelope() {
    for ty in [TxType::Legacy, TxType::Eip1559, TxType::Monad] {
        let receipt = ReceiptEnvelope::from_typed(ty, Receipt::default().with_bloom());
        assert_record_bounds::<ReceiptWithLogIndex>(
            &receipt.encoded_2718(),
            &alloy_rlp::encode(receipt),
            &alloy_rlp::encode(257u64),
        );
    }
}

#[test]
fn block_body_requires_wrapped_typed_transactions_and_preserves_mixed_blocks() {
    let txs = transactions();
    let body = EthBlockBody {
        transactions: txs.clone().try_into().unwrap(),
        ..Default::default()
    };
    let bytes = alloy_rlp::encode(&body);
    let decoded: EthBlockBody = alloy_rlp::decode_exact(&bytes).unwrap();
    assert_eq!(decoded, body);
    assert_eq!(alloy_rlp::encode(decoded), bytes);

    for tx in txs.into_iter().filter(|tx| !tx.is_legacy()) {
        let raw = tx.encoded_2718();
        let mut malformed = vec![raw.clone()];
        for length in 0..=raw.len() + 1 {
            if length != raw.len() {
                malformed.push(frame(false, length, &raw));
            }
        }
        let with_suffix = [raw, vec![0x80]].concat();
        malformed.push(alloy_rlp::encode(with_suffix.as_slice()));
        for item in malformed {
            let tx_list = list(&item);
            let body = list(&[tx_list, vec![0xc0, 0xc0]].concat());
            assert!(alloy_rlp::decode_exact::<EthBlockBody>(body).is_err());
        }
    }
}
