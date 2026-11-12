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
use alloy_primitives::{Address, Log as PrimitiveLog, Signature, B256};
use monad_transaction::{TxEnvelope, TxMonad, TxType};

use super::*;

fn receipt(ty: TxType, logs: Vec<PrimitiveLog>, index: u64) -> ReceiptWithLogIndex {
    ReceiptWithLogIndex {
        receipt: ReceiptEnvelope::from_typed(
            ty,
            alloy_consensus::Receipt {
                status: true.into(),
                cumulative_gas_used: 21_000,
                logs,
            }
            .with_bloom(),
        ),
        starting_log_index: index,
    }
}

fn log(address: u8, topic: u8) -> PrimitiveLog {
    PrimitiveLog::new(
        Address::repeat_byte(address),
        vec![B256::repeat_byte(topic)],
        vec![0x12, 0x34].into(),
    )
    .unwrap()
}

#[test]
fn indexed_receipt_log_extraction_handles_native_and_ethereum_variants() {
    let filter = FilteredParams::new(Some(
        Filter::new()
            .address(Address::repeat_byte(1))
            .event_signature(B256::repeat_byte(2)),
    ));
    for ty in [
        TxType::Legacy,
        TxType::Eip2930,
        TxType::Eip1559,
        TxType::Eip4844,
        TxType::Eip7702,
        TxType::Monad,
    ] {
        let parsed = parse_receipt_envelope(
            B256::repeat_byte(5),
            42,
            1234,
            7,
            B256::repeat_byte(6),
            receipt(ty, vec![log(1, 3), log(1, 2), log(4, 2)], 100),
        );
        let expected = parsed.logs()[1].clone();
        assert_eq!(
            receipt_envelope_to_logs_iter(parsed, &filter).collect::<Vec<_>>(),
            vec![expected]
        );
    }
}

#[test]
fn native_receipt_log_extraction_handles_empty_receipts_and_filters() {
    for filter in [
        FilteredParams::default(),
        FilteredParams::new(Some(Filter::new())),
        FilteredParams::new(Some(Filter::new().address(Address::repeat_byte(9)))),
    ] {
        let parsed = parse_receipt_envelope(
            B256::ZERO,
            42,
            1234,
            0,
            B256::ZERO,
            receipt(TxType::Monad, vec![], 0),
        );
        assert_eq!(receipt_envelope_to_logs_iter(parsed, &filter).count(), 0);
    }
    let parsed = parse_receipt_envelope(
        B256::ZERO,
        42,
        1234,
        0,
        B256::ZERO,
        receipt(TxType::Monad, vec![log(1, 2)], 0),
    );
    assert_eq!(
        receipt_envelope_to_logs_iter(parsed.clone(), &FilteredParams::default()).count(),
        1
    );
    assert_eq!(
        receipt_envelope_to_logs_iter(
            parsed,
            &FilteredParams::new(Some(Filter::new().address(Address::repeat_byte(9))))
        )
        .count(),
        0
    );
}

#[test]
fn mixed_block_log_scan_preserves_canonical_hashes_order_and_metadata() {
    let native: TxEnvelope = Signed::new_unhashed(
        TxMonad {
            chain_id: U256::from(20143),
            gas_limit: 21_000,
            ..Default::default()
        },
        Signature::new(U256::from(1), U256::from(1), false),
    )
    .into();
    let ethereum = TxEnvelope::default();
    let hashes = [*ethereum.hash(), *native.hash()];
    let transactions = vec![ethereum, native]
        .into_iter()
        .map(|tx| TxEnvelopeWithSender {
            tx,
            sender: Address::repeat_byte(8),
        })
        .collect();
    let header = BlockHeader {
        hash: B256::repeat_byte(5),
        header: RlpHeader {
            number: 42,
            timestamp: 1234,
            ..Default::default()
        },
    };
    let receipts = vec![
        receipt(TxType::Legacy, vec![log(1, 2), log(1, 3)], 0),
        receipt(TxType::Monad, vec![log(4, 2), log(1, 2), log(1, 3)], 2),
    ];
    let filter = FilteredParams::new(Some(
        Filter::new()
            .address(Address::repeat_byte(1))
            .event_signature(B256::repeat_byte(2)),
    ));
    let (number, logs) = header_transactions_receipts_to_block_number_logs(
        header.clone(),
        transactions,
        receipts,
        &filter,
    )
    .unwrap();
    assert_eq!(number, 42);
    let logs: Vec<_> = logs.collect();
    assert_eq!(logs.len(), 2);
    for (i, entry) in logs.iter().enumerate() {
        assert_eq!(entry.transaction_hash, Some(hashes[i]));
        assert_eq!(entry.transaction_index, Some(i as u64));
        assert_eq!(entry.log_index, Some([0, 3][i]));
        assert_eq!(entry.block_hash, Some(header.hash));
        assert_eq!(entry.block_number, Some(42));
        assert_eq!(entry.block_timestamp, Some(1234));
        assert!(!entry.removed);
        assert_eq!(entry.inner, log(1, 2));
    }
}
