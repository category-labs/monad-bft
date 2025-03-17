use alloy_consensus::ReceiptEnvelope;
use alloy_primitives::B256;
use alloy_rlp::{Decodable, Encodable};
use monad_eth_testutil::{make_eip1559_tx, make_legacy_tx, make_receipt};
use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};

#[test]
fn test_rlp_encode_decode_legacy_tx() {
    let tx = make_legacy_tx(B256::repeat_byte(0xAu8), 50_000_000_000, 200_000_000, 1, 10);
    let sender = tx.recover_signer().unwrap();
    let tx_with_sender = TxEnvelopeWithSender { tx, sender };
    let mut rlp_encoded_tx = Vec::new();
    tx_with_sender.encode(&mut rlp_encoded_tx);

    let result = TxEnvelopeWithSender::decode(&mut rlp_encoded_tx.as_slice());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), tx_with_sender);
}

#[test]
fn test_rlp_encode_decode_eip1559_tx() {
    let tx = make_eip1559_tx(
        B256::repeat_byte(0xAu8),
        50_000_000_000,
        2_000_000_000,
        200_000_000,
        1,
        10,
    );
    let sender = tx.recover_signer().unwrap();
    let tx_with_sender = TxEnvelopeWithSender { tx, sender };
    let mut rlp_encoded_tx = Vec::new();
    tx_with_sender.encode(&mut rlp_encoded_tx);

    let result = TxEnvelopeWithSender::decode(&mut rlp_encoded_tx.as_slice());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), tx_with_sender);
}

#[test]
fn test_rlp_encode_decode_legacy_receipt() {
    let receipt = ReceiptEnvelope::Legacy(make_receipt(50));
    let receipt_with_index = ReceiptWithLogIndex {
        receipt,
        starting_log_index: 5,
    };
    let mut rlp_encoded_receipt = Vec::new();
    receipt_with_index.encode(&mut rlp_encoded_receipt);

    let result = ReceiptWithLogIndex::decode(&mut rlp_encoded_receipt.as_slice());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), receipt_with_index);
}

#[test]
fn test_rlp_encode_decode_eip1559_receipt() {
    let receipt = ReceiptEnvelope::Eip1559(make_receipt(50));
    let receipt_with_index = ReceiptWithLogIndex {
        receipt,
        starting_log_index: 5,
    };
    let mut rlp_encoded_receipt = Vec::new();
    receipt_with_index.encode(&mut rlp_encoded_receipt);

    let result = ReceiptWithLogIndex::decode(&mut rlp_encoded_receipt.as_slice());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), receipt_with_index);
}
