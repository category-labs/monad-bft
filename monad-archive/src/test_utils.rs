pub use alloy_consensus::{Receipt, SignableTransaction, TxEip1559};
pub use alloy_primitives::{Bloom, Log, LogData, B256};
pub use alloy_signer::SignerSync;
pub use alloy_signer_local::PrivateKeySigner;

pub use crate::{kvstore::memory::MemoryStorage, prelude::*};

pub fn mock_tx(salt: u64) -> TxEnvelopeWithSender {
    let tx = TxEip1559 {
        nonce: salt,
        gas_limit: 456 + salt,
        max_fee_per_gas: 789,
        max_priority_fee_per_gas: 135,
        ..Default::default()
    };
    let signer = PrivateKeySigner::from_bytes(&B256::from(U256::from(123))).unwrap();
    let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
    let tx = tx.into_signed(sig);
    TxEnvelopeWithSender {
        tx: tx.into(),
        sender: signer.address(),
    }
}

pub fn mock_rx(receipt_len: usize, cumulative_gas: u128) -> ReceiptWithLogIndex {
    let receipt = ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
        Receipt::<Log> {
            logs: vec![Log {
                address: Default::default(),
                data: LogData::new(
                    vec![],
                    std::iter::repeat(42)
                        .take(receipt_len)
                        .collect::<Vec<u8>>()
                        .into(),
                )
                .unwrap(),
            }],
            status: alloy_consensus::Eip658Value::Eip658(true),
            cumulative_gas_used: cumulative_gas,
        },
        Bloom::repeat_byte(b'a'),
    ));
    ReceiptWithLogIndex {
        receipt,
        starting_log_index: 0,
    }
}

pub fn mock_block(number: u64, transactions: Vec<TxEnvelopeWithSender>) -> Block {
    Block {
        header: Header {
            number,
            timestamp: 1234567,
            base_fee_per_gas: Some(100),
            ..Default::default()
        },
        body: BlockBody {
            transactions,
            ommers: vec![],
            withdrawals: Some(alloy_eips::eip4895::Withdrawals::default()),
        },
    }
}
