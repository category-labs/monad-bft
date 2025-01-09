use std::collections::BTreeMap;

use monad_consensus_types::{
    block::{Block, BlockKind},
    payload::{ExecutionProtocol, FullTransactionList, Payload, RandaoReveal, TransactionPayload},
    quorum_certificate::QuorumCertificate,
};
use monad_crypto::{certificate_signature::CertificateKeyPair, NopKeyPair, NopSignature};
use monad_eth_block_policy::{compute_txn_max_value, EthValidatedBlock};
use monad_eth_tx::{EthFullTransactionList, EthSignedTransaction, EthTransaction};
use monad_eth_types::EthAddress;
use monad_secp::KeyPair;
use monad_testutil::signing::MockSignatures;
use monad_types::{Epoch, NodeId, Round, SeqNum};
use reth_primitives::{
    keccak256, revm_primitives::FixedBytes, sign_message, Address, Transaction, TransactionKind, TxLegacy, B256, U256
};

pub const ACCOUNTS: [(B256, Address); 3] = [
    (
        // 0xc29b3e29e33fe4612c946e72ffe4fcea013bf99b
        B256::new([0x71, 0xca, 0x04, 0x72, 0x4a, 0x6d, 0x89, 0x0c, 0xa9, 0x6b, 0xe3, 0xc2, 0xd3, 0xaa, 0x15, 0xdf, 0x5e, 0x16, 0x61, 0x9b, 0xec, 0x2b, 0xfe, 0x6d, 0x89, 0x10, 0x65, 0xfb, 0x5f, 0x70, 0xef, 0xf5]),
        Address(FixedBytes::<20>([0xc2, 0x9b, 0x3e, 0x29, 0xe3, 0x3f, 0xe4, 0x61, 0x2c, 0x94, 0x6e, 0x72, 0xff, 0xe4, 0xfc, 0xea, 0x01, 0x3b, 0xf9, 0x9b])),
    ),
    (
        // 0xf78357155A03e155e0EdFbC3aC5f4532C95367f6
        B256::new([0x07, 0xcb, 0x04, 0x0b, 0x0e, 0x2b, 0xda, 0xad, 0x5b, 0xad, 0x62, 0xd9, 0x43, 0x3f, 0x6a, 0x38, 0x80, 0x35, 0x80, 0x05, 0xcf, 0x05, 0x42, 0x60, 0xc7, 0xdd, 0xfc, 0x8d, 0x8a, 0xe1, 0x69, 0xf0]),
        Address(FixedBytes::<20>([0xf7, 0x83, 0x57, 0x15, 0x5A, 0x03, 0xe1, 0x55, 0xe0, 0xEd, 0xFb, 0xC3, 0xaC, 0x5f, 0x45, 0x32, 0xC9, 0x53, 0x67, 0xf6])),
    ),
    (
        // 0xB6A5df2311E4D3F5376619FD05224AAFe4352aB9
        B256::new([0xae, 0x20, 0x8c, 0xc6, 0xa2, 0x8d, 0xe2, 0x48, 0x17, 0x3a, 0x7b, 0xa8, 0x38, 0x5c, 0x3e, 0x1b, 0x98, 0x11, 0x16, 0x00, 0x99, 0xdc, 0x55, 0xfb, 0xf8, 0x60, 0x6c, 0xc9, 0x74, 0xb9, 0x6c, 0x72]),
        Address(FixedBytes::<20>([0xB6, 0xA5, 0xdf, 0x23, 0x11, 0xE4, 0xD3, 0xF5, 0x37, 0x66, 0x19, 0xFD, 0x05, 0x22, 0x4A, 0xAF, 0xe4, 0x35, 0x2a, 0xB9])),
    ),
];

pub fn make_tx(
    sender: FixedBytes<32>,
    gas_price: u128,
    gas_limit: u64,
    nonce: u64,
    input_len: usize,
) -> EthSignedTransaction {
    let input = vec![0; input_len];
    let transaction = Transaction::Legacy(TxLegacy {
        chain_id: Some(1337),
        nonce,
        gas_price,
        gas_limit,
        to: TransactionKind::Call(Address::repeat_byte(0u8)),
        value: 0.into(),
        input: input.into(),
    });

    let hash = transaction.signature_hash();

    let sender_secret_key = sender;
    let signature = sign_message(sender_secret_key, hash).expect("signature should always succeed");

    EthSignedTransaction::from_transaction_and_signature(transaction, signature)
}

pub fn secret_to_eth_address(mut secret: FixedBytes<32>) -> EthAddress {
    let kp = KeyPair::from_bytes(secret.as_mut_slice()).unwrap();
    let pubkey_bytes = kp.pubkey().bytes();
    assert!(pubkey_bytes.len() == 65);
    let hash = keccak256(&pubkey_bytes[1..]);
    EthAddress(Address::from_slice(&hash[12..]))
}

pub fn generate_block_with_txs(
    round: Round,
    seq_num: SeqNum,
    txs: Vec<EthSignedTransaction>,
) -> EthValidatedBlock<MockSignatures<NopSignature>> {
    let payload = {
        let full_txs = EthFullTransactionList(
            txs.clone()
                .into_iter()
                .map(|signed_txn| {
                    let sender_address = signed_txn.recover_signer().unwrap();
                    EthTransaction::from_signed_transaction(signed_txn, sender_address)
                })
                .collect(),
        );

        Payload {
            txns: TransactionPayload::List(FullTransactionList::new(full_txs.rlp_encode())),
        }
    };

    let keypair = NopKeyPair::from_bytes(rand::random::<[u8; 32]>().as_mut_slice()).unwrap();

    let block = Block::new(
        NodeId::new(keypair.pubkey()),
        0,
        Epoch(1),
        round,
        &ExecutionProtocol {
            state_root: Default::default(),
            seq_num,
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::new::<NopSignature>(Round(1), &keypair),
        },
        payload.get_id(),
        BlockKind::Executable,
        &QuorumCertificate::genesis_qc(),
    );

    let validated_txns: Vec<_> = txs
        .into_iter()
        .map(|tx| tx.into_ecrecovered().expect("tx is recoverable"))
        .collect();

    let nonces = validated_txns
        .iter()
        .map(|t| (EthAddress(t.signer()), t.nonce()))
        .collect();

    let txn_fees = validated_txns
        .iter()
        .map(|t| (EthAddress(t.signer()), compute_txn_max_value(t)))
        .fold(BTreeMap::new(), |mut costs, (address, cost)| {
            *costs.entry(address).or_insert(U256::ZERO) += cost;
            costs
        });

    EthValidatedBlock {
        block,
        orig_payload: payload,
        validated_txns,
        nonces,
        txn_fees,
    }
}

#[cfg(test)]
mod test {
    use reth_primitives::B256;

    use super::*;
    #[test]
    fn test_secret_to_eth_address() {
        let secret = B256::random();

        let eth_address_converted = secret_to_eth_address(secret);

        let tx = make_tx(secret, 0, 0, 0, 0);
        let eth_address_recovered = EthAddress(tx.recover_signer().unwrap());

        assert_eq!(eth_address_converted, eth_address_recovered);
    }
}
