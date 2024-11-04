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
    keccak256, revm_primitives::FixedBytes, sign_message, Address, Transaction, TransactionKind,
    TxLegacy, U256,
};

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
