use std::{collections::BTreeMap, iter::repeat};

use alloy_consensus::{
    transaction::Recovered, Eip658Value, Receipt, ReceiptWithBloom, SignableTransaction,
    Transaction, TxEip1559, TxEnvelope, TxLegacy,
};
use alloy_primitives::{keccak256, Address, Bloom, FixedBytes, Log, LogData, TxKind, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use monad_consensus_types::{
    block::{ConsensusBlockHeader, ConsensusFullBlock},
    payload::{ConsensusBlockBody, ConsensusBlockBodyInner, RoundSignature},
    quorum_certificate::QuorumCertificate,
};
use monad_crypto::{certificate_signature::CertificateKeyPair, NopKeyPair, NopSignature};
use monad_eth_block_policy::{compute_txn_max_value, EthValidatedBlock};
use monad_eth_types::EthBlockBody;
use monad_secp::KeyPair;
use monad_testutil::signing::MockSignatures;
use monad_types::{Epoch, NodeId, Round, SeqNum};

pub fn make_legacy_tx(
    sender: FixedBytes<32>,
    gas_price: u128,
    gas_limit: u64,
    nonce: u64,
    input_len: usize,
) -> TxEnvelope {
    let transaction = TxLegacy {
        chain_id: Some(1337),
        nonce,
        gas_price,
        gas_limit,
        to: TxKind::Call(Address::repeat_byte(0u8)),
        value: Default::default(),
        input: vec![0; input_len].into(),
    };

    let signer = PrivateKeySigner::from_bytes(&sender).unwrap();
    let signature = signer
        .sign_hash_sync(&transaction.signature_hash())
        .unwrap();
    transaction.into_signed(signature).into()
}

pub fn make_eip1559_tx(
    sender: FixedBytes<32>,
    max_fee_per_gas: u128,
    max_priority_fee_per_gas: u128,
    gas_limit: u64,
    nonce: u64,
    input_len: usize,
) -> TxEnvelope {
    let transaction = TxEip1559 {
        chain_id: 1337,
        nonce,
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        to: TxKind::Call(Address::repeat_byte(0u8)),
        value: Default::default(),
        access_list: Default::default(),
        input: vec![0; input_len].into(),
    };

    let signer = PrivateKeySigner::from_bytes(&sender).unwrap();
    let signature = signer
        .sign_hash_sync(&transaction.signature_hash())
        .unwrap();
    transaction.into_signed(signature).into()
}

pub fn make_receipt(logs_len: usize) -> ReceiptWithBloom {
    ReceiptWithBloom::new(
        Receipt::<Log> {
            logs: vec![Log {
                address: Default::default(),
                data: LogData::new(
                    vec![],
                    repeat(42).take(logs_len).collect::<Vec<u8>>().into(),
                )
                .unwrap(),
            }],
            status: Eip658Value::Eip658(true),
            cumulative_gas_used: 21000,
        },
        Bloom::repeat_byte(b'a'),
    )
}

pub fn secret_to_eth_address(mut secret: FixedBytes<32>) -> Address {
    let kp = KeyPair::from_bytes(secret.as_mut_slice()).unwrap();
    let pubkey_bytes = kp.pubkey().bytes();
    assert!(pubkey_bytes.len() == 65);
    let hash = keccak256(&pubkey_bytes[1..]);
    Address::from_slice(&hash[12..])
}

pub fn generate_block_with_txs(
    round: Round,
    seq_num: SeqNum,
    txs: Vec<Recovered<TxEnvelope>>,
) -> EthValidatedBlock<NopSignature, MockSignatures<NopSignature>> {
    let body = ConsensusBlockBody::new(ConsensusBlockBodyInner {
        execution_body: EthBlockBody {
            transactions: txs.iter().map(|tx| tx.tx().to_owned()).collect(),
            ommers: Vec::default(),
            withdrawals: Vec::default(),
        },
    });

    let keypair = NopKeyPair::from_bytes(rand::random::<[u8; 32]>().as_mut_slice()).unwrap();

    let header = ConsensusBlockHeader::new(
        NodeId::new(keypair.pubkey()),
        Epoch(1),
        round,
        Default::default(), // delayed_execution_results
        Default::default(), // execution_inputs
        body.get_id(),
        QuorumCertificate::genesis_qc(),
        seq_num,
        0,
        RoundSignature::new(Round(1), &keypair),
    );

    let nonces = txs.iter().map(|t| (t.signer(), t.nonce())).fold(
        BTreeMap::default(),
        |mut map, (address, nonce)| {
            match map.entry(address) {
                std::collections::btree_map::Entry::Vacant(v) => {
                    v.insert(nonce);
                }
                std::collections::btree_map::Entry::Occupied(mut o) => {
                    o.insert(nonce.max(*o.get()));
                }
            }

            map
        },
    );

    let txn_fees = txs
        .iter()
        .map(|t| (t.signer(), compute_txn_max_value(t)))
        .fold(BTreeMap::new(), |mut costs, (address, cost)| {
            *costs.entry(address).or_insert(U256::ZERO) += cost;
            costs
        });

    EthValidatedBlock {
        block: ConsensusFullBlock::new(header, body).expect("header doesn't match body"),
        validated_txns: txs,
        nonces,
        txn_fees,
    }
}

#[cfg(test)]
mod test {
    use alloy_primitives::B256;

    use super::*;
    #[test]
    fn test_secret_to_eth_address() {
        let secret = B256::repeat_byte(10);

        let eth_address_converted = secret_to_eth_address(secret);

        let tx = make_legacy_tx(secret, 0, 0, 0, 0);
        let eth_address_recovered = tx.recover_signer().unwrap();

        assert_eq!(eth_address_converted, eth_address_recovered);
    }
}
