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

use std::{
    collections::{BTreeMap, VecDeque},
    iter::repeat_n,
};

use alloy_consensus::{
    constants::EMPTY_WITHDRAWALS, proofs::calculate_transaction_root, transaction::Recovered,
    Eip658Value, Receipt, ReceiptWithBloom, SignableTransaction, Transaction, TxEip1559, TxEip7702,
    TxEnvelope, TxLegacy, EMPTY_OMMER_ROOT_HASH,
};
use alloy_eips::eip7702::{Authorization, SignedAuthorization};
use alloy_primitives::{keccak256, Address, Bloom, FixedBytes, Log, LogData, TxKind, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use monad_chain_config::{revision::ChainRevision, ChainConfig, MockChainConfig};
use monad_consensus_types::{
    block::{ConsensusBlockHeader, ConsensusFullBlock, TxnFee},
    payload::{ConsensusBlockBody, ConsensusBlockBodyInner, RoundSignature},
    quorum_certificate::QuorumCertificate,
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    NopKeyPair, NopSignature,
};
use monad_eth_block_policy::{
    compute_max_txn_cost, compute_txn_max_gas_cost,
    nonce_usage::{NonceUsage, NonceUsageMap},
    EthValidatedBlock,
};
use monad_eth_types::{EthBlockBody, EthExecutionProtocol, ProposedEthHeader};
use monad_secp::KeyPair;
use monad_testutil::signing::MockSignatures;
use monad_types::{Balance, Epoch, NodeId, Round, SeqNum};
use monad_validator::signature_collection::SignatureCollection;

const BASE_FEE: u64 = 100_000_000_000;

#[derive(Debug)]
pub struct ConsensusTestBlock<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub block: ConsensusFullBlock<ST, SCT, EthExecutionProtocol>,
    pub validated_txns: Vec<Recovered<TxEnvelope>>,
    pub nonce_usages: NonceUsageMap,
    pub txn_fees: BTreeMap<Address, TxnFee>,
}

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
    make_eip1559_tx_with_value(
        sender,
        0,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        gas_limit,
        nonce,
        input_len,
    )
}

pub fn make_eip1559_tx_with_value(
    sender: FixedBytes<32>,
    value: u128,
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
        value: U256::from(value),
        access_list: Default::default(),
        input: vec![0; input_len].into(),
    };

    let signer = PrivateKeySigner::from_bytes(&sender).unwrap();
    let signature = signer
        .sign_hash_sync(&transaction.signature_hash())
        .unwrap();
    transaction.into_signed(signature).into()
}

pub fn make_eip7702_tx(
    sender: FixedBytes<32>,
    max_fee_per_gas: u128,
    max_priority_fee_per_gas: u128,
    gas_limit: u64,
    nonce: u64,
    authorization_list: Vec<SignedAuthorization>,
    input_len: usize,
) -> TxEnvelope {
    make_eip7702_tx_with_value(
        sender,
        0,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        gas_limit,
        nonce,
        authorization_list,
        input_len,
    )
}

pub fn make_eip7702_tx_with_value(
    sender: FixedBytes<32>,
    value: u128,
    max_fee_per_gas: u128,
    max_priority_fee_per_gas: u128,
    gas_limit: u64,
    nonce: u64,
    authorization_list: Vec<SignedAuthorization>,
    input_len: usize,
) -> TxEnvelope {
    let transaction = TxEip7702 {
        chain_id: 1337,
        nonce,
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        to: Address::repeat_byte(0u8),
        value: U256::from(value),
        access_list: Default::default(),
        authorization_list,
        input: vec![0; input_len].into(),
    };

    let signer = PrivateKeySigner::from_bytes(&sender).unwrap();
    let signature = signer
        .sign_hash_sync(&transaction.signature_hash())
        .unwrap();
    transaction.into_signed(signature).into()
}

pub fn make_signed_authorization(
    authority: FixedBytes<32>,
    address: Address,
    nonce: u64,
) -> SignedAuthorization {
    let authorization = Authorization {
        chain_id: 1337,
        address,
        nonce,
    };

    sign_authorization(authority, authorization)
}

pub fn sign_authorization(
    authority: FixedBytes<32>,
    authorization: Authorization,
) -> SignedAuthorization {
    let signer = PrivateKeySigner::from_bytes(&authority).unwrap();
    let signature = signer
        .sign_hash_sync(&authorization.signature_hash())
        .unwrap();
    authorization.into_signed(signature)
}

pub fn recover_tx(tx: TxEnvelope) -> Recovered<TxEnvelope> {
    let signer = tx.recover_signer().unwrap();
    Recovered::new_unchecked(tx, signer)
}

pub fn make_receipt(logs_len: usize) -> ReceiptWithBloom {
    ReceiptWithBloom::new(
        Receipt::<Log> {
            logs: vec![Log {
                address: Default::default(),
                data: LogData::new(vec![], repeat_n(42, logs_len).collect::<Vec<u8>>().into())
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

fn compute_expected_txn_fees_and_nonce_usages(
    txs: &[Recovered<TxEnvelope>],
) -> (BTreeMap<Address, TxnFee>, NonceUsageMap) {
    let mut txn_fees: BTreeMap<_, TxnFee> = BTreeMap::new();

    for eth_txn in txs.iter() {
        txn_fees
            .entry(eth_txn.signer())
            .and_modify(|e| {
                e.max_gas_cost = e
                    .max_gas_cost
                    .saturating_add(compute_txn_max_gas_cost(eth_txn, BASE_FEE));
            })
            .or_insert(TxnFee {
                first_txn_value: eth_txn.value(),
                first_txn_gas: compute_txn_max_gas_cost(eth_txn, BASE_FEE),
                max_gas_cost: Balance::ZERO,
                max_txn_cost: compute_max_txn_cost(eth_txn),
                is_delegated: false,
            });
    }

    let nonce_usages = txs
        .iter()
        .flat_map(|t| {
            let mut pairs = vec![(t.signer(), NonceUsage::Known(t.nonce()))];

            if t.is_eip7702() {
                if let Some(auth_list) = t.authorization_list() {
                    for auth in auth_list {
                        let authority = auth.recover_authority().unwrap();

                        pairs.push((
                            authority,
                            NonceUsage::Possible(VecDeque::from_iter([auth.nonce()])),
                        ));

                        txn_fees
                            .entry(authority)
                            .and_modify(|e| {
                                e.is_delegated = true;
                            })
                            .or_insert(TxnFee {
                                first_txn_value: Balance::ZERO,
                                first_txn_gas: Balance::ZERO,
                                max_gas_cost: Balance::ZERO,
                                max_txn_cost: Balance::ZERO,

                                is_delegated: true,
                            });
                    }
                }
            }
            pairs
        })
        .rev()
        .fold(
            NonceUsageMap::default(),
            |mut map, (address, nonce_usage)| {
                match map.entry(address) {
                    std::collections::btree_map::Entry::Vacant(v) => {
                        v.insert(nonce_usage);
                    }
                    std::collections::btree_map::Entry::Occupied(mut o) => {
                        o.get_mut().merge_with_previous(&nonce_usage);
                    }
                }
                map
            },
        );

    (txn_fees, nonce_usages)
}

pub fn compute_expected_nonce_usages(txs: &[Recovered<TxEnvelope>]) -> NonceUsageMap {
    compute_expected_txn_fees_and_nonce_usages(txs).1
}

pub fn generate_consensus_test_block(
    round: Round,
    seq_num: SeqNum,
    base_fee: u64,
    chain_config: &MockChainConfig,
    txs: Vec<Recovered<TxEnvelope>>,
) -> ConsensusTestBlock<NopSignature, MockSignatures<NopSignature>> {
    let chain_params = chain_config.get_chain_revision(round).chain_params();

    let body = ConsensusBlockBody::new(ConsensusBlockBodyInner {
        execution_body: EthBlockBody {
            transactions: txs.iter().map(|tx| tx.tx().to_owned()).collect(),
            ommers: Vec::default(),
            withdrawals: Vec::default(),
        },
    });

    let keypair = NopKeyPair::from_bytes(rand::random::<[u8; 32]>().as_mut_slice()).unwrap();

    let signature = RoundSignature::new(round, &keypair);

    let header = ConsensusBlockHeader::new(
        NodeId::new(keypair.pubkey()),
        Epoch(1),
        round,
        Default::default(), // delayed_execution_results
        // execution_inputs
        ProposedEthHeader {
            ommers_hash: EMPTY_OMMER_ROOT_HASH.0,
            transactions_root: calculate_transaction_root(&txs).0,
            number: seq_num.0,
            gas_limit: chain_params.proposal_gas_limit,
            mix_hash: signature.get_hash().0,
            base_fee_per_gas: base_fee,
            withdrawals_root: EMPTY_WITHDRAWALS.0,
            ..Default::default()
        },
        body.get_id(),
        QuorumCertificate::genesis_qc(),
        seq_num,
        0,
        signature,
        monad_tfm::base_fee::MIN_BASE_FEE,
        monad_tfm::base_fee::GENESIS_BASE_FEE_TREND,
        monad_tfm::base_fee::GENESIS_BASE_FEE_MOMENT,
    );

    let (txn_fees, nonce_usages) = compute_expected_txn_fees_and_nonce_usages(&txs);

    ConsensusTestBlock {
        block: ConsensusFullBlock::new(header, body).expect("header doesn't match body"),
        validated_txns: txs,
        nonce_usages,
        txn_fees,
    }
}

pub fn generate_block_with_txs(
    round: Round,
    seq_num: SeqNum,
    base_fee: u64,
    chain_config: &MockChainConfig,
    txs: Vec<Recovered<TxEnvelope>>,
) -> EthValidatedBlock<NopSignature, MockSignatures<NopSignature>> {
    let test_block = generate_consensus_test_block(round, seq_num, base_fee, chain_config, txs);

    EthValidatedBlock {
        block: test_block.block,
        system_txns: Vec::new(),
        validated_txns: test_block.validated_txns,
        nonce_usages: test_block.nonce_usages,
        txn_fees: test_block.txn_fees,
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
