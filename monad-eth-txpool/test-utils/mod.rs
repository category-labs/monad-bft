use std::collections::BTreeMap;

use monad_consensus_types::{
    block::{Block, BlockKind},
    payload::{ExecutionProtocol, FullTransactionList, Payload, RandaoReveal, TransactionPayload},
    quorum_certificate::QuorumCertificate,
};
use monad_crypto::{certificate_signature::CertificateKeyPair, NopKeyPair, NopSignature};
use monad_eth_block_policy::{compute_txn_max_value, EthValidatedBlock};
use monad_eth_types::{EthAddress, EthFullTransactionList, EthSignedTransaction, EthTransaction};
use monad_testutil::signing::MockSignatures;
use monad_types::{Epoch, NodeId, Round, SeqNum};
use reth_primitives::U256;

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
