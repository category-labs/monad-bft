use alloy_rlp::Decodable;
use monad_consensus_types::{
    block::Block,
    payload::{ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal},
    quorum_certificate::QuorumCertificate,
};
use monad_crypto::{certificate_signature::CertificateKeyPair, NopKeyPair, NopSignature};
use monad_eth_tx::{EthFullTransactionList, EthSignedTransaction, EthTransaction};
use monad_eth_types::EthAddress;
use monad_multi_sig::MultiSig;
use monad_types::{Epoch, NodeId, Round, SeqNum};

use crate::EthValidatedBlock;

pub fn generate_random_block_with_txns(
    eth_txn_list: Vec<EthSignedTransaction>,
) -> EthValidatedBlock<MultiSig<NopSignature>> {
    let eth_full_tx_list = EthFullTransactionList(
        eth_txn_list
            .into_iter()
            .map(|signed_txn| {
                let sender_address = signed_txn.recover_signer().unwrap();
                EthTransaction::from_signed_transaction(signed_txn, sender_address)
            })
            .collect(),
    );
    let full_txn_list = FullTransactionList::new(eth_full_tx_list.rlp_encode());
    let keypair = NopKeyPair::from_bytes(rand::random::<[u8; 32]>().as_mut_slice()).unwrap();

    let block = Block::new(
        NodeId::new(keypair.pubkey()),
        Epoch(1),
        Round(1),
        &Payload {
            txns: full_txn_list,
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(1),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::new::<NopSignature>(Round(1), &keypair),
        },
        &QuorumCertificate::genesis_qc(),
    );
    let validated_txns =
        Vec::<EthSignedTransaction>::decode(&mut block.payload.txns.bytes().as_ref()).unwrap();
    let nonces = validated_txns
        .iter()
        .map(|t| (EthAddress(t.recover_signer().unwrap()), t.nonce()))
        .collect();

    EthValidatedBlock {
        block,
        validated_txns,
        nonces,
    }
}
