use monad_consensus_types::{
    block::Block,
    payload::{ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal},
    quorum_certificate::QuorumCertificate,
};
use monad_crypto::{certificate_signature::CertificateKeyPair, NopKeyPair, NopSignature};
use monad_eth_block_policy::EthValidatedBlock;
use monad_eth_tx::{EthFullTransactionList, EthSignedTransaction, EthTransaction};
use monad_eth_types::EthAddress;
use monad_multi_sig::MultiSig;
use monad_types::{Epoch, NodeId, Round, SeqNum};
use reth_primitives::{
    revm_primitives::FixedBytes, sign_message, Address, Transaction, TransactionKind, TxLegacy,
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

pub fn generate_random_block_with_txns(
    eth_txn_list: Vec<EthSignedTransaction>,
) -> EthValidatedBlock<MultiSig<NopSignature>> {
    let eth_full_tx_list = EthFullTransactionList(
        eth_txn_list
            .clone()
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
        0,
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
    let validated_txns: Vec<_> = eth_txn_list
        .into_iter()
        .map(|eth_txn| eth_txn.into_ecrecovered().unwrap())
        .collect();
    let nonces = validated_txns
        .iter()
        .map(|t| (EthAddress(t.signer()), t.nonce()))
        .collect();

    EthValidatedBlock {
        block,
        validated_txns,
        nonces,
    }
}
