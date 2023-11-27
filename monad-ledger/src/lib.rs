use monad_consensus_types::{
    block::{Block as MonadBlock, FullBlock as MonadFullBlock},
    payload::{ExecutionArtifacts, FullTransactionList},
    signature_collection::SignatureCollection,
};
use monad_crypto::hasher::{Hasher, HasherType};
use monad_eth_types::EthFullTransactionList;
use reth_primitives::{keccak256, BlockBody, Bloom, Bytes, Header, H256, U256};
use reth_rlp::Encodable;

pub fn encode_full_block<SCT: SignatureCollection>(full_block: MonadFullBlock<SCT>) -> Vec<u8> {
    let (monad_block, monad_full_txs) = full_block.split();

    let block_body = generate_block_body(monad_full_txs);

    let header = generate_header(monad_block, &block_body);

    let mut header_bytes = Vec::default();
    header.encode(&mut header_bytes);

    let header_hash = keccak256(header_bytes);

    let block = block_body.create_block(header);

    let mut buf = Vec::from(header_hash.0);

    block.encode(&mut buf);

    buf
}

fn generate_block_body(monad_full_txs: FullTransactionList) -> BlockBody {
    let transactions = EthFullTransactionList::rlp_decode(monad_full_txs.as_bytes().to_vec())
        .unwrap()
        .0
        .into_iter()
        .map(|tx| tx.into_signed())
        .collect();

    BlockBody {
        transactions,
        ommers: Vec::default(),
        withdrawals: None,
    }
}

fn generate_header<SCT>(monad_block: MonadBlock<SCT>, block_body: &BlockBody) -> Header {
    let ExecutionArtifacts {
        parent_hash,
        state_root,
        transactions_root,
        receipts_root,
        logs_bloom,
        gas_used,
    } = monad_block.payload.header;

    let mut randao_reveal_hasher = HasherType::new();

    randao_reveal_hasher.update(monad_block.payload.randao_reveal.0);

    Header {
        parent_hash: H256(parent_hash.0),
        ommers_hash: block_body.calculate_ommers_root(),
        beneficiary: monad_block.payload.beneficiary.0,
        state_root: H256(state_root.0),
        transactions_root: H256(transactions_root.0),
        receipts_root: H256(receipts_root.0),
        withdrawals_root: block_body.calculate_withdrawals_root(),
        logs_bloom: Bloom(logs_bloom.0),
        difficulty: U256::ZERO,
        number: monad_block.payload.seq_num.0,
        // TODO-1
        gas_limit: u64::MAX,
        gas_used: gas_used.0,
        // TODO-1: Add to BFT proposal
        timestamp: 0,
        mix_hash: randao_reveal_hasher.hash().0.into(),
        nonce: 0,
        base_fee_per_gas: None,
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: None,
        extra_data: Bytes::default(),
    }
}

#[cfg(test)]
mod test {
    use monad_consensus_types::{
        block::{Block, FullBlock as MonadFullBlock},
        ledger::LedgerCommitInfo,
        multi_sig::MultiSig,
        payload::{
            Bloom, ExecutionArtifacts, FullTransactionList, Gas, Payload, RandaoReveal,
            TransactionHashList,
        },
        quorum_certificate::{QcInfo, QuorumCertificate},
        transaction_validator::MockValidator,
        voting::VoteInfo,
    };
    use monad_crypto::{
        hasher::{Hash, HasherType},
        secp256k1::KeyPair,
        NopSignature,
    };
    use monad_eth_types::{EthAddress, EMPTY_RLP_TX_LIST};
    use monad_types::{BlockId, NodeId, Round, SeqNum};

    use crate::encode_full_block;

    #[test]
    fn encode_full_block_header_hash() {
        let pubkey = KeyPair::from_bytes(&mut [127; 32]).unwrap().pubkey();

        let full_block = MonadFullBlock::<MultiSig<NopSignature>>::from_block(
            Block::new::<HasherType>(
                NodeId(pubkey),
                Round(0),
                &Payload {
                    txns: TransactionHashList::new(vec![EMPTY_RLP_TX_LIST]),
                    header: ExecutionArtifacts {
                        parent_hash: Hash::default(),
                        state_root: Hash::default(),
                        transactions_root: Hash::default(),
                        receipts_root: Hash::default(),
                        logs_bloom: Bloom::zero(),
                        gas_used: Gas::default(),
                    },
                    seq_num: SeqNum(0),
                    beneficiary: EthAddress::default(),
                    randao_reveal: RandaoReveal::default(),
                },
                &QuorumCertificate::new::<HasherType>(
                    QcInfo {
                        vote: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit: LedgerCommitInfo {
                            commit_state_hash: None,
                            vote_info_hash: Hash::default(),
                        },
                    },
                    MultiSig::default(),
                ),
            ),
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            &MockValidator::default(),
        )
        .unwrap();

        let bytes = encode_full_block(full_block);

        // Check that encode_full_block starts with keccak header hash
        assert!(bytes.starts_with(&[
            212, 77, 82, 205, 229, 94, 135, 122, 87, 21, 202, 7, 45, 115, 186, 120, 39, 59, 86,
            171, 170, 230, 157, 75, 208, 15, 84, 70, 30, 39, 187, 94,
        ]));
    }
}
