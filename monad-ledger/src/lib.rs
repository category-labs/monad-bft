use monad_consensus_types::{
    block::{Block as MonadBlock, FullBlock as MonadFullBlock},
    payload::{ExecutionArtifacts, FullTransactionList},
    signature_collection::SignatureCollection,
};
use monad_crypto::hasher::{Hasher, HasherType};
use monad_eth_types::EthFullTransactionList;
use reth_primitives::{BlockBody, Bloom, Bytes, Header, H256, U256};
use reth_rlp::Encodable;

pub fn encode_full_block<SCT: SignatureCollection>(full_block: MonadFullBlock<SCT>) -> Vec<u8> {
    let (monad_block, monad_full_txs) = full_block.split();

    let block_body = generate_block_body(monad_full_txs);

    let header = generate_header(monad_block, &block_body);

    let block = block_body.create_block(header);

    let mut buf = Vec::default();

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
        number: monad_block.payload.seq_num,
        // TODO
        gas_limit: u64::MAX,
        gas_used: gas_used.0,
        // TODO: Add to BFT proposal
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
