use std::{error::Error, fs::File, io::Read};

use alloy_consensus::Transaction;
use alloy_primitives::{hex, U256};
use monad_bls::{BlsSignature, BlsSignatureCollection};
use monad_chain_config::{revision::MonadChainRevision, MonadChainConfig, MONAD_TESTNET_CHAIN_ID};
use monad_consensus_types::{
    block::{ConsensusBlockHeader, ConsensusFullBlock, PassthruBlockPolicy},
    block_validator::BlockValidator,
    payload::ConsensusBlockBody,
};
use monad_eth_block_policy::{compute_txn_max_gas_cost, EthBlockPolicy};
use monad_eth_block_validator::EthBlockValidator;
use monad_eth_types::EthExecutionProtocol;
use monad_secp::{PubKey, SecpSignature};
use monad_state_backend::NopStateBackend;

fn read_block(
    block_id: &str,
) -> Result<
    ConsensusFullBlock<SecpSignature, BlsSignatureCollection<PubKey>, EthExecutionProtocol>,
    Box<dyn Error>,
> {
    println!("reading block_id: {block_id:?}");
    let mut file = File::open(block_id)?;
    let size = file.metadata()?.len();
    let mut buf = vec![0; size as usize];
    file.read_exact(&mut buf)?;

    let header: ConsensusBlockHeader<
        SecpSignature,
        BlsSignatureCollection<PubKey>,
        EthExecutionProtocol,
    > = alloy_rlp::decode_exact(&buf).map_err(|err| {
        std::io::Error::other(format!(
            "failed to rlp decode ledger proposed_head bft header, err={:?}",
            err
        ))
    })?;

    let payload_id = hex::encode(header.block_body_id.0 .0);
    println!("reading payload_id: {payload_id:?}");

    let mut file = File::open(payload_id)?;
    let size = file.metadata()?.len();
    let mut buf = vec![0; size as usize];
    file.read_exact(&mut buf)?;

    let body: ConsensusBlockBody<EthExecutionProtocol> = alloy_rlp::decode_exact(&buf).unwrap();

    Ok(ConsensusFullBlock::new(header, body).unwrap())
}

// scp bue-004:~/monad-bft/ledger/headers/ab72b3e099da793e805f71ee495450fa6f02815df6e813a59bdb08829ee9bbe9 .
// scp bue-004:~/monad-bft/ledger/bodies/616d8d87c5c977d07bf84cb9a58e8adc9a2c0d47940a67317465ff9ae615e5ed .

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::try_init().unwrap();
    let mut block_id =
        "cb7ac9eda8c7566836f6335a9e2b06a47f0b6cd982e452b441990305e6bae863".to_owned();
    let mut blocks = Vec::new();
    while blocks.len() < 7 {
        let block = read_block(&block_id).unwrap();
        block_id = hex::encode(block.get_parent_id().0 .0);
        blocks.push(block);
    }
    blocks.reverse();

    let mut total = U256::ZERO;
    for block in &blocks {
        println!("scanning block {}...", block.get_seq_num().0);
        for (idx, tx) in block.body().execution_body.transactions.iter().enumerate() {
            if tx.recover_signer().unwrap()
                == alloy_primitives::address!("0xbf8619905d0667766bec7dbae2f20f757cb2b496")
            {
                println!("{} idx: {:?}, tx: {:?}", block.get_seq_num().0, idx, tx);
                total += compute_txn_max_gas_cost(tx, block.get_base_fee().unwrap());
            }
        }
    }
    println!("total max gas cost: {}", total);
    // println!(
    //     "354={:?}, signer={:?}",
    //     body.execution_body.transactions[354],
    //     body.execution_body.transactions[354].recover_signer()
    // );

    // let block = ConsensusFullBlock::new(header, body).unwrap();

    // let validator: EthBlockValidator<SecpSignature, BlsSignatureCollection<PubKey>> =
    //     EthBlockValidator::default();

    // let chain_config = MonadChainConfig::new(MONAD_TESTNET_CHAIN_ID, None).unwrap();
    // BlockValidator::<
    //     SecpSignature,
    //     BlsSignatureCollection<PubKey>,
    //     EthExecutionProtocol,
    //     EthBlockPolicy<
    //         SecpSignature,
    //         BlsSignatureCollection<PubKey>,
    //         MonadChainConfig,
    //         MonadChainRevision,
    //     >,
    //     NopStateBackend,
    //     MonadChainConfig,
    //     MonadChainRevision,
    // >::validate(&validator, header, body, None, &chain_config)
    // .unwrap();

    Ok(())
}
