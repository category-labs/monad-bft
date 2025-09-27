use std::{error::Error, fs::File, io::Read};

use alloy_primitives::hex;
use monad_bls::{BlsSignature, BlsSignatureCollection};
use monad_chain_config::{revision::MonadChainRevision, MonadChainConfig, MONAD_TESTNET_CHAIN_ID};
use monad_consensus_types::{
    block::{ConsensusBlockHeader, ConsensusFullBlock, PassthruBlockPolicy},
    block_validator::BlockValidator,
    payload::ConsensusBlockBody,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_block_validator::EthBlockValidator;
use monad_eth_types::EthExecutionProtocol;
use monad_secp::{PubKey, SecpSignature};
use monad_state_backend::NopStateBackend;

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::try_init().unwrap();
    let block_id = "6de995280bb6c0b3c20701d3853d605825d1f2218767b5e39856e74e63fd5776";

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
    println!("payload_id: {:?}", payload_id);

    let mut file = File::open(payload_id)?;
    let size = file.metadata()?.len();
    let mut buf = vec![0; size as usize];
    file.read_exact(&mut buf)?;

    let body: ConsensusBlockBody<EthExecutionProtocol> = alloy_rlp::decode_exact(&buf).unwrap();

    // let block = ConsensusFullBlock::new(header, body).unwrap();

    let validator: EthBlockValidator<SecpSignature, BlsSignatureCollection<PubKey>> =
        EthBlockValidator::default();

    let chain_config = MonadChainConfig::new(MONAD_TESTNET_CHAIN_ID, None).unwrap();
    BlockValidator::<
        SecpSignature,
        BlsSignatureCollection<PubKey>,
        EthExecutionProtocol,
        EthBlockPolicy<
            SecpSignature,
            BlsSignatureCollection<PubKey>,
            MonadChainConfig,
            MonadChainRevision,
        >,
        NopStateBackend,
        MonadChainConfig,
        MonadChainRevision,
    >::validate(&validator, header, body, None, &chain_config)
    .unwrap();

    Ok(())
}
