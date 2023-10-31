use std::path::PathBuf;

use clap::error::ErrorKind;
use monad_consensus_types::{
    block::{Block, BlockType, FullBlock},
    ledger::LedgerCommitInfo,
    payload::{
        ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal, TransactionHashList,
    },
    quorum_certificate::{genesis_vote_info, QuorumCertificate},
    signature_collection::SignatureCollection,
    voting::{ValidatorMapping, VoteInfo},
};
use monad_crypto::{
    hasher::{Hasher, HasherType},
    secp256k1::{KeyPair, PubKey},
};
use monad_eth_types::{EthAddress, EMPTY_RLP_TX_LIST};
use monad_types::{NodeId, Round};

use crate::{error::NodeSetupError, SignatureCollectionType, TransactionValidatorType};

mod config;
use config::GenesisConfig;

use self::config::GenesisSignatureConfig;

pub struct GenesisState {
    pub genesis_block: FullBlock<SignatureCollectionType>,
    pub genesis_signatures: SignatureCollectionType,
    pub genesis_vote_info: VoteInfo,
}

impl GenesisState {
    pub fn setup(
        genesis_config_path: PathBuf,
        val_mapping: &ValidatorMapping<KeyPair>,
    ) -> Result<Self, NodeSetupError> {
        let config: GenesisConfig = toml::from_str(&std::fs::read_to_string(genesis_config_path)?)?;

        let genesis_block = build_genesis_block(config.author)?;

        let genesis_vote_info = genesis_vote_info(genesis_block.get_id());

        let genesis_signatures =
            build_genesis_signatures(&genesis_block, config.signatures, val_mapping)?;

        Ok(Self {
            genesis_block,
            genesis_signatures,
            genesis_vote_info,
        })
    }
}

fn build_genesis_block(
    author: PubKey,
) -> Result<FullBlock<SignatureCollectionType>, NodeSetupError> {
    // TODO: Deserialize transactions from GenesisConfig
    let (genesis_txs, genesis_full_txs) = (
        TransactionHashList(vec![EMPTY_RLP_TX_LIST]),
        FullTransactionList(vec![EMPTY_RLP_TX_LIST]),
    );

    let genesis_prime_qc = QuorumCertificate::genesis_prime_qc::<HasherType>();
    let genesis_execution_header = ExecutionArtifacts::zero();

    FullBlock::from_block(
        Block::new::<HasherType>(
            NodeId(author),
            Round(0),
            &Payload {
                txns: genesis_txs,
                header: genesis_execution_header,
                seq_num: 0,
                beneficiary: EthAddress::default(),
                randao_reveal: RandaoReveal::default(),
            },
            &genesis_prime_qc,
        ),
        genesis_full_txs,
        &TransactionValidatorType::default(),
    )
    .ok_or_else(|| NodeSetupError::Custom {
        kind: ErrorKind::ValueValidation,
        msg: "genesis_full_txs does not match genesis_txs".to_string(),
    })
}

fn build_genesis_signatures(
    genesis_block: &FullBlock<SignatureCollectionType>,
    signatures: Vec<GenesisSignatureConfig>,
    val_mapping: &ValidatorMapping<KeyPair>,
) -> Result<SignatureCollectionType, NodeSetupError> {
    let genesis_lci =
        LedgerCommitInfo::new::<HasherType>(None, &genesis_vote_info(genesis_block.get_id()));

    let msg = HasherType::hash_object(&genesis_lci);

    let sigs = signatures
        .into_iter()
        .map(|sig_config| (NodeId(sig_config.id), sig_config.signature))
        .collect();

    Ok(SignatureCollectionType::new(
        sigs,
        val_mapping,
        msg.as_ref(),
    )?)
}
