use std::path::PathBuf;

use clap::{CommandFactory, FromArgMatches, Parser};
use monad_block_persist::{BlockPersist, FileBlockPersist};
use monad_consensus_types::{
    block::ConsensusFullBlock, checkpoint::Checkpoint, tip::ConsensusTip, RoundCertificate,
};
use monad_node_config::{ExecutionProtocolType, SignatureCollectionType, SignatureType};
use monad_types::{BlockId, Hash, SeqNum};

#[derive(Debug, Parser)]
#[command(name = "forkpoint-gen", about, long_about = None)]
pub struct Cli {
    /// Set the path where consensus blocks will be read
    #[arg(long)]
    pub ledger_path: PathBuf,

    #[arg(long)]
    pub forkpoint_high_qc_block_id: String,

    #[arg(long)]
    pub forkpoint_root_block_id: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Cli::command();

    let Cli {
        ledger_path,
        forkpoint_high_qc_block_id,
        forkpoint_root_block_id,
    } = Cli::from_arg_matches_mut(&mut cmd.get_matches_mut())?;

    let block_persist: FileBlockPersist<
        SignatureType,
        SignatureCollectionType,
        ExecutionProtocolType,
    > = FileBlockPersist::new(ledger_path);

    let tip_block_id = {
        let block_id = forkpoint_high_qc_block_id
            .strip_prefix("0x")
            .expect("missing 0x prefix");
        let block_id = hex::decode(block_id).expect("invalid hex");
        BlockId(Hash(block_id.try_into().expect("block_id not 32 bytes")))
    };
    let root_block_id = {
        let block_id = forkpoint_root_block_id
            .strip_prefix("0x")
            .expect("missing 0x prefix");
        let block_id = hex::decode(block_id).expect("invalid hex");
        BlockId(Hash(block_id.try_into().expect("block_id not 32 bytes")))
    };

    let tip =
        read_full_block(&block_persist, &tip_block_id).expect("failed to read tip from ledger");
    println!(
        "tip={:?}, seq_num={:?}, timestamp={:?}",
        tip.get_id(),
        tip.header().seq_num,
        tip.header().timestamp_ns
    );
    let mut current_block = tip.clone();
    let mut found_root = false;
    while tip.get_seq_num() - current_block.get_seq_num() < SeqNum(20) {
        current_block = read_full_block(&block_persist, &current_block.get_parent_id())
            .unwrap_or_else(|| panic!("failed to read block: {:?}", current_block.get_parent_id()));
        let current_block_id = current_block.get_id();
        if current_block_id == root_block_id {
            found_root = true;
            println!(
                "root={:?}, seq_num={:?}",
                current_block_id,
                current_block.get_seq_num()
            );
        }
        println!(
            "block_id={:?}, seq_num={:?}",
            current_block_id,
            current_block.get_seq_num()
        );
    }
    assert!(found_root);
    println!("success, found root\n");

    let checkpoint: Checkpoint<SignatureType, SignatureCollectionType, ExecutionProtocolType> =
        Checkpoint {
            root: root_block_id,
            high_certificate: RoundCertificate::Qc(tip.get_qc().clone()),
            maybe_high_tip: Some(ConsensusTip::new(
                &monad_secp::KeyPair::from_bytes(&mut [5_u8; 32]).expect("invalid keypair"),
                tip.header().clone(),
                None,
            )),
            validator_sets: Vec::new(),
        };

    println!("copy out root, high_certificate, maybe_high_tip:");
    println!("{}", toml::to_string(&checkpoint).unwrap());

    Ok(())
}

fn read_full_block(
    block_persist: &FileBlockPersist<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
    block_id: &BlockId,
) -> Option<ConsensusFullBlock<SignatureType, SignatureCollectionType, ExecutionProtocolType>> {
    let header = block_persist.read_bft_header(block_id).ok()?;
    let body = block_persist.read_bft_body(&header.block_body_id).ok()?;
    ConsensusFullBlock::new(header, body).ok()
}
