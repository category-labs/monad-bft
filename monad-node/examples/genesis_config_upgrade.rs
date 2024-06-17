use std::{fs, path::PathBuf};

use clap::{CommandFactory, FromArgMatches, Parser};
use monad_consensus_types::validator_data::{ValidatorData, ValidatorSetData};
use monad_node::config::{ForkpointConfig, GenesisConfig, SignatureCollectionType};
use monad_state::Forkpoint;
use monad_types::NodeId;

#[derive(Debug, Parser)]
#[command(name="genesis-upgrade-tool", about, long_about = None)]
struct Args {
    // Path to old genesis config file
    #[arg(long)]
    pub genesis_path: PathBuf,

    // Path to new forkpoint config file
    #[arg(long)]
    pub forkpoint_path: PathBuf,
}

fn main() {
    let mut args = Args::command();
    let args = Args::from_arg_matches_mut(&mut args.get_matches_mut()).unwrap();

    let genesis_config: GenesisConfig =
        toml::from_str(&std::fs::read_to_string(args.genesis_path).unwrap()).unwrap();

    let validator_set_data: ValidatorSetData<SignatureCollectionType> = ValidatorSetData(
        genesis_config
            .validators
            .into_iter()
            .map(|peer| ValidatorData {
                node_id: NodeId::new(peer.secp256k1_pubkey),
                stake: peer.stake,
                cert_pubkey: peer.bls12_381_pubkey,
            })
            .collect(),
    );

    for v in validator_set_data.0.iter() {
        println!(
            "pubkey bytes {}",
            hex::encode(v.node_id.pubkey().bytes_compressed())
        );
    }
    let forkpoint = ForkpointConfig {
        forkpoint: Forkpoint::genesis(validator_set_data),
    };

    let forkpoint_str = toml::to_string(&forkpoint).unwrap();
    fs::write(args.forkpoint_path, forkpoint_str).unwrap();
}
