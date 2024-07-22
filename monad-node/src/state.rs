use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use clap::{error::ErrorKind, FromArgMatches};
use log::info;
use monad_bls::BlsKeyPair;
use monad_keystore::keystore::Keystore;
use monad_secp::KeyPair;

use crate::{
    cli::Cli,
    config::{ForkpointConfig, NodeConfig},
    error::NodeSetupError,
    mode::RunModeCommand,
};

pub struct NodeState {
    pub node_config: NodeConfig,
    pub forkpoint_config: ForkpointConfig,

    pub secp256k1_identity: KeyPair,
    pub gossip_identity: KeyPair,
    pub bls12_381_identity: BlsKeyPair,

    pub wal_path: PathBuf,
    pub execution_ledger_path: PathBuf,
    pub mempool_ipc_path: PathBuf,
    pub control_panel_ipc_path: PathBuf,
    pub blockdb_path: PathBuf,
    pub triedb_path: PathBuf,
    pub otel_endpoint: Option<String>,
    pub record_metrics_interval: Option<Duration>,
    pub node_name: String,
    pub network_name: String,

    pub run_mode: RunModeCommand,
}

impl NodeState {
    pub fn setup(cmd: &mut clap::Command) -> Result<Self, NodeSetupError> {
        let cli = Cli::from_arg_matches_mut(&mut cmd.get_matches_mut())?;

        let keystore_password = cli.keystore_password.as_deref().unwrap_or("");

        let secp_key = load_secp256k1_keypair(&cli.secp_identity, keystore_password)?;
        let secp_pubkey = secp_key.pubkey();
        info!(
            "Loaded secp256k1 key from {:?}, pubkey=0x{}",
            &cli.secp_identity,
            hex::encode(secp_pubkey.bytes_compressed())
        );
        // FIXME this is somewhat jank.. is there a better way?
        let gossip_key = load_secp256k1_keypair(&cli.secp_identity, keystore_password)?;
        info!(
            "Loaded gossip key from {:?}, pubkey=0x{}",
            &cli.secp_identity,
            hex::encode(gossip_key.pubkey().bytes_compressed())
        );
        let bls_key = load_bls12_381_keypair(&cli.bls_identity, keystore_password)?;
        info!(
            "Loaded bls12_381 key from {:?}, pubkey=0x{}",
            &cli.bls_identity,
            hex::encode(bls_key.pubkey().compress())
        );

        let node_config: NodeConfig = toml::from_str(&std::fs::read_to_string(cli.node_config)?)?;
        let node_name = node_config
            .name
            .clone()
            .unwrap_or(format!("monad-node-{:?}", secp_pubkey));
        let network_name = node_config
            .network_name
            .clone()
            .unwrap_or("monad-coordinator".to_owned());
        let forkpoint_config: ForkpointConfig =
            toml::from_str(&std::fs::read_to_string(cli.forkpoint_config)?)?;

        let run_mode = cli.run_mode.unwrap_or_default();

        Ok(Self {
            node_config,
            forkpoint_config,

            secp256k1_identity: secp_key,
            gossip_identity: gossip_key,
            bls12_381_identity: bls_key,

            wal_path: cli.wal_path,
            execution_ledger_path: cli.execution_ledger_path,
            blockdb_path: cli.blockdb_path,
            triedb_path: cli.triedb_path,
            mempool_ipc_path: cli.mempool_ipc_path,
            control_panel_ipc_path: cli.control_panel_ipc_path,
            otel_endpoint: cli.otel_endpoint,
            record_metrics_interval: cli
                .record_metrics_interval_seconds
                .and_then(|s| Some(Duration::from_secs(s))),
            node_name,
            network_name,

            run_mode,
        })
    }
}

fn load_secp256k1_keypair(path: &Path, keystore_password: &str) -> Result<KeyPair, NodeSetupError> {
    let result = Keystore::load_key(path, keystore_password);
    if result.is_ok() {
        let mut secret = result.unwrap();
        if secret.len() == 32 {
            return Ok(KeyPair::from_bytes(&mut secret)?);
        }
    }
    Err(NodeSetupError::Custom {
        kind: ErrorKind::ValueValidation,
        msg: "secp secret must be encoded in keystore json".to_owned(),
    })
}

fn load_bls12_381_keypair(
    path: &Path,
    keystore_password: &str,
) -> Result<BlsKeyPair, NodeSetupError> {
    let result = Keystore::load_key(path, keystore_password);
    if result.is_ok() {
        let mut secret = result.unwrap();
        if secret.len() == 32 {
            return Ok(BlsKeyPair::from_bytes(&mut secret)?);
        }
    }
    Err(NodeSetupError::Custom {
        kind: ErrorKind::ValueValidation,
        msg: "bls secret secret must be encoded in keystore json".to_owned(),
    })
}
