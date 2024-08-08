use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use clap::{error::ErrorKind, FromArgMatches};
use monad_bls::BlsKeyPair;
use monad_keystore::keystore::Keystore;
use monad_secp::KeyPair;
use tracing::info;

use crate::{
    cli::Cli,
    config::{ForkpointConfig, NodeConfig},
    error::NodeSetupError,
};

pub struct NodeState {
    pub node_config: NodeConfig,
    pub forkpoint_config: ForkpointConfig,

    pub secp256k1_identity: KeyPair,
    pub router_identity: KeyPair,
    pub bls12_381_identity: BlsKeyPair,

    pub forkpoint_path: PathBuf,
    pub genesis_path: PathBuf,
    pub wal_path: PathBuf,
    pub execution_ledger_path: PathBuf,
    pub mempool_ipc_path: PathBuf,
    pub control_panel_ipc_path: PathBuf,
    pub statesync_ipc_path: PathBuf,
    pub blockdb_path: PathBuf,
    pub triedb_path: PathBuf,
    pub otel_endpoint: Option<String>,
    pub record_metrics_interval: Option<Duration>,
    pub node_name: String,
    pub network_name: String,
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
        let router_key = load_secp256k1_keypair(&cli.secp_identity, keystore_password)?;
        info!(
            "Loaded router key from {:?}, pubkey=0x{}",
            &cli.secp_identity,
            hex::encode(router_key.pubkey().bytes_compressed())
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
            toml::from_str(&std::fs::read_to_string(&cli.forkpoint_config)?)?;

        let wal_path = cli.wal_path.with_file_name(format!(
            "{}_{}_{}_{}",
            cli.wal_path
                .file_name()
                .expect("no wal file name")
                .to_owned()
                .into_string()
                .expect("invalid wal path"),
            forkpoint_config.root.round.0,
            forkpoint_config.root.block_id.0,
            std::time::UNIX_EPOCH
                .elapsed()
                .expect("time went backwards")
                .as_millis()
        ));

        Ok(Self {
            node_config,
            forkpoint_config,

            secp256k1_identity: secp_key,
            router_identity: router_key,
            bls12_381_identity: bls_key,

            forkpoint_path: cli.forkpoint_config,
            genesis_path: cli.genesis_path,
            wal_path,
            execution_ledger_path: cli.execution_ledger_path,
            blockdb_path: cli.blockdb_path,
            triedb_path: cli.triedb_path,
            mempool_ipc_path: cli.mempool_ipc_path,
            control_panel_ipc_path: cli.control_panel_ipc_path,
            statesync_ipc_path: cli.statesync_ipc_path,
            otel_endpoint: cli.otel_endpoint,
            record_metrics_interval: cli
                .record_metrics_interval_seconds
                .and_then(|s| Some(Duration::from_secs(s))),
            node_name,
            network_name,
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
