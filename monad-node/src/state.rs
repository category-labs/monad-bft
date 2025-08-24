// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use clap::{error::ErrorKind, FromArgMatches};
use monad_bls::BlsKeyPair;
use monad_chain_config::MonadChainConfig;
use monad_keystore::keystore::Keystore;
use monad_node_config::{ForkpointConfig, MonadNodeConfig};
use monad_secp::KeyPair;
use tracing::info;

use crate::{cli::Cli, error::NodeSetupError};

pub struct NodeState {
    pub node_config: MonadNodeConfig,
    pub node_config_path: PathBuf,
    pub forkpoint_config: ForkpointConfig,
    pub validators_path: PathBuf,
    pub chain_config: MonadChainConfig,

    pub secp256k1_identity: KeyPair,
    pub router_identity: KeyPair,
    pub bls12_381_identity: BlsKeyPair,

    pub forkpoint_path: PathBuf,
    pub wal_path: PathBuf,
    pub ledger_path: PathBuf,
    pub mempool_ipc_path: PathBuf,
    pub control_panel_ipc_path: PathBuf,
    pub statesync_ipc_path: PathBuf,
    pub statesync_sq_thread_cpu: Option<u32>,
    pub triedb_path: PathBuf,

    pub otel_endpoint_interval: Option<(String, Duration)>,
    pub pprof: String,
    pub manytrace_socket: Option<String>,
}

impl NodeState {
    pub fn setup(cmd: &mut clap::Command) -> Result<Self, NodeSetupError> {
        let Cli {
            bls_identity,
            secp_identity,
            node_config: node_config_path,
            forkpoint_config: forkpoint_config_path,
            validators_path,
            devnet_chain_config_override: maybe_devnet_chain_config_override_path,
            wal_path,
            ledger_path,
            mempool_ipc_path,
            triedb_path,
            control_panel_ipc_path,
            statesync_ipc_path,
            statesync_sq_thread_cpu,
            keystore_password,
            otel_endpoint,
            record_metrics_interval_seconds,
            pprof,
            manytrace_socket,
        } = Cli::from_arg_matches_mut(&mut cmd.get_matches_mut())?;

        let keystore_password = keystore_password.as_deref().unwrap_or("");

        let secp_key = load_secp256k1_keypair(&secp_identity, keystore_password)?;
        let secp_pubkey = secp_key.pubkey();
        info!(
            "Loaded secp256k1 key from {:?}, pubkey=0x{}",
            &secp_identity,
            hex::encode(secp_pubkey.bytes_compressed())
        );
        // FIXME this is somewhat jank.. is there a better way?
        let router_key = load_secp256k1_keypair(&secp_identity, keystore_password)?;
        info!(
            "Loaded router key from {:?}, pubkey=0x{}",
            &secp_identity,
            hex::encode(router_key.pubkey().bytes_compressed())
        );
        let bls_key = load_bls12_381_keypair(&bls_identity, keystore_password)?;
        info!(
            "Loaded bls12_381 key from {:?}, pubkey=0x{}",
            &bls_identity,
            hex::encode(bls_key.pubkey().compress())
        );

        let node_config: MonadNodeConfig =
            toml::from_str(&std::fs::read_to_string(&node_config_path)?)?;
        let forkpoint_config: ForkpointConfig =
            toml::from_str(&std::fs::read_to_string(&forkpoint_config_path)?)?;
        let devnet_chain_config_override =
            if let Some(devnet_override_path) = maybe_devnet_chain_config_override_path {
                Some(toml::from_str(&std::fs::read_to_string(
                    &devnet_override_path,
                )?)?)
            } else {
                None
            };
        let chain_config =
            MonadChainConfig::new(node_config.chain_id, devnet_chain_config_override)?;

        let wal_path = wal_path.with_file_name(format!(
            "{}_{}",
            wal_path
                .file_name()
                .expect("no wal file name")
                .to_owned()
                .into_string()
                .expect("invalid wal path"),
            std::time::UNIX_EPOCH
                .elapsed()
                .expect("time went backwards")
                .as_millis()
        ));

        let otel_endpoint_interval = match (otel_endpoint, record_metrics_interval_seconds) {
            (Some(otel_endpoint), Some(record_metrics_interval_seconds)) => Some((
                otel_endpoint,
                Duration::from_secs(record_metrics_interval_seconds),
            )),
            (None, None) => None,
            _ => panic!("cli accepted otel_endpoint without record_metrics_interval_seconds"),
        };

        Ok(Self {
            node_config,
            node_config_path,
            forkpoint_config,
            validators_path,
            chain_config,

            secp256k1_identity: secp_key,
            router_identity: router_key,
            bls12_381_identity: bls_key,

            forkpoint_path: forkpoint_config_path,
            wal_path,
            ledger_path,
            triedb_path,
            mempool_ipc_path,
            control_panel_ipc_path,
            statesync_ipc_path,
            statesync_sq_thread_cpu,

            otel_endpoint_interval,
            pprof,
            manytrace_socket,
        })
    }
}

fn load_secp256k1_keypair(path: &Path, keystore_password: &str) -> Result<KeyPair, NodeSetupError> {
    Keystore::load_secp_key(path, keystore_password).map_err(|_| NodeSetupError::Custom {
        kind: ErrorKind::ValueValidation,
        msg: "secp secret must be encoded in keystore json".to_owned(),
    })
}

fn load_bls12_381_keypair(
    path: &Path,
    keystore_password: &str,
) -> Result<BlsKeyPair, NodeSetupError> {
    Keystore::load_bls_key(path, keystore_password).map_err(|_| NodeSetupError::Custom {
        kind: ErrorKind::ValueValidation,
        msg: "bls secret secret must be encoded in keystore json".to_owned(),
    })
}
