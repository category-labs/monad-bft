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
    env,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use agent::AgentBuilder;
use clap::{error::ErrorKind, FromArgMatches};
use monad_bls::BlsKeyPair;
use monad_chain_config::MonadChainConfig;
use monad_control_panel::TracingReload;
use monad_keystore::keystore::Keystore;
use monad_node_config::{ForkpointConfig, MonadNodeConfig, ValidatorsConfigType};
use monad_secp::KeyPair;
use monad_types::Round;
use reqwest::{blocking::Client, Url};
use tracing::{info, warn};
use tracing_manytrace::{ManytraceLayer, TracingExtension};
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer as FmtLayer},
    layer::SubscriberExt,
    Layer,
};

use crate::{cli::Cli, error::NodeSetupError};

const REMOTE_FORKPOINT_URL_ENV: &str = "REMOTE_FORKPOINT_URL";
const REMOTE_VALIDATORS_URL_ENV: &str = "REMOTE_VALIDATORS_URL";

pub struct NodeState {
    pub node_config: MonadNodeConfig,
    pub node_config_path: PathBuf,
    pub forkpoint_config: ForkpointConfig,
    pub validators_config: ValidatorsConfigType,
    pub chain_config: MonadChainConfig,

    pub secp256k1_identity: KeyPair,
    pub router_identity: KeyPair,
    pub bls12_381_identity: BlsKeyPair,

    pub forkpoint_path: PathBuf,
    pub validators_path: PathBuf,
    pub wal_path: PathBuf,
    pub ledger_path: PathBuf,
    pub mempool_ipc_path: PathBuf,
    pub control_panel_ipc_path: PathBuf,
    pub statesync_ipc_path: PathBuf,
    pub statesync_sq_thread_cpu: Option<u32>,
    pub triedb_path: PathBuf,

    pub otel_endpoint_interval: Option<(String, Duration)>,
    pub pprof: String,
    pub reload_handle: Box<dyn TracingReload>,
}

impl NodeState {
    pub fn setup(cmd: &mut clap::Command) -> Result<Self, NodeSetupError> {
        let Cli {
            bls_identity,
            secp_identity,
            node_config: node_config_path,
            forkpoint_config: forkpoint_config_path,
            validators_path: validators_config_path,
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

        let (reload_handle, _agent) = NodeState::setup_tracing(manytrace_socket)?;

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

        let (forkpoint_config, validators_config) =
            get_latest_configs(&forkpoint_config_path, &validators_config_path)?;

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
            validators_config,
            chain_config,

            secp256k1_identity: secp_key,
            router_identity: router_key,
            bls12_381_identity: bls_key,

            forkpoint_path: forkpoint_config_path,
            validators_path: validators_config_path,
            wal_path,
            ledger_path,
            triedb_path,
            mempool_ipc_path,
            control_panel_ipc_path,
            statesync_ipc_path,
            statesync_sq_thread_cpu,

            otel_endpoint_interval,
            pprof,
            reload_handle,
        })
    }

    fn setup_tracing(
        manytrace_socket: Option<String>,
    ) -> Result<(Box<dyn TracingReload>, Option<agent::Agent>), NodeSetupError> {
        if let Some(socket_path) = manytrace_socket {
            let extension = std::sync::Arc::new(TracingExtension::new());
            let agent = AgentBuilder::new(socket_path)
                .register_tracing(Box::new((*extension).clone()))
                .build()
                .map_err(|e| NodeSetupError::Custom {
                    kind: clap::error::ErrorKind::Io,
                    msg: format!("failed to build manytrace agent: {}", e),
                })?;
            let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(
                tracing_subscriber::EnvFilter::from_default_env(),
            );
            let subscriber = tracing_subscriber::Registry::default()
                .with(ManytraceLayer::new(extension))
                .with(
                    FmtLayer::default()
                        .json()
                        .with_span_events(FmtSpan::NONE)
                        .with_current_span(false)
                        .with_span_list(false)
                        .with_writer(std::io::stdout)
                        .with_ansi(false)
                        .with_filter(filter),
                );

            tracing::subscriber::set_global_default(subscriber)?;
            info!("manytrace tracing enabled");
            Ok((Box::new(reload_handle), Some(agent)))
        } else {
            let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(
                tracing_subscriber::EnvFilter::from_default_env(),
            );

            let subscriber = tracing_subscriber::Registry::default().with(filter).with(
                FmtLayer::default()
                    .json()
                    .with_span_events(FmtSpan::NONE)
                    .with_current_span(false)
                    .with_span_list(false)
                    .with_writer(std::io::stdout)
                    .with_ansi(false),
            );

            tracing::subscriber::set_global_default(subscriber)?;
            Ok((Box::new(reload_handle), None))
        }
    }
}

fn fetch_local_configs(
    forkpoint_config_path: &Path,
    validators_config_path: &Path,
) -> Result<(ForkpointConfig, ValidatorsConfigType), String> {
    let local_forkpoint_config: ForkpointConfig = toml::from_str(
        &std::fs::read_to_string(forkpoint_config_path)
            .map_err(|_| "failed to read local forkpoint.toml file".to_owned())?,
    )
    .map_err(|err| err.to_string())?;
    let local_validators_config = ValidatorsConfigType::read_from_path(validators_config_path)
        .map_err(|_| "failed to read local validators.toml file".to_owned())?;

    Ok((local_forkpoint_config, local_validators_config))
}

fn fetch_remote_configs() -> Result<(ForkpointConfig, ValidatorsConfigType), String> {
    let forkpoint_url_str = env::var(REMOTE_FORKPOINT_URL_ENV)
        .map_err(|_| format!("{REMOTE_FORKPOINT_URL_ENV} env variable unset"))?;
    let remote_forkpoint_url = Url::from_str(&forkpoint_url_str)
        .map_err(|err| format!("failed to parse remote forkpoint url: {err}"))?;

    let validators_url_str = env::var(REMOTE_VALIDATORS_URL_ENV)
        .map_err(|_| format!("{REMOTE_VALIDATORS_URL_ENV} env variable unset"))?;
    let remote_validators_url = Url::from_str(&validators_url_str)
        .map_err(|err| format!("failed to parse remote validators url: {err}"))?;

    let client = Client::new();

    let forkpoint_config_str = client
        .get(remote_forkpoint_url)
        .send()
        .and_then(|forkpoint_response| forkpoint_response.error_for_status())
        .and_then(|valid_forkpoint_response| valid_forkpoint_response.text())
        .map_err(|err| format!("error fetching remote forkpoint config: {err}"))?;
    let forkpoint_config = toml::from_str(&forkpoint_config_str)
        .map_err(|err| format!("failed to parse remote forkpoint config: {err}"))?;

    let validators_config_str = client
        .get(remote_validators_url)
        .send()
        .and_then(|validators_response| validators_response.error_for_status())
        .and_then(|valid_validators_response| valid_validators_response.text())
        .map_err(|err| format!("error fetching remote validators config: {err}"))?;
    let validators_config = ValidatorsConfigType::read_from_str(&validators_config_str)
        .map_err(|err| format!("failed to parse remote validators config: {err}"))?;

    Ok((forkpoint_config, validators_config))
}

fn get_latest_configs(
    forkpoint_config_path: &Path,
    validators_config_path: &Path,
) -> Result<(ForkpointConfig, ValidatorsConfigType), NodeSetupError> {
    let local_configs = fetch_local_configs(forkpoint_config_path, validators_config_path);
    let remote_configs = fetch_remote_configs();

    if local_configs.is_err() && remote_configs.is_err() {
        return Err(NodeSetupError::Custom {
            kind: ErrorKind::MissingRequiredArgument,
            msg: "failed to fetch local and remote configs".to_owned(),
        });
    }

    match local_configs {
        Ok((local_forkpoint_config, local_validators_config)) => {
            match remote_configs {
                Ok((remote_forkpoint_config, remote_validators_config)) => {
                    let local_forkpoint_round = local_forkpoint_config.high_certificate.round();
                    let remote_forkpoint_round = remote_forkpoint_config.high_certificate.round();

                    // if remote config is more recent, use that over local config
                    if remote_forkpoint_round > local_forkpoint_round {
                        info!(
                            ?remote_forkpoint_round,
                            ?local_forkpoint_round,
                            "remote forkpoint newer than local forkpoint, using remote configs"
                        );

                        return Ok((remote_forkpoint_config, remote_validators_config));
                    }

                    if remote_forkpoint_round < local_forkpoint_round - Round(200) {
                        // warn user if remote configs are stale
                        warn!(
                            ?remote_forkpoint_round,
                            ?local_forkpoint_round,
                            "remote forkpoint 200 rounds older than local forkpoint"
                        );
                    }
                }
                Err(fetch_err) => {
                    info!(
                        fetch_err,
                        "failed to fetch remote configs, using local forkpoint and validators config"
                    );
                }
            }

            Ok((local_forkpoint_config, local_validators_config))
        }
        Err(fetch_err) => {
            info!(
                fetch_err,
                "failed to fetch local configs, using remote forkpoint and validators config"
            );

            Ok(remote_configs.unwrap())
        }
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
