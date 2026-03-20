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
    ffi::OsStr,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use agent::AgentBuilder;
use clap::{FromArgMatches, error::ErrorKind};
use monad_bls::BlsKeyPair;
use monad_chain_config::MonadChainConfig;
use monad_consensus_types::validator_data::ValidatorsConfigFile;
use monad_control_panel::TracingReload;
use monad_keystore::keystore::Keystore;
use monad_node_config::{ForkpointConfig, MonadNodeConfig, ValidatorsConfigType};
use monad_secp::KeyPair;
use monad_types::Round;
use reqwest::{Url, blocking::Client};
use tracing::{info, warn};
use tracing_manytrace::{ManytraceLayer, TracingExtension};
use tracing_subscriber::{
    Layer,
    fmt::{Layer as FmtLayer, format::FmtSpan},
    layer::SubscriberExt,
};

use crate::{cli::Cli, error::NodeSetupError};

const REMOTE_FORKPOINT_URL_ENV: &str = "REMOTE_FORKPOINT_URL";
const REMOTE_VALIDATORS_URL_ENV: &str = "REMOTE_VALIDATORS_URL";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PprofConfig {
    pub addr: String,
    pub enable_metrics: bool,
    pub enable_profiling: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OtelConfig {
    pub endpoint: String,
    pub export_interval: Duration,
}

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
    pub persisted_peers_path: PathBuf,

    pub otel: Option<OtelConfig>,
    pub pprof: Option<PprofConfig>,
    pub reload_handle: Box<dyn TracingReload>,
    // should be kept as long as node is alive, tracing listener is stopped when handle is dropped
    #[allow(unused)]
    manytrace_agent: Option<agent::Agent>,
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
            otel_endpoint,
            record_metrics_interval_seconds,
            keystore_password,
            pprof,
            pprof_enable_profiling,
            pprof_disable_metrics,
            manytrace_socket,
            persisted_peers_path,
        } = Cli::from_arg_matches_mut(&mut cmd.get_matches_mut())?;

        let otel = parse_otel_config(otel_endpoint, record_metrics_interval_seconds)?;
        let pprof = parse_pprof_config(pprof, pprof_disable_metrics, pprof_enable_profiling)?;

        let (reload_handle, agent) = NodeState::setup_tracing(manytrace_socket)?;

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

        if !matches!(
            forkpoint_config_path.extension().and_then(OsStr::to_str),
            Some("toml" | "rlp")
        ) {
            return Err(NodeSetupError::Custom {
                kind: ErrorKind::InvalidValue,
                msg: "forkpoint must have .toml or .rlp extension".to_owned(),
            });
        }

        let (forkpoint_config, validators_config) = get_latest_configs(
            &forkpoint_config_path,
            &validators_config_path,
            Round(node_config.statesync_threshold as u64),
        )?;

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

            otel,
            pprof,
            reload_handle,
            manytrace_agent: agent,
            persisted_peers_path,
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

fn parse_otel_config(
    endpoint: Option<String>,
    export_interval_seconds: Option<u64>,
) -> Result<Option<OtelConfig>, NodeSetupError> {
    let Some(endpoint) = endpoint else {
        if export_interval_seconds.is_some() {
            return Err(NodeSetupError::Custom {
                kind: ErrorKind::MissingRequiredArgument,
                msg: "--record-metrics-interval-seconds requires --otel-endpoint".to_owned(),
            });
        }
        return Ok(None);
    };

    let export_interval_seconds = export_interval_seconds.unwrap_or(1);
    if export_interval_seconds == 0 {
        return Err(NodeSetupError::Custom {
            kind: ErrorKind::InvalidValue,
            msg: "--record-metrics-interval-seconds must be greater than zero".to_owned(),
        });
    }

    Ok(Some(OtelConfig {
        endpoint,
        export_interval: Duration::from_secs(export_interval_seconds),
    }))
}

fn parse_pprof_config(
    addr: String,
    disable_metrics: bool,
    enable_profiling: bool,
) -> Result<Option<PprofConfig>, NodeSetupError> {
    if addr.is_empty() {
        if disable_metrics || enable_profiling {
            return Err(NodeSetupError::Custom {
                kind: ErrorKind::MissingRequiredArgument,
                msg: "--pprof-disable-metrics and --pprof-enable-profiling require --pprof"
                    .to_owned(),
            });
        }
        return Ok(None);
    }

    let enable_metrics = !disable_metrics;

    if !enable_metrics && !enable_profiling {
        return Err(NodeSetupError::Custom {
            kind: ErrorKind::InvalidValue,
            msg: "--pprof cannot disable both metrics and profiling at the same time".to_owned(),
        });
    }

    Ok(Some(PprofConfig {
        addr,
        enable_metrics,
        enable_profiling,
    }))
}

fn fetch_local_configs(
    forkpoint_config_path: &Path,
    validators_config_path: &Path,
) -> Result<(ForkpointConfig, ValidatorsConfigType), Box<dyn std::error::Error>> {
    let local_forkpoint_bytes = std::fs::read(forkpoint_config_path)
        .map_err(|err| format!("failed to read local forkpoint, err={}", err))?;
    let local_forkpoint_config = ForkpointConfig::try_parse_bytes(
        &forkpoint_config_path.to_string_lossy(),
        &local_forkpoint_bytes,
    )
    .map_err(|err| format!("failed to parse local forkpoint bytes (hint: if it's toml, file extension must be .toml) err={:?}", err))?;
    let local_validators_config = ValidatorsConfigType::read_from_path(validators_config_path)
        .map_err(|_| "failed to read local validators.toml file".to_owned())?;

    Ok((local_forkpoint_config, local_validators_config))
}

#[cfg(test)]
mod tests {
    use clap::error::ErrorKind;

    use super::{OtelConfig, PprofConfig, parse_otel_config, parse_pprof_config};

    #[test]
    fn rejects_interval_without_otel_endpoint() {
        let error = parse_otel_config(None, Some(1)).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn defaults_otel_interval_to_one_second() {
        let config = parse_otel_config(Some("http://127.0.0.1:4317".to_owned()), None)
            .expect("otel config")
            .expect("otel should be enabled");

        assert_eq!(
            config,
            OtelConfig {
                endpoint: "http://127.0.0.1:4317".to_owned(),
                export_interval: std::time::Duration::from_secs(1),
            }
        );
    }

    #[test]
    fn rejects_zero_otel_interval() {
        let error =
            parse_otel_config(Some("http://127.0.0.1:4317".to_owned()), Some(0)).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidValue);
    }

    #[test]
    fn rejects_disable_flags_without_pprof_address() {
        let error = parse_pprof_config(String::new(), true, false).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn rejects_when_all_routes_are_disabled() {
        let error = parse_pprof_config("127.0.0.1:8888".to_owned(), true, false).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidValue);
    }

    #[test]
    fn preserves_metrics_only_configuration() {
        let config = parse_pprof_config("127.0.0.1:8888".to_owned(), false, false)
            .expect("pprof config")
            .expect("pprof should be enabled");

        assert_eq!(
            config,
            PprofConfig {
                addr: "127.0.0.1:8888".to_owned(),
                enable_metrics: true,
                enable_profiling: false,
            }
        );
    }

    #[test]
    fn preserves_metrics_and_profiling_configuration() {
        let config = parse_pprof_config("127.0.0.1:8888".to_owned(), false, true)
            .expect("pprof config")
            .expect("pprof should be enabled");

        assert_eq!(
            config,
            PprofConfig {
                addr: "127.0.0.1:8888".to_owned(),
                enable_metrics: true,
                enable_profiling: true,
            }
        );
    }
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

    let forkpoint_bytes = client
        .get(remote_forkpoint_url.clone())
        .send()
        .and_then(|forkpoint_response| forkpoint_response.error_for_status())
        .and_then(|valid_forkpoint_response| valid_forkpoint_response.bytes())
        .map_err(|err| format!("error fetching remote forkpoint config: {err}"))?;
    let forkpoint_config =
        ForkpointConfig::try_parse_bytes(remote_forkpoint_url.path(), &forkpoint_bytes)
            .map_err(|err| format!("failed to parse remote forkpoint bytes (hint: if it's toml, file extension must be .toml), err={err}"))?;

    let validators_str = client
        .get(remote_validators_url)
        .send()
        .and_then(|validators_response| validators_response.error_for_status())
        .and_then(|valid_validators_response| valid_validators_response.text())
        .map_err(|err| format!("error fetching remote validators config: {err}"))?;
    let validators_config = ValidatorsConfigType::read_from_str(&validators_str)
        .map_err(|err| format!("failed to parse remote validators config: {err}"))?;

    Ok((forkpoint_config, validators_config))
}

fn get_latest_configs(
    forkpoint_config_path: &Path,
    validators_config_path: &Path,
    remote_configs_threshold: Round,
) -> Result<(ForkpointConfig, ValidatorsConfigType), NodeSetupError> {
    let local_configs = fetch_local_configs(forkpoint_config_path, validators_config_path);
    let remote_configs = fetch_remote_configs();

    let replace_local_configs =
        |remote_forkpoint: &ForkpointConfig, remote_validators: &ValidatorsConfigType| {
            // can fail if local forkpoint doesn't exist
            let _ = std::fs::rename(
                forkpoint_config_path,
                format!("{}.local", forkpoint_config_path.to_string_lossy()),
            );

            let forkpoint_toml_path = forkpoint_config_path.with_extension("toml");
            match remote_forkpoint.try_to_toml_string() {
                Ok(remote_forkpoint_toml) => {
                    std::fs::write(&forkpoint_toml_path, remote_forkpoint_toml).expect(
                        "failed to overwrite local forkpoint config toml with remote config",
                    );
                }
                Err(err) => {
                    warn!(?err, "failed to write remote forkpoint to toml string");
                    let _ = std::fs::remove_file(&forkpoint_toml_path);
                }
            }
            std::fs::write(
                forkpoint_config_path.with_extension("rlp"),
                remote_forkpoint.to_rlp_bytes(),
            )
            .expect("failed to overwrite local forkpoint config rlp with remote config");

            // can fail if local validators doesn't exist
            let _ = std::fs::rename(
                validators_config_path,
                format!("{}.local", validators_config_path.to_string_lossy()),
            );

            let remote_validators_str =
                toml::to_string_pretty(&ValidatorsConfigFile::from(remote_validators))
                    .expect("failed to re-serialize remote validators config");
            std::fs::write(validators_config_path, remote_validators_str)
                .expect("failed to overwrite local validators config with remote config");

            info!("replaced local configs with remote configs");
        };

    match (local_configs, remote_configs) {
        (Err(local_err), Err(remote_err)) => Err(NodeSetupError::Custom {
            kind: ErrorKind::MissingRequiredArgument,
            msg: format!(
                "failed to fetch local and remote configs, local_err={:?}, remote_err={:?}",
                local_err, remote_err
            ),
        }),
        (
            Ok((local_forkpoint_config, local_validators_config)),
            Ok((remote_forkpoint_config, remote_validators_config)),
        ) => {
            let local_forkpoint_round = local_forkpoint_config.high_certificate.round();
            let remote_forkpoint_round = remote_forkpoint_config.high_certificate.round();

            // if remote config is more recent, use that over local config
            if remote_forkpoint_round > local_forkpoint_round + remote_configs_threshold {
                info!(
                    ?remote_forkpoint_round,
                    ?local_forkpoint_round,
                    "local forkpoint over {} rounds older than remote forkpoint, using remote configs",
                    remote_configs_threshold.0
                );

                replace_local_configs(&remote_forkpoint_config, &remote_validators_config);
                return Ok((remote_forkpoint_config, remote_validators_config));
            }

            info!(
                ?remote_forkpoint_round,
                ?local_forkpoint_round,
                "local forkpoint within {} rounds of remote forkpoint, using local configs",
                remote_configs_threshold.0
            );

            if remote_forkpoint_round + remote_configs_threshold < local_forkpoint_round {
                // warn user if remote configs are stale
                warn!(
                    ?remote_forkpoint_round,
                    ?local_forkpoint_round,
                    "remote forkpoint over {} rounds older than local forkpoint",
                    remote_configs_threshold.0
                );
            }
            Ok((local_forkpoint_config, local_validators_config))
        }
        (Ok((local_forkpoint_config, local_validators_config)), Err(fetch_err)) => {
            info!(
                fetch_err,
                "failed to fetch remote configs, using local forkpoint and validators config"
            );

            Ok((local_forkpoint_config, local_validators_config))
        }
        (Err(fetch_err), Ok((remote_forkpoint_config, remote_validators_config))) => {
            info!(
                fetch_err,
                "failed to fetch local configs, using remote forkpoint and validators config"
            );

            replace_local_configs(&remote_forkpoint_config, &remote_validators_config);
            Ok((remote_forkpoint_config, remote_validators_config))
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
