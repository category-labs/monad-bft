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

use std::path::PathBuf;

use clap::Parser;
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_node_config::NodeConfig;

#[derive(Debug, Parser)]
#[command(name = "monad-node", about, long_about = None, version = monad_version::version!())]
pub struct Cli {
    /// Set the node config path
    #[arg(long)]
    pub node_config: PathBuf,

    /// Set the bls12_381 secret key path
    #[arg(long)]
    pub bls_identity: Option<PathBuf>,

    /// Set the secp256k1 key path
    #[arg(long)]
    pub secp_identity: Option<PathBuf>,

    /// Set the forkpoint config path
    #[arg(long)]
    pub forkpoint_config: Option<PathBuf>,

    /// Set the validators config path
    #[arg(long)]
    pub validators_path: Option<PathBuf>,

    /// Set devnet chain config override path
    #[arg(long)]
    pub devnet_chain_config_override: Option<PathBuf>,

    /// Set the path where the write-ahead log will be stored
    #[arg(long)]
    pub wal_path: Option<PathBuf>,

    /// Set the path where consensus blocks will be stored
    #[arg(long)]
    pub ledger_path: Option<PathBuf>,

    /// Set a custom monad mempool ipc path
    #[arg(long)]
    pub mempool_ipc_path: Option<PathBuf>,

    /// Set the monad triedb path
    #[arg(long)]
    pub triedb_path: Option<PathBuf>,

    /// Set a custom monad control panel ipc path
    #[arg(long)]
    pub control_panel_ipc_path: Option<PathBuf>,

    /// Set a custom monad statesync ipc path
    #[arg(long)]
    pub statesync_ipc_path: Option<PathBuf>,

    /// Set the sq_thread_cpu for statesync client. None means SQPOLL mode is
    /// disabled
    #[arg(long)]
    pub statesync_sq_thread_cpu: Option<u32>,

    /// Set the opentelemetry OTLP exporter endpoint
    #[arg(long)]
    pub otel_endpoint: Option<String>,

    /// Set the password for decrypting keystore file
    /// Default to empty string
    #[arg(long)]
    pub keystore_password: Option<String>,

    /// Set the time interval for metrics collection
    #[arg(long, requires = "otel_endpoint")]
    pub record_metrics_interval_seconds: Option<u64>,

    #[arg(
        long,
        help = "listen address for pprof server. pprof server won't be enabled if address is empty"
    )]
    pub pprof: Option<String>,

    #[arg(long)]
    pub manytrace_socket: Option<String>,

    /// Set the path for the file that will persist peer discovery records across restarts
    #[arg(long)]
    pub persisted_peers_path: Option<PathBuf>,
}

impl Cli {
    pub fn apply_to_config<ST>(self, config: &mut NodeConfig<ST>)
    where
        ST: CertificateSignatureRecoverable,
    {
        let Cli {
            node_config: _,

            bls_identity,
            secp_identity,
            forkpoint_config,
            validators_path,
            devnet_chain_config_override,
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
            persisted_peers_path,
        } = self;
        config.bls_identity = bls_identity.or(config.bls_identity.take());
        config.secp_identity = secp_identity.or(config.secp_identity.take());
        config.forkpoint_config = forkpoint_config.or(config.forkpoint_config.take());
        config.validators_path = validators_path.or(config.validators_path.take());
        config.devnet_chain_config_override =
            devnet_chain_config_override.or(config.devnet_chain_config_override.take());
        config.wal_path = wal_path.or(config.wal_path.take());
        config.ledger_path = ledger_path.or(config.ledger_path.take());
        config.mempool_ipc_path = mempool_ipc_path.or(config.mempool_ipc_path.take());
        config.triedb_path = triedb_path.or(config.triedb_path.take());
        config.control_panel_ipc_path =
            control_panel_ipc_path.or(config.control_panel_ipc_path.take());
        config.statesync_ipc_path = statesync_ipc_path.or(config.statesync_ipc_path.take());
        config.statesync_sq_thread_cpu =
            statesync_sq_thread_cpu.or(config.statesync_sq_thread_cpu.take());
        config.keystore_password = keystore_password.or(config.keystore_password.take());
        config.otel_endpoint = otel_endpoint.or(config.otel_endpoint.take());
        config.record_metrics_interval_seconds =
            record_metrics_interval_seconds.or(config.record_metrics_interval_seconds.take());
        config.pprof = pprof.or(config.pprof.take());
        config.manytrace_socket = manytrace_socket.or(config.manytrace_socket.take());
        config.persisted_peers_path = persisted_peers_path.or(config.persisted_peers_path.take());
    }
}
