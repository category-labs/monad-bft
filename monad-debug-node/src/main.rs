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

use std::{io::Error, path::PathBuf};

use clap::{ArgGroup, Args, Parser, Subcommand};
use futures::{SinkExt, StreamExt};
use itertools::Itertools;
use monad_crypto::certificate_signature::CertificateSignatureRecoverable;
use monad_executor_glue::{
    ClearMetrics, ControlPanelCommand, GetFullNodes, GetMetrics, GetPeers, ReadCommand,
    WriteCommand,
};
use monad_node_config::{
    FullNodeConfig, FullNodeIdentityConfig, NodeBootstrapConfig, NodeBootstrapPeerConfig,
};
use monad_secp::SecpSignature;
use tokio::net::{
    unix::{OwnedReadHalf, OwnedWriteHalf},
    UnixStream,
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

type SignatureType = SecpSignature;
type Command = ControlPanelCommand<SignatureType>;

/// CLI program to manage validators and metrics.
#[derive(Parser, Debug)]
#[command(name = "Validator Manager")]
#[command(about = "CLI program to manage validators and metrics", long_about = None)]
struct Cli {
    #[arg(short, long)]
    control_panel_ipc_path: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Args)]
#[clap(group(ArgGroup::new("method").required(true).args(&["filter", "file"])))]
struct UpdateLogFilter {
    #[arg(long, value_name = "FILTER")]
    filter: Option<String>,
    #[arg(long, value_name = "FILE")]
    file: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Gets snapshot of current metrics
    Metrics,
    /// Clears the metrics
    ClearMetrics,
    /// Update the logging filter
    UpdateLogFilter(UpdateLogFilter),
    /// Display peer list
    GetPeers,
    /// Display full node list
    GetFullNodes,
    /// Reload node config
    ReloadConfig,
}

struct Read {
    read: FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
}

impl Read {
    pub async fn next<ST: CertificateSignatureRecoverable>(
        &mut self,
    ) -> Result<ControlPanelCommand<ST>, Error> {
        let bytes = self
            .read
            .next()
            .await
            .ok_or(Error::other("client stream ended"))??
            .freeze();
        serde_json::from_str(std::str::from_utf8(&bytes).map_err(Error::other)?)
            .map_err(Error::other)
    }
}

struct Write {
    write: FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
}

impl Write {
    pub async fn send<ST: CertificateSignatureRecoverable>(
        &mut self,
        request: ControlPanelCommand<ST>,
    ) -> Result<(), Error> {
        let bytes = serde_json::to_string(&request).map_err(Error::other)?;
        self.write.send(bytes.into()).await
    }
}

fn main() -> Result<(), Error> {
    let cli = Cli::parse();
    let socket_path = cli.control_panel_ipc_path;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (read, write) = rt.block_on(async move {
        let client_stream = UnixStream::connect(socket_path.as_path()).await?;

        let (read, write) = client_stream.into_split();

        let read = FramedRead::new(read, LengthDelimitedCodec::default());
        let write = FramedWrite::new(write, LengthDelimitedCodec::default());

        Ok::<_, Error>((read, write))
    })?;

    let mut read = Read { read };
    let mut write = Write { write };

    match cli.command {
        Commands::ClearMetrics => {
            rt.block_on(write.send(Command::Write(WriteCommand::ClearMetrics(
                ClearMetrics::Request,
            ))))?;

            let response = rt.block_on(read.next::<SignatureType>())?;
            println!("{}", serde_json::to_string(&response).unwrap());
        }

        Commands::UpdateLogFilter(filter) => match (filter.filter, filter.file) {
            (Some(filter), None) => {
                rt.block_on(write.send(Command::Write(WriteCommand::UpdateLogFilter(filter))))?;

                let response = rt.block_on(read.next::<SignatureType>())?;
                println!("{}", serde_json::to_string(&response).unwrap());
            }
            (None, Some(file)) => {
                rt.block_on(
                    write.send(Command::Write(WriteCommand::UpdateLogFilter(
                        std::fs::read_to_string(file)?
                            .split("\n")
                            .filter(|s| !s.is_empty())
                            .join(","),
                    ))),
                )?;

                let response = rt.block_on(read.next::<SignatureType>())?;
                println!("{}", serde_json::to_string(&response).unwrap());
            }
            _ => unreachable!(),
        },
        Commands::Metrics => {
            rt.block_on(write.send(Command::Read(ReadCommand::GetMetrics(GetMetrics::Request))))?;
            let response = rt.block_on(read.next::<SignatureType>())?;
            println!("{}", serde_json::to_string(&response).unwrap());
        }

        Commands::GetPeers => {
            rt.block_on(write.send(Command::Read(ReadCommand::GetPeers(GetPeers::Request))))?;
            let response = rt.block_on(read.next::<SignatureType>())?;
            if let ControlPanelCommand::Read(ReadCommand::GetPeers(GetPeers::Response(peers))) =
                response
            {
                // build toml config from peer list
                let mut peer_configs = Vec::new();
                for peer in peers {
                    let peer_config = NodeBootstrapPeerConfig {
                        address: peer.addr.to_string(),
                        secp256k1_pubkey: peer.pubkey,
                        name_record_sig: peer.signature,
                        record_seq_num: peer.record_seq_num,
                        auth_port: peer.auth_port,
                        auth_tx_ingestion_port: peer.auth_tx_ingestion_port,
                    };
                    peer_configs.push(peer_config);
                }
                let bootstrap_config = NodeBootstrapConfig {
                    peers: peer_configs,
                };
                println!("{}", toml::to_string(&bootstrap_config).unwrap());
            } else {
                println!(
                    "unexpected response{}",
                    serde_json::to_string(&response).unwrap()
                )
            }
        }
        Commands::GetFullNodes => {
            rt.block_on(write.send(Command::Read(ReadCommand::GetFullNodes(
                GetFullNodes::Request,
            ))))?;
            let response = rt.block_on(read.next::<SignatureType>())?;
            if let ControlPanelCommand::Read(ReadCommand::GetFullNodes(GetFullNodes::Response(
                full_nodes,
            ))) = response
            {
                // build toml config from peer list
                let mut full_node_configs = Vec::new();
                for node in full_nodes {
                    let config = FullNodeIdentityConfig {
                        secp256k1_pubkey: node.pubkey(),
                    };
                    full_node_configs.push(config);
                }
                let full_node_config = FullNodeConfig {
                    identities: full_node_configs,
                };

                println!("{}", toml::to_string(&full_node_config).unwrap());
            } else {
                println!(
                    "unexpected response{}",
                    serde_json::to_string(&response).unwrap()
                )
            }
        }
        Commands::ReloadConfig => {
            rt.block_on(write.send(Command::Write(WriteCommand::ReloadConfig(
                monad_executor_glue::ReloadConfig::Request,
            ))))?;

            let response = rt.block_on(read.next::<SignatureType>())?;
            if let ControlPanelCommand::Write(WriteCommand::ReloadConfig(
                monad_executor_glue::ReloadConfig::Response(msg),
            )) = response
            {
                println!("{}", msg);
            } else {
                println!(
                    "unexpected response{}",
                    serde_json::to_string(&response).unwrap()
                )
            }
        }
    }

    Ok(())
}
