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
    collections::{BTreeMap, BTreeSet},
    io::Read,
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
};

use clap::Parser;
use monad_consensus_types::validator_data::ValidatorsConfig;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_node_config::{
    MonadNodeConfig, NodeBootstrapConfig, SignatureCollectionType, SignatureType,
};
use monad_types::NodeId;

fn main() {
    let args = Args::parse();

    // Read NodeBootstrapConfig from stdin
    let mut stdin_buffer = String::new();
    std::io::stdin()
        .read_to_string(&mut stdin_buffer)
        .expect("failed to read from stdin");

    let known_peers: NodeBootstrapConfig<SignatureType> =
        toml::from_str(&stdin_buffer).expect("failed to parse NodeBootstrapConfig from stdin");

    // Read NodeConfig from file path
    let node_config: MonadNodeConfig = toml::from_str(
        &std::fs::read_to_string(&args.config_path).expect("failed to read config file"),
    )
    .expect("failed to parse NodeConfig from file");

    // Read ValidatorsConfig from file path
    let validators_config: ValidatorsConfig<SignatureCollectionType> =
        ValidatorsConfig::read_from_path(&args.validators_path).unwrap_or_else(|err| {
            panic!(
                "failed to read validators.toml, or validators.toml corrupt. was this edited manually? err={:?}",
                err
            )
        });

    // Create IP mapping from known_peers
    // Key: NodeId (public key), Value: IPv4 socket address
    let ip_mapping: BTreeMap<NodeId<CertificateSignaturePubKey<SignatureType>>, Ipv4Addr> =
        known_peers
            .peers
            .iter()
            .map(|peer| {
                // Parse SocketAddrV4 from address string (format: "ip:port")
                let socket_addr = peer
                    .address
                    .parse::<SocketAddrV4>()
                    .expect(&format!("failed to parse address {}", peer.address));
                (NodeId::new(peer.secp256k1_pubkey), *socket_addr.ip())
            })
            .collect();

    // validators for current and next epoch
    let validators: BTreeSet<_> = validators_config
        .validators
        .into_values()
        .flat_map(|set| set.0)
        .map(|v| v.node_id)
        .collect();
    let dedicated_full_nodes: BTreeSet<_> = node_config
        .fullnode_dedicated
        .identities
        .into_iter()
        .map(|fullnode| NodeId::new(fullnode.secp256k1_pubkey))
        .collect();
    let prioritized_full_nodes: BTreeSet<_> = node_config
        .fullnode_raptorcast
        .full_nodes_prioritized
        .identities
        .into_iter()
        .map(|fullnode| NodeId::new(fullnode.secp256k1_pubkey))
        .collect();

    let whitelist: BTreeSet<_> = validators
        .iter()
        .cloned()
        .chain(dedicated_full_nodes.iter().cloned())
        .chain(prioritized_full_nodes.iter().cloned())
        .collect();

    for node_id in whitelist {
        if let Some(ip) = ip_mapping.get(&node_id) {
            println!("{}", ip);
        } else {
            let role = if validators.contains(&node_id) {
                "Validator"
            } else if dedicated_full_nodes.contains(&node_id) {
                "Dedicated full node"
            } else if prioritized_full_nodes.contains(&node_id) {
                "Prioritized full node"
            } else {
                ""
            };
            eprintln!("{}: {:?} not found in peers map", role, node_id);
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    about = "Generate IP whitelist for validators and full nodes",
    long_about = "Whitelist IP Utility\n
This utility generates an IP whitelist by combining peer information with node configuration data.

INPUTS:
  • NodeBootstrapConfig (peers) - Read from stdin in TOML format
  • NodeConfig - Loaded from --config-path (contains full node identities)
  • ValidatorsConfig - Loaded from --validators-path (contains validator identities)

OUTPUTS:
  • stdout: List of IP addresses (one per line) for whitelisted nodes
  • stderr: Warnings for NodeIds not found in the peer mapping

INCLUDED NODES:
  • All validators (current and next epoch)
  • Dedicated full nodes
  • Prioritized full nodes

EXAMPLE USAGE:
  # Using monad-debug-node to fetch peers
  monad-debug-node --control-panel-ipc-path /path/to/controlpanel.sock get-peers | \\
    whitelist-ip --config-path /path/to/node.toml --validators-path /path/to/validators.toml

  # Using a static peers file
  cat peers.toml | whitelist-ip -c node.toml -v validators.toml"
)]
pub struct Args {
    /// Path to the node.toml
    #[arg(long, short, default_value = "/monad/config/node.toml")]
    pub config_path: PathBuf,

    /// Path to validators.toml
    #[arg(
        long,
        short,
        default_value = "/monad/config/validators/validators.toml"
    )]
    pub validators_path: PathBuf,
}
