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
use monad_node_config::MonadNodeConfig;
use monad_peer_discovery::{MonadNameRecord, NameRecord};
use monad_secp::SecpSignature;

/// Recover the node ID (public key) from self.name_record in node.toml
///
/// Example command to run:
/// cargo run --example recover-node-id -- --node-config /path/to/node.toml
#[derive(Debug, Parser)]
#[command(name = "recover-node-id", about)]
struct Args {
    /// Path to node.toml configuration file
    #[arg(long)]
    node_config: PathBuf,
}

fn main() {
    let args = Args::parse();

    // Read and parse the node.toml file
    let config_contents = match std::fs::read_to_string(&args.node_config) {
        Ok(contents) => contents,
        Err(err) => {
            eprintln!("Failed to read node.toml file: {:?}", err);
            std::process::exit(1);
        }
    };

    let node_config: MonadNodeConfig = match toml::from_str(&config_contents) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("Failed to parse node.toml file: {:?}", err);
            std::process::exit(1);
        }
    };

    // Parse the self_address
    let self_address = match node_config.peer_discovery.self_address.parse::<std::net::SocketAddrV4>() {
        Ok(addr) => addr,
        Err(err) => {
            eprintln!("Failed to parse self_address: {:?}", err);
            std::process::exit(1);
        }
    };

    // Construct the NameRecord
    let name_record = NameRecord::new(
        *self_address.ip(),
        self_address.port(),
        node_config.peer_discovery.self_record_seq_num,
    );

    // Construct the MonadNameRecord with the signature from config
    let monad_name_record = MonadNameRecord::<SecpSignature> {
        name_record,
        signature: node_config.peer_discovery.self_name_record_sig,
    };

    // Recover the public key (node_id)
    match monad_name_record.recover_pubkey() {
        Ok(node_id) => {
            println!("Successfully recovered node_id from self.name_record:");
            println!();
            println!("node_id (pubkey): {}", hex::encode(node_id.pubkey().bytes()));
            println!();
            println!("Details:");
            println!("  self_address: {}", self_address);
            println!("  self_record_seq_num: {}", node_config.peer_discovery.self_record_seq_num);
            println!("  self_name_record_sig: {}", hex::encode(node_config.peer_discovery.self_name_record_sig.serialize()));
        }
        Err(err) => {
            eprintln!("Failed to recover public key from signature: {:?}", err);
            std::process::exit(1);
        }
    }
}
