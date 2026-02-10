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

use std::{net::SocketAddrV4, panic, path::PathBuf};

use clap::Parser;
use monad_keystore::keystore::Keystore;
use monad_node_config::MonadNodeConfig;
use monad_peer_discovery::{MonadNameRecord, NameRecord};
use monad_secp::SecpSignature;

/// Example command to run the following program:
/// sign-name-record -- --address 0.0.0.0:8888 --node-config <...> --keystore-path <...> --password ""
#[derive(Debug, Parser)]
#[command(name = "monad-peer-discovery", about)]
struct Args {
    /// SocketV4 address in format x.x.x.x:<port>
    #[arg(long)]
    address: SocketAddrV4,

    #[arg(
        long,
        help = "Optional authenticated UDP port. If provided, will create name record with authenticated UDP port"
    )]
    authenticated_udp_port: Option<u16>,

    #[arg(
        long,
        help = "Optional authenticated tx-ingestion UDP port. Requires --authenticated-udp-port"
    )]
    auth_tx_ingestion_port: Option<u16>,

    /// Sequence number for the name record
    #[arg(long)]
    self_record_seq_num: Option<u64>,

    /// Set the node config path
    #[arg(long)]
    node_config: Option<PathBuf>,

    /// File path to secp keystore json file
    #[arg(long)]
    keystore_path: PathBuf,

    /// Keystore password
    #[arg(long)]
    password: String,
}

fn main() {
    let args = Args::parse();

    let keypair = match Keystore::load_secp_key(&args.keystore_path, &args.password) {
        Ok(keypair) => keypair,
        Err(err) => {
            println!("Unable to read private key from keystore file: {:?}", err);
            return;
        }
    };

    let self_record_seq_num = if let Some(node_config_path) = args.node_config {
        let contents =
            std::fs::read_to_string(node_config_path).expect("Failed to read node toml file");
        let node_config: MonadNodeConfig =
            toml::from_str(&contents).expect("Invalid format in node toml file");
        node_config.peer_discovery.self_record_seq_num + 1
    } else {
        args.self_record_seq_num
            .unwrap_or_else(|| panic!("Either node_config or self_record_seq_num must be provided"))
    };
    let self_address = args.address;
    let name_record = match (args.authenticated_udp_port, args.auth_tx_ingestion_port) {
        (Some(authenticated_udp_port), Some(auth_tx_ingestion_port)) => {
            NameRecord::new_with_lean_udp_p2p(
                *self_address.ip(),
                self_address.port(),
                self_address.port(),
                authenticated_udp_port,
                auth_tx_ingestion_port,
                self_record_seq_num,
            )
        }
        (Some(authenticated_udp_port), None) => NameRecord::new_with_authentication(
            *self_address.ip(),
            self_address.port(),
            self_address.port(),
            authenticated_udp_port,
            self_record_seq_num,
        ),
        (None, Some(_)) => {
            panic!("--auth-tx-ingestion-port requires --authenticated-udp-port")
        }
        (None, None) => {
            NameRecord::new(*self_address.ip(), self_address.port(), self_record_seq_num)
        }
    };
    let signed_name_record: MonadNameRecord<SecpSignature> =
        MonadNameRecord::new(name_record, &keypair);

    println!("self_address = {:?}", self_address.to_string());
    println!("self_record_seq_num = {}", self_record_seq_num);
    if let Some(authenticated_udp_port) = args.authenticated_udp_port {
        println!("authenticated_udp_port = {}", authenticated_udp_port);
    }
    if let Some(auth_tx_ingestion_port) = args.auth_tx_ingestion_port {
        println!("auth_tx_ingestion_port = {}", auth_tx_ingestion_port);
    }
    println!(
        "self_name_record_sig = {:?}",
        hex::encode(signed_name_record.signature.serialize())
    );
}
