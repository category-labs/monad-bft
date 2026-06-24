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

use std::{net::Ipv4Addr, num::NonZeroU16, panic, path::PathBuf};

use clap::Parser;
use monad_keystore::keystore::Keystore;
use monad_node_config::MonadNodeConfig;
use monad_peer_discovery::{MonadNameRecord, NameRecord};
use monad_secp::SecpSignature;

/// Example command to run the following program:
/// sign-name-record -- --ip 0.0.0.0 --tcp-port 8888 --udp-port 8888 --authenticated-udp-port 8889 --node-config <...> --keystore-path <...> --password ""
#[derive(Debug, Parser)]
#[command(name = "monad-peer-discovery", about)]
struct Args {
    /// IPv4 address for the name record.
    #[arg(long)]
    ip: Ipv4Addr,

    #[arg(long, help = "TCP port for the name record")]
    tcp_port: NonZeroU16,

    #[arg(long, help = "Optional non-authenticated UDP port")]
    udp_port: Option<NonZeroU16>,

    #[arg(long, help = "Authenticated UDP port for the name record")]
    authenticated_udp_port: NonZeroU16,

    #[arg(long, help = "Optional direct UDP port")]
    direct_udp_port: Option<NonZeroU16>,

    #[arg(long, help = "Optional authenticated TCP port")]
    authenticated_tcp_port: Option<NonZeroU16>,

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
    let name_record = NameRecord::new_with_ports(
        args.ip,
        args.tcp_port.get(),
        args.udp_port.map(NonZeroU16::get),
        args.authenticated_udp_port.get(),
        args.direct_udp_port.map(NonZeroU16::get),
        args.authenticated_tcp_port.map(NonZeroU16::get),
        self_record_seq_num,
    );
    let signed_name_record: MonadNameRecord<SecpSignature> =
        MonadNameRecord::new(name_record, &keypair);

    println!("self_address = {:?}", args.ip.to_string());
    println!("self_tcp_port = {}", args.tcp_port);
    if let Some(udp_port) = args.udp_port {
        println!("self_udp_port = {}", udp_port);
    }
    println!("self_record_seq_num = {}", self_record_seq_num);
    println!("authenticated_udp_port = {}", args.authenticated_udp_port);
    if let Some(direct_udp_port) = args.direct_udp_port {
        println!("direct_udp_port = {}", direct_udp_port);
    }
    if let Some(authenticated_tcp_port) = args.authenticated_tcp_port {
        println!("authenticated_tcp_port = {}", authenticated_tcp_port);
    }
    println!(
        "self_name_record_sig = {:?}",
        hex::encode(signed_name_record.signature.serialize())
    );
}
