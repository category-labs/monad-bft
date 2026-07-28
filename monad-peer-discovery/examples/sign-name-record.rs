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
    net::{Ipv4Addr, SocketAddrV4},
    num::NonZeroU16,
    panic,
    path::PathBuf,
};

use clap::Parser;
use monad_keystore::keystore::Keystore;
use monad_node_config::MonadNodeConfig;
use monad_peer_discovery::{MonadNameRecord, NameRecord};
use monad_secp::SecpSignature;

/// Example commands to run the following program:
/// sign-name-record -- --address 0.0.0.0:8888 --authenticated-udp-port 8889 --node-config <...> --keystore-path <...> --password ""
/// sign-name-record -- --ip 0.0.0.0 --tcp-port 8888 --udp-port 8888 --authenticated-udp-port 8889 --node-config <...> --keystore-path <...> --password ""
#[derive(Debug, Parser)]
#[command(name = "monad-peer-discovery", about)]
struct Args {
    /// Legacy IPv4 address and shared TCP/UDP port.
    #[arg(long, conflicts_with_all = ["ip", "tcp_port", "udp_port"])]
    address: Option<SocketAddrV4>,

    /// IPv4 address for the name record.
    #[arg(long, required_unless_present = "address")]
    ip: Option<Ipv4Addr>,

    #[arg(
        long,
        required_unless_present = "address",
        help = "TCP port for the name record"
    )]
    tcp_port: Option<NonZeroU16>,

    #[arg(long, help = "Optional non-authenticated UDP port")]
    udp_port: Option<NonZeroU16>,

    #[arg(long, help = "Authenticated UDP port for the name record")]
    authenticated_udp_port: NonZeroU16,

    #[arg(long, help = "Optional direct UDP port")]
    direct_udp_port: Option<NonZeroU16>,

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

#[derive(Debug, PartialEq, Eq)]
enum Endpoint {
    Legacy(SocketAddrV4),
    Split {
        ip: Ipv4Addr,
        tcp_port: NonZeroU16,
        udp_port: Option<NonZeroU16>,
    },
}

impl Endpoint {
    fn name_record_parts(&self) -> (Ipv4Addr, u16, Option<u16>) {
        match self {
            Self::Legacy(address) => (*address.ip(), address.port(), Some(address.port())),
            Self::Split {
                ip,
                tcp_port,
                udp_port,
            } => (*ip, tcp_port.get(), udp_port.map(NonZeroU16::get)),
        }
    }

    fn print_config(&self) {
        match self {
            Self::Legacy(address) => {
                println!("self_address = {:?}", address.to_string());
            }
            Self::Split {
                ip,
                tcp_port,
                udp_port,
            } => {
                println!("self_address = {:?}", ip.to_string());
                println!("self_tcp_port = {}", tcp_port);
                if let Some(udp_port) = udp_port {
                    println!("self_udp_port = {}", udp_port);
                }
            }
        }
    }
}

impl Args {
    fn endpoint(&self) -> Endpoint {
        match self.address {
            Some(address) => Endpoint::Legacy(address),
            None => Endpoint::Split {
                ip: self.ip.expect("--ip is required without --address"),
                tcp_port: self
                    .tcp_port
                    .expect("--tcp-port is required without --address"),
                udp_port: self.udp_port,
            },
        }
    }
}

fn main() {
    let args = Args::parse();
    let endpoint = args.endpoint();
    let (ip, tcp_port, udp_port) = endpoint.name_record_parts();

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
        ip,
        tcp_port,
        udp_port,
        args.authenticated_udp_port.get(),
        args.direct_udp_port.map(NonZeroU16::get),
        self_record_seq_num,
    );
    let signed_name_record: MonadNameRecord<SecpSignature> =
        MonadNameRecord::new(name_record, &keypair);

    endpoint.print_config();
    println!("self_record_seq_num = {}", self_record_seq_num);
    println!("self_auth_port = {}", args.authenticated_udp_port);
    if let Some(direct_udp_port) = args.direct_udp_port {
        println!("self_direct_udp_auth_port = {}", direct_udp_port);
    }
    println!(
        "self_name_record_sig = {:?}",
        hex::encode(signed_name_record.signature.serialize())
    );
}

#[cfg(test)]
mod tests {
    use clap::{Parser, error::ErrorKind};

    use super::*;

    fn parse(endpoint: &[&str]) -> Result<Args, clap::Error> {
        let mut arguments = vec!["sign-name-record"];
        arguments.extend_from_slice(endpoint);
        arguments.extend_from_slice(&[
            "--authenticated-udp-port",
            "8001",
            "--keystore-path",
            "/tmp/id-secp",
            "--password",
            "secret",
            "--self-record-seq-num",
            "0",
        ]);
        Args::try_parse_from(arguments)
    }

    #[test]
    fn legacy_address_sets_tcp_and_udp_ports() {
        let args = parse(&["--address", "65.109.127.109:8000"]).unwrap();
        let endpoint = args.endpoint();

        assert_eq!(
            endpoint,
            Endpoint::Legacy("65.109.127.109:8000".parse().unwrap())
        );
        assert_eq!(
            endpoint.name_record_parts(),
            ("65.109.127.109".parse().unwrap(), 8000, Some(8000))
        );
    }

    #[test]
    fn split_address_keeps_independent_ports() {
        let args = parse(&[
            "--ip",
            "65.109.127.109",
            "--tcp-port",
            "8000",
            "--udp-port",
            "8002",
        ])
        .unwrap();
        let endpoint = args.endpoint();

        assert_eq!(
            endpoint,
            Endpoint::Split {
                ip: "65.109.127.109".parse().unwrap(),
                tcp_port: NonZeroU16::new(8000).unwrap(),
                udp_port: NonZeroU16::new(8002),
            }
        );
        assert_eq!(
            endpoint.name_record_parts(),
            ("65.109.127.109".parse().unwrap(), 8000, Some(8002))
        );
    }

    #[test]
    fn endpoint_forms_are_mutually_exclusive() {
        let error = parse(&[
            "--address",
            "65.109.127.109:8000",
            "--ip",
            "65.109.127.109",
            "--tcp-port",
            "8000",
        ])
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
    }

    #[test]
    fn split_address_requires_tcp_port() {
        let error = parse(&["--ip", "65.109.127.109"]).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }
}
