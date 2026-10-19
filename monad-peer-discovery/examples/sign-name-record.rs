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

use clap::{Args as _, CommandFactory, FromArgMatches, Parser};
use monad_keystore::keystore::Keystore;
use monad_node_config::MonadNodeConfig;
use monad_peer_discovery::{MonadNameRecord, NameRecord, name_record};
use monad_secp::SecpSignature;

const PRINT_EXAMPLE: &str = r"Print a node configuration snippet
  sign-name-record --address 192.0.2.1:8000 --authenticated-udp-port 8001 \
    --node-config /path/to/node.toml --keystore-path /path/to/id-secp --password ''";

const WRITE_EXAMPLE: &str = concat!(
    "Write a signed self.toml file\n",
    "  sign-name-record write --node-config /path/to/node.toml --output /path/to/self.toml",
);

/// Sign a peer discovery name record and print its settings or write it to a file.
///
/// Run without a subcommand to sign the supplied endpoints and print a node configuration snippet.
/// With --node-config, this mode signs self_record_seq_num + 1.
///
/// Run write to use the endpoints and current sequence in node.toml and save the signed record
/// in self.toml format.
#[derive(Debug, Parser)]
#[command(name = "monad-peer-discovery")]
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

    #[arg(long, help = "Optional encrypted TCP port")]
    encrypted_tcp_port: Option<NonZeroU16>,

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

#[derive(clap::Args)]
struct WriteArgs {
    #[arg(
        long,
        value_name = "FILE",
        help = "Read advertised endpoints, sequence, and any configured signature from this node TOML file",
        long_help = "Read advertised endpoints, sequence, and any configured signature from this node TOML file. \
                     Omit peer_discovery.self_name_record_sig to sign automatically. A configured signature is used \
                     at revision 0.\n\n\
                     For a hostname in self_address, the first IPv4 DNS answer becomes the signed address. \
                     A wrong DNS answer can produce a valid signature for an unintended IP. \
                     Use a literal IPv4 address to pin it."
    )]
    node_config: PathBuf,

    #[arg(
        long,
        value_name = "FILE",
        help = "Write the signed record to this file in self.toml format"
    )]
    output: PathBuf,

    #[arg(
        long,
        value_name = "FILE",
        help = "Path to the secp256k1 keystore. Defaults to id-secp in the node config directory"
    )]
    keystore_path: Option<PathBuf>,

    #[arg(
        long,
        default_value = "",
        hide_default_value = true,
        help = "Password for the secp256k1 keystore. Defaults to an empty password"
    )]
    password: String,
}

impl WriteArgs {
    fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let key_path = self
            .keystore_path
            .unwrap_or_else(|| self.node_config.with_file_name("id-secp"));
        let keypair = Keystore::load_secp_key(&key_path, &self.password).map_err(|err| {
            format!(
                "unable to load secp keystore {}: {err:?}",
                key_path.display()
            )
        })?;
        let config: MonadNodeConfig = toml::from_str(&std::fs::read_to_string(self.node_config)?)?;
        let peer_discovery = &config.peer_discovery;
        let record = name_record::from_config(peer_discovery)?;
        name_record::load_or_create(
            &self.output,
            record,
            peer_discovery.self_name_record_sig,
            &keypair,
            name_record::Mode::ReadWrite,
        )?;
        Ok(())
    }
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

fn command() -> clap::Command {
    Args::command()
        .after_help(format!("{PRINT_EXAMPLE}\n\n{WRITE_EXAMPLE}"))
        .subcommand(WriteArgs::augment_args(
            clap::Command::new("write")
                .about("Prepare and write a signed name record from node config")
                .after_help(WRITE_EXAMPLE),
        ))
        .args_conflicts_with_subcommands(true)
        .subcommand_negates_reqs(true)
}

fn main() {
    let mut matches = command().get_matches();
    if let Some((_, mut matches)) = matches.remove_subcommand() {
        let args = WriteArgs::from_arg_matches_mut(&mut matches).unwrap_or_else(|err| err.exit());
        if let Err(err) = args.run() {
            eprintln!("{err}");
            std::process::exit(1);
        }
        return;
    }
    let args = Args::from_arg_matches_mut(&mut matches).unwrap_or_else(|err| err.exit());
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
        args.encrypted_tcp_port.map(NonZeroU16::get),
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
    if let Some(encrypted_tcp_port) = args.encrypted_tcp_port {
        println!("self_encrypted_tcp_port = {}", encrypted_tcp_port);
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

    #[test]
    fn write_requires_only_node_config_and_output() {
        let matches = command()
            .try_get_matches_from([
                "sign-name-record",
                "write",
                "--node-config",
                "node.toml",
                "--output",
                "self.toml",
            ])
            .unwrap();
        let args =
            WriteArgs::from_arg_matches(matches.subcommand_matches("write").unwrap()).unwrap();
        assert_eq!(args.node_config, PathBuf::from("node.toml"));
        assert_eq!(args.output, PathBuf::from("self.toml"));
        assert!(args.keystore_path.is_none());
        assert!(args.password.is_empty());

        for args in [
            ["sign-name-record", "write", "--node-config", "node.toml"],
            ["sign-name-record", "write", "--output", "self.toml"],
        ] {
            assert_eq!(
                command().try_get_matches_from(args).unwrap_err().kind(),
                ErrorKind::MissingRequiredArgument
            );
        }
    }

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
