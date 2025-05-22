use std::{net::SocketAddrV4, path::PathBuf};

use clap::Parser;
use monad_keystore::keystore::Keystore;
use monad_node_config::MonadNodeConfig;
use monad_peer_discovery::{MonadNameRecord, NameRecord};
use monad_secp::{KeyPair, SecpSignature};

/// Example command to run the following program:
/// sign-name-record -- --address 0.0.0.0:8888 --node-config <...> --keystore-path <...> --password ""
#[derive(Debug, Parser)]
#[command(name = "monad-peer-discovery", about)]
struct Args {
    /// SocketV4 address in format x.x.x.x:<port>
    #[arg(long)]
    address: SocketAddrV4,

    /// Set the node config path
    #[arg(long)]
    node_config: PathBuf,

    /// File path to secp keystore json file
    #[arg(long)]
    keystore_path: PathBuf,

    /// Keystore password
    #[arg(long)]
    password: String,
}

fn main() {
    let args = Args::parse();

    let result = Keystore::load_key(&args.keystore_path, &args.password);
    let mut private_key = match result {
        Ok(private_key) => private_key,
        Err(err) => {
            println!("Unable to read private key from keystore file: {:?}", err);
            return;
        }
    };

    let node_config: MonadNodeConfig =
        toml::from_str(&std::fs::read_to_string(args.node_config).expect("node.toml not found"))
            .unwrap();

    let self_address = args.address;
    let self_record_seq_num = node_config.peer_discovery.self_record_seq_num + 1;
    let name_record = NameRecord {
        address: self_address,
        seq: self_record_seq_num,
    };
    let keypair = KeyPair::from_bytes(&mut private_key).expect("Invalid keypair");
    let signed_name_record: MonadNameRecord<SecpSignature> =
        MonadNameRecord::new(name_record, &keypair);

    println!("self_address = {:?}", self_address.to_string());
    println!("self_record_seq_num = {}", self_record_seq_num);
    println!(
        "self_name_record_sig = {:?}",
        hex::encode(signed_name_record.signature.serialize())
    );
}
