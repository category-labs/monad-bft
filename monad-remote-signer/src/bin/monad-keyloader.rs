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

//! One-time provisioning client.
//!
//! Reads encrypted keystore files on the host and forwards the **ciphertext +
//! password** to the enclave, which decrypts them in its protected memory. The
//! keyloader never decrypts on the host, so the plaintext secret never exists
//! in host RAM. After provisioning, the operator deletes the keystore files
//! from the running host.

use std::{path::PathBuf, process::exit};

use clap::Parser;
use monad_remote_signer_proto::{
    ProvisionBundle, ProvisionRequest, RemoteSigner, TransportConfig,
};

#[derive(Parser, Debug)]
#[command(about = "Provision the SEV-SNP remote signer with keystore keys")]
struct Args {
    /// secp256k1 keystore JSON file to load into the enclave.
    #[arg(long)]
    secp: Option<PathBuf>,

    /// BLS12-381 keystore JSON file to load into the enclave.
    #[arg(long)]
    bls: Option<PathBuf>,

    /// Keystore passphrase (forwarded to the enclave; decryption happens there).
    /// If omitted, falls back to the KEYSTORE_PASSWORD env var, then empty.
    /// Prefer the env var so the secret never appears in process arguments.
    #[arg(long)]
    password: Option<String>,

    /// Provision over AF_VSOCK to this guest CID (with --vsock-port).
    #[arg(long)]
    vsock_cid: Option<u32>,

    /// AF_VSOCK port (with --vsock-cid).
    #[arg(long)]
    vsock_port: Option<u32>,

    /// Provision over this unix socket (development transport).
    #[arg(long)]
    unix: Option<PathBuf>,
}

fn read_bundle(path: &PathBuf, password: &str) -> ProvisionBundle {
    let keystore_json = std::fs::read(path)
        .unwrap_or_else(|e| panic!("read keystore {}: {e}", path.display()));
    ProvisionBundle {
        keystore_json,
        password: password.to_string(),
    }
}

fn main() {
    let args = Args::parse();

    let password = args
        .password
        .clone()
        .or_else(|| std::env::var("KEYSTORE_PASSWORD").ok())
        .unwrap_or_default();

    let transport = match (args.vsock_cid, args.vsock_port, args.unix.as_ref()) {
        (Some(cid), Some(port), None) => TransportConfig::Vsock { cid, port },
        (None, None, Some(path)) => TransportConfig::Unix { path: path.clone() },
        _ => {
            eprintln!(
                "error: specify a transport: (--vsock-cid N --vsock-port N) or --unix <PATH>"
            );
            exit(2);
        }
    };

    if args.secp.is_none() && args.bls.is_none() {
        eprintln!("error: provide at least one of --secp / --bls");
        exit(2);
    }

    let req = ProvisionRequest {
        secp: args.secp.as_ref().map(|p| read_bundle(p, &password)),
        bls: args.bls.as_ref().map(|p| read_bundle(p, &password)),
    };

    let signer = RemoteSigner::connect(transport, 1).unwrap_or_else(|e| {
        eprintln!("error: cannot reach signer: {e}");
        exit(1);
    });

    match signer.provision(&req) {
        Ok(pubkeys) => {
            println!("provisioned OK");
            println!("  secp pubkey: 0x{}", hex::encode(&pubkeys.secp));
            println!("  bls  pubkey: 0x{}", hex::encode(&pubkeys.bls));
            eprintln!(
                "\nNext: shred the keystore file(s) on this host, then start the node in remote mode."
            );
        }
        Err(e) => {
            eprintln!("error: provisioning failed: {e}");
            exit(1);
        }
    }
}
