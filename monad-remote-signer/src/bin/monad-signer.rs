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

//! The enclave signer service. Runs *inside* the SEV-SNP confidential VM and
//! serves signing / ECDH / provisioning requests over AF_VSOCK (production) or
//! a unix socket (development).

use std::{io::Read, sync::Arc, thread};

use clap::Parser;
use monad_bls::BlsKeyPair;
use monad_remote_signer::{serve_conn, SignerState};
use monad_secp::KeyPair;

#[derive(Parser, Debug)]
#[command(about = "SEV-SNP remote signer (enclave side)")]
struct Args {
    /// Listen on this AF_VSOCK port (any CID). Production transport.
    #[arg(long)]
    vsock_port: Option<u32>,

    /// Listen on this unix socket path. Development transport.
    #[arg(long)]
    unix: Option<String>,

    /// Generate fresh keys on boot instead of waiting to be provisioned.
    /// Convenience for local demos; in production the keyloader provisions.
    #[arg(long)]
    generate: bool,
}

/// 32 bytes of OS randomness (used only by `--generate`).
fn random_32() -> std::io::Result<[u8; 32]> {
    let mut f = std::fs::File::open("/dev/urandom")?;
    let mut buf = [0u8; 32];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

fn main() {
    tracing_subscriber::fmt().init();

    let args = Args::parse();
    let state = Arc::new(SignerState::new());

    if args.generate {
        let mut secp_seed = random_32().expect("read /dev/urandom");
        let mut bls_seed = random_32().expect("read /dev/urandom");
        let secp = KeyPair::from_bytes(&mut secp_seed).expect("secp keygen");
        let bls = BlsKeyPair::from_bytes(&mut bls_seed).expect("bls keygen");
        tracing::info!(
            secp_pubkey = %hex::encode(secp.pubkey().bytes_compressed()),
            bls_pubkey = %hex::encode(bls.pubkey().compress()),
            "generated fresh keys in-enclave (--generate)"
        );
        state.set_keys(Some(secp), Some(bls));
    }

    match (args.vsock_port, args.unix.as_deref()) {
        (Some(port), None) => serve_vsock(state, port),
        (None, Some(path)) => serve_unix(state, path),
        (Some(_), Some(_)) => {
            eprintln!("error: specify only one of --vsock-port or --unix");
            std::process::exit(2);
        }
        (None, None) => {
            eprintln!("error: specify a listen transport: --vsock-port <N> or --unix <PATH>");
            std::process::exit(2);
        }
    }
}

fn serve_unix(state: Arc<SignerState>, path: &str) {
    let _ = std::fs::remove_file(path);
    let listener = std::os::unix::net::UnixListener::bind(path)
        .unwrap_or_else(|e| panic!("bind unix socket {path}: {e}"));
    tracing::info!(path, "signer listening on unix socket");
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let state = Arc::clone(&state);
                thread::spawn(move || serve_conn(&state, stream));
            }
            Err(e) => tracing::warn!(error = %e, "accept failed"),
        }
    }
}

#[cfg(target_os = "linux")]
fn serve_vsock(state: Arc<SignerState>, port: u32) {
    let listener = vsock::VsockListener::bind_with_cid_port(vsock::VMADDR_CID_ANY, port)
        .unwrap_or_else(|e| panic!("bind vsock port {port}: {e}"));
    tracing::info!(port, "signer listening on AF_VSOCK");
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let state = Arc::clone(&state);
                thread::spawn(move || serve_conn(&state, stream));
            }
            Err(e) => tracing::warn!(error = %e, "accept failed"),
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn serve_vsock(_state: Arc<SignerState>, _port: u32) {
    eprintln!("error: AF_VSOCK is only available on Linux; use --unix for local development");
    std::process::exit(2);
}
