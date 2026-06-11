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

//! End-to-end: real server + real wire protocol + pooled client over a unix
//! socket. Proves provision -> sign(secp/bls) -> ecdh against a key that was
//! decrypted *inside* the server, with results matching local computation.
//! The vsock transport is a drop-in swap for the unix socket used here.

#![cfg(unix)]

use std::{os::unix::net::UnixListener, sync::Arc, thread};

use monad_bls::{BlsKeyPair, BlsSignature};
use monad_crypto::{
    certificate_signature::CertificateSignature,
    hasher::{Hasher, HasherType},
    signing_domain::{self, SigningDomain},
};
use monad_keystore::keystore::Keystore;
use monad_remote_signer::{serve_conn, SignerState};
use monad_remote_signer_proto::{ProvisionBundle, ProvisionRequest, RemoteSigner, TransportConfig};
use monad_secp::{KeyPair, SecpSignature};

fn secp_digest<SD: SigningDomain>(msg: &[u8]) -> [u8; 32] {
    let mut h = HasherType::new();
    h.update(SD::PREFIX);
    h.update(msg);
    h.hash().0
}

#[test]
fn provision_then_sign_and_ecdh_over_unix() {
    let tmp = std::env::temp_dir();
    let pid = std::process::id();
    let sock = tmp.join(format!("monad-signer-{pid}.sock"));
    let secp_ks = tmp.join(format!("secp-{pid}.json"));
    let bls_ks = tmp.join(format!("bls-{pid}.json"));

    // Known secrets -> encrypted keystores on the "host".
    let secp_secret = [3u8; 32];
    let bls_secret = [5u8; 32];
    Keystore::create_keystore_json(&secp_secret, "pw", &secp_ks).unwrap();
    Keystore::create_keystore_json(&bls_secret, "pw", &bls_ks).unwrap();

    // Expected pubkeys / local reference keys.
    let local_secp = KeyPair::from_bytes(&mut secp_secret.clone()).unwrap();
    let local_bls = BlsKeyPair::from_bytes(&mut bls_secret.clone()).unwrap();
    let expected_secp_pub = local_secp.pubkey();
    let expected_bls_pub = local_bls.pubkey();

    // Start the enclave server (here: a thread serving the unix socket).
    let state = Arc::new(SignerState::new());
    let listener = UnixListener::bind(&sock).unwrap();
    {
        let state = Arc::clone(&state);
        thread::spawn(move || {
            for conn in listener.incoming() {
                if let Ok(c) = conn {
                    let state = Arc::clone(&state);
                    thread::spawn(move || serve_conn(&state, c));
                }
            }
        });
    }

    let signer =
        Arc::new(RemoteSigner::connect(TransportConfig::Unix { path: sock.clone() }, 4).unwrap());

    // Provision: forward ciphertext + password; the server decrypts internally.
    let req = ProvisionRequest {
        secp: Some(ProvisionBundle {
            keystore_json: std::fs::read(&secp_ks).unwrap(),
            password: "pw".into(),
        }),
        bls: Some(ProvisionBundle {
            keystore_json: std::fs::read(&bls_ks).unwrap(),
            password: "pw".into(),
        }),
    };
    let pubkeys = signer.provision(&req).unwrap();
    assert_eq!(pubkeys.secp, expected_secp_pub.bytes_compressed().to_vec());
    assert_eq!(pubkeys.bls, expected_bls_pub.compress().to_vec());

    // secp: the hot RaptorCast path. Node hashes; enclave signs the digest.
    let msg = b"hot-path chunk header";
    let digest = secp_digest::<signing_domain::RaptorcastChunk>(msg);
    let sig = SecpSignature::deserialize(&signer.sign_secp(&digest).unwrap()).unwrap();
    let recovered = sig
        .recover_pubkey::<signing_domain::RaptorcastChunk>(msg)
        .unwrap();
    assert_eq!(recovered, expected_secp_pub, "secp signature must recover to provisioned key");

    // bls: low-frequency consensus vote. Node prefixes; enclave signs with DST.
    let vote = b"vote payload";
    let prefixed = [signing_domain::Vote::PREFIX, vote].concat();
    let bls_sig = BlsSignature::deserialize(&signer.sign_bls(&prefixed).unwrap()).unwrap();
    bls_sig
        .verify::<signing_domain::Vote>(vote, &expected_bls_pub)
        .expect("bls signature must verify under provisioned key");

    // ecdh (wireauth): remote result matches local computation.
    let peer = KeyPair::from_bytes(&mut [11u8; 32]).unwrap();
    let peer_pub = peer.pubkey();
    let expected_shared = local_secp.ecdh(&peer_pub);
    let remote_shared = signer.ecdh(&peer_pub.bytes_compressed()).unwrap();
    assert_eq!(expected_shared, remote_shared, "ecdh must match local");

    // The actual node seam: build the Remote enum variants and drive them
    // through the generic CertificateSignature trait path that RaptorCast
    // (regular.rs) and consensus voting use. This is what monad-node relies on.
    let remote_secp = KeyPair::remote(Arc::clone(&signer)).unwrap();
    let remote_bls = BlsKeyPair::remote(Arc::clone(&signer)).unwrap();
    assert_eq!(remote_secp.pubkey(), expected_secp_pub);
    assert_eq!(
        remote_bls.pubkey().compress(),
        expected_bls_pub.compress(),
        "remote bls pubkey must match"
    );

    let chunk_msg = b"raptorcast chunk via Remote enum";
    let trait_sig =
        SecpSignature::sign::<signing_domain::RaptorcastChunk>(chunk_msg, &remote_secp);
    assert_eq!(
        trait_sig
            .recover_pubkey::<signing_domain::RaptorcastChunk>(chunk_msg)
            .unwrap(),
        expected_secp_pub,
        "CertificateSignature::sign over a Remote KeyPair must recover correctly"
    );

    let vote2 = b"consensus vote via Remote enum";
    let bls_trait_sig = BlsSignature::sign::<signing_domain::Vote>(vote2, &remote_bls);
    bls_trait_sig
        .verify::<signing_domain::Vote>(vote2, &expected_bls_pub)
        .expect("CertificateSignature::sign over a Remote BlsKeyPair must verify");

    // wireauth's direct ecdh call through the Remote enum.
    assert_eq!(remote_secp.ecdh(&peer_pub), expected_shared);

    // Many signs to exercise the pool + populate the latency histogram.
    for _ in 0..1000 {
        let _ = signer.sign_secp(&digest).unwrap();
    }
    eprintln!("latency over unix socket:\n{}", signer.latency_report());

    let _ = std::fs::remove_file(&sock);
    let _ = std::fs::remove_file(&secp_ks);
    let _ = std::fs::remove_file(&bls_ks);
}
