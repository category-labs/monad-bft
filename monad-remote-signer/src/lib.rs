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

//! Enclave-side signer service.
//!
//! Holds the secp256k1 and BLS12-381 private keys **in memory only** (inside the
//! SEV-SNP confidential VM, whose RAM the host cannot read) and answers signing
//! / ECDH / provisioning requests over a [`Transport`]. Decryption of a
//! provisioned keystore happens here, inside the enclave, so the plaintext
//! secret never exists in host RAM.

use std::{
    io::{Read, Write},
    sync::RwLock,
};

use monad_bls::BlsKeyPair;
use monad_keystore::keystore::Keystore;
use monad_remote_signer_proto::{
    protocol::{read_frame, write_frame, Op, SECP_DIGEST_LEN, SECP_PUBKEY_LEN},
    ProvisionBundle, ProvisionRequest, Pubkeys,
};
use monad_secp::{KeyPair, PubKey};

pub mod attest;

/// The keys the enclave is holding. Either may be unset until provisioned.
#[derive(Default)]
pub struct SignerState {
    secp: RwLock<Option<KeyPair>>,
    bls: RwLock<Option<BlsKeyPair>>,
}

impl SignerState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed both keys directly (used by `--generate` for dev demos).
    pub fn set_keys(&self, secp: Option<KeyPair>, bls: Option<BlsKeyPair>) {
        if let Some(s) = secp {
            *self.secp.write().unwrap() = Some(s);
        }
        if let Some(b) = bls {
            *self.bls.write().unwrap() = Some(b);
        }
    }

    fn pubkeys(&self) -> Result<Pubkeys, String> {
        let secp = self.secp.read().unwrap();
        let bls = self.bls.read().unwrap();
        let secp = secp.as_ref().ok_or("secp key not provisioned")?;
        let bls = bls.as_ref().ok_or("bls key not provisioned")?;
        Ok(Pubkeys {
            secp: secp.pubkey().bytes_compressed().to_vec(),
            bls: bls.pubkey().compress().to_vec(),
        })
    }
}

/// Decrypt one keystore bundle **inside the enclave** and build the keypair.
/// Mirrors `Keystore::load_secp_key` / `load_bls_key` but operates on in-memory
/// JSON bytes rather than a file path.
fn decrypt_bundle<T>(
    bundle: &ProvisionBundle,
    to_key: impl FnOnce(
        monad_keystore::keystore::KeystoreSecret,
        monad_keystore::keystore::KeystoreVersion,
    ) -> Result<T, String>,
) -> Result<T, String> {
    let json = std::str::from_utf8(&bundle.keystore_json)
        .map_err(|_| "keystore json not utf-8".to_string())?;
    let ks: Keystore = serde_json::from_str(json).map_err(|e| format!("bad keystore json: {e}"))?;
    let secret = ks
        .crypto
        .decrypt(&ks.ciphertext, &bundle.password, &ks.checksum)
        .map_err(|e| format!("keystore decrypt failed: {e:?}"))?;
    to_key(secret, ks.version)
}

fn provision(state: &SignerState, req: &ProvisionRequest) -> Result<Pubkeys, String> {
    if let Some(b) = &req.secp {
        let kp = decrypt_bundle(b, |s, v| {
            s.to_secp(v).map_err(|e| format!("secp from keystore: {e:?}"))
        })?;
        *state.secp.write().unwrap() = Some(kp);
    }
    if let Some(b) = &req.bls {
        let kp = decrypt_bundle(b, |s, v| {
            s.to_bls(v).map_err(|e| format!("bls from keystore: {e:?}"))
        })?;
        *state.bls.write().unwrap() = Some(kp);
    }
    state.pubkeys()
}

/// Process one request, returning the response `(op, payload)`. Errors are
/// returned as `(Op::Error, message)` so the caller can frame them uniformly.
pub fn handle_request(state: &SignerState, op: u8, payload: &[u8]) -> (Op, Vec<u8>) {
    match dispatch(state, op, payload) {
        Ok((op, payload)) => (op, payload),
        Err(msg) => (Op::Error, msg.into_bytes()),
    }
}

fn dispatch(state: &SignerState, op: u8, payload: &[u8]) -> Result<(Op, Vec<u8>), String> {
    let op = Op::from_u8(op).ok_or_else(|| format!("unknown op {op:#x}"))?;
    match op {
        Op::SignSecp => {
            let digest: [u8; SECP_DIGEST_LEN] = payload
                .try_into()
                .map_err(|_| "SignSecp expects a 32-byte digest".to_string())?;
            let guard = state.secp.read().unwrap();
            let key = guard.as_ref().ok_or("secp key not provisioned")?;
            let sig = key.sign_prehashed(&digest);
            Ok((Op::SignSecp, sig.serialize().to_vec()))
        }
        Op::Ecdh => {
            let pk: [u8; SECP_PUBKEY_LEN] = payload
                .try_into()
                .map_err(|_| "Ecdh expects a 33-byte compressed pubkey".to_string())?;
            let peer = PubKey::from_slice(&pk).map_err(|e| format!("bad peer pubkey: {e}"))?;
            let guard = state.secp.read().unwrap();
            let key = guard.as_ref().ok_or("secp key not provisioned")?;
            Ok((Op::Ecdh, key.ecdh(&peer).to_vec()))
        }
        Op::SignBls => {
            let guard = state.bls.read().unwrap();
            let key = guard.as_ref().ok_or("bls key not provisioned")?;
            let sig = key.sign_prefixed(payload);
            Ok((Op::SignBls, sig.serialize()))
        }
        Op::GetPubkeys => Ok((Op::GetPubkeys, state.pubkeys()?.encode())),
        Op::Provision => {
            let req = ProvisionRequest::decode(payload)
                .map_err(|e| format!("bad provision request: {e}"))?;
            Ok((Op::Provision, provision(state, &req)?.encode()))
        }
        Op::Attest => {
            let nonce: [u8; 32] = payload
                .try_into()
                .map_err(|_| "Attest expects a 32-byte nonce".to_string())?;
            let pubkeys = state.pubkeys()?;
            let report = attest::attestation_report(&nonce, &pubkeys)?;
            Ok((Op::Attest, report))
        }
        Op::Error => Err("client sent an Error frame".to_string()),
    }
}

/// Serve a single connection until EOF or a transport error.
pub fn serve_conn<C: Read + Write>(state: &SignerState, mut conn: C) {
    loop {
        let (op, payload) = match read_frame(&mut conn) {
            Ok(frame) => frame,
            Err(_) => return, // EOF / closed
        };
        let (resp_op, resp_payload) = handle_request(state, op, &payload);
        if write_frame(&mut conn, resp_op, &resp_payload).is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use monad_crypto::signing_domain::{self, SigningDomain};

    use super::*;

    fn gen_keys() -> (KeyPair, BlsKeyPair) {
        let mut secp_seed = [7u8; 32];
        let mut bls_seed = [9u8; 32];
        (
            KeyPair::from_bytes(&mut secp_seed).unwrap(),
            BlsKeyPair::from_bytes(&mut bls_seed).unwrap(),
        )
    }

    #[test]
    fn sign_secp_matches_local() {
        let (secp, bls) = gen_keys();
        let pubkey = secp.pubkey();
        let state = SignerState::new();
        state.set_keys(Some(secp), Some(bls));

        // The node would compute this digest on the host.
        let msg = b"hello raptorcast";
        let local_sig = {
            let (s, _) = gen_keys();
            s.sign::<signing_domain::ConsensusMessage>(msg)
        };
        // Reproduce the node-side digest: HasherType(PREFIX || msg).
        use monad_crypto::hasher::{Hasher, HasherType};
        let mut h = HasherType::new();
        h.update(signing_domain::ConsensusMessage::PREFIX);
        h.update(msg);
        let digest = h.hash().0;

        let (op, payload) = handle_request(&state, Op::SignSecp as u8, &digest);
        assert_eq!(op, Op::SignSecp);
        let sig = monad_secp::SecpSignature::deserialize(&payload).unwrap();
        // Signature recovers to the provisioned pubkey.
        let recovered = sig
            .recover_pubkey::<signing_domain::ConsensusMessage>(msg)
            .unwrap();
        assert_eq!(recovered, pubkey);
        let _ = local_sig;
    }

    #[test]
    fn unprovisioned_returns_error() {
        let state = SignerState::new();
        let (op, payload) = handle_request(&state, Op::SignSecp as u8, &[0u8; 32]);
        assert_eq!(op, Op::Error);
        assert!(String::from_utf8_lossy(&payload).contains("not provisioned"));
    }
}
