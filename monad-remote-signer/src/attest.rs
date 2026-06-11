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

//! SEV-SNP attestation (provisioning-time gate — stretch goal).
//!
//! The meaningful use is at *provisioning* time: the keyloader asks the enclave
//! for a report, verifies the measurement and that `report_data` binds the
//! pubkeys it is about to trust, and only then releases the key. The binding
//! computation below is real and testable; fetching the signed SNP report from
//! `/dev/sev-guest` is the documented stretch (see `attestation_report`).

use sha2::{Digest, Sha256};

use monad_remote_signer_proto::Pubkeys;

/// `report_data` ties an attestation report to the exact keys held by the
/// enclave: `sha256(nonce || secp_pubkey || bls_pubkey)`. The keyloader
/// recomputes this from the pubkeys it receives and checks it against the
/// report, so a swapped-key enclave fails verification.
pub fn report_data(nonce: &[u8; 32], pubkeys: &Pubkeys) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(nonce);
    h.update(&pubkeys.secp);
    h.update(&pubkeys.bls);
    h.finalize().into()
}

/// Fetch a signed SNP attestation report whose `report_data` is the binding
/// above.
///
/// STRETCH / TODO(snp): on a real SEV-SNP guest, set `report_data` and issue
/// `SNP_GET_REPORT` against `/dev/sev-guest` (via the `sev` crate's
/// `firmware::guest::Firmware::get_report`), returning the raw report bytes.
/// The keyloader then verifies the measurement and the binding before
/// provisioning. Until then this returns an explanatory error so the rest of
/// the flow works on non-SNP hardware.
pub fn attestation_report(nonce: &[u8; 32], pubkeys: &Pubkeys) -> Result<Vec<u8>, String> {
    let _binding = report_data(nonce, pubkeys);
    Err("attestation unavailable: run on a SEV-SNP guest with /dev/sev-guest support".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_is_deterministic_and_nonce_sensitive() {
        let pk = Pubkeys {
            secp: vec![1, 2, 3],
            bls: vec![4, 5, 6],
        };
        let a = report_data(&[0u8; 32], &pk);
        let b = report_data(&[0u8; 32], &pk);
        let c = report_data(&[1u8; 32], &pk);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
