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

use std::str::FromStr;

use alloy_consensus::SignableTransaction;
use alloy_primitives::{keccak256, Address, Signature, B256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use monad_secp::KeyPair;
use rand::RngCore;

#[derive(Clone, Debug)]
pub struct PrivateKey {
    priv_key: PrivateKeySigner,
}

impl PrivateKey {
    pub fn new(private_key: impl AsRef<str>) -> (Address, Self) {
        Self::new_with_pk(B256::from_str(private_key.as_ref()).unwrap())
    }

    pub fn new_with_random(random: &mut impl RngCore) -> (Address, Self) {
        let mut bytes = [0u8; 32];
        random.fill_bytes(&mut bytes);
        let pk = B256::from_slice(&bytes);

        Self::new_with_pk(pk)
    }

    pub fn new_with_pk(pk: B256) -> (Address, Self) {
        let kp = KeyPair::from_bytes(pk.clone().as_mut_slice()).expect("valid pk");

        let pubkey_bytes = kp.pubkey().bytes();
        assert!(pubkey_bytes.len() == 65);

        let hash = keccak256(&pubkey_bytes[1..]);

        let address = Address::from_slice(&hash[12..]);

        (
            address,
            Self {
                priv_key: PrivateKeySigner::from_bytes(&pk).expect("invalid pk"),
            },
        )
    }

    pub fn sign_transaction(&self, transaction: &impl SignableTransaction<Signature>) -> Signature {
        self.priv_key
            .sign_hash_sync(&transaction.signature_hash())
            .expect("signature works")
    }

    pub fn sign_hash(&self, hash: &B256) -> Signature {
        self.priv_key.sign_hash_sync(hash).expect("signature works")
    }
}
