use std::str::FromStr;

use monad_secp::KeyPair;
use rand::RngCore;
use reth_primitives::{keccak256, sign_message, Address, Signature, Transaction, B256};

#[derive(Clone, Debug)]
pub struct PrivateKey {
    priv_key: B256,
}

impl PrivateKey {
    pub fn new(private_key: String) -> (Address, Self) {
        Self::new_with_pk(B256::from_str(&private_key).unwrap())
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

        (address, Self { priv_key: pk })
    }

    pub fn sign_transaction(&self, transaction: &Transaction) -> Signature {
        sign_message(self.priv_key, transaction.signature_hash()).expect("signature works")
    }
}
