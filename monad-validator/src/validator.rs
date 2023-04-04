use std::fmt::Debug;
use std::hash::Hash;

use monad_crypto::secp256k1::PubKey;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct Address(pub i64); // placeholder type for Address

#[derive(Clone, Copy)]
pub struct Validator {
    pub address: Address,
    pub pubkey: PubKey,
    pub stake: i64,
}

impl Debug for Validator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "validator {:?}", self.address.0)
    }
}
