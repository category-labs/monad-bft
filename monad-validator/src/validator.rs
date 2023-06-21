use std::fmt::Debug;

use monad_crypto::secp256k1::SecpPubKey;

#[derive(Clone, Copy)]
pub struct Validator {
    pub pubkey: SecpPubKey,
    pub stake: i64,
}

impl Debug for Validator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "validator {:?}", self.pubkey)
    }
}
