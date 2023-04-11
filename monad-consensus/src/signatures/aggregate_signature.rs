use monad_crypto::{
    secp256k1::{Error, PubKey},
    Signature,
};
use monad_types::Hash;

use crate::types::signature::SignatureCollection;
use sha2::Digest;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateSignatures<S> {
    pub sigs: Vec<S>,
}

impl<S: Signature> Default for AggregateSignatures<S> {
    fn default() -> Self {
        Self { sigs: Vec::new() }
    }
}

impl<S: Signature> SignatureCollection for AggregateSignatures<S> {
    type SignatureType = S;

    fn new() -> Self {
        AggregateSignatures { sigs: Vec::new() }
    }

    fn get_hash(&self) -> Hash {
        let mut hasher = sha2::Sha256::new();

        for v in self.sigs.iter() {
            hasher.update(v.serialize());
        }

        hasher.finalize().into()
    }

    fn add_signature(&mut self, sig: Self::SignatureType) {
        self.sigs.push(sig);
    }

    fn verify_signatures(&self, msg: &[u8]) -> Result<(), Error> {
        for s in self.sigs.iter() {
            let pubkey = s.recover_pubkey(msg)?;
            s.verify(msg, &pubkey)?;
        }
        Ok(())
    }

    fn get_pubkeys(&self, msg: &[u8]) -> Result<Vec<PubKey>, Error> {
        self.sigs
            .iter()
            .map(|s| -> Result<PubKey, Error> { s.recover_pubkey(msg) })
            .collect()
    }

    fn num_signatures(&self) -> usize {
        self.sigs.len()
    }
}
