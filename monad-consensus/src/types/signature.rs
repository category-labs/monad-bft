use monad_crypto::{
    secp256k1::{Error, PubKey},
    Signature,
};
use monad_types::Hash;

pub trait SignatureCollection: Clone + Default + std::fmt::Debug {
    type SignatureType: Signature;

    fn new() -> Self;

    // hash of all the signatures
    fn get_hash(&self) -> Hash;

    // add the signature from a signed vote message
    fn add_signature(&mut self, s: Self::SignatureType);

    fn verify_signatures(&self, msg: &[u8]) -> Result<(), Error>;

    fn get_pubkeys(&self, msg: &[u8]) -> Result<Vec<PubKey>, Error>;

    fn num_signatures(&self) -> usize;
}
