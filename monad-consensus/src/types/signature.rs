use monad_crypto::secp256k1::Signature;
use monad_types::Hash;

#[derive(Copy, Clone, Debug)]
pub struct ConsensusSignature(pub Signature);

pub trait SignatureCollection: Default + Clone {
    fn new() -> Self;

    // hash of all the signatures
    fn get_hash(&self) -> Hash;

    // add the signature from a signed vote message
    fn add_signature(&mut self, s: ConsensusSignature);

    fn get_signatures(&self) -> Vec<&ConsensusSignature>;
}
