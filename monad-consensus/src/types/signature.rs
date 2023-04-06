use crate::Hash;
use monad_crypto::secp256k1::Signature;

#[derive(Clone, Debug)]
pub struct ConsensusSignature(pub Signature);

pub trait SignatureCollection: Default + Clone {
    fn new(max_stake: i64) -> Self;

    // is voting power >= 2f+1
    fn verify_quorum(&self) -> bool;

    // TODO get the return type from monad-validators later
    fn current_stake(&self) -> i64;

    // hash of all the signatures
    fn get_hash(&self) -> Hash;

    // add the signature from a signed vote message and its voting power to the
    // aggregate
    fn add_signature(&mut self, s: ConsensusSignature, stake: i64);

    fn get_signatures(&self) -> Vec<&ConsensusSignature>;
}
