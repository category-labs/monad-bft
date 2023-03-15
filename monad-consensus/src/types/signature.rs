use monad_crypto::secp256k1::Signature;

#[derive(Clone, Debug)]
pub struct ConsensusSignature(Signature);
