use crate::types::signature::{ConsensusSignature, SignatureCollection};
use sha2::Digest;

#[derive(Clone, Debug)]
pub struct AggregateSignatures {
    pub sigs: Vec<ConsensusSignature>,
}

impl Default for AggregateSignatures {
    fn default() -> Self {
        Self { sigs: Vec::new() }
    }
}

impl SignatureCollection for AggregateSignatures {
    fn new() -> Self {
        AggregateSignatures { sigs: Vec::new() }
    }

    fn get_hash(&self) -> crate::Hash {
        let mut hasher = sha2::Sha256::new();

        for v in self.sigs.iter() {
            hasher.update(v.0.serialize());
        }

        hasher.finalize().into()
    }

    fn add_signature(&mut self, s: ConsensusSignature) {
        self.sigs.push(s);
    }

    fn get_signatures(&self) -> Vec<&ConsensusSignature> {
        self.sigs.iter().collect()
    }
}
