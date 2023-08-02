use std::fmt::Debug;

use monad_crypto::CertificateSignature;
use monad_types::{Hash, NodeId};

use crate::voting::ValidatorMapping;

#[derive(Clone, Debug)]
pub struct SignatureBuilder<SCT: SignatureCollection> {
    sigs: Vec<(usize, SCT::SignatureType)>,
}

impl<SCT: SignatureCollection> Default for SignatureBuilder<SCT> {
    fn default() -> Self {
        Self::new()
    }
}

impl<SCT: SignatureCollection> SignatureBuilder<SCT> {
    pub fn new() -> Self {
        Self { sigs: Vec::new() }
    }

    pub fn add_signature(&mut self, idx: usize, sig: SCT::SignatureType) {
        self.sigs.push((idx, sig))
    }
}

impl<SCT: SignatureCollection> IntoIterator for SignatureBuilder<SCT> {
    type Item = (usize, SCT::SignatureType);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.sigs.into_iter()
    }
}

pub trait SignatureCollection: Clone + Send + Sync + std::fmt::Debug + 'static {
    type SignatureError: std::error::Error + Send + Sync;
    type SignatureType: CertificateSignature + Copy;

    /// the new() function verifies:
    ///   1. nodeId idx is in range
    ///   2. no conflicting signature
    ///   3. the signature collection built is valid
    fn new(
        sigs: Vec<(NodeId, Self::SignatureType)>,
        validator_mapping: &ValidatorMapping<
            <Self::SignatureType as CertificateSignature>::KeyPairType,
        >,
        msg: &[u8],
    ) -> Result<Self, Self::SignatureError>;

    // hash of all the signatures
    fn get_hash(&self) -> Hash;

    fn verify(
        &self,
        validator_mapping: &ValidatorMapping<
            <Self::SignatureType as CertificateSignature>::KeyPairType,
        >,
        msg: &[u8],
    ) -> Result<Vec<NodeId>, Self::SignatureError>;

    // TODO: deprecate this function: only used by tests
    fn num_signatures(&self) -> usize;
}
