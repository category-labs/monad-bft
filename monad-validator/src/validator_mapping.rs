use std::collections::BTreeMap;

use monad_crypto::certificate_signature::{CertificateKeyPair, PubKey};
use monad_types::NodeId;

/// Map validator NodeId to its Certificate PubKey
pub struct ValidatorMapping<PT: PubKey, VKT: CertificateKeyPair> {
    pub map: BTreeMap<NodeId<PT>, VKT::PubKeyType>,
}

impl<PT: PubKey, VKT: CertificateKeyPair> ValidatorMapping<PT, VKT> {
    pub fn new(iter: impl IntoIterator<Item = (NodeId<PT>, VKT::PubKeyType)>) -> Self {
        Self {
            map: iter.into_iter().collect(),
        }
    }
}

impl<PT: PubKey, VKT: CertificateKeyPair> IntoIterator for ValidatorMapping<PT, VKT> {
    type Item = (NodeId<PT>, VKT::PubKeyType);
    type IntoIter = std::collections::btree_map::IntoIter<NodeId<PT>, VKT::PubKeyType>;

    fn into_iter(self) -> Self::IntoIter {
        self.map.into_iter()
    }
}
