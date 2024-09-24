use std::collections::BTreeMap;

use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey},
    hasher::{Hasher, HasherType},
};
use monad_types::{NodeId, Stake};

#[derive(Clone)]
pub struct EpochValidators<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub validators: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Validator>,
}

impl<ST> EpochValidators<ST>
where
    ST: CertificateSignatureRecoverable,
{
    /// Returns a view of the validator set without a given node. On ValidatorsView being dropped,
    /// the validator set is reverted back to normal.
    pub fn view_without(
        &mut self,
        without: Vec<&NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> ValidatorsView<ST> {
        let mut removed = Vec::new();
        for without in without {
            if let Some(removed_validator) = self.validators.remove(without) {
                removed.push((*without, removed_validator));
            }
        }
        ValidatorsView {
            view: &mut self.validators,
            removed,
        }
    }
}

#[derive(Debug)]
pub struct ValidatorsView<'a, ST>
where
    ST: CertificateSignatureRecoverable,
{
    view: &'a mut BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Validator>,
    removed: Vec<(NodeId<CertificateSignaturePubKey<ST>>, Validator)>,
}
impl<'a, ST> ValidatorsView<'a, ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn view(&self) -> &BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, Validator> {
        self.view
    }
}

impl<'a, ST> Drop for ValidatorsView<'a, ST>
where
    ST: CertificateSignatureRecoverable,
{
    fn drop(&mut self) {
        while let Some((without, removed_validator)) = self.removed.pop() {
            self.view.insert(without, removed_validator);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Validator {
    pub stake: Stake,
}

#[derive(Debug)]
pub enum BuildTarget<'a, ST: CertificateSignatureRecoverable> {
    Broadcast(
        // validator stakes for given epoch_no, not including self
        // this MUST NOT BE EMPTY
        ValidatorsView<'a, ST>,
    ),
    Raptorcast(
        // validator stakes for given epoch_no, not including self
        // this MUST NOT BE EMPTY
        ValidatorsView<'a, ST>,
    ), // sharded raptor-aware broadcast
    PointToPoint(&'a NodeId<CertificateSignaturePubKey<ST>>),
}

pub fn compute_hash<PT>(id: &NodeId<PT>) -> [u8; 20]
where
    PT: PubKey,
{
    let mut hasher = HasherType::new();
    hasher.update(id.pubkey().bytes());
    hasher.hash().0[..20].try_into().expect("20 bytes")
}
